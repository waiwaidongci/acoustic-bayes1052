"""HTTP 端到端：队列完全不可用时仍能受理、补算、导出与显示阶段。"""
import pytest
from fastapi.testclient import TestClient


@pytest.fixture(scope='module')
def client():
    from src.main import app
    with TestClient(app) as c:
        yield c


def login(client,email):
    r=client.post('/auth/login',json={'email':email,'password':'Research2026!'})
    assert r.status_code==200,r.text
    return {'Authorization':'Bearer '+r.json()['token']}


@pytest.fixture(scope='module',autouse=True)
def users(client):
    from src.config.database import Base, Session, engine
    from src.models.user import User
    from src.utils.security import hash_password
    Base.metadata.create_all(engine)
    with Session() as db:
        for role in ('admin','analyst','viewer'):
            email=role+'@acoustic-bayes.demo'
            if not db.query(User).filter_by(email=email).first():
                db.add(User(email=email,password_hash=hash_password('Research2026!'),role=role))
        db.commit()


@pytest.fixture
def seeded_source():
    from src.config.database import Session
    with Session() as db:
        from hooks import make_source
        return make_source(db).id


def test_health_reports_queue_without_failing(client):
    r=client.get('/health')
    assert r.status_code==200
    assert r.json()=={'status':'ok','queue':False}


def test_run_lifecycle_with_queue_down(client,seeded_source):
    auth=login(client,'analyst@acoustic-bayes.demo')
    r=client.post('/runs',headers=auth,json={'source_id':seeded_source,'name':'停机补算研究','parameters':{'folds':3,'smoothing':1e-09}})
    assert r.status_code==200,r.text
    created=r.json();rid=created['id']
    assert created['state']=='queued' and created['phase']=='waiting' and created['artifact_ready'] is False

    listed={x['id']:x for x in client.get('/runs',headers=auth).json()}
    assert listed[rid]['phase']=='waiting'

    # 队列停机：导出被明确拒绝而非报 500
    assert client.get(f'/runs/{rid}/export',headers=auth).status_code==409

    from src.services import jobs
    assert jobs.execute(rid)=='completed'

    detail=client.get(f'/runs/{rid}',headers=auth).json()
    assert detail['state']=='completed' and detail['phase']=='completed'
    assert detail['artifact_ready'] is True and detail['artifact_checksum']
    assert detail['result']['rows']

    csv=client.get(f'/runs/{rid}/export',headers=auth)
    assert csv.status_code==200
    body=csv.content.decode('utf-8-sig')
    lines=body.strip().splitlines()
    assert lines[0]=='reference,actual,predicted,posterior'
    assert len(lines)-1==len(detail['result']['rows']), '导出必须包含全部结果行'

    # 完成的任务可以据此提交说明
    note=client.post('/notes',headers=auth,json={'run_id':rid,'title':'停机补算说明','body':'队列恢复后补算完成。'})
    assert note.status_code==200,note.text


def test_failed_run_exposes_reason(client,seeded_source):
    auth=login(client,'analyst@acoustic-bayes.demo')
    r=client.post('/runs',headers=auth,json={'source_id':seeded_source,'name':'参数错误研究','parameters':{'folds':3,'smoothing':1e-09}})
    rid=r.json()['id']
    from src.config.database import Session
    from src.models.run import Run
    from src.services import jobs
    with Session() as db:
        run=db.get(Run,rid);run.snapshot={'source':{},'records':[{'reference':'x','values':{'rms':1,'centroid':2,'kurtosis':3,'label':'正常'}}]};db.commit()
    assert jobs.execute(rid)=='failed'
    detail=client.get(f'/runs/{rid}',headers=auth).json()
    assert detail['state']=='failed' and detail['phase']=='failed'
    assert detail['error'] and detail['artifact_ready'] is False
    assert client.get(f'/runs/{rid}/export',headers=auth).status_code==409
