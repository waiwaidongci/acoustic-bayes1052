import os, sys, tempfile
ROOT=os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BACKEND=os.path.join(ROOT,'backend')
_tmp=tempfile.mkdtemp(prefix='gb0041-')
os.environ['DATABASE_URL']='sqlite:///'+os.path.join(_tmp,'test.db')
os.environ['ARTIFACT_DIR']=os.path.join(_tmp,'artifacts')
os.environ['SEED_DEMO']='false'
os.environ['RUN_LEASE_SECONDS']='30'
os.environ['RUN_HEARTBEAT_SECONDS']='1'
os.environ['RUN_MAX_ATTEMPTS']='5'
os.environ['REDIS_URL']='redis://127.0.0.1:6399/0'
sys.path.insert(0,BACKEND)
import pytest

@pytest.fixture(autouse=True)
def clean():
    from src.config.database import Base, engine, Session
    from src.config.settings import ARTIFACT_DIR
    from src.models import source, record, note, audit
    from src.models.run import Run
    Base.metadata.create_all(engine)
    with Session() as db:
        for model in (audit.Audit,note.Note,Run,record.Record,source.Source):
            db.query(model).delete()
        db.commit()
    ARTIFACT_DIR.mkdir(parents=True,exist_ok=True)
    for p in ARTIFACT_DIR.glob('*'):
        if p.is_file():p.unlink()
    yield


@pytest.fixture
def source_id():
    from src.config.database import Session
    from hooks import make_source
    with Session() as db:
        return make_source(db).id
