"""共享测试夹具，供 test_recovery / test_worker / test_api 复用。"""
import time
import pytest

LABELS=['正常','轴承磨损','转子偏心']


def make_source(db,n=30):
    from src.models import source,record
    from src.services.domain_validation import validate_record
    s=source.Source(key='test-'+str(time.time_ns()),name='测试来源',filename='t.csv',description='')
    db.add(s);db.flush()
    for i in range(n):
        cls=i%3
        vals=validate_record({'rms':1.2+cls*1.8+(i%5)*.01,'centroid':900+cls*700,'kurtosis':2.8+cls*1.2,'label':LABELS[cls]})
        db.add(record.Record(source_id=s.id,reference=f'R-{i:04}',values=vals))
    db.commit();return s


def make_run(source_id,state='queued',attempt=0,lease=0.0,result=None,artifact_name='',artifact_checksum=''):
    from src.config.database import Session
    from src.models import source,record
    from src.models.run import Run
    from src.utils.serialization import now
    with Session() as db:
        s=db.get(source.Source,source_id)
        recs=[{'id':r.id,'source_id':r.source_id,'reference':r.reference,'values':r.values}
              for r in db.query(record.Record).filter_by(source_id=source_id).order_by(record.Record.id)]
        r=Run(source_id=source_id,name='测试任务',state=state,parameters={'folds':3,'smoothing':1e-09},
              snapshot={'source':{'id':s.id,'name':s.name,'filename':s.filename},'records':recs},
              created_at=now(),attempt=attempt,lease_expires_at=lease,result=result,
              artifact_name=artifact_name,artifact_checksum=artifact_checksum)
        db.add(r);db.commit();return r.id


@pytest.fixture
def source_id():
    from src.config.database import Session
    with Session() as db:return make_source(db).id
