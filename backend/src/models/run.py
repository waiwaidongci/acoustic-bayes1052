from sqlalchemy import Column,Integer,String,JSON,Text,FLOAT,text
from sqlalchemy import ForeignKey
from ..config.database import Base
from ..config.settings import CONFIG
class Run(Base):
    __tablename__=CONFIG['run_table']
    id=Column(Integer,primary_key=True)
    source_id=Column(Integer,ForeignKey(CONFIG['source_table']+'.id'),nullable=False)
    name=Column(String(160),nullable=False)
    state=Column(String(20),default='queued')
    parameters=Column(JSON,nullable=False)
    snapshot=Column(JSON,nullable=False)
    result=Column(JSON,nullable=True)
    error=Column(Text,default='')
    created_at=Column(String(40),nullable=False)
    # 执行租约：只有持有 lease_token 的 worker 能发布结果或失败原因。
    # lease_expires_at 是 Unix 秒；进程断电后租约到期，其他 worker 可接管。
    attempt=Column(Integer,nullable=False,default=0,server_default=text('0'))
    lease_token=Column(String(64),nullable=False,default='',server_default=text("''"))
    lease_owner=Column(String(160),nullable=False,default='',server_default=text("''"))
    # 必须双精度：单精度 FLOAT 存 epoch 秒会丢失约 2 分钟的精度。
    lease_expires_at=Column(FLOAT(precision=53),nullable=False,default=0.0,server_default=text('0'))
    started_at=Column(String(40),nullable=False,default='',server_default=text("''"))
    finished_at=Column(String(40),nullable=False,default='',server_default=text("''"))
    # 成品 CSV 的文件名与内容 SHA256；结果与文件都一致后才算完成。
    artifact_name=Column(String(200),nullable=False,default='',server_default=text("''"))
    artifact_checksum=Column(String(64),nullable=False,default='',server_default=text("''"))
