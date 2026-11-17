import logging
from sqlalchemy import select
from fastapi import HTTPException
from redis import Redis
from ..config.settings import REDIS_URL, QUEUE_NAME
from ..models.run import Run
from ..utils.serialization import row, now
from .sources import source_or_404, list_records
from .domain_validation import validate_parameters
from .audit import log
from . import artifacts
queue=Redis.from_url(REDIS_URL,socket_connect_timeout=2,socket_timeout=2)

def get_run(db,id):
    r=db.get(Run,id)
    if not r:raise HTTPException(404,'未找到分析任务')
    return r

def valid_result(result):
    return (isinstance(result,dict) and isinstance(result.get('rows'),list)
            and len(result['rows'])>0 and isinstance(result.get('columns'),list)
            and len(result['columns'])>0)

def _legacy(r):
    """历史完成记录：没有成品登记，升级后仍按原样可读。"""
    return r.state=='completed' and not r.artifact_name and not (r.artifact_checksum or '')

def artifact_ready(r,strict=True):
    """完成态要求：结果完整、成品文件存在且校验和一致。

    strict 会按数据库中的结果重建 CSV 并比对 SHA256，确认“结果与导出文件内容一致”；
    列表接口用非 strict 只校验文件校验和，避免一次列出大量任务时重复重建 CSV。
    """
    result=r.result
    if not valid_result(result):return False
    if _legacy(r):return True
    data=artifacts.read_verified(r.artifact_name,r.artifact_checksum or '')
    if data is None:return False
    if not strict:return True
    try:return artifacts.checksum(artifacts.csv_bytes(result))==r.artifact_checksum
    except Exception:return False

def phase_of(r,strict=True):
    """给页面用的显式阶段：等待 / 执行 / 接管 / 恢复成品 / 完成 / 失败。"""
    if r.state=='queued':return 'waiting'
    if r.state=='running':return 'takeover' if (r.attempt or 0)>1 else 'running'
    if r.state=='completed':return 'completed' if artifact_ready(r,strict) else 'recovering'
    return 'failed'

def _with_status(r,data,strict=True):
    """phase 是页面显示用的真实阶段；成品未齐备时不会是 completed。"""
    data['phase']=phase_of(r,strict)
    data['artifact_ready']=data['phase']=='completed'
    return data

def view(r):
    data={k:v for k,v in row(r).items() if k not in ('snapshot','result','lease_token','lease_owner')}
    return _with_status(r,data,strict=False)

def detail(r):
    data=row(r);data.pop('lease_token',None);data.pop('lease_owner',None)
    return _with_status(r,data)

def enqueue(id):
    """入队只是唤醒提示；队列停机时任务仍在数据库排队，worker 会补算。"""
    try:
        queue.rpush(QUEUE_NAME,id);return True
    except Exception:
        logging.warning('analysis queue unavailable; run %s stays queued for database backfill',id)
        return False

def create_run(db,user,data):
    s=source_or_404(db,data.source_id);records=list_records(db,s.id)
    if len(records)<3:raise HTTPException(422,'至少录入3条样本后才能执行')
    try: params=validate_parameters(data.parameters)
    except ValueError as e:raise HTTPException(422,str(e))
    r=Run(source_id=s.id,name=data.name,parameters=params,snapshot={'source':row(s),'records':records},created_at=now(),state='queued')
    db.add(r);db.flush();log(db,user,'run.queued',r.id,data.name);db.commit()
    enqueue(r.id)
    return view(r)

def list_runs(db):
    return [view(r) for r in db.scalars(select(Run).order_by(Run.id.desc()))]
