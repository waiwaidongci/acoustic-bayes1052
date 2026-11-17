"""任务租约、接管与幂等发布。

数据库是任务的唯一事实来源，Redis 列表只是唤醒提示：
- `create_run` 先落库再入队，入队失败也不影响受理；
- worker 每轮都会从数据库补领 `queued` 或租约过期的 `running` 任务；
- 领取、续租、失败、发布全部以 `lease_token` 做条件更新，
  因此并发争抢只有一个赢家，超时接管后旧进程无法发布结果、
  也无法覆盖新的失败原因或成品文件。
"""
import json, logging, os, socket, threading, time, uuid
from sqlalchemy import and_, case, func, or_, select, update
from ..config.database import Session
from ..config.settings import RUN_HEARTBEAT_SECONDS, RUN_LEASE_SECONDS, RUN_MAX_ATTEMPTS
from ..models.run import Run
from ..services import artifacts
from ..services.algorithm import calculate
from ..services.audit import log
from ..utils.serialization import now

WORKER_ID=f'{socket.gethostname()}:{os.getpid()}'

def claimable(now_ts=None):
    now_ts=time.time() if now_ts is None else now_ts
    return or_(Run.state=='queued',and_(Run.state=='running',Run.lease_expires_at<=now_ts))

def claimable_ids(db,limit=20):
    """按创建顺序返回可领取的任务：排队中，或租约已过期的执行中。"""
    return list(db.scalars(select(Run.id).where(claimable()).order_by(Run.id).limit(limit)))

def claim(db,run_id,owner=WORKER_ID):
    """原子领取任务；返回 None 表示已被其他 worker 持有或任务已结束。"""
    token=uuid.uuid4().hex;now_ts=time.time()
    result=db.execute(update(Run).where(
        Run.id==run_id,claimable(now_ts),
    ).values(
        state='running',lease_token=token,lease_owner=owner,
        lease_expires_at=now_ts+RUN_LEASE_SECONDS,error='',
        attempt=func.coalesce(Run.attempt,0)+1,
        started_at=case((or_(Run.started_at.is_(None),Run.started_at==''),now()),else_=Run.started_at),
    ))
    db.commit()
    if result.rowcount!=1:
        db.rollback();return None
    db.expire_all()
    return db.get(Run,run_id)

class Lease:
    """执行期间后台续租；续租失败即失去持有权。

    `on_beat` 用来在长计算期间维持 worker 心跳，避免健康检查误判。
    """
    def __init__(self,run_id,token,on_beat=None):
        self.run_id=run_id;self.token=token;self.on_beat=on_beat;self.lost=threading.Event()
        self._stop=threading.Event();self._thread=None
    def start(self):
        self._thread=threading.Thread(target=self._run,daemon=True,name=f'lease-{self.run_id}')
        self._thread.start()
    def _run(self):
        while not self._stop.wait(RUN_HEARTBEAT_SECONDS):
            if self.on_beat:self.on_beat()
            if not self.renew():
                self.lost.set();return
    def renew(self):
        try:
            with Session() as db:
                result=db.execute(update(Run).where(
                    Run.id==self.run_id,Run.lease_token==self.token,Run.state=='running',
                ).values(lease_expires_at=time.time()+RUN_LEASE_SECONDS))
                db.commit()
                ok=result.rowcount==1
        except Exception:
            logging.exception('lease renew failed');ok=False
        if not ok:self.lost.set()
        return ok
    def stop(self):
        self._stop.set()
        if self._thread:self._thread.join(timeout=5)

def _has_result(value):
    """中断前已落库的计算结果；有行即可沿用，不必重算。"""
    return (isinstance(value,dict) and isinstance(value.get('rows'),list)
            and isinstance(value.get('columns'),list) and len(value['rows'])>0)

def _save_result(db,run,result):
    """先持久化计算结果；崩溃后接管者可直接沿用，避免重复计算。"""
    rows=db.execute(update(Run).where(
        Run.id==run.id,Run.lease_token==run.lease_token,Run.state=='running',
    ).values(result=result))
    if rows.rowcount!=1:
        db.rollback();return False
    db.commit();return True

def _still_owns(run):
    """用独立事务确认归属，避免长连接快照误判（MySQL REPEATABLE READ）。"""
    with Session() as fresh:
        owns=fresh.scalar(select(Run.id).where(
            Run.id==run.id,Run.lease_token==run.lease_token,Run.state=='running',
        ))
    return owns is not None

def _fail(db,run,message):
    """只有当前持有者能写入失败原因；旧进程的条件更新会被拒绝。"""
    message=(message or '未知错误')[:2000]
    rows=db.execute(update(Run).where(
        Run.id==run.id,Run.lease_token==run.lease_token,Run.state=='running',
    ).values(state='failed',error=message,finished_at=now(),lease_token='',lease_expires_at=0))
    if rows.rowcount!=1:
        db.rollback();return False
    log(db,'worker','run.failed',run.id,message);db.commit();return True

def _publish(db,run,result):
    """文件落盘并校验后，才以租约条件发布完成状态。"""
    if not _still_owns(run):return False
    data=artifacts.csv_bytes(result);digest=artifacts.checksum(data)
    name=artifacts.artifact_name(run.id,run.lease_token)
    artifacts.write_atomic(name,data)
    if artifacts.read_verified(name,digest) is None:
        artifacts.remove(name)
        _fail(db,run,'结果导出文件写入后校验失败')
        return False
    rows=db.execute(update(Run).where(
        Run.id==run.id,Run.lease_token==run.lease_token,Run.state=='running',
    ).values(
        result=result,state='completed',error='',finished_at=now(),
        artifact_name=name,artifact_checksum=digest,lease_token='',lease_expires_at=0,
    ))
    if rows.rowcount!=1:
        db.rollback();artifacts.remove(name);return False
    log(db,'worker','run.completed',run.id,f'{len(result.get("rows") or [])} 行 · {digest[:12]}')
    db.commit()
    artifacts.cleanup_superseded(run.id,name)
    return True

def repair_run(run_id):
    """确保单条已完成任务的成品可用；返回 'valid'/'repaired'/False。

    只处理两类可安全恢复的情况：旧记录尚未登记成品，或成品文件丢失。
    若已登记校验和但数据库结果与其不一致，说明结果本身损坏，
    此时宁可明确报“恢复失败”，也不导出一个与结果不符的文件。
    """
    with Session() as db:
        run=db.get(Run,run_id)
        if not run or run.state!='completed' or not _has_result(run.result):return False
        name=run.artifact_name;expected=run.artifact_checksum or ''
        if artifacts.read_verified(name,expected) is not None and expected:
            return 'valid'
        data=artifacts.csv_bytes(run.result);digest=artifacts.checksum(data)
        if expected and digest!=expected:
            log(db,'worker','run.artifact_mismatch',run.id,expected[:12]);db.commit();return False
        previous=run.artifact_name or ''
        name=name or artifacts.artifact_name(run.id,'recovered')
        artifacts.write_atomic(name,data)
        rows=db.execute(update(Run).where(
            Run.id==run.id,Run.state=='completed',Run.artifact_name==previous,
        ).values(artifact_name=name,artifact_checksum=digest))
        if rows.rowcount!=1:
            db.rollback();return False
        log(db,'worker','run.artifact_repaired',run.id,digest[:12]);db.commit()
        artifacts.cleanup_superseded(run.id,name)
        return 'repaired'

_repair_cursor=0

def repair_completed(limit=50):
    """轮转扫描已完成任务，补写丢失或缺失登记的成品；不重算结果。

    使用游标轮转，保证任务数量超过一轮扫描窗口时也不会被永久跳过。
    """
    global _repair_cursor
    with Session() as db:
        ids=list(db.scalars(select(Run.id).where(Run.state=='completed')
                            .order_by(Run.id).limit(limit).offset(_repair_cursor)))
        if not ids and _repair_cursor:
            _repair_cursor=0
            ids=list(db.scalars(select(Run.id).where(Run.state=='completed')
                                .order_by(Run.id).limit(limit)))
    _repair_cursor+=len(ids)
    repaired=0
    for run_id in ids:
        try:
            if repair_run(run_id)=='repaired':repaired+=1
        except Exception:logging.exception('artifact repair failed for run %s',run_id)
    return repaired

def execute(run_id,owner=WORKER_ID,on_beat=None):
    """执行一次任务，返回 completed/failed/lost/None（未领取到）。"""
    with Session() as db:
        run=claim(db,run_id,owner)
        if not run:return None
        token=run.lease_token;attempt=run.attempt or 1
        saved=run.result if _has_result(run.result) else None
        if attempt>RUN_MAX_ATTEMPTS:
            # 结果已经算好时再发布一次（发布成本低）；仍失败则明确失败，避免无限重试。
            if saved is not None and _publish(db,run,saved):return 'completed'
            reason=(f'执行中断 {attempt-1} 次，超过自动恢复上限；'
                    +('结果已计算但成品文件未能发布。' if saved else '请检查 worker 或输入后重新执行'))
            return 'failed' if _fail(db,run,reason) else 'lost'
        lease=Lease(run.id,token,on_beat);lease.start()
        try:
            if attempt>1:
                log(db,'worker','run.taken_over',run.id,f'第 {attempt} 次执行，接管租约过期任务');db.commit()
            if saved is None:
                saved=calculate(run.snapshot['records'],run.parameters)
                json.dumps(saved,allow_nan=False)
                if not _save_result(db,run,saved):return 'lost'
            else:
                json.dumps(saved,allow_nan=False)
            if lease.lost.is_set():return 'lost'
            return 'completed' if _publish(db,run,saved) else 'lost'
        except Exception as error:
            logging.exception('analysis failed')
            if lease.lost.is_set():return 'lost'
            return 'failed' if _fail(db,run,str(error)) else 'lost'
        finally:
            lease.stop()
