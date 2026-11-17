"""独立 worker：数据库补算 + Redis 唤醒。

Redis 只是唤醒提示，数据库中的 queued 与租约过期的 running 才是待办来源。
因此队列停机、重启后，worker 仍会把积压和断电前的任务补算到完成或明确失败。
"""
import logging,time
from redis import Redis
from .config.database import Base,Session,engine
from .config.settings import REDIS_URL,ARTIFACT_DIR,QUEUE_NAME
from .config.schema import ensure_schema
# 导入全部模型，确保 worker 单独启动时也能建表。
from .models import source,record,user,note,audit
from .services import jobs
queue=Redis.from_url(REDIS_URL,socket_connect_timeout=2,socket_timeout=2)
ARTIFACT_DIR.mkdir(parents=True,exist_ok=True)

def beat():
    try:(ARTIFACT_DIR/'heartbeat').write_text(str(time.time()))
    except OSError:logging.exception('heartbeat write failed')

def run_one(id):
    """执行单个任务；任何异常都不应终止 worker 主循环。"""
    try:jobs.execute(id,on_beat=beat)
    except Exception:logging.exception('run %s execution failed unexpectedly',id)

def scan():
    """领取所有排队中或租约过期的任务；原子领取保证多 worker 只有一个赢家。"""
    try:
        with Session() as db:ids=jobs.claimable_ids(db)
    except Exception:
        logging.exception('task scan failed');return False
    for id in ids:run_one(id)
    return bool(ids)

_queue_warned=False

def wait_for_queue():
    """等待 Redis 唤醒；返回任务 id 或 None。

    队列不可用时短暂休眠并返回，让主循环回到数据库补算，而不是空转。
    """
    global _queue_warned
    try:value=queue.blpop(QUEUE_NAME,timeout=2)
    except Exception:
        if not _queue_warned:
            logging.warning('queue unavailable; periodic database backfill continues')
            _queue_warned=True
        time.sleep(2);return None
    if _queue_warned:
        logging.warning('queue reachable again');_queue_warned=False
    return int(value[1]) if value else None

def main():
    Base.metadata.create_all(engine)
    ensure_schema(engine)
    last_repair=0.0
    while True:
        beat()
        worked=scan()
        if time.time()-last_repair>=15:
            last_repair=time.time()
            try:jobs.repair_completed()
            except Exception:logging.exception('artifact repair failed')
        if worked:continue
        run_id=wait_for_queue()
        if run_id is not None:run_one(run_id)

if __name__=='__main__':main()
