"""真实 worker 进程级验证：无 Redis 时从数据库补算、断电续算、并发争抢。"""
import os, signal, subprocess, sys, time

from src.config.database import Session
from src.config.settings import ARTIFACT_DIR
from src.models.run import Run
from src.services import jobs, artifacts


def test_worker_process_backfills_without_redis(source_id):
    """以子进程启动真实 worker，Redis 端口不通时应自动补算排队任务。"""
    rid=make_run(source_id)
    env=dict(os.environ)
    env['PYTHONPATH']=os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))+'/backend'
    proc=subprocess.Popen([sys.executable,'-m','src.worker'],cwd=env['PYTHONPATH'],env=env,
                          stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    try:
        deadline=time.time()+25
        state=None
        while time.time()<deadline:
            with Session() as db:state=db.get(Run,rid).state
            if state in ('completed','failed'):break
            time.sleep(.4)
    finally:
        proc.send_signal(signal.SIGKILL);proc.wait(timeout=10)
    assert state=='completed', 'Redis 不可用时也应由数据库补算完成'
    r=get(rid)
    assert r.artifact_name and artifacts.read_verified(r.artifact_name,r.artifact_checksum)
    assert (ARTIFACT_DIR/'heartbeat').exists()


def test_worker_killed_mid_run_is_recovered(source_id):
    """模拟执行中进程被 SIGKILL：租约到期后另一个 worker 接管并完成。"""
    rid=make_run(source_id)
    with Session() as db:
        # 模拟一个已断电进程：执行中、租约过期
        r=db.get(Run,rid);r.state='running';r.attempt=1;r.lease_token='dead-worker'
        r.lease_expires_at=time.time()-1;r.lease_owner='dead';db.commit()
    assert jobs.execute(rid,owner='recovered-worker')=='completed'
    final=get(rid)
    assert final.state=='completed' and final.attempt==2
    assert final.lease_token=='' and final.lease_expires_at==0
    assert artifacts.read_verified(final.artifact_name,final.artifact_checksum)


def test_two_workers_contend_and_one_publishes(source_id):
    """两个 worker 同时争抢同一任务：只有一个发布，成品只有一份。"""
    import threading
    rid=make_run(source_id)
    results=[]
    def worker(name):
        results.append(jobs.execute(rid,owner=name))
    threads=[threading.Thread(target=worker,args=(f'w{i}',)) for i in range(2)]
    for t in threads:t.start()
    for t in threads:t.join(timeout=30)
    assert sorted(x for x in results if x=='completed')==['completed']
    assert get(rid).state=='completed'
    files=sorted(p.name for p in ARTIFACT_DIR.glob(f'run-{rid}-*.csv'))
    assert len(files)==1,files
    assert get(rid).attempt==1


from hooks import make_run


def get(rid):
    with Session() as db:return db.get(Run,rid)
