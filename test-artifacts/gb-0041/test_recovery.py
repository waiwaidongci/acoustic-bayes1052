"""任务断电恢复、接管与幂等发布的回归测试。

不依赖真实 Redis：队列只是唤醒提示，数据库补算是唯一可靠路径。
"""
import time
import pytest

from src.config.database import Session
from src.config.settings import ARTIFACT_DIR
from src.models import record
from src.models.run import Run
from src.services import jobs, artifacts

from hooks import make_run

def get(rid):
    with Session() as db:return db.get(Run,rid)

def test_power_loss_running_is_taken_over_and_completed(source_id):
    """执行中进程断电（租约过期）后，新 worker 自动接管到完成。"""
    rid=make_run(source_id,state='running',attempt=1,lease=time.time()-1)
    assert jobs.execute(rid,owner='worker-b')=='completed'
    r=get(rid)
    assert r.state=='completed' and r.attempt==2 and r.error==''
    assert r.artifact_name and artifacts.read_verified(r.artifact_name,r.artifact_checksum)
    assert r.result['rows']

def test_claim_is_exclusive_under_concurrency(source_id):
    """同一排队任务被两处同时领取，只有一个赢家。"""
    rid=make_run(source_id)
    with Session() as d1, Session() as d2:
        first=jobs.claim(d1,rid,owner='w1')
        second=jobs.claim(d2,rid,owner='w2')
    assert first is not None and second is None
    assert get(rid).attempt==1

def test_stale_worker_cannot_publish_after_takeover(source_id, monkeypatch):
    """超时接管后，旧进程晚到也不能覆盖成品或状态。"""
    rid=make_run(source_id,state='running',attempt=1,lease=time.time()-1)
    # 让旧进程持有一个过期令牌，模拟它醒来后仍想发布
    with Session() as db:
        stale=db.get(Run,rid);stale.lease_token='stale-token';db.commit()
        stale_token=stale.lease_token
    taken=jobs.execute(rid,owner='worker-b')
    assert taken=='completed'
    completed=get(rid)
    published_file=completed.artifact_name
    # 旧进程再用自己的令牌发布/失败
    with Session() as db:
        run=db.get(Run,rid);run.lease_token=stale_token
        assert jobs._publish(db,run,{'rows':[{'reference':'OLD'}],'columns':['reference']}) is False
        assert jobs._fail(db,run,'旧进程的失败原因') is False
    after=get(rid)
    assert after.state=='completed' and after.error==''
    assert after.artifact_name==published_file
    assert artifacts.read_verified(after.artifact_name,after.artifact_checksum)
    for p in ARTIFACT_DIR.glob(f'run-{rid}-*.csv'):
        assert p.name==published_file, '旧进程不得留下额外成品'

def test_old_worker_cannot_overwrite_failure_reason(source_id, monkeypatch):
    """接管后的失败原因只能由当前持有者写，旧进程不能覆盖。"""
    rid=make_run(source_id,state='running',attempt=1,lease=time.time()-1)
    with Session() as db:
        stale=db.get(Run,rid);stale.lease_token='stale';db.commit()
        # 脱离会话的旧进程视图，仍带着过期令牌
        db.expunge(stale)
    with Session() as db:
        assert jobs.claim(db,rid,owner='worker-b') is not None
        latest=db.get(Run,rid)
        latest_id=latest.id
    from types import SimpleNamespace
    old_view=SimpleNamespace(id=latest_id,lease_token='stale')
    with Session() as db:
        assert jobs._fail(db,old_view,'旧进程的失败原因') is False
    current=get(rid)
    assert current.state=='running' and current.attempt==2
    with Session() as db:
        fresh=db.get(Run,rid)
        assert jobs._fail(db,fresh,'真正的失败原因') is True
    assert get(rid).error=='真正的失败原因'

def test_result_is_not_recomputed_after_crash_before_publish(source_id, monkeypatch):
    """计算已落库、发布前崩溃：接管者沿用结果，不再调用算法。"""
    from src.services import jobs as jobs_module
    with Session() as db:
        recs=[{'id':r.id,'source_id':r.source_id,'reference':r.reference,'values':r.values}
              for r in db.query(record.Record).filter_by(source_id=source_id).order_by(record.Record.id)]
        good=jobs_module.calculate(recs,{'folds':3,'smoothing':1e-09})
    rid=make_run(source_id,state='running',attempt=1,lease=time.time()-1,result=good)
    calls=[]
    monkeypatch.setattr(jobs_module,'calculate',lambda *a,**k:calls.append(1) or {})
    assert jobs.execute(rid,owner='worker-b')=='completed'
    assert calls==[], '已有结果不应重算'
    assert get(rid).state=='completed'

def test_artifact_is_repaired_when_publish_crashed_midway(source_id):
    """完成态但成品文件丢失：自动补写，且导出内容与发布校验一致。"""
    rid=make_run(source_id)
    assert jobs.execute(rid)=='completed'
    r=get(rid)
    (ARTIFACT_DIR/r.artifact_name).unlink()
    assert jobs.repair_completed()==1
    repaired=get(rid)
    assert artifacts.read_verified(repaired.artifact_name,repaired.artifact_checksum)
    body=artifacts.read_verified(repaired.artifact_name,repaired.artifact_checksum).decode('utf-8')
    header=body.lstrip('﻿').splitlines()[0]
    assert header=='reference,actual,predicted,posterior'

def test_queue_outage_still_accepts_and_backfills(source_id, monkeypatch):
    """队列停机时仍能受理任务；恢复后由数据库补算。"""
    from src.services import runs as runs_service
    class Down:
        def rpush(self,*a,**k):raise ConnectionError('redis down')
    monkeypatch.setattr(runs_service,'queue',Down())
    class Input:
        def __init__(self,source_id):
            self.source_id=source_id;self.name='队列停机研究';self.parameters={'folds':3,'smoothing':1e-09}
    with Session() as db:
        data=runs_service.create_run(db,'tester',Input(source_id))
    assert data['state']=='queued' and data['phase']=='waiting'
    rid=data['id']
    with Session() as db:
        assert jobs.claimable_ids(db)==[rid]
    assert jobs.execute(rid)=='completed'
    assert get(rid).state=='completed'

def test_export_requires_verified_artifact(source_id):
    """结果与导出文件齐备且一致才显示完成；文件丢失时按已存结果补写后才可导出。"""
    from src.services import runs as runs_service
    from src.services.export import run_csv
    rid=make_run(source_id)
    assert jobs.execute(rid)=='completed'
    with Session() as db:
        assert runs_service.detail(runs_service.get_run(db,rid))['artifact_ready'] is True
        run_csv(db,rid)
    r=get(rid)
    (ARTIFACT_DIR/r.artifact_name).unlink()
    with Session() as db:
        detail=runs_service.detail(runs_service.get_run(db,rid))
        assert detail['phase']=='recovering' and detail['artifact_ready'] is False
        # 导出时按已保存结果补写成品，内容来自 result，不重新计算。
        response=run_csv(db,rid)
        lines=response.body.decode('utf-8-sig').strip().splitlines()
        assert lines[0]=='reference,actual,predicted,posterior'
        assert len(lines)-1==len(r.result['rows'])
    healed=get(rid)
    assert artifacts.read_verified(healed.artifact_name,healed.artifact_checksum)
    with Session() as db:
        assert runs_service.detail(runs_service.get_run(db,rid))['artifact_ready'] is True


def test_completed_without_result_cannot_export(source_id):
    """状态完成但结果被损坏、与成品校验和不一致时，不得显示完成或允许导出。"""
    from src.services import runs as runs_service
    from src.services.export import run_csv
    rid=make_run(source_id)
    assert jobs.execute(rid)=='completed'
    with Session() as db:
        r=db.get(Run,rid);r.result={'rows':[],'columns':[]};db.commit()
    with Session() as db:
        detail=runs_service.detail(runs_service.get_run(db,rid))
        assert detail['phase']=='recovering' and detail['artifact_ready'] is False
        with pytest.raises(Exception):run_csv(db,rid)
    assert get(rid).state=='completed', '不因成品问题改写任务状态'

def test_legacy_completed_row_without_artifact_metadata_is_readable(source_id):
    """旧研究照常读取：没有成品元数据的完成记录仍显示完成并可恢复成品。"""
    rid=make_run(source_id)
    assert jobs.execute(rid)=='completed'
    with Session() as db:
        r=db.get(Run,rid);r.artifact_name='';r.artifact_checksum='';db.commit()
        from src.services import runs as runs_service
        detail=runs_service.detail(r)
        assert detail['state']=='completed' and detail['result'] is not None
        assert detail['phase']=='completed' and detail['artifact_ready'] is True
    assert jobs.repair_completed()==1
    r=get(rid)
    assert r.artifact_name and artifacts.read_verified(r.artifact_name,r.artifact_checksum)

def test_repeated_execute_does_not_duplicate(source_id):
    """重复唤醒/重复执行只有一次发布，不会重复生成结果。"""
    rid=make_run(source_id)
    assert jobs.execute(rid)=='completed'
    files_before=sorted(p.name for p in ARTIFACT_DIR.glob(f'run-{rid}-*.csv'))
    assert jobs.execute(rid) is None
    files_after=sorted(p.name for p in ARTIFACT_DIR.glob(f'run-{rid}-*.csv'))
    assert files_before==files_after and len(files_after)==1
    assert get(rid).attempt==1

def test_attempt_cap_fails_clearly(source_id):
    """反复中断超过上限时明确失败并保留原因，而不是无限重试。"""
    rid=make_run(source_id,state='running',attempt=5,lease=time.time()-1)
    assert jobs.execute(rid)=='failed'
    r=get(rid)
    assert r.state=='failed' and '超过自动恢复上限' in r.error
