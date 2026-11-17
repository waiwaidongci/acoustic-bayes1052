# gb-0041 测试与恢复验证

本目录不混入源码，覆盖声检研究的断电恢复、队列停机补算、并发接管与发布一致性。

## 运行

```bash
cd test-artifacts/gb-0041
python -m pip install -r ../../backend/requirements.txt pytest httpx
python -m pytest . -q
```

测试使用临时 SQLite 与临时 `ARTIFACT_DIR`，Redis 指向不可达端口，因此全部用例在**无 Redis** 条件下运行，正好验证数据库补算路径。

## 覆盖

| 文件 | 场景 |
|---|---|
| `test_recovery.py` | 执行中断电后接管完成；并发领取只有一个赢家；旧进程晚到不能发布结果/失败原因/成品；已落库结果不重算；发布中途崩溃补写成品；队列停机受理与补算；结果与导出内容一致；旧研究可读；重复执行不重复生成；超过接管上限明确失败 |
| `test_worker.py` | 真实 `python -m src.worker` 子进程在 Redis 不可达时补算；模拟 SIGKILL 后的接管；两线程争抢同一任务只发布一份成品 |
| `test_api.py` | HTTP 全流程：创建→等待→补算→完成→导出；健康检查在队列停机时仍 200；失败任务显示原因且禁止导出 |

## 手工冒烟

```bash
cd ../../backend
DATABASE_URL=sqlite:////tmp/e2e/db.sqlite ARTIFACT_DIR=/tmp/e2e/art \
  REDIS_URL=redis://127.0.0.1:6399/0 python -m scripts.seed_demo
DATABASE_URL=sqlite:////tmp/e2e/db.sqlite ARTIFACT_DIR=/tmp/e2e/art \
  REDIS_URL=redis://127.0.0.1:6399/0 RUN_LEASE_SECONDS=20 RUN_HEARTBEAT_SECONDS=5 \
  python -m src.worker
```

种子任务会在日志出现 `queue unavailable; falling back to periodic database backfill` 后仍自动完成，`classifier_runs` 中 `state=completed`、`artifact_name`/`artifact_checksum` 非空，`/tmp/e2e/art` 下只有一份 `run-1-<token>.csv` 与 `heartbeat`。
