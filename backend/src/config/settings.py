import json, os
from pathlib import Path
CONFIG=json.loads((Path(__file__).parent / 'domain.json').read_text())
DATABASE_URL=os.getenv('DATABASE_URL','sqlite:///./dev.db')
REDIS_URL=os.getenv('REDIS_URL','redis://localhost:6379/0')
JWT_SECRET=os.getenv('JWT_SECRET','local-only-change-before-deployment')
ARTIFACT_DIR=Path(os.getenv('ARTIFACT_DIR','/artifacts'))
# 任务租约：worker 每隔 RUN_HEARTBEAT_SECONDS 续租；断电后最多 RUN_LEASE_SECONDS 秒被接管。
RUN_LEASE_SECONDS=max(5.0,float(os.getenv('RUN_LEASE_SECONDS','60')))
RUN_HEARTBEAT_SECONDS=max(1.0,min(RUN_LEASE_SECONDS/3,float(os.getenv('RUN_HEARTBEAT_SECONDS','10'))))
# 队列不可用时，worker 仍会按数据库中的 queued/过期 running 记录补算。
QUEUE_NAME=os.getenv('RUN_QUEUE_NAME','analysis:queue')
# 断电或反复崩溃时自动接管的上限，超过后明确失败并保留原因，避免无限重试。
RUN_MAX_ATTEMPTS=max(1,int(os.getenv('RUN_MAX_ATTEMPTS','5')))
