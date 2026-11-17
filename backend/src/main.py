import os,logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from sqlalchemy import text
from .config.database import Base,engine
from .config.schema import ensure_schema
from .config.settings import CONFIG
# 导入全部模型，确保建表时元数据完整。
from .models import user,source,record,run,note,audit
from .middlewares.errors import error_boundary
from .middlewares.rate_limit import rate_limit
from .routes import auth,sources,runs,notes,audit as audit_routes
from .services.runs import queue
from .services import jobs
@asynccontextmanager
async def lifespan(app):
    Base.metadata.create_all(engine)
    ensure_schema(engine)
    try:jobs.repair_completed()
    except Exception:logging.exception('artifact repair failed; worker will retry')
    if os.getenv('SEED_DEMO','true')=='true':
        try:
            from scripts.seed_demo import seed
            seed()
        except Exception:logging.exception('seed failed; service remains available')
    yield
app=FastAPI(title=CONFIG['title'],lifespan=lifespan)
app.middleware('http')(error_boundary)
app.middleware('http')(rate_limit)
for router in (auth.router,sources.router,runs.router,notes.router,audit_routes.router):app.include_router(router)
@app.get('/health')
def health():
    with engine.connect() as c:c.execute(text('SELECT 1'))
    try:queue.ping();queue_ok=True
    except Exception:queue_ok=False
    # 队列停机不影响业务受理与数据库补算，所以不把整体健康拖红。
    return {'status':'ok','queue':queue_ok}
@app.get('/catalog')
def catalog():return CONFIG
