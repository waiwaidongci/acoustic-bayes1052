"""向后兼容的轻量迁移。

`Base.metadata.create_all` 只建新表，不会给已存在的 `classifier_runs` 补列。
演示环境和已有卷升级时，这里按列名补齐新增列，旧研究数据保持原样可读。
"""
from sqlalchemy import inspect, text
from .settings import CONFIG

# 列名 -> 各数据库都可接受的 DDL 片段
RUN_COLUMNS={
    'attempt':'INTEGER NOT NULL DEFAULT 0',
    'lease_token':"VARCHAR(64) NOT NULL DEFAULT ''",
    'lease_owner':"VARCHAR(160) NOT NULL DEFAULT ''",
    'lease_expires_at':'DOUBLE NOT NULL DEFAULT 0',
    'started_at':"VARCHAR(40) NOT NULL DEFAULT ''",
    'finished_at':"VARCHAR(40) NOT NULL DEFAULT ''",
    'artifact_name':"VARCHAR(200) NOT NULL DEFAULT ''",
    'artifact_checksum':"VARCHAR(64) NOT NULL DEFAULT ''",
}

def ensure_schema(engine):
    """补齐新增列；并发启动时（后端与 worker 同时初始化）忽略先到者已加的列。"""
    from sqlalchemy.exc import DBAPIError
    table=CONFIG['run_table']
    inspector=inspect(engine)
    if table not in inspector.get_table_names():
        raise RuntimeError('任务表尚未创建：'+table)
    quote=engine.dialect.identifier_preparer.quote
    for _ in range(2):
        existing={c['name'] for c in inspect(engine).get_columns(table)}
        missing=[(name,ddl) for name,ddl in RUN_COLUMNS.items() if name not in existing]
        if not missing:return
        try:
            with engine.begin() as conn:
                for name,ddl in missing:
                    conn.execute(text(f'ALTER TABLE {quote(table)} ADD COLUMN {quote(name)} {ddl}'))
        except DBAPIError:
            # 另一个进程刚加过同名列：重新检查后重试一次。
            continue
    remaining={c['name'] for c in inspect(engine).get_columns(table)}
    missing=[name for name in RUN_COLUMNS if name not in remaining]
    if missing:raise RuntimeError('任务表缺少列且无法自动补齐：'+', '.join(missing))
