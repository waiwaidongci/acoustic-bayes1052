"""导出成品文件的读写与校验。

成品文件按“任务 + 租约令牌”命名，因此旧进程晚到时只能写自己的文件，
无法覆盖当前持有者已经发布的 `run-<id>-<token>.csv`。写入采用
临时文件 + fsync + os.replace，保证读者永远看不到半截文件。
"""
import csv, hashlib, io, os
from ..config.settings import ARTIFACT_DIR

BOM='﻿'

def csv_bytes(result):
    buf=io.StringIO();fields=result['columns'];w=csv.DictWriter(buf,fieldnames=fields);w.writeheader()
    for r in result['rows']:w.writerow({k:r.get(k,'') for k in fields})
    return (BOM+buf.getvalue()).encode('utf-8')

def checksum(data):
    return hashlib.sha256(data).hexdigest()

def artifact_name(run_id,token):
    return f'run-{int(run_id)}-{token}.csv'

def path(name):
    return ARTIFACT_DIR/name

def write_atomic(name,data):
    ARTIFACT_DIR.mkdir(parents=True,exist_ok=True)
    final=ARTIFACT_DIR/name;tmp=ARTIFACT_DIR/('.'+name+'.tmp')
    with open(tmp,'wb') as fh:
        fh.write(data);fh.flush();os.fsync(fh.fileno())
    os.replace(tmp,final)
    return final

def read_verified(name,expected):
    """读取文件并校验 SHA256；不存在或内容不符时返回 None。"""
    if not name:return None
    p=ARTIFACT_DIR/name
    try:
        if not p.is_file():return None
        data=p.read_bytes()
    except OSError:return None
    return data if checksum(data)==expected else None

def remove(name):
    if not name:return
    try:(ARTIFACT_DIR/name).unlink()
    except OSError:pass

def cleanup_superseded(run_id,keep):
    """清理同一任务的旧尝试产物；只保留已发布的那个文件。"""
    for p in ARTIFACT_DIR.glob(f'run-{int(run_id)}-*.csv'):
        if p.name==keep:continue
        try:p.unlink()
        except OSError:pass
    for p in ARTIFACT_DIR.glob(f'.run-{int(run_id)}-*.csv.tmp'):
        try:p.unlink()
        except OSError:pass
