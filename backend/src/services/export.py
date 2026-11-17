import csv,io
from fastapi import HTTPException
from fastapi.responses import Response
from ..models.note import Note
from . import artifacts
from .runs import get_run, artifact_ready, valid_result

def csv_response(result,filename):
    buf=io.StringIO();fields=result['columns'];w=csv.DictWriter(buf,fieldnames=fields);w.writeheader()
    for r in result['rows']:w.writerow({k:r.get(k,'') for k in fields})
    return Response('﻿'+buf.getvalue(),media_type='text/csv; charset=utf-8',headers={'Content-Disposition':'attachment; filename="'+filename+'"'})

def run_csv(db,id):
    """直接下发已发布并校验过的成品字节，保证导出内容与完成态一致。"""
    r=get_run(db,id)
    if r.state!='completed':raise HTTPException(409,'结果尚未完成')
    if not valid_result(r.result):raise HTTPException(409,'结果数据不完整，请重新执行')
    data=artifacts.read_verified(r.artifact_name,r.artifact_checksum or '')
    if data is None:
        # 成品丢失或旧研究未登记成品：按已保存结果补写，不重算。
        # 已登记校验和却与结果不符时 repair_run 会拒绝，不会导出不一致的文件。
        from .jobs import repair_run
        if repair_run(r.id):db.expire_all();r=get_run(db,id)
        data=artifacts.read_verified(r.artifact_name,r.artifact_checksum or '')
    if data is None:raise HTTPException(409,'成品文件正在恢复，请稍后重试')
    return Response(data,media_type='text/csv; charset=utf-8',headers={'Content-Disposition':'attachment; filename="run-'+str(id)+'.csv"'})

def result_ready(r):
    return r.state=='completed' and artifact_ready(r)

def note_csv(db,id):
    n=db.get(Note,id)
    if not n:raise HTTPException(404,'未找到说明')
    if n.state!='approved':raise HTTPException(403,'仅批准后的业务说明可导出')
    return csv_response(n.frozen_result,'approved-'+str(id)+'.csv')
