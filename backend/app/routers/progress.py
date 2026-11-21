"""进度凭证接口：离线采集进度文件导入、确认续做与可核对覆盖查询。

资源路由已经覆盖了按模块的列表/登记/动作；这里补的是文件导入、
从未确认凭证续做、覆盖读数与待回补清单这些领域动作。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile
from pydantic import BaseModel

from app.services.backfill_job import BackfillJobService
from app.services.progress_receipt import ProgressReceiptService

router = APIRouter(prefix="/api/progress_receipt", tags=["采集进度凭证"])
backfill_router = APIRouter(prefix="/api/backfill_job", tags=["历史回补任务"])


class ProgressRowPayload(BaseModel):
    endpoint: str | None = None
    date: str | None = None
    location: str | None = None
    write_result: str | None = None


class ProgressImportPayload(BaseModel):
    endpoint_id: int | str | None = None
    filename: str | None = None
    acquired_date: str | None = None
    source: str = "离线凭证"
    caliber: str = "日期"
    auto_confirm: bool = True
    rows: list[dict[str, Any]] | None = None


class OnlineReceiptPayload(BaseModel):
    endpoint_id: int | str
    receipts: list[dict[str, Any]] | None = None
    start_date: str | None = None
    end_date: str | None = None


@router.post("/import")
async def import_receipt(
    file: UploadFile | None = File(default=None),
    endpoint_id: str | None = Form(default=None),
    acquired_date: str | None = Form(default=None),
    source: str = Form(default="离线凭证"),
    caliber: str = Form(default="日期"),
    auto_confirm: bool = Form(default=True),
) -> dict[str, Any]:
    """上传离线采集进度文件，登记原文件并校验成可核对的覆盖数据。

    同一端点下重复上传同一个文件时只确认一次凭证，不重复落湖。
    """
    service = ProgressReceiptService()
    if file is None:
        raise HTTPException(status_code=400, detail="缺少进度文件")
    content = await file.read()
    return service.import_receipt(
        endpoint_id=endpoint_id,
        content=content,
        filename=file.filename or "progress.json",
        acquired_date=acquired_date,
        source=source or "离线凭证",
        caliber=caliber or "日期",
        auto_confirm=auto_confirm,
    )


@router.post("/imports")
def import_receipt_rows(payload: ProgressImportPayload) -> dict[str, Any]:
    """按行数据导入进度凭证（没有原文件时的入口）。"""
    if payload.rows is None:
        raise HTTPException(status_code=400, detail="缺少进度数据行")
    service = ProgressReceiptService()
    return service.import_receipt(
        endpoint_id=payload.endpoint_id,
        rows=payload.rows,
        filename=payload.filename or "progress.json",
        acquired_date=payload.acquired_date,
        source=payload.source,
        caliber=payload.caliber,
        auto_confirm=payload.auto_confirm,
    )


@router.post("/{receipt_id}/confirm")
def confirm_receipt(receipt_id: int, payload: ProgressImportPayload | None = None) -> dict[str, Any]:
    """校验失败后从未确认的凭证续做校验。"""
    service = ProgressReceiptService()
    kwargs: dict[str, Any] = {}
    if payload is not None and payload.rows is not None:
        kwargs["rows"] = payload.rows
    return service.confirm_receipt(receipt_id, **kwargs)


@router.get("/{receipt_id}/coverage")
def receipt_coverage(receipt_id: int) -> dict[str, Any]:
    """列出某份凭证已登记的全部覆盖记录。"""
    service = ProgressReceiptService()
    receipt = service.get_receipt(receipt_id)
    if receipt is None:
        raise HTTPException(status_code=404, detail=f"进度凭证 {receipt_id} 不存在")
    rows = [
        row for row in service.get_coverage(receipt.get("关联端点"))
        if str(row.get("凭证ID")) == str(receipt_id)
    ]
    return {
        "ok": True,
        "receipt_id": receipt_id,
        "receipt_no": receipt.get("凭证编号"),
        "endpoint_id": receipt.get("关联端点"),
        "status": receipt.get("status"),
        "coverage": rows,
    }


@router.get("/coverage")
def list_coverage(
    endpoint_id: str | None = Query(default=None),
    start_date: str | None = Query(default=None),
    end_date: str | None = Query(default=None),
    confirmed_only: bool = Query(default=False),
) -> dict[str, Any]:
    """读取可核对的采集进度覆盖。"""
    service = ProgressReceiptService()
    rows = service.get_coverage(endpoint_id, start_date, end_date, confirmed_only=confirmed_only)
    return {
        "ok": True,
        "endpoint_id": endpoint_id,
        "total": len(rows),
        "coverage": rows,
        "sources": service.coverage_sources(endpoint_id, start_date, end_date),
    }


@router.get("/pending")
def list_pending(
    endpoint_id: str = Query(...),
    start_date: str = Query(...),
    end_date: str = Query(...),
) -> dict[str, Any]:
    """列出区间内没有已确认覆盖的日期。"""
    service = ProgressReceiptService()
    pending = service.pending_dates(endpoint_id, start_date, end_date)
    return {
        "ok": True,
        "endpoint_id": endpoint_id,
        "date_range": {"start": start_date, "end": end_date},
        "pending_dates": pending,
        "pending_count": len(pending),
    }


@router.get("/next-window")
def next_window(
    endpoint_id: str = Query(...),
    start_date: str = Query(...),
    end_date: str = Query(...),
) -> dict[str, Any]:
    """给出下一次采集应从哪一天续做。"""
    return ProgressReceiptService().next_collection_window(endpoint_id, start_date, end_date)


@router.post("/online")
def record_online(payload: OnlineReceiptPayload) -> dict[str, Any]:
    """登记线上采集回执，与离线凭证形成两份可核对的进度依据。"""
    return ProgressReceiptService().ingest_online_receipts(
        payload.endpoint_id,
        payload.receipts,
        start_date=payload.start_date,
        end_date=payload.end_date,
    )


@backfill_router.get("/{job_id}/pending")
def backfill_pending(job_id: int) -> dict[str, Any]:
    """读取某次回补的待回补清单。"""
    job = BackfillJobService().get_entry(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"回补任务 {job_id} 不存在")
    rows = BackfillJobService().list_pending(job_id)
    return {
        "ok": True,
        "job_id": job_id,
        "total": len(rows),
        "pending": rows,
        "gap_dates": [row["日期"] for row in rows],
        "date_range": {"start": job.get("起始日期"), "end": job.get("截止日期")},
    }
