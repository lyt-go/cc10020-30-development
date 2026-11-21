"""采集进度凭证与历史回补链路测试。

场景：线上采集已走到 12 日；同源同口径的离线凭证证明 8 日已有效写入，
9 日仍没有确认结果，10-12 日线上回执完整，13 日也可采。
"""
from __future__ import annotations

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.persistence import JsonStore
from app.services import progress_receipt as pr_mod
from app.services.progress_receipt import ProgressReceiptService
from app.services.backfill_job import BackfillJobService


@pytest.fixture()
def svc(tmp_path, monkeypatch):
    store = JsonStore(base_dir=str(tmp_path))
    monkeypatch.setattr(pr_mod, "json_store", store)
    import app.services.backfill_job as bj_mod
    monkeypatch.setattr(bj_mod, "json_store", store)
    return store, ProgressReceiptService(), BackfillJobService()


OFFLINE_CSV = """日期,位置,写入结果
2026-11-08,offset-20261108-0001,已写入
2026-11-09,offset-20261109-0002,失败
"""


def _seed_online(svc, endpoint="7"):
    return svc.ingest_online_receipts(endpoint, [
        {"date": "2026-11-10", "record_no": "CALL-10", "write_result": "已写入"},
        {"date": "2026-11-11", "record_no": "CALL-11", "write_result": "已写入"},
        {"date": "2026-11-12", "record_no": "CALL-12", "write_result": "已写入"},
    ])


def test_offline_receipt_confirms_only_written_days(svc):
    _, receipt_svc, _ = svc
    result = receipt_svc.import_receipt(
        endpoint_id="7",
        content=OFFLINE_CSV,
        filename="offline-1108.csv",
        acquired_date="2026-11-09",
    )

    assert result["confirmed_dates"] == ["2026-11-08"]
    assert result["unconfirmed_dates"] == ["2026-11-09"]
    assert result["status"] == "部分确认"

    coverage = receipt_svc.get_coverage("7")
    assert {row["日期"] for row in coverage} == {"2026-11-08", "2026-11-09"}
    day8 = next(row for row in coverage if row["日期"] == "2026-11-08")
    assert day8["关联端点"] == "7"
    assert day8["取得日期"] == "2026-11-09"
    assert day8["写入结果"] == "已写入"
    assert day8["已确认"] is True


def test_duplicate_upload_confirms_once_and_does_not_relake(svc):
    store, receipt_svc, _ = svc
    first = receipt_svc.import_receipt(
        endpoint_id="7", content=OFFLINE_CSV, filename="offline-1108.csv")
    lake_file = os.path.join(str(store.base_dir), "lake", "progress_receipt", "7", "2026-11-08.json")
    before = open(lake_file, "rb").read()

    second = receipt_svc.import_receipt(
        endpoint_id="7", content=OFFLINE_CSV, filename="offline-1108.csv")

    assert second["duplicate"] is True
    assert second["receipt_id"] == first["receipt_id"]
    assert len(receipt_svc.list_receipts("7")) == 1
    assert len(receipt_svc.get_coverage("7")) == 2
    assert open(lake_file, "rb").read() == before
    day8_sources = [row for row in receipt_svc.get_coverage("7") if row["日期"] == "2026-11-08"]
    assert len(day8_sources) == 1


def test_failed_receipt_can_be_resumed(svc):
    _, receipt_svc, _ = svc
    failed = receipt_svc.import_receipt(
        endpoint_id="7", content="日期,位置,写入结果\nbad-date,x,已写入\n", filename="broken.csv")
    assert failed["status"] == "校验失败"
    receipt_id = failed["receipt_id"]
    assert receipt_svc.get_receipt(receipt_id)["原文件路径"]  # 原文件仍已登记

    resumed = receipt_svc.confirm_receipt(receipt_id, rows=[
        {"date": "2026-11-09", "location": "offset-9", "write_result": "已写入"},
    ])
    assert resumed["ok"] is True
    assert resumed["confirmed_dates"] == ["2026-11-09"]
    assert [row["日期"] for row in receipt_svc.get_coverage("7")] == ["2026-11-09"]


def test_two_valid_sources_for_same_endpoint_and_day(svc):
    _, receipt_svc, _ = svc
    receipt_svc.ingest_online_receipts("7", [
        {"date": "2026-11-08", "record_no": "CALL-08", "write_result": "已写入"},
    ])
    receipt_svc.import_receipt(endpoint_id="7", content=OFFLINE_CSV, filename="offline-1108.csv")

    sources = receipt_svc.coverage_sources("7", "2026-11-08", "2026-11-08")
    assert sources[0]["confirmed"] is True
    assert len(sources[0]["sources"]) == 2
    assert {item["type"] for item in sources[0]["sources"]} == {"离线凭证", "线上回执"}

    lake = json.load(open(os.path.join(
        str(svc[0].base_dir), "lake", "progress_receipt", "7", "2026-11-08.json"), encoding="utf-8"))
    assert lake["source_count"] == 2

    # 复查已有回执：来源记录不重复落湖，仍是一份文件两条来源。
    receipt_svc.ingest_online_receipts("7", [
        {"date": "2026-11-08", "record_no": "CALL-08", "write_result": "已写入"},
    ])
    assert len(receipt_svc.get_coverage("7", "2026-11-08", "2026-11-08")) == 2
    lake_again = json.load(open(os.path.join(
        str(svc[0].base_dir), "lake", "progress_receipt", "7", "2026-11-08.json"), encoding="utf-8"))
    assert lake_again["source_count"] == 2


def test_backfill_requests_only_gaps(svc):
    store, receipt_svc, backfill_svc = svc
    receipt_svc.import_receipt(endpoint_id="7", content=OFFLINE_CSV, filename="offline-1108.csv")
    _seed_online(receipt_svc)

    job = store.insert("backfill_job", {
        "任务编号": "BACK-1108", "关联端点": "7",
        "起始日期": "2026-11-08", "截止日期": "2026-11-12",
        "并行度": "3", "覆盖策略": "追加不覆写",
        "status": "运行中", "pending": False, "abnormal": False,
    })

    result = backfill_svc.execute_backfill(int(job["id"]))

    assert result["requested_dates"] == ["2026-11-09"]
    assert result["skipped_dates"] == ["2026-11-08", "2026-11-10", "2026-11-11", "2026-11-12"]
    assert result["gap_dates"] == ["2026-11-09"]
    assert result["coverage"]["confirmed_dates"] == [
        "2026-11-08", "2026-11-10", "2026-11-11", "2026-11-12"]
    refs = {item["date"]: item["sources"] for item in result["coverage"]["source_refs"]}
    assert refs["2026-11-08"][0]["receipt_no"].startswith("CRED-")
    assert refs["2026-11-10"][0]["receipt_no"] == "CALL-10"

    pending = backfill_svc.list_pending(int(job["id"]))
    assert [row["日期"] for row in pending] == ["2026-11-09"]
    assert pending[0]["状态"] == "已回补"

    updated = store.get_by_id("backfill_job", int(job["id"]))
    assert updated["已确认跳过"] == "2026-11-08、2026-11-10、2026-11-11、2026-11-12"


def test_next_collection_resumes_from_unconfirmed_day(svc):
    _, receipt_svc, _ = svc
    receipt_svc.import_receipt(endpoint_id="7", content=OFFLINE_CSV, filename="offline-1108.csv")
    _seed_online(receipt_svc)

    window = receipt_svc.next_collection_window("7", "2026-11-08", "2026-11-13")
    assert window["resume_from"] == "2026-11-09"
    assert window["pending_dates"] == ["2026-11-09", "2026-11-13"]
    assert window["confirmed_dates"] == ["2026-11-08", "2026-11-10", "2026-11-11", "2026-11-12"]
    assert window["is_complete"] is False


def test_coverage_traces_back_to_original_receipt(svc):
    store, receipt_svc, _ = svc
    result = receipt_svc.import_receipt(
        endpoint_id="7", content=OFFLINE_CSV, filename="offline-1108.csv")
    receipt = receipt_svc.get_receipt(result["receipt_id"])
    assert receipt["原文件名"] == "offline-1108.csv"
    assert os.path.exists(receipt["原文件路径"])
    assert receipt["文件摘要"]

    detail = receipt_svc.get_entry(result["receipt_id"])
    assert {row["日期"] for row in detail["coverage"]} == {"2026-11-08", "2026-11-09"}
    assert all(row["凭证ID"] == result["receipt_id"] for row in detail["coverage"])
