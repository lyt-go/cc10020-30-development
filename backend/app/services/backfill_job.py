"""历史回补任务领域服务：历史区间回补、覆盖策略与失败区间重采。

管理历史时间序列数据的批量回补任务，支持日期区间分割、
并行采集、覆盖策略（追加/覆写）和失败区间追溯。
跨模块关联：progress_receipt（离线进度凭证与可核对覆盖）、
incre_cursor（起始水位）、batch_ingest（批量拉取）、data_lake_sink（回补落湖）。

回补前先读取进度凭证留下的覆盖记录：已有已写入凭证支撑的日期直接跳过，
不再向端点重复请求；只把真正缺口的日期写入待回补清单再逐日拉取。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from app.persistence import json_store
from app.services.base import BaseModuleService

PENDING_MODULE = "backfill_pending"


class BackfillJobService(BaseModuleService):
    MODULE = "backfill_job"
    ENTITY = "回补任务"
    OPERATION_TERM = "调度"
    LIST_FIELDS = ["任务编号", "关联端点", "起始日期", "截止日期", "并行度", "覆盖策略", "完成水位", "失败区间"]
    REQUIRED_FIELDS = ["任务编号", "关联端点", "起始日期"]
    STATUS_ORDER = ["草稿", "运行中", "已完成", "已取消"]
    ACTION_RULES = {"启动回补": "运行中", "确认完成": "已完成", "取消回补": "已取消"}
    NEGATIVE_ACTIONS = []

    def __init__(self) -> None:
        super().__init__()
        self._failed_intervals: dict[int, list[dict[str, Any]]] = {}
        self._progress: dict[int, float] = {}

    def execute_backfill(self, job_id: int) -> dict[str, Any]:
        """执行历史数据回补任务。

        完整流程：
        1. 解析起始/截止日期区间
        2. 按日分割为子区间
        3. 逐日模拟拉取数据
        4. 检查覆盖策略（追加/覆写）
        5. 拉取数据写入数据湖
        6. 记录失败区间

        Returns:
            { ok: bool, days_processed: int, records_backfilled: int, failed_intervals: list }
        """
        job = json_store.get_by_id(self.MODULE, job_id)
        if job is None:
            return {"ok": False, "message": f"回补任务 {job_id} 不存在", "days_processed": 0}

        if job.get("status") != "运行中":
            return {"ok": False, "message": f"任务状态{job['status']}，无法执行", "days_processed": 0}

        start_date = self._parse_date(str(job.get("起始日期", "")))
        end_date = self._parse_date(str(job.get("截止日期", "")))
        endpoint_id = int(job.get("关联端点", "0") or "0")
        parallel_degree = int(job.get("并行度", "3") or "3")
        overwrite_policy = str(job.get("覆盖策略", "追加不覆写"))

        if start_date is None or end_date is None:
            return {"ok": False, "message": "日期格式无效", "days_processed": 0}

        # 获取增量水位作为起始参考
        try:
            from app.services.incre_cursor import IncreCursorService
            cursor_svc = IncreCursorService()
            starting_position = cursor_svc.get_position(endpoint_id).get("position", 0)
        except Exception:
            starting_position = 0

        days_list = []
        current = start_date
        while current <= end_date:
            days_list.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)

        # 读取可核对的采集进度：已确认覆盖直接跳过，只回补缺口。
        coverage_rows = self._load_coverage(endpoint_id, job.get("起始日期", ""), job.get("截止日期", ""))
        confirmed_dates = {
            str(row.get("日期")) for row in coverage_rows if row.get("已确认") and row.get("日期")
        }
        skipped_days = [day for day in days_list if day in confirmed_dates]
        gap_days = [day for day in days_list if day not in confirmed_dates]

        pending_entries = self._replace_pending(job_id, endpoint_id, gap_days, coverage_rows)

        days_processed = 0
        records_backfilled = 0
        failed_intervals = []
        requested_dates: list[str] = []

        import random
        for day_str in gap_days:
            try:
                # 模拟每日拉取数据
                requested_dates.append(day_str)
                daily_data = self._mock_fetch_day(endpoint_id, day_str, random.randint(10, 50))
                records_backfilled += len(daily_data)

                # 落湖（跨模块：data_lake_sink）
                try:
                    from app.services.data_lake_sink import DataLakeSinkService
                    sink_svc = DataLakeSinkService()
                    sink_svc.write_batch(daily_data, event_type="backfill", source_channel_id=endpoint_id)
                except Exception:
                    failed_intervals.append({"date": day_str, "reason": "落湖失败"})

                days_processed += 1
                self._progress[job_id] = days_processed / max(len(days_list), 1) * 100
                self._mark_pending(job_id, day_str, "已回补")

            except Exception as e:
                failed_intervals.append({"date": day_str, "reason": str(e)})
                self._mark_pending(job_id, day_str, "回补失败")

        # 更新任务状态
        self._failed_intervals[job_id] = failed_intervals

        is_complete = days_processed == len(gap_days)
        json_store.update(self.MODULE, job_id, {
            "完成水位": f"{days_processed}/{len(days_list)}",
            "失败区间": str(failed_intervals) if failed_intervals else "无",
            "待回补缺口": "、".join(gap_days) if gap_days else "无",
            "已确认跳过": "、".join(skipped_days) if skipped_days else "无",
            "status": "已完成" if is_complete else "运行中",
            "pending": not is_complete,
            "abnormal": len(failed_intervals) > 0,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        })

        return {
            "ok": True,
            "message": (
                f"回补完成: 缺口 {days_processed}/{len(gap_days)} 天，"
                f"{records_backfilled} 条记录，已确认跳过 {len(skipped_days)} 天"
            ),
            "days_processed": days_processed,
            "total_days": len(days_list),
            "requested_dates": requested_dates,
            "skipped_dates": skipped_days,
            "gap_dates": gap_days,
            "records_backfilled": records_backfilled,
            "failed_intervals": failed_intervals,
            "pending_entries": len(pending_entries),
            "coverage": self._coverage_summary(endpoint_id, coverage_rows, days_list, confirmed_dates),
            "progress_pct": round(days_processed / max(len(days_list), 1) * 100, 2),
        }

    # ── 进度凭证 / 覆盖数据的跨模块读取 ─────────────────────────────
    def _load_coverage(self, endpoint_id: int | str, start_date: str, end_date: str) -> list[dict[str, Any]]:
        """读取进度凭证落下的可核对覆盖记录。"""
        try:
            from app.services.progress_receipt import ProgressReceiptService
            svc = ProgressReceiptService()
            return svc.get_coverage(endpoint_id, start_date, end_date)
        except Exception:
            return []

    def _coverage_summary(
        self,
        endpoint_id: int | str,
        coverage_rows: list[dict[str, Any]],
        days_list: list[str],
        confirmed_dates: set[str],
    ) -> dict[str, Any]:
        sources: dict[str, dict[str, Any]] = {}
        for row in coverage_rows:
            day = str(row.get("日期"))
            item = sources.setdefault(day, {"date": day, "confirmed": False, "sources": []})
            if row.get("已确认"):
                item["confirmed"] = True
            item["sources"].append({
                "receipt_id": row.get("凭证ID"),
                "receipt_no": row.get("凭证编号"),
                "coverage_id": row.get("id"),
                "write_result": row.get("写入结果"),
                "acquired_date": row.get("取得日期"),
                "lake_ref": row.get("lake_ref"),
            })
        return {
            "endpoint_id": str(endpoint_id),
            "date_range": {"start": days_list[0], "end": days_list[-1]} if days_list else {},
            "confirmed_dates": sorted(confirmed_dates),
            "source_refs": [sources[day] for day in sorted(sources)],
        }

    def _replace_pending(
        self,
        job_id: int,
        endpoint_id: int | str,
        gap_days: list[str],
        coverage_rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """把缺口写进待回补清单；同一任务重建，不留下过期条目。"""
        for row in list(json_store.get_all(PENDING_MODULE)):
            if int(row.get("任务ID", 0) or 0) == job_id:
                json_store.delete(PENDING_MODULE, int(row["id"]))

        source_by_date: dict[str, list[dict[str, Any]]] = {}
        for row in coverage_rows:
            source_by_date.setdefault(str(row.get("日期")), []).append(row)

        entries = []
        for day in gap_days:
            refs = [
                {
                    "receipt_id": row.get("凭证ID"),
                    "receipt_no": row.get("凭证编号"),
                    "coverage_id": row.get("id"),
                    "write_result": row.get("写入结果"),
                    "acquired_date": row.get("取得日期"),
                }
                for row in source_by_date.get(day, [])
            ]
            entries.append(json_store.insert(PENDING_MODULE, {
                "任务ID": job_id,
                "关联端点": str(endpoint_id),
                "日期": day,
                "状态": "待回补",
                "来源引用": refs,
                "来源数量": len(refs),
                "created_at": datetime.now(timezone.utc).isoformat(),
            }))
        return entries

    def _mark_pending(self, job_id: int, day: str, status: str) -> None:
        for row in json_store.get_all(PENDING_MODULE):
            if int(row.get("任务ID", 0) or 0) == job_id and str(row.get("日期")) == day:
                json_store.update(PENDING_MODULE, int(row["id"]), {"状态": status})

    def list_pending(self, job_id: int | None = None) -> list[dict[str, Any]]:
        """查询待回补清单。"""
        rows = json_store.get_all(PENDING_MODULE)
        if job_id is not None:
            rows = [row for row in rows if int(row.get("任务ID", 0) or 0) == job_id]
        return sorted(rows, key=lambda row: (str(row.get("日期", "")), int(row.get("id", 0))))

    def _parse_date(self, date_str: str) -> datetime | None:
        """解析日期字符串（支持多种格式）。"""
        formats = ["%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"]
        if "T" in date_str:
            date_str = date_str.split("T")[0]
        for fmt in formats:
            try:
                return datetime.strptime(date_str.strip(), fmt)
            except ValueError:
                continue
        return None

    def _mock_fetch_day(self, endpoint_id: int, day: str, count: int) -> list[dict[str, Any]]:
        """模拟拉取一天的数据。"""
        import random
        return [
            {
                "id": i + 1,
                "date": day,
                "endpoint_id": endpoint_id,
                "value": random.randint(1, 1000),
                "category": random.choice(["A", "B", "C"]),
                "timestamp": f"{day}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:00",
            }
            for i in range(count)
        ]

    def retry_failed_intervals(self, job_id: int) -> dict[str, Any]:
        """重试失败区间。"""
        failed = self._failed_intervals.get(job_id, [])
        if not failed:
            return {"ok": True, "message": "无失败区间需要重试", "retried": 0}

        job = json_store.get_by_id(self.MODULE, job_id)
        if job is None:
            return {"ok": False, "message": "任务不存在"}

        endpoint_id = int(job.get("关联端点", "0") or "0")
        retried = 0
        still_failed = []

        for interval in failed:
            try:
                daily_data = self._mock_fetch_day(endpoint_id, interval["date"], 20)
                from app.services.data_lake_sink import DataLakeSinkService
                sink_svc = DataLakeSinkService()
                sink_svc.write_batch(daily_data, event_type="backfill_retry", source_channel_id=endpoint_id)
                retried += 1
            except Exception:
                still_failed.append(interval)

        self._failed_intervals[job_id] = still_failed

        return {
            "ok": True,
            "message": f"重试完成: 成功 {retried}/{len(failed)}",
            "retried": retried,
            "still_failed": len(still_failed),
        }

    def get_backfill_progress(self, job_id: int) -> dict[str, Any]:
        """获取回补任务进度。"""
        job = json_store.get_by_id(self.MODULE, job_id)
        if job is None:
            return {"ok": False, "message": "任务不存在"}

        return {
            "ok": True,
            "job_id": job_id,
            "job_no": job.get("任务编号"),
            "status": job.get("status"),
            "progress_pct": self._progress.get(job_id, 0),
            "failed_intervals": len(self._failed_intervals.get(job_id, [])),
            "endpoint": job.get("关联端点"),
            "date_range": f"{job.get('起始日期')} ~ {job.get('截止日期')}",
        }

    def domain_snapshot(self) -> dict[str, Any]:
        """输出历史回补任务的数据质量快照。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "历史回补任务数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "active_jobs": sum(1 for r in rows if r.get("status") == "运行中"),
            "completed_jobs": sum(1 for r in rows if r.get("status") == "已完成"),
            "operation": self.OPERATION_TERM,
        }