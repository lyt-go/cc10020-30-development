"""增量水位管理领域服务：增量同步水位标记、断点恢复与区间管理。

管理每个端点的增量拉取进度，记录上次成功位置、
当前采集位置和增量类型，支持断点恢复和回溯。

离线采集回收的日期与位置数据经 progress_receipt 校验后形成可核对的
覆盖记录；本服务读取这些记录，给出“已确认到哪一天、还缺哪些天”，
作为下一次增量采集和断点恢复的依据。
跨模块关联：batch_ingest（批量断点）、backfill_job（回补起始位置）、
progress_receipt（离线进度凭证与可核对覆盖）。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.persistence import json_store
from app.services.base import BaseModuleService


class IncreCursorService(BaseModuleService):
    MODULE = "incre_cursor"
    ENTITY = "水位标记"
    OPERATION_TERM = "调度"
    LIST_FIELDS = ["标记编号", "关联端点", "增量字段", "上次位置", "当前位置", "增量类型", "覆写策略", "更新间隔"]
    REQUIRED_FIELDS = ["标记编号", "关联端点", "增量字段"]
    STATUS_ORDER = ["草稿", "待校验", "采集中", "已复位"]
    ACTION_RULES = {"提交校验": "待校验", "标记采集中": "采集中", "复位游标": "已复位"}
    NEGATIVE_ACTIONS = []

    def __init__(self) -> None:
        super().__init__()
        self._positions: dict[int, int] = {}
        self._position_history: list[dict[str, Any]] = []

    def update_position(self, endpoint_id: int, new_position: int,
                        data_count: int = 0) -> dict[str, Any]:
        """更新端点的增量水位位置。

        记录位置变更历史，供断点恢复使用。
        """
        cursors = json_store.get_all(self.MODULE)
        matching = [
            c for c in cursors
            if str(c.get("关联端点", "")) == str(endpoint_id)
        ]

        old_position = self._positions.get(endpoint_id, 0)
        self._positions[endpoint_id] = new_position

        now = datetime.now(timezone.utc)

        if matching:
            cursor = matching[0]
            json_store.update(self.MODULE, int(cursor["id"]), {
                "上次位置": str(old_position),
                "当前位置": str(new_position),
                "status": "采集中",
                "updated_at": now.isoformat(),
            })
        else:
            # 自动创建水位标记
            max_id = max((int(c.get("id", 0)) for c in cursors), default=0)
            cursor_entry = {
                "id": max_id + 1,
                "标记编号": f"CURSOR-{endpoint_id:04d}",
                "关联端点": str(endpoint_id),
                "增量字段": "sequence_id",
                "上次位置": str(old_position),
                "当前位置": str(new_position),
                "增量类型": "追加",
                "覆写策略": "追加不覆写",
                "更新间隔": "60秒",
                "status": "采集中",
                "pending": False,
                "abnormal": False,
                "created_at": now.isoformat(),
            }
            json_store.insert(self.MODULE, cursor_entry)

        self._position_history.append({
            "endpoint_id": endpoint_id,
            "old_position": old_position,
            "new_position": new_position,
            "data_count": data_count,
            "timestamp": now.isoformat(),
        })

        return {
            "ok": True,
            "endpoint_id": endpoint_id,
            "old_position": old_position,
            "new_position": new_position,
            "delta": new_position - old_position,
        }

    def get_position(self, endpoint_id: int) -> dict[str, Any]:
        """获取端点的当前水位位置。"""
        pos = self._positions.get(endpoint_id, 0)

        cursors = json_store.get_all(self.MODULE)
        matching = [c for c in cursors if str(c.get("关联端点", "")) == str(endpoint_id)]

        if matching:
            cursor = matching[0]
            pos = int(cursor.get("当前位置", "0") or "0")

        return {
            "endpoint_id": endpoint_id,
            "position": pos,
            "cursor_count": len(matching),
        }

    def reset_position(self, endpoint_id: int, reset_to: int = 0) -> dict[str, Any]:
        """复位端点的水位到指定位置（支持回溯重采）。"""
        old_pos = self._positions.get(endpoint_id, 0)
        self._positions[endpoint_id] = reset_to

        cursors = json_store.get_all(self.MODULE)
        matching = [c for c in cursors if str(c.get("关联端点", "")) == str(endpoint_id)]

        if matching:
            cursor_id = int(matching[0]["id"])
            json_store.update(self.MODULE, cursor_id, {
                "上次位置": str(old_pos),
                "当前位置": str(reset_to),
                "status": "已复位",
                "updated_at": datetime.now(timezone.utc).isoformat(),
            })

        return {
            "ok": True,
            "endpoint_id": endpoint_id,
            "old_position": old_pos,
            "reset_to": reset_to,
            "message": f"水位已从 {old_pos} 复位至 {reset_to}",
        }

    def get_position_history(self, endpoint_id: int, limit: int = 50) -> list[dict[str, Any]]:
        """获取端点的水位变更历史。"""
        return [
            h for h in self._position_history
            if h["endpoint_id"] == endpoint_id
        ][-limit:]

    def get_all_positions(self) -> dict[int, int]:
        """获取所有端点的当前水位（供监控模块批量查询）。"""
        return dict(self._positions)

    def get_coverage_progress(self, endpoint_id: int | str) -> dict[str, Any]:
        """读取可核对的采集进度，给出已确认日期与缺口。

        水位（位置）和日期覆盖是两件事：位置已经推进到 12 日，
        不代表 9 日已经有效写入。这里以日期覆盖为准。
        """
        from app.services.progress_receipt import ProgressReceiptService

        receipt_svc = ProgressReceiptService()
        coverage = receipt_svc.get_coverage(endpoint_id)
        confirmed = sorted({str(row.get("日期")) for row in coverage if row.get("已确认")})
        unconfirmed = sorted({
            str(row.get("日期")) for row in coverage if not row.get("已确认")
        } - set(confirmed))
        position = self.get_position(endpoint_id)
        return {
            "endpoint_id": str(endpoint_id),
            "position": position.get("position", 0),
            "confirmed_dates": confirmed,
            "unconfirmed_dates": unconfirmed,
            "confirmed_until": confirmed[-1] if confirmed else "",
            "earliest_gap": unconfirmed[0] if unconfirmed else "",
            "sources": receipt_svc.coverage_sources(endpoint_id),
        }

    def next_watermark(self, endpoint_id: int | str, start_date: str, end_date: str) -> dict[str, Any]:
        """下一次增量采集应从哪一天续做（含未确认日期）。"""
        from app.services.progress_receipt import ProgressReceiptService

        return ProgressReceiptService().next_collection_window(endpoint_id, start_date, end_date)

    def domain_snapshot(self) -> dict[str, Any]:
        """输出增量水位管理的数据质量快照。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "增量水位管理数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "active_cursors": sum(1 for r in rows if r.get("status") == "采集中"),
            "total_positions": len(self._positions),
            "operation": self.OPERATION_TERM,
        }

    def get_coverage_dates(self, endpoint_id: int | str) -> list[str]:
        """已确认写入的日期清单，供监控与回补共用。"""
        from app.services.progress_receipt import ProgressReceiptService

        return ProgressReceiptService().confirmed_dates(endpoint_id)