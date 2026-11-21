"""事件归档清理领域服务：历史事件归档、压缩与过期清理。

管理历史事件数据的归档策略，支持按条件归档、
数据压缩和过期数据的清理调度。
跨模块关联：data_lake_sink（归档来源）、integration_monitor（归档统计）。
"""
from __future__ import annotations

import os
import shutil
import zlib
from datetime import datetime, timedelta, timezone
from typing import Any

from app.persistence import json_store
from app.services.base import BaseModuleService


class EventArchiveService(BaseModuleService):
    MODULE = "event_archive"
    ENTITY = "归档策略"
    OPERATION_TERM = "归档"
    LIST_FIELDS = ["策略编号", "事件类型", "归档条件", "归档路径", "压缩格式", "保留天数", "清理方式", "恢复超时"]
    REQUIRED_FIELDS = ["策略编号", "事件类型", "归档条件"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用策略": "已停用"}
    NEGATIVE_ACTIONS = ["停用策略"]

    def __init__(self) -> None:
        super().__init__()
        self._archive_base = os.path.join(str(json_store.base_dir), "archive")
        os.makedirs(self._archive_base, exist_ok=True)
        self._archive_history: list[dict[str, Any]] = []

    def execute_archive(self, policy_id: int) -> dict[str, Any]:
        """执行事件归档任务。

        流程：
        1. 加载归档策略
        2. 扫描数据湖中符合条件的事件分区
        3. 对分区数据进行压缩归档
        4. 清理热存储中的已归档数据
        """
        policy = json_store.get_by_id(self.MODULE, policy_id)
        if policy is None:
            return {"ok": False, "message": f"归档策略 {policy_id} 不存在", "archived": 0}

        if policy.get("status") != "已生效":
            return {"ok": False, "message": f"归档策略未生效({policy['status']})", "archived": 0}

        event_type = str(policy.get("事件类型", "ALL"))
        archive_condition = str(policy.get("归档条件", "days>30"))
        compress_format = str(policy.get("压缩格式", "zlib"))
        retention_days = int(policy.get("保留天数", "90") or "90")

        # 扫描数据湖
        lake_base = os.path.join(str(json_store.base_dir), "lake")
        events_dir = os.path.join(lake_base, "events")

        total_archived = 0
        total_size = 0

        now = datetime.now(timezone.utc)
        archive_date = now.strftime("%Y%m%d")
        archive_batch = os.path.join(self._archive_base, f"arch_{archive_date}")

        if os.path.exists(events_dir):
            for root, dirs, files in os.walk(events_dir):
                for f in sorted(files):
                    if not f.endswith(".json"):
                        continue

                    filepath = os.path.join(root, f)
                    mtime = datetime.fromtimestamp(os.path.getmtime(filepath), tz=timezone.utc)
                    days_old = (now - mtime).days

                    # 检查归档条件
                    skip = False
                    if "days" in archive_condition:
                        try:
                            threshold = int(archive_condition.split(">")[-1].strip())
                            if days_old < threshold:
                                skip = True
                        except ValueError:
                            pass

                    if skip:
                        continue

                    # 执行归档
                    try:
                        os.makedirs(archive_batch, exist_ok=True)
                        rel_path = os.path.relpath(root, events_dir).replace("/", "_")
                        archive_name = f"{rel_path}_{f}"
                        archive_path = os.path.join(archive_batch, archive_name)

                        if compress_format == "zlib":
                            with open(filepath, "rb") as src:
                                compressed = zlib.compress(src.read())
                            with open(archive_path + ".zlib", "wb") as dst:
                                dst.write(compressed)
                        else:
                            shutil.copy2(filepath, archive_path)

                        total_archived += 1
                        total_size += os.path.getsize(filepath)
                    except OSError:
                        continue

        # 清理过期归档
        cleaned = self._clean_expired_archives(retention_days)

        self._archive_history.append({
            "policy_id": policy_id,
            "event_type": event_type,
            "archived": total_archived,
            "size_bytes": total_size,
            "cleaned": cleaned,
            "timestamp": now.isoformat(),
        })

        return {
            "ok": True,
            "message": f"归档完成: {total_archived} 文件, {round(total_size/1024, 2)} KB",
            "archived": total_archived,
            "total_size_bytes": total_size,
            "cleaned_old": cleaned,
        }

    def _clean_expired_archives(self, retention_days: int) -> int:
        """清理超过保留期的归档文件。"""
        cleaned = 0
        now = datetime.now(timezone.utc)
        if not os.path.exists(self._archive_base):
            return 0

        for root, dirs, files in os.walk(self._archive_base):
            for f in files:
                filepath = os.path.join(root, f)
                try:
                    mtime = datetime.fromtimestamp(os.path.getmtime(filepath), tz=timezone.utc)
                    if (now - mtime).days > retention_days:
                        os.remove(filepath)
                        cleaned += 1
                except OSError:
                    continue
        return cleaned

    def get_archive_stats(self) -> dict[str, Any]:
        """获取归档存储统计。"""
        total_files = 0
        total_size = 0
        for root, dirs, files in os.walk(self._archive_base):
            for f in files:
                total_files += 1
                try:
                    total_size += os.path.getsize(os.path.join(root, f))
                except OSError:
                    pass

        return {
            "base_dir": self._archive_base,
            "total_files": total_files,
            "total_size_bytes": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "archive_batches": len([d for d in os.listdir(self._archive_base)
                                    if os.path.isdir(os.path.join(self._archive_base, d))]) if os.path.exists(self._archive_base) else 0,
            "history": self._archive_history[-10:],
        }

    def restore_from_archive(self, archive_file: str) -> dict[str, Any]:
        """从归档恢复数据。"""
        for root, dirs, files in os.walk(self._archive_base):
            for f in files:
                if f == archive_file or f.startswith(archive_file):
                    filepath = os.path.join(root, f)
                    try:
                        if f.endswith(".zlib"):
                            with open(filepath, "rb") as src:
                                decompressed = zlib.decompress(src.read())
                            return {"ok": True, "data": decompressed.decode()}
                        else:
                            with open(filepath, "r") as src:
                                return {"ok": True, "data": src.read()}
                    except (OSError, zlib.error):
                        return {"ok": False, "message": "归档文件损坏或不存在"}
        return {"ok": False, "message": "归档文件未找到"}

    def domain_snapshot(self) -> dict[str, Any]:
        """输出事件归档清理的数据质量快照。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        archive_stats = self.get_archive_stats()
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "事件归档清理数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "archive_files": archive_stats.get("total_files", 0),
            "archive_size_mb": archive_stats.get("total_size_mb", 0),
            "operation": self.OPERATION_TERM,
        }