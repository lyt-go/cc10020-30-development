"""采集进度凭证领域服务：离线采集进度文件的登记、校验与可核对覆盖。

离线采集（爬虫/RPA/导出脚本）产出的日期与位置数据以文件形式回收，
本服务负责：

1. 登记原文件（落盘保存字节、文件名与 SHA-256 摘要）；
2. 解析 CSV / JSON / JSONL 进度文件，校验端点、日期、位置与写入结果；
3. 把校验通过的日期写成可核对的覆盖记录，保留源端点、取得日期和写入结果；
4. 同一个文件重复上传时只确认一次凭证，不重复落湖；
5. 校验失败时保留未确认凭证，后续可以从这份凭证续做。

覆盖记录按“(来源类型, 回执, 端点, 日期)”唯一。离线凭证和线上回执
可以同时作为同一端点的进度依据；每条覆盖记录都回指原回执。
数据湖里每个“(端点, 日期)”只保留一份来源记录，复查回执不会重复落湖。
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from app.persistence import json_store
from app.services.base import BaseModuleService

RECEIPT_MODULE = "progress_receipt"
COVERAGE_MODULE = "incre_coverage"

SOURCE_OFFLINE = "离线凭证"
SOURCE_ONLINE = "线上回执"

# 文件里可能出现的列名，统一归一化成内部字段。
_FIELD_ALIASES: dict[str, str] = {
    "关联端点": "endpoint",
    "端点": "endpoint",
    "端点编号": "endpoint",
    "endpoint": "endpoint",
    "endpoint_id": "endpoint",
    "endpointid": "endpoint",
    "日期": "date",
    "取得日期": "date",
    "采集日期": "date",
    "date": "date",
    "collect_date": "date",
    "acquisition_date": "date",
    "位置": "location",
    "位置数据": "location",
    "采集位置": "location",
    "location": "location",
    "position": "location",
    "offset": "location",
    "写入结果": "write_result",
    "落湖结果": "write_result",
    "写入状态": "write_result",
    "结果": "write_result",
    "write_result": "write_result",
    "result": "write_result",
    "status": "write_result",
    "状态": "write_result",
    "已确认": "confirmed",
    "confirmed": "confirmed",
    "备注": "remark",
    "remark": "remark",
}

_WRITTEN_VALUES = {
    "已写入", "写入成功", "已落湖", "落湖成功", "成功", "已确认", "完成",
    "ok", "written", "success", "succeeded", "done", "true", "yes", "y", "1",
}
_NOT_WRITTEN_VALUES = {
    "未写入", "写入失败", "失败", "未确认", "待确认", "待写入", "异常", "缺失",
    "failed", "fail", "error", "pending", "missing", "false", "no", "n", "0",
}

_DATE_RE = re.compile(r"(\d{4})[-/]?(\d{2})[-/]?(\d{2})")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalise_date(value: Any) -> str | None:
    """把 2026-11-08 / 2026/11/08 / 20261108 / ISO 时间统一成 YYYY-MM-DD。"""
    text = str(value or "").strip()
    if not text:
        return None
    match = _DATE_RE.search(text)
    if not match:
        return None
    year, month, day = match.groups()
    try:
        return datetime(int(year), int(month), int(day)).strftime("%Y-%m-%d")
    except ValueError:
        return None


def _date_list(start: str, end: str) -> list[str]:
    first = normalise_date(start)
    last = normalise_date(end)
    if not first or not last or first > last:
        return []
    days: list[str] = []
    current = datetime.strptime(first, "%Y-%m-%d")
    stop = datetime.strptime(last, "%Y-%m-%d")
    while current <= stop:
        days.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return days


def _is_written(value: Any, confirmed_flag: Any = None) -> bool:
    """判断一行进度是否构成“已写入”的有效覆盖。"""
    if confirmed_flag is not None and str(confirmed_flag).strip():
        flag = str(confirmed_flag).strip().casefold()
        if flag in {"1", "true", "yes", "y", "是", "已确认"}:
            return True
        if flag in {"0", "false", "no", "n", "否", "未确认"}:
            return False
    text = str(value if value is not None else "").strip()
    if not text:
        return False
    lowered = text.casefold()
    if lowered in _WRITTEN_VALUES or text in _WRITTEN_VALUES:
        return True
    if lowered in _NOT_WRITTEN_VALUES or text in _NOT_WRITTEN_VALUES:
        return False
    # 未知但非空的写入结果：按未确认处理，避免把失败当覆盖。
    return False


def _normalise_header(name: str) -> str | None:
    key = str(name or "").strip().casefold().replace(" ", "")
    if key in _FIELD_ALIASES:
        return _FIELD_ALIASES[key]
    for alias, field in _FIELD_ALIASES.items():
        if key.startswith(alias.casefold()):
            return field
    return None


def _normalise_row(raw: dict[str, Any]) -> dict[str, Any]:
    row: dict[str, Any] = {}
    for key, value in raw.items():
        field = _normalise_header(str(key))
        if field:
            row[field] = value
    return row


class ProgressReceiptService(BaseModuleService):
    MODULE = RECEIPT_MODULE
    ENTITY = "进度凭证"
    OPERATION_TERM = "导入"
    LIST_FIELDS = ["凭证编号", "关联端点", "取得日期", "覆盖范围", "已确认天数", "写入结果", "校验状态", "原文件名"]
    REQUIRED_FIELDS = ["关联端点", "原文件名"]
    STATUS_ORDER = ["待校验", "已确认", "部分确认", "校验失败"]
    ACTION_RULES = {"确认凭证": "已确认", "标记失败": "校验失败"}
    NEGATIVE_ACTIONS = ["标记失败"]

    # ── 查询：凭证与覆盖 ─────────────────────────────────────────────
    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = json_store.get_all(self.MODULE)
        for field, expected in (filters or {}).items():
            if field in self.LIST_FIELDS:
                rows = [
                    row for row in rows
                    if str(expected).casefold() in str(row.get(field, "")).casefold()
                ]
        if keyword:
            rows = [
                row for row in rows
                if any(keyword.casefold() in str(row.get(field, "")).casefold()
                       for field in self.LIST_FIELDS)
            ]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """「确认凭证」走完整校验续做，不只是改状态。"""
        if action == "确认凭证":
            result = self.confirm_receipt(entry_id)
            return self.get_entry(entry_id), str(result.get("message", ""))
        return super().run_action(entry_id, action)

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        receipt = json_store.get_by_id(self.MODULE, entry_id)
        if receipt is None:
            return None
        detail = dict(receipt)
        detail["coverage"] = [
            row for row in self.get_coverage(receipt.get("关联端点"))
            if str(row.get("凭证ID")) == str(entry_id)
        ]
        return detail

    def list_receipts(self, endpoint_id: int | str | None = None) -> list[dict[str, Any]]:
        """列出进度凭证，可按端点过滤。"""
        rows = json_store.get_all(self.MODULE)
        if endpoint_id is not None and str(endpoint_id).strip():
            rows = [row for row in rows if str(row.get("关联端点")) == str(endpoint_id)]
        return rows

    def get_receipt(self, receipt_id: int) -> dict[str, Any] | None:
        return json_store.get_by_id(self.MODULE, receipt_id)

    def get_coverage(
        self,
        endpoint_id: int | str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        *,
        confirmed_only: bool = False,
    ) -> list[dict[str, Any]]:
        """读取可核对的覆盖记录（跨模块入口）。"""
        rows = json_store.get_all(COVERAGE_MODULE)
        if endpoint_id is not None and str(endpoint_id).strip():
            rows = [row for row in rows if str(row.get("关联端点")) == str(endpoint_id)]
        start = normalise_date(start_date) if start_date else None
        end = normalise_date(end_date) if end_date else None
        if start:
            rows = [row for row in rows if str(row.get("日期", "")) >= start]
        if end:
            rows = [row for row in rows if str(row.get("日期", "")) <= end]
        if confirmed_only:
            rows = [row for row in rows if row.get("已确认")]
        rows.sort(key=lambda row: (str(row.get("日期", "")), int(row.get("id", 0))))
        return rows

    def confirmed_dates(
        self,
        endpoint_id: int | str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[str]:
        """返回有已写入回执支撑的日期。"""
        rows = self.get_coverage(endpoint_id, start_date, end_date, confirmed_only=True)
        return sorted({str(row["日期"]) for row in rows if row.get("日期")})

    def pending_dates(
        self,
        endpoint_id: int | str,
        start_date: str,
        end_date: str,
    ) -> list[str]:
        """返回区间内没有任何已确认覆盖的日期。"""
        confirmed = set(self.confirmed_dates(endpoint_id, start_date, end_date))
        return [day for day in _date_list(start_date, end_date) if day not in confirmed]

    def coverage_sources(
        self,
        endpoint_id: int | str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[dict[str, Any]]:
        """按日期聚合来源引用，确认与未确认都可回溯原回执。"""
        grouped: dict[str, dict[str, Any]] = {}
        for row in self.get_coverage(endpoint_id, start_date, end_date):
            day = str(row.get("日期", ""))
            item = grouped.setdefault(day, {"date": day, "confirmed": False, "sources": []})
            item["sources"].append(self._source_ref(row))
            if row.get("已确认"):
                item["confirmed"] = True
        return [grouped[day] for day in sorted(grouped)]

    def _source_ref(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "type": row.get("来源类型", SOURCE_OFFLINE),
            "receipt_id": row.get("凭证ID"),
            "receipt_no": row.get("回执编号") or row.get("凭证编号"),
            "coverage_id": row.get("id"),
            "write_result": row.get("写入结果"),
            "acquired_date": row.get("取得日期"),
            "location": row.get("位置"),
            "confirmed": bool(row.get("已确认")),
            "lake_ref": row.get("lake_ref"),
        }

    # ── 导入入口 ─────────────────────────────────────────────────────
    def import_receipt(
        self,
        *,
        endpoint_id: int | str | None = None,
        content: bytes | str | None = None,
        rows: list[Any] | None = None,
        filename: str | None = None,
        acquired_date: str | None = None,
        source: str = SOURCE_OFFLINE,
        caliber: str = "日期",
        auto_confirm: bool = True,
    ) -> dict[str, Any]:
        """导入一份离线采集进度凭证。

        同一个端点、同一个文件（按 SHA-256 判定）重复导入时只确认一次：
        返回已有凭证，不新增覆盖记录，也不重复写数据湖。
        """
        raw_bytes = b""
        if content is not None:
            raw_bytes = content.encode("utf-8") if isinstance(content, str) else bytes(content)
        elif rows is not None:
            raw_bytes = json.dumps(rows, ensure_ascii=False, sort_keys=True).encode("utf-8")
        filename = (filename or "progress.json").strip() or "progress.json"
        digest = hashlib.sha256(raw_bytes).hexdigest() if (raw_bytes or rows is not None) else ""

        endpoint_text = str(endpoint_id).strip() if endpoint_id is not None else ""
        existing = self._find_receipt(endpoint_text, digest) if digest else None
        if existing is not None:
            # 已确认的凭证直接回报；未确认的凭证用同一文件续做校验。
            if existing.get("status") in ("已确认", "部分确认"):
                return self._duplicate_result(existing)
            result = self._validate_and_confirm(existing, auto_confirm=auto_confirm)
            result["duplicate"] = True
            return result

        parsed_rows, errors = self._parse(content=raw_bytes, rows=rows, filename=filename)
        file_path = self._save_original(raw_bytes, digest, filename)
        receipt_no = self._next_receipt_no()
        receipt = json_store.insert(self.MODULE, {
            "凭证编号": receipt_no,
            "关联端点": endpoint_text,
            "取得日期": normalise_date(acquired_date) or datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "覆盖范围": "—",
            "已确认天数": "0",
            "写入结果": "待校验",
            "校验状态": "待校验",
            "status": "待校验",
            "pending": True,
            "abnormal": False,
            "原文件名": filename,
            "原文件路径": file_path,
            "文件摘要": digest,
            "文件字节数": len(raw_bytes),
            "数据来源": source,
            "数据口径": caliber,
            "行数": len(parsed_rows),
            "raw_rows": parsed_rows,
            "校验错误": errors,
        })
        return self._validate_and_confirm(receipt, auto_confirm=auto_confirm)

    # 兼容常见命名
    import_progress_receipt = import_receipt
    register_receipt = import_receipt
    upload_receipt = import_receipt

    def confirm_receipt(
        self,
        receipt_id: int,
        *,
        rows: list[Any] | None = None,
        content: bytes | str | None = None,
    ) -> dict[str, Any]:
        """校验失败后从已登记的未确认凭证续做。

        可以补交修正后的行数据；补交内容会替换原凭证的待校验行。
        """
        receipt = json_store.get_by_id(self.MODULE, receipt_id)
        if receipt is None:
            return {"ok": False, "message": f"进度凭证 {receipt_id} 不存在"}
        if receipt.get("status") in ("已确认", "部分确认"):
            return self._duplicate_result(receipt)

        if rows is not None or content is not None:
            parsed_rows, errors = self._parse(
                content=(content.encode("utf-8") if isinstance(content, str) else bytes(content or b"")),
                rows=rows,
                filename=str(receipt.get("原文件名") or "progress.json"),
            )
            raw = content.encode("utf-8") if isinstance(content, str) else bytes(content or b"")
            if raw:
                digest = hashlib.sha256(raw).hexdigest()
                receipt = json_store.update(self.MODULE, receipt_id, {
                    "raw_rows": parsed_rows,
                    "行数": len(parsed_rows),
                    "文件摘要": digest,
                    "文件字节数": len(raw),
                    "原文件路径": self._save_original(
                        raw, digest, str(receipt.get("原文件名") or "progress.json")),
                    "校验错误": errors,
                    "updated_at": _now(),
                }) or receipt
            else:
                receipt = json_store.update(self.MODULE, receipt_id, {
                    "raw_rows": parsed_rows, "行数": len(parsed_rows),
                    "校验错误": errors, "updated_at": _now(),
                }) or receipt
        return self._validate_and_confirm(receipt, auto_confirm=True)

    def resume_receipt(self, receipt_id: int, **kwargs: Any) -> dict[str, Any]:
        """confirm_receipt 的别名，表达“从上次未确认处续做”。"""
        return self.confirm_receipt(receipt_id, **kwargs)

    def _duplicate_result(self, receipt: dict[str, Any]) -> dict[str, Any]:
        coverage = [
            row for row in self.get_coverage(receipt.get("关联端点"), confirmed_only=True)
            if str(row.get("凭证ID")) == str(receipt.get("id"))
        ]
        dates = sorted({str(row.get("日期")) for row in coverage})
        return {
            "ok": True,
            "message": f"进度凭证 {receipt.get('凭证编号')} 已确认，本次未重复登记",
            "duplicate": True,
            "confirmed": True,
            "receipt_id": receipt.get("id"),
            "凭证编号": receipt.get("凭证编号"),
            "endpoint_id": receipt.get("关联端点"),
            "status": receipt.get("status"),
            "coverage_dates": dates,
            "confirmed_dates": dates,
            "unconfirmed_dates": [],
            "row_count": receipt.get("行数", 0),
            "file_path": receipt.get("原文件路径"),
            "file_hash": receipt.get("文件摘要"),
            "errors": [],
            "source_refs": [
                {"date": row.get("日期"), "receipt_id": receipt.get("id"),
                 "receipt_no": receipt.get("凭证编号"), "coverage_id": row.get("id")}
                for row in coverage
            ],
        }

    def _find_receipt(self, endpoint_id: str, digest: str) -> dict[str, Any] | None:
        if not endpoint_id or not digest:
            return None
        for row in json_store.get_all(self.MODULE):
            if str(row.get("关联端点")) == endpoint_id and row.get("文件摘要") == digest:
                return row
        return None

    def _next_receipt_no(self) -> str:
        max_no = 0
        for row in json_store.get_all(self.MODULE):
            match = re.search(r"(\d+)$", str(row.get("凭证编号", "")))
            if match:
                max_no = max(max_no, int(match.group(1)))
        return f"CRED-{max_no + 1:04d}"

    def _save_original(self, raw: bytes, digest: str, filename: str) -> str:
        """把上传的原文件登记到存储根目录下，保持可追溯。"""
        folder = os.path.join(str(json_store.base_dir), "progress_receipts")
        os.makedirs(folder, exist_ok=True)
        safe_name = re.sub(r"[^0-9A-Za-z_.\-一-鿿]", "_", filename) or "progress.json"
        path = os.path.join(folder, f"{digest[:16]}_{safe_name}")
        try:
            with open(path, "wb") as handle:
                handle.write(raw)
        except OSError:
            return ""
        return path

    # ── 解析 ─────────────────────────────────────────────────────────
    def _parse(
        self,
        *,
        content: bytes,
        rows: list[Any] | None,
        filename: str,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        errors: list[str] = []
        text = content.decode("utf-8-sig", errors="replace").strip()

        if rows is not None:
            return [self._clean_row(row) for row in rows if isinstance(row, dict)], errors

        if not text:
            return [], ["进度文件为空"]

        suffix = os.path.splitext(filename)[1].casefold()
        if suffix in (".json", "") or text.startswith(("[", "{")):
            try:
                payload = json.loads(text)
                if isinstance(payload, dict):
                    payload = (payload.get("rows") or payload.get("records")
                               or payload.get("data") or [])
                if not isinstance(payload, list):
                    return [], ["进度文件 JSON 结构不是数组"]
                return [self._clean_row(row) for row in payload if isinstance(row, dict)], errors
            except json.JSONDecodeError:
                if suffix == ".json":
                    return [], ["进度文件不是合法 JSON"]
        if suffix in (".jsonl", ".ndjson"):
            parsed = []
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    errors.append(f"无法解析的 JSONL 行：{line[:40]}")
                    continue
                if isinstance(item, dict):
                    parsed.append(self._clean_row(item))
            return parsed, errors

        try:
            dialect = csv.Sniffer().sniff(text.splitlines()[0], delimiters=",\t;")
        except csv.Error:
            dialect = csv.excel
        raw_rows = list(csv.reader(io.StringIO(text), dialect))
        if not raw_rows:
            return [], ["进度文件为空"]
        header = raw_rows[0]
        normalised_header = [_normalise_header(name) for name in header]
        if all(field is None for field in normalised_header):
            normalised_header = ["date", "location", "write_result"][: len(header)]
            body = raw_rows
        else:
            body = raw_rows[1:]
        parsed = []
        for values in body:
            if not any(str(value).strip() for value in values):
                continue
            record = {
                normalised_header[index]: values[index]
                for index in range(min(len(normalised_header), len(values)))
                if normalised_header[index]
            }
            parsed.append(self._clean_row(record))
        return parsed, errors

    def _clean_row(self, raw: dict[str, Any]) -> dict[str, Any]:
        row = _normalise_row(raw)
        return {
            "endpoint": str(row.get("endpoint", "") or "").strip(),
            "date": normalise_date(row.get("date")),
            "raw_date": str(row.get("date", "") or "").strip(),
            "location": str(row.get("location", "") or "").strip(),
            "write_result": str(row.get("write_result", "") or "").strip(),
            "confirmed": row.get("confirmed"),
            "remark": str(row.get("remark", "") or "").strip(),
        }

    # ── 校验与确认 ───────────────────────────────────────────────────
    def _validate_and_confirm(
        self,
        receipt: dict[str, Any],
        *,
        auto_confirm: bool = True,
    ) -> dict[str, Any]:
        endpoint_id = str(receipt.get("关联端点", "") or "").strip()
        raw_rows = receipt.get("raw_rows") or []
        errors: list[str] = []
        valid_rows: list[dict[str, Any]] = []

        if not endpoint_id:
            errors.append("缺少关联端点")
        if not raw_rows:
            errors.append("进度文件中没有可校验的数据行")

        for index, row in enumerate(raw_rows, start=1):
            row_endpoint = str(row.get("endpoint", "") or "").strip()
            if row_endpoint and row_endpoint != endpoint_id:
                errors.append(f"第 {index} 行端点 {row_endpoint} 与凭证端点 {endpoint_id} 不一致")
                continue
            if not row.get("date"):
                errors.append(f"第 {index} 行日期无效：{row.get('raw_date') or '空'}")
                continue
            valid_rows.append(row)

        if not auto_confirm:
            json_store.update(self.MODULE, int(receipt["id"]), {
                "校验状态": "待校验",
                "status": "待校验",
                "pending": True,
                "校验错误": errors,
                "updated_at": _now(),
            })
            return {
                "ok": not errors,
                "message": "进度文件已登记，等待确认" if not errors else "进度文件校验失败",
                "duplicate": False,
                "confirmed": False,
                "receipt_id": receipt.get("id"),
                "凭证编号": receipt.get("凭证编号"),
                "endpoint_id": endpoint_id,
                "status": "待校验",
                "errors": errors,
                "row_count": len(raw_rows),
            }

        if errors and not valid_rows:
            json_store.update(self.MODULE, int(receipt["id"]), {
                "校验状态": "校验失败",
                "status": "校验失败",
                "pending": True,
                "abnormal": True,
                "校验错误": errors,
                "写入结果": "校验失败",
                "updated_at": _now(),
            })
            return {
                "ok": False,
                "message": f"进度凭证 {receipt.get('凭证编号')} 校验失败：" + "；".join(errors[:3]),
                "duplicate": False,
                "confirmed": False,
                "receipt_id": receipt.get("id"),
                "凭证编号": receipt.get("凭证编号"),
                "endpoint_id": endpoint_id,
                "status": "校验失败",
                "coverage_dates": [],
                "confirmed_dates": [],
                "unconfirmed_dates": [],
                "row_count": len(raw_rows),
                "errors": errors,
                "file_path": receipt.get("原文件路径"),
                "file_hash": receipt.get("文件摘要"),
                "source_refs": [],
            }

        coverage_dates: list[str] = []
        unconfirmed_dates: list[str] = []

        seen_dates: set[str] = set()
        for row in valid_rows:
            day = str(row["date"])
            if day in seen_dates:
                continue
            seen_dates.add(day)
            confirmed = _is_written(row.get("write_result"), row.get("confirmed"))
            self._upsert_coverage(receipt, row, confirmed)
            if confirmed:
                coverage_dates.append(day)
            else:
                unconfirmed_dates.append(day)

        status = "已确认"
        if not coverage_dates:
            status = "校验失败"
        elif errors or unconfirmed_dates:
            status = "部分确认"

        coverage_start = min(seen_dates) if seen_dates else ""
        coverage_end = max(seen_dates) if seen_dates else ""
        coverage_range = f"{coverage_start} ~ {coverage_end}" if seen_dates else "—"
        write_results = sorted({row.get("write_result") for row in valid_rows if row.get("write_result")})

        json_store.update(self.MODULE, int(receipt["id"]), {
            "覆盖范围": coverage_range,
            "已确认天数": str(len(coverage_dates)),
            "写入结果": "、".join(write_results) if write_results else "未确认",
            "校验状态": status,
            "status": status,
            "pending": status != "已确认",
            "abnormal": status == "校验失败",
            "校验错误": errors,
            "confirmed_at": _now() if coverage_dates else None,
            "updated_at": _now(),
        })
        receipt = json_store.get_by_id(self.MODULE, int(receipt["id"])) or receipt

        return {
            "ok": bool(coverage_dates) and status != "校验失败",
            "message": (
                f"进度凭证 {receipt.get('凭证编号')} 已确认 {len(coverage_dates)} 天"
                + (f"，{len(unconfirmed_dates)} 天未确认" if unconfirmed_dates else "")
                if coverage_dates else
                f"进度凭证 {receipt.get('凭证编号')} 校验失败："
                + "；".join(errors[:3] or ["无已写入的日期"])
            ),
            "duplicate": False,
            "confirmed": status != "校验失败",
            "receipt_id": receipt.get("id"),
            "凭证编号": receipt.get("凭证编号"),
            "endpoint_id": endpoint_id,
            "acquired_date": receipt.get("取得日期"),
            "status": status,
            "coverage_range": coverage_range,
            "coverage_dates": sorted(coverage_dates),
            "confirmed_dates": sorted(coverage_dates),
            "unconfirmed_dates": sorted(unconfirmed_dates),
            "pending_dates": self.pending_dates(endpoint_id, coverage_start, coverage_end)
            if seen_dates else [],
            "row_count": len(raw_rows),
            "errors": errors,
            "file_path": receipt.get("原文件路径"),
            "file_hash": receipt.get("文件摘要"),
            "source_refs": [
                {"date": day, "receipt_id": receipt.get("id"),
                 "receipt_no": receipt.get("凭证编号")}
                for day in sorted(seen_dates)
            ],
        }

    def _upsert_coverage(
        self,
        receipt: dict[str, Any],
        row: dict[str, Any],
        confirmed: bool,
    ) -> dict[str, Any]:
        """按 (来源, 回执, 端点, 日期) 写覆盖记录；复查同一回执不新增。"""
        endpoint_id = str(receipt.get("关联端点"))
        day = str(row["date"])
        receipt_no = str(receipt.get("凭证编号") or "")
        existing = next(
            (item for item in json_store.get_all(COVERAGE_MODULE)
             if str(item.get("关联端点")) == endpoint_id
             and str(item.get("日期")) == day
             and str(item.get("来源类型", SOURCE_OFFLINE)) == SOURCE_OFFLINE
             and str(item.get("凭证ID")) == str(receipt.get("id"))),
            None,
        )
        values = {
            "关联端点": endpoint_id,
            "日期": day,
            "位置": row.get("location", ""),
            "写入结果": row.get("write_result", "") or ("已写入" if confirmed else "未确认"),
            "已确认": confirmed,
            "取得日期": receipt.get("取得日期"),
            "来源类型": SOURCE_OFFLINE,
            "凭证ID": receipt.get("id"),
            "凭证编号": receipt_no,
            "回执编号": receipt_no,
            "原文件名": receipt.get("原文件名"),
            "文件摘要": receipt.get("文件摘要"),
            "数据来源": receipt.get("数据来源", SOURCE_OFFLINE),
            "数据口径": receipt.get("数据口径", "日期"),
            "pending": not confirmed,
            "abnormal": False,
        }

        if existing is None:
            created = json_store.insert(COVERAGE_MODULE, values)
            if confirmed:
                self._refresh_lake_record(endpoint_id, day)
            return json_store.get_by_id(COVERAGE_MODULE, int(created["id"])) or created

        # 同一文件复查：只补齐确认状态，不新增来源记录，也不重复落湖。
        if confirmed and not existing.get("已确认"):
            json_store.update(COVERAGE_MODULE, int(existing["id"]), {
                "已确认": True,
                "写入结果": values["写入结果"],
                "pending": False,
                "updated_at": _now(),
            })
            self._refresh_lake_record(endpoint_id, day)
        return json_store.get_by_id(COVERAGE_MODULE, int(existing["id"])) or existing

    def _refresh_lake_record(self, endpoint_id: str, day: str) -> str:
        """每个 (端点, 日期) 只有一份来源记录，记录里列出全部已确认回执。"""
        sources = [
            self._source_ref(row)
            for row in self.get_coverage(endpoint_id, day, day)
            if row.get("已确认")
        ]
        if not sources:
            return ""
        lake_dir = os.path.join(str(json_store.base_dir), "lake", "progress_receipt", endpoint_id)
        os.makedirs(lake_dir, exist_ok=True)
        path = os.path.join(lake_dir, f"{day}.json")
        record = {
            "event_id": f"coverage:{endpoint_id}:{day}",
            "event_type": "progress_receipt",
            "source_endpoint": endpoint_id,
            "date": day,
            "sources": sources,
            "source_count": len(sources),
            "ingested_at": _now(),
        }
        try:
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as handle:
                    if json.load(handle).get("sources") == sources:
                        return path
            for row in self.get_coverage(endpoint_id, day, day):
                if row.get("已确认") and not row.get("lake_ref"):
                    json_store.update(COVERAGE_MODULE, int(row["id"]), {"lake_ref": path})
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(record, handle, ensure_ascii=False, indent=2)
        except (OSError, json.JSONDecodeError):
            return ""
        return path

    _write_lake_once = _refresh_lake_record

    # ── 线上回执 ─────────────────────────────────────────────────────
    def ingest_online_receipts(
        self,
        endpoint_id: int | str,
        receipts: list[dict[str, Any]] | None = None,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        source: str = SOURCE_ONLINE,
    ) -> dict[str, Any]:
        """登记线上采集回执；未显式给出时从调用链路记录中读取。

        同一端点同一天可能同时存在离线凭证和线上回执，两份依据都保留。
        重复登记同一份回执只刷新状态，不会新增来源记录或重复落湖。
        """
        endpoint_text = str(endpoint_id)
        if receipts is None:
            receipts = self._receipts_from_traces(endpoint_text, start_date, end_date)

        accepted: list[str] = []
        skipped: list[str] = []
        errors: list[str] = []
        for index, item in enumerate(receipts, start=1):
            day = normalise_date(item.get("date") or item.get("日期") or item.get("调用时间"))
            if not day:
                errors.append(f"第 {index} 条线上回执日期无效")
                continue
            receipt_no = str(
                item.get("receipt_no") or item.get("回执编号") or item.get("记录编号")
                or item.get("record_no") or f"ONLINE-{endpoint_text}-{day}"
            )
            confirmed = _is_written(
                item.get("write_result") or item.get("写入结果") or item.get("状态"),
                item.get("confirmed"),
            )
            existing = next(
                (row for row in json_store.get_all(COVERAGE_MODULE)
                 if str(row.get("关联端点")) == endpoint_text
                 and str(row.get("日期")) == day
                 and str(row.get("来源类型")) == source
                 and str(row.get("回执编号")) == receipt_no),
                None,
            )
            if existing is not None:
                if confirmed and not existing.get("已确认"):
                    json_store.update(COVERAGE_MODULE, int(existing["id"]), {
                        "已确认": True,
                        "pending": False,
                        "写入结果": item.get("write_result") or "已写入",
                        "updated_at": _now(),
                    })
                if confirmed:
                    self._refresh_lake_record(endpoint_text, day)
                skipped.append(day)
                continue

            created = json_store.insert(COVERAGE_MODULE, {
                "关联端点": endpoint_text,
                "日期": day,
                "位置": str(item.get("location") or item.get("位置") or ""),
                "写入结果": str(item.get("写入结果") or item.get("write_result") or "已写入"),
                "已确认": confirmed,
                "取得日期": normalise_date(item.get("acquired_date") or item.get("取得日期")) or day,
                "来源类型": source,
                "凭证ID": None,
                "凭证编号": receipt_no,
                "回执编号": receipt_no,
                "原文件名": "",
                "文件摘要": "",
                "数据来源": source,
                "数据口径": "日期",
                "pending": not confirmed,
                "abnormal": False,
            })
            json_store.update(COVERAGE_MODULE, int(created["id"]), {"lake_ref": ""})
            if confirmed:
                self._refresh_lake_record(endpoint_text, day)
            accepted.append(day)

        return {
            "ok": not errors,
            "message": f"线上回执登记完成：新增 {len(accepted)} 天，复查 {len(skipped)} 天",
            "endpoint_id": endpoint_text,
            "accepted_dates": sorted(set(accepted)),
            "reviewed_dates": sorted(set(skipped)),
            "errors": errors,
            "confirmed_dates": self.confirmed_dates(endpoint_text, start_date, end_date),
            "source_refs": self.coverage_sources(endpoint_text, start_date, end_date),
        }

    record_online_receipts = ingest_online_receipts

    def _receipts_from_traces(
        self,
        endpoint_id: str,
        start_date: str | None,
        end_date: str | None,
    ) -> list[dict[str, Any]]:
        """把调用链路记录当作线上回执：正常响应视为已写入。"""
        receipts = []
        for row in json_store.get_all("call_trace_log"):
            if str(row.get("关联端点")) != endpoint_id:
                continue
            day = normalise_date(row.get("调用时间"))
            if not day:
                continue
            if start_date and day < str(normalise_date(start_date)):
                continue
            if end_date and day > str(normalise_date(end_date)):
                continue
            ok = str(row.get("响应码", "200")) == "200" and row.get("status") in ("正常", "慢调用")
            receipts.append({
                "date": day,
                "record_no": row.get("记录编号"),
                "回执编号": row.get("记录编号"),
                "write_result": "已写入" if ok else "未写入",
                "confirmed": ok,
                "location": row.get("请求参数", ""),
            })
        return receipts

    # ── 下一次采集 ───────────────────────────────────────────────────
    def next_collection_window(
        self,
        endpoint_id: int | str,
        start_date: str,
        end_date: str,
        *,
        include_backfilled: bool = True,
    ) -> dict[str, Any]:
        """给出下一次采集应从哪里续做。

        水位走到 12 日不代表 9 日已经补齐：只要区间里仍有未确认的日期，
        下一次采集就从最早缺口开始，不会因为后面日期已确认而跳过它。
        """
        days = _date_list(start_date, end_date)
        confirmed = set(self.confirmed_dates(endpoint_id, start_date, end_date))
        backfilled: set[str] = set()
        if include_backfilled:
            for row in json_store.get_all("backfill_pending"):
                if (str(row.get("关联端点")) == str(endpoint_id)
                        and str(row.get("状态")) == "已回补"):
                    backfilled.add(str(row.get("日期")))
        gaps = [day for day in days if day not in confirmed and day not in backfilled]
        confirmed_in_range = [day for day in days if day in confirmed]
        return {
            "endpoint_id": str(endpoint_id),
            "date_range": {"start": days[0], "end": days[-1]} if days else {},
            "confirmed_dates": confirmed_in_range,
            "pending_dates": gaps,
            "start_date": gaps[0] if gaps else "",
            "resume_from": gaps[0] if gaps else "",
            "skipped_dates": [day for day in days if day not in gaps],
            "is_complete": not gaps,
            "message": (
                f"下一次采集从 {gaps[0]} 开始，待补 {len(gaps)} 天" if gaps
                else "区间内没有待补日期"
            ),
        }

    # ── 统计 ─────────────────────────────────────────────────────────
    def domain_snapshot(self) -> dict[str, Any]:
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            status = str(row.get("status", ""))
            if status in by_status:
                by_status[status] += 1
        coverage = json_store.get_all(COVERAGE_MODULE)
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "采集进度凭证数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for row in rows if row.get("abnormal")),
            "confirmed_receipts": sum(1 for row in rows if row.get("status") == "已确认"),
            "coverage_days": sum(1 for row in coverage if row.get("已确认")),
            "unconfirmed_days": sum(1 for row in coverage if not row.get("已确认")),
            "operation": self.OPERATION_TERM,
        }
