"""模块记录访问与运营概览汇总。

所有记录统一保存在 JSON 分区里；示例数据只在服务启动时播种一次，
领域服务、资源路由和跨模块链路读到的是同一份数据。
"""
from __future__ import annotations

from typing import Any

from app import persistence
from app.seed import SEED_ROWS


def seed_store() -> None:
    """首次启动（分区为空）时写入示例数据。"""
    store = persistence.json_store
    for module, rows in SEED_ROWS.items():
        if not rows:
            continue
        if store.get_all(module):
            continue
        for row in rows:
            store.insert(module, dict(row))


def module_rows(module: str) -> list[dict[str, Any]]:
    return persistence.json_store.get_all(module)


def find_row(module: str, entry_id: int) -> dict[str, Any] | None:
    return persistence.json_store.get_by_id(module, entry_id)


class Store:
    def module_names(self) -> list[str]:
        return sorted(SEED_ROWS)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return module_rows(module)

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        return find_row(module, entry_id)

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}


store = Store()
