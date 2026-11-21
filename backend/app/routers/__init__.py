"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import resource, pipeline, progress

# 固定前缀/catch-all 顺序：动态 /api/{module}/{entry_id} 必须最后注册，
# 否则 /api/pipeline/... 这类固定路径会被它抢先匹配。
ROUTERS = [pipeline, progress, resource]
