"""领域服务注册表：通用资源路由按模块 key 分派到独立业务服务。"""
from __future__ import annotations

from app.services.base import BaseModuleService
from app.services.api_endpoint import ApiEndpointService
from app.services.webhook_channel import WebhookChannelService
from app.services.event_schema import EventSchemaService
from app.services.mq_consumer import MqConsumerService
from app.services.dlq_handler import DlqHandlerService
from app.services.api_auth_store import ApiAuthStoreService
from app.services.rate_limit_config import RateLimitConfigService
from app.services.data_transform import DataTransformService
from app.services.dispatch_rule import DispatchRuleService
from app.services.dedup_filter import DedupFilterService
from app.services.batch_ingest import BatchIngestService
from app.services.data_lake_sink import DataLakeSinkService
from app.services.data_quality_rule import DataQualityRuleService
from app.services.call_trace_log import CallTraceLogService
from app.services.incre_cursor import IncreCursorService
from app.services.backfill_job import BackfillJobService
from app.services.event_archive import EventArchiveService
from app.services.integration_monitor import IntegrationMonitorService
from app.services.progress_receipt import ProgressReceiptService

SERVICES: dict[str, BaseModuleService] = {
    "api_endpoint": ApiEndpointService(),
    "webhook_channel": WebhookChannelService(),
    "event_schema": EventSchemaService(),
    "mq_consumer": MqConsumerService(),
    "dlq_handler": DlqHandlerService(),
    "api_auth_store": ApiAuthStoreService(),
    "rate_limit_config": RateLimitConfigService(),
    "data_transform": DataTransformService(),
    "dispatch_rule": DispatchRuleService(),
    "dedup_filter": DedupFilterService(),
    "batch_ingest": BatchIngestService(),
    "data_lake_sink": DataLakeSinkService(),
    "data_quality_rule": DataQualityRuleService(),
    "call_trace_log": CallTraceLogService(),
    "incre_cursor": IncreCursorService(),
    "backfill_job": BackfillJobService(),
    "event_archive": EventArchiveService(),
    "integration_monitor": IntegrationMonitorService(),
    "progress_receipt": ProgressReceiptService(),
}


def service_for(module: str) -> BaseModuleService | None:
    return SERVICES.get(module)
