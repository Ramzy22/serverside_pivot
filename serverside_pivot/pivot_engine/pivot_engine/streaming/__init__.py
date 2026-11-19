"""Streaming package with strict one-way import boundaries (PEP 562 lazy facade)."""

from __future__ import annotations

from typing import Any

from .exact_state import (
    CDCJobSnapshot,
    CDCStateStore,
    CanonicalScalarCodec,
    ExactCDCStateError,
    ExactGroupState,
    ExactMeasureAccumulator,
    MemoryCDCStateStore,
    RedisCDCStateStore,
    StateVersionConflict,
)
from .streaming_processor import StreamAggregationProcessor
from .versioned_source_adapter import (
    CommittedChangeBatch,
    DuckDBSingleOwnerError,
    SourceCapabilities,
    SourceCapabilityTier,
    SourceChangeProvider,
    SourceMutationError,
    SubscriptionInbox,
    VersionedSourceAdapter,
)
from .exact_measure_engine import (
    CanonicalPathCodec,
    CrossNodeDependencyGraph,
    DependencyScope,
    ExactMeasureEngine,
    GroupSufficientStats,
    MeasureAlgebraClass,
    MeasureDiffResult,
    SufficientStatCell,
    SufficientStatsProjector,
    compute_measure_diff,
    decode_wire_value,
    encode_wire_value,
)

_LAZY_EXPORTS = {
    "AuthoritativeQueryResult": (".reactive_query_coordinator", "AuthoritativeQueryResult"),
    "ComparisonKey": (".reactive_query_coordinator", "ComparisonKey"),
    "ExecutionStrategy": (".reactive_query_coordinator", "ExecutionStrategy"),
    "LiveQueryDelta": (".reactive_query_coordinator", "LiveQueryDelta"),
    "LiveQueryKey": (".reactive_query_coordinator", "LiveQueryKey"),
    "ReactiveQueryCoordinator": (".reactive_query_coordinator", "ReactiveQueryCoordinator"),
    "SharedLiveQuery": (".reactive_query_coordinator", "SharedLiveQuery"),
    "ClientViewportSubscription": (".subscription_broker", "ClientViewportSubscription"),
    "PROTOCOL_VERSION_V2": (".subscription_broker", "PROTOCOL_VERSION_V2"),
    "PauseMode": (".subscription_broker", "PauseMode"),
    "SubscriptionBroker": (".subscription_broker", "SubscriptionBroker"),
    "SubscriptionState": (".subscription_broker", "SubscriptionState"),
    "ViewportWindow": (".subscription_broker", "ViewportWindow"),
}


def __getattr__(name: str) -> Any:
    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = target
    import importlib

    module = importlib.import_module(module_name, __name__)
    return getattr(module, attr_name)


__all__ = [
    "AuthoritativeQueryResult",
    "CDCJobSnapshot",
    "CDCStateStore",
    "CanonicalPathCodec",
    "CanonicalScalarCodec",
    "ClientViewportSubscription",
    "CommittedChangeBatch",
    "ComparisonKey",
    "CrossNodeDependencyGraph",
    "DependencyScope",
    "DuckDBSingleOwnerError",
    "ExactCDCStateError",
    "ExactGroupState",
    "ExactMeasureAccumulator",
    "ExactMeasureEngine",
    "ExecutionStrategy",
    "GroupSufficientStats",
    "LiveQueryDelta",
    "LiveQueryKey",
    "MeasureAlgebraClass",
    "MeasureDiffResult",
    "MemoryCDCStateStore",
    "PROTOCOL_VERSION_V2",
    "PauseMode",
    "ReactiveQueryCoordinator",
    "RedisCDCStateStore",
    "SharedLiveQuery",
    "SourceCapabilities",
    "SourceCapabilityTier",
    "SourceChangeProvider",
    "SourceMutationError",
    "StateVersionConflict",
    "StreamAggregationProcessor",
    "SubscriptionBroker",
    "SubscriptionInbox",
    "SubscriptionState",
    "SufficientStatCell",
    "SufficientStatsProjector",
    "VersionedSourceAdapter",
    "ViewportWindow",
    "compute_measure_diff",
    "decode_wire_value",
    "encode_wire_value",
]
