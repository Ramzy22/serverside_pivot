"""Profiling assembly for transport-agnostic runtime requests."""

from __future__ import annotations

import time
from typing import Any, Dict, Optional

from .models import PivotRequestContext, PivotViewState
from .request_coordinator import RuntimeRequestCoordinator


class RuntimeProfileBuilder:
    """Collect stable lifecycle timing fields without routing requests."""

    def __init__(
        self,
        *,
        enabled: bool,
        state: PivotViewState,
        context: PivotRequestContext,
        trigger_kind: str,
        started_at: Optional[float] = None,
    ) -> None:
        self.enabled = bool(enabled)
        self.state = state
        self.context = context
        self.trigger_kind = trigger_kind
        self.started_at = started_at if started_at is not None else time.perf_counter()
        self.adapter_lookup_started_at = self.started_at
        self.adapter_lookup_finished_at: Optional[float] = None
        self.gate_finished_at: Optional[float] = None
        self.request_built_at: Optional[float] = None

    @staticmethod
    def _milliseconds(
        start: Optional[float],
        end: Optional[float],
    ) -> Optional[float]:
        if start is None or end is None:
            return None
        return round((end - start) * 1000, 3)

    def mark_adapter_lookup_finished(self, at: Optional[float] = None) -> None:
        self.adapter_lookup_finished_at = at if at is not None else time.perf_counter()

    def mark_gate_finished(self, at: Optional[float] = None) -> None:
        self.gate_finished_at = at if at is not None else time.perf_counter()

    def mark_request_built(self, at: Optional[float] = None) -> None:
        self.request_built_at = at if at is not None else time.perf_counter()

    def build(
        self,
        *,
        execution_started_at: Optional[float] = None,
        execution_finished_at: Optional[float] = None,
        postprocess_started_at: Optional[float] = None,
        response_rows: Optional[int] = None,
        response_columns: Optional[int] = None,
        cancellation_outcome: str = "not_cancelled",
        extra: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        if not self.enabled:
            return None

        now = time.perf_counter()
        context = self.context
        state = self.state
        active_key = RuntimeRequestCoordinator.active_request_key(context)
        profile: Dict[str, Any] = {
            "request": {
                "requestId": context.request_id,
                "kind": self.trigger_kind,
                "viewMode": state.view_mode,
                "intent": context.intent,
                "table": context.table,
                "sessionId": context.session_id,
                "clientInstance": context.client_instance,
                "stateEpoch": context.state_epoch,
                "windowSeq": context.window_seq,
                "abortGeneration": context.abort_generation,
                "cacheKey": context.cache_key,
                "lifecycleLane": active_key[2] if active_key is not None else None,
                "cancellationOutcome": cancellation_outcome,
                "rowFields": len(state.row_fields or []),
                "colFields": len(state.col_fields or []),
                "valueFields": len(state.val_configs or []),
                "startRow": context.start_row,
                "endRow": context.end_row,
                "colStart": context.col_start,
                "colEnd": context.col_end,
                "needsColSchema": context.needs_col_schema,
                "includeGrandTotal": context.include_grand_total,
            },
            "service": {
                "adapterLookupMs": self._milliseconds(
                    self.adapter_lookup_started_at,
                    self.adapter_lookup_finished_at,
                ),
                "gateMs": self._milliseconds(
                    self.adapter_lookup_finished_at,
                    self.gate_finished_at,
                ),
                "requestBuildMs": self._milliseconds(
                    self.gate_finished_at,
                    self.request_built_at,
                ),
                "executeMs": self._milliseconds(
                    execution_started_at,
                    execution_finished_at,
                ),
                "postProcessMs": (
                    self._milliseconds(postprocess_started_at, now)
                    if postprocess_started_at is not None
                    else None
                ),
                "totalMs": self._milliseconds(self.started_at, now),
                "responseRows": response_rows,
                "responseColumns": response_columns,
            },
        }
        if isinstance(extra, dict):
            profile.update(extra)
        return profile
