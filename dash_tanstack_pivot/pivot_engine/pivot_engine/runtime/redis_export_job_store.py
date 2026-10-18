"""Redis export control state using JSON and WATCH/MULTI fencing.

Each record, its cancellation intent and owner lease share one key, so every
decision is a single-key atomic transaction. No native handles or output
bytes are stored here. Active keys survive owner death until a reader or the
expiry sweeper converts them to interrupted; terminal records have a Redis TTL.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import fields
import json
import math
from typing import Any
from urllib.parse import quote, unquote

from .distributed import DistributedRuntimeUnavailable, RedisRuntimeState
from .export_job_store import ExportJobStore, _JobRecord
from .export_jobs import ExportJobSnapshot, EXPORT_JOB_TERMINAL_PHASES


def _json_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_json_value(item) for item in value]
    return value


class RedisExportJobStore(ExportJobStore):
    def __init__(self, runtime_state: RedisRuntimeState, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._runtime_state = runtime_state
        self._prefix = runtime_state.key("export-jobs") + ":"

    def _key(self, job_id: str) -> str:
        return self._prefix + quote(job_id, safe="")

    @staticmethod
    def _encode(record: _JobRecord) -> bytes:
        snapshot = {field.name: _json_value(getattr(record.snapshot, field.name)) for field in fields(ExportJobSnapshot)}
        return json.dumps({"version": 1, "snapshot": snapshot, "owner_id": record.owner_id,
                           "deadline": record.deadline}, separators=(",", ":"), allow_nan=False).encode("utf-8")

    @staticmethod
    def _decode(raw: Any) -> _JobRecord | None:
        if raw is None:
            return None
        try:
            value = json.loads(raw)
            if value["version"] != 1:
                raise ValueError("Unknown export job record version.")
            deadline = float(value["deadline"])
            owner_id = value["owner_id"]
            snapshot = ExportJobSnapshot(**value["snapshot"])
            if not math.isfinite(deadline) or (owner_id is not None and not isinstance(owner_id, str)):
                raise ValueError("Invalid export lease.")
            return _JobRecord(snapshot, owner_id, deadline)
        except (TypeError, ValueError, KeyError) as exc:
            raise DistributedRuntimeUnavailable("Distributed export job state is invalid.") from exc

    def _transaction(self, job_id, update):
        # Importing this optional dependency occurs only in distributed mode.
        from redis.exceptions import WatchError

        def transact():
            key = self._key(job_id)
            for _ in range(64):
                with self._runtime_state.client.pipeline() as pipe:
                    try:
                        pipe.watch(key)
                        record, result = update(self._decode(pipe.get(key)))
                        pipe.multi()
                        if record is None:
                            pipe.delete(key)
                        elif record.snapshot.phase in EXPORT_JOB_TERMINAL_PHASES:
                            remaining_ms = max(1, math.ceil((record.deadline - self._clock()) * 1000))
                            pipe.set(key, self._encode(record), px=remaining_ms)
                        else:
                            pipe.set(key, self._encode(record))
                        pipe.execute()
                        return result
                    except WatchError:
                        continue
            raise DistributedRuntimeUnavailable("Distributed export job state is contended.")

        # Preserve lifecycle input errors while translating only Redis failures.
        self._runtime_state._require_open()
        try:
            return transact()
        except (ValueError, DistributedRuntimeUnavailable):
            raise
        except Exception as exc:
            raise DistributedRuntimeUnavailable("Distributed runtime Redis is unavailable.") from exc

    def _job_ids(self):
        def scan():
            return [unquote((key.decode("utf-8") if isinstance(key, bytes) else key)[len(self._prefix):])
                    for key in self._runtime_state.client.scan_iter(match=self._prefix + "*")]
        return self._runtime_state._call(scan)
