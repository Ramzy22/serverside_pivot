"""Redis export control state using JSON and atomic server-time fencing.

Each record, its cancellation intent and owner lease share one key, so every
decision is a single-key atomic transaction. Lua checks Redis TIME at commit,
so a paused client cannot commit after its ownership deadline. No output
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
from .export_jobs import ExportJobSnapshot


_COMMIT_RECORD = """
local raw = redis.call('GET', KEYS[1])
if (raw or '') ~= ARGV[1] then return 0 end
local stamp = redis.call('TIME')
local now = tonumber(stamp[1]) + tonumber(stamp[2]) / 1000000
local candidate = nil
if ARGV[2] ~= '' then candidate = cjson.decode(ARGV[2]) end
local function terminal(phase)
    return phase == 'cancelled' or phase == 'completed'
        or phase == 'failed' or phase == 'interrupted'
end
if raw then
    local current = cjson.decode(raw)
    if not terminal(current.snapshot.phase) and now >= current.deadline then
        -- The only valid write after lease loss is the fenced interruption.
        -- Returning -1 retries the Python transition against fresh Redis time.
        if not candidate or candidate.snapshot.phase ~= 'interrupted'
            or candidate.snapshot.owner_epoch ~= current.snapshot.owner_epoch + 1
            or candidate.owner_id ~= cjson.null then
            return -1
        end
    elseif terminal(current.snapshot.phase) and now >= current.deadline then
        if candidate then return -1 end
    end
end
if not candidate then
    redis.call('DEL', KEYS[1])
elseif now >= candidate.deadline then
    return -1
elseif terminal(candidate.snapshot.phase) then
    redis.call('SET', KEYS[1], ARGV[2], 'PX', math.max(1, math.ceil((candidate.deadline - now) * 1000)))
else
    redis.call('SET', KEYS[1], ARGV[2])
end
return 1
"""


def _json_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_json_value(item) for item in value]
    return value


class RedisExportJobStore(ExportJobStore):
    def __init__(self, runtime_state: RedisRuntimeState, **kwargs: Any) -> None:
        self._runtime_state = runtime_state
        # Accept the common constructor's clock argument for API symmetry;
        # distributed leases always use the Redis server clock instead.
        kwargs["clock"] = self._redis_time
        super().__init__(**kwargs)
        self._prefix = runtime_state.key("export-jobs") + ":"

    def _redis_time(self) -> float:
        seconds, microseconds = self._runtime_state._call(self._runtime_state.client.time)
        return float(seconds) + float(microseconds) / 1_000_000

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
                        raw = pipe.get(key)
                        record, result = update(self._decode(raw))
                        pipe.multi()
                        pipe.eval(_COMMIT_RECORD, 1, key, raw or b"",
                                  self._encode(record) if record is not None else b"")
                        if pipe.execute()[0] == 1:
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
