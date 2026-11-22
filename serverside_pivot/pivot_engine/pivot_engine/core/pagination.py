"""Hierarchy pagination with binding cursors (REQ-PAGE-001..003, F-PAGE-DEEP).

Modes (REQ-PAGE-003):

* ``restart_on_change``: each page is a direct windowed query; the cursor
  binds the source revision and is rejected once it moves.
* ``snapshot``: like ``restart_on_change`` but bound to a snapshot id; the
  edge resolves the snapshot relation, so revision moves are irrelevant.
* ``materialized``: the flattened, ordered tree is computed once into a
  backend temp table with a row number (portable through Ibis
  ``create_table(temp=True)``); every page is a range scan and the total row
  count is known.  The handle lives in the core cache under the active scope,
  so it can never be served to another tenant (REQ-SEC-004); eviction drops
  the table.

A cursor binds the query fingerprint (without window), server epoch, mode,
revision or snapshot/materialization id, grant fingerprint, policy version,
sort tuple and expansion hash (REQ-PAGE-002).  It is HMAC-signed with a
per-process secret; anything that does not match exactly is rejected with a
reason, never reinterpreted.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import secrets
import threading
import uuid
from dataclasses import asdict, dataclass, replace
from typing import Any, Dict, Optional, Tuple

import ibis

from .authority.scope import current_scope
from .authority.verifier import verify_scoped
from .cache import CacheKey, CacheManager, key_for
from .exec import Resolver, canonical, execute, scope_fingerprint
from .frame import ResultFrame
from .ir import PivotQuery, Window
from .logical import plan as logical_plan
from .physical import TREES, choose
from .physical.union_all import DEPTH
from .registry import type_family

__all__ = ["MODES", "CursorRejectedError", "MaterializationTooLargeError", "Page", "Pager"]

MODES = ("restart_on_change", "snapshot", "materialized")
ROW = "__row"
_LOG = logging.getLogger(__name__)


class CursorRejectedError(ValueError):
    """The cursor does not bind the current request; the client must restart."""

    def __init__(self, reason: str) -> None:
        super().__init__(f"Cursor rejected: {reason}.")
        self.reason = reason


class MaterializationTooLargeError(ValueError):
    """The flattened tree exceeds what the server may hold; page with restart_on_change instead."""


@dataclass(frozen=True)
class _Binding:
    fingerprint: str
    epoch: str
    mode: str
    revision: str
    snapshot_id: str
    materialization_id: str
    scope: str
    policy: str
    sort: str
    expansion: str


@dataclass(frozen=True)
class Page:
    frame: ResultFrame
    offset: int
    total_rows: Optional[int]  # known in materialized mode
    next_cursor: Optional[str]  # None when this page reaches the end


@dataclass(frozen=True)
class _Materialized:
    backend: Any
    table: str
    total: int


def _held_rows(value: Any) -> int:
    return value.total if isinstance(value, _Materialized) else 1


@dataclass(frozen=True)
class _Index:
    mat_id: str
    nbytes: int = 64


def _drop(_key: CacheKey, value: Any) -> None:
    if isinstance(value, _Materialized):
        try:
            value.backend.drop_table(value.table, force=True)
        except Exception:  # noqa: BLE001 - best effort: a temp table dies with its connection anyway
            _LOG.warning("could not drop materialized page table %s", value.table, exc_info=True)


class Pager:
    def __init__(self, cache: Optional[CacheManager] = None, *, epoch: Optional[str] = None,
                 secret: Optional[bytes] = None, max_held_rows: int = 100_000_000,
                 materialize_after: int = 2, materialize_offset: int = 10_000) -> None:
        # Direct pages are cheap near the top (top-N) and slow deep down; building
        # the held tree costs ~10 direct pages (6 levels x 10M rows: direct
        # 4.5 s/page, build 49 s, then 0.03-0.14 s per page).  Build once a
        # scroll is clearly under way.
        self.materialize_after = materialize_after
        self.materialize_offset = materialize_offset
        self._pages_seen: Dict[Tuple[str, str, str], int] = {}
        # Server-held trees live in the database (DuckDB spills to disk), so the
        # snapshot tier is budgeted in rows, not Python bytes.
        self.max_held_rows = max_held_rows
        self.cache = cache or CacheManager(budgets={"snapshot": max_held_rows}, sizeof=_held_rows, on_evict=_drop)
        self.epoch = epoch or uuid.uuid4().hex
        self._secret = secret or secrets.token_bytes(32)
        self._lock = threading.Lock()

    # ----------------------------------------------------------- cursor

    def _binding(self, query: PivotQuery, mode: str, revision: str, snapshot_id: str, mat_id: str) -> _Binding:
        unwindowed = replace(query, window=Window())
        grant = current_scope().grant_for(query.dataset)
        body = json.loads(canonical(unwindowed))
        return _Binding(
            fingerprint=hashlib.sha256(canonical(unwindowed).encode()).hexdigest()[:32],
            epoch=self.epoch,
            mode=mode,
            revision=revision,
            snapshot_id=snapshot_id,
            materialization_id=mat_id,
            scope=scope_fingerprint(query.dataset),
            policy=str(getattr(grant, "policy_version", "") or ""),
            sort=json.dumps(body["sort"]),
            expansion=hashlib.sha256(json.dumps(body["expanded"]).encode()).hexdigest()[:16],
        )

    def _encode(self, binding: _Binding, offset: int, limit: int) -> str:
        payload = json.dumps({"b": asdict(binding), "o": offset, "l": limit}, sort_keys=True).encode()
        sig = hmac.new(self._secret, payload, hashlib.sha256).digest()
        return base64.urlsafe_b64encode(payload).decode() + "." + base64.urlsafe_b64encode(sig).decode()

    def _decode(self, token: str) -> Tuple[_Binding, int, int]:
        try:
            raw_payload, raw_sig = token.split(".", 1)
            payload = base64.urlsafe_b64decode(raw_payload.encode())
            sig = base64.urlsafe_b64decode(raw_sig.encode())
        except (ValueError, TypeError):
            raise CursorRejectedError("malformed") from None
        if not hmac.compare_digest(sig, hmac.new(self._secret, payload, hashlib.sha256).digest()):
            raise CursorRejectedError("signature mismatch (forged, or issued by another server epoch)")
        data = json.loads(payload)
        return _Binding(**data["b"]), int(data["o"]), int(data["l"])

    @staticmethod
    def _check(issued: _Binding, current: _Binding) -> None:
        for name in ("epoch", "mode", "fingerprint", "scope", "policy", "sort", "expansion", "snapshot_id"):
            if getattr(issued, name) != getattr(current, name):
                raise CursorRejectedError(f"{name} changed")
        direct = issued.mode == "restart_on_change" or (
            issued.mode == "materialized" and not issued.materialization_id
        )
        if direct and issued.revision != current.revision:
            raise CursorRejectedError("revision changed")

    # ----------------------------------------------------------- paging

    def page(self, query: PivotQuery, resolve: Resolver, *, mode: str = "restart_on_change",
             revision: str = "unversioned", snapshot_id: str = "", cursor: Optional[str] = None,
             strategy: Optional[str] = None) -> Page:
        if mode not in MODES:
            raise ValueError(f"unknown pagination mode {mode!r}")
        if mode == "snapshot" and not snapshot_id:
            raise ValueError("snapshot mode requires a snapshot id")
        offset, limit = query.window.offset, query.window.limit
        mat_id = ""
        if cursor is not None:
            issued, offset, limit = self._decode(cursor)
            mat_id = issued.materialization_id
            self._check(issued, self._binding(query, mode, revision, snapshot_id, mat_id))
        if limit is None:
            raise ValueError("a page needs a window limit")
        handle = None
        if mode == "materialized":
            handle, mat_id = self._materialized(query, resolve, revision, mat_id, strategy, offset)
        if handle is not None:
            frame = self._slice(query, handle, offset, limit)
            total: Optional[int] = handle.total
        else:
            frame = execute(replace(query, window=Window(offset, limit)), resolve, strategy=strategy)
            total = None
        returned = frame.table.num_rows
        exhausted = returned < limit or (total is not None and offset + returned >= total)
        binding = self._binding(query, mode, revision, snapshot_id, mat_id)
        return Page(frame=frame, offset=offset, total_rows=total,
                    next_cursor=None if exhausted else self._encode(binding, offset + returned, limit))

    def _materialized(self, query: PivotQuery, resolve: Resolver, revision: str, mat_id: str,
                      strategy: Optional[str], offset: int = 0,
                      force: bool = False) -> Tuple[Optional[_Materialized], str]:
        base_fp = hashlib.sha256(canonical(replace(query, window=Window())).encode()).hexdigest()[:32]
        if mat_id:
            held = self.cache.get(key_for("snapshot", query.dataset, f"mat:{mat_id}"))
            if held is None:
                raise CursorRejectedError("materialized result expired")
            return held, mat_id
        index_key = key_for("plan", query.dataset, f"matidx:{base_fp}", revision)
        with self._lock:
            known = self.cache.get(index_key)
            if known is not None:
                held = self.cache.get(key_for("snapshot", query.dataset, f"mat:{known.mat_id}"))
                if held is not None:
                    return held, str(known.mat_id)
            seen_key = (scope_fingerprint(query.dataset), base_fp, revision)
            seen = self._pages_seen.get(seen_key, 0)
            if not force and seen < self.materialize_after and offset < self.materialize_offset:
                if len(self._pages_seen) > 10_000:
                    self._pages_seen.clear()
                self._pages_seen[seen_key] = seen + 1
                return None, ""
            self._pages_seen.pop(seen_key, None)
            mat_id = uuid.uuid4().hex
            handle = self._build(query, resolve, strategy, mat_id)
            if handle.total > self.max_held_rows:
                _drop(index_key, handle)
                raise MaterializationTooLargeError(
                    f"materialized tree has {handle.total} rows, above the {self.max_held_rows} row limit; "
                    "use restart_on_change paging"
                )
            self.cache.put(key_for("snapshot", query.dataset, f"mat:{mat_id}"), handle)
            self.cache.put(index_key, _Index(mat_id))
        return handle, mat_id

    def _build(self, query: PivotQuery, resolve: Resolver, strategy: Optional[str], mat_id: str) -> _Materialized:
        relation = resolve(query.dataset)
        schema = relation.schema()
        unwindowed = replace(query, window=Window())
        unwindowed.validate({name: type_family(dtype) for name, dtype in schema.items()})
        plan = logical_plan(unwindowed)
        from .physical.union_all import prepared

        relation = prepared(relation, unwindowed)  # the strategy chooser sees the category and axis columns
        union, order = TREES[strategy or choose(plan, relation.schema())](plan, relation)
        numbered = union.mutate(**{ROW: ibis.row_number().over(order_by=order) if order else ibis.literal(0)})
        verify_scoped(numbered)
        backend = ibis.get_backend(numbered)
        name = f"__pivot_mat_{mat_id}"
        backend.create_table(name, obj=numbered, temp=True)
        table = backend.table(name)
        total = int(table.count().to_pyarrow().as_py())
        return _Materialized(backend=backend, table=name, total=total)

    def _slice(self, query: PivotQuery, handle: _Materialized, offset: int, limit: int) -> ResultFrame:
        t = handle.backend.table(handle.table)
        columns = [*query.rows, *query.value_columns(), DEPTH]
        page = t.filter((t[ROW] >= offset) & (t[ROW] < offset + limit)).order_by(ROW).select(*columns)
        return ResultFrame(
            table=page.to_pyarrow(),
            rows=tuple(query.rows),
            measures=tuple(m.alias for m in query.measures),
            fingerprint=f"mat:{handle.table}",
            window=(offset, limit),
            meta={"total_rows": handle.total},
        )

    def total_rows(self, query: PivotQuery, resolve: Resolver, *, revision: str = "unversioned") -> int:
        handle, _ = self._materialized(query, resolve, revision, "", None, force=True)
        assert handle is not None
        return handle.total
