"""ResultFrame: the one result shape every edge maps from (Arrow + identity)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa

from .identity import CanonicalPathCodec

DEPTH = "__depth"


@dataclass(frozen=True)
class ResultFrame:
    table: pa.Table  # dimensions, measures, __depth
    rows: Tuple[str, ...]
    measures: Tuple[str, ...]
    fingerprint: str = ""
    revision: Optional[int] = None
    window: Tuple[int, Optional[int]] = (0, None)
    meta: Dict[str, Any] = field(default_factory=dict)

    def records(self) -> List[Dict[str, Any]]:
        """Rows with typed path, canonical identity and depth (REQ-ID-001/002)."""
        out = []
        for rec in self.table.to_pylist():
            depth = int(rec.pop(DEPTH))
            path = tuple(rec[k] for k in self.rows[:depth])
            out.append({
                "path": path,
                "depth": depth,
                "key": CanonicalPathCodec.encode(list(path)),
                "values": {m: rec[m] for m in self.measures},
                "dims": {k: rec[k] for k in self.rows[:depth]},
            })
        return out
