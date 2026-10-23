"""Regenerate the tracked, deterministic wide browser qualification fixture."""

from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "tests" / "data" / "flagship_scroll_rows.json"


def make_fixture() -> dict:
    return {
        "id": "flagship-scroll-v1",
        "locale": "en-GB",
        "rows": [
            {
                "region": f"R{region:03d}",
                "account": f"A{region:03d}",
                "scope": "Included",
                "period": f"P{period:02d}",
                "ventes_€": region * 100 + period + 0.25,
                "unités": 1,
                "écart": float(region),
            }
            for region in range(120)
            for period in range(24)
        ],
    }


if __name__ == "__main__":
    OUTPUT.write_text(json.dumps(make_fixture(), ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
