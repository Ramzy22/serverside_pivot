import json
import subprocess
import sys
from pathlib import Path

import pyarrow as pa
import pytest

from pivot_engine.scalable_pivot_controller import ScalablePivotController
from pivot_engine.types.pivot_spec import Measure, PivotSpec


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
BENCHMARK = REPOSITORY_ROOT / "scripts" / "benchmark_pivot_runtime.py"


def test_runtime_benchmark_emits_latency_throughput_and_cache_metrics():
    result = subprocess.run(
        [sys.executable, str(BENCHMARK), "--rows", "2000", "--repeat-runs", "2", "--json"],
        cwd=REPOSITORY_ROOT,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )

    assert result.returncode == 0, result.stderr or result.stdout
    report = json.loads(result.stdout)

    assert report["inputRows"] == 2000
    assert report["outputRows"] > 0
    assert report["viewportRows"] > 0
    assert report["repeatRuns"] == 2
    assert set(report["timingsMs"]) == {
        "load",
        "coldPivot",
        "repeatPivotP50",
        "repeatPivotP95",
        "viewportPage",
    }
    assert all(value >= 0 for value in report["timingsMs"].values())
    assert report["throughput"]["coldInputRowsPerSecond"] > 0
    assert report["throughput"]["repeatInputRowsPerSecond"] > 0
    assert report["cache"]["hits"] > 0


@pytest.mark.asyncio
async def test_async_standard_pivot_populates_the_diff_engine_cache():
    controller = ScalablePivotController(
        backend_uri=":memory:",
        enable_streaming=False,
        enable_incremental_views=False,
    )
    spec = PivotSpec(
        table="cache_probe",
        rows=["region"],
        measures=[Measure(field="sales", agg="sum", alias="sales_sum")],
    )
    controller.load_data_from_arrow(
        "cache_probe",
        pa.table({"region": ["East", "West", "East"], "sales": [10, 20, 30]}),
    )

    try:
        first = await controller.run_pivot_async(spec, return_format="dict", force_refresh=True)
        hits_before_repeat = controller._cache_hits
        second = await controller.run_pivot_async(spec, return_format="dict")

        assert first == second
        assert controller._cache_hits > hits_before_repeat
    finally:
        controller.close()
