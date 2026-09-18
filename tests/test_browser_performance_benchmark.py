"""Unit contracts for the opt-in browser/Dash latency benchmark."""

from scripts.benchmark_browser_runtime import (
    initial_viewport_timing,
    percentile,
    prepare_scroll_samples,
    summarize,
)


def test_browser_benchmark_percentile_uses_nearest_rank_for_tail_latency():
    assert percentile([10.0, 20.0, 30.0, 40.0], 0.5) == 20.0
    assert percentile([10.0, 20.0, 30.0, 40.0], 0.95) == 40.0
    assert percentile([], 0.95) is None


def test_browser_benchmark_reports_end_to_end_network_and_render_percentiles():
    report = summarize(
        [
            {"derived": {"totalMs": 90, "responseMs": 70, "renderMs": 8}},
            {"derived": {"totalMs": 110, "responseMs": 80, "renderMs": 9}},
            {"derived": {"totalMs": 170, "responseMs": 130, "renderMs": 11}},
        ],
        input_rows=100_000,
        requested_samples=30,
    )

    assert report["inputRows"] == 100_000
    assert report["requestedSamples"] == 30
    assert report["completedSamples"] == 3
    assert report["timingsMs"] == {
        "endToEndP50": 110.0,
        "endToEndP95": 170.0,
        "networkAndServerP50": 80.0,
        "networkAndServerP95": 130.0,
        "dashCallbackP50": None,
        "dashCallbackP95": None,
        "pivotServiceP50": None,
        "pivotServiceP95": None,
        "reactCommitP50": 9.0,
        "reactCommitP95": 11.0,
    }
    assert report["runtimePaths"] == {"adapter": {}}
    assert report["serverStagesMs"] == {
        "adapterHierarchyViewP50": None,
        "adapterHierarchyViewP95": None,
        "controllerPageKeyFetchP50": None,
        "controllerPageKeyFetchP95": None,
        "controllerRootQueryP50": None,
        "controllerRootQueryP95": None,
        "controllerRootCountP50": None,
        "controllerRootCountP95": None,
    }


def test_browser_benchmark_keeps_initial_viewport_timing_separate_from_scroll_samples():
    timing = initial_viewport_timing(
        [
            {"derived": {"totalMs": 80, "responseMs": 70, "renderMs": 7}},
            {
                "derived": {"totalMs": 120, "responseMs": 100, "renderMs": 9},
                "profile": {
                    "callback": {"totalMs": 95.125},
                    "service": {"totalMs": 87.5},
                },
            },
        ]
    )

    assert timing == {
        "endToEnd": 120.0,
        "networkAndServer": 100.0,
        "dashCallback": 95.125,
        "pivotService": 87.5,
        "reactCommit": 9.0,
    }


def test_browser_benchmark_preparation_preserves_hydration_before_clearing_history(monkeypatch):
    class _Driver:
        execute_script_calls = []

        def execute_script(self, script):
            self.execute_script_calls.append(script)

    driver = _Driver()
    hydration = [{"derived": {"totalMs": 80}}]

    class _Wait:
        def __init__(self, _driver, _timeout):
            pass

        def until(self, condition):
            return condition(driver) if callable(condition) else True

    monkeypatch.setattr(
        "scripts.benchmark_browser_runtime.WebDriverWait", _Wait
    )
    monkeypatch.setattr(
        "scripts.benchmark_browser_runtime.profiler_history", lambda _driver: hydration
    )
    monkeypatch.setattr(
        "scripts.benchmark_browser_runtime.expected_conditions.presence_of_element_located",
        lambda _locator: "element-ready",
    )

    assert prepare_scroll_samples(driver) == hydration
    assert driver.execute_script_calls == ["window.__pivotProfiler.clear('pivot-grid');"]
