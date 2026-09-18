"""Contract checks for the flagship direct-transport benchmark."""

from scripts.benchmark_flagship_transport import flagship_request, percentile


def test_flagship_transport_request_is_canonical_v1_and_matches_the_showcase_layout():
    request = flagship_request(7)

    assert request["schemaVersion"] == 1
    assert request["kind"] == "data"
    assert request["requestId"] == "flagship-transport-7"
    assert request["state"]["rowFields"] == ["region", "segment", "product"]
    assert request["state"]["colFields"] == ["quarter"]
    assert len(request["state"]["valConfigs"]) == 4
    assert request["payload"]["intent"] == "structural"
    assert request["payload"]["end"] == 199


def test_flagship_transport_benchmark_uses_nearest_rank_for_p95():
    assert percentile([10, 20, 30, 40], 0.5) == 20.0
    assert percentile([10, 20, 30, 40], 0.95) == 40.0
    assert percentile([], 0.95) is None
