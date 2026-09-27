"""Behavioral tests for the process-local resource admission boundary."""

from __future__ import annotations

import array

import pytest

from pivot_engine.resource_governor import (
    ResourceExhaustedError,
    ResourceGovernor,
    ResourceKind,
    ResourceLease,
    ResourceLimits,
    estimate_value_bytes,
)


def test_resource_governor_rejects_exhausted_bytes_or_slots():
    governor = ResourceGovernor(
        ResourceLimits.for_testing(
            total_bytes=100,
            headroom_bytes=10,
            query_bytes=80,
            max_queries=1,
        ),
        rss_bytes_getter=lambda: 0,
    )

    lease = governor.reserve(ResourceKind.QUERY, 80)
    with pytest.raises(ResourceExhaustedError, match="resource_exhausted") as raised:
        governor.reserve(ResourceKind.QUERY, 1)

    assert raised.value.code == "resource_exhausted"
    assert raised.value.resource_kind is ResourceKind.QUERY
    assert raised.value.retry_after_ms > 0
    lease.release()
    assert governor.reserve(ResourceKind.QUERY, 80).kind is ResourceKind.QUERY


def test_resource_limits_use_safe_defaults_for_missing_or_malformed_environment():
    defaults = ResourceLimits.from_environment({})
    limits = ResourceLimits.from_environment(
        {
            "PIVOT_RESOURCE_MAX_BYTES": "not-an-integer",
            "PIVOT_RESOURCE_HEADROOM_BYTES": "0",
            "PIVOT_MAX_CONCURRENT_QUERIES": "-3",
        }
    )

    assert limits == defaults
    assert defaults.total_bytes == 1024**3
    assert defaults.headroom_bytes == 256 * 1024**2


def test_resource_governor_rejects_new_work_above_rss_soft_limit():
    limits = ResourceLimits.for_testing(
        total_bytes=100,
        headroom_bytes=10,
        query_bytes=80,
        rss_soft_limit_bytes=50,
    )
    governor = ResourceGovernor(limits, rss_bytes_getter=lambda: 50)

    with pytest.raises(ResourceExhaustedError, match="resource_exhausted") as raised:
        governor.reserve(ResourceKind.QUERY, 1)

    assert raised.value.resource_kind is ResourceKind.QUERY
    assert governor.snapshot()["rejected"]["query"]["rss_soft_limit"] == 1


def test_lease_growth_is_atomic_and_release_is_idempotent():
    governor = ResourceGovernor(
        ResourceLimits.for_testing(total_bytes=100, headroom_bytes=10, query_bytes=80),
        rss_bytes_getter=lambda: 0,
    )
    lease = governor.reserve(ResourceKind.QUERY, 40)

    lease.grow(40)
    assert lease.reserved_bytes == 80
    with pytest.raises(ResourceExhaustedError, match="resource_exhausted"):
        lease.grow(1)
    assert lease.reserved_bytes == 80

    lease.release()
    lease.release()
    snapshot = governor.snapshot()
    assert snapshot["reserved_bytes"] == 0
    assert snapshot["active_slots"]["query"] == 0


def test_snapshot_reports_class_metrics_and_high_water_rss():
    governor = ResourceGovernor(
        ResourceLimits.for_testing(total_bytes=100, headroom_bytes=10, cache_bytes=80),
        rss_bytes_getter=lambda: 17,
    )

    lease = governor.reserve(ResourceKind.CACHE, 20)
    snapshot = governor.snapshot()

    assert snapshot["reserved_bytes"] == 20
    assert snapshot["reserved_bytes_by_kind"]["cache"] == 20
    assert snapshot["admitted"]["cache"] == 1
    assert snapshot["rss_bytes"] == 17
    assert snapshot["high_water_rss_bytes"] == 17
    lease.release()


def test_estimate_value_bytes_handles_arrow_bytes_and_text():
    pyarrow = pytest.importorskip("pyarrow")

    assert estimate_value_bytes(b"abc") == 3
    assert estimate_value_bytes("abc") == len("abc".encode("utf-8"))
    assert estimate_value_bytes(pyarrow.array(["ab", "c"])) > 0
    assert estimate_value_bytes(object()) is None


def test_estimate_value_bytes_counts_unicode_without_encoding_or_surrogate_errors():
    assert estimate_value_bytes("Aé€𐍈") == 10
    assert estimate_value_bytes("\ud800") is None


def test_estimate_value_bytes_uses_memoryview_byte_length_not_element_count():
    values = array.array("I", [1, 2, 3])

    assert estimate_value_bytes(memoryview(values)) == 12


def test_snapshot_retains_rejection_kind_and_reason():
    limits = ResourceLimits.for_testing(
        rss_soft_limit_bytes=1,
        total_bytes=100,
        headroom_bytes=10,
    )
    governor = ResourceGovernor(limits, rss_bytes_getter=lambda: 1)

    for kind in (ResourceKind.QUERY, ResourceKind.EXPORT):
        with pytest.raises(ResourceExhaustedError):
            governor.reserve(kind, 1)

    rejected = governor.snapshot()["rejected"]
    assert rejected["query"]["rss_soft_limit"] == 1
    assert rejected["export"]["rss_soft_limit"] == 1


def test_resource_exhausted_error_caps_huge_retry_metadata():
    error = ResourceExhaustedError(
        ResourceKind.QUERY,
        reason="slot",
        retry_after_ms=10**100,
    )

    assert 1 <= error.retry_after_ms <= 60_000


def test_rss_probe_failure_rejects_admission_without_reserving_capacity():
    def unavailable_rss() -> int:
        raise RuntimeError("process unavailable")

    governor = ResourceGovernor(
        ResourceLimits.for_testing(total_bytes=100, headroom_bytes=10, query_bytes=80),
        rss_bytes_getter=unavailable_rss,
    )

    with pytest.raises(ResourceExhaustedError) as raised:
        governor.reserve(ResourceKind.QUERY, 20)

    assert raised.value.reason == "rss_unavailable"
    snapshot = governor.snapshot()
    assert snapshot["rejected"]["query"]["rss_unavailable"] == 1
    assert snapshot["rss_bytes"] is None
    assert snapshot["reserved_bytes_by_kind"]["query"] == 0
    assert snapshot["active_slots"]["query"] == 0


def test_rss_probe_failure_rejects_growth_without_changing_live_charge():
    available = True

    def rss_reading() -> int:
        if not available:
            raise RuntimeError("process unavailable")
        return 0

    governor = ResourceGovernor(
        ResourceLimits.for_testing(total_bytes=100, headroom_bytes=10, query_bytes=80),
        rss_bytes_getter=rss_reading,
    )
    lease = governor.reserve(ResourceKind.QUERY, 20)
    available = False

    with pytest.raises(ResourceExhaustedError, match="resource_exhausted") as raised:
        lease.grow(1)

    assert raised.value.reason == "rss_unavailable"
    snapshot = governor.snapshot()
    assert snapshot["rejected"]["query"]["rss_unavailable"] == 1
    assert snapshot["reserved_bytes_by_kind"]["query"] == 20
    assert snapshot["active_slots"]["query"] == 1
    lease.release()


def test_forged_lease_release_cannot_free_an_admitted_reservation():
    governor = ResourceGovernor(
        ResourceLimits.for_testing(
            total_bytes=100,
            headroom_bytes=10,
            query_bytes=80,
            max_queries=1,
        ),
        rss_bytes_getter=lambda: 0,
    )
    live_lease = governor.reserve(ResourceKind.QUERY, 80)

    ResourceLease(governor, ResourceKind.QUERY, 80).release()

    snapshot = governor.snapshot()
    assert snapshot["reserved_bytes_by_kind"]["query"] == 80
    assert snapshot["active_slots"]["query"] == 1
    with pytest.raises(ResourceExhaustedError):
        governor.reserve(ResourceKind.QUERY, 1)
    live_lease.release()


def test_mutating_public_lease_charge_cannot_corrupt_release_accounting():
    governor = ResourceGovernor(
        ResourceLimits.for_testing(
            total_bytes=100,
            headroom_bytes=10,
            query_bytes=80,
            max_queries=1,
        ),
        rss_bytes_getter=lambda: 0,
    )
    lease = governor.reserve(ResourceKind.QUERY, 80)

    lease.reserved_bytes = 1
    assert governor.snapshot()["reserved_bytes_by_kind"]["query"] == 80
    lease.release()

    snapshot = governor.snapshot()
    assert snapshot["reserved_bytes_by_kind"]["query"] == 0
    assert snapshot["active_slots"]["query"] == 0
    governor.reserve(ResourceKind.QUERY, 80)


def test_limits_reject_inconsistent_duckdb_worker_envelope():
    with pytest.raises(ValueError, match="worker memory hard limit"):
        ResourceLimits.for_testing(
            total_bytes=512,
            native_headroom_bytes=128,
            duckdb_memory_limit_bytes=900,
            worker_hard_limit_bytes=1024,
        )
