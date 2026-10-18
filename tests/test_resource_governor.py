"""Behavioral tests for the process-local resource admission boundary."""

from __future__ import annotations

import array
from fractions import Fraction

import pytest

from pivot_engine.resource_governor import (
    ResourceExhaustedError,
    ResourceGovernor,
    ResourceKind,
    ResourceLease,
    ResourceLimits,
    estimate_value_bytes,
    WorkloadAccount,
    get_worker_resource_governor,
    account_for_principal,
)


def test_default_worker_governor_is_shared_within_a_process():
    assert get_worker_resource_governor() is get_worker_resource_governor()


def test_admin_health_uses_the_default_worker_governor(monkeypatch):
    from pivot_engine import admin_api

    monkeypatch.setattr(admin_api, "_resource_governor", None)
    assert admin_api.get_resource_governor() is get_worker_resource_governor()


def test_default_runtime_services_share_one_worker_governor():
    from pivot_engine.runtime.service import PivotRuntimeService

    first = PivotRuntimeService(lambda: None)
    second = PivotRuntimeService(lambda: None)
    assert first.resource_governor is second.resource_governor
    assert first._lifecycle.resource_governor is get_worker_resource_governor()


def test_runtime_rejects_controller_bound_to_another_governor():
    from types import SimpleNamespace
    from pivot_engine.runtime.service import PivotRuntimeService

    first = ResourceGovernor(ResourceLimits.for_testing(), rss_bytes_getter=lambda: 0)
    second = ResourceGovernor(ResourceLimits.for_testing(), rss_bytes_getter=lambda: 0)
    adapter = SimpleNamespace(controller=SimpleNamespace(resource_governor=second))
    service = PivotRuntimeService(lambda: adapter, resource_governor=first)
    with pytest.raises(ValueError, match="worker governor"):
        service._adapter_getter()


def test_tenant_byte_and_slot_caps_compose_with_worker_limits():
    governor = ResourceGovernor(
        ResourceLimits.for_testing(
            total_bytes=100, headroom_bytes=10, query_bytes=90,
            tenant_bytes=40, system_bytes=20, tenant_max_queries=1,
        ), rss_bytes_getter=lambda: 0,
    )
    alice = WorkloadAccount.tenant("alice")
    bob = WorkloadAccount.tenant("bob")
    with governor.reserve(ResourceKind.QUERY, 40, account=alice):
        with pytest.raises(ResourceExhaustedError) as rejected:
            governor.reserve(ResourceKind.QUERY, 1, account=alice)
        assert rejected.value.reason == "tenant_slot"
        with governor.reserve(ResourceKind.QUERY, 40, account=bob):
            with pytest.raises(ResourceExhaustedError) as rejected:
                governor.reserve(ResourceKind.CACHE, 1, account=bob)
            assert rejected.value.reason == "tenant_bytes"
            assert governor.snapshot()["reserved_bytes"] == 80
    assert governor.snapshot()["reserved_bytes"] == 0


def test_resize_releases_tenant_and_worker_bytes_and_hides_tenant_names():
    governor = ResourceGovernor(
        ResourceLimits.for_testing(total_bytes=100, headroom_bytes=10,
                                   cache_bytes=90, tenant_bytes=40),
        rss_bytes_getter=lambda: 0,
    )
    account = WorkloadAccount.tenant("sensitive-customer")
    lease = governor.reserve(ResourceKind.CACHE, 40, account=account)
    with pytest.raises(ResourceExhaustedError):
        lease.grow(1)
    lease.resize(15)
    with governor.reserve(ResourceKind.CACHE, 25, account=account):
        snapshot = governor.snapshot()
        assert snapshot["reserved_bytes"] == 40
        assert snapshot["max_tenant_reserved_bytes"] == 40
        assert "sensitive-customer" not in str(snapshot)
    lease.release()
    lease.release()
    assert governor.snapshot()["reserved_bytes"] == 0


def test_cache_failed_replacement_preserves_old_entry_and_owner():
    from pivot_engine.cache.memory_cache import MemoryCache
    from pivot_engine.resource_governor import current_workload_account

    governor = ResourceGovernor(
        ResourceLimits.for_testing(tenant_bytes=4, cache_bytes=100),
        rss_bytes_getter=lambda: 0,
    )
    cache = MemoryCache(max_bytes=100, governor=governor)
    token = current_workload_account.set(WorkloadAccount.tenant("payer"))
    try:
        assert cache.set("key", b"abc")
        assert not cache.set("key", b"abcde")
        assert cache.get("key") == b"abc"
        assert governor.snapshot()["reserved_bytes"] == 3
    finally:
        current_workload_account.reset(token)
        cache.clear()


def test_cache_zero_byte_replacement_releases_previous_account_charge():
    from pivot_engine.cache.memory_cache import MemoryCache
    from pivot_engine.resource_governor import current_workload_account

    governor = ResourceGovernor(ResourceLimits.for_testing(cache_bytes=100), rss_bytes_getter=lambda: 0)
    cache = MemoryCache(max_bytes=100, governor=governor)
    first = current_workload_account.set(WorkloadAccount.tenant("payer-a"))
    try:
        assert cache.set("last-page", b"abc")
    finally:
        current_workload_account.reset(first)
    second = current_workload_account.set(WorkloadAccount.tenant("payer-b"))
    try:
        assert cache.set("last-page", b"")
        assert cache.get("last-page") == b""
        assert cache.bytes_used == 0
        assert governor.snapshot()["reserved_bytes"] == 0
    finally:
        current_workload_account.reset(second)
        cache.clear()


def test_two_cache_instances_share_worker_and_tenant_budget():
    from pivot_engine.cache.memory_cache import MemoryCache
    from pivot_engine.resource_governor import current_workload_account

    governor = ResourceGovernor(
        ResourceLimits.for_testing(cache_bytes=100, tenant_bytes=4),
        rss_bytes_getter=lambda: 0,
    )
    first = MemoryCache(max_bytes=100, governor=governor)
    second = MemoryCache(max_bytes=100, governor=governor)
    token = current_workload_account.set(WorkloadAccount.tenant("payer-a"))
    try:
        assert first.set("one", b"abc")
        assert not second.set("two", b"de")
        assert first.get("one") == b"abc"
    finally:
        current_workload_account.reset(token)
        first.clear()
        second.clear()


def test_malformed_tenant_budget_fails_configuration():
    with pytest.raises(ValueError, match="PIVOT_TENANT_MAX_BYTES"):
        ResourceLimits.from_environment({"PIVOT_TENANT_MAX_BYTES": "unlimited"})
    with pytest.raises(ValueError, match="account byte budgets"):
        ResourceLimits.from_environment({"PIVOT_TENANT_MAX_BYTES": str(1024**3)})


def test_accounting_identity_comes_from_trusted_principal_not_data_tenant_scope():
    from types import SimpleNamespace

    principal = SimpleNamespace(attributes={
        "tenant": ["data-a", "data-b"], "accounting_tenant": "payer-a",
    })
    assert account_for_principal(principal) == WorkloadAccount.tenant("payer-a")
    assert account_for_principal(SimpleNamespace(attributes={"tenant": "data-a"})) == WorkloadAccount()
    with pytest.raises(ValueError):
        account_for_principal(SimpleNamespace(attributes={"accounting_tenant": ["a", "b"]}))
    with pytest.raises(ValueError):
        WorkloadAccount("")
    with pytest.raises(ValueError):
        WorkloadAccount("system")

    from pivot_engine.runtime.http_transport import RuntimeHttpGateway
    gateway = RuntimeHttpGateway(lambda: None)
    context = gateway._context_from_request(
        {"table": "sales", "kind": "data", "payload": {
            "accountingTenant": "attacker", "tenant": "data-b",
        }}, principal=principal,
    )
    assert context.accounting_account == WorkloadAccount.tenant("payer-a")


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


def test_estimate_value_bytes_returns_none_for_released_memoryview():
    view = memoryview(b"abc")
    view.release()

    assert estimate_value_bytes(view) is None


@pytest.mark.parametrize(
    "reported",
    [-0.5, 0.5, -1, Fraction(-1, 2), Fraction(1, 2), True, False,
     float("nan"), float("inf"), float("-inf"), "0", None, 1 + 0j],
)
@pytest.mark.parametrize("operation", ["reserve", "grow"])
def test_invalid_rss_readings_reject_without_leaking_capacity(reported, operation):
    reading = 17
    governor = ResourceGovernor(
        ResourceLimits.for_testing(), rss_bytes_getter=lambda: reading
    )
    lease = governor.reserve(ResourceKind.QUERY, 20)
    reading = reported

    with pytest.raises(ResourceExhaustedError) as raised:
        if operation == "reserve":
            governor.reserve(ResourceKind.QUERY, 1)
        else:
            lease.grow(1)

    assert raised.value.reason == "rss_unavailable"
    snapshot = governor.snapshot()
    assert snapshot["rss_bytes"] is None
    assert snapshot["high_water_rss_bytes"] == 17
    assert snapshot["rejected"]["query"]["rss_unavailable"] == 1
    assert snapshot["admitted"]["query"] == 1
    assert snapshot["reserved_bytes"] == 20
    assert snapshot["reserved_bytes_by_kind"]["query"] == 20
    assert snapshot["active_slots"]["query"] == 1
    assert lease.reserved_bytes == 20
    lease.release()
    snapshot = governor.snapshot()
    assert snapshot["reserved_bytes"] == 0
    assert snapshot["active_slots"]["query"] == 0


@pytest.mark.parametrize("reported", [0, 17, 17.0, Fraction(17, 1)])
def test_integral_real_rss_readings_allow_reservation_and_growth(reported):
    governor = ResourceGovernor(
        ResourceLimits.for_testing(), rss_bytes_getter=lambda: reported
    )

    with governor.reserve(ResourceKind.QUERY, 20) as lease:
        lease.grow(1)
        snapshot = governor.snapshot()
        assert snapshot["rss_bytes"] == reported
        assert type(snapshot["rss_bytes"]) is int
        assert snapshot["reserved_bytes"] == 21


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
