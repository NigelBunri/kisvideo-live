"""Tests for app/config/capacity.py - the centralized capacity
configuration model and its startup validation. Covers env-var parsing
(_parse_port_pool) and the cross-field validation rules that must reject
a contradictory configuration before the service starts accepting
traffic (see CapacitySettings.validate's own docstring for why this
matters - a bad value here must fail loudly at startup, not surface
later as confusing runtime behavior)."""

from __future__ import annotations

import pytest

from app.config.capacity import CapacityConfigError, CapacitySettings, _parse_port_pool


class TestPortPoolParsing:
    def test_single_port(self):
        assert _parse_port_pool("19350") == [19350]

    def test_multiple_comma_separated_ports(self):
        assert _parse_port_pool("19350,19351,19352") == [19350, 19351, 19352]

    def test_tolerates_whitespace_around_entries(self):
        assert _parse_port_pool(" 19350 , 19351 ") == [19350, 19351]

    def test_empty_string_is_rejected(self):
        with pytest.raises(CapacityConfigError, match="empty pool"):
            _parse_port_pool("")

    def test_non_integer_entry_is_rejected_with_the_offending_value_named(self):
        with pytest.raises(CapacityConfigError, match="19350a"):
            _parse_port_pool("19350,19350a")

    def test_out_of_range_port_is_rejected(self):
        with pytest.raises(CapacityConfigError, match="out-of-range"):
            _parse_port_pool("70000")

    def test_duplicate_ports_are_rejected(self):
        with pytest.raises(CapacityConfigError, match="duplicate"):
            _parse_port_pool("19350,19350")


class TestMaxConcurrentBroadcastsDerivation:
    def test_derived_from_port_pool_length_not_set_independently(self):
        # This is the directive's own concern made concrete: the pool
        # size and the concurrency limit must be mathematically the same
        # number, never two separately-configured values that could
        # silently drift apart.
        settings = CapacitySettings(ingest_proxy_ports=[19350, 19351, 19352])
        assert settings.max_concurrent_broadcasts == 3

    def test_single_port_pool_yields_limit_of_one(self):
        settings = CapacitySettings(ingest_proxy_ports=[19350])
        assert settings.max_concurrent_broadcasts == 1


class TestValidation:
    def _valid(self, **overrides) -> CapacitySettings:
        defaults = dict(
            ingest_proxy_ports=[19350],
            ingest_reconnect_grace_seconds=8.0,
            disk_warn_free_bytes=10 * 1024**3,
            disk_critical_free_bytes=5 * 1024**3,
            disk_monitor_interval_seconds=60,
            finalize_grace_seconds=4.0,
            upload_timeout_seconds=300.0,
            max_local_recording_bytes=3 * 1024**3,
            retry_critical_after_seconds=86400,
            reconcile_interval_seconds=300,
            reconcile_stale_seconds=900,
            shutdown_drain_timeout_seconds=25.0,
            measured_cpu_fraction_per_broadcast=0.88,
            measured_memory_mb_per_broadcast=188,
        )
        defaults.update(overrides)
        return CapacitySettings(**defaults)

    def test_valid_default_configuration_passes(self):
        self._valid().validate()  # must not raise

    def test_rejects_warn_threshold_not_greater_than_critical(self):
        with pytest.raises(CapacityConfigError, match="DISK_WARN_FREE_BYTES"):
            self._valid(disk_warn_free_bytes=5 * 1024**3, disk_critical_free_bytes=5 * 1024**3).validate()

    def test_rejects_warn_threshold_below_critical(self):
        with pytest.raises(CapacityConfigError, match="DISK_WARN_FREE_BYTES"):
            self._valid(disk_warn_free_bytes=1 * 1024**3, disk_critical_free_bytes=5 * 1024**3).validate()

    def test_rejects_zero_or_negative_critical_disk_threshold(self):
        with pytest.raises(CapacityConfigError, match="DISK_CRITICAL_FREE_BYTES"):
            self._valid(disk_critical_free_bytes=0, disk_warn_free_bytes=10 * 1024**3).validate()

    def test_rejects_non_positive_upload_timeout(self):
        with pytest.raises(CapacityConfigError, match="UPLOAD_TIMEOUT_SECONDS"):
            self._valid(upload_timeout_seconds=0).validate()

    def test_rejects_reconcile_stale_not_greater_than_interval(self):
        # If the stale threshold isn't strictly greater than the sweep
        # interval, every single sweep pass would immediately treat a
        # currently-live, still-actively-publishing stream as abandoned.
        with pytest.raises(CapacityConfigError, match="RECONCILE_STALE_SECONDS"):
            self._valid(reconcile_interval_seconds=300, reconcile_stale_seconds=300).validate()

    def test_rejects_negative_reconnect_grace(self):
        with pytest.raises(CapacityConfigError, match="INGEST_RECONNECT_GRACE_SECONDS"):
            self._valid(ingest_reconnect_grace_seconds=-1).validate()

    def test_rejects_reconnect_grace_not_less_than_reconcile_stale(self):
        # The reconnect grace window exists to resolve well before the
        # reconciliation sweep would separately consider the session
        # abandoned - if it's equal or longer, the two mechanisms race.
        with pytest.raises(CapacityConfigError, match="INGEST_RECONNECT_GRACE_SECONDS"):
            self._valid(ingest_reconnect_grace_seconds=900, reconcile_stale_seconds=900).validate()

    def test_rejects_non_positive_shutdown_drain_timeout(self):
        with pytest.raises(CapacityConfigError, match="SHUTDOWN_DRAIN_TIMEOUT_SECONDS"):
            self._valid(shutdown_drain_timeout_seconds=0).validate()

    def test_rejects_implausible_measured_cpu_fraction(self):
        with pytest.raises(CapacityConfigError, match="MEASURED_CPU_FRACTION_PER_BROADCAST"):
            self._valid(measured_cpu_fraction_per_broadcast=0).validate()

    def test_rejects_non_positive_measured_memory(self):
        with pytest.raises(CapacityConfigError, match="MEASURED_MEMORY_MB_PER_BROADCAST"):
            self._valid(measured_memory_mb_per_broadcast=0).validate()
