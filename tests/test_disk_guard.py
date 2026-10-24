"""Tests for app/disk_guard.py - host-level disk-capacity monitoring and
backpressure for the recording spool. shutil.disk_usage is mocked
throughout - these tests must never depend on the real test runner's
actual disk state."""

from __future__ import annotations

import shutil
from unittest.mock import patch

import pytest

from app.disk_guard import DiskGuard, DISK_CRITICAL_FREE_BYTES, DISK_WARN_FREE_BYTES


def _usage(total_gb: float, free_gb: float):
    total = int(total_gb * 1e9)
    free = int(free_gb * 1e9)
    used = total - free
    # shutil.disk_usage returns a namedtuple accessed by attribute
    # (.total/.used/.free), not just positionally - match that shape
    # exactly rather than a plain tuple, so these tests fail if
    # production code ever stops using attribute access too.
    return shutil._ntuple_diskusage(total, used, free)


@pytest.fixture
def guard(tmp_path):
    return DiskGuard(tmp_path)


class TestLevel:
    def test_ok_when_well_above_both_thresholds(self, guard):
        with patch("app.disk_guard.shutil.disk_usage", return_value=_usage(77, 30)):
            assert guard.level() == "ok"
            assert guard.is_critical() is False

    def test_warn_below_warn_threshold_but_above_critical(self, guard):
        with patch("app.disk_guard.shutil.disk_usage", return_value=_usage(77, 8)):
            assert guard.level() == "warn"
            assert guard.is_critical() is False

    def test_critical_below_critical_threshold(self, guard):
        with patch("app.disk_guard.shutil.disk_usage", return_value=_usage(77, 3)):
            assert guard.level() == "critical"
            assert guard.is_critical() is True

    def test_boundary_values_are_exclusive_critical(self, guard):
        # Exactly at the critical threshold is NOT yet critical - only
        # strictly below it is, matching check_and_log's own `<` test.
        exact = DISK_CRITICAL_FREE_BYTES
        total = int(77e9)
        with patch("app.disk_guard.shutil.disk_usage", return_value=shutil._ntuple_diskusage(total, total - exact, exact)):
            assert guard.level() == "warn"


class TestCheckAndLog:
    def test_logs_critical_at_critical_level(self, guard):
        with patch("app.disk_guard.shutil.disk_usage", return_value=_usage(77, 3)), \
             patch("app.disk_guard.logger.critical") as mock_critical:
            guard.check_and_log()
        mock_critical.assert_called_once()
        assert "[DiskCapacity]" in mock_critical.call_args[0][0]

    def test_logs_warning_at_warn_level(self, guard):
        with patch("app.disk_guard.shutil.disk_usage", return_value=_usage(77, 8)), \
             patch("app.disk_guard.logger.warning") as mock_warning:
            guard.check_and_log()
        mock_warning.assert_called_once()

    def test_healthy_box_only_logs_once_on_transition_into_ok(self, guard):
        with patch("app.disk_guard.shutil.disk_usage", return_value=_usage(77, 30)), \
             patch("app.disk_guard.logger.info") as mock_info:
            guard.check_and_log()  # first call: transition into "ok" -> logs once
            guard.check_and_log()  # still "ok": must NOT log again
        assert mock_info.call_count == 1

    def test_persisting_critical_condition_keeps_logging_every_tick(self, guard):
        with patch("app.disk_guard.shutil.disk_usage", return_value=_usage(77, 3)), \
             patch("app.disk_guard.logger.critical") as mock_critical:
            guard.check_and_log()
            guard.check_and_log()
            guard.check_and_log()
        assert mock_critical.call_count == 3, "an unresolved critical condition must keep alerting, not go silent after the first tick"


class TestIsCriticalGate:
    def test_matches_the_actual_production_figures_at_implementation_time(self, guard):
        # 81% used / 16GB free of 77GB - the real, rechecked production
        # numbers this module's thresholds were sized against. 16GB is
        # comfortably above the 10GB WARN threshold, so the real box is
        # "ok" by this module's definition today - new broadcasts are
        # NOT refused at the current measured state, only once free
        # space actually drops below 10GB (warn) / 5GB (critical).
        with patch("app.disk_guard.shutil.disk_usage", return_value=_usage(77, 16)):
            assert guard.level() == "ok"
            assert guard.is_critical() is False
