"""Tests for app/ingest_proxy.py - the per-stream RTMP ingest proxy and
its port-pool admission control. No subprocess/ffmpeg is ever actually
spawned here - _spawn and _supervise are patched out in every test that
would otherwise trigger them, since allocate() starts a real background
thread that calls _spawn. Focuses on the two things the KIS capacity
directive specifically asked to be verified with real tests: atomic
admission (two requests can never claim the same port) and reservation
release under every real-world path (explicit DELETE, disconnect grace
expiry, reconnect cancellation, shutdown drain) - not just the happy
path this module already had a human-reasoned docstring for.
"""

from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from app.config.capacity import capacity_settings
from app.ingest_proxy import IngestProxyManager


@pytest.fixture
def manager(monkeypatch):
    # Every test gets a fresh manager AND a short, deterministic grace
    # period - the real default (8s) would make the release-related
    # tests slow without adding any real coverage.
    monkeypatch.setattr(capacity_settings, "ingest_proxy_ports", [19350, 19351])
    monkeypatch.setattr(capacity_settings, "ingest_reconnect_grace_seconds", 0.05)
    m = IngestProxyManager()
    # _supervise spawns a real background thread per allocate() call that
    # would otherwise try to exec a real ffmpeg - patch it to a no-op so
    # tests only exercise the admission-control bookkeeping itself.
    with patch.object(IngestProxyManager, "_supervise", lambda self, *a, **k: None):
        yield m


class TestAtomicAllocation:
    def test_allocate_returns_a_port_from_the_pool(self, manager):
        port = manager.allocate("live/a", "keya")
        assert port in (19350, 19351)

    def test_second_distinct_stream_gets_a_different_port(self, manager):
        port1 = manager.allocate("live/a", "keya")
        port2 = manager.allocate("live/b", "keyb")
        assert port1 != port2
        assert {port1, port2} == {19350, 19351}

    def test_pool_exhaustion_returns_none_not_a_colliding_reuse(self, manager):
        manager.allocate("live/a", "keya")
        manager.allocate("live/b", "keyb")

        third = manager.allocate("live/c", "keyc")

        assert third is None

    def test_repeated_allocate_for_the_same_stream_id_returns_the_same_port(self, manager):
        # A duplicate POST /streams for a stream that already has a port
        # (e.g. a retried request) must be idempotent, not steal a
        # second slot out of the pool for what is really one broadcast.
        first = manager.allocate("live/a", "keya")
        second = manager.allocate("live/a", "keya")
        assert first == second

    def test_two_concurrent_requests_for_the_last_slot_never_both_win(self, manager):
        # The real concurrency property the directive asked to be
        # tested directly, not just reasoned about: fire two allocate()
        # calls for DISTINCT stream ids from separate threads at (as
        # close to) the same instant, against a pool that only has ONE
        # slot. Exactly one must succeed; the other must see exhaustion,
        # never a colliding duplicate port.
        results = [None, None]
        barrier = threading.Barrier(2)

        def attempt(idx, stream_id):
            barrier.wait()
            results[idx] = manager.allocate(stream_id, f"key{idx}")

        # Shrink the pool to exactly one port for this specific test, so
        # a race has something real to race over.
        manager._ports_in_use.clear()
        manager._stream_ports.clear()
        import app.ingest_proxy as ingest_proxy_module
        with patch.object(ingest_proxy_module, "capacity_settings") as mock_caps:
            mock_caps.ingest_proxy_ports = [19350]
            t1 = threading.Thread(target=attempt, args=(0, "live/race-a"))
            t2 = threading.Thread(target=attempt, args=(1, "live/race-b"))
            t1.start()
            t2.start()
            t1.join()
            t2.join()

        winners = [r for r in results if r is not None]
        losers = [r for r in results if r is None]
        assert len(winners) == 1, "exactly one of two racing requests must win the single slot"
        assert len(losers) == 1
        assert winners[0] == 19350


class TestRelease:
    def test_explicit_release_frees_the_port_for_reuse(self, manager):
        manager.allocate("live/a", "keya")
        manager.allocate("live/b", "keyb")  # pool now full (2 ports)

        manager.release("live/a")
        reused = manager.allocate("live/c", "keyc")

        assert reused == 19350 or reused == 19351

    def test_release_is_idempotent_safe_to_call_twice(self, manager):
        manager.allocate("live/a", "keya")
        manager.release("live/a")
        manager.release("live/a")  # must not raise, must not corrupt state

        assert manager.port_for("live/a") is None

    def test_release_of_a_never_allocated_stream_is_a_safe_noop(self, manager):
        manager.release("live/never-existed")  # must not raise

    def test_active_count_reflects_held_ports(self, manager):
        assert manager.active_count() == 0
        manager.allocate("live/a", "keya")
        assert manager.active_count() == 1
        manager.allocate("live/b", "keyb")
        assert manager.active_count() == 2
        manager.release("live/a")
        assert manager.active_count() == 1


class TestDisconnectGraceRelease:
    """The real, previously-unverified bug this round fixed: a publisher
    disconnecting (MediaMTX's runOnUnavailable -> app/main.py's
    hook_not_ready) never released the ingest-proxy port on its own -
    only an explicit DELETE /streams did, which Django's organic
    "stream ended" webhook path never calls. With a one-port pool, a
    single ungraceful disconnect (OBS crash, app killed, creator closes
    the encoder without tapping "End Stream") permanently exhausted this
    service's entire capacity. schedule_release/cancel_scheduled_release
    close that gap with a bounded grace timer."""

    def test_scheduled_release_frees_the_port_after_the_grace_period_elapses(self, manager):
        manager.allocate("live/a", "keya")

        manager.schedule_release("live/a")
        assert manager.port_for("live/a") is not None, "must not release immediately - that would defeat reconnect tolerance"

        time.sleep(capacity_settings.ingest_reconnect_grace_seconds + 0.1)

        assert manager.port_for("live/a") is None, "must auto-release once the grace period elapses with no reconnect"

    def test_cancel_scheduled_release_prevents_the_auto_release(self, manager):
        manager.allocate("live/a", "keya")
        manager.schedule_release("live/a")

        manager.cancel_scheduled_release("live/a")
        time.sleep(capacity_settings.ingest_reconnect_grace_seconds + 0.1)

        assert manager.port_for("live/a") is not None, "a reconnect (cancel) must keep the port reserved past the original grace window"

    def test_a_genuine_reconnect_before_the_grace_window_expires_keeps_the_slot(self, manager):
        # End-to-end simulation of the real scenario: disconnect, then a
        # reconnect lands (hook_ready -> cancel_scheduled_release)
        # BEFORE the grace timer fires.
        manager.allocate("live/a", "keya")
        manager.schedule_release("live/a")
        time.sleep(capacity_settings.ingest_reconnect_grace_seconds * 0.3)  # well before expiry

        manager.cancel_scheduled_release("live/a")
        time.sleep(capacity_settings.ingest_reconnect_grace_seconds + 0.1)

        assert manager.port_for("live/a") is not None

    def test_scheduling_release_for_an_unallocated_stream_is_a_safe_noop(self, manager):
        manager.schedule_release("live/never-existed")  # must not raise
        time.sleep(capacity_settings.ingest_reconnect_grace_seconds + 0.1)
        # Nothing to assert beyond "did not raise" - there was never a
        # reservation to release.

    def test_explicit_delete_during_the_grace_window_also_cancels_the_timer(self, manager):
        # An explicit DELETE arriving while a disconnect-grace timer is
        # already pending (e.g. the creator taps "End Stream" right
        # after their encoder drops) must not leave a dangling timer
        # that fires a redundant (harmless, but untidy) release later.
        manager.allocate("live/a", "keya")
        manager.schedule_release("live/a")

        manager.release("live/a")

        assert manager._release_timers.get("live/a") is None

    def test_reallocating_after_auto_release_gets_a_fresh_port(self, manager):
        manager.allocate("live/a", "keya")
        manager.schedule_release("live/a")
        time.sleep(capacity_settings.ingest_reconnect_grace_seconds + 0.1)

        # The pool slot is free again - a brand new POST /streams (new
        # stream_id, since Django would issue a fresh key) can claim it.
        new_port = manager.allocate("live/fresh", "freshkey")
        assert new_port is not None


class TestShutdownDrain:
    def test_shutdown_releases_every_held_port(self, manager):
        manager.allocate("live/a", "keya")
        manager.allocate("live/b", "keyb")

        manager.shutdown(drain_timeout_seconds=1.0)

        assert manager.port_for("live/a") is None
        assert manager.port_for("live/b") is None
        assert manager.active_count() == 0

    def test_shutdown_with_nothing_active_is_a_safe_noop(self, manager):
        manager.shutdown(drain_timeout_seconds=1.0)  # must not raise
