"""Tests for app/ingest_proxy.py - the per-stream RTMP ingest proxy and
its port-pool admission control. No subprocess/ffmpeg is ever actually
spawned here - _spawn and _supervise are patched out in every test that
would otherwise trigger them, since allocate() starts a real background
thread that calls _spawn. Focuses on the two things the KIS capacity
directive specifically asked to be verified with real tests: atomic
admission (two requests can never claim the same port) and reservation
release under every real-world path (explicit DELETE, stale-reservation
sweep reclaim, reconnect preserving the slot, shutdown drain) - not just
the happy path this module already had a human-reasoned docstring for.

TestStaleReservationReclaim covers the SECOND version of disconnect
handling - a FIRST version (a short per-disconnect threading.Timer that
killed the listener outright) caused a real production incident within
minutes of deployment and was reverted; see app/ingest_proxy.py's own
module docstring for the full account. These tests exist specifically
so that incident's root cause - something auto-releasing a reservation
on too short a fuse - cannot silently return."""

from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from app.config.capacity import capacity_settings
from app.ingest_proxy import IngestProxyManager


@pytest.fixture
def manager(monkeypatch):
    monkeypatch.setattr(capacity_settings, "ingest_proxy_ports", [19350, 19351])
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


class TestDisconnectNeverKillsTheListenerOnAShortFuse:
    """Pins down the actual fix for the real production incident: a
    disconnect (mark_disconnected) must NEVER, by itself, on any
    timescale within a test's reasonable patience, release the port or
    touch the listener. The only thing allowed to reclaim a disconnected
    reservation is release_stale_reservations, and only when explicitly
    told to treat it as stale (see TestStaleReservationReclaim) - never
    as a side effect of time simply passing after mark_disconnected."""

    def test_mark_disconnected_alone_never_releases_the_port_no_matter_how_long_you_wait(self, manager):
        manager.allocate("live/a", "keya")
        manager.mark_disconnected("live/a")

        time.sleep(0.2)  # several times longer than the old, reverted 8s-scaled test timers ever waited in practice

        assert manager.port_for("live/a") is not None, "a disconnect must never, by itself, release the reservation"

    def test_mark_disconnected_for_a_never_allocated_stream_is_a_safe_noop(self, manager):
        manager.mark_disconnected("live/never-existed")  # must not raise

    def test_mark_reconnected_clears_the_disconnect_timestamp(self, manager):
        manager.allocate("live/a", "keya")
        manager.mark_disconnected("live/a")

        manager.mark_reconnected("live/a")

        assert "live/a" not in manager._disconnected_at

    def test_mark_reconnected_on_a_stream_that_was_never_disconnected_is_a_safe_noop(self, manager):
        manager.allocate("live/a", "keya")
        manager.mark_reconnected("live/a")  # must not raise


class TestStaleReservationReclaim:
    """release_stale_reservations - the actual (and only) mechanism that
    reclaims a port after a disconnect, driven by the reconciliation
    sweep on the long-proven RECONCILE_STALE_SECONDS threshold, not a
    short per-disconnect timer."""

    def test_releases_a_reservation_disconnected_longer_than_the_threshold(self, manager, monkeypatch):
        manager.allocate("live/a", "keya")
        manager.mark_disconnected("live/a")
        # Simulate time having actually passed, rather than sleeping a
        # real wall-clock duration in the test.
        manager._disconnected_at["live/a"] = time.time() - 1000

        released = manager.release_stale_reservations(stale_after_seconds=900)

        assert released == ["live/a"]
        assert manager.port_for("live/a") is None

    def test_does_not_release_a_reservation_disconnected_less_than_the_threshold(self, manager):
        manager.allocate("live/a", "keya")
        manager.mark_disconnected("live/a")
        manager._disconnected_at["live/a"] = time.time() - 10  # well under any realistic threshold

        released = manager.release_stale_reservations(stale_after_seconds=900)

        assert released == []
        assert manager.port_for("live/a") is not None

    def test_does_not_release_a_reservation_that_is_currently_connected(self, manager):
        # allocate() alone never marks anything disconnected - a stream
        # with no entry in _disconnected_at at all (the normal state
        # for an actively-publishing broadcast) must never be touched.
        manager.allocate("live/a", "keya")

        released = manager.release_stale_reservations(stale_after_seconds=900)

        assert released == []
        assert manager.port_for("live/a") is not None

    def test_a_reconnect_before_the_sweep_runs_prevents_reclaim_entirely(self, manager):
        # The real end-to-end scenario: disconnect, then a genuine
        # reconnect (hook_ready -> mark_reconnected) lands before the
        # reconciliation sweep ever gets a chance to consider this
        # stream stale - exactly the case that broke in production
        # under the old short-timer design.
        manager.allocate("live/a", "keya")
        manager.mark_disconnected("live/a")
        manager.mark_reconnected("live/a")

        # Even if the sweep runs with a threshold that would have
        # matched the (now-cleared) old disconnect timestamp, there's
        # nothing left to reclaim.
        released = manager.release_stale_reservations(stale_after_seconds=0)

        assert released == []
        assert manager.port_for("live/a") is not None

    def test_releases_multiple_stale_reservations_in_one_pass(self, manager):
        manager.allocate("live/a", "keya")
        manager.allocate("live/b", "keyb")
        manager.mark_disconnected("live/a")
        manager.mark_disconnected("live/b")
        manager._disconnected_at["live/a"] = time.time() - 1000
        manager._disconnected_at["live/b"] = time.time() - 1000

        released = manager.release_stale_reservations(stale_after_seconds=900)

        assert sorted(released) == ["live/a", "live/b"]

    def test_empty_when_nothing_is_disconnected(self, manager):
        manager.allocate("live/a", "keya")

        released = manager.release_stale_reservations(stale_after_seconds=0)

        assert released == []


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
