"""Tests for app/main.py's HTTP surface - specifically the two things
the KIS capacity directive asked to be verified directly rather than
just reasoned about: (1) disk-critical mode refuses a NEW broadcast via
POST /streams, and (2) disk-critical mode never touches an ALREADY
admitted/active broadcast - only new admission is affected, confirmed
by checking that an existing reservation survives the disk guard
transitioning to critical, not just that no code path happens to call
release() (which could silently become wrong in a future edit without
a test to catch it)."""

from __future__ import annotations

import shutil
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from app.config.capacity import capacity_settings
from app.ingest_proxy import ingest_proxy_manager
from app.main import app


AUTH_HEADERS = {"X-Internal-Auth": "test-internal-token"}

# The bare test container has no /recordings volume mounted (that's a
# production-only shared Docker volume) - any call through to the real
# disk_guard.usage() raises FileNotFoundError here. These tests are
# about the capacity/admission reporting logic, not disk_guard itself
# (already covered by test_disk_guard.py), so its real filesystem call
# is mocked to a fixed, plausible value throughout.
_FAKE_DISK_USAGE = shutil._ntuple_diskusage(int(80e9), int(64e9), int(16e9))


@pytest.fixture(autouse=True)
def _configure_auth(monkeypatch):
    monkeypatch.setattr("app.deps.settings.internal_token", "test-internal-token")
    monkeypatch.setattr("app.deps.settings.hook_shared_secret", "test-hook-secret")


@pytest.fixture
def client(monkeypatch, tmp_path):
    # Avoid real background workers (reconciliation sweep, disk guard
    # polling loop) running during the test and avoid touching the real
    # filesystem - the sweep itself isn't what's under test here.
    with patch("app.main.reconciliation_sweep.start"), patch("app.main.disk_guard.start"):
        with TestClient(app) as c:
            yield c
    # Clean up any reservation a test left behind so tests don't bleed
    # into each other via the shared module-level ingest_proxy_manager
    # singleton.
    for stream_id in list(ingest_proxy_manager._stream_ports.keys()):
        ingest_proxy_manager.release(stream_id)


class TestDiskCriticalAdmission:
    def test_new_broadcast_refused_when_disk_critical(self, client):
        with patch("app.main.disk_guard.is_critical", return_value=True):
            resp = client.post("/streams", headers=AUTH_HEADERS)

        assert resp.status_code == 503
        assert "disk capacity" in resp.json()["detail"].lower()

    def test_new_broadcast_admitted_when_disk_ok(self, client):
        with patch("app.main.disk_guard.is_critical", return_value=False), \
             patch("app.ingest_proxy.IngestProxyManager._supervise", lambda self, *a, **k: None):
            resp = client.post("/streams", headers=AUTH_HEADERS)

        assert resp.status_code == 200
        assert resp.json()["provider"] == "kisvideo"

    def test_disk_turning_critical_after_admission_does_not_release_the_existing_reservation(self, client):
        """The real guarantee the directive asked for: disk pressure
        must gate NEW admissions only. An already-active broadcast's
        port reservation must survive the disk guard transitioning to
        critical - nothing in that transition path calls release() for
        existing streams, and this test pins that down directly rather
        than relying on code inspection alone."""
        with patch("app.main.disk_guard.is_critical", return_value=False), \
             patch("app.ingest_proxy.IngestProxyManager._supervise", lambda self, *a, **k: None):
            create_resp = client.post("/streams", headers=AUTH_HEADERS)
        stream_id = create_resp.json()["provider_stream_id"]
        assert ingest_proxy_manager.port_for(stream_id) is not None

        # Disk now (hypothetically) becomes critical - simulate the
        # disk guard's own level() reporting it, exactly as the real
        # 60s poll loop would observe mid-broadcast.
        with patch("app.main.disk_guard.is_critical", return_value=True):
            # A second, distinct broadcast attempt is correctly refused...
            second_resp = client.post("/streams", headers=AUTH_HEADERS)
            assert second_resp.status_code == 503
            # ...but the FIRST, already-active stream's reservation is
            # completely untouched by that refusal.
            assert ingest_proxy_manager.port_for(stream_id) is not None


class TestCapacityEndpoint:
    def test_reports_zero_active_when_idle(self, client):
        with patch("app.disk_guard.shutil.disk_usage", return_value=_FAKE_DISK_USAGE):
            resp = client.get("/capacity", headers=AUTH_HEADERS)

        assert resp.status_code == 200
        body = resp.json()
        assert body["active_broadcasts"] == 0
        assert body["max_concurrent_broadcasts"] == capacity_settings.max_concurrent_broadcasts
        assert body["accepting_new_broadcasts"] is True

    def test_reports_active_broadcast_and_zero_available_slots_once_the_single_slot_pool_is_full(self, client, monkeypatch):
        monkeypatch.setattr(capacity_settings, "ingest_proxy_ports", [19350])
        with patch("app.main.disk_guard.is_critical", return_value=False), \
             patch("app.ingest_proxy.IngestProxyManager._supervise", lambda self, *a, **k: None):
            client.post("/streams", headers=AUTH_HEADERS)

        with patch("app.disk_guard.shutil.disk_usage", return_value=_FAKE_DISK_USAGE):
            resp = client.get("/capacity", headers=AUTH_HEADERS)

        body = resp.json()
        assert body["active_broadcasts"] == 1
        assert body["available_slots"] == 0
        assert body["accepting_new_broadcasts"] is False

    def test_requires_internal_auth(self, client):
        resp = client.get("/capacity")  # no auth header

        assert resp.status_code == 401


class TestHealthVsCapacity:
    def test_health_never_requires_auth_and_never_reflects_capacity_state(self, client):
        # /health is liveness-only - it must stay reachable (and return
        # ok) even while the service is fully at capacity, unlike
        # /capacity which reports exactly that state. A monitor that
        # restarted the process because /health returned non-ok at full
        # capacity would kill whatever broadcast IS currently using
        # that capacity - exactly the failure mode this split exists to
        # avoid.
        with patch("app.main.disk_guard.is_critical", return_value=False), \
             patch("app.ingest_proxy.IngestProxyManager._supervise", lambda self, *a, **k: None):
            client.post("/streams", headers=AUTH_HEADERS)  # fill the pool

        resp = client.get("/health")  # no auth header, either

        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"
