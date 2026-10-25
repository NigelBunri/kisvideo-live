"""Centralized, validated capacity configuration — the single place every
concurrency, disk, recording-size, retry, and timing limit in this
service is defined, instead of scattered as bare module-level constants
across app/ingest_proxy.py, app/recording.py, and app/disk_guard.py (the
shape this grew into through several independent, previously-separate
rounds of work — each one correct in isolation, but leaving no single
place an operator could read to answer "what are this service's actual
limits today, and which env vars change them").

This does NOT change any default value from what was already running in
production — every default below is the exact constant that module
already had. This only (a) makes each one independently overridable via
an env var, (b) validates the whole set at startup so a contradictory or
nonsensical configuration fails loudly before serving traffic rather
than producing confusing runtime behavior, and (c) gives the
capacity-sizing guide (docs/CAPACITY.md) one source of truth to point at
instead of five files.

Vertical vs. horizontal scaling, stated explicitly rather than left
implicit: every limit here is enforced via in-process Python state
(threading.Lock-guarded dicts in IngestProxyManager, a module-level
singleton RecordingManager) inside a SINGLE uvicorn worker process (see
the Dockerfile's CMD — no --workers flag, confirmed). That is the
correct, sufficient mechanism for this service's actual deployment: one
container, one process, one box (see docker-compose.prod.yml's own
comment on why this shares a 2-CPU host with Django/Nest/Postgres/Redis
rather than running on a dedicated machine). It is NOT sufficient the
moment this service is ever run as more than one process or more than
one container reaching the same MediaMTX instance — at that point
MAX_CONCURRENT_BROADCASTS must be enforced by a shared store (e.g. a
Postgres row with SELECT...FOR UPDATE, or a Redis lock) instead of this
process's own memory, because two independent processes each holding
"port 19350 is free" in their own private dict would both allocate it.
Scaling this service is a config change (raise the numbers below,
raise the matching Docker cpus/memory limits, add more ports) as long
as it stays one process on one box; scaling it across multiple
processes or machines is an architecture change this module deliberately
does not attempt to paper over.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import List

logger = logging.getLogger(__name__)


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise CapacityConfigError(f"{name}={raw!r} is not a valid number") from exc


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise CapacityConfigError(f"{name}={raw!r} is not a valid integer") from exc


def _parse_port_pool(raw: str) -> List[int]:
    ports: List[int] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            port = int(part)
        except ValueError as exc:
            raise CapacityConfigError(
                f"INGEST_PROXY_PORTS contains a non-integer entry: {part!r} (full value: {raw!r})"
            ) from exc
        if port < 1 or port > 65535:
            raise CapacityConfigError(f"INGEST_PROXY_PORTS contains an out-of-range port: {port}")
        ports.append(port)
    if not ports:
        raise CapacityConfigError("INGEST_PROXY_PORTS resolved to an empty pool - at least one port is required")
    if len(ports) != len(set(ports)):
        raise CapacityConfigError(f"INGEST_PROXY_PORTS contains duplicate ports: {raw!r}")
    return ports


class CapacityConfigError(ValueError):
    """Raised at startup for an invalid or self-contradictory capacity
    configuration - deliberately a plain ValueError subclass so FastAPI's
    startup-event failure path (which re-raises whatever the handler
    raises) surfaces this as a clear, immediate process-exit reason in
    `docker logs`, not a confusing later runtime failure."""


@dataclass
class CapacitySettings:
    # Deliberately mutable (not frozen) - tests monkeypatch individual
    # fields directly on the shared `capacity_settings` singleton (e.g.
    # `monkeypatch.setattr(capacity_settings, "reconcile_stale_seconds", 0)`),
    # matching the same per-attribute patching convention this repo's
    # tests already used for the bare module constants this class
    # replaced. Every consumer module reads `capacity_settings.<field>`
    # at the point of use (never binds the value at import time), so a
    # test's mutation is visible everywhere immediately - see
    # app/recording.py/app/disk_guard.py/app/ingest_proxy.py's own usage.
    # ── Admission control ───────────────────────────────────────────────
    # The port pool IS the concurrency limit - each port is one
    # concurrently-admittable broadcast slot (see app/ingest_proxy.py's
    # own docstring for why one port == one real-time ffmpeg encode).
    # max_concurrent_broadcasts is derived from the pool, not set
    # independently, specifically so the two numbers can never silently
    # drift apart - see validate() below for the alternative this
    # rejects (an explicit override smaller than the pool would just be
    # confusing; larger than the pool is impossible, since the pool is
    # literally where ports come from).
    ingest_proxy_ports: List[int] = field(default_factory=lambda: [19350])

    @property
    def max_concurrent_broadcasts(self) -> int:
        return len(self.ingest_proxy_ports)

    # How long an ingest-proxy port stays reserved after MediaMTX reports
    # the publisher disconnected (runOnUnavailable) before this service
    # auto-releases it back to the pool - see app/ingest_proxy.py's
    # schedule_release/cancel_scheduled_release. Long enough to cover a
    # real OBS auto-reconnect after a brief network blip; short enough
    # that an ungraceful disconnect (crash, app killed, creator simply
    # closes OBS without tapping "End Stream") doesn't permanently starve
    # a one-port pool. 8s was chosen as comfortably longer than a typical
    # TCP reconnect/renegotiation, comfortably shorter than a human
    # noticing and manually intervening.
    ingest_reconnect_grace_seconds: float = 8.0

    # ── Disk capacity (app/disk_guard.py) ───────────────────────────────
    disk_warn_free_bytes: int = 10 * 1024 * 1024 * 1024
    disk_critical_free_bytes: int = 5 * 1024 * 1024 * 1024
    disk_monitor_interval_seconds: int = 60

    # ── Recording pipeline (app/recording.py) ───────────────────────────
    finalize_grace_seconds: float = 4.0
    upload_timeout_seconds: float = 300.0
    max_local_recording_bytes: int = 3 * 1024 * 1024 * 1024
    retry_critical_after_seconds: int = 24 * 60 * 60
    # Bounded, in-attempt retry for a single upload - distinct from (and
    # much shorter than) the outer sweep-level retention in
    # retry_pending_uploads, which deliberately has NO maximum (a real
    # recording is never given up on just because enough sweep passes
    # failed - see that method's own docstring). This bound exists so a
    # single transient blip (a dropped connection mid-PUT, a momentary
    # 5xx) doesn't make a recording wait a full
    # reconcile_interval_seconds (minutes) before even trying again, when
    # a few seconds of backoff would have resolved it. Only applies to
    # errors classified as retryable (connection errors, timeouts, 5xx) -
    # see _upload's own classification; a 4xx (e.g. an expired presigned
    # URL) is never retried within the same attempt, since retrying the
    # exact same URL again cannot succeed - the sweep-level retry already
    # fetches a FRESH presigned URL on its next pass.
    upload_max_attempts: int = 3
    upload_retry_base_delay_seconds: float = 1.0

    # ── Reconciliation sweep ────────────────────────────────────────────
    reconcile_interval_seconds: int = 5 * 60
    reconcile_stale_seconds: int = 15 * 60

    # ── Graceful shutdown ────────────────────────────────────────────────
    # How long the FastAPI shutdown handler waits for in-flight recording
    # finalize/upload work to finish before the process exits anyway -
    # see app/main.py's shutdown event. Bounded, not unbounded: a
    # container orchestrator (docker compose, Lightsail) sends SIGTERM
    # then SIGKILL after its own grace period regardless of what this
    # process does, so this must stay comfortably under that external
    # timeout rather than assuming unlimited time to drain.
    shutdown_drain_timeout_seconds: float = 25.0

    # ── Documented-but-not-independently-enforced resource budget ───────
    # Not read by any admission-control code path - this is the measured
    # real-world figure (see docs/CAPACITY.md's own measurement section)
    # that JUSTIFIES why max_concurrent_broadcasts stays at 1 on THIS
    # box's 1.0 CPU limit. Exposed here (rather than left buried in a doc
    # comment) so the capacity status endpoint can report it alongside
    # the actual configured limit, making the "why" inspectable at
    # runtime, not just in prose.
    measured_cpu_fraction_per_broadcast: float = 0.88
    measured_memory_mb_per_broadcast: int = 188

    @classmethod
    def from_env(cls) -> "CapacitySettings":
        ports = _parse_port_pool(os.environ.get("INGEST_PROXY_PORTS", "19350"))
        return cls(
            ingest_proxy_ports=ports,
            ingest_reconnect_grace_seconds=_env_float("INGEST_RECONNECT_GRACE_SECONDS", 8.0),
            disk_warn_free_bytes=_env_int("DISK_WARN_FREE_BYTES", 10 * 1024 * 1024 * 1024),
            disk_critical_free_bytes=_env_int("DISK_CRITICAL_FREE_BYTES", 5 * 1024 * 1024 * 1024),
            disk_monitor_interval_seconds=_env_int("DISK_MONITOR_INTERVAL_SECONDS", 60),
            finalize_grace_seconds=_env_float("FINALIZE_GRACE_SECONDS", 4.0),
            upload_timeout_seconds=_env_float("UPLOAD_TIMEOUT_SECONDS", 300.0),
            upload_max_attempts=_env_int("UPLOAD_MAX_ATTEMPTS", 3),
            upload_retry_base_delay_seconds=_env_float("UPLOAD_RETRY_BASE_DELAY_SECONDS", 1.0),
            max_local_recording_bytes=_env_int("MAX_LOCAL_RECORDING_BYTES", 3 * 1024 * 1024 * 1024),
            retry_critical_after_seconds=_env_int("RETRY_CRITICAL_AFTER_SECONDS", 24 * 60 * 60),
            reconcile_interval_seconds=_env_int("RECONCILE_INTERVAL_SECONDS", 5 * 60),
            reconcile_stale_seconds=_env_int("RECONCILE_STALE_SECONDS", 15 * 60),
            shutdown_drain_timeout_seconds=_env_float("SHUTDOWN_DRAIN_TIMEOUT_SECONDS", 25.0),
            measured_cpu_fraction_per_broadcast=_env_float("MEASURED_CPU_FRACTION_PER_BROADCAST", 0.88),
            measured_memory_mb_per_broadcast=_env_int("MEASURED_MEMORY_MB_PER_BROADCAST", 188),
        )

    def validate(self) -> None:
        """Called once at startup (app/main.py's startup event) - raises
        CapacityConfigError with a specific, actionable message for the
        first problem found, rather than letting a bad value surface
        later as a confusing runtime symptom (e.g. every broadcast
        mysteriously failing admission, or the disk guard never
        alerting)."""
        if self.disk_critical_free_bytes <= 0:
            raise CapacityConfigError("DISK_CRITICAL_FREE_BYTES must be positive")
        if self.disk_warn_free_bytes <= self.disk_critical_free_bytes:
            raise CapacityConfigError(
                f"DISK_WARN_FREE_BYTES ({self.disk_warn_free_bytes}) must be greater than "
                f"DISK_CRITICAL_FREE_BYTES ({self.disk_critical_free_bytes}) - warn must trigger "
                "before critical, not after"
            )
        if self.disk_monitor_interval_seconds <= 0:
            raise CapacityConfigError("DISK_MONITOR_INTERVAL_SECONDS must be positive")
        if self.finalize_grace_seconds < 0:
            raise CapacityConfigError("FINALIZE_GRACE_SECONDS cannot be negative")
        if self.upload_timeout_seconds <= 0:
            raise CapacityConfigError("UPLOAD_TIMEOUT_SECONDS must be positive")
        if self.upload_max_attempts < 1:
            raise CapacityConfigError("UPLOAD_MAX_ATTEMPTS must be at least 1")
        if self.upload_retry_base_delay_seconds < 0:
            raise CapacityConfigError("UPLOAD_RETRY_BASE_DELAY_SECONDS cannot be negative")
        if self.max_local_recording_bytes <= 0:
            raise CapacityConfigError("MAX_LOCAL_RECORDING_BYTES must be positive")
        if self.retry_critical_after_seconds <= 0:
            raise CapacityConfigError("RETRY_CRITICAL_AFTER_SECONDS must be positive")
        if self.reconcile_interval_seconds <= 0:
            raise CapacityConfigError("RECONCILE_INTERVAL_SECONDS must be positive")
        if self.reconcile_stale_seconds <= self.reconcile_interval_seconds:
            raise CapacityConfigError(
                f"RECONCILE_STALE_SECONDS ({self.reconcile_stale_seconds}) must be greater than "
                f"RECONCILE_INTERVAL_SECONDS ({self.reconcile_interval_seconds}) - otherwise every "
                "sweep pass would immediately treat a currently-live stream as abandoned"
            )
        if self.ingest_reconnect_grace_seconds < 0:
            raise CapacityConfigError("INGEST_RECONNECT_GRACE_SECONDS cannot be negative")
        if self.ingest_reconnect_grace_seconds >= self.reconcile_stale_seconds:
            raise CapacityConfigError(
                f"INGEST_RECONNECT_GRACE_SECONDS ({self.ingest_reconnect_grace_seconds}) must be less than "
                f"RECONCILE_STALE_SECONDS ({self.reconcile_stale_seconds}) - the reconnect grace window is "
                "meant to resolve long before the reconciliation sweep would otherwise consider the "
                "session abandoned"
            )
        if self.shutdown_drain_timeout_seconds <= 0:
            raise CapacityConfigError("SHUTDOWN_DRAIN_TIMEOUT_SECONDS must be positive")
        if not (0 < self.measured_cpu_fraction_per_broadcast <= 8.0):
            raise CapacityConfigError(
                "MEASURED_CPU_FRACTION_PER_BROADCAST must be a plausible positive CPU-core fraction"
            )
        if self.measured_memory_mb_per_broadcast <= 0:
            raise CapacityConfigError("MEASURED_MEMORY_MB_PER_BROADCAST must be positive")
        logger.info(
            "capacity config validated: max_concurrent_broadcasts=%d ports=%s "
            "disk_warn_gb=%.1f disk_critical_gb=%.1f reconnect_grace_s=%.1f",
            self.max_concurrent_broadcasts, self.ingest_proxy_ports,
            self.disk_warn_free_bytes / 1e9, self.disk_critical_free_bytes / 1e9,
            self.ingest_reconnect_grace_seconds,
        )


capacity_settings = CapacitySettings.from_env()
