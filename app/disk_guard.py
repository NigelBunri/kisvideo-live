"""Host-level disk-capacity monitoring and backpressure for the
recording spool, independent of app/recording.py's own
MAX_LOCAL_RECORDING_BYTES backstop (which only ever bounds GARBAGE left
behind by concat/validation failures, never a pending-upload recording -
see that module's own docstring).

This box is disk-constrained (confirmed via `df -h /` at implementation
time: 81% used, 16GB free of 77GB - not a guess, rechecked immediately
before writing these thresholds). Nothing in this module ever deletes a
recording; it only:
  1. Logs a structured, severity-tiered operational event on a timer, so
     an operator tailing/shipping `docker logs` sees capacity pressure
     building before it becomes an outage.
  2. Refuses to let a NEW recording/broadcast start once free space is
     already below a safe margin (POST /streams returns 503), so an
     in-progress broadcast is never the thing that runs the disk out
     from under itself mid-stream. Already-running recordings are never
     interrupted by this check - MediaMTX's own recordDeleteAfter and
     this process's upload-then-delete pipeline are what bound their
     footprint.

Threshold sizing: a single concurrent recording is the architectural
ceiling today (INGEST_PROXY_PORTS pool size - see app/ingest_proxy.py),
and mediamtx.yml caps a single segment at recordSegmentDuration (2h).
A realistic creator-stream encode (this ingest proxy's own fixed
ultrafast x264 + 128k AAC settings - see app/ingest_proxy.py's _spawn)
runs well under 1GB/hour. CRITICAL (5GB free) leaves comfortable room
for a 2h worst-case segment plus the already-running concat/upload
working set; WARN (10GB free) is the early-warning tier an operator can
still act on before it's urgent.
"""

from __future__ import annotations

import logging
import shutil
import threading
import time
from pathlib import Path

logger = logging.getLogger(__name__)

DISK_WARN_FREE_BYTES = 10 * 1024 * 1024 * 1024  # 10GB
DISK_CRITICAL_FREE_BYTES = 5 * 1024 * 1024 * 1024  # 5GB
MONITOR_INTERVAL_SECONDS = 60


class DiskGuard:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._last_level: str = ""

    def usage(self):
        return shutil.disk_usage(self._path)

    def level(self) -> str:
        free = self.usage().free
        if free < DISK_CRITICAL_FREE_BYTES:
            return "critical"
        if free < DISK_WARN_FREE_BYTES:
            return "warn"
        return "ok"

    def is_critical(self) -> bool:
        return self.level() == "critical"

    def check_and_log(self) -> None:
        total, used, free = self.usage()
        level = self.level()
        pct_used = (used / total * 100) if total else 0.0

        # Log every tick while not fully healthy (so a persisting
        # problem keeps showing up, not just once at the transition),
        # but only log the "ok" level on the transition INTO it, to
        # avoid a healthy box spamming logs every 60s forever.
        should_log = level != "ok" or level != self._last_level
        if should_log:
            log_fn = {
                "critical": logger.critical,
                "warn": logger.warning,
                "ok": logger.info,
            }[level]
            log_fn(
                "[DiskCapacity] level=%s free_gb=%.2f used_pct=%.1f total_gb=%.2f path=%s",
                level, free / 1e9, pct_used, total / 1e9, self._path,
            )
        self._last_level = level

    def start(self) -> None:
        threading.Thread(target=self._loop, daemon=True).start()

    def _loop(self) -> None:
        while True:
            try:
                self.check_and_log()
            except Exception:
                logger.exception("[DiskCapacity] monitor raised")
            time.sleep(MONITOR_INTERVAL_SECONDS)


def _recordings_root() -> Path:
    import os
    return Path(os.environ.get("RECORDINGS_ROOT", "/recordings"))


disk_guard = DiskGuard(_recordings_root())
