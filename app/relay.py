"""Simulcast relay — pushes a copy of an already-ingested stream out to
external RTMP destinations (YouTube/Twitch/Facebook/Instagram/TikTok/
custom), matching backend/kis's ChannelLiveStreamTarget model. Stream-copy
only (`-c copy`), no re-encode — v1 is explicitly passthrough, and relaying
what MediaMTX already ingested is cheap (bandwidth, not CPU) whether the
source came in over RTMP or WHIP, since MediaMTX re-exposes every ingested
path as a playable RTMP source regardless of how it was published.

State is process-local and in-memory (a dict of Popen handles), not
persisted anywhere — acceptable because Django's ChannelLiveStreamTarget
rows are the actual source of truth (see KisVideoLiveProvider.sync_targets)
and always re-PUT the full desired target list on every call, so this
service restarting just means relays get re-started on the next sync
rather than needing to recover state from disk.
"""

from __future__ import annotations

import logging
import subprocess
import threading
from dataclasses import dataclass
from typing import Dict, List

from app.config.settings import settings
from app.redact import redact_stream_id

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Target:
    platform: str
    rtmp_url: str
    stream_key: str

    @property
    def key(self) -> str:
        # Identity for diffing, not a display value — if the same
        # platform+rtmp_url gets a new stream_key, that's treated as a
        # different target (old relay stopped, new one started) rather
        # than an in-place update, since ffmpeg has no way to swap a
        # running process's output URL.
        return f"{self.platform}|{self.rtmp_url}|{self.stream_key}"

    @property
    def publish_url(self) -> str:
        base = self.rtmp_url.rstrip("/")
        return f"{base}/{self.stream_key}" if self.stream_key else base


class RelayManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._processes: Dict[str, Dict[str, subprocess.Popen]] = {}

    def sync(self, stream_id: str, targets: List[Target]) -> None:
        # Dict mutation and process spawn happen under the lock (both are
        # fast - Popen only forks+execs, it doesn't block on the child).
        # Termination of removed targets does NOT happen under the lock -
        # _stop can block up to ~10s (terminate, wait, kill, wait again),
        # and this lock is shared across every stream_id, not per-stream -
        # holding it through that wait would stall every other
        # broadcaster's concurrent sync/stop_all call behind one slow
        # teardown.
        desired = {t.key: t for t in targets}
        to_stop: List[subprocess.Popen] = []
        logger.info(
            "relay.sync: stream=%s desired_targets=%s",
            redact_stream_id(stream_id), [t.platform for t in targets],
        )
        with self._lock:
            running = self._processes.setdefault(stream_id, {})

            for key in list(running):
                if key not in desired:
                    proc = running.pop(key, None)
                    if proc is not None:
                        to_stop.append(proc)

            for key, target in desired.items():
                if key in running and running[key].poll() is None:
                    continue  # already running and alive
                if key in running:
                    running.pop(key, None)  # dead process, restart below
                running[key] = self._start(stream_id, target)

        for proc in to_stop:
            self._terminate(proc)

    def stop_all(self, stream_id: str) -> None:
        with self._lock:
            running = self._processes.pop(stream_id, {})
        logger.info("relay.stop_all: stream=%s stopping %d relay(s)", redact_stream_id(stream_id), len(running))
        for proc in running.values():
            self._terminate(proc)

    def _start(self, stream_id: str, target: Target) -> subprocess.Popen:
        # MediaMTX re-exposes every ingested path (regardless of whether
        # it arrived over RTMP or WHIP) as a playable RTMP source at its
        # own rtmpAddress — reading from mediamtx:1935 rather than
        # re-pulling from the original publisher keeps this relay
        # protocol-agnostic and lets it live entirely inside the
        # docker-compose network (see mediamtx_api_url/rtmp host naming
        # in docker-compose.yml).
        source_url = f"rtmp://mediamtx:1935/{stream_id}"
        cmd = [
            "ffmpeg", "-i", source_url,
            "-c", "copy", "-f", "flv", target.publish_url,
        ]
        logger.info("relay: starting %s -> %s", redact_stream_id(stream_id), target.platform)
        return subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def _terminate(self, proc: subprocess.Popen) -> None:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    # SIGKILL should be unrefusable - if the process still
                    # hasn't died, something is very wrong at the OS level
                    # (e.g. stuck in uninterruptible sleep). Don't crash
                    # the caller over an orphaned ffmpeg process; it's
                    # already been dropped from `running` either way.
                    logger.warning("relay process %s did not exit after SIGKILL", proc.pid)


relay_manager = RelayManager()
