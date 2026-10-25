"""Pre-MediaMTX RTMP ingest proxy — every broadcaster (OBS/external
encoder) now connects HERE first, not to MediaMTX directly. This process
re-encodes to a predictable, zero-B-frame H264/AAC shape and only THEN
republishes the clean result into MediaMTX at the normal path.

Why this exists, and why app/normalize.py (an earlier attempt at this
same problem) was wrong: MediaMTX's HLS muxer crashes on some encoders'
output with "unable to extract DTS: too many reordered frames" (confirmed
against bluenviron/mediamtx's own GitHub issue tracker - a hardcoded
library limit on consecutive B-frames, not a mediamtx.yml setting). The
first fix attempt tried re-encoding AFTER MediaMTX (reading the already-
ingested path back out over RTMP, same trick relay.py's simulcast relay
uses) - but a REAL test against a real OBS stream proved that fails too:
MediaMTX's *own* RTMP-source re-exposition of an ingested path hits the
identical "too many reordered frames" crash, because the bug lives in
MediaMTX's shared internal H264 track processing, used by every output
protocol (HLS, RTMP-out, WHIP/WebRTC-out) alike. By the time anything
reads ANY representation of the stream back out of MediaMTX, the damage
is already done - there is no MediaMTX-internal path that avoids it.

The only thing that actually works, confirmed by that same real test, is
re-encoding BEFORE MediaMTX's own H264 parsing ever sees the problematic
frame pattern at all - i.e. terminating the broadcaster's RTMP connection
in ffmpeg (a full libavformat/libavcodec demuxer, unaffected by this
MediaMTX-specific limit) and only forwarding the already-clean,
zero-B-frame result into MediaMTX.

Port pool, not a single fixed port: ffmpeg's "-listen 1" RTMP input
serves exactly one inbound connection per process/port, so each
concurrent live stream needs its own reserved port. The pool (see
app/config/capacity.py's CapacitySettings.ingest_proxy_ports, sourced
from INGEST_PROXY_PORTS) is deliberately small today - this service's
own container is capped at 1 CPU (see docker-compose.prod.yml)
specifically for ONE real-time ultrafast x264 encode; a bigger pool
would let more streams START than this box can actually encode in real
time. Grow the pool and the CPU/memory limit together, not separately -
see that file's comment for the full reasoning, and docs/CAPACITY.md for
the full sizing procedure.

Supervised, not one-shot: "-listen 1" exits once the one connection it
accepted closes - a broadcaster's encoder reconnecting (confirmed to
happen in practice: OBS's own auto-reconnect) would find a dead listener
with nothing accepting on that port anymore. Each allocated port runs
under a supervisor thread that immediately respawns a fresh listener
after the previous one exits, for as long as the stream is still
"desired" (i.e. hasn't been explicitly released via DELETE /streams OR
auto-released after a disconnect grace period - see
schedule_release/cancel_scheduled_release below).

Reservation release on disconnect — the bug this module previously had:
MediaMTX's runOnUnavailable hook (app/main.py's hook_not_ready) fires the
instant a publisher disconnects, but the ONLY thing that ever called
release() was an explicit DELETE /streams/{id} - which Django's organic
"stream ended" webhook path (ChannelLiveStreamWebhookView, triggered by
that same hook_not_ready call, one hop away) never actually calls; only
the creator's own explicit "End Stream"/"Cancel" actions in the app do
(ChannelLiveStreamEndView/ChannelLiveStreamCancelView). A broadcaster
whose OBS crashed, lost network, or who simply closed the encoder without
tapping "End Stream" in the app left its port reserved to a dead stream
forever - with a pool size of 1, exactly ONE such disconnect permanently
exhausts this service's entire capacity until a human intervenes or the
container restarts. schedule_release()/cancel_scheduled_release() below
close that gap: a disconnect starts a bounded grace timer (long enough
for the same OBS to reconnect to the same port, matching why the
supervisor respawns in the first place); a reconnect (hook_ready for the
same stream_id) cancels it; letting it expire releases the port
automatically, no explicit DELETE required.
"""

from __future__ import annotations

import logging
import subprocess
import threading
from typing import Dict, Optional, Set

from app.config.capacity import capacity_settings
from app.redact import redact_stream_id

logger = logging.getLogger(__name__)


class IngestProxyManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._ports_in_use: Dict[int, str] = {}
        self._stream_ports: Dict[str, int] = {}
        self._procs: Dict[str, subprocess.Popen] = {}
        self._desired: Set[str] = set()
        self._release_timers: Dict[str, threading.Timer] = {}

    def allocate(self, stream_id: str, key: str) -> Optional[int]:
        """Reserve a port and start this stream's supervisor. Returns the
        assigned port, or None if the whole pool is already in use (the
        caller turns that into a 503 - "no ingest capacity right now" is
        an honest answer, not something to silently paper over by, say,
        reusing a port two streams would then collide on). Atomic: the
        availability check and the reservation happen under the same
        lock acquisition, so two concurrent POST /streams calls can
        never both claim the same port - the second one simply sees it
        already in self._ports_in_use."""
        with self._lock:
            existing = self._stream_ports.get(stream_id)
            if existing is not None:
                return existing
            port = next((p for p in capacity_settings.ingest_proxy_ports if p not in self._ports_in_use), None)
            if port is None:
                return None
            self._ports_in_use[port] = stream_id
            self._stream_ports[stream_id] = port
            self._desired.add(stream_id)
        logger.info("ingest_proxy: allocated port=%s for %s", port, redact_stream_id(stream_id))
        threading.Thread(target=self._supervise, args=(stream_id, key, port), daemon=True).start()
        return port

    def port_for(self, stream_id: str) -> Optional[int]:
        with self._lock:
            return self._stream_ports.get(stream_id)

    def active_count(self) -> int:
        """Number of currently-held ports - what the capacity status
        endpoint (app/main.py's GET /capacity) reports as "in use",
        distinct from how many are merely reserved-pending-grace-release
        (still counted here deliberately: a slot in its disconnect grace
        window is NOT available for a new admission, so it must still
        read as occupied)."""
        with self._lock:
            return len(self._ports_in_use)

    def release(self, stream_id: str) -> None:
        self._cancel_release_timer(stream_id)
        with self._lock:
            self._desired.discard(stream_id)
            proc = self._procs.pop(stream_id, None)
            port = self._stream_ports.pop(stream_id, None)
            if port is not None:
                self._ports_in_use.pop(port, None)
        if proc is not None:
            self._terminate(proc)
        logger.info("ingest_proxy: released %s (port=%s)", redact_stream_id(stream_id), port)

    def schedule_release(self, stream_id: str) -> None:
        """Called from app/main.py's hook_not_ready - the publisher just
        disconnected. Does NOT release immediately (that would defeat
        the whole point of _supervise's respawn-on-reconnect design for
        a broadcaster whose encoder briefly drops and auto-reconnects to
        the exact same port/key). Starts a bounded timer instead; if
        nothing cancels it (see cancel_scheduled_release) before it
        fires, the port is released automatically - closing the
        permanent-capacity-leak gap this module's own docstring
        describes, without requiring Django to ever call DELETE
        /streams for an organic disconnect it doesn't currently react
        to that way."""
        with self._lock:
            if stream_id not in self._stream_ports:
                return  # already released, or never allocated - nothing to schedule
            existing_timer = self._release_timers.get(stream_id)
            if existing_timer is not None:
                existing_timer.cancel()
            timer = threading.Timer(
                capacity_settings.ingest_reconnect_grace_seconds,
                self._fire_scheduled_release,
                args=(stream_id,),
            )
            timer.daemon = True
            self._release_timers[stream_id] = timer
            timer.start()
        logger.info(
            "ingest_proxy: scheduled auto-release for %s in %.1fs if no reconnect",
            redact_stream_id(stream_id), capacity_settings.ingest_reconnect_grace_seconds,
        )

    def cancel_scheduled_release(self, stream_id: str) -> None:
        """Called from app/main.py's hook_ready - the publisher (re)
        connected. If a disconnect grace timer was pending for this
        exact stream_id, cancel it: the port is still genuinely in use,
        not abandoned."""
        cancelled = self._cancel_release_timer(stream_id)
        if cancelled:
            logger.info("ingest_proxy: cancelled pending auto-release for %s (reconnected)", redact_stream_id(stream_id))

    def _cancel_release_timer(self, stream_id: str) -> bool:
        with self._lock:
            timer = self._release_timers.pop(stream_id, None)
        if timer is not None:
            timer.cancel()
            return True
        return False

    def _fire_scheduled_release(self, stream_id: str) -> None:
        with self._lock:
            # The timer firing races against cancel_scheduled_release
            # clearing it - if it's already gone from the dict, a
            # reconnect won the race and this is a no-op, not a bug.
            if stream_id not in self._release_timers:
                return
            self._release_timers.pop(stream_id, None)
        logger.warning(
            "ingest_proxy: auto-releasing %s - no reconnect within %.1fs of disconnect",
            redact_stream_id(stream_id), capacity_settings.ingest_reconnect_grace_seconds,
        )
        self.release(stream_id)

    def shutdown(self, drain_timeout_seconds: float) -> None:
        """Called from app/main.py's shutdown event - releases every
        still-held port so a graceful restart doesn't leave stale
        in-memory reservations lingering for the (very short) remainder
        of this process's life. Purely defensive: this process exiting
        kills every ffmpeg child it spawned regardless, and a fresh
        process starts with empty state anyway (see
        app/config/capacity.py's own docstring on why in-memory state
        never needs cross-restart recovery for a single-process
        deployment) - this just makes the shutdown-time logs honest
        about what's being torn down instead of silently vanishing."""
        with self._lock:
            stream_ids = list(self._desired)
        for stream_id in stream_ids:
            logger.info("ingest_proxy: releasing %s for shutdown", redact_stream_id(stream_id))
            self.release(stream_id)

    def _supervise(self, stream_id: str, key: str, port: int) -> None:
        # Deliberately no lock held across _spawn/proc.wait() - both can
        # block for a long time (proc.wait() for the entire duration of
        # the broadcast), and this lock is shared across every stream's
        # supervisor thread, same reasoning as relay.py's own _terminate
        # discipline.
        while True:
            with self._lock:
                if stream_id not in self._desired:
                    return
            proc = self._spawn(stream_id, key, port)
            with self._lock:
                if stream_id not in self._desired:
                    self._terminate(proc)
                    return
                self._procs[stream_id] = proc
            proc.wait()
            with self._lock:
                if stream_id not in self._desired:
                    return
            # Falls through and loops: respawns a fresh listener
            # immediately so a broadcaster's reconnect has somewhere to
            # land, for as long as this stream is still desired (or
            # until schedule_release's grace timer expires and releases
            # it out from under this loop, at which point the next
            # desired-check above returns).

    def _spawn(self, stream_id: str, key: str, port: int) -> subprocess.Popen:
        listen_url = f"rtmp://0.0.0.0:{port}/live/{key}"
        dest_url = f"rtmp://mediamtx:1935/{stream_id}"
        cmd = [
            "ffmpeg", "-nostdin", "-loglevel", "warning",
            "-listen", "1", "-i", listen_url,
            "-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency",
            "-bf", "0",
            "-vsync", "cfr",
            "-force_key_frames", "expr:gte(t,n_forced*2)",
            "-c:a", "aac", "-b:a", "128k", "-ar", "48000",
            "-f", "flv", dest_url,
        ]
        logger.info("ingest_proxy: listening for %s on port %s", redact_stream_id(stream_id), port)
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
                    logger.warning("ingest proxy process %s did not exit after SIGKILL", proc.pid)


ingest_proxy_manager = IngestProxyManager()
