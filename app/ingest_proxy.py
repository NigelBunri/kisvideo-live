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
concurrent live stream needs its own reserved port. INGEST_PROXY_PORTS
(comma-separated, env-configured) is deliberately small today - this
service's own container is capped at 1 CPU (see docker-compose.prod.yml)
specifically for ONE real-time ultrafast x264 encode; a bigger pool
would let more streams START than this box can actually encode in real
time. Grow the pool and the CPU/memory limit together, not separately -
see that file's comment for the full reasoning.

Supervised, not one-shot: "-listen 1" exits once the one connection it
accepted closes - a broadcaster's encoder reconnecting (confirmed to
happen in practice: OBS's own auto-reconnect) would find a dead listener
with nothing accepting on that port anymore. Each allocated port runs
under a supervisor thread that immediately respawns a fresh listener
after the previous one exits, for as long as the stream is still
"desired" (i.e. hasn't been explicitly released via DELETE /streams).
"""

from __future__ import annotations

import logging
import os
import subprocess
import threading
from typing import Dict, Optional, Set

from app.redact import redact_stream_id

logger = logging.getLogger(__name__)


def _load_port_pool() -> list:
    raw = os.environ.get("INGEST_PROXY_PORTS", "19350")
    ports = []
    for part in raw.split(","):
        part = part.strip()
        if part:
            ports.append(int(part))
    return ports


PORT_POOL = _load_port_pool()


class IngestProxyManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._ports_in_use: Dict[int, str] = {}
        self._stream_ports: Dict[str, int] = {}
        self._procs: Dict[str, subprocess.Popen] = {}
        self._desired: Set[str] = set()

    def allocate(self, stream_id: str, key: str) -> Optional[int]:
        """Reserve a port and start this stream's supervisor. Returns the
        assigned port, or None if the whole pool is already in use (the
        caller turns that into a 503 - "no ingest capacity right now" is
        an honest answer, not something to silently paper over by, say,
        reusing a port two streams would then collide on)."""
        with self._lock:
            existing = self._stream_ports.get(stream_id)
            if existing is not None:
                return existing
            port = next((p for p in PORT_POOL if p not in self._ports_in_use), None)
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

    def release(self, stream_id: str) -> None:
        with self._lock:
            self._desired.discard(stream_id)
            proc = self._procs.pop(stream_id, None)
            port = self._stream_ports.pop(stream_id, None)
            if port is not None:
                self._ports_in_use.pop(port, None)
        if proc is not None:
            self._terminate(proc)
        logger.info("ingest_proxy: released %s (port=%s)", redact_stream_id(stream_id), port)

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
            # land, for as long as this stream is still desired.

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
