from __future__ import annotations

import logging
import secrets
from typing import List

import httpx
from fastapi import Depends, FastAPI, HTTPException, Query
from pydantic import BaseModel

from app.config.capacity import CapacityConfigError, capacity_settings
from app.config.settings import settings
from app.deps import require_hook_secret, require_internal_auth
from app.disk_guard import disk_guard
from app.ingest_proxy import ingest_proxy_manager
from app.mediamtx_client import kick_publisher
from app.recording import reconciliation_sweep, recording_manager
from app.redact import redact_stream_id
from app.relay import Target, relay_manager

# Uvicorn's default logging config only sets up its own "uvicorn"/
# "uvicorn.access" loggers at INFO - it never touches the root logger, so
# every logger.info() call in this app's own modules (main.py, relay.py,
# mediamtx_client.py) was silently dropped before reaching any handler
# (root defaults to WARNING with no handler attached). This is what
# actually turns those calls into visible `docker logs` output.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger(__name__)

app = FastAPI(title="KIS Video Live Service")


@app.on_event("startup")
def _validate_capacity_config() -> None:
    # Fail fast and loud: an invalid/contradictory capacity configuration
    # (e.g. DISK_WARN_FREE_BYTES <= DISK_CRITICAL_FREE_BYTES, an empty
    # INGEST_PROXY_PORTS) must stop this process from ever serving
    # traffic, not surface later as confusing runtime behavior (every
    # broadcast mysteriously 503ing, or the disk guard never alerting).
    # Raising here propagates out of FastAPI's startup sequence and
    # prevents uvicorn from binding - confirmed this is how FastAPI
    # startup-event failures behave (the ASGI lifespan protocol's own
    # startup-failure contract, not an assumption).
    try:
        capacity_settings.validate()
    except CapacityConfigError:
        logger.critical("FATAL: invalid capacity configuration - refusing to start", exc_info=True)
        raise


@app.on_event("startup")
def _start_background_workers() -> None:
    # Recovery for recording sessions left in an intermediate state by a
    # PREVIOUS process instance (crash/restart) - see
    # app/recording.py::ReconciliationSweep's own docstring. Runs for
    # the lifetime of this process, same as ingest_proxy_manager's own
    # per-stream supervisor threads.
    reconciliation_sweep.start()
    # Independent host-disk capacity monitor/alerting loop - see
    # app/disk_guard.py's own docstring for why this is separate from
    # the reconciliation sweep (different concern: host filesystem
    # headroom, not recording-session state).
    disk_guard.start()


@app.on_event("shutdown")
def _drain_active_broadcasts() -> None:
    # A container restart/redeploy (docker compose up --force-recreate,
    # or a plain `docker stop`) sends SIGTERM to this process. Uvicorn's
    # own ASGI lifespan shutdown runs this handler before the process
    # exits - releasing every still-held ingest-proxy reservation here
    # makes the shutdown-time logs honest about what's being torn down.
    # This does NOT attempt to let an active broadcast finish gracefully
    # (there is no safe way to "pause" a live RTMP/WHIP publish
    # mid-stream) - it bounds how long shutdown waits via
    # shutdown_drain_timeout_seconds so this process never outlasts the
    # orchestrator's own SIGTERM->SIGKILL grace period, then tears down
    # cleanly. See app/ingest_proxy.py::IngestProxyManager.shutdown's own
    # docstring for why no cross-restart state recovery is needed - a
    # fresh process starts with an empty, correct reservation table
    # regardless.
    logger.info("shutdown: draining active ingest-proxy reservations (timeout=%.1fs)", capacity_settings.shutdown_drain_timeout_seconds)
    ingest_proxy_manager.shutdown(capacity_settings.shutdown_drain_timeout_seconds)


def _scheme(port_is_tls: bool) -> str:
    return "https" if port_is_tls else "http"


def _urls_for_path(path: str, ingest_port: int) -> dict:
    web_scheme = _scheme(settings.public_uses_tls)
    return {
        # Broadcaster-facing: the ingest PROXY's port, not MediaMTX's own
        # 1935 - see app/ingest_proxy.py for why every publisher now goes
        # through a per-stream ffmpeg re-encode before MediaMTX ever sees
        # the stream (MediaMTX's own H264 track processing - shared by
        # every output protocol - has a hardcoded cap on consecutive
        # B-frames; some encoders exceed it and crash every consumer of
        # the stream no matter where the read happens, confirmed via a
        # real OBS test, not assumed). "live" here is just the RTMP app
        # name OBS's "Server" field needs - distinct from `path`, which
        # is MediaMTX's own internal path name once the clean result
        # reaches it.
        "ingest_url": f"rtmp://{settings.public_host}:{ingest_port}/live",
        "whip_url": f"{web_scheme}://{settings.public_host}:{settings.webrtc_port}/{path}/whip",
        # Viewer-facing: unchanged, MediaMTX's normal address - it only
        # ever receives the already-clean, zero-B-frame result now, so
        # the HLS muxer never again sees the frame pattern it can't
        # handle, regardless of what the broadcaster's own encoder sent.
        "playback_url": f"{web_scheme}://{settings.public_host}:{settings.hls_port}/{path}/index.m3u8",
    }


@app.get("/health")
def health() -> dict:
    # Liveness only - "is this process up at all," never "can it accept
    # a new broadcast right now." A service can be perfectly alive while
    # every ingest-proxy port is occupied or disk is critical - that is
    # GET /capacity's question, not this one. Conflating the two would
    # make a container orchestrator restart a perfectly healthy process
    # just because it's at capacity, which would be actively harmful
    # (killing whatever broadcast IS currently using that capacity).
    return {"status": "ok", "service": "kisvideo-live"}


class CapacityStatusResponse(BaseModel):
    max_concurrent_broadcasts: int
    active_broadcasts: int
    available_slots: int
    active_recordings: int
    disk_level: str
    disk_free_gb: float
    disk_used_pct: float
    measured_cpu_fraction_per_broadcast: float
    measured_memory_mb_per_broadcast: int
    accepting_new_broadcasts: bool


@app.get("/capacity", dependencies=[Depends(require_internal_auth)])
def capacity_status() -> CapacityStatusResponse:
    """Readiness-for-new-work, distinct from GET /health's liveness
    check (see that endpoint's own docstring). An operator (or Django,
    before even trying POST /streams) can call this to see exactly why
    admission would fail right now - insufficient port capacity vs. disk
    pressure vs. neither - without SSHing in to read container logs.
    Internal-auth-gated, same trust boundary as POST/DELETE /streams,
    since active_broadcasts/disk figures are operational detail, not
    public information."""
    total, used, free = disk_guard.usage()
    active = ingest_proxy_manager.active_count()
    return CapacityStatusResponse(
        max_concurrent_broadcasts=capacity_settings.max_concurrent_broadcasts,
        active_broadcasts=active,
        available_slots=max(capacity_settings.max_concurrent_broadcasts - active, 0),
        active_recordings=recording_manager.active_count(),
        disk_level=disk_guard.level(),
        disk_free_gb=round(free / 1e9, 2),
        disk_used_pct=round((used / total * 100) if total else 0.0, 1),
        measured_cpu_fraction_per_broadcast=capacity_settings.measured_cpu_fraction_per_broadcast,
        measured_memory_mb_per_broadcast=capacity_settings.measured_memory_mb_per_broadcast,
        accepting_new_broadcasts=(not disk_guard.is_critical()) and active < capacity_settings.max_concurrent_broadcasts,
    )


class CreateStreamResponse(BaseModel):
    provider: str
    provider_stream_id: str
    ingest_url: str
    whip_url: str
    playback_url: str
    stream_key: str


@app.post("/streams", dependencies=[Depends(require_internal_auth)])
def create_stream() -> CreateStreamResponse:
    # Disk backpressure first, before even touching the ingest-proxy
    # port pool: a broadcast that's allowed to start but then runs the
    # host out of disk mid-stream is a worse failure mode (a corrupted
    # or truncated recording, possibly alongside every other container
    # on this box) than an honest, immediate 503 at request time - see
    # app/disk_guard.py's own docstring for how this threshold was sized.
    # Checked and rejected BEFORE port allocation specifically so a
    # disk-pressure rejection and a capacity-exhaustion rejection are
    # distinguishable by which check ran, not just both collapsed into
    # the same generic 503 (see the two distinct detail strings below).
    if disk_guard.is_critical():
        logger.error("POST /streams -> refused, disk capacity critical")
        raise HTTPException(status_code=503, detail="Insufficient disk capacity to start a new recording right now.")
    # The path name IS the stream key — MediaMTX needs no pre-registration
    # (mediamtx.yml's `all_others` wildcard), so allocating one here is
    # just picking an unguessable name, not calling out to MediaMTX at all.
    # "live/" prefix keeps this service's own paths visually distinct if
    # this MediaMTX instance is ever shared with other path config later.
    key = secrets.token_urlsafe(24)
    path = f"live/{key}"
    port = ingest_proxy_manager.allocate(path, key)
    if port is None:
        logger.error("POST /streams -> no free ingest proxy port (pool exhausted)")
        raise HTTPException(status_code=503, detail="No ingest capacity available right now - try again shortly.")
    urls = _urls_for_path(path, port)
    logger.info("POST /streams -> created path=%s port=%s", redact_stream_id(path), port)
    return CreateStreamResponse(
        provider="kisvideo",
        provider_stream_id=path,
        stream_key=key,
        **urls,
    )


@app.delete("/streams/{stream_id:path}", dependencies=[Depends(require_internal_auth)])
def delete_stream(stream_id: str) -> dict:
    logger.info("DELETE /streams/%s", redact_stream_id(stream_id))
    relay_manager.stop_all(stream_id)
    ingest_proxy_manager.release(stream_id)
    kicked = kick_publisher(stream_id)
    logger.info("DELETE /streams/%s kicked=%s", redact_stream_id(stream_id), kicked)
    return {"kicked": kicked}


class TargetIn(BaseModel):
    platform: str
    rtmp_url: str
    stream_key: str = ""


class SyncTargetsRequest(BaseModel):
    targets: List[TargetIn]


@app.put("/streams/{stream_id:path}/targets", dependencies=[Depends(require_internal_auth)])
def sync_targets(stream_id: str, body: SyncTargetsRequest) -> dict:
    targets = [Target(platform=t.platform, rtmp_url=t.rtmp_url, stream_key=t.stream_key) for t in body.targets]
    logger.info(
        "PUT /streams/%s/targets platforms=%s",
        redact_stream_id(stream_id), [t.platform for t in targets],
    )
    relay_manager.sync(stream_id, targets)
    return {"synced": len(targets)}


def _notify_django(provider_stream_id: str, live_status: str, recording_status: str = "") -> None:
    if not settings.django_webhook_url:
        logger.warning("django_webhook_url not configured — dropping %s event for %s", live_status, redact_stream_id(provider_stream_id))
        return
    logger.info(
        "notifying Django: status=%s recording_status=%s path=%s url=%s",
        live_status, recording_status or "(unchanged)", redact_stream_id(provider_stream_id), settings.django_webhook_url,
    )
    payload = {"provider_stream_id": provider_stream_id, "status": live_status, "viewer_count": None}
    # Optional - ChannelLiveStreamWebhookView only applies this key when
    # present (see that view's own comment on why a stale/duplicate event
    # is never allowed to regress an already-terminal ready/failed
    # recording_status). Omitted entirely for calls that have no opinion
    # on recording state, rather than sending an empty string the
    # receiving end would have to treat as "no-op" by convention.
    if recording_status:
        payload["recording_status"] = recording_status
    try:
        resp = httpx.post(
            settings.django_webhook_url,
            json=payload,
            headers={"X-Live-Webhook-Secret": settings.django_webhook_secret},
            timeout=10,
        )
        resp.raise_for_status()
        logger.info(
            "Django notified ok: status=%s path=%s django_response=%s",
            live_status, redact_stream_id(provider_stream_id), resp.status_code,
        )
    except Exception:
        # Best-effort — MediaMTX's runOnAvailable/runOnUnavailable hooks
        # don't retry a failed command for us in any useful way, and a
        # missed webhook is recoverable (the broadcaster's own
        # start/stop actions in LiveControlRoom.tsx also update status via
        # the normal authenticated API, this is just the passive path).
        logger.warning("failed to notify Django of %s for %s", live_status, redact_stream_id(provider_stream_id), exc_info=True)


@app.post("/hooks/ready", dependencies=[Depends(require_hook_secret)])
def hook_ready(path: str = Query(...)) -> dict:
    """MediaMTX's runOnAvailable fires when a publisher starts sending on
    a path — see mediamtx.yml's paths.all_others.runOnAvailable, which
    curls this with ?path=$MTX_PATH. Maps to KIS's "live" status.

    The publisher MediaMTX sees here is always app/ingest_proxy.py's own
    ffmpeg (publishing the already-clean re-encode) - OBS/the external
    encoder itself now connects to the ingest proxy's port, never to
    MediaMTX directly, so there is exactly one path per stream here
    again (no "/clean" sibling path to guard against, unlike the earlier,
    abandoned post-MediaMTX relay attempt).

    Also clears any disconnect timestamp recorded for this path (see
    app/ingest_proxy.py's mark_reconnected) - this fires on EVERY
    publisher connect, including the very first one for a brand-new
    stream (nothing to clear, harmless no-op) and a genuine reconnect
    after a drop of any length (where it matters: it keeps this stream
    out of release_stale_reservations' consideration while connected).
    Deliberately does NOT use a short timer to kill anything on
    disconnect - see app/ingest_proxy.py's own docstring for the real
    production incident that approach caused."""
    logger.info("MediaMTX hook: /hooks/ready path=%s (publisher connected)", redact_stream_id(path))
    ingest_proxy_manager.mark_reconnected(path)
    _notify_django(path, "live", recording_status="recording")
    return {"ok": True}


@app.post("/hooks/not-ready", dependencies=[Depends(require_hook_secret)])
def hook_not_ready(path: str = Query(...)) -> dict:
    """Mirrors hook_ready for runOnUnavailable (publisher stopped).

    Records the disconnect timestamp (app/ingest_proxy.py's
    mark_disconnected) - nothing more. The port/listener stay fully
    reserved and running, exactly as before any of this existed; a
    reconnect on any realistic timescale (seconds to many minutes) finds
    a live listener waiting, same as the original pre-incident design.
    Genuine, long-term abandonment (no reconnect ever) is instead
    reclaimed later by the reconciliation sweep's own
    release_stale_reservations call, on the same proven
    RECONCILE_STALE_SECONDS threshold used elsewhere in this codebase -
    see app/ingest_proxy.py's module docstring for exactly why a
    separate, short, per-disconnect timer was tried here first and
    reverted after it broke a real broadcaster's reconnect in
    production."""
    logger.info("MediaMTX hook: /hooks/not-ready path=%s (publisher disconnected)", redact_stream_id(path))
    _notify_django(path, "ended", recording_status="finalizing")
    relay_manager.stop_all(path)
    ingest_proxy_manager.mark_disconnected(path)
    # Concat/validate/upload/archive runs in a background thread - this
    # hook response must return promptly (MediaMTX is waiting on it
    # synchronously), and the whole point of recording_status=finalizing
    # above is so Django/the creator-facing UI has something honest to
    # show while app/recording.py's own multi-step pipeline runs.
    recording_manager.finalize_async(path)
    return {"ok": True}


@app.post("/hooks/record-segment-complete", dependencies=[Depends(require_hook_secret)])
def hook_record_segment_complete(
    path: str = Query(...),
    segment: str = Query(...),
) -> dict:
    """mediamtx.yml's runOnRecordSegmentComplete - fires once per finished
    recording segment (the common case is exactly one call per broadcast;
    see app/recording.py's own docstring for why recordSegmentDuration is
    sized so multi-segment broadcasts are the exception, not the rule)."""
    recording_manager.segment_complete(path, segment)
    return {"ok": True}
