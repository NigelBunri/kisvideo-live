from __future__ import annotations

import logging
import secrets
from typing import List

import httpx
from fastapi import Depends, FastAPI, HTTPException, Query
from pydantic import BaseModel

from app.config.settings import settings
from app.deps import require_hook_secret, require_internal_auth
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
def _start_background_workers() -> None:
    # Recovery for recording sessions left in an intermediate state by a
    # PREVIOUS process instance (crash/restart) - see
    # app/recording.py::ReconciliationSweep's own docstring. Runs for
    # the lifetime of this process, same as ingest_proxy_manager's own
    # per-stream supervisor threads.
    reconciliation_sweep.start()


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
    return {"status": "ok", "service": "kisvideo-live"}


class CreateStreamResponse(BaseModel):
    provider: str
    provider_stream_id: str
    ingest_url: str
    whip_url: str
    playback_url: str
    stream_key: str


@app.post("/streams", dependencies=[Depends(require_internal_auth)])
def create_stream() -> CreateStreamResponse:
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
    abandoned post-MediaMTX relay attempt)."""
    logger.info("MediaMTX hook: /hooks/ready path=%s (publisher connected)", redact_stream_id(path))
    _notify_django(path, "live", recording_status="recording")
    return {"ok": True}


@app.post("/hooks/not-ready", dependencies=[Depends(require_hook_secret)])
def hook_not_ready(path: str = Query(...)) -> dict:
    """Mirrors hook_ready for runOnUnavailable (publisher stopped)."""
    logger.info("MediaMTX hook: /hooks/not-ready path=%s (publisher disconnected)", redact_stream_id(path))
    _notify_django(path, "ended", recording_status="finalizing")
    relay_manager.stop_all(path)
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
