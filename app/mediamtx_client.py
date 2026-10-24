"""Thin client for MediaMTX's own HTTP control API (mediamtx.yml's
apiAddress) — used only by DELETE /streams/{id} to forcibly disconnect a
publisher. Endpoint shapes below were read directly from the real
bluenviron/mediamtx v1.21.1 openapi.yaml (2026-09-22), not guessed —
notably the kick endpoints are /v3/rtmp/conns/kick/{id} and
/v3/webrtc/sessions/kick/{id} (nested conns/sessions segments), not the
flatter /v3/rtmpconns/kick/{id} shape a couple of secondary sources
suggested; verify against that repo's api/openapi.yaml again if this ever
breaks against a future MediaMTX release.
"""

from __future__ import annotations

import logging
from typing import Optional

import httpx

from app.config.settings import settings
from app.redact import redact_stream_id

logger = logging.getLogger(__name__)


def _source_for_path(client: httpx.Client, path_name: str) -> Optional[dict]:
    resp = client.get(f"{settings.mediamtx_api_url}/v3/paths/get/{path_name}")
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return (resp.json() or {}).get("source")


def _find_conn_id(client: httpx.Client, list_path: str, path_name: str) -> Optional[str]:
    # No path= filter exists on MediaMTX's list endpoints (only page/
    # itemsPerPage) — fine at v1's scale (a handful of concurrent
    # streams), but would need real pagination if that ever changes.
    resp = client.get(f"{settings.mediamtx_api_url}{list_path}", params={"itemsPerPage": 1000})
    resp.raise_for_status()
    for item in (resp.json() or {}).get("items", []):
        if item.get("path") == path_name:
            return item.get("id")
    return None


def kick_publisher(path_name: str) -> bool:
    """Disconnects whichever RTMP or WHIP publisher is currently sending
    to path_name, if any. Best-effort: returns False (never raises) on
    any failure, since a caller (DELETE /streams/{id}) shouldn't fail the
    whole stream-teardown just because the publisher had already
    disconnected on its own."""
    try:
        with httpx.Client(timeout=10) as client:
            source = _source_for_path(client, path_name)
            if not source:
                return True  # nothing publishing — nothing to kick
            source_type = source.get("type")
            if source_type == "rtmpConn":
                conn_id = _find_conn_id(client, "/v3/rtmp/conns/list", path_name)
                if not conn_id:
                    return True
                client.post(f"{settings.mediamtx_api_url}/v3/rtmp/conns/kick/{conn_id}").raise_for_status()
                return True
            if source_type == "webRTCSession":
                session_id = _find_conn_id(client, "/v3/webrtc/sessions/list", path_name)
                if not session_id:
                    return True
                client.post(f"{settings.mediamtx_api_url}/v3/webrtc/sessions/kick/{session_id}").raise_for_status()
                return True
            # Some other source type (unexpected for this service — only
            # RTMP and WHIP ingest are wired) — nothing this client knows
            # how to kick.
            logger.warning("kick_publisher: unhandled source type %r for path %s", source_type, redact_stream_id(path_name))
            return False
    except Exception:
        logger.warning("kick_publisher failed for path %s", redact_stream_id(path_name), exc_info=True)
        return False
