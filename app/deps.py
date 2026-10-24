import hmac
import logging

from fastapi import Header, HTTPException, status

from app.config.settings import settings

logger = logging.getLogger(__name__)


def require_internal_auth(x_internal_auth: str = Header(default="")) -> None:
    """Same shared-secret scheme as kisvideo's app/api/deps.py — the only
    caller of this service's API is Django (KisVideoLiveProvider in
    live_stream_providers.py), so one bare token comparison is the right
    scope, matching what's already proven for the VOD-side service rather
    than inventing a second auth convention for the same service family."""
    if not settings.internal_token:
        logger.error("require_internal_auth: service has no internal_token configured")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service is not configured with an internal token.",
        )
    if not x_internal_auth or not hmac.compare_digest(x_internal_auth, settings.internal_token):
        logger.warning("require_internal_auth: rejected request (header_present=%s)", bool(x_internal_auth))
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or missing internal auth token.")


def require_hook_secret(x_hook_secret: str = Header(default="")) -> None:
    """Guards /hooks/ready and /hooks/not-ready — the caller here is
    MediaMTX's own runOnAvailable/runOnUnavailable curl commands (see
    mediamtx.yml), not Django, so this is a separate secret from
    require_internal_auth above rather than reusing it."""
    if not settings.hook_shared_secret:
        logger.error("require_hook_secret: service has no hook_shared_secret configured")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service is not configured with a hook shared secret.",
        )
    if not x_hook_secret or not hmac.compare_digest(x_hook_secret, settings.hook_shared_secret):
        logger.warning("require_hook_secret: rejected request (header_present=%s)", bool(x_hook_secret))
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or missing hook secret.")
