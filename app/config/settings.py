from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Same shared-secret pattern as kisvideo's own internal_token
    # (app/config/settings.py in that repo) and Django's DJANGO_INTERNAL_TOKEN
    # — Django is the only caller of this service's API, checked via the
    # X-Internal-Auth header (see app/deps.py::require_internal_auth).
    internal_token: str = ""

    # This service's own public-facing hostname(s) — used to build the
    # ingest_url/whip_url/playback_url returned from POST /streams. A single
    # host is assumed (one dedicated box terminating RTMP+WHIP+HLS), not
    # split across multiple domains, since v1 is one small server, not a
    # multi-region deployment.
    public_host: str = "localhost"
    rtmp_port: int = 1935
    webrtc_port: int = 8889
    hls_port: int = 8888
    # False in local dev (plain http/rtmp against a bare MediaMTX
    # container); true in production, where Caddy terminates TLS in front
    # of the WHIP/HLS HTTP ports (see docker-compose.prod.yml) — browsers
    # require HTTPS for WHIP, so this must be true whenever this service
    # is reachable from outside the docker network.
    public_uses_tls: bool = False

    # MediaMTX's own HTTP control API (mediamtx.yml's apiAddress) — used to
    # kick a publisher on DELETE /streams/{id}. Not the same thing as this
    # service's own API; this is the *client* config for calling MediaMTX.
    mediamtx_api_url: str = "http://mediamtx:9997"

    # Where the generic webhook contract (X-Live-Webhook-Secret header +
    # {provider_stream_id, status, viewer_count} JSON) gets POSTed —
    # ChannelLiveStreamWebhookView in backend/kis/apps/broadcasts/views.py,
    # which already falls back to this exact contract whenever a provider
    # adapter (KisVideoLiveProvider) doesn't implement Mux's
    # signature/envelope methods. See live_stream_providers.py's
    # KisVideoLiveProvider docstring for why that's intentional, not a gap.
    django_webhook_url: str = ""
    django_webhook_secret: str = ""

    # Base URL for the small number of Django endpoints app/recording.py
    # calls DIRECTLY (not via the generic webhook contract above) -
    # requesting a presigned S3 PUT and reporting archive success/failure.
    # Deliberately a separate setting rather than deriving it from
    # django_webhook_url by string-splitting - that URL's own path shape
    # (.../live-streams/webhook/kisvideo/) is a webhook-provider detail
    # that has nothing to do with this being the API's base path; tying
    # the two together would make a future webhook URL change silently
    # break recording uploads. Same X-Live-Webhook-Secret auth as the
    # webhook above - same trust boundary, same secret, see
    # app/recording.py's own request headers.
    django_api_base_url: str = ""

    # Shared secret MediaMTX's runOnAvailable/runOnUnavailable curl calls
    # send as X-Hook-Secret (see mediamtx.yml). Separate from internal_token
    # because the caller is different (MediaMTX itself, not Django) — and
    # because once Caddy exposes this service's /streams* routes publicly
    # for Django to reach from its own separate box (see Caddyfile), the
    # /hooks/* routes ride along on the same public listener unless
    # independently authenticated, even though Caddy's route matcher
    # doesn't forward them today. Defense in depth, not optional.
    hook_shared_secret: str = ""


settings = Settings()
