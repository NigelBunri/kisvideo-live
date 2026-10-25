# KIS Video Live

Self-hosted live-streaming ingest — RTMP + WHIP in, HLS out, server-side
simulcast relay to external platforms — replacing Mux's live-streaming API
for KISTube. Sibling service to [`kisvideo`](../kisvideo) (which replaces
Mux for VOD only); this one is intentionally a separate repo/deploy rather
than a module in that one, for the same reason `kisvideo` is separate from
Django itself — a distinct concern with its own release cycle, not because
it needs separate hardware.

**Runs on the existing kis-production box**, as an isolated Docker Compose
stack alongside Django/Nest/Postgres/Redis/`kisvideo`(VOD) — NOT a
dedicated server. An earlier draft of this plan assumed a second machine;
that was superseded (2026-09-22) in favor of keeping the MVP simple and
cheap, after confirming the existing box has real (if not generous)
headroom — see [`docs/CAPACITY.md`](docs/CAPACITY.md) for the current,
measured capacity model (this used to be a "Capacity assumptions"
section in this file; it moved there once it grew past a README-sized
note, and because it now documents real measured numbers rather than an
MVP-stage assumption).

**Live in production**: broadcaster RTMP/WHIP ingest, a real-time
per-stream re-encode (`app/ingest_proxy.py`) to work around a MediaMTX
HLS-muxer crash on some encoders' output, HLS/WHIP playback, automatic
server-side recording (`app/recording.py`, native MediaMTX recording +
concat/validate/upload to private S3), and simulcast relay to external
platforms. Not passthrough-only or single-quality-only anymore — that
described the original MVP scope before the recording/replay feature was
built; see `docs/CAPACITY.md` for what's current.

## How it fits together

- **MediaMTX** (a real, unmodified open-source media server — not
  hand-rolled) does the actual RTMP/WHIP ingest and HLS packaging. See
  `mediamtx.yml`.
- **`app/`** (FastAPI) is a thin control plane Django talks to
  (`KisVideoLiveProvider` in `backend/kis/apps/broadcasts/
  live_stream_providers.py`) — allocates stream keys, relays MediaMTX's
  publish/unpublish events to Django's webhook, and manages simulcast
  relay processes. It holds no durable state of its own; Django's
  `ChannelLiveStream`/`ChannelLiveStreamTarget` rows are the source of
  truth (see `app/relay.py`'s docstring).
- **nginx** (production only — the box's existing edge proxy, confirmed by
  reading `/etc/nginx/sites-available/kis` directly; there is no Caddy
  anywhere on this box) terminates TLS in front of MediaMTX's WHIP and HLS
  HTTP ports, required for WHIP to work from browsers at all. New `listen`
  ports (8889, 8888) added to the *existing*
  `api.kingdomimpactventures.org` server block, reusing its existing
  certbot cert — not a new subdomain/cert, and not a modification to that
  block's existing `:443` routing. See `nginx-live-streaming.conf.snippet`
  (not yet applied to the live config — reviewed as a diff first).
- **The control API is not publicly exposed at all.** Django and this
  service now run on the same host, so `api` joins the same external
  `kis-production-network` Docker network Django and `kisvideo`(VOD)
  already use, and is reached by container-name DNS
  (`http://kisvideo-live-api:8000`) — no host port, no TLS, no nginx
  route needed for this hop.

## Local dev

```
cp .env.example .env   # fill in real values (INTERNAL_TOKEN at minimum)
docker compose up --build
```

Control API: http://localhost:8011/health
RTMP ingest: `rtmp://localhost:1935/live/<key>` (get `<key>` from a real
`POST /streams` call, authenticated with `X-Internal-Auth: $INTERNAL_TOKEN`)
WHIP: `http://localhost:8889/live/<key>/whip`
HLS playback: `http://localhost:8888/live/<key>/index.m3u8`

Manual smoke test without Django in the loop:

```
curl -X POST localhost:8011/streams -H "X-Internal-Auth: $INTERNAL_TOKEN"
# publish to the returned ingest_url with ffmpeg or OBS, e.g.:
ffmpeg -re -i some_test_file.mp4 -c copy -f flv rtmp://localhost:1935/live/<key>
# then play the returned playback_url with ffplay/VLC/hls.js
```

## Production deployment

```
cp .env.production.example .env.production   # fill in real values
docker compose -f docker-compose.prod.yml --env-file .env.production up -d --build
```

`--env-file .env.production` is required, not optional — Compose only
auto-loads a file literally named `.env` for the `${VAR}` substitution
used in `docker-compose.prod.yml` itself (e.g. `${HOOK_SHARED_SECRET}`
in the `mediamtx` service's `environment:` block); `env_file:
.env.production` on individual services loads it *into those
containers* but does NOT feed Compose's own YAML-level substitution.
Without this flag, `KISVIDEO_LIVE_HOOK_SECRET` would silently resolve to
an empty string in the `mediamtx` container.

Runs on the shared kis-production box (see scope note above). Before the
first run there: create the external network reference if it doesn't
already resolve (it should — `kisvideo`(VOD) already created
`kis-production-network`), and apply `nginx-live-streaming.conf.snippet`
to `/etc/nginx/sites-available/kis` (`sudo nginx -t` before `sudo
systemctl reload nginx`, per the ops runbook's own golden rules) — and
open TCP 1935/8889/8888 in the Lightsail instance firewall (console/API;
this is outside Docker/nginx entirely and outside what any session without
AWS credentials can do).

**`INTERNAL_TOKEN`**: generate with `openssl rand -base64 48`. Must be set
identically here and as `KIS_VIDEO_LIVE_SERVICE_INTERNAL_TOKEN` in
Django's env — same trust model as `kisvideo`'s own `INTERNAL_TOKEN` /
Django's `DJANGO_INTERNAL_TOKEN`.

**Django's `KIS_VIDEO_LIVE_SERVICE_URL`** should be set to
`http://kisvideo-live-api:8000` — the internal `kis-production-network`
container-DNS address, not a public URL at all (Django and this service
share a host now). Note the deliberately distinct alias
`kisvideo-live-api`, not `api` — `kisvideo`(VOD)'s own container is
*already* registered on that same external network under the bare alias
`api` (confirmed via `docker inspect kis-video-api`); reusing that alias
here would make Docker's embedded DNS round-robin between two unrelated
services. See `docker-compose.prod.yml`'s header comment.

## Capacity, lifecycle, and operations

See [`docs/CAPACITY.md`](docs/CAPACITY.md) — the capacity model (every
env-configurable limit and why it's set where it is), the full recording
lifecycle and recovery/retry behavior, the synthetic load-test harness,
the procedure for raising concurrency when the host gets more CPU/RAM,
and the operational runbook. That document is now the source of truth
for all of this; it superseded the "Capacity assumptions (MVP, shared
box)" section that used to live here, which had grown stale (it still
said "no ffmpeg re-encode anywhere in this service" and "not yet
load-tested," both no longer true).

**`HOOK_SHARED_SECRET`**: generate the same way, but this one has nothing
to do with Django — it authenticates MediaMTX's own `runOnAvailable`/
`runOnUnavailable` hook calls into this service (see `app/deps.py::
require_hook_secret`). Keep it distinct from `INTERNAL_TOKEN` and
`DJANGO_WEBHOOK_SECRET` — three different caller identities.

**Why MediaMTX gets its own Dockerfile** (`Dockerfile.mediamtx`, not just
`image: bluenviron/mediamtx`): the official image is `FROM scratch` — a
bare static binary, no shell, no `curl`. `mediamtx.yml`'s
`runOnAvailable`/`runOnUnavailable` hooks shell out to `curl` to notify
this service's `/hooks/ready`/`/hooks/not-ready` — that fails silently
against the bare official image, so this multi-stage build lifts the real
binary onto a small Alpine base that actually has `curl`.

**Why `runOnAvailable`/`runOnUnavailable`, not `runOnReady`/
`runOnNotReady`**: those latter names don't exist in current MediaMTX (a
couple of secondary sources incorrectly suggested them — verified
directly against the real `mediamtx.yml` shipped in `bluenviron/mediamtx`
main, 2026-09-22). `runOnAvailable`/`runOnUnavailable` fire when a
publisher starts/stops sending on a path; `runOnRead`/`runOnUnread` fire
per *viewer* connect/disconnect and would be the wrong signal here.

**Status**: live in production. `nginx-live-streaming.conf.snippet` is
applied to the live nginx config, the Lightsail firewall ports are open,
and this has been run end-to-end against the real mobile app and real
test broadcasts repeatedly, including a full recording→archive→replay
cycle. See `docs/CAPACITY.md` §11 for what remains a genuine, documented
limitation rather than simply this file being out of date.
