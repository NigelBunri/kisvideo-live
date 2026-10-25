# kisvideo-live — Capacity, Lifecycle, and Operations Reference

This is the single place to look for: what this service's concurrency/
disk/retry limits actually are today, how to raise them safely when the
host gets more CPU/RAM, how the recording lifecycle really behaves
(including every failure/recovery path), and how to deploy, verify, and
roll back. Every numeric default named below lives in
[`app/config/capacity.py`](../app/config/capacity.py) as the single
source of truth — this document explains the *why* and the *procedure*;
that module is the actual enforced values.

## 1. Current deployment architecture (verified, not assumed)

```
OBS/encoder --RTMP--> [ingest-proxy :19350] --clean RTMP--> [MediaMTX]
                        (ffmpeg re-encode,                   |  |
                         1 CPU, real-time)                   |  +--HLS--> viewers
                                                               |  +--native recording--> /recordings (shared volume)
                                                               |
                                                    runOnAvailable/runOnUnavailable/
                                                    runOnRecordSegmentComplete hooks
                                                               |
                                                               v
                                                   [kisvideo-live-api :8000]
                                              (admission control, recording
                                               finalize/upload, disk guard,
                                               reconciliation sweep)
                                                               |
                                                   presigned S3 PUT + webhook
                                                               |
                                                               v
                                                           [Django]
                                        (ChannelLiveStream / ChannelContent /
                                         ChannelContentAsset - source of truth)
```

**Host**: a *shared* 2-CPU / ~3.7GB Lightsail instance also running
Django, Nest, Postgres, Redis, Celery, and the separate `kisvideo` (VOD)
service — confirmed via direct inspection (`free -h`, `nproc`,
`docker stats`), not a dedicated machine. Every capacity number below is
sized against that reality, not against what a dedicated box could do.

**Process model**: `kisvideo-live-api` runs as a **single uvicorn
worker, single process** (`Dockerfile`'s `CMD` has no `--workers` flag -
confirmed by reading it, not assumed). Every admission-control structure
(`IngestProxyManager`, `RecordingManager`) is a plain in-process
`threading.Lock`-guarded dict. This is the *correct, sufficient*
mechanism for this exact deployment — see §6 ("Vertical vs. horizontal
scaling") for exactly where that stops being true.

## 2. The capacity model (`app/config/capacity.py`)

All values below are current production defaults, each overridable by
its named env var. `CapacitySettings.validate()` runs at startup and
**refuses to start the process** if any value is missing, malformed, or
self-contradictory (e.g. a warn threshold that isn't above the critical
one) — check `docker logs kisvideo-live-api` immediately after any
config change; a validation failure is loud and immediate, not a
silent misconfiguration.

| Setting | Env var | Default | Meaning |
|---|---|---|---|
| Ingest port pool | `INGEST_PROXY_PORTS` | `19350` | Comma-separated list. **Length of this list IS `max_concurrent_broadcasts`** — there is no separate "concurrency limit" setting to accidentally drift out of sync with the pool. |
| Reconnect grace period | `INGEST_RECONNECT_GRACE_SECONDS` | `8.0` | How long a disconnected publisher's port reservation survives before auto-releasing (see §4.1). |
| Disk warn threshold | `DISK_WARN_FREE_BYTES` | `10GB` | Structured `[DiskCapacity]` WARNING log once free space drops below this. |
| Disk critical threshold | `DISK_CRITICAL_FREE_BYTES` | `5GB` | `POST /streams` starts returning `503` once free space drops below this. Already-running recordings are never interrupted. |
| Disk monitor interval | `DISK_MONITOR_INTERVAL_SECONDS` | `60` | How often the disk guard polls. |
| Finalize grace | `FINALIZE_GRACE_SECONDS` | `4.0` | Wait after unpublish before collecting segments, covering hook-ordering race between `runOnUnavailable` and the final `runOnRecordSegmentComplete`. |
| Upload timeout | `UPLOAD_TIMEOUT_SECONDS` | `300.0` | Per-attempt S3 PUT timeout. |
| Upload max attempts | `UPLOAD_MAX_ATTEMPTS` | `3` | Bounded in-attempt retry for a transient failure (connection error, timeout, 5xx) - exponential backoff between attempts. A 4xx is never retried within this bound (the same presigned URL cannot succeed on retry). Distinct from, and much shorter than, the sweep-level retention in `retry_pending_uploads`, which has no maximum by design. |
| Upload retry base delay | `UPLOAD_RETRY_BASE_DELAY_SECONDS` | `1.0` | Base for the exponential backoff between attempts (`base * 2^attempt`). |
| Local quota backstop | `MAX_LOCAL_RECORDING_BYTES` | `3GB` | Only ever reclaims *garbage* (failed concat/probe, no `recording.mp4`) — never a validated, not-yet-uploaded recording. |
| Retry escalation | `RETRY_CRITICAL_AFTER_SECONDS` | `24h` | A pending upload still unresolved after this long logs `CRITICAL`, not `WARNING`. |
| Reconciliation interval | `RECONCILE_INTERVAL_SECONDS` | `300` | How often the sweep runs (orphan recovery + pending-upload retry). |
| Reconciliation staleness | `RECONCILE_STALE_SECONDS` | `900` | How old a live-segment directory's newest file must be before the sweep treats the session as abandoned, not mid-broadcast. |
| Shutdown drain timeout | `SHUTDOWN_DRAIN_TIMEOUT_SECONDS` | `25.0` | Upper bound on graceful-shutdown work before the process exits anyway (must stay under the orchestrator's own SIGTERM→SIGKILL grace period). |
| Measured CPU/broadcast | `MEASURED_CPU_FRACTION_PER_BROADCAST` | `0.88` | Reported via `GET /capacity`, not enforced — see §3. |
| Measured memory/broadcast | `MEASURED_MEMORY_MB_PER_BROADCAST` | `188` | Same. |

**Changing the port pool changes the concurrency limit.** There is
deliberately no second "max broadcasts" setting — see §6.1 for why that
specific design choice matters.

**Upload integrity**: a bare S3 `200 OK` on the PUT is not, by itself,
proof the object actually exists at the size that was sent. After every
successful PUT, `_upload` issues a plain `HEAD` against the recording's
public `object_url` (recordings live under the
`broadcast_videos/live_recordings` public-prefix convention — see
Django's `AWS_S3_PUBLIC_PREFIXES`) and compares `Content-Length` against
the local file size before reporting success to Django. A HEAD that
itself fails (network blip) is treated as a soft pass, not a hard
failure — the PUT already succeeded; re-uploading a large file because a
tiny follow-up request hiccuped would be a worse outcome than trusting
S3's own prior confirmation.

## 3. Measured resource cost per broadcast

Measured directly (not estimated) by pushing a real synthetic 18-second
RTMP test stream through the full ingest-proxy → MediaMTX → recording
pipeline while sampling `docker stats` every 2 seconds, on *this exact
production host*, under its real current load from the other
co-resident services:

| Container | Idle | During one active encode |
|---|---|---|
| `kisvideo-live-api` (ingest-proxy re-encode) | ~0.17% CPU, 91MB | **65–88% CPU** of its 1.0-CPU limit, 188MB |
| `kisvideo-live-mediamtx` | ~0.02% CPU, 33MB | ~0.2–1.6% CPU, 35MB |

**This is why `max_concurrent_broadcasts` is 1 today, and why it must
stay 1 until the container's own CPU limit is also raised.** A single
real-time `libx264 -preset ultrafast` encode already consumes nearly the
entire 1.0 CPU budget `docker-compose.prod.yml` gives the `api`
container. A second concurrent stream sharing that same 1.0-CPU cgroup
would not get a second CPU's worth of encode time — both streams would
compete for the one CPU already nearly saturated by the first, degrading
*both* (dropped frames, encoder falling behind real time, viewers seeing
stutter) rather than cleanly refusing the second one. The port-pool
admission control exists specifically to make that refusal happen
*before* a stream starts, not discover it mid-broadcast as visible
quality degradation.

MediaMTX itself is not the bottleneck (recording is a native disk write,
no transcode — confirmed near-zero CPU during the same test). All
headroom math below is about the `api` container's ingest-proxy
re-encode, not MediaMTX.

## 4. Lifecycle — states and transitions

```
                     POST /streams (admission: disk + port pool)
                              |
                              v
                    [reserved, awaiting publish]
                              |
         MediaMTX runOnAvailable (hook_ready)
                              |
                              v
                          [live / recording]  <---+
                              |                    | hook_ready (reconnect -
      MediaMTX runOnUnavailable (hook_not_ready)    | cancels pending release,
                              |                    | see §4.1)
                              v                    |
            [disconnect grace window, port --------+
             still reserved, finalize_async
             running on a background thread]
                              |
              (no reconnect within grace window)
                              |
                              v
                       [port released]
                              |
                              v (independently, on the finalize thread)
                   segments collected -> concat -> ffprobe validate
                              |
                   +----------+----------+
                   |                     |
            invalid/empty           valid + playable
                   |                     |
                   v                     v
            recording_status=       recording_status=processing
              failed                (meta.json written - now
                                      recoverable across a restart)
                                          |
                                          v
                                   upload to S3 (presigned PUT)
                                          |
                        +-----------------+-----------------+
                        |                                   |
                   upload fails                        upload succeeds
                        |                                   |
                        v                                   v
            [retained on disk,              recording_status=ready
             retried every sweep             + real ChannelContentAsset
             interval, NEVER deleted          created in Django
             until a confirmed upload]        (replay genuinely playable,
                        |                      not just a status flag)
                        |                                   |
                        +---------------> (eventually) ------+
                                                              |
                                              creator Publish/Unpublish
                                              (ChannelContent.status)
                                                              |
                                              creator Delete
                                              (recording_status=deleted,
                                               S3 object purged via
                                               storage_path)
```

Terminal states (`ready`, `failed`, `deleted`) can never be regressed by
a late/duplicate webhook delivery — enforced on the Django side
(`ChannelLiveStreamArchiveView`/`ChannelLiveStreamWebhookView`'s own
terminal-state guards, confirmed via dedicated tests in
`apps/broadcasts/test_channel_live_stream.py`).

### 4.1 The disconnect-grace-release fix (this round)

**Previously a real, production-reachable bug**: `hook_not_ready` never
released the ingest-proxy port — only an explicit `DELETE /streams` did,
and Django's organic "stream ended" webhook handler
(`ChannelLiveStreamWebhookView`) never calls that; only the creator's own
explicit End/Cancel actions in the app do. A single ungraceful disconnect
(OBS crash, lost network, the creator just closing the encoder without
tapping "End Stream") permanently exhausted this service's entire
one-port capacity until a human intervened or the container restarted.

**Fix**: `hook_not_ready` now calls `schedule_release()` — a bounded
timer (`INGEST_RECONNECT_GRACE_SECONDS`, default 8s), not an immediate
release. `hook_ready` for the same stream (a genuine reconnect, which
OBS's own auto-reconnect does routinely) calls
`cancel_scheduled_release()`. If nothing reconnects within the grace
window, the port releases itself automatically — no explicit `DELETE`
required. Verified with real concurrent-thread tests in
`tests/test_ingest_proxy.py`, not just reasoned about.

### 4.2 Idempotency and duplicate/out-of-order events

- Django's generic webhook has a 10-second `cache.add()` dedup window
  for exact-duplicate deliveries.
- `recording_status` transitions are a strict one-way ratchet toward a
  terminal state — a late `finalizing`/`processing` event can never
  revert an already-`ready`/`failed`/`deleted` row.
- The archive webhook itself checks for an existing replay
  (`metadata__archived_from_stream`) before creating a new one — a
  resubmitted success report is a no-op, never a duplicate replay.
- `retry_pending_uploads` skips any stream still `is_tracked()` in the
  live finalize path, so a sweep pass and an in-flight live finalize can
  never race to upload the same recording twice.

### 4.3 Recovery after a process restart

No persisted state is needed for in-flight *port reservations* — a
process restart kills every ffmpeg child it owns, so an empty
reservation table on the next boot is already correct, not something
requiring recovery (see §6 for why this stops being true the moment this
service runs as more than one process).

Recording-pipeline state **does** need recovery, and does get it:
- **Orphaned segments** (api crashed between a segment landing on disk
  and being archived): found by the reconciliation sweep scanning
  `/recordings/live/*` directly — the filesystem, not this process's
  memory, is the durable record MediaMTX itself maintains.
- **Validated-but-unuploaded recordings** (crashed between writing
  `meta.json` and confirming the S3 PUT): found via
  `retry_pending_uploads` scanning `/recordings/work/*` for a
  `meta.json` + `recording.mp4` pair with no in-memory tracking.

Both paths were verified this round against the real production
container's own logs — a genuine WARN→CRITICAL-eligible
`[RecordingRetained]` entry was observed for a test artifact correctly
retained and retried, not silently dropped.

## 5. Capacity-sizing procedure (when the host gets more CPU/RAM)

Follow in order. Do not skip straight to raising the port pool.

1. **Measure the new host's real headroom** — `nproc`, `free -h`,
   `docker stats` with the *current* (1-broadcast) load running, exactly
   as was done to produce the numbers in §1 and §3. Do not assume a
   bigger instance type's advertised vCPU count is actually free of
   other tenants' work — Django/Celery/Postgres/Redis/`kisvideo` all
   still compete for it.
2. **Run the synthetic concurrency test** (see §5.1) against a staging
   copy or a deliberately brief, low-risk production smoke window — not
   a sustained heavy load test against real users.
3. **Compute a conservative concurrency number** from the measured
   per-broadcast CPU fraction (§3's `measured_cpu_fraction_per_broadcast`
   — re-measure this on the new host, don't reuse the old box's number)
   against the new host's actual free CPU, leaving real margin for
   Django/Celery/Postgres/Redis's own load, not just `N = total_cpu /
   per_broadcast_cpu`.
4. **Change these together, in the same deploy, never independently**:
   - `INGEST_PROXY_PORTS` — add N new ports.
   - `docker-compose.prod.yml`'s `api` service `deploy.resources.limits`
     — raise `cpus` proportionally (the ingest-proxy's re-encode is the
     CPU-bound part; `mediamtx`'s own limit does not need to grow at the
     same rate since it stays bandwidth/disk-IO bound, not CPU-bound,
     confirmed in §3).
   - The Lightsail/host firewall — open every new port in
     `INGEST_PROXY_PORTS`, or broadcasters simply can't reach them.
5. **Leave unchanged**: `DISK_WARN_FREE_BYTES`/`DISK_CRITICAL_FREE_BYTES`
   (these are about total disk headroom for the recording spool, not
   per-broadcast CPU — re-derive them from the new host's actual disk
   size if that also changed, not from the concurrency number),
   `UPLOAD_TIMEOUT_SECONDS`, `RECONCILE_*` (these govern pipeline
   correctness, not raw capacity).
6. **Validate after deploy**: `GET /capacity` (internal-auth) should
   report the new `max_concurrent_broadcasts`; `docker logs` should show
   the startup validation line confirming the new port list; attempt N
   simultaneous real test broadcasts (synthetic, not real users) and
   confirm all N succeed and the N+1th gets a clean `503`.
7. **Roll back if resource pressure appears**: revert
   `INGEST_PROXY_PORTS` and the CPU/memory limits together to the prior
   values, redeploy, confirm `GET /capacity` reflects the reverted
   limit. Existing *in-progress* broadcasts are never force-terminated
   by a config rollback — only new admissions are affected.

### 5.1 Synthetic concurrency test harness

A repeatable, local/staging-safe load test — never run the heavy version
against real users or the live production port pool without an explicit,
separate, low-risk smoke-test plan.

```bash
# From a machine that can reach the target host's ingest ports.
# N = number of synthetic concurrent broadcasts to test.
N=2
for i in $(seq 1 $N); do
  RESP=$(curl -s -X POST "$BASE_URL/streams" -H "X-Internal-Auth: $TOKEN")
  KEY=$(echo "$RESP" | python3 -c "import json,sys; print(json.load(sys.stdin)['stream_key'])")
  echo "stream $i key=$KEY"
  ffmpeg -re -f lavfi -i "testsrc=size=1280x720:rate=30" \
         -f lavfi -i "sine=frequency=1000" \
         -t 30 -c:v libx264 -preset veryfast -c:a aac \
         -f flv "rtmp://$HOST:19350/live/$KEY" &
done
wait
# While the above runs, sample resource usage from the host itself:
#   docker stats --no-stream kisvideo-live-api kisvideo-live-mediamtx
# and confirm via GET /capacity that active_broadcasts == N and
# accepting_new_broadcasts reflects the pool state correctly.
```

Clean up afterward: each `POST /streams` response's `provider_stream_id`
can be released via `DELETE /streams/<id>` once ffmpeg exits; verify
`/recordings/work` and `/recordings/live` contain no leftover test
artifacts (`docker exec kisvideo-live-api find /recordings -mindepth 1`).

**What this harness measures**: CPU/memory per broadcast (via
`docker stats`), admission success/failure counts, recording
success/failure. **What it does not measure**: real public-internet
upload bandwidth variance, concurrent viewer load on the HLS/WHIP side
(a separate concern from ingest capacity, not covered by this document).

## 6. Vertical vs. horizontal scaling — the boundary this module does not cross

**Vertical** (more CPU/RAM on the same single host, same single
process): a configuration + deployment change, fully covered by §5. This
is the only kind of scaling this service currently supports.

**Horizontal** (more than one `kisvideo-live-api` process, or more than
one MediaMTX instance, or more than one host): **an architecture change,
not a config change.** Every admission-control guarantee this document
describes — atomic port allocation, accurate `active_broadcasts`
counts, the disconnect-grace-release timer — is correct *only* because
exactly one process holds all of this state in its own memory, guarded
by one `threading.Lock`. Two independent processes each running their
own `IngestProxyManager` would each believe port 19350 is free at the
same moment and both allocate it — a real collision, not a theoretical
one.

If this service is ever run as more than one process or more than one
container reaching the same MediaMTX instance, `max_concurrent_broadcasts`
and port allocation must move to a shared store (a Postgres row with
`SELECT ... FOR UPDATE`, or a Redis-based distributed lock) *before* that
change ships — not discovered after a real double-allocation incident.
This document deliberately does not implement that now: it would be
unused complexity for a single-process deployment that has no current
plan to become multi-process, and the directive that produced this
document was explicit about not building infrastructure the current
scale doesn't need.

## 7. Recording duration and file-size limits

- `recordSegmentDuration: 2h` (`mediamtx.yml`) caps a single native
  recording segment. A broadcast longer than 2h produces multiple
  segments, stream-copy-concatenated by `app/recording.py` — not a
  re-encode, cheap.
- S3's single-PUT limit is 5GB. At this ingest proxy's fixed encode
  settings (~1GB/hour observed, §3), a 5GB file represents roughly a
  5-hour broadcast — comfortably longer than any realistic single KIS
  live stream to date.
- **No multipart upload is implemented.** Reviewed and confirmed
  unnecessary at current measured scale, not an oversight: the
  single-PUT ceiling is not reachable by any broadcast length this
  architecture can actually sustain (bounded at 1 concurrent stream by
  §3's own CPU math). If a future product requirement needs
  multi-hour-plus single broadcasts at a scale where this stops holding,
  that is the trigger to revisit — not before.

## 8. Security model summary

- Every privileged endpoint (`POST`/`DELETE /streams`, `GET /capacity`,
  `PUT .../targets`) requires `X-Internal-Auth` matching `INTERNAL_TOKEN`
  — Django is the only intended caller.
- Every MediaMTX-originated hook requires `X-Hook-Secret` matching
  `HOOK_SHARED_SECRET` — a distinct secret from the above, since the
  caller identity is different (MediaMTX, not Django).
- This service holds **no AWS credentials of its own** — upload URLs are
  short-lived presigned PUTs requested from Django, which already owns
  the real S3 storage backend. S3 remains private; nothing here ever
  makes a bucket or object public.
- Authorization for publish/unpublish/delete of the resulting replay is
  resolved entirely on the Django side from the authenticated user's
  session/token (`_user_can_edit_content`, checking real channel-role
  membership) — never from any client-supplied user/channel id.

## 9. Operational runbook

| Symptom | Where to look | Likely cause |
|---|---|---|
| Every `POST /streams` returns 503 "No ingest capacity" | `GET /capacity` → `active_broadcasts` | Pool exhausted - either a real concurrent stream, or (pre-this-round) a leaked reservation. Check `docker logs` for `[RecordingRetained]`/`auto-releasing` lines. |
| `POST /streams` returns 503 "Insufficient disk capacity" | `GET /capacity` → `disk_level`/`disk_free_gb` | Real disk pressure - see §2's thresholds. Do not delete anything under `/recordings/work` manually without checking for `meta.json` first (§4.3). |
| `[RecordingRetained]` repeating in logs | `docker logs kisvideo-live-api \| grep RecordingRetained` | Upload has been failing repeatedly - check Django availability, S3 credentials/connectivity, `DJANGO_API_BASE_URL`/`DJANGO_WEBHOOK_SECRET` configuration. Escalates to `CRITICAL` after `RETRY_CRITICAL_AFTER_SECONDS`. |
| A replay never reaches `ready` | Django: `ChannelLiveStream.recording_status` + `metadata.recording_error` | Check the specific failure reason recorded there (`no_segments_recorded`, `concat_failed`, `recording_failed_validation`, `no_video_stream`, `upload_url_request_failed`, `s3_upload_failed`, `internal_error`). |
| Startup fails immediately with `CapacityConfigError` | `docker logs` right after container start | A bad env var - the error message names the exact setting and why. |

## 10. Deployment and rollback

Follow the server-side build pattern (clone the already-pushed commit
directly on the production host, build natively for its real
architecture, push to GHCR from there) — **never build locally and push
a cross-architecture image**; this exact mistake was caught mid-session
once already (a locally-built `arm64` image nearly got deployed to this
`amd64` host) via `docker image inspect --format '{{.Architecture}}'`
*before* recreating any container. Always run that check before
`docker compose up --force-recreate`, every time, regardless of where
the image was built.

1. `git clone --depth 1 --branch main <repo>` on the production host.
2. `docker build -t ghcr.io/.../kisvideo-live-api:production -t
   .../2026-MM-DD-HHMM .` from that clone.
3. `docker image inspect ... --format '{{.Architecture}}'` → must read
   `amd64` for this host.
4. `docker run --rm -v ./tests:/app/tests:ro <image> python3 -m pytest
   /app/tests/` → full suite must pass against the actual image that
   will be deployed, not just against source on a dev machine.
5. Push both tags to GHCR.
6. `docker compose -f docker-compose.prod.yml up -d --force-recreate
   --no-deps api` (and `mediamtx` only if that Dockerfile changed).
7. Verify: `docker ps` healthy, `docker logs` shows clean startup +
   capacity validation, `GET /capacity` responds correctly, `GET
   /health` responds, a real (synthetic, not user-facing) test broadcast
   completes through recording → archive → playable asset.
8. Keep the prior dated tag — rollback is `docker compose up -d
   --force-recreate --no-deps api` after re-tagging that prior image
   back onto `:production` locally on the host.

## 11. Known limitations not eliminated by this round

- Single-process, single-host only (§6) — by design, not yet a gap at
  current real traffic, but the explicit ceiling before an architecture
  change (not a config change) is required.
- No `.dockerignore`-equivalent risk here (this repo already had a
  correct one before this round; the sibling `kis-django` repo's
  equivalent gap was fixed in that repo instead).
- Alerting is log-based only (`[DiskCapacity]`, `[RecordingRetained]`,
  `CapacityConfigError`) — there is no push notification/paging
  integration wired up, because no alerting service credentials exist
  in this environment. Treat `docker logs`/whatever log-shipping already
  exists on this host as the actual alert channel until a real
  notification integration is deployed and tested end-to-end — this is
  an explicit external prerequisite, not something silently claimed as
  done.
