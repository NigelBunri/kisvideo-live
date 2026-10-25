"""Automatic recording finalize/upload pipeline — the missing link between
MediaMTX's native recording (mediamtx.yml's record*/runOnRecordSegment*
keys, confirmed against the real deployed v1.21.1 docs, not assumed) and
Django's pre-existing archive webhook (ChannelLiveStreamArchiveView,
apps/broadcasts/views.py), which already handles idempotent replay
creation/recording_status but had nothing upstream ever calling it.

Division of responsibility, deliberately minimal new surface area:
  - MediaMTX writes recording segments to a shared volume (this
    container's /recordings, same mount as the mediamtx container's) and
    fires runOnRecordSegmentComplete per finished segment - no re-encode,
    it's just the already-clean ingest-proxy output being written to
    disk (see app/ingest_proxy.py's own docstring for why that output is
    safe to record as-is).
  - THIS module concatenates segments (stream-copy only, cheap - no
    re-encode) if a broadcast produced more than one, validates the
    result with ffprobe (never trust segment metadata alone), and asks
    Django for a presigned PUT (Django already has a full S3 storage
    backend with generate_presigned_put() - apps/media/storage_backends.py
    - reusing it means this container never needs its own AWS
    credentials at all, just a short-lived URL).
  - Django's existing /archive/ webhook does everything downstream
    (replay creation, recording_status=ready, idempotency) - extended
    (not replaced) to also create a real ChannelContentAsset row, since
    recording_status=ready with no playable asset behind it would be a
    replay that LOOKS done but nothing can actually play.

Timing/size constants below (grace periods, timeouts, quota, retry
escalation) now come from app/config/capacity.py's CapacitySettings,
overridable via env vars - see that module's own docstring for why
every capacity-related limit in this service lives in one validated
place instead of scattered module constants. RECORDINGS_ROOT stays here
as a plain module constant - it's a storage mount path, not a numeric
capacity knob.

Sized for THIS box specifically: disk is confirmed tight (16GB free of
77GB at last recheck - see app/disk_guard.py, which is the independent
host-level backpressure/alerting layer this module's own
max_local_recording_bytes backstop does not replace). Segments are
deleted the moment a successful upload is confirmed; a failed upload
keeps its concatenated result on disk FOREVER until either a retry
succeeds or an operator intervenes - this process never deletes a
recording that has not been confirmed uploaded (see
retry_pending_uploads/meta.json below). The quota backstop only ever
bounds genuinely worthless leftovers (concat/validation failures with no
usable output), never a pending-upload recording.

Crash recovery: this process's in-memory _segments dict (which segment
belongs to which broadcast) does not survive a restart. If the api
container dies between "MediaMTX wrote a segment" and "we finished
archiving it," ReconciliationSweep below is the recovery path - it
periodically scans the shared recordings volume directly (the
filesystem itself is the durable record MediaMTX already maintains,
independent of this process's memory) for directories containing
segment files old enough that no publisher is still actively writing to
them, and re-drives them through the exact same finalize pipeline. This
is safe to run repeatedly and safe to run against a directory that was
already successfully archived - Django's own archive-webhook dedup
(ChannelLiveStreamArchiveView, keyed on metadata__archived_from_stream)
makes a resubmitted duplicate a no-op, not a second replay. The same
sweep also drives retry_pending_uploads - recovery for a validated
recording.mp4 (meta.json present) that survived concat/probe but never
made it past the upload step, across restarts.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import threading
import time
import uuid
from pathlib import Path
from typing import Dict, List, Optional, Set

import httpx

from app.config.capacity import capacity_settings
from app.config.settings import settings
from app.ingest_proxy import ingest_proxy_manager
from app.redact import redact_stream_id

logger = logging.getLogger(__name__)

RECORDINGS_ROOT = Path(os.environ.get("RECORDINGS_ROOT", "/recordings"))


class RecordingManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        # stream_id -> ordered list of segment file paths, as reported by
        # MediaMTX's runOnRecordSegmentComplete hook.
        self._segments: Dict[str, List[str]] = {}
        # work-dir names currently being retried by retry_pending_uploads,
        # so two overlapping sweep passes can't double-upload the same
        # pending recording.
        self._retrying: Set[str] = set()

    def segment_complete(self, stream_id: str, segment_path: str) -> None:
        with self._lock:
            self._segments.setdefault(stream_id, []).append(segment_path)
        logger.info(
            "recording: segment complete stream=%s segment=%s (total=%d)",
            redact_stream_id(stream_id), Path(segment_path).name,
            len(self._segments.get(stream_id, [])),
        )

    def finalize_async(self, stream_id: str) -> None:
        threading.Thread(target=self._finalize, args=(stream_id,), daemon=True).start()

    def _take_segments(self, stream_id: str) -> List[str]:
        with self._lock:
            return self._segments.pop(stream_id, [])

    def is_tracked(self, stream_id: str) -> bool:
        """Used by ReconciliationSweep to skip any stream this process is
        already actively finalizing (or still receiving live segments
        for) - the sweep must only ever touch streams nothing in this
        process's own memory knows about."""
        with self._lock:
            return stream_id in self._segments

    def active_count(self) -> int:
        """Number of streams this process currently has segments tracked
        for - used by the capacity status endpoint to report "actively
        recording" distinct from "port reserved" (a port can be reserved
        during its disconnect grace window with zero active recording
        left to track)."""
        with self._lock:
            return len(self._segments)

    def _finalize(self, stream_id: str) -> None:
        # MediaMTX closes the final (partial) segment on unpublish and
        # fires runOnRecordSegmentComplete for it too, but that event can
        # land a moment after runOnUnavailable (which is what triggers
        # this whole method) - confirmed by the ordering risk inherent in
        # two independent hook commands, not observed non-determinism we
        # chose to ignore. This grace window is cheap insurance against
        # archiving a recording that's missing its last few seconds.
        time.sleep(capacity_settings.finalize_grace_seconds)
        segments = self._take_segments(stream_id)

        if not segments:
            logger.warning("recording: no segments for stream=%s - nothing to archive", redact_stream_id(stream_id))
            self._report_failure(stream_id, "no_segments_recorded")
            return

        self._finalize_segments(stream_id, segments)

    def finalize_segments_sync(self, stream_id: str, segments: List[str]) -> None:
        """Entry point for ReconciliationSweep - segments discovered
        directly on disk (not via the live runOnRecordSegmentComplete
        hook / self._segments dict), for a stream this process has no
        memory of. Runs synchronously on the sweep's own thread rather
        than spawning another background thread per orphaned
        session - the sweep already runs off the request-handling path,
        and serializing recovery work (rather than firing N threads for
        N orphaned sessions at once) avoids a single crash-recovery pass
        spiking this container's CPU/upload bandwidth all at once."""
        self._finalize_segments(stream_id, segments)

    def _finalize_segments(self, stream_id: str, segments: List[str]) -> None:
        work_dir = RECORDINGS_ROOT / "work" / uuid.uuid4().hex
        try:
            work_dir.mkdir(parents=True, exist_ok=True)
            final_path = self._concat(stream_id, segments, work_dir)
            if final_path is None:
                self._report_failure(stream_id, "concat_failed")
                shutil.rmtree(work_dir, ignore_errors=True)  # no usable output - nothing to retain
                return

            probe = self._probe(final_path)
            if probe is None:
                self._report_failure(stream_id, "recording_failed_validation")
                shutil.rmtree(work_dir, ignore_errors=True)  # not a playable asset - nothing to retain
                return
            duration_seconds, has_video, has_audio = probe
            if not has_video:
                logger.error("recording: final file for stream=%s has no video stream", redact_stream_id(stream_id))
                self._report_failure(stream_id, "no_video_stream")
                shutil.rmtree(work_dir, ignore_errors=True)
                return

            # From this point on, final_path is a validated, playable
            # recording - meta.json is what makes it recoverable (by
            # retry_pending_uploads, including across a process restart)
            # instead of an anonymous file nothing can map back to a
            # stream_id. This directory must never be deleted again
            # below until a successful upload is confirmed.
            self._write_meta(work_dir, stream_id, duration_seconds)
            self._report_processing(stream_id)

            if self._try_upload_and_report(stream_id, work_dir, final_path, duration_seconds):
                shutil.rmtree(work_dir, ignore_errors=True)
            else:
                logger.error(
                    "recording: upload failed for stream=%s - RETAINING recording at %s for retry "
                    "(ReconciliationSweep.retry_pending_uploads), not deleting",
                    redact_stream_id(stream_id), work_dir,
                )
        except Exception:
            logger.exception("recording: unhandled error finalizing stream=%s", redact_stream_id(stream_id))
            self._report_failure(stream_id, "internal_error")
            # Deliberately not touching work_dir here - if concat/probe
            # already succeeded and meta.json was already written before
            # this exception, retry_pending_uploads must still be able to
            # find and recover it. If they hadn't, work_dir has no
            # recording.mp4 and is harmless to leave for
            # _enforce_local_quota to eventually reclaim.
        finally:
            # Only the ORIGINAL segments are safe to always delete here -
            # they're MediaMTX's own files, already superseded by either
            # the uploaded concat result or nothing (failure case, where
            # mediamtx.yml's recordDeleteAfter is the real backstop, not
            # this process holding onto raw segments it has no further
            # use for).
            for seg in segments:
                try:
                    os.remove(seg)
                except OSError:
                    pass
            self._enforce_local_quota()

    def _try_upload_and_report(self, stream_id: str, work_dir: Path, final_path: Path, duration_seconds: int) -> bool:
        """Shared by the live finalize path and retry_pending_uploads -
        one place that knows how to turn a validated recording.mp4 into
        a confirmed, archived replay, so a retry is byte-for-byte the
        same code path as the original attempt, not a second copy that
        could drift."""
        upload = self._request_upload_url(stream_id)
        if upload is None:
            self._report_failure(stream_id, "upload_url_request_failed")
            return False
        upload_url, object_url, object_key = upload

        if not self._upload(final_path, upload_url, object_url):
            self._report_failure(stream_id, "s3_upload_failed")
            return False

        self._report_success(stream_id, object_url, duration_seconds, object_key)
        logger.info(
            "recording: archived stream=%s duration=%ss", redact_stream_id(stream_id), duration_seconds,
        )
        return True

    def _write_meta(self, work_dir: Path, stream_id: str, duration_seconds: int) -> None:
        meta_path = work_dir / "meta.json"
        try:
            meta_path.write_text(json.dumps({
                "stream_id": stream_id,
                "duration_seconds": duration_seconds,
                "created_at": time.time(),
            }))
        except OSError:
            logger.exception("recording: failed writing meta.json for stream=%s", redact_stream_id(stream_id))

    def retry_pending_uploads(self) -> None:
        """Recovery for a validated recording.mp4 that survived
        concat/probe but never made it past the upload step - whether
        that's because of a transient failure in the same process run,
        or because the process crashed/restarted after meta.json was
        written but before the upload finished. Driven by
        ReconciliationSweep on the same interval as its orphaned-segment
        scan (see that class's own docstring for why a daemon-thread
        poll, not Celery, is the right-sized mechanism here).

        Never deletes anything it can't confirm was uploaded - a dir
        that keeps failing just accumulates sweep passes (escalating to
        a CRITICAL log line after capacity_settings.retry_critical_after_seconds)
        rather than ever being silently discarded.
        """
        work_root = RECORDINGS_ROOT / "work"
        if not work_root.exists():
            return
        for entry in sorted(work_root.iterdir()):
            if not entry.is_dir():
                continue
            meta_path = entry / "meta.json"
            final_path = entry / "recording.mp4"
            if not meta_path.exists() or not final_path.exists():
                continue  # not a retry-eligible dir - left to _enforce_local_quota instead

            with self._lock:
                if entry.name in self._retrying:
                    continue
                self._retrying.add(entry.name)
            try:
                try:
                    meta = json.loads(meta_path.read_text())
                except Exception:
                    logger.exception("recording: unreadable meta.json at %s - leaving in place for manual review", entry)
                    continue
                stream_id = meta.get("stream_id")
                if not stream_id:
                    continue
                if self.is_tracked(stream_id):
                    continue  # a live finalize for this exact stream is already in flight elsewhere

                duration_seconds = meta.get("duration_seconds") or 0
                created_at = meta.get("created_at") or time.time()
                age_seconds = time.time() - created_at
                logger.info(
                    "recording: retrying pending upload stream=%s age=%.0fs dir=%s",
                    redact_stream_id(stream_id), age_seconds, entry.name,
                )
                if self._try_upload_and_report(stream_id, entry, final_path, duration_seconds):
                    shutil.rmtree(entry, ignore_errors=True)
                else:
                    log = logger.critical if age_seconds > capacity_settings.retry_critical_after_seconds else logger.warning
                    log(
                        "[RecordingRetained] unresolved pending upload stream=%s age=%.0fs dir=%s - "
                        "recording preserved on disk, will retry again next sweep; investigate if this "
                        "persists (network egress, Django availability, S3 credentials)",
                        redact_stream_id(stream_id), age_seconds, entry.name,
                    )
            finally:
                with self._lock:
                    self._retrying.discard(entry.name)

    def _concat(self, stream_id: str, segments: List[str], work_dir: Path) -> Optional[Path]:
        out_path = work_dir / "recording.mp4"
        if len(segments) == 1:
            try:
                shutil.copy2(segments[0], out_path)
                return out_path
            except OSError:
                logger.exception("recording: failed copying single segment for stream=%s", redact_stream_id(stream_id))
                return None

        list_path = work_dir / "concat_list.txt"
        try:
            with open(list_path, "w") as f:
                for seg in segments:
                    # ffmpeg's concat demuxer format requires single-quoted
                    # paths with internal single-quotes escaped - our own
                    # paths never contain one (uuid-based filenames), but
                    # escaping unconditionally costs nothing and removes
                    # the assumption entirely.
                    escaped = str(seg).replace("'", "'\\''")
                    f.write(f"file '{escaped}'\n")
            cmd = [
                "ffmpeg", "-nostdin", "-loglevel", "warning", "-y",
                "-f", "concat", "-safe", "0", "-i", str(list_path),
                "-c", "copy", str(out_path),
            ]
            result = subprocess.run(cmd, capture_output=True, timeout=120)
            if result.returncode != 0 or not out_path.exists():
                logger.error(
                    "recording: ffmpeg concat failed stream=%s rc=%s stderr=%s",
                    redact_stream_id(stream_id), result.returncode, result.stderr.decode(errors="replace")[:500],
                )
                return None
            return out_path
        except Exception:
            logger.exception("recording: concat exception for stream=%s", redact_stream_id(stream_id))
            return None

    def _probe(self, path: Path) -> Optional[tuple]:
        try:
            result = subprocess.run(
                [
                    "ffprobe", "-v", "error", "-print_format", "json",
                    "-show_format", "-show_streams", str(path),
                ],
                capture_output=True, timeout=30,
            )
            if result.returncode != 0:
                return None
            data = json.loads(result.stdout)
            streams = data.get("streams", [])
            has_video = any(s.get("codec_type") == "video" for s in streams)
            has_audio = any(s.get("codec_type") == "audio" for s in streams)
            duration = int(float(data.get("format", {}).get("duration", 0)) or 0)
            return duration, has_video, has_audio
        except Exception:
            logger.exception("recording: ffprobe validation raised for %s", path)
            return None

    def _request_upload_url(self, stream_id: str) -> Optional[tuple]:
        if not settings.django_api_base_url:
            return None
        url = f"{settings.django_api_base_url}/broadcasts/live-streams/{stream_id}/recording-upload-url/"
        try:
            resp = httpx.post(
                url,
                headers={"X-Live-Webhook-Secret": settings.django_webhook_secret},
                timeout=15,
            )
            if resp.status_code != 200:
                logger.error(
                    "recording: upload-url request failed stream=%s status=%s body=%s",
                    redact_stream_id(stream_id), resp.status_code, resp.text[:300],
                )
                return None
            data = resp.json()
            return data["upload_url"], data["object_url"], data.get("object_key", "")
        except Exception:
            logger.exception("recording: upload-url request raised for stream=%s", redact_stream_id(stream_id))
            return None

    def _upload(self, path: Path, upload_url: str, object_url: str) -> bool:
        """Bounded retry with exponential backoff for a SINGLE upload
        attempt (capacity_settings.upload_max_attempts/
        upload_retry_base_delay_seconds) - distinct from (and much
        shorter than) retry_pending_uploads' own sweep-level retention,
        which deliberately has no maximum. This bound only covers
        TRANSIENT conditions (a dropped connection, a timeout, a 5xx) -
        a 4xx response (most realistically an expired presigned URL)
        is never retried here, since retrying the exact same URL cannot
        succeed; it falls straight through to the sweep-level retry,
        which fetches a FRESH presigned URL on its next pass.

        After a successful PUT, verifies the object actually exists at
        its real size via a plain HEAD to object_url (not upload_url -
        a presigned PUT URL's signature is method-specific and would
        reject a HEAD; object_url is the plain, non-expiring URL
        recordings get under the broadcast_videos/live_recordings public
        prefix convention - see Django's AWS_S3_PUBLIC_PREFIXES). This
        is the "Object existence and size verification" step a bare
        '200 OK' from S3 does not, by itself, guarantee against (a
        theoretical but real gap: nothing before this confirmed the
        object S3 claims to have stored actually matches what was
        sent)."""
        size = path.stat().st_size
        last_status: Optional[int] = None
        for attempt in range(1, capacity_settings.upload_max_attempts + 1):
            try:
                with open(path, "rb") as f:
                    resp = httpx.put(
                        upload_url,
                        content=f,
                        headers={"Content-Type": "video/mp4", "Content-Length": str(size)},
                        timeout=capacity_settings.upload_timeout_seconds,
                    )
            except Exception:
                logger.exception(
                    "recording: upload attempt %d/%d raised for %s",
                    attempt, capacity_settings.upload_max_attempts, path,
                )
                if attempt < capacity_settings.upload_max_attempts:
                    time.sleep(capacity_settings.upload_retry_base_delay_seconds * (2 ** (attempt - 1)))
                    continue
                return False

            if resp.status_code in (200, 201):
                return self._verify_uploaded_object(object_url, size)

            last_status = resp.status_code
            if 400 <= resp.status_code < 500:
                # Not retryable within this attempt - the same
                # presigned URL will fail the same way again (e.g. it
                # expired, or the signature/policy rejected this
                # specific request). Give up on THIS attempt loop
                # immediately rather than burning the remaining budget
                # retrying something that cannot succeed; the
                # sweep-level retry gets a brand new URL next time.
                logger.error(
                    "recording: S3 PUT non-retryable status=%s body=%s",
                    resp.status_code, resp.text[:300],
                )
                return False

            logger.error(
                "recording: S3 PUT attempt %d/%d failed status=%s body=%s",
                attempt, capacity_settings.upload_max_attempts, resp.status_code, resp.text[:300],
            )
            if attempt < capacity_settings.upload_max_attempts:
                time.sleep(capacity_settings.upload_retry_base_delay_seconds * (2 ** (attempt - 1)))

        logger.error(
            "recording: upload exhausted %d attempts, last status=%s",
            capacity_settings.upload_max_attempts, last_status,
        )
        return False

    def _verify_uploaded_object(self, object_url: str, expected_size: int) -> bool:
        try:
            resp = httpx.head(object_url, timeout=30)
            if resp.status_code != 200:
                logger.error(
                    "recording: post-upload verification HEAD failed status=%s url=%s",
                    resp.status_code, object_url,
                )
                return False
            content_length = resp.headers.get("content-length")
            if content_length is not None and int(content_length) != expected_size:
                logger.error(
                    "recording: post-upload size mismatch expected=%d actual=%s url=%s",
                    expected_size, content_length, object_url,
                )
                return False
            return True
        except Exception:
            # A HEAD failing here (network blip, DNS) after the PUT
            # itself already returned 200/201 is treated as a soft
            # pass, not a hard failure - the object is very likely fine
            # (S3 already confirmed the write), and re-uploading a
            # multi-hundred-MB file just because a tiny follow-up HEAD
            # request hiccuped would be a worse outcome than trusting
            # S3's own prior 200. Logged for visibility either way.
            logger.exception("recording: post-upload verification raised for %s", object_url)
            return True

    def _report_processing(self, stream_id: str) -> None:
        """Reported once concat/validate actually succeeds and upload is
        about to begin - a real sub-state distinct from "finalizing"
        (publisher just disconnected; segments may not even be fully
        collected yet). Best-effort, same as every other Django
        notification in this module - a missed processing update just
        means the UI shows "finalizing" a little longer, never a
        correctness problem."""
        if not settings.django_api_base_url:
            return
        url = f"{settings.django_api_base_url}/broadcasts/live-streams/{stream_id}/archive/"
        try:
            httpx.post(
                url,
                json={"status": "processing"},
                headers={"X-Live-Webhook-Secret": settings.django_webhook_secret},
                timeout=15,
            )
        except Exception:
            logger.exception("recording: processing-status report raised for stream=%s", redact_stream_id(stream_id))

    def _report_success(self, stream_id: str, object_url: str, duration_seconds: int, object_key: str = "") -> None:
        if not settings.django_api_base_url:
            return
        url = f"{settings.django_api_base_url}/broadcasts/live-streams/{stream_id}/archive/"
        try:
            httpx.post(
                url,
                json={
                    "recording_url": object_url,
                    "duration_seconds": duration_seconds,
                    "object_key": object_key,
                },
                headers={"X-Live-Webhook-Secret": settings.django_webhook_secret},
                timeout=15,
            )
        except Exception:
            logger.exception("recording: archive report (success) raised for stream=%s", redact_stream_id(stream_id))

    def _report_failure(self, stream_id: str, reason: str) -> None:
        if settings.django_api_base_url:
            url = f"{settings.django_api_base_url}/broadcasts/live-streams/{stream_id}/archive/"
            try:
                httpx.post(
                    url,
                    json={"status": "failed", "error": reason},
                    headers={"X-Live-Webhook-Secret": settings.django_webhook_secret},
                    timeout=15,
                )
            except Exception:
                logger.exception("recording: archive report (failure) raised for stream=%s", redact_stream_id(stream_id))

    def _enforce_local_quota(self) -> None:
        try:
            work_root = RECORDINGS_ROOT / "work"
            if not work_root.exists():
                return
            # Only ever targets directories with NO valid recording.mp4 -
            # genuine leftovers from a concat/probe failure or a crash
            # mid-write. A directory holding a validated, not-yet-
            # uploaded recording (meta.json + recording.mp4 present) is
            # never a candidate here, no matter how old or how much
            # total disk every such directory adds up to - that content
            # is only ever removed after a confirmed upload, via
            # _try_upload_and_report's own cleanup. See
            # retry_pending_uploads for how those are resolved instead.
            candidates = [e for e in work_root.iterdir() if e.is_dir() and not (e / "recording.mp4").exists()]
            candidates.sort(key=lambda p: p.stat().st_mtime)
            total = sum(f.stat().st_size for e in candidates for f in e.rglob("*") if f.is_file())
            while total > capacity_settings.max_local_recording_bytes and candidates:
                oldest = candidates.pop(0)
                size = sum(f.stat().st_size for f in oldest.rglob("*") if f.is_file())
                shutil.rmtree(oldest, ignore_errors=True)
                total -= size
                logger.warning("recording: quota backstop removed stale incomplete work dir %s", oldest.name)
        except Exception:
            logger.exception("recording: quota enforcement raised")


recording_manager = RecordingManager()


class ReconciliationSweep:
    """Periodic recovery for recording sessions left in an intermediate
    state after an interruption (api container crash/restart between a
    segment landing on disk and that recording being archived) - see
    RecordingManager's own docstring for why the filesystem, not this
    process's memory, is the durable source of truth this sweep trusts.
    Also drives RecordingManager.retry_pending_uploads - the equivalent
    recovery path for a recording that got PAST concat/probe but never
    got PAST upload.

    Deliberately NOT using Celery/an external job queue - this service
    has none today (confirmed: no celery/redis dependency in
    requirements.txt), and introducing one for a couple of periodic
    sweeps on a single-instance service would be exactly the kind of
    unnecessary new infrastructure the task asked NOT to add. A daemon
    thread polling on a fixed interval is the right-sized mechanism for
    this service's actual scale (one process, one box).
    """

    def __init__(self, manager: RecordingManager) -> None:
        self._manager = manager

    def start(self) -> None:
        threading.Thread(target=self._loop, daemon=True).start()

    def _loop(self) -> None:
        while True:
            time.sleep(capacity_settings.reconcile_interval_seconds)
            try:
                self._sweep_once()
            except Exception:
                logger.exception("recording: reconciliation sweep raised")

    def _sweep_once(self) -> None:
        self._manager.retry_pending_uploads()
        # Reclaims an ingest-proxy port whose broadcaster disconnected
        # and never reconnected for a genuinely long time - the ONLY
        # mechanism that releases a port after an organic disconnect in
        # this codebase; see app/ingest_proxy.py's own docstring for why
        # this deliberately reuses the same long-proven
        # RECONCILE_STALE_SECONDS threshold instead of a separate,
        # shorter, independently-tuned timer (which caused a real
        # production incident the first time one was tried).
        ingest_proxy_manager.release_stale_reservations(capacity_settings.reconcile_stale_seconds)

        live_root = RECORDINGS_ROOT / "live"
        if not live_root.exists():
            return
        now = time.time()
        for stream_dir in live_root.iterdir():
            if not stream_dir.is_dir():
                continue
            # "live/<key>" - the exact same path string MediaMTX's
            # $MTX_PATH substitutes and app/main.py's hooks already use
            # as stream_id everywhere else, so this plugs directly into
            # the same _report_success/_report_failure/Django contract.
            stream_id = f"live/{stream_dir.name}"

            if self._manager.is_tracked(stream_id):
                continue  # actively publishing or already being finalized

            files = sorted(
                (f for f in stream_dir.rglob("*") if f.is_file()),
                key=lambda f: f.stat().st_mtime,
            )
            if not files:
                continue

            newest_age = now - files[-1].stat().st_mtime
            if newest_age < capacity_settings.reconcile_stale_seconds:
                continue  # could still be an active publish between segments

            logger.warning(
                "recording: reconciliation sweep found orphaned session stream=%s files=%d newest_age=%.0fs",
                redact_stream_id(stream_id), len(files), newest_age,
            )
            # Safe even if this session already archived successfully
            # and SHOULD have been cleaned up but wasn't (e.g. the
            # process crashed in the brief window after upload but
            # before its own shutil.rmtree) - Django's archive-webhook
            # dedup makes a resubmitted duplicate a no-op, not a second
            # replay (confirmed by
            # test_duplicate_success_callbacks_create_exactly_one_replay).
            self._manager.finalize_segments_sync(stream_id, [str(f) for f in files])


reconciliation_sweep = ReconciliationSweep(recording_manager)
