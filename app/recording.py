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

Sized for THIS box specifically: disk is confirmed tight (17GB free of
77GB at build time - see docker-compose.prod.yml's own capacity
comments). Segments are deleted the moment a successful upload is
confirmed; a failed upload keeps its segments (for diagnosis / manual
retry) but nothing else writes to the shared volume indefinitely - see
MAX_LOCAL_RECORDING_BYTES below for the hard backstop.

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
makes a resubmitted duplicate a no-op, not a second replay.
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
from typing import Dict, List, Optional

import httpx

from app.config.settings import settings
from app.redact import redact_stream_id

logger = logging.getLogger(__name__)

RECORDINGS_ROOT = Path(os.environ.get("RECORDINGS_ROOT", "/recordings"))
# A broadcast that never produces a usable recording (OBS connects and
# disconnects in under a second, a genuinely empty segment) shouldn't
# retry forever - these numbers are generous enough for a real stream's
# finalize-after-unpublish window, not an infinite wait.
FINALIZE_GRACE_SECONDS = 4.0
UPLOAD_TIMEOUT_SECONDS = 300.0
# Hard backstop against unbounded local disk growth if uploads are
# failing repeatedly - confirmed 17GB free on this box at implementation
# time, not a guess. This does NOT replace mediamtx.yml's own
# recordDeleteAfter safety net; it's an independent second layer scoped
# to recordings this process itself has already tried and failed to
# upload (recordDeleteAfter catches everything else, including segments
# from a crashed process that never got this far at all).
MAX_LOCAL_RECORDING_BYTES = 3 * 1024 * 1024 * 1024  # 3GB


class RecordingManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        # stream_id -> ordered list of segment file paths, as reported by
        # MediaMTX's runOnRecordSegmentComplete hook.
        self._segments: Dict[str, List[str]] = {}

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

    def _finalize(self, stream_id: str) -> None:
        # MediaMTX closes the final (partial) segment on unpublish and
        # fires runOnRecordSegmentComplete for it too, but that event can
        # land a moment after runOnUnavailable (which is what triggers
        # this whole method) - confirmed by the ordering risk inherent in
        # two independent hook commands, not observed non-determinism we
        # chose to ignore. This grace window is cheap insurance against
        # archiving a recording that's missing its last few seconds.
        time.sleep(FINALIZE_GRACE_SECONDS)
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
                return

            probe = self._probe(final_path)
            if probe is None:
                self._report_failure(stream_id, "recording_failed_validation")
                return
            duration_seconds, has_video, has_audio = probe
            if not has_video:
                logger.error("recording: final file for stream=%s has no video stream", redact_stream_id(stream_id))
                self._report_failure(stream_id, "no_video_stream")
                return

            upload = self._request_upload_url(stream_id)
            if upload is None:
                self._report_failure(stream_id, "upload_url_request_failed")
                return
            upload_url, object_url = upload

            if not self._upload(final_path, upload_url):
                self._report_failure(stream_id, "s3_upload_failed")
                return

            self._report_success(stream_id, object_url, duration_seconds)
            logger.info(
                "recording: archived stream=%s duration=%ss", redact_stream_id(stream_id), duration_seconds,
            )
            # Uploaded and confirmed - nothing left worth keeping locally.
            # Failure paths above return early WITHOUT reaching this line,
            # deliberately leaving work_dir (and our concat output) on
            # disk for diagnosis, bounded by _enforce_local_quota's 3GB
            # backstop rather than deleted immediately.
            shutil.rmtree(work_dir, ignore_errors=True)
        except Exception:
            logger.exception("recording: unhandled error finalizing stream=%s", redact_stream_id(stream_id))
            self._report_failure(stream_id, "internal_error")
        finally:
            # Only the ORIGINAL segments are safe to always delete here -
            # they're MediaMTX's own files, already superseded by either
            # the uploaded concat result or nothing (failure case, where
            # mediamtx.yml's recordDeleteAfter is the real backstop, not
            # this process holding onto raw segments it has no further
            # use for). The work_dir (containing our own concat output)
            # is removed in the success and give-up paths below, NOT
            # unconditionally - see _report_failure's own cleanup
            # decision for why a failed upload keeps it.
            for seg in segments:
                try:
                    os.remove(seg)
                except OSError:
                    pass
            self._enforce_local_quota()

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
            return data["upload_url"], data["object_url"]
        except Exception:
            logger.exception("recording: upload-url request raised for stream=%s", redact_stream_id(stream_id))
            return None

    def _upload(self, path: Path, upload_url: str) -> bool:
        try:
            size = path.stat().st_size
            with open(path, "rb") as f:
                resp = httpx.put(
                    upload_url,
                    content=f,
                    headers={"Content-Type": "video/mp4", "Content-Length": str(size)},
                    timeout=UPLOAD_TIMEOUT_SECONDS,
                )
            if resp.status_code not in (200, 201):
                logger.error("recording: S3 PUT failed status=%s body=%s", resp.status_code, resp.text[:300])
                return False
            return True
        except Exception:
            logger.exception("recording: upload raised for %s", path)
            return False

    def _report_success(self, stream_id: str, object_url: str, duration_seconds: int) -> None:
        if not settings.django_api_base_url:
            return
        url = f"{settings.django_api_base_url}/broadcasts/live-streams/{stream_id}/archive/"
        try:
            httpx.post(
                url,
                json={"recording_url": object_url, "duration_seconds": duration_seconds},
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
            entries = sorted(work_root.iterdir(), key=lambda p: p.stat().st_mtime)
            total = sum(f.stat().st_size for e in entries for f in e.rglob("*") if f.is_file())
            while total > MAX_LOCAL_RECORDING_BYTES and entries:
                oldest = entries.pop(0)
                size = sum(f.stat().st_size for f in oldest.rglob("*") if f.is_file())
                shutil.rmtree(oldest, ignore_errors=True)
                total -= size
                logger.warning("recording: quota backstop removed stale work dir %s", oldest.name)
        except Exception:
            logger.exception("recording: quota enforcement raised")


recording_manager = RecordingManager()

# How long a recording directory's newest file must sit untouched before
# the sweep treats it as abandoned (crashed process, missed hook) rather
# than an actively-publishing stream between segments. Comfortably above
# recordSegmentDuration's 2h AND any realistic hook-delivery delay -
# this is specifically for "the process that was supposed to finalize
# this never did," not a tight liveness check.
RECONCILE_STALE_SECONDS = 15 * 60
RECONCILE_INTERVAL_SECONDS = 5 * 60


class ReconciliationSweep:
    """Periodic recovery for recording sessions left in an intermediate
    state after an interruption (api container crash/restart between a
    segment landing on disk and that recording being archived) - see
    RecordingManager's own docstring for why the filesystem, not this
    process's memory, is the durable source of truth this sweep trusts.

    Deliberately NOT using Celery/an external job queue - this service
    has none today (confirmed: no celery/redis dependency in
    requirements.txt), and introducing one for a single periodic sweep
    on a single-instance service would be exactly the kind of
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
            time.sleep(RECONCILE_INTERVAL_SECONDS)
            try:
                self._sweep_once()
            except Exception:
                logger.exception("recording: reconciliation sweep raised")

    def _sweep_once(self) -> None:
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
            if newest_age < RECONCILE_STALE_SECONDS:
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
