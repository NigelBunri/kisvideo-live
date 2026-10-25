"""Tests for app/recording.py - the automatic recording finalize/upload
pipeline (KIS Priority C). No existing test harness existed in this repo
before this file (confirmed: no tests/ directory, pytest listed in
requirements.txt as "Test-only" but unused) - this establishes the
convention: plain pytest, module-level monkeypatching of the constants
that would otherwise point at real paths/durations, no real ffmpeg/
ffprobe/S3 calls (those are integration-level concerns, exercised
instead by the real end-to-end test broadcast documented in the
handover report).

Capacity-related constants (MAX_LOCAL_RECORDING_BYTES, RECONCILE_STALE_SECONDS,
etc.) moved to app/config/capacity.py's CapacitySettings - tests patch
individual fields on the shared `capacity_settings` singleton directly
(it's a plain mutable dataclass, not frozen, specifically so this works)
rather than on the recording module itself.
"""

from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from app import recording as recording_module
from app.config.capacity import capacity_settings
from app.recording import RecordingManager, ReconciliationSweep


@pytest.fixture
def manager():
    return RecordingManager()


class TestSegmentBookkeeping:
    def test_segment_complete_accumulates_in_order(self, manager):
        manager.segment_complete("live/abc", "/recordings/live/abc/seg1.mp4")
        manager.segment_complete("live/abc", "/recordings/live/abc/seg2.mp4")

        taken = manager._take_segments("live/abc")

        assert taken == ["/recordings/live/abc/seg1.mp4", "/recordings/live/abc/seg2.mp4"]

    def test_take_segments_is_consuming(self, manager):
        manager.segment_complete("live/abc", "/recordings/live/abc/seg1.mp4")
        manager._take_segments("live/abc")

        # A second take for the same stream must come back empty - this
        # is what makes _finalize safe to call at most once per real
        # broadcast's worth of segments, not a bug to "fix" by returning
        # the same list twice.
        assert manager._take_segments("live/abc") == []

    def test_is_tracked_reflects_pending_segments(self, manager):
        assert manager.is_tracked("live/abc") is False
        manager.segment_complete("live/abc", "/recordings/live/abc/seg1.mp4")
        assert manager.is_tracked("live/abc") is True
        manager._take_segments("live/abc")
        assert manager.is_tracked("live/abc") is False

    def test_active_count_reflects_tracked_streams(self, manager):
        assert manager.active_count() == 0
        manager.segment_complete("live/a", "/recordings/live/a/seg1.mp4")
        manager.segment_complete("live/b", "/recordings/live/b/seg1.mp4")
        assert manager.active_count() == 2
        manager._take_segments("live/a")
        assert manager.active_count() == 1


class TestConcat:
    def test_single_segment_is_a_plain_copy_not_an_ffmpeg_invocation(self, manager, tmp_path):
        src = tmp_path / "seg1.mp4"
        src.write_bytes(b"fake mp4 bytes")
        work_dir = tmp_path / "work"
        work_dir.mkdir()

        with patch("app.recording.subprocess.run") as mock_run:
            result = manager._concat("live/abc", [str(src)], work_dir)

        mock_run.assert_not_called()
        assert result is not None
        assert result.read_bytes() == b"fake mp4 bytes"

    def test_multi_segment_invokes_ffmpeg_concat_with_stream_copy(self, manager, tmp_path):
        seg1 = tmp_path / "seg1.mp4"
        seg2 = tmp_path / "seg2.mp4"
        seg1.write_bytes(b"a")
        seg2.write_bytes(b"b")
        work_dir = tmp_path / "work"
        work_dir.mkdir()

        def fake_run(cmd, capture_output, timeout):
            # Simulate ffmpeg producing the output file - what the real
            # subprocess would do, without actually invoking ffmpeg.
            out_path = Path(cmd[-1])
            out_path.write_bytes(b"concatenated")
            return subprocess.CompletedProcess(cmd, 0, stdout=b"", stderr=b"")

        with patch("app.recording.subprocess.run", side_effect=fake_run) as mock_run:
            result = manager._concat("live/abc", [str(seg1), str(seg2)], work_dir)

        assert mock_run.called
        cmd = mock_run.call_args[0][0]
        assert "-c" in cmd and "copy" in cmd, "concat must be stream-copy, never a re-encode"
        assert result is not None
        assert result.read_bytes() == b"concatenated"

    def test_concat_failure_returns_none_not_a_partial_file(self, manager, tmp_path):
        seg1 = tmp_path / "seg1.mp4"
        seg2 = tmp_path / "seg2.mp4"
        seg1.write_bytes(b"a")
        seg2.write_bytes(b"b")
        work_dir = tmp_path / "work"
        work_dir.mkdir()

        with patch(
            "app.recording.subprocess.run",
            return_value=subprocess.CompletedProcess([], 1, stdout=b"", stderr=b"boom"),
        ):
            result = manager._concat("live/abc", [str(seg1), str(seg2)], work_dir)

        assert result is None


class TestProbe:
    def test_valid_video_and_audio_streams(self, manager, tmp_path):
        probe_json = json.dumps({
            "format": {"duration": "123.45"},
            "streams": [{"codec_type": "video"}, {"codec_type": "audio"}],
        }).encode()
        fake = tmp_path / "f.mp4"
        fake.write_bytes(b"x")

        with patch("app.recording.subprocess.run", return_value=subprocess.CompletedProcess([], 0, stdout=probe_json, stderr=b"")):
            result = manager._probe(fake)

        assert result == (123, True, True)

    def test_no_video_stream_is_distinguishable_from_probe_failure(self, manager, tmp_path):
        probe_json = json.dumps({
            "format": {"duration": "10"},
            "streams": [{"codec_type": "audio"}],
        }).encode()
        fake = tmp_path / "f.mp4"
        fake.write_bytes(b"x")

        with patch("app.recording.subprocess.run", return_value=subprocess.CompletedProcess([], 0, stdout=probe_json, stderr=b"")):
            result = manager._probe(fake)

        assert result is not None
        duration, has_video, has_audio = result
        assert has_video is False  # _finalize_segments must reject this, not publish a video-less "replay"

    def test_ffprobe_nonzero_exit_is_treated_as_validation_failure(self, manager, tmp_path):
        fake = tmp_path / "f.mp4"
        fake.write_bytes(b"x")

        with patch("app.recording.subprocess.run", return_value=subprocess.CompletedProcess([], 1, stdout=b"", stderr=b"corrupt")):
            result = manager._probe(fake)

        assert result is None

    def test_malformed_probe_output_does_not_raise(self, manager, tmp_path):
        fake = tmp_path / "f.mp4"
        fake.write_bytes(b"x")

        with patch("app.recording.subprocess.run", return_value=subprocess.CompletedProcess([], 0, stdout=b"not json", stderr=b"")):
            result = manager._probe(fake)  # must not raise

        assert result is None


class TestFinalizeSegmentsEndToEnd:
    """Exercises _finalize_segments (the shared core both the live hook
    path and the reconciliation sweep call) with every external call
    (ffmpeg/ffprobe/httpx) mocked, verifying the full success and
    failure-reporting flow end to end at the unit level."""

    def test_success_path_reports_archive_with_duration(self, manager, tmp_path, monkeypatch):
        monkeypatch.setattr(recording_module, "RECORDINGS_ROOT", tmp_path)
        seg = tmp_path / "seg.mp4"
        seg.write_bytes(b"video bytes")

        probe_json = json.dumps({"format": {"duration": "42"}, "streams": [{"codec_type": "video"}]}).encode()

        with patch("app.recording.subprocess.run", return_value=subprocess.CompletedProcess([], 0, stdout=probe_json, stderr=b"")), \
             patch.object(manager, "_request_upload_url", return_value=("https://s3.example/put", "https://s3.example/get", "broadcast_videos/live_recordings/key.mp4")) as mock_req, \
             patch.object(manager, "_upload", return_value=True) as mock_upload, \
             patch.object(manager, "_report_processing") as mock_processing, \
             patch.object(manager, "_report_success") as mock_success, \
             patch.object(manager, "_report_failure") as mock_failure:
            manager._finalize_segments("live/abc", [str(seg)])

        mock_processing.assert_called_once_with("live/abc")
        mock_req.assert_called_once()
        mock_upload.assert_called_once()
        mock_success.assert_called_once_with(
            "live/abc", "https://s3.example/get", 42, "broadcast_videos/live_recordings/key.mp4",
        )
        mock_failure.assert_not_called()
        # The original segment file must be removed after a successful
        # finalize - nothing left for the quota backstop to find.
        assert not seg.exists()
        # And the work dir (holding the now-uploaded concat result) must
        # be cleaned up too - nothing pending left behind on success.
        assert not any((tmp_path / "work").iterdir())

    def test_no_video_stream_reports_failure_not_success(self, manager, tmp_path, monkeypatch):
        monkeypatch.setattr(recording_module, "RECORDINGS_ROOT", tmp_path)
        seg = tmp_path / "seg.mp4"
        seg.write_bytes(b"audio only")
        probe_json = json.dumps({"format": {"duration": "10"}, "streams": [{"codec_type": "audio"}]}).encode()

        with patch("app.recording.subprocess.run", return_value=subprocess.CompletedProcess([], 0, stdout=probe_json, stderr=b"")), \
             patch.object(manager, "_request_upload_url") as mock_req, \
             patch.object(manager, "_report_success") as mock_success, \
             patch.object(manager, "_report_failure") as mock_failure:
            manager._finalize_segments("live/abc", [str(seg)])

        mock_req.assert_not_called()  # must never even attempt an upload for unplayable media
        mock_success.assert_not_called()
        mock_failure.assert_called_once_with("live/abc", "no_video_stream")

    def test_upload_failure_reports_failure_and_keeps_work_dir(self, manager, tmp_path, monkeypatch):
        monkeypatch.setattr(recording_module, "RECORDINGS_ROOT", tmp_path)
        seg = tmp_path / "seg.mp4"
        seg.write_bytes(b"video bytes")
        probe_json = json.dumps({"format": {"duration": "10"}, "streams": [{"codec_type": "video"}]}).encode()

        with patch("app.recording.subprocess.run", return_value=subprocess.CompletedProcess([], 0, stdout=probe_json, stderr=b"")), \
             patch.object(manager, "_request_upload_url", return_value=("https://s3.example/put", "https://s3.example/get", "some/key.mp4")), \
             patch.object(manager, "_upload", return_value=False), \
             patch.object(manager, "_report_processing"), \
             patch.object(manager, "_report_success") as mock_success, \
             patch.object(manager, "_report_failure") as mock_failure:
            manager._finalize_segments("live/abc", [str(seg)])

        mock_success.assert_not_called()
        mock_failure.assert_called_once_with("live/abc", "s3_upload_failed")
        # work dir (holding the concat result) must survive a failed
        # upload for retry_pending_uploads - this is real, not-yet-
        # uploaded user content, never deleted by this path or by the
        # quota backstop (see TestLocalQuotaEnforcement).
        work_root = tmp_path / "work"
        assert work_root.exists()
        work_dirs = list(work_root.iterdir())
        assert len(work_dirs) == 1
        assert (work_dirs[0] / "recording.mp4").exists()
        assert (work_dirs[0] / "meta.json").exists()
        meta = json.loads((work_dirs[0] / "meta.json").read_text())
        assert meta["stream_id"] == "live/abc"
        assert meta["duration_seconds"] == 10


class TestLocalQuotaEnforcement:
    def test_evicts_oldest_work_dirs_first_once_over_budget(self, manager, tmp_path, monkeypatch):
        monkeypatch.setattr(recording_module, "RECORDINGS_ROOT", tmp_path)
        monkeypatch.setattr(capacity_settings, "max_local_recording_bytes", 10)

        work_root = tmp_path / "work"
        work_root.mkdir()
        old_dir = work_root / "old"
        new_dir = work_root / "new"
        old_dir.mkdir()
        new_dir.mkdir()
        (old_dir / "f.bin").write_bytes(b"0" * 8)
        (new_dir / "f.bin").write_bytes(b"0" * 8)
        # Force a deterministic mtime ordering independent of real clock
        # resolution/execution speed.
        import os
        os.utime(old_dir / "f.bin", (time.time() - 100, time.time() - 100))

        manager._enforce_local_quota()

        assert not old_dir.exists(), "oldest directory must be evicted first"
        assert new_dir.exists(), "newer directory must survive if removing only the oldest satisfies the budget"

    def test_never_evicts_a_directory_holding_a_pending_upload(self, manager, tmp_path, monkeypatch):
        """The single most important guarantee of this module: a
        recording.mp4 that passed validation but hasn't been confirmed
        uploaded must survive quota pressure no matter how old it is or
        how far over budget the box is - only retry_pending_uploads
        (after a CONFIRMED upload) is allowed to remove it."""
        monkeypatch.setattr(recording_module, "RECORDINGS_ROOT", tmp_path)
        monkeypatch.setattr(capacity_settings, "max_local_recording_bytes", 10)

        work_root = tmp_path / "work"
        work_root.mkdir()
        pending_dir = work_root / "pending"
        pending_dir.mkdir()
        (pending_dir / "recording.mp4").write_bytes(b"0" * 1000)  # way over the 10-byte budget
        (pending_dir / "meta.json").write_text(json.dumps({"stream_id": "live/x", "duration_seconds": 10, "created_at": 0}))
        import os
        os.utime(pending_dir / "recording.mp4", (time.time() - 10_000, time.time() - 10_000))

        manager._enforce_local_quota()

        assert pending_dir.exists(), "a validated, not-yet-uploaded recording must never be deleted by the quota backstop"
        assert (pending_dir / "recording.mp4").exists()

    def test_garbage_dirs_without_a_recording_are_still_evicted_under_pressure(self, manager, tmp_path, monkeypatch):
        monkeypatch.setattr(recording_module, "RECORDINGS_ROOT", tmp_path)
        monkeypatch.setattr(capacity_settings, "max_local_recording_bytes", 10)

        work_root = tmp_path / "work"
        work_root.mkdir()
        garbage_dir = work_root / "garbage"
        garbage_dir.mkdir()
        (garbage_dir / "concat_list.txt").write_bytes(b"0" * 1000)
        import os
        os.utime(garbage_dir / "concat_list.txt", (time.time() - 10_000, time.time() - 10_000))

        manager._enforce_local_quota()

        assert not garbage_dir.exists(), "a dir with no recording.mp4 (failed concat/probe) has nothing worth retaining"


class TestRetryPendingUploads:
    def test_successful_retry_archives_and_removes_the_work_dir(self, manager, tmp_path, monkeypatch):
        monkeypatch.setattr(recording_module, "RECORDINGS_ROOT", tmp_path)
        work_root = tmp_path / "work"
        pending_dir = work_root / "pending1"
        pending_dir.mkdir(parents=True)
        (pending_dir / "recording.mp4").write_bytes(b"video")
        (pending_dir / "meta.json").write_text(json.dumps({"stream_id": "live/xyz", "duration_seconds": 30, "created_at": time.time()}))

        with patch.object(manager, "_try_upload_and_report", return_value=True) as mock_try:
            manager.retry_pending_uploads()

        mock_try.assert_called_once()
        called_stream_id = mock_try.call_args[0][0]
        assert called_stream_id == "live/xyz"
        assert not pending_dir.exists()

    def test_failed_retry_retains_the_work_dir(self, manager, tmp_path, monkeypatch):
        monkeypatch.setattr(recording_module, "RECORDINGS_ROOT", tmp_path)
        work_root = tmp_path / "work"
        pending_dir = work_root / "pending2"
        pending_dir.mkdir(parents=True)
        (pending_dir / "recording.mp4").write_bytes(b"video")
        (pending_dir / "meta.json").write_text(json.dumps({"stream_id": "live/xyz", "duration_seconds": 30, "created_at": time.time()}))

        with patch.object(manager, "_try_upload_and_report", return_value=False):
            manager.retry_pending_uploads()

        assert pending_dir.exists(), "a retry that fails again must still never delete the recording"
        assert (pending_dir / "recording.mp4").exists()

    def test_skips_directories_with_no_meta_or_recording(self, manager, tmp_path, monkeypatch):
        monkeypatch.setattr(recording_module, "RECORDINGS_ROOT", tmp_path)
        work_root = tmp_path / "work"
        incomplete_dir = work_root / "incomplete"
        incomplete_dir.mkdir(parents=True)
        (incomplete_dir / "concat_list.txt").write_bytes(b"x")

        with patch.object(manager, "_try_upload_and_report") as mock_try:
            manager.retry_pending_uploads()

        mock_try.assert_not_called()

    def test_skips_a_stream_already_actively_tracked_in_memory(self, manager, tmp_path, monkeypatch):
        monkeypatch.setattr(recording_module, "RECORDINGS_ROOT", tmp_path)
        work_root = tmp_path / "work"
        pending_dir = work_root / "pending3"
        pending_dir.mkdir(parents=True)
        (pending_dir / "recording.mp4").write_bytes(b"video")
        (pending_dir / "meta.json").write_text(json.dumps({"stream_id": "live/active", "duration_seconds": 5, "created_at": time.time()}))
        manager.segment_complete("live/active", "/recordings/live/active/seg.mp4")

        with patch.object(manager, "_try_upload_and_report") as mock_try:
            manager.retry_pending_uploads()

        mock_try.assert_not_called()

    def test_unreadable_meta_json_is_left_in_place_not_deleted(self, manager, tmp_path, monkeypatch):
        monkeypatch.setattr(recording_module, "RECORDINGS_ROOT", tmp_path)
        work_root = tmp_path / "work"
        pending_dir = work_root / "pending4"
        pending_dir.mkdir(parents=True)
        (pending_dir / "recording.mp4").write_bytes(b"video")
        (pending_dir / "meta.json").write_text("not valid json {{{")

        with patch.object(manager, "_try_upload_and_report") as mock_try:
            manager.retry_pending_uploads()  # must not raise

        mock_try.assert_not_called()
        assert pending_dir.exists()


class TestReconciliationSweep:
    def test_sweep_also_drives_retry_pending_uploads(self, manager, tmp_path, monkeypatch):
        monkeypatch.setattr(recording_module, "RECORDINGS_ROOT", tmp_path)
        sweep = ReconciliationSweep(manager)
        with patch.object(manager, "retry_pending_uploads") as mock_retry:
            sweep._sweep_once()
        mock_retry.assert_called_once()

    def test_skips_streams_currently_tracked_in_memory(self, manager, tmp_path, monkeypatch):
        monkeypatch.setattr(recording_module, "RECORDINGS_ROOT", tmp_path)
        monkeypatch.setattr(capacity_settings, "reconcile_stale_seconds", 0)
        live_root = tmp_path / "live" / "activekey"
        live_root.mkdir(parents=True)
        (live_root / "seg.mp4").write_bytes(b"x")
        manager.segment_complete("live/activekey", str(live_root / "seg.mp4"))

        sweep = ReconciliationSweep(manager)
        with patch.object(manager, "finalize_segments_sync") as mock_finalize:
            sweep._sweep_once()

        mock_finalize.assert_not_called()

    def test_ignores_recently_written_directories(self, manager, tmp_path, monkeypatch):
        monkeypatch.setattr(recording_module, "RECORDINGS_ROOT", tmp_path)
        monkeypatch.setattr(capacity_settings, "reconcile_stale_seconds", 600)
        live_root = tmp_path / "live" / "freshkey"
        live_root.mkdir(parents=True)
        (live_root / "seg.mp4").write_bytes(b"x")  # mtime = now

        sweep = ReconciliationSweep(manager)
        with patch.object(manager, "finalize_segments_sync") as mock_finalize:
            sweep._sweep_once()

        mock_finalize.assert_not_called()

    def test_recovers_an_orphaned_session_past_the_staleness_threshold(self, manager, tmp_path, monkeypatch):
        monkeypatch.setattr(recording_module, "RECORDINGS_ROOT", tmp_path)
        monkeypatch.setattr(capacity_settings, "reconcile_stale_seconds", 1)
        live_root = tmp_path / "live" / "orphankey"
        live_root.mkdir(parents=True)
        seg = live_root / "seg.mp4"
        seg.write_bytes(b"x")
        import os
        old_time = time.time() - 1000
        os.utime(seg, (old_time, old_time))

        sweep = ReconciliationSweep(manager)
        with patch.object(manager, "finalize_segments_sync") as mock_finalize:
            sweep._sweep_once()

        mock_finalize.assert_called_once()
        called_stream_id, called_segments = mock_finalize.call_args[0]
        assert called_stream_id == "live/orphankey"
        assert called_segments == [str(seg)]

    def test_skips_empty_directories(self, manager, tmp_path, monkeypatch):
        monkeypatch.setattr(recording_module, "RECORDINGS_ROOT", tmp_path)
        monkeypatch.setattr(capacity_settings, "reconcile_stale_seconds", 0)
        (tmp_path / "live" / "emptykey").mkdir(parents=True)

        sweep = ReconciliationSweep(manager)
        with patch.object(manager, "finalize_segments_sync") as mock_finalize:
            sweep._sweep_once()

        mock_finalize.assert_not_called()
