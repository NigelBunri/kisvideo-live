"""Tests for app/recording.py - the automatic recording finalize/upload
pipeline (KIS Priority C). No existing test harness existed in this repo
before this file (confirmed: no tests/ directory, pytest listed in
requirements.txt as "Test-only" but unused) - this establishes the
convention: plain pytest, module-level monkeypatching of the constants
that would otherwise point at real paths/durations, no real ffmpeg/
ffprobe/S3 calls (those are integration-level concerns, exercised
instead by the real end-to-end test broadcast documented in the
handover report).
"""

from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from app import recording as recording_module
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
             patch.object(manager, "_request_upload_url", return_value=("https://s3.example/put", "https://s3.example/get")) as mock_req, \
             patch.object(manager, "_upload", return_value=True) as mock_upload, \
             patch.object(manager, "_report_success") as mock_success, \
             patch.object(manager, "_report_failure") as mock_failure:
            manager._finalize_segments("live/abc", [str(seg)])

        mock_req.assert_called_once()
        mock_upload.assert_called_once()
        mock_success.assert_called_once_with("live/abc", "https://s3.example/get", 42)
        mock_failure.assert_not_called()
        # The original segment file must be removed after a successful
        # finalize - nothing left for the quota backstop to find.
        assert not seg.exists()

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
             patch.object(manager, "_request_upload_url", return_value=("https://s3.example/put", "https://s3.example/get")), \
             patch.object(manager, "_upload", return_value=False), \
             patch.object(manager, "_report_success") as mock_success, \
             patch.object(manager, "_report_failure") as mock_failure:
            manager._finalize_segments("live/abc", [str(seg)])

        mock_success.assert_not_called()
        mock_failure.assert_called_once_with("live/abc", "s3_upload_failed")
        # work dir (holding the concat result) must survive a failed
        # upload for diagnosis/manual retry - only the quota backstop,
        # not this path, ever removes it.
        work_root = tmp_path / "work"
        assert work_root.exists()
        assert any(work_root.iterdir())


class TestLocalQuotaEnforcement:
    def test_evicts_oldest_work_dirs_first_once_over_budget(self, manager, tmp_path, monkeypatch):
        monkeypatch.setattr(recording_module, "RECORDINGS_ROOT", tmp_path)
        monkeypatch.setattr(recording_module, "MAX_LOCAL_RECORDING_BYTES", 10)

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


class TestReconciliationSweep:
    def test_skips_streams_currently_tracked_in_memory(self, manager, tmp_path, monkeypatch):
        monkeypatch.setattr(recording_module, "RECORDINGS_ROOT", tmp_path)
        monkeypatch.setattr(recording_module, "RECONCILE_STALE_SECONDS", 0)
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
        monkeypatch.setattr(recording_module, "RECONCILE_STALE_SECONDS", 600)
        live_root = tmp_path / "live" / "freshkey"
        live_root.mkdir(parents=True)
        (live_root / "seg.mp4").write_bytes(b"x")  # mtime = now

        sweep = ReconciliationSweep(manager)
        with patch.object(manager, "finalize_segments_sync") as mock_finalize:
            sweep._sweep_once()

        mock_finalize.assert_not_called()

    def test_recovers_an_orphaned_session_past_the_staleness_threshold(self, manager, tmp_path, monkeypatch):
        monkeypatch.setattr(recording_module, "RECORDINGS_ROOT", tmp_path)
        monkeypatch.setattr(recording_module, "RECONCILE_STALE_SECONDS", 1)
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
        monkeypatch.setattr(recording_module, "RECONCILE_STALE_SECONDS", 0)
        (tmp_path / "live" / "emptykey").mkdir(parents=True)

        sweep = ReconciliationSweep(manager)
        with patch.object(manager, "finalize_segments_sync") as mock_finalize:
            sweep._sweep_once()

        mock_finalize.assert_not_called()
