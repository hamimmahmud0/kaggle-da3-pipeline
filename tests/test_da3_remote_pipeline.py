from __future__ import annotations

import json
import os
import sys
import tempfile
import types
import unittest
from unittest import mock
from pathlib import Path

import da3_remote_pipeline as module


class RemotePipelineSessionTest(unittest.TestCase):
    def test_append_worker_log_writes_timestamped_lines(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            worker_log = Path(tempdir) / "logs" / "worker_a.log"

            module.append_worker_log(worker_log, "task-error id=task-1\nreason line")

            content = worker_log.read_text(encoding="utf-8").splitlines()

        self.assertEqual(len(content), 2)
        self.assertIn("task-error id=task-1", content[0])
        self.assertIn("reason line", content[1])

    def test_init_session_builds_workers_and_manifest_tasks(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            workspace = Path(tempdir) / "workspace"
            manifest_path = Path(tempdir) / "manifest.json"
            config_path = Path(tempdir) / "config.json"
            manifest_path.write_text(
                json.dumps(
                    [
                        {
                            "video_name": "video-a",
                            "file_name": "clip-a",
                            "image_paths": ["/tmp/frame-1.png", "/tmp/frame-2.png"],
                            "frame_start": 12,
                            "frame_end": 34,
                        }
                    ]
                ),
                encoding="utf-8",
            )
            config_path.write_text(
                json.dumps(
                    {
                        "manifest_path": str(manifest_path),
                        "worker_count": 2,
                        "transport": "fare-drive",
                        "inference_batch_size": 24,
                        "video_frame_task_size": 10,
                        "export_format": "npz",
                    }
                ),
                encoding="utf-8",
            )

            session = module.init_session(workspace, str(config_path))

        self.assertEqual(session["summary"]["total"], 1)
        self.assertEqual(session["tasks"][0]["status"], "pending")
        self.assertEqual(session["tasks"][0]["frame_start"], 12)
        self.assertEqual(session["tasks"][0]["frame_end"], 34)
        self.assertEqual(session["tasks"][0]["export_format"], "npz")
        self.assertEqual(sorted(session["workers"]), ["worker_a", "worker_b"])
        self.assertEqual(session["transport"], "fare-drive")
        self.assertEqual(session["inference_batch_size"], 24)
        self.assertEqual(session["video_frame_task_size"], 10)
        self.assertEqual(session["export_format"], "npz")
        self.assertEqual(session["checkpoint"]["interval"], 20)
        self.assertTrue(session["workers"]["worker_a"]["backend_log_path"].endswith("logs/backend_worker_a.log"))

    def test_backend_script_uses_same_log_path_recorded_in_session(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            workspace = Path(tempdir) / "workspace"
            session = module.init_session(workspace)

            script = module.backend_script(workspace, "worker_a", 0, 8008, 7)

        self.assertIn(session["workers"]["worker_a"]["backend_log_path"], script)
        self.assertIn("--batch-size 7", script)
        self.assertIn("PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True", script)

    def test_backend_script_exports_hf_token_when_present(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            workspace = Path(tempdir) / "workspace"

            script = module.backend_script(workspace, "worker_a", 0, 8008, 7, hf_token="hf_demo_token")

        self.assertIn("HF_TOKEN=hf_demo_token", script)
        self.assertIn("HUGGINGFACE_HUB_TOKEN=hf_demo_token", script)

    def test_sync_worker_runtime_state_marks_dead_workers_as_error(self) -> None:
        session = {
            "workers": {
                "worker_a": {
                    "pid": 1234,
                    "status": "starting",
                    "claimed_task": "task-1",
                },
                "worker_b": {
                    "pid": 5678,
                    "status": "running",
                    "claimed_task": "task-2",
                },
                "worker_c": {
                    "pid": 9012,
                    "status": "idle",
                    "claimed_task": None,
                },
            }
        }

        with mock.patch.object(module, "is_process_alive", side_effect=[False, True, False]):
            updated = module.sync_worker_runtime_state(session)

        self.assertEqual(updated["workers"]["worker_a"]["status"], "error")
        self.assertIsNone(updated["workers"]["worker_a"]["pid"])
        self.assertIsNone(updated["workers"]["worker_a"]["claimed_task"])
        self.assertEqual(updated["workers"]["worker_b"]["status"], "running")
        self.assertEqual(updated["workers"]["worker_c"]["status"], "idle")
        self.assertIsNone(updated["workers"]["worker_c"]["pid"])

    def test_status_payload_reports_token_configured_when_endpoint_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            workspace = Path(tempdir) / "workspace"
            module.ensure_workspace(workspace)
            module.save_json(
                module.workspace_paths(workspace)["session"],
                {
                    "fare_drive": {
                        "access_token": "token-value",
                        "client_home": "/tmp/fare-drive-home",
                        "endpoint": "",
                    },
                    "workers": {},
                    "tasks": [],
                },
            )

            payload = module.status_payload(workspace)

        self.assertEqual(payload["fare_drive_status"], "client:token-configured")

    def test_build_tasks_from_inputs_root_detects_videos_and_images(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir) / "incoming"
            root.mkdir()
            (root / "clip one.mp4").write_bytes(b"video")
            image_dir = root / "frameset-a"
            image_dir.mkdir()
            (image_dir / "frame_000001.png").write_bytes(b"img")
            (image_dir / "frame_000002.png").write_bytes(b"img")

            with mock.patch.object(module, "get_video_frame_count", return_value=300):
                tasks = module.build_tasks_from_inputs_root(root, video_frame_task_size=64)

        self.assertEqual(len(tasks), 6)
        video_task = next(task for task in tasks if task["video_path"])
        image_task = next(task for task in tasks if task["image_paths"])
        self.assertEqual(video_task["file_name"], "clip_one_batch_000000_000063")
        self.assertEqual(video_task["frame_start"], 0)
        self.assertEqual(video_task["frame_end"], 63)
        self.assertEqual(image_task["file_name"], "frameset-a")
        self.assertEqual(len(image_task["image_paths"]), 2)

    def test_init_session_generates_manifest_from_drive_when_manifest_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            workspace = Path(tempdir) / "workspace"
            config_path = Path(tempdir) / "config.json"
            config_path.write_text(json.dumps({"drive_folder_url": "https://drive.google.com/drive/folders/demo"}), encoding="utf-8")

            generated_manifest = workspace / "generated-manifest.json"
            generated_payload = [
                {
                    "id": "video-0000-demo",
                    "video_name": "drive-folder",
                    "file_name": "clip",
                    "video_path": str(workspace / "incoming" / "drive-folder" / "clip.mp4"),
                    "image_paths": [],
                }
            ]

            with mock.patch.object(module, "generate_manifest_from_drive_folder", return_value=generated_manifest), mock.patch.object(
                module, "build_tasks_from_manifest", return_value=generated_payload
            ):
                session = module.init_session(workspace, str(config_path))

        self.assertEqual(session["manifest_path"], str(generated_manifest))
        self.assertEqual(session["tasks"][0]["video_path"], generated_payload[0]["video_path"])

    def test_init_session_generates_manifest_from_fare_drive_when_input_root_is_configured(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            workspace = Path(tempdir) / "workspace"
            config_path = Path(tempdir) / "config.json"
            config_path.write_text(
                json.dumps(
                    {
                        "fare_drive": {
                            "input_root": "inputs/jobs/current",
                            "client_home": "/tmp/fare-drive-home",
                        }
                    }
                ),
                encoding="utf-8",
            )

            generated_manifest = workspace / "generated-manifest.json"
            generated_payload = [
                {
                    "id": "video-0000-demo",
                    "video_name": "fare-drive",
                    "file_name": "clip",
                    "video_path": str(workspace / "incoming" / "fare-drive" / "clip.mp4"),
                    "image_paths": [],
                }
            ]

            with mock.patch.object(module, "generate_manifest_from_fare_drive", return_value=generated_manifest), mock.patch.object(
                module, "build_tasks_from_manifest", return_value=generated_payload
            ):
                session = module.init_session(workspace, str(config_path))

        self.assertEqual(session["manifest_path"], str(generated_manifest))
        self.assertEqual(session["tasks"][0]["video_name"], "fare-drive")

    def test_init_session_restores_checkpoint_from_fare_drive_when_available(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            workspace = Path(tempdir) / "workspace"
            config_path = Path(tempdir) / "config.json"
            config_path.write_text(
                json.dumps(
                    {
                        "worker_count": 2,
                        "inference_batch_size": 7,
                        "video_frame_task_size": 4,
                        "export_format": "npz",
                        "fare_drive": {
                            "client_home": "/tmp/fd-home",
                            "access_token": "token",
                            "upload_root": "da3-output",
                        },
                    }
                ),
                encoding="utf-8",
            )
            restored = {
                "tasks": [
                    {
                        "id": "task-1",
                        "status": "completed",
                        "video_name": "video-a",
                        "file_name": "clip-a",
                        "image_paths": ["frame.png"],
                        "export_format": "png",
                    }
                ],
                "workers": {"old": {}},
                "fare_drive": {"upload_root": "old-root"},
                "summary": {"completed": 1},
            }

            with mock.patch.object(module, "restore_session_from_fare_drive", return_value=restored), mock.patch.object(
                module, "build_tasks_from_manifest"
            ) as build_manifest:
                session = module.init_session(workspace, str(config_path))

        self.assertEqual(session, restored)
        build_manifest.assert_not_called()

    def test_maybe_upload_session_checkpoint_uploads_every_twentieth_completion(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            workspace = Path(tempdir) / "workspace"
            module.ensure_workspace(workspace)
            session = {
                "fare_drive": {"upload_root": "da3-output", "client_home": "/tmp/fd-home"},
                "summary": {"completed": 20},
                "tasks": [],
                "workers": {},
                "checkpoint": {
                    "interval": 20,
                    "last_uploaded_completed_count": 0,
                    "remote_path": "da3-output/_pipeline_state/session.json",
                    "uploaded_at": None,
                },
            }
            module.save_json(module.workspace_paths(workspace)["session"], session)

            with mock.patch.object(module, "upload_file_via_fare_drive", return_value="da3-output/_pipeline_state/session.json") as upload_mock:
                uploaded_path = module.maybe_upload_session_checkpoint(workspace)

            saved = module.read_session(workspace)

        self.assertEqual(uploaded_path, "da3-output/_pipeline_state/session.json")
        upload_mock.assert_called_once()
        self.assertEqual(saved["checkpoint"]["last_uploaded_completed_count"], 20)
        self.assertEqual(saved["checkpoint"]["remote_path"], "da3-output/_pipeline_state/session.json")

    def test_restore_session_from_fare_drive_ignores_missing_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            workspace = Path(tempdir) / "workspace"
            config = {
                "fare_drive": {
                    "client_home": "/tmp/fd-home",
                    "access_token": "token",
                    "upload_root": "da3-output",
                }
            }
            failure = mock.Mock(returncode=1, stdout="", stderr="Request failed: 404 Not Found")

            with mock.patch.object(module.subprocess, "run", return_value=failure):
                restored = module.restore_session_from_fare_drive(workspace, config)

        self.assertIsNone(restored)

    def test_update_session_config_preserves_tasks_and_updates_batch_size(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            workspace = Path(tempdir) / "workspace"
            initial_config = Path(tempdir) / "initial.json"
            update_config = Path(tempdir) / "updated.json"
            manifest_path = Path(tempdir) / "manifest.json"
            manifest_path.write_text(
                json.dumps([{"video_name": "video-a", "file_name": "clip-a", "image_paths": ["frame.png"]}]),
                encoding="utf-8",
            )
            initial_config.write_text(
                json.dumps(
                    {
                        "manifest_path": str(manifest_path),
                        "inference_batch_size": 16,
                        "video_frame_task_size": 64,
                        "export_format": "npz",
                    }
                ),
                encoding="utf-8",
            )
            update_config.write_text(
                json.dumps(
                    {
                        "manifest_path": "",
                        "inference_batch_size": 7,
                        "video_frame_task_size": 16,
                        "export_format": "npz",
                        "fare_drive": {"input_root": "inputs/jobs/current", "upload_root": "outputs/jobs/current"},
                    }
                ),
                encoding="utf-8",
            )

            session = module.init_session(workspace, str(initial_config))
            task_id = session["tasks"][0]["id"]
            updated = module.update_session_config(workspace, str(update_config))

        self.assertEqual(updated["inference_batch_size"], 7)
        self.assertEqual(updated["video_frame_task_size"], 16)
        self.assertEqual(updated["export_format"], "npz")
        self.assertEqual(updated["fare_drive"]["input_root"], "inputs/jobs/current")
        self.assertEqual(updated["fare_drive"]["upload_root"], "outputs/jobs/current")
        self.assertEqual(updated["manifest_path"], str(manifest_path))
        self.assertEqual(updated["tasks"][0]["export_format"], "npz")
        self.assertEqual(updated["tasks"][0]["id"], task_id)

    def test_cleanup_launch_processes_kills_worker_and_backend_pids(self) -> None:
        session = {
            "workers": {
                "worker_a": {
                    "pid": 111,
                    "backend_port": 8008,
                    "claimed_task": "task-1",
                    "status": "running",
                }
            },
            "tasks": [
                {
                    "id": "task-1",
                    "status": "running",
                    "claimed_by": "worker_a",
                    "started_at": "2026-01-01T00:00:00Z",
                    "last_error": "old",
                }
            ],
        }

        with mock.patch.object(module, "kill_pid") as kill_pid, mock.patch.object(
            module, "pids_listening_on_port", return_value=[222, 333]
        ), mock.patch.object(
            module, "pids_matching_pattern", side_effect=[[444], [555]]
        ):
            module.cleanup_launch_processes(session)

        self.assertEqual(kill_pid.call_args_list[0].args[0], 111)
        self.assertEqual(kill_pid.call_args_list[1].args[0], 444)
        self.assertEqual(kill_pid.call_args_list[2].args[0], 555)
        self.assertEqual(kill_pid.call_args_list[3].args[0], 222)
        self.assertEqual(kill_pid.call_args_list[4].args[0], 333)
        self.assertEqual(session["workers"]["worker_a"]["status"], "idle")
        self.assertIsNone(session["workers"]["worker_a"]["pid"])
        self.assertIsNone(session["workers"]["worker_a"]["claimed_task"])
        self.assertEqual(session["tasks"][0]["status"], "pending")
        self.assertIsNone(session["tasks"][0]["claimed_by"])
        self.assertIsNone(session["tasks"][0]["started_at"])
        self.assertIsNone(session["tasks"][0]["last_error"])

    def test_stop_cleans_up_workers_and_clears_runtime_launch_marker(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            workspace = Path(tempdir) / "workspace"
            module.ensure_workspace(workspace)
            module.save_json(
                module.workspace_paths(workspace)["session"],
                {
                    "workers": {
                        "worker_a": {
                            "pid": 111,
                            "backend_port": 8008,
                            "claimed_task": "task-1",
                            "status": "running",
                        }
                    },
                    "tasks": [
                        {
                            "id": "task-1",
                            "status": "running",
                            "claimed_by": "worker_a",
                            "started_at": "2026-01-01T00:00:00Z",
                            "last_error": "old",
                        }
                    ],
                    "summary": {"pending": 0, "running": 1, "completed": 0, "failed": 0, "total": 1},
                },
            )
            module.save_json(module.workspace_paths(workspace)["runtime"], {"last_launch_at": "2026-01-01T00:00:00Z"})

            with mock.patch.object(module, "cleanup_launch_processes") as cleanup_mock:
                def cleanup_side_effect(session: dict) -> None:
                    session["workers"]["worker_a"]["pid"] = None
                    session["workers"]["worker_a"]["claimed_task"] = None
                    session["workers"]["worker_a"]["status"] = "idle"
                    session["tasks"][0]["status"] = "pending"
                    session["tasks"][0]["claimed_by"] = None
                    session["tasks"][0]["started_at"] = None
                    session["tasks"][0]["last_error"] = None

                cleanup_mock.side_effect = cleanup_side_effect
                payload = module.stop(workspace)

            runtime = module.load_json(module.workspace_paths(workspace)["runtime"], {})
            pipeline_pid = module.load_json(module.workspace_paths(workspace)["pipeline_pid"], {})

        self.assertEqual(payload["summary"]["running"], 0)
        self.assertEqual(payload["summary"]["pending"], 1)
        self.assertIsNone(runtime["last_launch_at"])
        self.assertEqual(pipeline_pid["workers"], {})

    def test_cleanup_uploaded_output_artifacts_removes_uploaded_output_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            workspace = Path(tempdir) / "workspace"
            output_dir = workspace / "output" / "video-a" / "clip-a"
            output_dir.mkdir(parents=True)
            (output_dir / "result.glb").write_bytes(b"glb")

            module.cleanup_uploaded_output_artifacts(
                workspace,
                {"video_name": "video-a", "file_name": "clip-a"},
            )

        self.assertFalse(output_dir.exists())
        self.assertFalse((workspace / "output" / "video-a").exists())

    def test_reconcile_task_runtime_state_resets_orphaned_running_tasks(self) -> None:
        session = {
            "workers": {
                "worker_a": {"status": "idle", "claimed_task": None},
                "worker_b": {"status": "running", "claimed_task": "task-2"},
            },
            "tasks": [
                {"id": "task-1", "status": "running", "claimed_by": "worker_a", "started_at": "x"},
                {"id": "task-2", "status": "running", "claimed_by": "worker_b", "started_at": "y"},
            ],
        }

        updated = module.reconcile_task_runtime_state(session)

        self.assertEqual(updated["tasks"][0]["status"], "pending")
        self.assertIsNone(updated["tasks"][0]["claimed_by"])
        self.assertIsNone(updated["tasks"][0]["started_at"])
        self.assertEqual(updated["tasks"][1]["status"], "running")

    def test_retry_failed_tasks_requeues_failures_and_clears_error_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            workspace = Path(tempdir) / "workspace"
            module.ensure_workspace(workspace)
            module.save_json(
                module.workspace_paths(workspace)["session"],
                {
                    "tasks": [
                        {
                            "id": "task-1",
                            "status": "failed",
                            "claimed_by": "worker_a",
                            "started_at": "2026-01-01T00:00:00Z",
                            "completed_at": "2026-01-01T00:01:00Z",
                            "elapsed_ms": 60000,
                            "last_error": "boom",
                        },
                        {
                            "id": "task-2",
                            "status": "completed",
                            "claimed_by": "worker_b",
                            "completed_at": "2026-01-01T00:02:00Z",
                        },
                    ],
                    "workers": {},
                    "summary": {"pending": 0, "running": 0, "completed": 1, "failed": 1, "total": 2},
                },
            )

            updated = module.retry_failed_tasks(workspace)

        self.assertEqual(updated["summary"]["pending"], 1)
        self.assertEqual(updated["summary"]["failed"], 0)
        self.assertEqual(updated["tasks"][0]["status"], "pending")
        self.assertIsNone(updated["tasks"][0]["claimed_by"])
        self.assertIsNone(updated["tasks"][0]["started_at"])
        self.assertIsNone(updated["tasks"][0]["completed_at"])
        self.assertIsNone(updated["tasks"][0]["elapsed_ms"])
        self.assertIsNone(updated["tasks"][0]["last_error"])
        self.assertEqual(updated["tasks"][1]["status"], "completed")

    def test_prepare_video_for_frame_extraction_rebuilds_invalid_sidecar_atomically(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            video_path = Path(tempdir) / "clip.dav"
            video_path.write_bytes(b"dav")
            converted_path = video_path.with_suffix(".mp4")
            converted_path.write_bytes(b"bad")

            def fake_run(command, **_kwargs):
                Path(command[-1]).write_bytes(b"good")
                return mock.Mock(returncode=0)

            with mock.patch.object(module.shutil, "which", return_value="/usr/bin/ffmpeg"), mock.patch.object(
                module, "is_video_readable", side_effect=[False, True]
            ), mock.patch.object(
                module.subprocess, "run", side_effect=fake_run
            ) as run_mock:
                result = module.prepare_video_for_frame_extraction(video_path)

        self.assertEqual(result, converted_path)
        self.assertEqual(run_mock.call_args.args[0][-1], str(converted_path.with_name(f".{converted_path.name}.{os.getpid()}.tmp")))

    def test_extract_video_frames_falls_back_to_original_video_when_sidecar_fails(self) -> None:
        class FakeCapture:
            def __init__(self, opened: bool, frames: list[object | None]) -> None:
                self._opened = opened
                self._frames = iter(frames)

            def isOpened(self) -> bool:
                return self._opened

            def release(self) -> None:
                return None

            def set(self, *_args) -> None:
                return None

            def read(self) -> tuple[bool, object | None]:
                frame = next(self._frames)
                if frame is None:
                    return False, None
                return True, frame

        with tempfile.TemporaryDirectory() as tempdir:
            video_path = Path(tempdir) / "clip.dav"
            converted_path = video_path.with_suffix(".mp4")
            video_path.write_bytes(b"dav")
            converted_path.write_bytes(b"mp4")
            output_dir = Path(tempdir) / "frames"
            captures = [
                FakeCapture(False, []),
                FakeCapture(True, ["frame", None]),
            ]

            def fake_imwrite(path, *_args, **_kwargs):
                Path(path).write_bytes(b"good")
                return True

            fake_cv2 = types.SimpleNamespace(
                VideoCapture=lambda *_args: captures.pop(0),
                CAP_PROP_POS_FRAMES=1,
                IMWRITE_PNG_COMPRESSION=16,
                imwrite=fake_imwrite,
            )

            with mock.patch.object(module, "prepare_video_for_frame_extraction", return_value=converted_path), mock.patch.dict(
                sys.modules, {"cv2": fake_cv2}
            ):
                frame_paths = module.extract_video_frames(video_path, output_dir, frame_start=0, frame_end=0)

        self.assertEqual(len(frame_paths), 1)
        self.assertTrue(frame_paths[0].endswith("frame_000000.png"))

    def test_extract_video_frames_discards_unreadable_existing_frames(self) -> None:
        class FakeCapture:
            def __init__(self, opened: bool, frames: list[object | None]) -> None:
                self._opened = opened
                self._frames = iter(frames)

            def isOpened(self) -> bool:
                return self._opened

            def release(self) -> None:
                return None

            def set(self, *_args) -> None:
                return None

            def read(self) -> tuple[bool, object | None]:
                frame = next(self._frames)
                if frame is None:
                    return False, None
                return True, frame

        with tempfile.TemporaryDirectory() as tempdir:
            video_path = Path(tempdir) / "clip.mp4"
            output_dir = Path(tempdir) / "frames"
            video_path.write_bytes(b"video")
            output_dir.mkdir()
            (output_dir / "frame_000000.png").write_bytes(b"bad")

            def fake_imwrite(path, *_args, **_kwargs):
                Path(path).write_bytes(b"good")
                return True

            fake_cv2 = types.SimpleNamespace(
                VideoCapture=lambda *_args: FakeCapture(True, ["frame", None]),
                CAP_PROP_POS_FRAMES=1,
                IMWRITE_PNG_COMPRESSION=16,
                imwrite=fake_imwrite,
            )

            with mock.patch.object(module, "prepare_video_for_frame_extraction", return_value=video_path), mock.patch.object(
                module, "is_image_readable", side_effect=[False, True]
            ), mock.patch.dict(sys.modules, {"cv2": fake_cv2}):
                frame_paths = module.extract_video_frames(video_path, output_dir, frame_start=0, frame_end=0)
                self.assertTrue(Path(frame_paths[0]).exists())

        self.assertEqual(len(frame_paths), 1)

    def test_resolve_task_image_paths_restores_missing_drive_video(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            workspace = Path(tempdir) / "workspace"
            video_path = workspace / "incoming" / "drive-folder" / "clip.dav"
            task = {
                "id": "video-0000-demo-0000",
                "video_path": str(video_path),
                "image_paths": [],
                "frame_start": 4,
                "frame_end": 7,
            }
            session = {"drive_folder_url": "https://drive.google.com/drive/folders/demo"}

            def fake_download(_url: str, output_dir: Path) -> None:
                output_dir.mkdir(parents=True, exist_ok=True)
                video_path.write_bytes(b"video")

            with mock.patch.object(module, "download_drive_folder", side_effect=fake_download) as download_mock, mock.patch.object(
                module, "extract_video_frames", return_value=["frame_000004.png"]
            ) as extract_mock:
                frame_paths = module.resolve_task_image_paths(workspace, session, task)

        self.assertEqual(frame_paths, ["frame_000004.png"])
        self.assertEqual(download_mock.call_args.args[1], workspace / "incoming" / "drive-folder")
        self.assertEqual(extract_mock.call_args.args[0], video_path)
        self.assertEqual(extract_mock.call_args.kwargs["frame_start"], 4)
        self.assertEqual(extract_mock.call_args.kwargs["frame_end"], 7)


if __name__ == "__main__":
    unittest.main()
