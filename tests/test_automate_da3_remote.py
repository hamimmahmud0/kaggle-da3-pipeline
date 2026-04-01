from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import da3_pipe as module


class ResolveConfigTest(unittest.TestCase):
    def test_resolve_config_prefers_json_file_and_cli(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            config_path = Path(tempdir) / "config.json"
            config_path.write_text(
                json.dumps(
                    {
                        "host": "10.0.0.8",
                        "port": 2200,
                        "password": "secret",
                        "worker_count": 4,
                        "transport": "fare-drive",
                        "local_fare_drive_input_root": "inputs/jobs/current",
                        "inference_batch_size": 32,
                        "video_frame_task_size": 8,
                        "export_format": "npz",
                        "HF_TOKEN": "hf_demo_token",
                    }
                ),
                encoding="utf-8",
            )
            parser = module.build_parser()
            args = parser.parse_args(
                [
                    "status",
                    "--config-file",
                    str(config_path),
                    "--host",
                    "127.0.0.1",
                ]
            )
            cfg = module.resolve_config(args)

        self.assertEqual(cfg["host"], "127.0.0.1")
        self.assertEqual(cfg["port"], 2200)
        self.assertEqual(cfg["worker_count"], 4)
        self.assertEqual(cfg["transport"], "fare-drive")
        self.assertEqual(cfg["local_fare_drive_input_root"], "inputs/jobs/current")
        self.assertEqual(cfg["inference_batch_size"], 32)
        self.assertEqual(cfg["video_frame_task_size"], 8)
        self.assertEqual(cfg["export_format"], "npz")
        self.assertEqual(cfg["hf_token"], "hf_demo_token")

    def test_resolve_config_uses_default_da3_remote_json_when_present(self) -> None:
        parser = module.build_parser()
        args = parser.parse_args(["status"])
        original_default = module.DEFAULT_CONFIG_FILE

        with tempfile.TemporaryDirectory() as tempdir:
            config_path = Path(tempdir) / "da3_remote.json"
            config_path.write_text(
                json.dumps(
                    {
                        "host": "10.0.0.9",
                        "port": 2201,
                        "password": "secret",
                        "worker_count": 3,
                        "export_format": "glb",
                    }
                ),
                encoding="utf-8",
            )
            module.DEFAULT_CONFIG_FILE = config_path
            try:
                cfg = module.resolve_config(args)
            finally:
                module.DEFAULT_CONFIG_FILE = original_default

        self.assertEqual(cfg["host"], "10.0.0.9")
        self.assertEqual(cfg["port"], 2201)
        self.assertEqual(cfg["worker_count"], 3)
        self.assertEqual(cfg["export_format"], "glb")

    def test_build_remote_session_config_includes_fare_drive_paths(self) -> None:
        payload = module.build_remote_session_config(
            {
                "transport": "fare-drive",
                "drive_folder_url": "",
                "manifest_path": "",
                "worker_count": 2,
                "inference_batch_size": 7,
                "video_frame_task_size": 16,
                "export_format": "npz",
                "hf_token": "hf_demo_token",
                "local_fare_drive_endpoint": "",
                "local_fare_drive_access_token": "token",
                "remote_fare_drive_client_home": "/tmp/fd-home",
                "local_fare_drive_input_root": "inputs/jobs/current",
                "local_fare_drive_upload_root": "outputs/jobs/current",
            }
        )

        self.assertEqual(payload["inference_batch_size"], 7)
        self.assertEqual(payload["video_frame_task_size"], 16)
        self.assertEqual(payload["export_format"], "npz")
        self.assertEqual(payload["hf_token"], "hf_demo_token")
        self.assertEqual(payload["fare_drive"]["input_root"], "inputs/jobs/current")
        self.assertEqual(payload["fare_drive"]["upload_root"], "outputs/jobs/current")

    def test_build_datalog_tail_script_includes_backend_logs(self) -> None:
        script = module.build_datalog_tail_script("/kaggle/working/DA3", 150)

        self.assertIn("tail -n 150 -F", script)
        self.assertIn("logs/backend_worker_a.log", script)
        self.assertIn("logs/backend_worker_b.log", script)

    def test_datop_defaults_to_two_second_refresh(self) -> None:
        parser = module.build_parser()

        args = parser.parse_args(["datop"])

        self.assertEqual(args.refresh_seconds, 2.0)

    def test_build_remote_env_script_installs_depth_anything(self) -> None:
        script = module.build_remote_env_script(
            {
                "remote_miniforge": "/kaggle/working/miniforge3",
                "remote_workspace": "/kaggle/working/DA3",
                "remote_env_name": "da3-remote",
            }
        )

        self.assertIn("Depth-Anything-3", script)
        self.assertIn(module.DEPTH_ANYTHING_REPO_URL, script)
        self.assertIn("gdown", script)
        self.assertIn("python -m pip install --no-cache-dir -e .", script)

    def test_retry_failed_command_is_available_in_parser(self) -> None:
        parser = module.build_parser()

        args = parser.parse_args(["retry-failed"])

        self.assertEqual(args.command, "retry-failed")
        self.assertIs(args.handler, module.command_retry_failed)

    def test_stop_command_is_available_in_parser(self) -> None:
        parser = module.build_parser()

        args = parser.parse_args(["stop"])

        self.assertEqual(args.command, "stop")
        self.assertIs(args.handler, module.command_stop)

    def test_fare_drive_login_command_is_available_in_parser(self) -> None:
        parser = module.build_parser()

        args = parser.parse_args(["fare-drive-login"])

        self.assertEqual(args.command, "fare-drive-login")
        self.assertIs(args.handler, module.command_fare_drive_login)

    def test_retry_failed_remote_tasks_invokes_remote_pipeline_command(self) -> None:
        runner = mock.Mock()
        cfg = {
            "remote_workspace": "/kaggle/working/DA3",
            "remote_miniforge": "/kaggle/working/miniforge3",
            "remote_env_name": "da3-remote",
        }

        module.retry_failed_remote_tasks(runner, cfg)

        script = runner.bash.call_args.args[0]
        self.assertIn("python da3_remote_pipeline.py retry-failed", script)
        self.assertIn("--workspace /kaggle/working/DA3", script)

    def test_stop_remote_pipeline_invokes_remote_pipeline_command(self) -> None:
        runner = mock.Mock()
        cfg = {
            "remote_workspace": "/kaggle/working/DA3",
            "remote_miniforge": "/kaggle/working/miniforge3",
            "remote_env_name": "da3-remote",
        }

        module.stop_remote_pipeline(runner, cfg)

        script = runner.bash.call_args.args[0]
        self.assertIn("python da3_remote_pipeline.py stop", script)
        self.assertIn("--workspace /kaggle/working/DA3", script)

    def test_command_fare_drive_login_reconfigures_client_and_syncs_session(self) -> None:
        parser = module.build_parser()
        args = parser.parse_args(["fare-drive-login", "--password", "secret", "--local-fare-drive-access-token", "token"])

        with mock.patch.object(module, "RemoteRunner") as runner_cls, mock.patch.object(
            module, "configure_remote_fare_drive_client"
        ) as configure_mock, mock.patch.object(module, "sync_remote_session_config") as sync_mock, mock.patch.object(
            module, "print_status"
        ) as status_mock:
            runner = runner_cls.return_value.__enter__.return_value
            module.command_fare_drive_login(args)

        configure_mock.assert_called_once_with(runner, mock.ANY)
        sync_mock.assert_called_once_with(runner, mock.ANY)
        status_mock.assert_called_once_with(runner, mock.ANY)


if __name__ == "__main__":
    unittest.main()
