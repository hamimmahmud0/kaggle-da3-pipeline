from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import da3_remote_pipeline as module


class RemotePipelineSessionTest(unittest.TestCase):
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
                    }
                ),
                encoding="utf-8",
            )

            session = module.init_session(workspace, str(config_path))

        self.assertEqual(session["summary"]["total"], 1)
        self.assertEqual(session["tasks"][0]["status"], "pending")
        self.assertEqual(sorted(session["workers"]), ["worker_a", "worker_b"])
        self.assertEqual(session["transport"], "fare-drive")


if __name__ == "__main__":
    unittest.main()
