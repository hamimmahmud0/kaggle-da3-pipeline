from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import automate_da3_remote as module


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
                        "inference_batch_size": 32,
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
        self.assertEqual(cfg["inference_batch_size"], 32)


if __name__ == "__main__":
    unittest.main()
