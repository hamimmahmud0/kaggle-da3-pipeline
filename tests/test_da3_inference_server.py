from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

import da3_inference_server as module


class InferenceServerTest(unittest.TestCase):
    def test_run_inference_passes_batch_size_when_supported(self) -> None:
        calls = []

        class Model:
            def inference(self, image, export_dir, export_format, batch_size):
                calls.append(
                    {
                        "image": image,
                        "export_dir": export_dir,
                        "export_format": export_format,
                        "batch_size": batch_size,
                    }
                )

        with tempfile.TemporaryDirectory() as tempdir:
            cwd = Path.cwd()
            try:
                os.chdir(tempdir)
                module._run_inference(
                    Model(),
                    {"image_paths": ["frame.png"], "video_name": "video", "file_name": "clip"},
                    default_export_format="npz",
                    default_batch_size=8,
                )
            finally:
                os.chdir(cwd)

        self.assertEqual(calls[0]["batch_size"], 8)

    def test_run_inference_omits_batch_size_when_unsupported(self) -> None:
        calls = []

        class Model:
            def inference(self, image, export_dir, export_format):
                calls.append(
                    {
                        "image": image,
                        "export_dir": export_dir,
                        "export_format": export_format,
                    }
                )

        with tempfile.TemporaryDirectory() as tempdir:
            cwd = Path.cwd()
            try:
                os.chdir(tempdir)
                module._run_inference(
                    Model(),
                    {"image_paths": ["frame.png"], "video_name": "video", "file_name": "clip"},
                    default_export_format="npz",
                    default_batch_size=8,
                )
            finally:
                os.chdir(cwd)

        self.assertEqual(calls[0]["export_format"], "npz")

    def test_run_inference_retries_without_batch_size_when_runtime_rejects_it(self) -> None:
        calls = []

        class Model:
            def inference(self, image, export_dir, export_format, **kwargs):
                calls.append(dict(kwargs))
                if "batch_size" in kwargs:
                    raise TypeError("unexpected keyword argument 'batch_size'")

        with tempfile.TemporaryDirectory() as tempdir:
            cwd = Path.cwd()
            try:
                os.chdir(tempdir)
                module._run_inference(
                    Model(),
                    {"image_paths": ["frame.png"], "video_name": "video", "file_name": "clip"},
                    default_export_format="npz",
                    default_batch_size=8,
                )
            finally:
                os.chdir(cwd)

        self.assertEqual(calls, [{"batch_size": 8}, {}])


if __name__ == "__main__":
    unittest.main()
