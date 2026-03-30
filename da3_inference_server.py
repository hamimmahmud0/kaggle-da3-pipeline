import argparse
import inspect
import json
import os
import socket
import sys
import time
import traceback

DEFAULT_EXPORT_FORMAT = "npz"


def _load_model(model_id: str, device_no: int):
    try:
        from depth_anything_3.api import DepthAnything3
    except Exception as exc:
        raise RuntimeError(
            "Failed to import depth_anything_3. Ensure the package is installed in your environment."
        ) from exc

    model = DepthAnything3.from_pretrained(model_id).to(f"cuda:{device_no}")
    return model


def _recv_json(conn: socket.socket, max_bytes: int, timeout_s: int) -> dict:
    conn.settimeout(timeout_s)
    chunks = []
    total = 0
    while True:
        data = conn.recv(4096)
        if not data:
            break
        chunks.append(data)
        total += len(data)
        if total > max_bytes:
            raise ValueError(f"Payload exceeds max size of {max_bytes} bytes.")
        if b"\n" in data:
            break
    raw = b"".join(chunks).decode("utf-8").strip()
    if not raw:
        raise ValueError("Empty request payload.")
    # Allow newline-delimited JSON; only parse the first line if present.
    line = raw.splitlines()[0]
    return json.loads(line)


def _send_json(conn: socket.socket, payload: dict) -> None:
    data = json.dumps(payload, ensure_ascii=True) + "\n"
    conn.sendall(data.encode("utf-8"))


def _validate_request(req: dict) -> dict:
    required = ["image_paths", "video_name", "file_name"]
    missing = [k for k in required if k not in req]
    if missing:
        raise ValueError(f"Missing required fields: {', '.join(missing)}.")
    return req


def _run_inference(model, req: dict, default_export_format: str, default_batch_size: int) -> None:
    image_paths = req["image_paths"]
    video_name = req["video_name"]
    file_name = req["file_name"]
    export_format = req.get("export_format", default_export_format)
    batch_size = int(req.get("batch_size", default_batch_size))

    export_dir = os.path.join("output", str(video_name), str(file_name))
    os.makedirs(export_dir, exist_ok=True)
    print(
        f"[request] file={file_name} frames={len(image_paths)} batch_size={batch_size} export_format={export_format}",
        flush=True,
    )

    inference_kwargs = {
        "image": image_paths,
        "export_dir": export_dir,
        "export_format": export_format,
    }
    try:
        signature = inspect.signature(model.inference)
    except (TypeError, ValueError):
        signature = None
    supports_batch_size = signature is None or "batch_size" in signature.parameters or any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in (signature.parameters.values() if signature else [])
    )
    if supports_batch_size:
        inference_kwargs["batch_size"] = batch_size

    try:
        model.inference(**inference_kwargs)
    except TypeError as exc:
        if "batch_size" not in inference_kwargs or "batch_size" not in str(exc):
            raise
        inference_kwargs.pop("batch_size", None)
        model.inference(**inference_kwargs)


def serve(
    device_no: int,
    port: int,
    host: str,
    model_id: str,
    export_format: str,
    timeout_s: int,
    max_bytes: int,
    debug: bool,
    batch_size: int,
) -> None:
    print(f"[setup] Loading model '{model_id}' on cuda:{device_no}...")
    model = _load_model(model_id, device_no)
    print(f"[setup] Model loaded. Listening on {host}:{port}")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((host, port))
        server.listen(5)

        while True:
            conn, addr = server.accept()
            with conn:
                start = time.time()
                try:
                    req = _recv_json(conn, max_bytes=max_bytes, timeout_s=timeout_s)
                    _validate_request(req)
                    _run_inference(model, req, default_export_format=export_format, default_batch_size=batch_size)
                    elapsed_ms = int((time.time() - start) * 1000)
                    _send_json(
                        conn,
                        {
                            "status": "success",
                            "message": "Inference completed.",
                            "elapsed_ms": elapsed_ms,
                        },
                    )
                except Exception as exc:
                    elapsed_ms = int((time.time() - start) * 1000)
                    payload = {
                        "status": "error",
                        "message": str(exc),
                        "elapsed_ms": elapsed_ms,
                    }
                    if debug:
                        payload["traceback"] = traceback.format_exc()
                    try:
                        _send_json(conn, payload)
                    except Exception:
                        pass


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Socket server for DepthAnything3 inference."
    )
    parser.add_argument("--device-no", "--device_no", type=int, required=True, help="CUDA device id.")
    parser.add_argument("--port", type=int, default=8008, help="TCP port to listen on.")
    parser.add_argument(
        "--host", default="0.0.0.0", help="Bind address (default: 0.0.0.0)."
    )
    parser.add_argument(
        "--model-id",
        default="depth-anything/DA3NESTED-GIANT-LARGE",
        help="Model id for DepthAnything3.from_pretrained.",
    )
    parser.add_argument(
        "--export-format",
        default=DEFAULT_EXPORT_FORMAT,
        help="Default export format if not provided by client.",
    )
    parser.add_argument(
        "--timeout-s",
        type=int,
        default=300,
        help="Socket read timeout in seconds.",
    )
    parser.add_argument(
        "--max-bytes",
        type=int,
        default=10 * 1024 * 1024,
        help="Max request size in bytes.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Include traceback in error responses.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=16,
        help="Default frame batch size for model inference.",
    )
    return parser.parse_args(argv)


def main() -> int:
    args = _parse_args(sys.argv[1:])
    serve(
        device_no=args.device_no,
        port=args.port,
        host=args.host,
        model_id=args.model_id,
        export_format=args.export_format,
        timeout_s=args.timeout_s,
        max_bytes=args.max_bytes,
        debug=args.debug,
        batch_size=args.batch_size,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
