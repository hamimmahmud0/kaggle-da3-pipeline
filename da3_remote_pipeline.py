#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path

try:
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None


DEFAULT_WORKSPACE = Path("/kaggle/working/DA3")


def iso_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def workspace_paths(workspace: Path) -> dict[str, Path]:
    return {
        "workspace": workspace,
        "logs": workspace / "logs",
        "session": workspace / "session.json",
        "lock": workspace / "session.lock",
        "pipeline_pid": workspace / "pipeline.pid",
        "runtime": workspace / "runtime.json",
        "config": workspace / "remote-session-config.json",
    }


def ensure_workspace(workspace: Path) -> dict[str, Path]:
    paths = workspace_paths(workspace)
    for key in ["workspace", "logs"]:
        paths[key].mkdir(parents=True, exist_ok=True)
    return paths


@contextmanager
def session_lock(lock_path: Path):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as handle:
        if fcntl is not None:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            if fcntl is not None:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def load_json(path: Path, default):
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def build_tasks_from_manifest(manifest_path: str) -> list[dict]:
    if not manifest_path:
        return []
    payload = load_json(Path(manifest_path), [])
    if not isinstance(payload, list):
        raise SystemExit("Manifest file must contain a JSON list.")
    tasks = []
    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            raise SystemExit("Every manifest entry must be a JSON object.")
        tasks.append(
            {
                "id": item.get("id") or f"task-{index:04d}",
                "video_name": item.get("video_name", "video"),
                "file_name": item.get("file_name", f"item-{index:04d}"),
                "image_paths": item.get("image_paths", []),
                "export_format": item.get("export_format", "glb-npz"),
                "status": "pending",
                "claimed_by": None,
                "started_at": None,
                "completed_at": None,
                "elapsed_ms": None,
                "uploaded_at": None,
                "uploaded_path": None,
                "last_error": None,
            }
        )
    return tasks


def init_session(workspace: Path, config_file: str | None = None) -> dict:
    paths = ensure_workspace(workspace)
    config = load_json(Path(config_file), {}) if config_file else load_json(paths["config"], {})
    tasks = build_tasks_from_manifest(config.get("manifest_path", ""))
    worker_count = int(config.get("worker_count", 2))
    session = {
        "created_at": iso_now(),
        "updated_at": iso_now(),
        "workspace": str(workspace),
        "transport": config.get("transport", "mega"),
        "drive_folder_url": config.get("drive_folder_url", ""),
        "inference_batch_size": int(config.get("inference_batch_size", 16)),
        "fare_drive": config.get("fare_drive", {}),
        "tasks": tasks,
        "workers": {
            f"worker_{chr(ord('a') + index)}": {
                "gpu": index,
                "status": "idle",
                "pid": None,
                "claimed_task": None,
                "last_heartbeat": None,
                "backend_port": 8008 + index,
                "log_path": str(paths["logs"] / f"worker_{chr(ord('a') + index)}.log"),
                "backend_log_path": str(paths["logs"] / f"backend_{chr(ord('a') + index)}.log"),
            }
            for index in range(worker_count)
        },
        "summary": {
            "pending": len(tasks),
            "running": 0,
            "completed": 0,
            "failed": 0,
            "total": len(tasks),
        },
    }
    save_json(paths["session"], session)
    save_json(paths["runtime"], {"updated_at": iso_now(), "last_launch_at": None})
    return session


def refresh_summary(session: dict) -> dict:
    counts = {"pending": 0, "running": 0, "completed": 0, "failed": 0}
    for task in session.get("tasks", []):
        status = task.get("status", "pending")
        counts[status] = counts.get(status, 0) + 1
    counts["total"] = len(session.get("tasks", []))
    session["summary"] = counts
    session["updated_at"] = iso_now()
    return session


def claim_task(session: dict, worker_name: str) -> dict | None:
    for task in session.get("tasks", []):
        if task["status"] == "pending":
            task["status"] = "running"
            task["claimed_by"] = worker_name
            task["started_at"] = iso_now()
            session["workers"][worker_name]["claimed_task"] = task["id"]
            session["workers"][worker_name]["status"] = "running"
            refresh_summary(session)
            return task
    session["workers"][worker_name]["status"] = "idle"
    session["workers"][worker_name]["claimed_task"] = None
    refresh_summary(session)
    return None


def complete_task(session: dict, worker_name: str, task_id: str, *, elapsed_ms: int | None = None, error: str | None = None) -> None:
    for task in session.get("tasks", []):
        if task["id"] != task_id:
            continue
        task["status"] = "failed" if error else "completed"
        task["completed_at"] = iso_now()
        task["elapsed_ms"] = elapsed_ms
        task["last_error"] = error
        task["claimed_by"] = worker_name
        break
    session["workers"][worker_name]["claimed_task"] = None
    session["workers"][worker_name]["status"] = "idle" if error is None else "error"
    refresh_summary(session)


def heartbeat(session: dict, worker_name: str, pid: int) -> None:
    session["workers"][worker_name]["pid"] = pid
    session["workers"][worker_name]["last_heartbeat"] = iso_now()
    session["updated_at"] = iso_now()


def read_session(workspace: Path) -> dict:
    return load_json(workspace_paths(workspace)["session"], {})


def save_session(workspace: Path, session: dict) -> None:
    save_json(workspace_paths(workspace)["session"], refresh_summary(session))


def backend_script(workspace: Path, worker_name: str, device_no: int, port: int) -> str:
    paths = workspace_paths(workspace)
    backend_log = paths["logs"] / f"backend_{worker_name}.log"
    return (
        f"cd {shlex_quote(str(workspace))} && "
        f"PYTHONUNBUFFERED=1 {shlex_quote(sys.executable)} da3_inference_server.py --device-no {device_no} --port {port} "
        f">> {shlex_quote(str(backend_log))} 2>&1"
    )


def shlex_quote(value: str) -> str:
    import shlex

    return shlex.quote(value)


def wait_for_port(host: str, port: int, timeout: float = 60.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket() as sock:
            sock.settimeout(0.5)
            try:
                sock.connect((host, port))
            except OSError:
                time.sleep(0.25)
                continue
            return
    raise RuntimeError(f"Timed out waiting for backend server on {host}:{port}")


def send_inference_request(host: str, port: int, payload: dict, timeout_s: int = 600) -> dict:
    raw = (json.dumps(payload) + "\n").encode("utf-8")
    with socket.create_connection((host, port), timeout=timeout_s) as sock:
        sock.sendall(raw)
        chunks: list[bytes] = []
        while True:
            data = sock.recv(65536)
            if not data:
                break
            chunks.append(data)
            if b"\n" in data:
                break
    response = b"".join(chunks).decode("utf-8", "replace").strip()
    if not response:
        raise RuntimeError("Empty response from inference server.")
    return json.loads(response.splitlines()[0])


def upload_artifacts_via_fare_drive(workspace: Path, session: dict, task: dict) -> str:
    fare_drive = session.get("fare_drive", {})
    client_home = fare_drive.get("client_home", "")
    upload_root = fare_drive.get("upload_root", "da3-output").strip("/")
    output_dir = workspace / "output" / str(task["video_name"]) / str(task["file_name"])
    if not output_dir.exists():
        raise RuntimeError(f"Expected output directory is missing: {output_dir}")
    remote_path = "/".join(part for part in [upload_root, str(task["video_name"]), str(task["file_name"])] if part)
    env = os.environ.copy()
    if client_home:
        env["HOME"] = client_home
    command = [sys.executable, "-m", "fare_drive.cli", "client", "put", str(output_dir), "--remote-path", remote_path]
    proc = subprocess.run(command, cwd=workspace, env=env, text=True, capture_output=True)
    if proc.returncode != 0:
        raise RuntimeError(
            f"fare-drive client put failed for {output_dir}.\n"
            f"stdout:\n{proc.stdout}\n"
            f"stderr:\n{proc.stderr}"
        )
    return remote_path


def worker_loop(workspace: Path, worker_name: str) -> int:
    paths = ensure_workspace(workspace)
    session = read_session(workspace)
    worker = session["workers"][worker_name]
    device_no = int(worker["gpu"])
    backend_port = int(worker["backend_port"])
    worker_log = Path(worker["log_path"])
    worker_log.parent.mkdir(parents=True, exist_ok=True)

    backend_process = subprocess.Popen(
        ["bash", "-lc", backend_script(workspace, worker_name, device_no, backend_port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    backend_log = Path(worker["backend_log_path"])
    startup_deadline = time.time() + 120.0
    while True:
        if backend_process.poll() is not None:
            log_tail = ""
            if backend_log.exists():
                log_tail = backend_log.read_text(encoding="utf-8", errors="replace")[-4000:]
            raise RuntimeError(
                f"Backend process exited before opening port {backend_port}.\n"
                f"Backend log tail:\n{log_tail}"
            )
        try:
            wait_for_port("127.0.0.1", backend_port, timeout=1.0)
            break
        except RuntimeError:
            if time.time() >= startup_deadline:
                log_tail = ""
                if backend_log.exists():
                    log_tail = backend_log.read_text(encoding="utf-8", errors="replace")[-4000:]
                raise RuntimeError(
                    f"Timed out waiting for backend server on 127.0.0.1:{backend_port}.\n"
                    f"Backend log tail:\n{log_tail}"
                )

    try:
        while True:
            with session_lock(paths["lock"]):
                session = read_session(workspace)
                heartbeat(session, worker_name, os.getpid())
                task = claim_task(session, worker_name)
                save_session(workspace, session)
            if task is None:
                worker_log.write_text(worker_log.read_text(encoding="utf-8") + f"{iso_now()} | idle\n" if worker_log.exists() else f"{iso_now()} | idle\n", encoding="utf-8")
                break
            started = time.time()
            try:
                response = send_inference_request(
                    "127.0.0.1",
                    backend_port,
                    {
                        "image_paths": task["image_paths"],
                        "video_name": task["video_name"],
                        "file_name": task["file_name"],
                        "export_format": task.get("export_format", "glb-npz"),
                        "batch_size": int(session.get("inference_batch_size", 16)),
                    },
                )
                if response.get("status") != "success":
                    raise RuntimeError(response.get("message", "Unknown inference error"))
                elapsed_ms = int((time.time() - started) * 1000)
                session = read_session(workspace)
                uploaded_path = upload_artifacts_via_fare_drive(workspace, session, task)
                with session_lock(paths["lock"]):
                    session = read_session(workspace)
                    heartbeat(session, worker_name, os.getpid())
                    for item in session.get("tasks", []):
                        if item.get("id") == task["id"]:
                            item["uploaded_path"] = uploaded_path
                            item["uploaded_at"] = iso_now()
                            break
                    complete_task(session, worker_name, task["id"], elapsed_ms=elapsed_ms)
                    save_session(workspace, session)
            except Exception as exc:
                with session_lock(paths["lock"]):
                    session = read_session(workspace)
                    heartbeat(session, worker_name, os.getpid())
                    complete_task(session, worker_name, task["id"], error=str(exc))
                    save_session(workspace, session)
    finally:
        backend_process.terminate()
        try:
            backend_process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            backend_process.kill()
    return 0


def launch(workspace: Path) -> dict:
    paths = ensure_workspace(workspace)
    session = read_session(workspace)
    if not session:
        raise SystemExit("Session is not initialized. Run init-session first.")
    pids: dict[str, int] = {}
    for worker_name, worker in session.get("workers", {}).items():
        log_path = Path(worker["log_path"])
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handle = log_path.open("a", encoding="utf-8")
        process = subprocess.Popen(
            [sys.executable, str(paths["workspace"] / "da3_remote_pipeline.py"), "worker", "--workspace", str(workspace), "--worker-name", worker_name],
            stdout=handle,
            stderr=subprocess.STDOUT,
            text=True,
        )
        handle.close()
        pids[worker_name] = process.pid
        worker["pid"] = process.pid
        worker["status"] = "starting"
    save_json(paths["pipeline_pid"], {"workers": pids, "updated_at": iso_now()})
    runtime = load_json(paths["runtime"], {})
    runtime["last_launch_at"] = iso_now()
    save_json(paths["runtime"], runtime)
    save_session(workspace, session)
    return pids


def status_payload(workspace: Path) -> dict:
    paths = ensure_workspace(workspace)
    session = read_session(workspace)
    fare_drive_status = "unknown"
    fare_drive = session.get("fare_drive", {})
    endpoint = fare_drive.get("endpoint", "")
    client_home = fare_drive.get("client_home", "")
    if endpoint and client_home:
        fare_drive_status = f"client:{endpoint}"
    elif endpoint:
        fare_drive_status = f"configured:{endpoint}"
    return {
        "workspace": str(workspace),
        "session": refresh_summary(session),
        "runtime": load_json(paths["runtime"], {}),
        "fare_drive_status": fare_drive_status,
    }


def handle_init_session(args: argparse.Namespace) -> None:
    session = init_session(Path(args.workspace), args.config_file)
    print(json.dumps(refresh_summary(session), indent=2))


def handle_launch(args: argparse.Namespace) -> None:
    payload = launch(Path(args.workspace))
    print(json.dumps(payload, indent=2))


def handle_worker(args: argparse.Namespace) -> None:
    raise SystemExit(worker_loop(Path(args.workspace), args.worker_name))


def handle_status(args: argparse.Namespace) -> None:
    payload = status_payload(Path(args.workspace))
    if args.json:
        print(json.dumps(payload, indent=2))
        return
    session = payload.get("session", {})
    summary = session.get("summary", {})
    print(f"Workspace: {payload['workspace']}")
    print(f"Inference batch size: {session.get('inference_batch_size', 'unknown')}")
    print(f"Pending: {summary.get('pending', 0)}")
    print(f"Running: {summary.get('running', 0)}")
    print(f"Completed: {summary.get('completed', 0)}")
    print(f"Failed: {summary.get('failed', 0)}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Remote DA3 multi-worker runtime.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init-session", help="Create or reset pipeline session state")
    init_parser.add_argument("--workspace", default=str(DEFAULT_WORKSPACE))
    init_parser.add_argument("--config-file")
    init_parser.set_defaults(handler=handle_init_session)

    launch_parser = subparsers.add_parser("launch", help="Launch worker processes in the background")
    launch_parser.add_argument("--workspace", default=str(DEFAULT_WORKSPACE))
    launch_parser.set_defaults(handler=handle_launch)

    worker_parser = subparsers.add_parser("worker", help="Run one worker loop")
    worker_parser.add_argument("--workspace", default=str(DEFAULT_WORKSPACE))
    worker_parser.add_argument("--worker-name", required=True)
    worker_parser.set_defaults(handler=handle_worker)

    status_parser = subparsers.add_parser("status", help="Print current session status")
    status_parser.add_argument("--workspace", default=str(DEFAULT_WORKSPACE))
    status_parser.add_argument("--json", action="store_true")
    status_parser.set_defaults(handler=handle_status)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.handler(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
