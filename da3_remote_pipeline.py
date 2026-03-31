#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import time
import traceback
from contextlib import contextmanager
from pathlib import Path

try:
    from PIL import Image, UnidentifiedImageError
except ImportError:  # pragma: no cover
    Image = None
    UnidentifiedImageError = OSError

try:
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None


DEFAULT_WORKSPACE = Path("/kaggle/working/DA3")
VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".mkv", ".webm", ".m4v", ".dav"}
IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".bmp", ".webp"}
VIDEO_FRAME_TASK_SIZE = 16
DEFAULT_EXPORT_FORMAT = "npz"
SESSION_CHECKPOINT_INTERVAL = 20


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


def checkpoint_remote_path(fare_drive: dict) -> str:
    upload_root = str(fare_drive.get("upload_root", "da3-output") or "da3-output").strip("/")
    return "/".join(part for part in [upload_root, "_pipeline_state", "session.json"] if part)


def ensure_checkpoint_state(session: dict) -> dict:
    checkpoint = session.get("checkpoint")
    if not isinstance(checkpoint, dict):
        checkpoint = {}
    checkpoint["interval"] = int(checkpoint.get("interval", SESSION_CHECKPOINT_INTERVAL) or SESSION_CHECKPOINT_INTERVAL)
    checkpoint["last_uploaded_completed_count"] = int(checkpoint.get("last_uploaded_completed_count", 0) or 0)
    checkpoint["remote_path"] = checkpoint_remote_path(session.get("fare_drive", {}))
    checkpoint["uploaded_at"] = checkpoint.get("uploaded_at")
    session["checkpoint"] = checkpoint
    return checkpoint


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
    last_error: Exception | None = None
    for _ in range(5):
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            last_error = exc
            time.sleep(0.05)
    if last_error is not None:
        raise last_error
    return default


def save_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temp_path.replace(path)


def build_tasks_from_manifest(manifest_path: str, default_export_format: str = DEFAULT_EXPORT_FORMAT) -> list[dict]:
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
                "video_path": item.get("video_path", ""),
                "frame_start": item.get("frame_start"),
                "frame_end": item.get("frame_end"),
                "export_format": item.get("export_format", default_export_format),
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


def safe_segment(value: str, fallback: str = "item") -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", (value or "").strip()).strip("._-")
    return cleaned or fallback


def classify_input_file(path: Path) -> str | None:
    suffix = path.suffix.lower()
    if suffix in VIDEO_EXTENSIONS:
        return "video"
    if suffix in IMAGE_EXTENSIONS:
        return "image"
    return None


def derive_task_names(root: Path, file_path: Path) -> tuple[str, str]:
    rel_parent = file_path.parent.relative_to(root) if file_path.parent != root else Path(".")
    video_name = safe_segment(rel_parent.as_posix().replace("/", "__"), safe_segment(root.name, "drive-folder"))
    file_name = safe_segment(file_path.stem, "item")
    return video_name, file_name


def build_tasks_from_inputs_root(
    root: Path,
    export_format: str = DEFAULT_EXPORT_FORMAT,
    video_frame_task_size: int = VIDEO_FRAME_TASK_SIZE,
) -> list[dict]:
    if not root.exists():
        raise SystemExit(f"Input root does not exist: {root}")

    files = sorted(path for path in root.rglob("*") if path.is_file())
    video_files: list[Path] = []
    image_groups: dict[Path, list[Path]] = {}
    for path in files:
        kind = classify_input_file(path)
        if kind == "video":
            video_files.append(path)
        elif kind == "image":
            image_groups.setdefault(path.parent, []).append(path)

    tasks: list[dict] = []
    for index, video_path in enumerate(video_files):
        video_name, file_name = derive_task_names(root, video_path)
        rel = video_path.relative_to(root).as_posix()
        digest = hashlib.sha1(rel.encode("utf-8")).hexdigest()[:10]
        frame_count = get_video_frame_count(video_path)
        if frame_count and frame_count > 0:
            chunk_size = max(1, int(video_frame_task_size))
            chunk_count = max(1, (frame_count + chunk_size - 1) // chunk_size)
            for chunk_index in range(chunk_count):
                frame_start = chunk_index * chunk_size
                frame_end = min(frame_count - 1, frame_start + chunk_size - 1)
                tasks.append(
                    {
                        "id": f"video-{index:04d}-{digest}-{chunk_index:04d}",
                        "video_name": video_name,
                        "file_name": f"{file_name}_batch_{frame_start:06d}_{frame_end:06d}",
                        "image_paths": [],
                        "video_path": str(video_path),
                        "frame_start": frame_start,
                        "frame_end": frame_end,
                        "export_format": export_format,
                    }
                )
        else:
            tasks.append(
                {
                    "id": f"video-{index:04d}-{digest}",
                    "video_name": video_name,
                    "file_name": file_name,
                    "image_paths": [],
                    "video_path": str(video_path),
                    "export_format": export_format,
                }
            )

    for index, (parent, image_paths) in enumerate(sorted(image_groups.items(), key=lambda item: item[0].as_posix())):
        rel = parent.relative_to(root).as_posix() if parent != root else "."
        digest = hashlib.sha1(rel.encode("utf-8")).hexdigest()[:10]
        if parent == root:
            video_name = safe_segment(root.name, "drive-folder")
            file_name = "images"
        else:
            rel_parent = parent.parent.relative_to(root) if parent.parent != root else Path(".")
            video_name = safe_segment(rel_parent.as_posix().replace("/", "__"), safe_segment(root.name, "drive-folder"))
            file_name = safe_segment(parent.name, f"images-{index:04d}")
        tasks.append(
            {
                "id": f"images-{index:04d}-{digest}",
                "video_name": video_name,
                "file_name": file_name,
                "image_paths": [str(path) for path in sorted(image_paths)],
                "video_path": "",
                "export_format": export_format,
            }
        )

    return tasks


def download_drive_folder(drive_folder_url: str, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        import gdown  # type: ignore
    except Exception as exc:
        raise SystemExit("gdown is required for drive_folder_url support. Install it in the remote environment.") from exc

    result = None
    if hasattr(gdown, "download_folder"):
        try:
            result = gdown.download_folder(url=drive_folder_url, output=str(output_dir), quiet=False, remaining_ok=True)
        except TypeError:
            result = gdown.download_folder(url=drive_folder_url, output=str(output_dir), quiet=False)
    else:
        proc = subprocess.run(
            [sys.executable, "-m", "gdown", "--folder", drive_folder_url, "-O", str(output_dir)],
            text=True,
            capture_output=True,
        )
        if proc.returncode != 0:
            raise SystemExit(f"gdown folder download failed.\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}")

    if result is not None and not list(output_dir.rglob("*")):
        raise SystemExit(f"No files were downloaded from Google Drive folder: {drive_folder_url}")


def generate_manifest_from_drive_folder(
    workspace: Path,
    drive_folder_url: str,
    export_format: str = DEFAULT_EXPORT_FORMAT,
    video_frame_task_size: int = VIDEO_FRAME_TASK_SIZE,
) -> Path:
    paths = ensure_workspace(workspace)
    input_root = paths["workspace"] / "incoming" / "drive-folder"
    frames_root = paths["workspace"] / "incoming" / "frames"
    if input_root.exists():
        for child in input_root.iterdir():
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)
            else:
                child.unlink(missing_ok=True)
    input_root.mkdir(parents=True, exist_ok=True)
    if frames_root.exists():
        shutil.rmtree(frames_root, ignore_errors=True)

    download_drive_folder(drive_folder_url, input_root)
    tasks = build_tasks_from_inputs_root(
        input_root,
        export_format=export_format,
        video_frame_task_size=video_frame_task_size,
    )
    if not tasks:
        raise SystemExit(f"No supported input files found in downloaded Google Drive folder: {drive_folder_url}")
    manifest_path = paths["workspace"] / "generated-manifest.json"
    save_json(manifest_path, tasks)
    return manifest_path


def download_fare_drive_path(workspace: Path, fare_drive: dict, remote_path: str, output_dir: Path) -> None:
    remote_path = remote_path.strip().strip("/")
    if not remote_path:
        raise SystemExit("fare_drive.input_root must not be empty when using Fare Drive downloads.")
    output_dir.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    client_home = fare_drive.get("client_home", "")
    if client_home:
        env["HOME"] = client_home
    command = [sys.executable, "-m", "fare_drive.cli", "client", "get", remote_path, "--output", str(output_dir)]
    proc = subprocess.run(command, cwd=workspace, env=env, text=True, capture_output=True)
    if proc.returncode != 0:
        raise SystemExit(
            f"fare-drive client get failed for {remote_path}.\n"
            f"stdout:\n{proc.stdout}\n"
            f"stderr:\n{proc.stderr}"
        )


def generate_manifest_from_fare_drive(
    workspace: Path,
    fare_drive: dict,
    export_format: str = DEFAULT_EXPORT_FORMAT,
    video_frame_task_size: int = VIDEO_FRAME_TASK_SIZE,
) -> Path:
    paths = ensure_workspace(workspace)
    input_root = paths["workspace"] / "incoming" / "fare-drive"
    frames_root = paths["workspace"] / "incoming" / "frames"
    if input_root.exists():
        shutil.rmtree(input_root, ignore_errors=True)
    input_root.mkdir(parents=True, exist_ok=True)
    if frames_root.exists():
        shutil.rmtree(frames_root, ignore_errors=True)

    remote_input_root = str(fare_drive.get("input_root", "") or "").strip("/")
    download_fare_drive_path(workspace, fare_drive, remote_input_root, input_root)

    scan_root = input_root
    named_root = input_root / Path(remote_input_root).name
    tasks = build_tasks_from_inputs_root(
        scan_root,
        export_format=export_format,
        video_frame_task_size=video_frame_task_size,
    )
    if not tasks and named_root.exists():
        tasks = build_tasks_from_inputs_root(
            named_root,
            export_format=export_format,
            video_frame_task_size=video_frame_task_size,
        )
        scan_root = named_root
    if not tasks:
        raise SystemExit(f"No supported input files found in Fare Drive path: {remote_input_root}")
    manifest_path = paths["workspace"] / "generated-manifest.json"
    save_json(manifest_path, tasks)
    return manifest_path


def init_session(workspace: Path, config_file: str | None = None) -> dict:
    paths = ensure_workspace(workspace)
    config = load_json(Path(config_file), {}) if config_file else load_json(paths["config"], {})
    restored = restore_session_from_fare_drive(workspace, config)
    if restored:
        return restored
    export_format = str(config.get("export_format", DEFAULT_EXPORT_FORMAT) or DEFAULT_EXPORT_FORMAT)
    video_frame_task_size = int(config.get("video_frame_task_size", VIDEO_FRAME_TASK_SIZE) or VIDEO_FRAME_TASK_SIZE)
    manifest_path = config.get("manifest_path", "")
    fare_drive = config.get("fare_drive", {})
    if not manifest_path and fare_drive.get("input_root"):
        manifest_path = str(
            generate_manifest_from_fare_drive(
                workspace,
                fare_drive,
                export_format=export_format,
                video_frame_task_size=video_frame_task_size,
            )
        )
    if not manifest_path and config.get("drive_folder_url"):
        manifest_path = str(
            generate_manifest_from_drive_folder(
                workspace,
                config["drive_folder_url"],
                export_format=export_format,
                video_frame_task_size=video_frame_task_size,
            )
        )
    tasks = build_tasks_from_manifest(manifest_path, default_export_format=export_format)
    worker_count = int(config.get("worker_count", 2))
    session = {
        "created_at": iso_now(),
        "updated_at": iso_now(),
        "workspace": str(workspace),
        "transport": config.get("transport", "mega"),
        "drive_folder_url": config.get("drive_folder_url", ""),
        "manifest_path": manifest_path,
        "inference_batch_size": int(config.get("inference_batch_size", 16)),
        "video_frame_task_size": video_frame_task_size,
        "export_format": export_format,
        "fare_drive": fare_drive,
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
                "backend_log_path": str(paths["logs"] / f"backend_worker_{chr(ord('a') + index)}.log"),
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
    ensure_checkpoint_state(session)
    save_json(paths["session"], session)
    save_json(paths["runtime"], {"updated_at": iso_now(), "last_launch_at": None})
    return session


def update_session_config(workspace: Path, config_file: str | None = None) -> dict:
    paths = ensure_workspace(workspace)
    config = load_json(Path(config_file), {}) if config_file else load_json(paths["config"], {})
    session = read_session(workspace)
    if not session:
        return init_session(workspace, config_file)
    if config.get("transport") not in (None, ""):
        session["transport"] = config["transport"]
    if config.get("drive_folder_url") not in (None, ""):
        session["drive_folder_url"] = config["drive_folder_url"]
    if config.get("manifest_path") not in (None, ""):
        session["manifest_path"] = config["manifest_path"]
    session["inference_batch_size"] = int(config.get("inference_batch_size", session.get("inference_batch_size", 16)))
    session["video_frame_task_size"] = int(config.get("video_frame_task_size", session.get("video_frame_task_size", VIDEO_FRAME_TASK_SIZE)))
    session["export_format"] = str(config.get("export_format", session.get("export_format", DEFAULT_EXPORT_FORMAT)) or DEFAULT_EXPORT_FORMAT)
    if "fare_drive" in config and isinstance(config["fare_drive"], dict):
        merged_fare_drive = dict(session.get("fare_drive", {}))
        for key, value in config["fare_drive"].items():
            if value not in (None, ""):
                merged_fare_drive[key] = value
        session["fare_drive"] = merged_fare_drive
    for task in session.get("tasks", []):
        task["export_format"] = session["export_format"]
    session["updated_at"] = iso_now()
    save_json(paths["config"], config)
    save_session(workspace, session)
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


def retry_failed_tasks(workspace: Path) -> dict:
    paths = ensure_workspace(workspace)
    with session_lock(paths["lock"]):
        session = read_session(workspace)
        if not session:
            raise SystemExit("Session is not initialized. Run init-session first.")
        for task in session.get("tasks", []):
            if task.get("status") != "failed":
                continue
            task["status"] = "pending"
            task["claimed_by"] = None
            task["started_at"] = None
            task["completed_at"] = None
            task["elapsed_ms"] = None
            task["last_error"] = None
        save_session(workspace, session)
        return refresh_summary(session)


def heartbeat(session: dict, worker_name: str, pid: int) -> None:
    session["workers"][worker_name]["pid"] = pid
    session["workers"][worker_name]["last_heartbeat"] = iso_now()
    session["updated_at"] = iso_now()


def read_session(workspace: Path) -> dict:
    return load_json(workspace_paths(workspace)["session"], {})


def save_session(workspace: Path, session: dict) -> None:
    ensure_checkpoint_state(session)
    save_json(workspace_paths(workspace)["session"], refresh_summary(session))


def upload_file_via_fare_drive(workspace: Path, fare_drive: dict, source_path: Path, remote_path: str) -> str:
    env = os.environ.copy()
    client_home = fare_drive.get("client_home", "")
    if client_home:
        env["HOME"] = client_home
    command = [sys.executable, "-m", "fare_drive.cli", "client", "put", str(source_path), "--remote-path", remote_path]
    proc = subprocess.run(command, cwd=workspace, env=env, text=True, capture_output=True)
    if proc.returncode != 0:
        raise RuntimeError(
            f"fare-drive client put failed for {source_path}.\n"
            f"stdout:\n{proc.stdout}\n"
            f"stderr:\n{proc.stderr}"
        )
    return remote_path


def maybe_upload_session_checkpoint(workspace: Path) -> str | None:
    paths = workspace_paths(workspace)
    with session_lock(paths["lock"]):
        session = read_session(workspace)
        if not session:
            return None
        checkpoint = ensure_checkpoint_state(session)
        completed = int(session.get("summary", {}).get("completed", 0) or 0)
        interval = int(checkpoint.get("interval", SESSION_CHECKPOINT_INTERVAL) or SESSION_CHECKPOINT_INTERVAL)
        last_uploaded = int(checkpoint.get("last_uploaded_completed_count", 0) or 0)
        if completed <= 0 or completed % interval != 0 or completed <= last_uploaded:
            save_session(workspace, session)
            return None
        fare_drive = dict(session.get("fare_drive", {}))
        remote_path = str(checkpoint.get("remote_path") or checkpoint_remote_path(fare_drive))

    uploaded_path = upload_file_via_fare_drive(workspace, fare_drive, paths["session"], remote_path)

    with session_lock(paths["lock"]):
        session = read_session(workspace)
        checkpoint = ensure_checkpoint_state(session)
        current_completed = int(session.get("summary", {}).get("completed", 0) or 0)
        if current_completed >= completed:
            checkpoint["last_uploaded_completed_count"] = completed
            checkpoint["uploaded_at"] = iso_now()
            checkpoint["remote_path"] = uploaded_path
            save_session(workspace, session)
    return uploaded_path


def restore_session_from_fare_drive(workspace: Path, config: dict) -> dict | None:
    fare_drive = config.get("fare_drive", {})
    if not isinstance(fare_drive, dict):
        return None
    client_home = str(fare_drive.get("client_home", "") or "").strip()
    has_auth = bool(fare_drive.get("access_token") or fare_drive.get("endpoint"))
    if not client_home or not has_auth:
        return None

    remote_path = checkpoint_remote_path(fare_drive)
    restore_root = workspace / "tmp" / "session-restore"
    if restore_root.exists():
        shutil.rmtree(restore_root, ignore_errors=True)
    restore_root.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env["HOME"] = client_home
    command = [sys.executable, "-m", "fare_drive.cli", "client", "get", remote_path, "--output", str(restore_root)]
    proc = subprocess.run(command, cwd=workspace, env=env, text=True, capture_output=True)
    if proc.returncode != 0:
        error_text = f"{proc.stdout}\n{proc.stderr}".lower()
        if "404" in error_text or "not found" in error_text:
            return None
        raise SystemExit(
            f"fare-drive client get failed for {remote_path}.\n"
            f"stdout:\n{proc.stdout}\n"
            f"stderr:\n{proc.stderr}"
        )

    restored_path = restore_root / "session.json"
    if not restored_path.exists():
        candidates = sorted(restore_root.rglob("session.json"))
        if not candidates:
            return None
        restored_path = candidates[0]
    restored = load_json(restored_path, {})
    if not isinstance(restored, dict) or not restored.get("tasks"):
        return None

    paths = ensure_workspace(workspace)
    worker_count = int(config.get("worker_count", len(restored.get("workers", {})) or 2))
    restored["workspace"] = str(workspace)
    restored["transport"] = config.get("transport", restored.get("transport", "fare-drive"))
    restored["drive_folder_url"] = config.get("drive_folder_url", restored.get("drive_folder_url", ""))
    restored["manifest_path"] = restored.get("manifest_path", config.get("manifest_path", ""))
    restored["inference_batch_size"] = int(config.get("inference_batch_size", restored.get("inference_batch_size", 16)))
    restored["video_frame_task_size"] = int(
        config.get("video_frame_task_size", restored.get("video_frame_task_size", VIDEO_FRAME_TASK_SIZE))
    )
    restored["export_format"] = str(config.get("export_format", restored.get("export_format", DEFAULT_EXPORT_FORMAT)) or DEFAULT_EXPORT_FORMAT)
    merged_fare_drive = dict(restored.get("fare_drive", {}))
    for key, value in fare_drive.items():
        if value not in (None, ""):
            merged_fare_drive[key] = value
    restored["fare_drive"] = merged_fare_drive
    restored["workers"] = {
        f"worker_{chr(ord('a') + index)}": {
            "gpu": index,
            "status": "idle",
            "pid": None,
            "claimed_task": None,
            "last_heartbeat": None,
            "backend_port": 8008 + index,
            "log_path": str(paths["logs"] / f"worker_{chr(ord('a') + index)}.log"),
            "backend_log_path": str(paths["logs"] / f"backend_worker_{chr(ord('a') + index)}.log"),
        }
        for index in range(worker_count)
    }
    for task in restored.get("tasks", []):
        if task.get("status") == "running":
            task["status"] = "pending"
            task["claimed_by"] = None
            task["started_at"] = None
        task["export_format"] = restored["export_format"]
    ensure_checkpoint_state(restored)
    restored["updated_at"] = iso_now()
    save_json(paths["session"], refresh_summary(restored))
    return restored


def is_process_alive(pid: int | None) -> bool:
    if not pid or pid <= 0:
        return False
    proc_stat = Path(f"/proc/{pid}/stat")
    if proc_stat.exists():
        try:
            state = proc_stat.read_text(encoding="utf-8").split()[2]
        except (IndexError, OSError):
            return False
        return state != "Z"
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def kill_pid(pid: int | None) -> None:
    if not pid or pid <= 0:
        return
    try:
        os.kill(pid, 15)
    except OSError:
        return

    deadline = time.time() + 10.0
    while time.time() < deadline:
        if not is_process_alive(pid):
            return
        time.sleep(0.1)

    try:
        os.kill(pid, 9)
    except OSError:
        return


def pids_listening_on_port(port: int) -> list[int]:
    proc = subprocess.run(
        ["bash", "-lc", f"ss -ltnp '( sport = :{port} )' | tail -n +2"],
        text=True,
        capture_output=True,
    )
    if proc.returncode != 0:
        return []
    pids: list[int] = []
    for line in proc.stdout.splitlines():
        for match in re.finditer(r"pid=(\d+)", line):
            pids.append(int(match.group(1)))
    return sorted(set(pids))


def pids_matching_pattern(pattern: str) -> list[int]:
    proc = subprocess.run(["pgrep", "-f", pattern], text=True, capture_output=True)
    if proc.returncode not in {0, 1}:
        return []
    pids: list[int] = []
    for line in proc.stdout.splitlines():
        try:
            pids.append(int(line.strip()))
        except ValueError:
            continue
    return sorted(set(pids))


def reset_inflight_tasks(session: dict) -> None:
    for task in session.get("tasks", []):
        if task.get("status") != "running":
            continue
        task["status"] = "pending"
        task["claimed_by"] = None
        task["started_at"] = None
        task["last_error"] = None


def cleanup_launch_processes(session: dict) -> None:
    for worker in session.get("workers", {}).values():
        kill_pid(worker.get("pid"))
        for pid in pids_matching_pattern(r"/kaggle/working/DA3/da3_remote_pipeline.py worker"):
            kill_pid(pid)
        for pid in pids_matching_pattern(r"da3_inference_server.py --device-no"):
            kill_pid(pid)
        for pid in pids_listening_on_port(int(worker.get("backend_port", 0))):
            kill_pid(pid)
        worker["pid"] = None
        worker["claimed_task"] = None
        if worker.get("status") in {"starting", "running", "error"}:
            worker["status"] = "idle"
    reset_inflight_tasks(session)


def sync_worker_runtime_state(session: dict) -> dict:
    for worker in session.get("workers", {}).values():
        pid = worker.get("pid")
        if is_process_alive(pid):
            continue
        worker["pid"] = None
        if worker.get("status") in {"starting", "running"}:
            worker["claimed_task"] = None
            worker["status"] = "error"
    session["updated_at"] = iso_now()
    return session


def reconcile_task_runtime_state(session: dict) -> dict:
    active_claims = {
        worker.get("claimed_task")
        for worker in session.get("workers", {}).values()
        if worker.get("status") == "running" and worker.get("claimed_task")
    }
    for task in session.get("tasks", []):
        if task.get("status") != "running":
            continue
        if task.get("id") in active_claims:
            continue
        task["status"] = "pending"
        task["claimed_by"] = None
        task["started_at"] = None
    session["updated_at"] = iso_now()
    return session


def backend_script(workspace: Path, worker_name: str, device_no: int, port: int, batch_size: int) -> str:
    paths = workspace_paths(workspace)
    backend_log = paths["logs"] / f"backend_{worker_name}.log"
    return (
        f"cd {shlex_quote(str(workspace))} && "
        f"PYTHONUNBUFFERED=1 PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True "
        f"{shlex_quote(sys.executable)} da3_inference_server.py --device-no {device_no} --port {port} --batch-size {batch_size} "
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
    upload_root = fare_drive.get("upload_root", "da3-output").strip("/")
    output_dir = workspace / "output" / str(task["video_name"]) / str(task["file_name"])
    if not output_dir.exists():
        raise RuntimeError(f"Expected output directory is missing: {output_dir}")
    remote_path = "/".join(part for part in [upload_root, str(task["video_name"]), str(task["file_name"])] if part)
    return upload_file_via_fare_drive(workspace, fare_drive, output_dir, remote_path)


def get_video_frame_count(video_path: Path) -> int | None:
    try:
        import cv2  # type: ignore
    except Exception:
        return None
    capture = cv2.VideoCapture(str(video_path))
    if not capture.isOpened():
        capture.release()
        return None
    try:
        frame_count = int(capture.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
    finally:
        capture.release()
    return frame_count or None


def extract_video_frames(video_path: Path, output_dir: Path, *, frame_start: int = 0, frame_end: int | None = None) -> list[str]:
    try:
        import cv2  # type: ignore
    except Exception as exc:
        raise RuntimeError("opencv-python is required to extract frames from downloaded videos.") from exc

    if not video_path.exists():
        raise RuntimeError(f"Video file is missing: {video_path}")

    prepared_video_path = prepare_video_for_frame_extraction(video_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    existing = sorted(output_dir.glob("frame_*.png"))
    if existing:
        if all(is_image_readable(path) for path in existing):
            return [str(path) for path in existing]
        shutil.rmtree(output_dir, ignore_errors=True)
        output_dir.mkdir(parents=True, exist_ok=True)

    capture = cv2.VideoCapture(str(prepared_video_path))
    if not capture.isOpened():
        capture.release()
        if prepared_video_path != video_path:
            capture = cv2.VideoCapture(str(video_path))
            if capture.isOpened():
                prepared_video_path = video_path
            else:
                capture.release()
                raise RuntimeError(f"Failed to open video for frame extraction: {prepared_video_path}")
        else:
            raise RuntimeError(f"Failed to open video for frame extraction: {prepared_video_path}")

    frame_paths: list[str] = []
    if frame_start > 0:
        capture.set(cv2.CAP_PROP_POS_FRAMES, frame_start)

    frame_index = frame_start
    try:
        while True:
            if frame_end is not None and frame_index > frame_end:
                break
            ok, frame = capture.read()
            if not ok or frame is None:
                break
            frame_path = output_dir / f"frame_{frame_index:06d}.png"
            temp_frame_path = output_dir / f"frame_{frame_index:06d}.partial.png"
            temp_frame_path.unlink(missing_ok=True)
            if not cv2.imwrite(str(temp_frame_path), frame, [cv2.IMWRITE_PNG_COMPRESSION, 3]):
                raise RuntimeError(f"Failed to write extracted frame: {frame_path}")
            temp_frame_path.replace(frame_path)
            frame_paths.append(str(frame_path))
            frame_index += 1
    finally:
        capture.release()

    if not frame_paths:
        raise RuntimeError(f"No frames were extracted from video: {prepared_video_path}")
    return frame_paths


def is_image_readable(image_path: Path) -> bool:
    if Image is None:
        return image_path.exists() and image_path.stat().st_size > 0
    try:
        with Image.open(image_path) as handle:
            handle.verify()
        return True
    except (FileNotFoundError, OSError, UnidentifiedImageError):
        return False


def is_video_readable(video_path: Path) -> bool:
    try:
        import cv2  # type: ignore
    except Exception:
        return video_path.exists() and video_path.stat().st_size > 0

    capture = cv2.VideoCapture(str(video_path))
    if not capture.isOpened():
        capture.release()
        return False
    capture.release()
    return True


def prepare_video_for_frame_extraction(video_path: Path) -> Path:
    if video_path.suffix.lower() != ".dav":
        return video_path

    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        return video_path

    converted_path = video_path.with_suffix(".mp4")
    if converted_path.exists():
        if converted_path.stat().st_size > 0 and is_video_readable(converted_path):
            return converted_path
        converted_path.unlink(missing_ok=True)

    lock_path = converted_path.with_suffix(f"{converted_path.suffix}.lock")
    with session_lock(lock_path):
        if converted_path.exists():
            if converted_path.stat().st_size > 0 and is_video_readable(converted_path):
                return converted_path
            converted_path.unlink(missing_ok=True)

        temp_converted_path = converted_path.with_name(f".{converted_path.name}.{os.getpid()}.tmp")
        temp_converted_path.unlink(missing_ok=True)
        try:
            proc = subprocess.run(
                [
                    ffmpeg,
                    "-y",
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-fflags",
                    "+genpts",
                    "-i",
                    str(video_path),
                    "-map",
                    "0:v:0",
                    "-map",
                    "0:a?",
                    "-c",
                    "copy",
                    "-movflags",
                    "+faststart",
                    str(temp_converted_path),
                ],
                text=True,
                capture_output=True,
            )
            if proc.returncode == 0 and temp_converted_path.exists() and temp_converted_path.stat().st_size > 0 and is_video_readable(temp_converted_path):
                temp_converted_path.replace(converted_path)
                return converted_path
        finally:
            temp_converted_path.unlink(missing_ok=True)
    return video_path


def resolve_task_image_paths(workspace: Path, task: dict) -> list[str]:
    image_paths = [str(path) for path in task.get("image_paths", []) if path]
    if image_paths:
        return image_paths

    video_path_value = task.get("video_path", "")
    if not video_path_value:
        raise RuntimeError(f"Task {task.get('id', 'unknown')} has neither image_paths nor video_path.")

    video_path = Path(video_path_value)
    if not video_path.is_absolute():
        video_path = (workspace / video_path).resolve()

    frames_dir = workspace / "incoming" / "frames" / safe_segment(task.get("id", video_path.stem), video_path.stem)
    frame_start = int(task.get("frame_start", 0) or 0)
    frame_end_value = task.get("frame_end")
    frame_end = int(frame_end_value) if frame_end_value not in (None, "") else None
    return extract_video_frames(video_path, frames_dir, frame_start=frame_start, frame_end=frame_end)


def cleanup_task_input_artifacts(workspace: Path, task: dict) -> None:
    if task.get("image_paths"):
        return
    frame_dir = workspace / "incoming" / "frames" / safe_segment(task.get("id", "task"), "task")
    if frame_dir.exists():
        shutil.rmtree(frame_dir, ignore_errors=True)


def append_worker_log(worker_log: Path, message: str) -> None:
    worker_log.parent.mkdir(parents=True, exist_ok=True)
    lines = [line.strip() for line in str(message).replace("\r\n", "\n").splitlines() if line.strip()]
    if not lines:
        lines = ["-"]
    with worker_log.open("a", encoding="utf-8") as handle:
        for line in lines:
            handle.write(f"{iso_now()} | {line}\n")


def worker_loop(workspace: Path, worker_name: str) -> int:
    paths = ensure_workspace(workspace)
    session = read_session(workspace)
    worker = session["workers"][worker_name]
    device_no = int(worker["gpu"])
    backend_port = int(worker["backend_port"])
    inference_batch_size = int(session.get("inference_batch_size", 16))
    worker_log = Path(worker["log_path"])
    worker_log.parent.mkdir(parents=True, exist_ok=True)
    append_worker_log(
        worker_log,
        f"worker-start name={worker_name} gpu={device_no} backend_port={backend_port} "
        f"batch_size={inference_batch_size} export_format={session.get('export_format', DEFAULT_EXPORT_FORMAT)}",
    )

    backend_process = subprocess.Popen(
        ["bash", "-lc", backend_script(workspace, worker_name, device_no, backend_port, inference_batch_size)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    append_worker_log(worker_log, f"backend-spawn pid={backend_process.pid} port={backend_port}")
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
            append_worker_log(worker_log, f"backend-ready port={backend_port}")
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
                append_worker_log(worker_log, "no-task-claimed")
                append_worker_log(worker_log, "idle")
                break
            started = time.time()
            try:
                append_worker_log(
                    worker_log,
                    f"task-claimed id={task['id']} file={task['file_name']} "
                    f"frame_start={task.get('frame_start')} frame_end={task.get('frame_end')}",
                )
                append_worker_log(worker_log, f"input-resolve-start id={task['id']}")
                image_paths = resolve_task_image_paths(workspace, task)
                append_worker_log(worker_log, f"input-resolve-done id={task['id']} frames={len(image_paths)}")
                append_worker_log(
                    worker_log,
                    f"task-start id={task['id']} file={task['file_name']} frames={len(image_paths)} "
                    f"batch_size={int(session.get('inference_batch_size', 16))} export_format={task.get('export_format', DEFAULT_EXPORT_FORMAT)}",
                )
                append_worker_log(worker_log, f"inference-request-start id={task['id']} port={backend_port}")
                response = send_inference_request(
                    "127.0.0.1",
                    backend_port,
                    {
                        "image_paths": image_paths,
                        "video_name": task["video_name"],
                        "file_name": task["file_name"],
                        "export_format": task.get("export_format", DEFAULT_EXPORT_FORMAT),
                        "batch_size": int(session.get("inference_batch_size", 16)),
                    },
                )
                append_worker_log(
                    worker_log,
                    f"inference-request-done id={task['id']} status={response.get('status', 'unknown')} "
                    f"elapsed_ms={response.get('elapsed_ms', '-')}",
                )
                if response.get("status") != "success":
                    raise RuntimeError(response.get("message", "Unknown inference error"))
                elapsed_ms = int((time.time() - started) * 1000)
                session = read_session(workspace)
                append_worker_log(worker_log, f"upload-start id={task['id']}")
                uploaded_path = upload_artifacts_via_fare_drive(workspace, session, task)
                append_worker_log(worker_log, f"upload-done id={task['id']} uploaded_path={uploaded_path}")
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
                checkpoint_uploaded_path = None
                try:
                    checkpoint_uploaded_path = maybe_upload_session_checkpoint(workspace)
                except Exception as exc:
                    append_worker_log(
                        worker_log,
                        f"checkpoint-error id={task['id']} type={type(exc).__name__} error={exc}",
                    )
                append_worker_log(
                    worker_log,
                    f"task-success id={task['id']} elapsed_ms={elapsed_ms} uploaded_path={uploaded_path}",
                )
                if checkpoint_uploaded_path:
                    append_worker_log(
                        worker_log,
                        f"checkpoint-uploaded id={task['id']} remote_path={checkpoint_uploaded_path}",
                    )
            except Exception as exc:
                with session_lock(paths["lock"]):
                    session = read_session(workspace)
                    heartbeat(session, worker_name, os.getpid())
                    complete_task(session, worker_name, task["id"], error=str(exc))
                    save_session(workspace, session)
                append_worker_log(
                    worker_log,
                    f"task-error id={task['id']} type={type(exc).__name__} error={exc}",
                )
                append_worker_log(
                    worker_log,
                    f"task-traceback id={task['id']}\n{traceback.format_exc().strip()}",
                )
            finally:
                append_worker_log(worker_log, f"cleanup-start id={task['id']}")
                cleanup_task_input_artifacts(workspace, task)
                append_worker_log(worker_log, f"cleanup-done id={task['id']}")
    finally:
        append_worker_log(worker_log, f"backend-stop pid={backend_process.pid}")
        backend_process.terminate()
        try:
            backend_process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            append_worker_log(worker_log, f"backend-kill pid={backend_process.pid}")
            backend_process.kill()
        append_worker_log(worker_log, "worker-stop")
    return 0


def launch(workspace: Path) -> dict:
    paths = ensure_workspace(workspace)
    with session_lock(paths["lock"]):
        session = read_session(workspace)
        if not session:
            raise SystemExit("Session is not initialized. Run init-session first.")
        cleanup_launch_processes(session)
        save_session(workspace, session)
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
    session = reconcile_task_runtime_state(sync_worker_runtime_state(read_session(workspace)))
    save_json(paths["session"], refresh_summary(session))
    fare_drive_status = "unknown"
    fare_drive = session.get("fare_drive", {})
    endpoint = fare_drive.get("endpoint", "")
    client_home = fare_drive.get("client_home", "")
    access_token = fare_drive.get("access_token", "")
    if endpoint and client_home:
        fare_drive_status = f"client:{endpoint}"
    elif endpoint:
        fare_drive_status = f"configured:{endpoint}"
    elif access_token and client_home:
        fare_drive_status = "client:token-configured"
    elif access_token:
        fare_drive_status = "configured:token-only"
    return {
        "workspace": str(workspace),
        "session": refresh_summary(session),
        "runtime": load_json(paths["runtime"], {}),
        "fare_drive_status": fare_drive_status,
    }


def handle_init_session(args: argparse.Namespace) -> None:
    session = init_session(Path(args.workspace), args.config_file)
    print(json.dumps(refresh_summary(session), indent=2))


def handle_update_session_config(args: argparse.Namespace) -> None:
    session = update_session_config(Path(args.workspace), args.config_file)
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
    print(f"Video frame task size: {session.get('video_frame_task_size', VIDEO_FRAME_TASK_SIZE)}")
    print(f"Export format: {session.get('export_format', DEFAULT_EXPORT_FORMAT)}")
    print(f"Pending: {summary.get('pending', 0)}")
    print(f"Running: {summary.get('running', 0)}")
    print(f"Completed: {summary.get('completed', 0)}")
    print(f"Failed: {summary.get('failed', 0)}")


def handle_retry_failed(args: argparse.Namespace) -> None:
    session = retry_failed_tasks(Path(args.workspace))
    print(json.dumps(session, indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Remote DA3 multi-worker runtime.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init-session", help="Create or reset pipeline session state")
    init_parser.add_argument("--workspace", default=str(DEFAULT_WORKSPACE))
    init_parser.add_argument("--config-file")
    init_parser.set_defaults(handler=handle_init_session)

    update_parser = subparsers.add_parser("update-session-config", help="Update config-backed session fields without resetting tasks")
    update_parser.add_argument("--workspace", default=str(DEFAULT_WORKSPACE))
    update_parser.add_argument("--config-file")
    update_parser.set_defaults(handler=handle_update_session_config)

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

    retry_parser = subparsers.add_parser("retry-failed", help="Move failed tasks back to pending")
    retry_parser.add_argument("--workspace", default=str(DEFAULT_WORKSPACE))
    retry_parser.set_defaults(handler=handle_retry_failed)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.handler(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
