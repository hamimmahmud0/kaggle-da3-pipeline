#!/usr/bin/env python3
from __future__ import annotations

import argparse
import io
import json
import os
import posixpath
import re
import shlex
import socket
import sys
import tarfile
import time
from pathlib import Path, PurePosixPath


ROOT = Path(__file__).resolve().parent
REMOTE_PIPELINE_LOCAL = ROOT / "da3_remote_pipeline.py"
REMOTE_LAUNCHER_LOCAL = ROOT / "run_da3_pipeline.sh"
REMOTE_INFERENCE_SERVER_LOCAL = ROOT / "da3_inference_server.py"
FARE_DRIVE_LOCAL = ROOT / "Fare-Drive"
DEFAULT_CONFIG_FILE = ROOT / "da3_remote.sample.json"

ENV_KEYS = {
    "host": "DA3_HOST",
    "port": "DA3_PORT",
    "username": "DA3_USERNAME",
    "password": "DA3_PASSWORD",
    "remote_workspace": "DA3_REMOTE_WORKSPACE",
    "remote_miniforge": "DA3_REMOTE_MINIFORGE",
    "remote_env_name": "DA3_REMOTE_ENV_NAME",
    "remote_fare_drive_client_home": "DA3_REMOTE_FARE_DRIVE_CLIENT_HOME",
    "local_fare_drive_endpoint": "DA3_LOCAL_FARE_DRIVE_ENDPOINT",
    "local_fare_drive_access_token": "DA3_LOCAL_FARE_DRIVE_ACCESS_TOKEN",
    "local_fare_drive_upload_root": "DA3_LOCAL_FARE_DRIVE_UPLOAD_ROOT",
    "transport": "DA3_TRANSPORT",
    "drive_folder_url": "DA3_DRIVE_FOLDER_URL",
    "manifest_path": "DA3_MANIFEST_PATH",
    "worker_count": "DA3_WORKER_COUNT",
    "inference_batch_size": "DA3_INFERENCE_BATCH_SIZE",
}

DEFAULTS = {
    "host": "127.0.0.1",
    "port": 10022,
    "username": "notebook",
    "remote_workspace": "/kaggle/working/DA3",
    "remote_miniforge": "/kaggle/working/miniforge3",
    "remote_env_name": "da3-remote",
    "remote_fare_drive_client_home": "/kaggle/working/DA3/.fare-drive-client",
    "local_fare_drive_endpoint": "",
    "local_fare_drive_access_token": "",
    "local_fare_drive_upload_root": "da3-output",
    "transport": "fare-drive",
    "manifest_path": "",
    "worker_count": 2,
    "inference_batch_size": 16,
}


class RemoteError(RuntimeError):
    pass


def _require_paramiko():
    try:
        import paramiko  # type: ignore
    except ImportError as exc:  # pragma: no cover - dependency guidance
        raise SystemExit(
            "Missing dependency: paramiko. Create the local environment from "
            "environment.local.yml or install paramiko first."
        ) from exc
    return paramiko


class RemoteRunner:
    def __init__(self, host: str, port: int, username: str, password: str):
        paramiko = _require_paramiko()
        self._paramiko = paramiko
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.client.connect(
            host,
            port=port,
            username=username,
            password=password,
            timeout=20,
            banner_timeout=20,
            auth_timeout=20,
        )

    def close(self) -> None:
        self.client.close()

    def bash(self, script: str, timeout: int = 3600, check: bool = True) -> tuple[int, str, str]:
        command = "bash -lc " + shlex.quote(script)
        stdin, stdout, stderr = self.client.exec_command(command, timeout=timeout)
        out = stdout.read().decode("utf-8", "replace")
        err = stderr.read().decode("utf-8", "replace")
        code = stdout.channel.recv_exit_status()
        if check and code != 0:
            raise RemoteError(f"remote command failed ({code})\nSTDOUT:\n{out}\nSTDERR:\n{err}")
        return code, out, err

    def _stream_bytes(self, script: str, payload: bytes, timeout: int = 3600) -> tuple[int, str, str]:
        command = "bash -lc " + shlex.quote(script)
        stdin, stdout, stderr = self.client.exec_command(command, timeout=timeout)
        stdin.write(payload)
        stdin.flush()
        stdin.channel.shutdown_write()
        out = stdout.read().decode("utf-8", "replace")
        err = stderr.read().decode("utf-8", "replace")
        code = stdout.channel.recv_exit_status()
        if code != 0:
            raise RemoteError(f"remote command failed ({code})\nSTDOUT:\n{out}\nSTDERR:\n{err}")
        return code, out, err

    def write_text(self, remote_path: str, content: str, executable: bool = False) -> None:
        remote = PurePosixPath(remote_path)
        script = "\n".join(
            [
                f"mkdir -p {shlex.quote(str(remote.parent))}",
                "python -c " + shlex.quote(
                    "from pathlib import Path; import sys; Path(%r).write_bytes(sys.stdin.buffer.read())" % str(remote)
                ),
            ]
        )
        self._stream_bytes(script, content.encode("utf-8"), timeout=600)
        if executable:
            self.bash(f"chmod 755 {shlex.quote(str(remote))}", timeout=120)

    def upload_file(self, local_path: Path, remote_path: str, executable: bool = False) -> None:
        remote = PurePosixPath(remote_path)
        script = "\n".join(
            [
                f"mkdir -p {shlex.quote(str(remote.parent))}",
                "python -c " + shlex.quote(
                    "from pathlib import Path; import sys; Path(%r).write_bytes(sys.stdin.buffer.read())" % str(remote)
                ),
            ]
        )
        self._stream_bytes(script, local_path.read_bytes(), timeout=1800)
        if executable:
            self.bash(f"chmod 755 {shlex.quote(str(remote))}", timeout=120)

    def upload_tree(self, local_dir: Path, remote_dir: str) -> None:
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
            archive.add(local_dir, arcname=local_dir.name)
        payload = buffer.getvalue()
        remote_parent = str(PurePosixPath(remote_dir).parent)
        remote_name = PurePosixPath(remote_dir).name
        extract_code = """import io
import shutil
import sys
import tarfile
from pathlib import Path

target_parent = Path(%r)
target_name = %r
target_parent.mkdir(parents=True, exist_ok=True)
target = target_parent / target_name
if target.exists():
    shutil.rmtree(target)
payload = sys.stdin.buffer.read()
with tarfile.open(fileobj=io.BytesIO(payload), mode='r:gz') as archive:
    archive.extractall(target_parent)
""" % (remote_parent, remote_name)
        script = "\n".join(
            [
                f"mkdir -p {shlex.quote(remote_parent)}",
                "python -c " + shlex.quote(extract_code),
            ]
        )
        self._stream_bytes(script, payload, timeout=1800)


def print_step(message: str) -> None:
    print(f"\n==> {message}")


def parse_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        raise FileNotFoundError(f".env file not found: {path}")
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def parse_json_config(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"config file not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("config file must contain a JSON object")
    return payload


def resolve_config(args: argparse.Namespace) -> dict:
    cfg = dict(DEFAULTS)

    env_file = Path(args.env_file) if args.env_file else ROOT / ".env"
    if env_file.exists():
        env_values = parse_env_file(env_file)
        for key, env_name in ENV_KEYS.items():
            if env_name in env_values and env_values[env_name] != "":
                cfg[key] = env_values[env_name]

    if args.config_file:
        config_values = parse_json_config(Path(args.config_file))
        for key in ENV_KEYS:
            if key in config_values and config_values[key] not in (None, ""):
                cfg[key] = config_values[key]

    for key in ENV_KEYS:
        value = getattr(args, key, None)
        if value not in (None, ""):
            cfg[key] = value

    cfg["port"] = int(cfg["port"])
    cfg["worker_count"] = int(cfg["worker_count"])
    cfg["inference_batch_size"] = int(cfg["inference_batch_size"])

    required_by_command = {
        "verify": ["password"],
        "setup": ["password", "local_fare_drive_access_token"],
        "upload-pipeline": ["password"],
        "launch": ["password"],
        "status": ["password"],
        "datop": ["password"],
        "datalog": ["password"],
        "full": ["password", "local_fare_drive_access_token"],
    }
    missing = [key for key in required_by_command.get(args.command, []) if cfg.get(key) in (None, "")]
    if missing:
        raise ValueError(
            "Missing required settings: "
            + ", ".join(missing)
            + ". Provide them via CLI flags, --env-file, or --config-file."
        )
    return cfg


def verify_local_files() -> None:
    required = [REMOTE_PIPELINE_LOCAL, REMOTE_LAUNCHER_LOCAL, REMOTE_INFERENCE_SERVER_LOCAL, FARE_DRIVE_LOCAL]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Missing required local files: {', '.join(missing)}")


def verify_remote(runner: RemoteRunner) -> None:
    print_step("Verifying remote connection and runtime")
    _, out, _ = runner.bash("python --version && pwd && (nvidia-smi || true)", timeout=300)
    print(out.strip())


def bootstrap_remote_workspace(runner: RemoteRunner, cfg: dict) -> None:
    print_step("Preparing remote workspace")
    workspace = shlex.quote(cfg["remote_workspace"])
    script = f"""
set -e
mkdir -p {workspace}/logs {workspace}/tmp {workspace}/artifacts {workspace}/incoming {workspace}/outgoing
"""
    runner.bash(script, timeout=300)


def bootstrap_remote_miniforge(runner: RemoteRunner, cfg: dict) -> None:
    print_step("Ensuring Miniforge exists on the remote host")
    remote_miniforge = shlex.quote(cfg["remote_miniforge"])
    script = f"""
set -e
if [ ! -x {remote_miniforge}/micromamba ]; then
  cd /tmp
  wget -q -O Miniforge3.sh https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh
  bash Miniforge3.sh -b -p {remote_miniforge}
fi
"""
    runner.bash(script, timeout=7200)


def upload_pipeline_assets(runner: RemoteRunner, cfg: dict) -> None:
    print_step("Uploading DA3 runtime and preparing Fare Drive sources")
    workspace = PurePosixPath(cfg["remote_workspace"])
    runner.upload_file(REMOTE_PIPELINE_LOCAL, str(workspace / "da3_remote_pipeline.py"))
    runner.upload_file(REMOTE_LAUNCHER_LOCAL, str(workspace / "run_da3_pipeline.sh"), executable=True)
    runner.upload_file(REMOTE_INFERENCE_SERVER_LOCAL, str(workspace / "da3_inference_server.py"))
    runner.upload_file(ROOT / "environment.remote.yml", str(workspace / "environment.remote.yml"))
    fare_drive_remote = posixpath.join(cfg["remote_workspace"], "Fare-Drive")
    script = f"""
set -e
if [ ! -d {shlex.quote(posixpath.join(fare_drive_remote, '.git'))} ]; then
  rm -rf {shlex.quote(fare_drive_remote)}
  git clone https://github.com/hamimmahmud0/Fare-Drive.git {shlex.quote(fare_drive_remote)}
else
  git -C {shlex.quote(fare_drive_remote)} fetch --depth 1 origin
  git -C {shlex.quote(fare_drive_remote)} reset --hard origin/main || git -C {shlex.quote(fare_drive_remote)} reset --hard origin/master
fi
"""
    runner.bash(script, timeout=3600)


def build_remote_env(runner: RemoteRunner, cfg: dict) -> None:
    print_step("Creating or updating the remote conda environment")
    miniforge = cfg["remote_miniforge"]
    workspace = cfg["remote_workspace"]
    env_name = cfg["remote_env_name"]
    script = f"""
set -e
export MAMBA_ROOT_PREFIX={shlex.quote(miniforge)}
source <({shlex.quote(miniforge)}/micromamba shell hook -s bash)
if ! {shlex.quote(miniforge)}/micromamba env list | grep -q '/envs/{shlex.quote(env_name).strip(chr(39))}$'; then
  {shlex.quote(miniforge)}/micromamba create -y -n {shlex.quote(env_name)} python=3.12 pip
fi
{shlex.quote(miniforge)}/micromamba run -n {shlex.quote(env_name)} python -m pip install --upgrade pip setuptools wheel
{shlex.quote(miniforge)}/micromamba run -n {shlex.quote(env_name)} python -m pip install psutil
cd {shlex.quote(posixpath.join(workspace, 'Fare-Drive'))}
{shlex.quote(miniforge)}/micromamba run -n {shlex.quote(env_name)} python -m pip install -e .
"""
    runner.bash(script, timeout=7200)


def configure_remote_fare_drive_client(runner: RemoteRunner, cfg: dict) -> None:
    print_step("Configuring remote Fare Drive client")
    miniforge = cfg["remote_miniforge"]
    env_name = cfg["remote_env_name"]
    client_home = cfg["remote_fare_drive_client_home"]
    access_token = cfg["local_fare_drive_access_token"]
    script = f"""
set -e
export MAMBA_ROOT_PREFIX={shlex.quote(miniforge)}
source <({shlex.quote(miniforge)}/micromamba shell hook -s bash)
mkdir -p {shlex.quote(client_home)}
HOME={shlex.quote(client_home)} {shlex.quote(miniforge)}/micromamba run -n {shlex.quote(env_name)} \
  fare-drive client login-token --access-token {shlex.quote(access_token)}
"""
    runner.bash(script, timeout=1800)


def initialize_remote_session(runner: RemoteRunner, cfg: dict) -> None:
    print_step("Initializing remote DA3 session state")
    workspace = cfg["remote_workspace"]
    miniforge = cfg["remote_miniforge"]
    env_name = cfg["remote_env_name"]
    session_config = {
        "transport": cfg["transport"],
        "drive_folder_url": cfg.get("drive_folder_url", ""),
        "manifest_path": cfg.get("manifest_path", ""),
        "worker_count": cfg["worker_count"],
        "inference_batch_size": cfg["inference_batch_size"],
        "fare_drive": {
            "endpoint": cfg["local_fare_drive_endpoint"],
            "access_token": cfg["local_fare_drive_access_token"],
            "client_home": cfg["remote_fare_drive_client_home"],
            "upload_root": cfg["local_fare_drive_upload_root"],
        },
    }
    remote_config_path = str(PurePosixPath(workspace) / "remote-session-config.json")
    runner.write_text(remote_config_path, json.dumps(session_config, indent=2) + "\n")
    script = f"""
set -e
export MAMBA_ROOT_PREFIX={shlex.quote(miniforge)}
source <({shlex.quote(miniforge)}/micromamba shell hook -s bash)
cd {shlex.quote(workspace)}
{shlex.quote(miniforge)}/micromamba run -n {shlex.quote(env_name)} python da3_remote_pipeline.py init-session \
  --workspace {shlex.quote(workspace)} \
  --config-file {shlex.quote(remote_config_path)}
"""
    runner.bash(script, timeout=1800)


def launch_remote_pipeline(runner: RemoteRunner, cfg: dict) -> None:
    print_step("Launching remote DA3 workers")
    workspace = cfg["remote_workspace"]
    miniforge = cfg["remote_miniforge"]
    env_name = cfg["remote_env_name"]
    env_python = posixpath.join(miniforge, 'envs', env_name, 'bin', 'python')
    script = f"""
set -e
cd {shlex.quote(workspace)}
DA3_ENV_PYTHON={shlex.quote(env_python)} ./run_da3_pipeline.sh launch --workspace {shlex.quote(workspace)}
"""
    runner.bash(script, timeout=300)


def remote_status(runner: RemoteRunner, cfg: dict) -> str:
    workspace = cfg["remote_workspace"]
    miniforge = cfg["remote_miniforge"]
    env_name = cfg["remote_env_name"]
    script = f"""
set -e
export MAMBA_ROOT_PREFIX={shlex.quote(miniforge)}
source <({shlex.quote(miniforge)}/micromamba shell hook -s bash)
cd {shlex.quote(workspace)}
{shlex.quote(miniforge)}/micromamba run -n {shlex.quote(env_name)} python da3_remote_pipeline.py status --workspace {shlex.quote(workspace)} --json
"""
    _, out, _ = runner.bash(script, timeout=300)
    return out.strip()


def print_status(runner: RemoteRunner, cfg: dict) -> None:
    print_step("Remote DA3 status")
    print(remote_status(runner, cfg))


def wait_for_port(host: str, port: int, timeout: float = 15.0) -> None:
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
    raise RemoteError(f"Timed out waiting for {host}:{port}")


def command_verify(args: argparse.Namespace) -> None:
    cfg = resolve_config(args)
    with RemoteRunner(cfg["host"], cfg["port"], cfg["username"], cfg["password"]) as runner:  # type: ignore[attr-defined]
        verify_remote(runner)


def command_setup(args: argparse.Namespace) -> None:
    cfg = resolve_config(args)
    verify_local_files()
    with RemoteRunner(cfg["host"], cfg["port"], cfg["username"], cfg["password"]) as runner:  # type: ignore[attr-defined]
        verify_remote(runner)
        bootstrap_remote_workspace(runner, cfg)
        bootstrap_remote_miniforge(runner, cfg)
        upload_pipeline_assets(runner, cfg)
        build_remote_env(runner, cfg)
        configure_remote_fare_drive_client(runner, cfg)
        initialize_remote_session(runner, cfg)
        print(f"Remote Fare Drive client is configured for {cfg['local_fare_drive_endpoint']}")


def command_upload_pipeline(args: argparse.Namespace) -> None:
    cfg = resolve_config(args)
    verify_local_files()
    with RemoteRunner(cfg["host"], cfg["port"], cfg["username"], cfg["password"]) as runner:  # type: ignore[attr-defined]
        upload_pipeline_assets(runner, cfg)


def command_launch(args: argparse.Namespace) -> None:
    cfg = resolve_config(args)
    with RemoteRunner(cfg["host"], cfg["port"], cfg["username"], cfg["password"]) as runner:  # type: ignore[attr-defined]
        launch_remote_pipeline(runner, cfg)
        print_status(runner, cfg)


def command_status(args: argparse.Namespace) -> None:
    cfg = resolve_config(args)
    with RemoteRunner(cfg["host"], cfg["port"], cfg["username"], cfg["password"]) as runner:  # type: ignore[attr-defined]
        print_status(runner, cfg)


def command_full(args: argparse.Namespace) -> None:
    command_setup(args)
    command_launch(args)


def render_datop(status_payload: dict) -> str:
    session = status_payload.get("session", {})
    summary = session.get("summary", {})
    lines = [
        "Datop",
        f"Workspace: {status_payload.get('workspace', 'unknown')}",
        f"Transport: {session.get('transport', 'unknown')}",
        f"Tasks: pending={summary.get('pending', 0)} running={summary.get('running', 0)} completed={summary.get('completed', 0)} failed={summary.get('failed', 0)} total={summary.get('total', 0)}",
        f"Fare Drive: {status_payload.get('fare_drive_status', 'unknown')}",
        "",
        "Workers:",
    ]
    workers = session.get("workers", {})
    for worker_name in sorted(workers):
        worker = workers[worker_name]
        lines.append(
            f"  {worker_name}: status={worker.get('status', 'unknown')} gpu={worker.get('gpu')} task={worker.get('claimed_task') or '-'} heartbeat={worker.get('last_heartbeat') or '-'}"
        )
    return "\n".join(lines)


def command_datop(args: argparse.Namespace) -> None:
    cfg = resolve_config(args)
    with RemoteRunner(cfg["host"], cfg["port"], cfg["username"], cfg["password"]) as runner:  # type: ignore[attr-defined]
        while True:
            payload = json.loads(remote_status(runner, cfg))
            if sys.stdout.isatty():
                sys.stdout.write("\x1b[2J\x1b[H")
            print(render_datop(payload))
            if args.once:
                break
            time.sleep(args.refresh_seconds)


def command_datalog(args: argparse.Namespace) -> None:
    cfg = resolve_config(args)
    with RemoteRunner(cfg["host"], cfg["port"], cfg["username"], cfg["password"]) as runner:  # type: ignore[attr-defined]
        workspace = cfg["remote_workspace"]
        script = (
            f"cd {shlex.quote(workspace)} && "
            f"tail -n {int(args.lines)} -F logs/pipeline.log logs/worker_a.log logs/worker_b.log logs/fare-drive.log"
        )
        transport = runner.client.get_transport()
        if transport is None:
            raise RemoteError("SSH transport is not available.")
        channel = transport.open_session()
        channel.exec_command("bash -lc " + shlex.quote(script))
        try:
            while True:
                if channel.recv_ready():
                    sys.stdout.write(channel.recv(4096).decode("utf-8", "replace"))
                    sys.stdout.flush()
                if channel.recv_stderr_ready():
                    sys.stderr.write(channel.recv_stderr(4096).decode("utf-8", "replace"))
                    sys.stderr.flush()
                if channel.exit_status_ready():
                    break
                time.sleep(0.1)
        finally:
            channel.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage the DA3 remote pipeline over SSH.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_common(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument("--env-file")
        subparser.add_argument("--config-file")
        for key in ENV_KEYS:
            flag = "--" + key.replace("_", "-")
            subparser.add_argument(flag, dest=key)

    verify = subparsers.add_parser("verify", help="Verify remote access and GPU visibility")
    add_common(verify)
    verify.set_defaults(handler=command_verify)

    setup = subparsers.add_parser("setup", help="Bootstrap remote workspace, env, and Fare Drive")
    add_common(setup)
    setup.set_defaults(handler=command_setup)

    upload = subparsers.add_parser("upload-pipeline", help="Upload the latest runtime files")
    add_common(upload)
    upload.set_defaults(handler=command_upload_pipeline)

    launch = subparsers.add_parser("launch", help="Launch the remote DA3 worker processes")
    add_common(launch)
    launch.set_defaults(handler=command_launch)

    status = subparsers.add_parser("status", help="Print remote pipeline status")
    add_common(status)
    status.set_defaults(handler=command_status)

    datop = subparsers.add_parser("datop", help="Show a live remote status dashboard")
    add_common(datop)
    datop.add_argument("--refresh-seconds", type=float, default=2.0)
    datop.add_argument("--once", action="store_true")
    datop.set_defaults(handler=command_datop)

    datalog = subparsers.add_parser("datalog", help="Tail remote logs")
    add_common(datalog)
    datalog.add_argument("--lines", type=int, default=200)
    datalog.set_defaults(handler=command_datalog)

    full = subparsers.add_parser("full", help="Run setup then launch")
    add_common(full)
    full.set_defaults(handler=command_full)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        args.handler(args)
    except (RemoteError, FileNotFoundError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


def _remote_runner_enter(self: RemoteRunner) -> RemoteRunner:
    return self


def _remote_runner_exit(self: RemoteRunner, exc_type, exc, tb) -> None:
    self.close()


RemoteRunner.__enter__ = _remote_runner_enter  # type: ignore[attr-defined]
RemoteRunner.__exit__ = _remote_runner_exit  # type: ignore[attr-defined]


if __name__ == "__main__":
    raise SystemExit(main())
