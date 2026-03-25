from flask import Flask, jsonify, render_template_string, request, send_from_directory, url_for
from threading import Thread, Lock, Event
from werkzeug.serving import make_server
from werkzeug.utils import secure_filename
from pathlib import Path
from collections import deque
from datetime import datetime
from queue import Queue, Empty, Full
import os
import shutil
import subprocess
import ipaddress
import socket
import time
import itertools
import urllib.parse
import urllib.request
import uuid
import cv2
import json

TWELVE_HOURS_SEC = 12 * 60 * 60


def _get_process_start_time_epoch():
    # Best-effort kernel start timestamp (seconds since Unix epoch)
    try:
        import psutil  # type: ignore

        return float(psutil.Process(os.getpid()).create_time())
    except Exception:
        pass

    if os.name == "nt":
        try:
            import ctypes
            import ctypes.wintypes as wintypes

            class FILETIME(ctypes.Structure):
                _fields_ = [
                    ("dwLowDateTime", wintypes.DWORD),
                    ("dwHighDateTime", wintypes.DWORD),
                ]

            creation_time = FILETIME()
            exit_time = FILETIME()
            kernel_time = FILETIME()
            user_time = FILETIME()

            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel32.GetCurrentProcess.restype = wintypes.HANDLE
            kernel32.GetProcessTimes.argtypes = [
                wintypes.HANDLE,
                ctypes.POINTER(FILETIME),
                ctypes.POINTER(FILETIME),
                ctypes.POINTER(FILETIME),
                ctypes.POINTER(FILETIME),
            ]
            kernel32.GetProcessTimes.restype = wintypes.BOOL

            handle = kernel32.GetCurrentProcess()
            ok = kernel32.GetProcessTimes(
                handle,
                ctypes.byref(creation_time),
                ctypes.byref(exit_time),
                ctypes.byref(kernel_time),
                ctypes.byref(user_time),
            )
            if not ok:
                raise ctypes.WinError(ctypes.get_last_error())

            ft = (creation_time.dwHighDateTime << 32) + creation_time.dwLowDateTime
            return (ft / 10_000_000) - 11644473600
        except Exception:
            pass

    return None


def _format_hhmm(seconds: float) -> str:
    seconds = max(0, int(seconds))
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    return f"{hours:02d}:{minutes:02d}"


# --- Web terminal (call web_print(...) from other cells) ---
if "WEB_PRINT_QUEUE" not in globals():
    WEB_PRINT_QUEUE = Queue(maxsize=5000)
if "WEB_TERMINAL_BUFFER" not in globals():
    WEB_TERMINAL_BUFFER = deque(maxlen=2000)
if "WEB_TERMINAL_LOCK" not in globals():
    WEB_TERMINAL_LOCK = Lock()


def web_print(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    text = "" if msg is None else str(msg)
    lines = text.splitlines() or [""]
    for line in lines:
        entry = f"[{ts}] {line}"
        try:
            WEB_PRINT_QUEUE.put_nowait(entry)
        except Full:
            try:
                WEB_PRINT_QUEUE.get_nowait()
            except Empty:
                pass
            try:
                WEB_PRINT_QUEUE.put_nowait(entry)
            except Exception:
                pass


def _drain_web_print_queue(max_items: int = 500) -> int:
    drained = 0
    while drained < max_items:
        try:
            entry = WEB_PRINT_QUEUE.get_nowait()
        except Empty:
            break
        with WEB_TERMINAL_LOCK:
            WEB_TERMINAL_BUFFER.append(entry)
        drained += 1
    return drained


# --- Uploaded video queue (FIFO) ---
VIDEO_EXTENSIONS = {
    ".mp4",
    ".mov",
    ".mkv",
    ".avi",
    ".webm",
    ".flv",
    ".wmv",
    ".m4v",
    ".3gp",
    ".mpeg",
    ".mpg",
    ".dav",
}

if "VIDEO_UPLOAD_QUEUE" not in globals():
    VIDEO_UPLOAD_QUEUE = deque(maxlen=200)
if "VIDEO_QUEUE_LOCK" not in globals():
    VIDEO_QUEUE_LOCK = Lock()

# --- fps adjusted queue (prepared videos) ---
if "DA3_QUEUE" not in globals():
    DA3_QUEUE = deque(maxlen=200)
if "DA3_QUEUE_LOCK" not in globals():
    DA3_QUEUE_LOCK = Lock()
if "DA3_JOB_QUEUE" not in globals():
    DA3_JOB_QUEUE = Queue(maxsize=500)
if "DA3_SEEN" not in globals():
    DA3_SEEN = set()
if "DA3_SEEN_LOCK" not in globals():
    DA3_SEEN_LOCK = Lock()

# --- Frame queue (bounded frames on disk) ---
if "FRAME_QUEUE" not in globals():
    FRAME_QUEUE = deque(maxlen=100)
if "FRAME_QUEUE_LOCK" not in globals():
    FRAME_QUEUE_LOCK = Lock()
if "FRAME_QUEUE_FILL_LOCK" not in globals():
    FRAME_QUEUE_FILL_LOCK = Lock()
if "FRAME_VIDEO_STATE" not in globals():
    FRAME_VIDEO_STATE = {}
if "FRAME_VIDEO_STATE_LOCK" not in globals():
    FRAME_VIDEO_STATE_LOCK = Lock()
if "FRAME_VIDEO_ORDER" not in globals():
    FRAME_VIDEO_ORDER = deque()
if "FRAME_QUEUE_WAKE_EVENT" not in globals():
    FRAME_QUEUE_WAKE_EVENT = Event()
if "FRAME_QUEUE_STOP_EVENT" not in globals():
    FRAME_QUEUE_STOP_EVENT = Event()
if "FRAME_QUEUE_WORKER" not in globals():
    FRAME_QUEUE_WORKER = None

# --- DA3 process worker (multi-device) ---
if "DA3_PROCESS_WORKERS" not in globals():
    DA3_PROCESS_WORKERS = []
if "DA3_PROCESS_WORKERS_LOCK" not in globals():
    DA3_PROCESS_WORKERS_LOCK = Lock()
if "DA3_PROCESS_STOP_EVENT" not in globals():
    DA3_PROCESS_STOP_EVENT = Event()
if "DA3_PROCESS_START_EVENT" not in globals():
    DA3_PROCESS_START_EVENT = Event()
if "DA3_PROCESS_WORKER_MANAGER" not in globals():
    DA3_PROCESS_WORKER_MANAGER = None
if "DA3_BACKEND_WORKERS" not in globals():
    DA3_BACKEND_WORKERS = []
if "DA3_BACKEND_WORKERS_LOCK" not in globals():
    DA3_BACKEND_WORKERS_LOCK = Lock()
# --- MegaCMD upload queue ---
if "MEGA_UPLOAD_QUEUE" not in globals():
    MEGA_UPLOAD_QUEUE = Queue(maxsize=500)
if "MEGA_UPLOAD_QUEUE_LOCK" not in globals():
    MEGA_UPLOAD_QUEUE_LOCK = Lock()
if "MEGA_FILE_UPLOAD_QUEUE" not in globals():
    MEGA_FILE_UPLOAD_QUEUE = Queue(maxsize=500)
if "MEGA_FILE_UPLOAD_QUEUE_LOCK" not in globals():
    MEGA_FILE_UPLOAD_QUEUE_LOCK = Lock()
if "MEGA_UPLOAD_WORKER" not in globals():
    MEGA_UPLOAD_WORKER = None
if "MEGA_FILE_UPLOAD_WORKER" not in globals():
    MEGA_FILE_UPLOAD_WORKER = None
if "MEGA_UPLOAD_STOP_EVENT" not in globals():
    MEGA_UPLOAD_STOP_EVENT = Event()


def _is_video_path(path: Path) -> bool:
    # Prefer extension check; fall back to lightweight header sniffing.
    if path.suffix.lower() in VIDEO_EXTENSIONS:
        return True

    try:
        with open(path, "rb") as f:
            head = f.read(64)
    except Exception:
        return False

    # MP4/MOV/3GP family: ....ftyp....
    if len(head) >= 8 and head[4:8] == b"ftyp":
        return True
    # Matroska/WebM
    if head.startswith(bytes.fromhex("1a45dfa3")):
        return True
    # AVI
    if len(head) >= 12 and head.startswith(b"RIFF") and head[8:12] in {b"AVI ", b"AVIX"}:
        return True
    # FLV
    if head.startswith(b"FLV"):
        return True
    # Ogg container (may include video)
    if head.startswith(b"OggS"):
        return True
    # ASF (WMV)
    if head.startswith(bytes.fromhex("3026b2758e66cf11a6d900aa0062ce6c")):
        return True
    # MPEG program/elementary stream (very rough)
    if head[:4] in {bytes.fromhex("000001ba"), bytes.fromhex("000001b3")}:
        return True

    return False


def _ffmpeg_exe() -> str:
    exe = shutil.which("ffmpeg")
    if exe:
        return exe
    try:
        import imageio_ffmpeg  # type: ignore

        exe = imageio_ffmpeg.get_ffmpeg_exe()
        if exe:
            return exe
    except Exception:
        pass
    raise RuntimeError("ffmpeg not found. Install ffmpeg and ensure it's on PATH.")


def convert_dav_to_mp4(dav_path: Path) -> Path:
    if dav_path.suffix.lower() != ".dav":
        return dav_path

    out_path = dav_path.with_suffix(".mp4")
    if out_path.exists():
        out_path = dav_path.with_name(f"{dav_path.stem}_{uuid.uuid4().hex[:8]}.mp4")

    ffmpeg = _ffmpeg_exe()
    attempts = [
        ("video+audio", ["-map", "0:v:0", "-map", "0:a?", "-c", "copy"]),
        ("video-only", ["-map", "0:v:0", "-c", "copy"]),
    ]

    last_err = ""
    for label, extra in attempts:
        out_path.unlink(missing_ok=True)
        cmd = [
            ffmpeg,
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-fflags",
            "+genpts",
            "-i",
            str(dav_path),
            *extra,
            "-movflags",
            "+faststart",
            str(out_path),
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode == 0 and out_path.exists() and out_path.stat().st_size > 0:
            web_print(f"DAV->MP4 conversion ok ({label}): {out_path.name}")
            return out_path

        last_err = (proc.stderr or proc.stdout or "").strip()
        if len(last_err) > 3000:
            last_err = last_err[-3000:]

    out_path.unlink(missing_ok=True)
    raise RuntimeError(f"ffmpeg DAV->MP4 failed. {last_err}")

    return out_path


def convert_video_to_target_fps(src_path: Path, target_fps: int) -> Path:
    fps = int(target_fps)
    if fps < 1 or fps > 240:
        raise ValueError('Target FPS must be between 1 and 240.')

    base = secure_filename(src_path.stem) or 'video'
    base = base[:60]
    out_name = f'fpsadj_{uuid.uuid4().hex[:12]}_{base}_fps{fps}.mp4'
    out_path = UPLOAD_DIR / out_name

    ffmpeg = _ffmpeg_exe()
    attempts = [
        ('copy-audio', ['-c:a', 'copy']),
        ('aac-audio', ['-c:a', 'aac', '-b:a', '192k']),
    ]

    last_err = ''
    for label, audio_args in attempts:
        out_path.unlink(missing_ok=True)
        cmd = [
            ffmpeg,
            '-y',
            '-hide_banner',
            '-loglevel',
            'error',
            '-fflags',
            '+genpts',
            '-i',
            str(src_path),
            '-map',
            '0:v:0',
            '-map',
            '0:a?',
            '-vf',
            f'fps={fps}',
            '-c:v',
            'libx264',
            '-preset',
            'veryfast',
            '-crf',
            '0',
            *audio_args,
            '-movflags',
            '+faststart',
            str(out_path),
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode == 0 and out_path.exists() and out_path.stat().st_size > 0:
            web_print(f'FPS adjust ok ({label}): {out_path.name}')
            return out_path

        last_err = (proc.stderr or proc.stdout or '').strip()
        if len(last_err) > 3000:
            last_err = last_err[-3000:]

    out_path.unlink(missing_ok=True)
    raise RuntimeError(f'ffmpeg fps conversion failed. {last_err}')


def enqueue_da3_job(video_path: Path, source: str = '') -> None:
    fps = get_target_fps()
    key = (str(video_path), int(fps))
    with DA3_SEEN_LOCK:
        if key in DA3_SEEN:
            return
        DA3_SEEN.add(key)

    job = {
        'input_path': str(video_path),
        'input_stored_name': video_path.name,
        'target_fps': int(fps),
        'ts': float(time.time()),
        'source': str(source or ''),
    }
    try:
        DA3_JOB_QUEUE.put_nowait(job)
    except Full:
        with DA3_SEEN_LOCK:
            DA3_SEEN.discard(key)
        web_print(f'FPS adjust job queue full; dropped: {video_path.name}')
        return

    web_print(f'FPS adjust job queued: {video_path.name} -> {fps}fps')


def _da3_worker_loop():
    web_print('FPS adjust worker started')
    while True:
        try:
            job = DA3_JOB_QUEUE.get(timeout=1)
        except Empty:
            continue
        if job is None:
            break

        src_path = Path(job.get('input_path', ''))
        fps = int(job.get('target_fps', 15))
        try:
            out_path = convert_video_to_target_fps(src_path, fps)
            try:
                size_bytes = out_path.stat().st_size
            except Exception:
                size_bytes = 0

            item = {
                'stored_name': out_path.name,
                'path': str(out_path),
                'size_bytes': int(size_bytes),
                'size_mb': round(size_bytes / (1024 * 1024), 2),
                'ts': float(time.time()),
                'source': job.get('source', ''),
                'target_fps': fps,
                'input_stored_name': job.get('input_stored_name', src_path.name),
                'input_path': job.get('input_path', str(src_path)),
            }

            dropped = None
            with DA3_QUEUE_LOCK:
                if DA3_QUEUE.maxlen and len(DA3_QUEUE) >= DA3_QUEUE.maxlen:
                    dropped = DA3_QUEUE[0]
                DA3_QUEUE.append(item)

            if dropped:
                web_print(f"FPS adjusted queue full; dropped oldest: {dropped.get('stored_name')}")
            web_print(f"FPS adjusted queued: {out_path.name}")

            try:
                register_video_for_frame_queue(out_path, source=job.get('source', ''))
            except Exception as exc:
                web_print(f"Frame queue register failed for {out_path.name}: {exc}")
        except Exception as exc:
            web_print(f"FPS adjust failed for {src_path.name}: {exc}")


def start_da3_worker():
    global DA3_WORKER
    try:
        if DA3_WORKER is not None and DA3_WORKER.is_alive():
            return
    except Exception:
        pass
    DA3_WORKER = Thread(target=_da3_worker_loop, daemon=True, name='FPS-Adjust-Worker')
    DA3_WORKER.start()


def da3_queue_list():
    with DA3_QUEUE_LOCK:
        return list(DA3_QUEUE)


def da3_queue_pop():
    with DA3_QUEUE_LOCK:
        if not DA3_QUEUE:
            return None
        return DA3_QUEUE.popleft()


def _is_da3_set() -> bool:
    try:
        return bool(globals().get("isDA3Set", False))
    except Exception:
        return False


def register_video_for_frame_queue(video_path: Path, video_stored_name: str = "", source: str = "") -> None:
    if not video_stored_name:
        video_stored_name = video_path.name
    key = str(video_path.resolve())

    with FRAME_VIDEO_STATE_LOCK:
        if key in FRAME_VIDEO_STATE:
            return

        base = secure_filename(video_path.stem) or 'video'
        base = base[:60]
        digest = uuid.uuid5(uuid.NAMESPACE_URL, key).hex[:10]
        frames_dir = FRAMES_DIR / f'{base}_{digest}'

        FRAME_VIDEO_STATE[key] = {
            'video_path': key,
            'video_stored_name': str(video_stored_name),
            'video_file_url': f'/uploads/{video_stored_name}',
            'frames_dir': str(frames_dir),
            'next_frame_idx': 0,
            'frame_count': None,
            'report_every': 100,
            'next_report': 100,
            'ts': float(time.time()),
            'source': str(source or ''),
        }
        FRAME_VIDEO_ORDER.append(key)

    web_print(f'Frame queue: registered video: {video_path.name}')
    start_frame_queue_worker()
    FRAME_QUEUE_WAKE_EVENT.set()


def _opencv_extract_frame(video_path: Path, frame_idx: int, out_path: Path):
    cap = cv2.VideoCapture(str(video_path))
    if not cap.isOpened():
        cap.release()
        raise RuntimeError('OpenCV failed to open video')

    try:
        frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
        if frame_count <= 0:
            frame_count = None

        cap.set(cv2.CAP_PROP_POS_FRAMES, float(frame_idx))
        ok, frame = cap.read()
    finally:
        cap.release()

    if not ok or frame is None:
        return False, frame_count

    out_path.parent.mkdir(parents=True, exist_ok=True)
    ok2 = cv2.imwrite(str(out_path), frame, [cv2.IMWRITE_PNG_COMPRESSION, 3])
    if not ok2:
        raise RuntimeError('Failed to write frame image')
    return True, frame_count


def _next_frame_task_for_video(video_key: str):
    with FRAME_VIDEO_STATE_LOCK:
        state = FRAME_VIDEO_STATE.get(video_key)
        if not state:
            return None
        video_path = Path(state.get('video_path', ''))
        video_stored_name = state.get('video_stored_name', '') or video_path.name
        frames_dir = Path(state.get('frames_dir', ''))
        next_idx = int(state.get('next_frame_idx', 0))
        frame_count = state.get('frame_count', None)

    if not video_path.exists():
        raise FileNotFoundError(f'Video not found: {video_path}')

    if frame_count is not None and next_idx >= int(frame_count):
        return None

    try:
        batch_limit = get_batch_size()
    except Exception:
        batch_limit = 1
    batch_limit = max(1, int(batch_limit))

    frame_nos = []
    frame_download_urls = []
    frame_local_paths = []
    current_idx = int(next_idx)

    while len(frame_nos) < batch_limit:
        if frame_count is not None and current_idx >= int(frame_count):
            break

        out_path = frames_dir / f'frame_{current_idx:06d}.png'
        if not out_path.exists():
            ok, fc = _opencv_extract_frame(video_path, current_idx, out_path)
            if fc is not None:
                frame_count = int(fc)
            if not ok:
                with FRAME_VIDEO_STATE_LOCK:
                    st = FRAME_VIDEO_STATE.get(video_key)
                    if st is not None and frame_count is not None:
                        st['frame_count'] = int(frame_count)
                        st['done'] = True
                break

        rel = out_path.relative_to(FRAMES_DIR).as_posix()
        frame_nos.append(int(current_idx))
        frame_download_urls.append(f'/frames/{rel}')
        frame_local_paths.append(str(out_path.resolve()))
        current_idx += 1

    if not frame_nos:
        return None

    task = {
        'video_file_url': f'/uploads/{video_stored_name}',
        'frame_no': int(frame_nos[0]),
        'frame_no_start': int(frame_nos[0]),
        'frame_no_end': int(frame_nos[-1]),
        'frame_count': int(len(frame_nos)),
        'frame_download_url': frame_download_urls[0],
        'frame_local_path': frame_local_paths[0],
        'frame_nos': frame_nos,
        'frame_download_urls': frame_download_urls,
        'frame_local_paths': frame_local_paths,
    }

    msg = None
    with FRAME_VIDEO_STATE_LOCK:
        st = FRAME_VIDEO_STATE.get(video_key)
        if st is not None:
            if frame_count is not None:
                st['frame_count'] = int(frame_count)
                if st.get('report_every', 100) == 100:
                    rep = max(10, int(frame_count) // 20) if int(frame_count) else 100
                    st['report_every'] = int(rep)
                    st['next_report'] = int(rep)

            st['next_frame_idx'] = int(current_idx)
            extracted = int(st['next_frame_idx'])
            fc2 = st.get('frame_count', None)
            rep = int(st.get('report_every', 100) or 100)
            nxt = int(st.get('next_report', rep) or rep)

            if extracted == 1:
                msg = f'Frame queue: started {video_path.name}'
            elif extracted >= nxt:
                if fc2:
                    pct = int((extracted / int(fc2)) * 100)
                    msg = f'Frame queue: {video_path.name} {extracted}/{int(fc2)} ({pct}%)'
                else:
                    msg = f'Frame queue: {video_path.name} extracted {extracted}'
                st['next_report'] = int(nxt) + int(rep)

            if fc2 and extracted >= int(fc2):
                st['done'] = True
                msg = f'Frame queue: done {video_path.name} ({extracted}/{int(fc2)})'

    if msg:
        web_print(msg)

    return task


def _fill_frame_queue(max_new: int = 200) -> int:
    try:
        max_new = int(max_new)
    except Exception:
        max_new = 200
    max_new = max(1, min(max_new, 2000))

    filled = 0
    with FRAME_QUEUE_FILL_LOCK:
        while filled < max_new:
            with FRAME_QUEUE_LOCK:
                max_len = FRAME_QUEUE.maxlen or 0
                if max_len and len(FRAME_QUEUE) >= max_len:
                    break

            with FRAME_VIDEO_STATE_LOCK:
                if not FRAME_VIDEO_ORDER:
                    break
                key = FRAME_VIDEO_ORDER.popleft()

            try:
                task = _next_frame_task_for_video(key)
            except Exception as exc:
                with FRAME_VIDEO_STATE_LOCK:
                    FRAME_VIDEO_STATE.pop(key, None)
                web_print(f'Frame queue: dropped video due to error: {exc}')
                continue

            if not task:
                with FRAME_VIDEO_STATE_LOCK:
                    FRAME_VIDEO_STATE.pop(key, None)
                continue

            with FRAME_QUEUE_LOCK:
                max_len = FRAME_QUEUE.maxlen or 0
                if (not max_len) or len(FRAME_QUEUE) < max_len:
                    FRAME_QUEUE.append(task)
                    filled += 1

            with FRAME_VIDEO_STATE_LOCK:
                st = FRAME_VIDEO_STATE.get(key)
                if st and not st.get('done', False):
                    FRAME_VIDEO_ORDER.append(key)
                else:
                    FRAME_VIDEO_STATE.pop(key, None)

    return int(filled)


def _frame_queue_worker_loop():
    web_print('Frame queue worker started')
    while True:
        if FRAME_QUEUE_STOP_EVENT.is_set():
            break
        try:
            filled = _fill_frame_queue(max_new=200)
        except Exception as exc:
            filled = 0
            web_print(f'Frame queue worker error: {exc}')
        if filled <= 0:
            FRAME_QUEUE_WAKE_EVENT.wait(timeout=0.5)
            FRAME_QUEUE_WAKE_EVENT.clear()


def start_frame_queue_worker():
    global FRAME_QUEUE_WORKER
    try:
        if FRAME_QUEUE_WORKER is not None and FRAME_QUEUE_WORKER.is_alive():
            return
    except Exception:
        pass
    try:
        FRAME_QUEUE_STOP_EVENT.clear()
    except Exception:
        pass
    FRAME_QUEUE_WORKER = Thread(target=_frame_queue_worker_loop, daemon=True, name='Frame-Queue-Worker')
    FRAME_QUEUE_WORKER.start()


def stop_frame_queue_worker():
    web_print("Stopping frame queue worker...")
    try:
        FRAME_QUEUE_STOP_EVENT.set()
        FRAME_QUEUE_WAKE_EVENT.set()
    except Exception:
        pass
    try:
        if FRAME_QUEUE_WORKER is not None:
            FRAME_QUEUE_WORKER.join(timeout=2.0)
    except Exception:
        pass
    web_print("Frame queue worker stopped")


def da3_task_queue_peek(limit: int = 200):
    try:
        limit = int(limit)
    except Exception:
        limit = 200
    limit = max(1, min(limit, 2000))

    with FRAME_QUEUE_LOCK:
        items = list(itertools.islice(reversed(FRAME_QUEUE), limit))
    items.reverse()
    return items


def da3_task_queue_pop():
    with FRAME_QUEUE_LOCK:
        if not FRAME_QUEUE:
            return None
        item = FRAME_QUEUE.popleft()
    FRAME_QUEUE_WAKE_EVENT.set()
    return item


def da3_task_queue_pop_batch(max_items: int):
    try:
        max_items = int(max_items)
    except Exception:
        max_items = 1
    max_items = max(1, min(max_items, 5000))

    items = []
    with FRAME_QUEUE_LOCK:
        while FRAME_QUEUE and len(items) < max_items:
            items.append(FRAME_QUEUE.popleft())
    if items:
        FRAME_QUEUE_WAKE_EVENT.set()
    return items


# --- DA3 Processing Worker (multi-device) ---

def _send_da3_inference_request(host: str, port: int, payload: dict, timeout_s: int = 1800) -> dict:
    data = json.dumps(payload, ensure_ascii=True) + "\n"
    with socket.create_connection((host, int(port)), timeout=10) as s:
        s.settimeout(timeout_s)
        s.sendall(data.encode("utf-8"))
        chunks = []
        while True:
            chunk = s.recv(65536)
            if not chunk:
                break
            chunks.append(chunk)
            if b"\n" in chunk:
                break
        raw = b"".join(chunks).decode("utf-8").strip()
    if not raw:
        raise RuntimeError("Empty response from DA3 inference server.")
    line = raw.splitlines()[0]
    return json.loads(line)


def _task_video_name(frame_task) -> str:
    video_file_url = frame_task.get('video_file_url', '')
    return Path(video_file_url).stem if video_file_url else "unknown"


def _task_frame_paths(frame_task):
    paths = frame_task.get('frame_local_paths', None)
    if isinstance(paths, list) and paths:
        return [str(p) for p in paths if p]
    frame_path = frame_task.get('frame_local_path', '')
    return [str(frame_path)] if frame_path else []


def _task_frame_nos(frame_task):
    nos = frame_task.get('frame_nos', None)
    if isinstance(nos, list) and nos:
        out = []
        for no in nos:
            try:
                out.append(int(no))
            except Exception:
                out.append(0)
        return out
    try:
        return [int(frame_task.get('frame_no', 0))]
    except Exception:
        return [0]


def _group_tasks_by_video(tasks):
    grouped = {}
    for task in tasks:
        grouped.setdefault(_task_video_name(task), []).append(task)
    return grouped


def _run_da3_inference_batch(frame_tasks, device_no, backend_port_no):
    if not frame_tasks:
        return False

    if isinstance(frame_tasks, dict):
        frame_tasks = [frame_tasks]

    video_name = _task_video_name(frame_tasks[0])
    frame_paths = []
    frame_nos = []
    for task in frame_tasks:
        frame_paths.extend(_task_frame_paths(task))
        frame_nos.extend(_task_frame_nos(task))

    if not frame_paths:
        web_print(f"[Device {device_no}] No valid frame paths to process.")
        return False

    min_no = min(frame_nos) if frame_nos else 0
    max_no = max(frame_nos) if frame_nos else min_no
    batch_id = uuid.uuid4().hex[:8]
    file_name = f"batch_{min_no:06d}_{max_no:06d}_{batch_id}"

    payload = {
        "image_paths": frame_paths,
        "video_name": video_name,
        "file_name": file_name,
        "export_format": "npz",
    }

    web_print(f"[Device {device_no}] Sending {len(frame_paths)} frames to DA3 (port {backend_port_no})")
    try:
        resp = _send_da3_inference_request("127.0.0.1", backend_port_no, payload)
    except Exception as exc:
        web_print(f"[Device {device_no}] DA3 inference request failed: {exc}")
        return False

    if resp.get("status") == "success":
        elapsed_ms = resp.get("elapsed_ms")
        web_print(f"[Device {device_no}] Inference done: {file_name} ({elapsed_ms} ms)")
        if isMegaCredSet:
            output_dir = f"output/{video_name}/{file_name}"
            enqueue_mega_upload(video_name, file_name, output_dir, frame_paths)
        else:
            web_print(f"[Device {device_no}] MegaCMD not connected; skipping upload.")
        return True

    message = resp.get("message", "Unknown error")
    web_print(f"[Device {device_no}] Inference failed: {message}")
    return False


def _da3_backend_worker(device_no, backend_port_no):
    """Backend worker that hosts the DA3 inference server for a device"""
    web_print(f"[Backend {device_no}] Starting DA3 inference server on port {backend_port_no}")



    cmd = [
        "bash", "on_conda",
        "python", "da3_inference_server.py",
        "--device_no", str(device_no),
        "--port", str(backend_port_no),
    ]

    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            stdin=subprocess.PIPE,
            universal_newlines=True,
            bufsize=1
        )

        if process.stdout:
            for line in process.stdout:
                if DA3_PROCESS_STOP_EVENT.is_set():
                    break
                line = line.strip()
                if line:
                    web_print(f"[Backend {device_no}] {line}")

        if DA3_PROCESS_STOP_EVENT.is_set():
            try:
                process.terminate()
            except Exception:
                pass

        process.wait()

        if process.returncode == 0:
            web_print(f"[Backend {device_no}] Inference server stopped")
        else:
            web_print(f"[Backend {device_no}] Inference server exited with code {process.returncode}")

    except Exception as exc:
        web_print(f"[Backend {device_no}] Error: {exc}")

def _da3_sub_worker(device_no):
    """Sub-worker for a specific device that sends frame batches to the inference server"""
    web_print(f"[Device {device_no}] Sub-worker started, waiting for DA3...")

    # Wait until isDA3Set = True
    while not _is_da3_set():
        if DA3_PROCESS_STOP_EVENT.is_set():
            return
        DA3_PROCESS_START_EVENT.wait(timeout=1.0)

    web_print(f"[Device {device_no}] DA3 ready, starting processing")
    backend_port_no = 8008 + int(device_no)

    while True:
        if DA3_PROCESS_STOP_EVENT.is_set():
            web_print(f"[Device {device_no}] Sub-worker stopping")
            break

        task = da3_task_queue_pop()

        if not task:
            time.sleep(0.5)
            continue

        _run_da3_inference_batch(task, device_no, backend_port_no)


def _da3_process_worker_manager():
    """Main worker that manages sub-workers for each device"""
    web_print("DA3 process worker manager started")
    
    # Wait until isDA3Set = True
    while not _is_da3_set():
        if DA3_PROCESS_STOP_EVENT.is_set():
            return
        DA3_PROCESS_START_EVENT.wait(timeout=1.0)
    
    web_print("DA3 is set, starting inference servers")

    device_count = get_device_count()
    with DA3_BACKEND_WORKERS_LOCK:
        while len(DA3_BACKEND_WORKERS) < device_count:
            device_no = len(DA3_BACKEND_WORKERS)
            backend_port_no = 8008 + device_no
            worker = Thread(
                target=_da3_backend_worker,
                args=(device_no, backend_port_no),
                daemon=True,
                name=f"backend_worker_{device_no}"
            )
            worker.start()
            DA3_BACKEND_WORKERS.append(worker)
            web_print(f"Started inference server for device {device_no} on port {backend_port_no}")

    web_print("Waiting 100s for inference servers to warm up")
    if DA3_PROCESS_STOP_EVENT.wait(timeout=100.0):
        return

    web_print("DA3 is set, spawning inference client workers")
    
    while True:
        if DA3_PROCESS_STOP_EVENT.is_set():
            web_print("DA3 process worker manager stopping")
            break
        
        # Get current device count
        device_count = get_device_count()
        device_nos = list(range(device_count))
        
        with DA3_PROCESS_WORKERS_LOCK:
            # Stop workers that exceed current device count
            while len(DA3_PROCESS_WORKERS) > device_count:
                worker = DA3_PROCESS_WORKERS.pop()
                # Workers will stop via stop event
            
            # Start new workers if needed
            while len(DA3_PROCESS_WORKERS) < device_count:
                device_no = len(DA3_PROCESS_WORKERS)
                worker = Thread(
                    target=_da3_sub_worker,
                    args=(device_no,),
                    daemon=True,
                    name=f'DA3-Sub-Worker-{device_no}'
                )
                worker.start()
                DA3_PROCESS_WORKERS.append(worker)
                web_print(f"Started DA3 sub-worker for device {device_no}")
        
        # Sleep and check for device count changes
        time.sleep(2.0)


def start_da3_process_workers():
    """Start the DA3 process worker manager"""
    global DA3_PROCESS_WORKER_MANAGER
    
    try:
        if DA3_PROCESS_WORKER_MANAGER is not None and DA3_PROCESS_WORKER_MANAGER.is_alive():
            return
    except Exception:
        pass
    
    try:
        DA3_PROCESS_STOP_EVENT.clear()
    except Exception:
        pass
    
    DA3_PROCESS_WORKER_MANAGER = Thread(
        target=_da3_process_worker_manager,
        daemon=True,
        name='DA3-Process-Worker-Manager'
    )
    DA3_PROCESS_WORKER_MANAGER.start()
    web_print("DA3 process worker manager started")


def stop_da3_process_workers():
    """Stop all DA3 process workers"""
    web_print("Stopping DA3 process workers...")
    
    DA3_PROCESS_STOP_EVENT.set()
    DA3_PROCESS_START_EVENT.set()  # Wake up any waiting workers
    
    with DA3_PROCESS_WORKERS_LOCK:
        for worker in DA3_PROCESS_WORKERS:
            try:
                worker.join(timeout=2.0)
            except Exception:
                pass
        DA3_PROCESS_WORKERS.clear()
    with DA3_BACKEND_WORKERS_LOCK:
        for worker in DA3_BACKEND_WORKERS:
            try:
                worker.join(timeout=2.0)
            except Exception:
                pass
        DA3_BACKEND_WORKERS.clear()
    
    try:
        if DA3_PROCESS_WORKER_MANAGER is not None:
            DA3_PROCESS_WORKER_MANAGER.join(timeout=2.0)
    except Exception:
        pass
    
    web_print("DA3 process workers stopped")


def enqueue_video(path: Path, source: str = "") -> None:
    original_path = path
    if path.suffix.lower() == ".dav":
        web_print(f"Converting DAV to MP4: {path.name}")
        try:
            path = convert_dav_to_mp4(path)
        except Exception as exc:
            web_print(f"DAV->MP4 conversion failed: {exc}")
            return
        web_print(f"Converted DAV -> MP4: {path.name}")
        source = (source + " (dav->mp4)") if source else "dav->mp4"

    if not _is_video_path(path):
        web_print(f"[enqueue_video] : {path} is not a video path")
        return

    try:
        size_bytes = path.stat().st_size
    except Exception:
        size_bytes = 0

    item = {
        "stored_name": path.name,
        "path": str(path),
        "size_bytes": int(size_bytes),
        "size_mb": round(size_bytes / (1024 * 1024), 2),
        "ts": float(time.time()),
        "source": str(source or ""),
        "original_stored_name": original_path.name if original_path != path else "",
        "original_path": str(original_path) if original_path != path else "",
    }

    dropped = None
    with VIDEO_QUEUE_LOCK:
        if VIDEO_UPLOAD_QUEUE.maxlen and len(VIDEO_UPLOAD_QUEUE) >= VIDEO_UPLOAD_QUEUE.maxlen:
            dropped = VIDEO_UPLOAD_QUEUE[0]
        VIDEO_UPLOAD_QUEUE.append(item)

    if dropped:
        web_print(f"Video queue full; dropped oldest: {dropped.get('stored_name')}")

    src = f" ({source})" if source else ""
    web_print(f"Enqueued video{src}: {item['stored_name']} ({item['size_mb']} MB)")

    try:
        enqueue_da3_job(path, source=source)
    except Exception as exc:
        web_print(f"FPS adjust enqueue failed for {path.name}: {exc}")


def video_queue_list():
    with VIDEO_QUEUE_LOCK:
        return list(VIDEO_UPLOAD_QUEUE)


def video_queue_pop():
    with VIDEO_QUEUE_LOCK:
        if not VIDEO_UPLOAD_QUEUE:
            return None
        return VIDEO_UPLOAD_QUEUE.popleft()


def _is_port_free(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.2)
        return sock.connect_ex((host, port)) != 0


def _pick_port(host: str, preferred: int = 5000) -> int:
    if _is_port_free(host, preferred):
        return preferred
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((host, 0))
        return int(sock.getsockname()[1])


class _ServerThread(Thread):
    def __init__(self, app: Flask, host: str, port: int):
        super().__init__(daemon=True)
        self._server = make_server(host, port, app)
        self._ctx = app.app_context()
        self._ctx.push()

    def run(self):
        self._server.serve_forever()

    def shutdown(self):
        self._server.shutdown()


# If you re-run this cell, stop the previous server first.
if "server" in globals():
    try:
        server.shutdown()
    except Exception:
        pass

# Persist kernel start time across cell re-runs.
if "_KERNEL_START_TS" not in globals():
    _KERNEL_START_TS = _get_process_start_time_epoch() or time.time()
KERNEL_START_TS = float(_KERNEL_START_TS)

# Persist settings across cell re-runs.
if "SETTINGS_LOCK" not in globals():
    SETTINGS_LOCK = Lock()
if "_TARGET_FPS" not in globals():
    _TARGET_FPS = 15
if "_DEVICE_COUNT" not in globals():
    _DEVICE_COUNT = 2
if "_MEGA_EMAIL" not in globals():
    _MEGA_EMAIL = ""
if "_MEGA_PASSWORD" not in globals():
    _MEGA_PASSWORD = ""
if "_MEGA_UPLOAD_DIR" not in globals():
    _MEGA_UPLOAD_DIR = "/"
if "isMegaCredSet" not in globals():
    isMegaCredSet = False
if "_MAX_FRAME_AMOUNT_IN_MEMORY" not in globals():
    _MAX_FRAME_AMOUNT_IN_MEMORY = int(FRAME_QUEUE.maxlen or 100)
if "_BATCH_SIZE" not in globals():
    _BATCH_SIZE = 4


def get_target_fps() -> int:
    with SETTINGS_LOCK:
        try:
            return int(_TARGET_FPS)
        except Exception:
            return 15


def set_target_fps(value) -> int:
    global _TARGET_FPS
    fps = int(value)
    if fps < 1 or fps > 240:
        raise ValueError("Target FPS must be between 1 and 240.")
    with SETTINGS_LOCK:
        _TARGET_FPS = fps
    return fps


def get_device_count() -> int:
    with SETTINGS_LOCK:
        try:
            return int(_DEVICE_COUNT)
        except Exception:
            return 2


def set_device_count(value) -> int:
    global _DEVICE_COUNT
    count = int(value)
    if count < 1 or count > 8:
        raise ValueError("Device count must be between 1 and 8.")
    with SETTINGS_LOCK:
        _DEVICE_COUNT = count
    return count


def _rebuild_frame_queue(max_len: int) -> None:
    global FRAME_QUEUE
    max_len = int(max_len)
    if max_len < 1:
        max_len = 1
    with FRAME_QUEUE_LOCK:
        items = list(FRAME_QUEUE)
        if max_len and len(items) > max_len:
            items = items[:max_len]
        FRAME_QUEUE = deque(items, maxlen=max_len)


def get_max_frame_amount_in_memory() -> int:
    with SETTINGS_LOCK:
        try:
            return int(_MAX_FRAME_AMOUNT_IN_MEMORY)
        except Exception:
            return int(FRAME_QUEUE.maxlen or 100)


def set_max_frame_amount_in_memory(value) -> int:
    global _MAX_FRAME_AMOUNT_IN_MEMORY
    count = int(value)
    if count < 1 or count > 5000:
        raise ValueError("Max frames in memory must be between 1 and 5000.")
    with SETTINGS_LOCK:
        _MAX_FRAME_AMOUNT_IN_MEMORY = count
    _rebuild_frame_queue(count)
    return count


def get_batch_size() -> int:
    with SETTINGS_LOCK:
        try:
            return int(_BATCH_SIZE)
        except Exception:
            return 16


def set_batch_size(value) -> int:
    global _BATCH_SIZE
    count = int(value)
    if count < 1 or count > 2048:
        raise ValueError("Batch size must be between 1 and 2048.")
    with SETTINGS_LOCK:
        _BATCH_SIZE = count
    return count


try:
    _rebuild_frame_queue(get_max_frame_amount_in_memory())
except Exception:
    pass


def _run_megacmd(cmd):
    try:
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except FileNotFoundError as exc:
        raise RuntimeError("MegaCMD not found. Install MegaCMD and ensure mega-login/mega-put are on PATH.") from exc
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(f"MegaCMD failed: {err or 'unknown error'}")
    return (proc.stdout or "").strip()


def set_mega_credentials(email: str, password: str, upload_dir: str) -> bool:
    global _MEGA_EMAIL, _MEGA_PASSWORD, _MEGA_UPLOAD_DIR, isMegaCredSet
    if not email or not password:
        return False
    upload_dir = (upload_dir or "/").strip() or "/"
    with SETTINGS_LOCK:
        _MEGA_EMAIL = email.strip()
        _MEGA_PASSWORD = password
        _MEGA_UPLOAD_DIR = upload_dir
    try:
        _run_megacmd(["mega-login", _MEGA_EMAIL, _MEGA_PASSWORD])
        with SETTINGS_LOCK:
            isMegaCredSet = True
        web_print("MegaCMD connected successfully")
        return True
    except Exception as exc:
        web_print(f"MegaCMD connection failed: {exc}")
        with SETTINGS_LOCK:
            isMegaCredSet = False
        return False


def enqueue_mega_upload(video_name, file_name, output_dir, frame_paths):
    if isinstance(frame_paths, (str, bytes)):
        frame_paths = [frame_paths]
    elif frame_paths is None:
        frame_paths = []
    item = {
        "video_name": video_name,
        "file_name": file_name,
        "output_dir": output_dir,
        "frame_paths": list(frame_paths),
        "expected_npz_count": 1,
        "ts": time.time(),
        "upload_dir": _MEGA_UPLOAD_DIR,
    }
    try:
        MEGA_UPLOAD_QUEUE.put_nowait(item)
    except Full:
        web_print(f"Mega upload queue full; dropped: {video_name}/{file_name}")
        return
    web_print(f"Enqueued for Mega upload: {video_name}/{file_name}")


def enqueue_mega_file_upload(zip_path: Path, remote_dir: str, video_name: str, file_name: str, output_dir: str, frame_paths):
    if isinstance(frame_paths, (str, bytes)):
        frame_paths = [frame_paths]
    elif frame_paths is None:
        frame_paths = []

    item = {
        "zip_path": str(zip_path),
        "remote_dir": str(remote_dir or "/"),
        "video_name": str(video_name or "unknown"),
        "file_name": str(file_name or "unknown"),
        "output_dir": str(output_dir or ""),
        "frame_paths": list(frame_paths),
        "ts": time.time(),
    }
    try:
        MEGA_FILE_UPLOAD_QUEUE.put_nowait(item)
    except Full:
        web_print(f"Mega file upload queue full; dropped: {video_name}/{file_name}")
        return False

    web_print(f"Queued Mega file upload: {zip_path.name} -> {remote_dir}")
    return True


def _zip_directory(src_dir, zip_path):
    import zipfile

    src_path = Path(src_dir)
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for file in src_path.rglob("*"):
            if file.is_file():
                arcname = file.relative_to(src_path)
                zipf.write(file, arcname)
    return zip_path


def _output_dir_snapshot(output_path: Path) -> tuple[int, int, int]:
    npz_files = list(output_path.rglob("*.npz"))
    total_size = 0
    latest_mtime_ns = 0
    for file_path in npz_files:
        try:
            stat = file_path.stat()
        except Exception:
            continue
        total_size += int(stat.st_size)
        latest_mtime_ns = max(latest_mtime_ns, int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000))))
    return (len(npz_files), int(total_size), int(latest_mtime_ns))


def _wait_for_output_dir_ready(output_path: Path, expected_npz_count: int, timeout_s: int = 900, poll_s: float = 1.0, stable_rounds: int = 3) -> tuple[int, int, int]:
    deadline = time.time() + max(1, int(timeout_s))
    expected_npz_count = max(1, int(expected_npz_count or 0))
    last_snapshot = None
    stable_hits = 0

    while time.time() < deadline:
        if not output_path.exists():
            time.sleep(max(0.2, float(poll_s)))
            continue

        snapshot = _output_dir_snapshot(output_path)
        npz_count = snapshot[0]
        if npz_count >= expected_npz_count:
            if snapshot == last_snapshot:
                stable_hits += 1
            else:
                stable_hits = 1
                last_snapshot = snapshot
            if stable_hits >= max(1, int(stable_rounds)):
                return snapshot
        else:
            last_snapshot = snapshot
            stable_hits = 0

        time.sleep(max(0.2, float(poll_s)))

    raise RuntimeError(
        f"Timed out waiting for output files in {output_path}. "
        f"Expected at least {expected_npz_count} npz files."
    )


def _mega_put(file_path: Path, upload_dir: str):
    upload_dir = (upload_dir or "/").strip() or "/"
    return _run_megacmd(["mega-put", str(file_path), upload_dir])


def _mega_safe_segment(value: str, fallback: str = "item") -> str:
    text = secure_filename(str(value or "").strip())
    return text or fallback


def _mega_join_path(base: str, *parts: str) -> str:
    cleaned = []
    base = (base or "/").strip() or "/"
    if base != "/":
        cleaned.append(base.strip("/"))
    for part in parts:
        segment = str(part or "").strip().strip("/")
        if segment:
            cleaned.append(segment)
    if not cleaned:
        return "/"
    return "/" + "/".join(cleaned)


def _mega_mkdir(remote_dir: str) -> None:
    remote_dir = (remote_dir or "/").strip() or "/"
    if remote_dir == "/":
        return
    try:
        _run_megacmd(["mega-mkdir", "-p", remote_dir])
    except Exception as exc:
        msg = str(exc).lower()
        if "folder already exists" in msg or "already exists" in msg:
            return
        raise


def _mega_remote_target(upload_dir: str, file_name: str) -> str:
    base = (upload_dir or "/").strip() or "/"
    if base == "/":
        return f"/{file_name}"
    return f"{base.rstrip('/')}/{file_name}"


def _wait_for_mega_file(remote_path: str, timeout_s: int = 3600, poll_s: float = 2.0) -> None:
    remote_path = (remote_path or "").strip()
    if not remote_path:
        raise ValueError("Remote path is required.")

    deadline = time.time() + max(1, int(timeout_s))
    last_exc = None
    while time.time() < deadline:
        try:
            _run_megacmd(["mega-ls", remote_path])
            return
        except Exception as exc:
            last_exc = exc
            time.sleep(max(0.2, float(poll_s)))

    raise RuntimeError(f"Timed out waiting for Mega file: {remote_path}. Last error: {last_exc}")


def _mega_file_upload_worker():
    web_print("Mega file upload worker started")
    while not isMegaCredSet:
        if MEGA_UPLOAD_STOP_EVENT.is_set():
            return
        time.sleep(1.0)
    web_print("MegaCMD credentials set, starting file uploads")
    while True:
        if MEGA_UPLOAD_STOP_EVENT.is_set():
            web_print("Mega file upload worker stopping")
            break
        try:
            item = MEGA_FILE_UPLOAD_QUEUE.get(timeout=1.0)
        except Exception:
            continue
        video_name = item.get("video_name", "unknown")
        file_name = item.get("file_name", "unknown")
        output_dir = item.get("output_dir", "")
        frame_paths = item.get("frame_paths", []) or []
        remote_dir = item.get("remote_dir", _MEGA_UPLOAD_DIR)
        zip_path = Path(item.get("zip_path", ""))
        if isinstance(frame_paths, (str, bytes)):
            frame_paths = [frame_paths]
        try:
            output_path = Path(output_dir)
            if not zip_path.exists():
                web_print(f"Zip file not found for upload: {zip_path}")
                continue
            web_print(f"Ensuring Mega directory exists: {remote_dir}")
            _mega_mkdir(remote_dir)
            web_print(f"Uploading with MegaCMD: {zip_path.name} -> {remote_dir}")
            _mega_put(zip_path, remote_dir)
            remote_zip_path = _mega_remote_target(remote_dir, zip_path.name)
            web_print(f"Waiting for Mega to confirm upload: {remote_zip_path}")
            _wait_for_mega_file(remote_zip_path)
            web_print(f"Uploaded to Mega: {video_name}/{file_name}")
            try:
                zip_path.unlink(missing_ok=True)
                web_print(f"Deleted zip: {zip_path}")
            except Exception as exc:
                web_print(f"Could not delete zip {zip_path}: {exc}")
            import shutil
            shutil.rmtree(output_path, ignore_errors=True)
            web_print(f"Deleted output dir: {output_dir}")
            for frame_path in frame_paths:
                try:
                    frame_file = Path(frame_path)
                    if frame_file.exists():
                        frame_file.unlink()
                        web_print(f"Deleted frame: {frame_path}")
                except Exception:
                    pass
        except Exception as exc:
            web_print(f"Mega upload failed for {video_name}/{file_name}: {exc}")


def _mega_upload_worker():
    web_print("Mega upload preparation worker started")
    while not isMegaCredSet:
        if MEGA_UPLOAD_STOP_EVENT.is_set():
            return
        time.sleep(1.0)
    web_print("MegaCMD credentials set, preparing uploads")
    while True:
        if MEGA_UPLOAD_STOP_EVENT.is_set():
            web_print("Mega upload preparation worker stopping")
            break
        try:
            item = MEGA_UPLOAD_QUEUE.get(timeout=1.0)
        except Exception:
            continue
        video_name = item.get("video_name", "unknown")
        file_name = item.get("file_name", "unknown")
        output_dir = item.get("output_dir", "")
        frame_paths = item.get("frame_paths", []) or []
        expected_npz_count = int(item.get("expected_npz_count", 1) or 1)
        upload_dir = item.get("upload_dir", _MEGA_UPLOAD_DIR)
        if isinstance(frame_paths, (str, bytes)):
            frame_paths = [frame_paths]
        try:
            output_path = Path(output_dir)
            zip_path = output_path.with_suffix(".zip")
            remote_video_dir = _mega_join_path(
                upload_dir,
                _mega_safe_segment(video_name, "video"),
            )
            if output_path.exists():
                web_print(
                    f"Waiting for DA3 outputs before zipping: {output_dir} "
                    f"(expected npz: {max(1, expected_npz_count)})"
                )
                snapshot = _wait_for_output_dir_ready(output_path, expected_npz_count=expected_npz_count)
                web_print(
                    f"DA3 outputs ready: {snapshot[0]} npz files, "
                    f"{_format_size_label(snapshot[1])}"
                )
                web_print(f"Zipping: {output_dir}")
                _zip_directory(output_path, zip_path)
                web_print(f"Created zip: {zip_path.name}")
            else:
                web_print(f"Output directory not found: {output_dir}")
                continue

            queued = enqueue_mega_file_upload(
                zip_path=zip_path,
                remote_dir=remote_video_dir,
                video_name=video_name,
                file_name=file_name,
                output_dir=output_dir,
                frame_paths=frame_paths,
            )
            if not queued:
                web_print(f"Prepared zip but could not queue upload: {zip_path}")
        except Exception as exc:
            web_print(f"Mega upload preparation failed for {video_name}/{file_name}: {exc}")


def start_mega_upload_worker():
    global MEGA_UPLOAD_WORKER, MEGA_FILE_UPLOAD_WORKER
    try:
        if MEGA_UPLOAD_WORKER is not None and MEGA_UPLOAD_WORKER.is_alive():
            if MEGA_FILE_UPLOAD_WORKER is not None and MEGA_FILE_UPLOAD_WORKER.is_alive():
                return
    except Exception:
        pass
    try:
        MEGA_UPLOAD_STOP_EVENT.clear()
    except Exception:
        pass
    MEGA_UPLOAD_WORKER = Thread(target=_mega_upload_worker, daemon=True, name="Mega-Upload-Worker")
    MEGA_UPLOAD_WORKER.start()
    MEGA_FILE_UPLOAD_WORKER = Thread(target=_mega_file_upload_worker, daemon=True, name="Mega-File-Upload-Worker")
    MEGA_FILE_UPLOAD_WORKER.start()
    web_print("Mega upload worker started")


def stop_mega_upload_worker():
    web_print("Stopping Mega upload worker...")
    MEGA_UPLOAD_STOP_EVENT.set()
    try:
        if MEGA_UPLOAD_WORKER is not None:
            MEGA_UPLOAD_WORKER.join(timeout=2.0)
    except Exception:
        pass
    try:
        if MEGA_FILE_UPLOAD_WORKER is not None:
            MEGA_FILE_UPLOAD_WORKER.join(timeout=2.0)
    except Exception:
        pass
    web_print("Mega upload worker stopped")


app = Flask(__name__)
UPLOAD_DIR = Path("outputs/uploads")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
FRAMES_DIR = UPLOAD_DIR / "frames"
FRAMES_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR = Path("/kaggle/working/output")
start_da3_worker()
start_frame_queue_worker()
start_da3_process_workers()
start_mega_upload_worker()

# On cell re-runs, also prepare already-queued videos.
try:
    with VIDEO_QUEUE_LOCK:
        _existing = list(VIDEO_UPLOAD_QUEUE)
    for _it in _existing:
        try:
            _p = Path(_it.get('path', ''))
            if _p and _p.exists():
                enqueue_da3_job(_p, source=_it.get('source', ''))
        except Exception:
            pass
except Exception:
    pass

# On cell re-runs, also build tasks for already FPS-adjusted videos.
try:
    with DA3_QUEUE_LOCK:
        _fps_existing = list(DA3_QUEUE)
    for _it in _fps_existing:
        try:
            _p = Path(_it.get('path', ''))
            if _p and _p.exists():
                register_video_for_frame_queue(_p, source=_it.get('source', ''))
        except Exception:
            pass
except Exception:
    pass
app.config["MAX_CONTENT_LENGTH"] = 25 * 1024 * 1024 * 1024  # 25GB


_HOME_HTML = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Flask + ngrok demo</title>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bulma@1.0.2/css/bulma.min.css">
    <style>
        html, body {
            height: 100%;
            margin: 0;
            display: flex;
            flex-direction: column;
        }

        /* Navbar fixed height 10% */
        .navbar {
            height: 10vh;
            min-height: 3rem;
            display: flex;
            align-items: center;
        }

        /* Main section fills remaining height, uses flex column */
        .main-section {
            flex: 1;
            display: flex;
            flex-direction: column;
            padding: 0.75rem;
            gap: 0.75rem;
            min-height: 0;
        }

        /* Top row: forms (height 40% of remaining space) */
        .top-row {
            height: 40%;
            min-height: 0;
            display: flex;
            gap: 0.75rem;
        }
        .top-row .column {
            height: 100%;
            overflow-y: auto;
        }

        /* Bottom row: queue + terminal (takes remaining height) */
        .bottom-row {
            flex: 1;
            min-height: 0;
            display: flex;
            gap: 0.75rem;
        }
        .bottom-row .column {
            height: 100%;
            display: flex;
            flex-direction: column;
        }
        .bottom-row .box {
            flex: 1;
            min-height: 0;
            display: flex;
            flex-direction: column;
        }

        .scrollable-content {
            overflow-y: auto;
            flex: 1;
            min-height: 0;
        }

        .terminal-pre {
            background: #0b0f0b;
            color: #d1f7c4;
            padding: 12px;
            border-radius: 6px;
            font-family: monospace;
            font-size: 13px;
            white-space: pre-wrap;
            height: 100%;
            margin: 0;
        }

        .small { font-size: 0.85rem; color: #666; }

        /* Column widths */
        .col-30 { width: 30%; flex-shrink: 0; }
        .col-40 { width: 40%; flex-shrink: 0; }
        .col-70 { width: 70%; flex-shrink: 0; }

        /* Two equal sub-columns inside the 60% area */
        .top-row-left {
            display: flex;
            gap: 0.75rem;
            flex: 1;       /* takes the space not used by col-40 */
            min-width: 0;
            height: 100%;
        }
        .top-row-left .sub-column {
            flex: 1;
            min-width: 0;
            height: 100%;
            overflow-y: auto;
        }

        /* ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Toast notifications ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ */
        #toast-container {
            position: fixed;
            top: 1rem;
            right: 1rem;
            z-index: 9999;
            display: flex;
            flex-direction: column;
            gap: 0.5rem;
            pointer-events: none;
        }
        .toast-item {
            pointer-events: auto;
            min-width: 260px;
            max-width: 420px;
            opacity: 0;
            transform: translateX(20px);
            transition: opacity 0.25s ease, transform 0.25s ease;
            box-shadow: 0 4px 16px rgba(0,0,0,0.15);
        }
        .toast-item.toast-visible {
            opacity: 1;
            transform: translateX(0);
        }
        .toast-item.toast-hiding {
            opacity: 0;
            transform: translateX(20px);
        }
        .toast-item .delete {
            pointer-events: auto;
        }
    </style>
</head>
<body>

    <!-- Toast container (rendered outside normal flow) -->
    <div id="toast-container"></div>

    <!-- Navbar: ~10vh, three sections -->
    <nav class="navbar" role="navigation" aria-label="main navigation">
        <div class="navbar-brand">
            <a class="navbar-item" href="/"><strong>DA3 Portal</strong></a>
        </div>
        <div class="navbar-menu">
            <div class="navbar-start">
                <span class="navbar-item">
                    Uptime: {{ uptime_hhmm }} | Remaining: {{ remaining_hhmm }}
                </span>
            </div>
            <div class="navbar-end">
                <a class="navbar-item" href="/">Home</a>
                <a class="navbar-item" href="/gallery">Gallery</a>
                <div class="navbar-item">
                    <form method="post" action="{{ url_for('shutdown_route') }}" onsubmit="return confirm('Stop everything and exit the process?');">
                        <button class="button is-danger is-light" type="submit">Shutdown</button>
                    </form>
                </div>
            </div>
        </div>
    </nav>

    <!-- Main section: flex column, fills remaining height -->
    <div class="main-section">

        <!-- Top row: forms (40% height) -->
        <div class="top-row">

            <!-- Left area (flex: 1): two equal sub-columns, one per form -->
            <div class="top-row-left">

                <!-- Sub-column 1: Download by URL -->
                <div class="sub-column">
                    <div class="box" style="height: 100%; overflow-y: auto;">
                        <h2 class="title is-5">Download by URL</h2>
                        <form method="post" action="{{ url_for('upload_url') }}">
                            <div class="field">
                                <label class="label" for="method">Method</label>
                                <div class="control">
                                    <div class="select">
                                        <select name="method" id="method" onchange="updateUrlMethodUI()">
                                            <option value="wget" {% if method == 'wget' %}selected{% endif %}>wget</option>
                                            <option value="google-drive" {% if method == 'google-drive' %}selected{% endif %}>google-drive</option>
                                        </select>
                                    </div>
                                </div>
                            </div>
                            <div class="field">
                                <label class="label" for="url">URL</label>
                                <div class="control">
                                    <input class="input" type="url" name="url" id="url"
                                           placeholder="https://example.com/file.jpg"
                                           value="{{ url_value }}" required>
                                </div>
                            </div>
                            <div class="field" id="filename_row" style="display: none;">
                                <label class="label" for="filename">Filename <span class="small">(required for google-drive)</span></label>
                                <div class="control">
                                    <input class="input" type="text" name="filename" id="filename"
                                           placeholder="output filename"
                                           value="{{ filename_value }}"
                                           disabled>
                                </div>
                            </div>
                            <div class="field">
                                <div class="control">
                                    <button class="button is-primary" type="submit">Download</button>
                                </div>
                            </div>
                        </form>
                    </div>
                </div>

                <!-- Sub-column 2: File Upload -->
                <div class="sub-column">
                    <div class="box" style="height: 100%; overflow-y: auto;">
                        <h2 class="title is-5">Upload a File</h2>
                        <form method="post" action="{{ url_for('upload_file') }}" enctype="multipart/form-data">
                            <div class="field">
                                <div class="file has-name is-fullwidth">
                                    <label class="file-label">
                                        <input class="file-input" type="file" name="file" id="file-input" required>
                                        <span class="file-cta">
                                            <span class="file-label">Choose file...</span>
                                        </span>
                                        <span class="file-name" id="file-name-display">
                                            No file chosen
                                        </span>
                                    </label>
                                </div>
                            </div>
                            <div class="field">
                                <div class="control">
                                    <button class="button is-primary" type="submit">Upload</button>
                                </div>
                            </div>
                        </form>
                    </div>
                </div>

            </div><!-- /.top-row-left -->

            <!-- Right column 40%: MegaCMD credentials + Settings -->
            <div class="column col-40">
                <div class="box" style="height: 100%; overflow-y: auto;">
                    <h2 class="title is-5">MegaCMD Credentials &amp; Settings</h2>

                    <form method="post" action="{{ url_for('set_mega_credentials_route') }}">
                        <div class="field">
                            <label class="label" for="mega_email">Email</label>
                            <div class="control">
                                <input class="input" type="email" id="mega_email" name="mega_email"
                                       value="{{ mega_email }}" placeholder="you@example.com" required>
                            </div>
                        </div>
                        <div class="field">
                            <label class="label" for="mega_password">Password</label>
                            <div class="control">
                                <input class="input" type="password" id="mega_password" name="mega_password"
                                       placeholder="Your Mega password" required>
                            </div>
                        </div>
                        <div class="field">
                            <label class="label" for="mega_upload_dir">Upload Folder</label>
                            <div class="control">
                                <input class="input" type="text" id="mega_upload_dir" name="mega_upload_dir"
                                       value="{{ mega_upload_dir }}" placeholder="/da3" required>
                            </div>
                        </div>
                        <div class="field">
                            <div class="control">
                                <button class="button is-info" type="submit">Connect to MegaCMD</button>
                            </div>
                        </div>
                    </form>

                    <p class="small mt-2">
                        Status:
                        {% if mega_cred_set %}
                            <span class="has-text-success">Connected</span>
                        {% else %}
                            <span class="has-text-danger">Not connected</span>
                        {% endif %}
                    </p>

                    <hr class="my-4">

                    <form method="post" action="{{ url_for('set_target_fps_route') }}">
                        <div class="field">
                            <label class="label" for="target_fps">Target FPS</label>
                            <div class="control">
                                <input class="input" type="number" id="target_fps" name="target_fps"
                                       min="1" max="240" step="1" value="{{ target_fps }}" required>
                            </div>
                        </div>
                        <div class="field">
                            <div class="control">
                                <button class="button is-info" type="submit">Set</button>
                            </div>
                        </div>
                    </form>

                    <form method="post" action="{{ url_for('set_device_count_route') }}">
                        <div class="field">
                            <label class="label" for="device_count">Device Count</label>
                            <div class="control">
                                <input class="input" type="number" id="device_count" name="device_count"
                                       min="1" max="8" step="1" value="{{ device_count }}" required>
                            </div>
                        </div>
                        <div class="field">
                            <div class="control">
                                <button class="button is-info" type="submit">Set</button>
                            </div>
                        </div>
                    </form>

                    <form method="post" action="{{ url_for('set_max_frame_amount_in_memory_route') }}">
                        <div class="field">
                            <label class="label" for="max_frame_amount_in_memory">Max Frames in Memory</label>
                            <div class="control">
                                <input class="input" type="number" id="max_frame_amount_in_memory" name="max_frame_amount_in_memory"
                                       min="1" max="5000" step="1" value="{{ max_frame_amount_in_memory }}" required>
                            </div>
                        </div>
                        <div class="field">
                            <div class="control">
                                <button class="button is-info" type="submit">Set</button>
                            </div>
                        </div>
                    </form>

                    <form method="post" action="{{ url_for('set_batch_size_route') }}">
                        <div class="field">
                            <label class="label" for="batch_size">Batch Size</label>
                            <div class="control">
                                <input class="input" type="number" id="batch_size" name="batch_size"
                                       min="1" max="2048" step="1" value="{{ batch_size }}" required>
                            </div>
                        </div>
                        <div class="field">
                            <div class="control">
                                <button class="button is-info" type="submit">Set</button>
                            </div>
                        </div>
                    </form>
                </div>
            </div>

        </div><!-- /.top-row -->

        <!-- Bottom row: queue status (30%) + terminal (70%) -->
        <div class="bottom-row">

            <!-- Left column: Queue status (30%) -->
            <div class="column col-30">
                <div class="box">
                    <h2 class="title is-5">Queue Status</h2>
                    <div class="scrollable-content">
                        <div class="content">
                            <p>
                                <strong>Video queue:</strong>
                                {{ video_queue_len }}{% if video_queue_max %} / {{ video_queue_max }}{% endif %}
                            </p>
                            {% if video_queue %}
                                <ol>
                                {% for item in video_queue %}
                                    <li>
                                        <a href="{{ url_for('get_upload', filename=item['stored_name']) }}">{{ item['stored_name'] }}</a>
                                        ({{ item['size_mb'] }} MB)
                                    </li>
                                {% endfor %}
                                </ol>
                            {% else %}
                                <p class="small">No videos queued yet.</p>
                            {% endif %}

                            <p>
                                <strong>FPS adjusted queue:</strong>
                                {{ fps_queue_len }}{% if fps_queue_max %} / {{ fps_queue_max }}{% endif %}
                                (pending jobs: {{ fps_jobs_pending }})
                            </p>
                            {% if fps_queue %}
                                <ol>
                                {% for item in fps_queue %}
                                    <li>
                                        <a href="{{ url_for('get_upload', filename=item['stored_name']) }}">{{ item['stored_name'] }}</a>
                                        ({{ item['size_mb'] }} MB, {{ item['target_fps'] }} fps)
                                    </li>
                                {% endfor %}
                                </ol>
                            {% else %}
                                <p class="small">No FPS adjusted videos yet.</p>
                            {% endif %}

                            <p>
                                <strong>DA3 ready:</strong> {{ da3_ready }} |
                                <strong>Tasks:</strong> {{ da3_task_queue_len }}{% if da3_task_queue_max %} / {{ da3_task_queue_max }}{% endif %}
                                (pending videos: {{ da3_task_pending_videos }})
                            </p>
                            <p class="small">Peek: <a href="/api/da3_task_queue">/api/da3_task_queue</a></p>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Right column: Terminal (70%) -->
            <div class="column col-70">
                <div class="box">
                    <h2 class="title is-5">Web Terminal</h2>
                    <p class="small">Call <code>web_print('hello')</code> from notebook cells to print here.</p>
                    <div class="scrollable-content">
                        <pre id="terminal" class="terminal-pre">{{ terminal_text|e }}</pre>
                    </div>
                </div>
            </div>

        </div><!-- /.bottom-row -->

    </div><!-- /.main-section -->

    <script>
        /* ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ URL method UI ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ */
        function updateUrlMethodUI() {
            var method = document.getElementById('method');
            var row    = document.getElementById('filename_row');
            var input  = document.getElementById('filename');
            if (!method || !row || !input) return;
            var needs = (method.value === 'google-drive');
            row.style.display  = needs ? 'block' : 'none';
            input.required     = needs;
            input.disabled     = !needs;
        }

        /* ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ File-input label ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ */
        document.addEventListener('DOMContentLoaded', function () {
            var fileInput   = document.getElementById('file-input');
            var fileDisplay = document.getElementById('file-name-display');
            if (fileInput && fileDisplay) {
                fileInput.addEventListener('change', function () {
                    fileDisplay.textContent = this.files.length
                        ? this.files[0].name
                        : 'No file chosen';
                });
            }
        });

        /* ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Toast notifications ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ */
        function showToast(html, colorClass) {
            var container = document.getElementById('toast-container');
            var toast = document.createElement('div');
            toast.className = 'toast-item notification ' + (colorClass || 'is-info') + ' is-light';

            // Close button
            var btn = document.createElement('button');
            btn.className = 'delete';
            btn.setAttribute('aria-label', 'Close');
            btn.addEventListener('click', function () { dismissToast(toast); });
            toast.appendChild(btn);

            // Message content
            var span = document.createElement('span');
            span.innerHTML = html;
            toast.appendChild(span);

            container.appendChild(toast);

            // Trigger enter animation on next frame
            requestAnimationFrame(function () {
                requestAnimationFrame(function () {
                    toast.classList.add('toast-visible');
                });
            });

            // Auto-dismiss after 5 s
            setTimeout(function () { dismissToast(toast); }, 5000);
        }

        function dismissToast(toast) {
            if (!toast || toast.dataset.dismissing) return;
            toast.dataset.dismissing = '1';
            toast.classList.remove('toast-visible');
            toast.classList.add('toast-hiding');
            setTimeout(function () { if (toast.parentNode) toast.parentNode.removeChild(toast); }, 300);
        }

        /* ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Terminal auto-refresh ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ */
        async function refreshTerminal() {
            var el = document.getElementById('terminal');
            if (!el) return;
            try {
                var res  = await fetch('/api/terminal');
                var data = await res.json();
                var shouldStick = (el.scrollTop + el.clientHeight + 30) >= el.scrollHeight;
                el.textContent = data.text || '';
                if (shouldStick) { el.scrollTop = el.scrollHeight; }
            } catch (e) {
                el.textContent = 'Error loading terminal: ' + e;
            }
        }

        /* ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Boot ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ */
        document.addEventListener('DOMContentLoaded', function () {
            updateUrlMethodUI();
            refreshTerminal();
            setInterval(refreshTerminal, 1000);

            /* Fire server-side flash messages as toasts */
            {% if message %}
                showToast({{ message | tojson }}, 'is-info');
            {% endif %}
            {% if uploaded_url %}
                showToast('Saved: <a href="{{ uploaded_url }}">{{ uploaded_url }}</a>', 'is-success');
            {% endif %}
        });
    </script>
</body>
</html>
"""


_GALLERY_HTML = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Gallery - DA3 Portal</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bulma@1.0.2/css/bulma.min.css">
  <style>
    :root {
      --gallery-blue: #1565c0;
      --gallery-blue-dark: #0f4fa8;
      --gallery-bg: #f4f6fa;
      --gallery-border: #dfe5ee;
      --gallery-text: #1f2937;
      --gallery-muted: #6b7280;
    }
    html, body {
      min-height: 100%;
      background: var(--gallery-bg);
      color: var(--gallery-text);
    }
    .gallery-shell {
      min-height: 100vh;
      background:
        radial-gradient(circle at top right, rgba(21, 101, 192, 0.08), transparent 22rem),
        linear-gradient(180deg, #f8fafc 0%, var(--gallery-bg) 100%);
    }
    .gallery-topbar {
      background: linear-gradient(135deg, var(--gallery-blue) 0%, var(--gallery-blue-dark) 100%);
      box-shadow: 0 10px 30px rgba(21, 101, 192, 0.18);
    }
    .gallery-topbar .navbar-item,
    .gallery-topbar .navbar-link,
    .gallery-topbar .title,
    .gallery-topbar .button {
      color: white;
    }
    .gallery-topbar .button.is-ghost:hover {
      background: rgba(255, 255, 255, 0.12);
    }
    .gallery-main {
      padding: 2rem 1.5rem 3rem;
    }
    .gallery-toolbar {
      background: rgba(255, 255, 255, 0.82);
      border: 1px solid rgba(223, 229, 238, 0.9);
      border-radius: 1rem;
      box-shadow: 0 12px 28px rgba(15, 23, 42, 0.06);
      padding: 1.25rem;
      backdrop-filter: blur(8px);
    }
    .gallery-browser {
      margin-top: 1.25rem;
      background: white;
      border: 1px solid var(--gallery-border);
      border-radius: 1rem;
      overflow: hidden;
      box-shadow: 0 16px 36px rgba(15, 23, 42, 0.06);
    }
    .gallery-table {
      width: 100%;
      border-collapse: collapse;
    }
    .gallery-table th,
    .gallery-table td {
      padding: 1.15rem 1.25rem;
      border-bottom: 1px solid #edf1f7;
      vertical-align: middle;
    }
    .gallery-table th {
      font-size: 0.95rem;
      font-weight: 700;
      color: #2b3442;
      background: #fbfcfe;
    }
    .gallery-row:hover {
      background: #f8fbff;
    }
    .gallery-name {
      display: flex;
      align-items: center;
      gap: 0.9rem;
      min-width: 0;
    }
    .gallery-link {
      color: var(--gallery-blue);
      font-weight: 700;
    }
    .gallery-link:hover {
      color: var(--gallery-blue-dark);
      text-decoration: underline;
    }
    .icon-tile {
      width: 2.6rem;
      height: 2.6rem;
      border-radius: 0.85rem;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      font-size: 0.75rem;
      font-weight: 800;
      color: white;
      flex-shrink: 0;
      box-shadow: inset 0 -6px 12px rgba(0, 0, 0, 0.08);
    }
    .icon-folder { background: linear-gradient(180deg, #ffc857 0%, #f59e0b 100%); color: #7c4a03; }
    .icon-image { background: linear-gradient(180deg, #7dd3fc 0%, #22c55e 100%); }
    .icon-archive { background: linear-gradient(180deg, #fcd34d 0%, #f97316 100%); }
    .icon-doc { background: linear-gradient(180deg, #60a5fa 0%, #2563eb 100%); }
    .icon-generic { background: linear-gradient(180deg, #c4b5fd 0%, #6366f1 100%); }
    .preview-thumb {
      width: 3rem;
      height: 3rem;
      border-radius: 0.85rem;
      object-fit: cover;
      border: 1px solid #dbe4f0;
      box-shadow: 0 4px 12px rgba(15, 23, 42, 0.08);
      flex-shrink: 0;
    }
    .meta-muted {
      color: var(--gallery-muted);
      font-size: 0.9rem;
    }
    .gallery-empty {
      padding: 4rem 1.5rem;
      text-align: center;
      color: var(--gallery-muted);
    }
    .gallery-footer {
      margin-top: 1.5rem;
      display: flex;
      justify-content: space-between;
      gap: 1rem;
      align-items: center;
      flex-wrap: wrap;
    }
    .gallery-pagination .pagination-link,
    .gallery-pagination .pagination-previous,
    .gallery-pagination .pagination-next {
      border-radius: 0.75rem;
    }
    @media (max-width: 860px) {
      .gallery-main {
        padding: 1rem 0.75rem 2rem;
      }
      .gallery-toolbar {
        padding: 1rem;
      }
      .gallery-table,
      .gallery-table thead,
      .gallery-table tbody,
      .gallery-table th,
      .gallery-table td,
      .gallery-table tr {
        display: block;
      }
      .gallery-table thead {
        display: none;
      }
      .gallery-row {
        border-bottom: 1px solid #edf1f7;
      }
      .gallery-table td {
        border-bottom: none;
        padding-top: 0.45rem;
        padding-bottom: 0.45rem;
      }
      .gallery-table td::before {
        content: attr(data-label);
        display: block;
        font-size: 0.72rem;
        font-weight: 700;
        color: var(--gallery-muted);
        text-transform: uppercase;
        letter-spacing: 0.04em;
        margin-bottom: 0.25rem;
      }
      .gallery-table td[data-label="Name"]::before {
        display: none;
      }
    }
  </style>
</head>
<body>
  <div class="gallery-shell">
    <nav class="navbar gallery-topbar" role="navigation" aria-label="gallery navigation">
      <div class="container is-fluid">
        <div class="navbar-brand">
          <a class="navbar-item" href="/gallery">
            <span class="title is-2 has-text-white mb-0">File Browser</span>
          </a>
        </div>
        <div class="navbar-menu is-active">
          <div class="navbar-end">
            <span class="navbar-item">
              <a class="button is-ghost" href="/">Home</a>
            </span>
            <span class="navbar-item">
              <button class="button is-ghost" type="button" disabled>Upload</button>
            </span>
            <span class="navbar-item">
              <button class="button is-ghost" type="button" disabled>New Folder</button>
            </span>
            <span class="navbar-item">
              <a class="button is-ghost" href="{{ refresh_url }}">Refresh</a>
            </span>
            <span class="navbar-item">
              <form method="post" action="{{ url_for('shutdown_route') }}" onsubmit="return confirm('Stop everything and exit the process?');">
                <button class="button is-danger" type="submit">Shutdown</button>
              </form>
            </span>
          </div>
        </div>
      </div>
    </nav>

    <section class="gallery-main">
      <div class="container is-fluid" style="max-width: 1280px;">
        <div class="gallery-toolbar">
          <div class="columns is-variable is-5 is-vcentered">
            <div class="column">
              <nav class="breadcrumb is-large mb-0" aria-label="breadcrumbs">
                <ul>
                  {% for part in breadcrumb %}
                    {% if loop.last %}
                      <li class="is-active"><a href="#" aria-current="page">{{ part.name }}</a></li>
                    {% else %}
                      <li><a href="{{ part.url }}">{{ part.name }}</a></li>
                    {% endif %}
                  {% endfor %}
                </ul>
              </nav>
            </div>
            <div class="column is-4-desktop is-5-tablet">
              <div class="field mb-0">
                <div class="control has-icons-left">
                  <input id="gallery-search" class="input is-medium" type="text" placeholder="Search..." autocomplete="off">
                  <span class="icon is-left"><strong>Q</strong></span>
                </div>
              </div>
            </div>
          </div>
          {% if message %}
            <article class="message is-info mt-4 mb-0">
              <div class="message-body">{{ message }}</div>
            </article>
          {% endif %}
        </div>

        <div class="gallery-browser">
          {% if entries %}
            <table class="table gallery-table is-fullwidth">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Size</th>
                  <th>Type</th>
                  <th>Modified</th>
                </tr>
              </thead>
              <tbody id="gallery-table-body">
                {% for entry in entries %}
                  <tr class="gallery-row" data-name="{{ entry.name|lower }}" data-kind="{{ entry.kind }}">
                    <td data-label="Name">
                      <div class="gallery-name">
                        {% if entry.is_image %}
                          <img src="{{ entry.url }}" alt="{{ entry.name }}" class="preview-thumb">
                        {% else %}
                          <span class="icon-tile {{ entry.icon_class }}">{{ entry.icon_text }}</span>
                        {% endif %}
                        <div class="is-flex is-flex-direction-column">
                          <a href="{{ entry.url }}" class="gallery-link">{{ entry.name }}</a>
                          <span class="meta-muted">{{ entry.kind_label }}</span>
                        </div>
                      </div>
                    </td>
                    <td data-label="Size">{{ entry.size_label }}</td>
                    <td data-label="Type">{{ entry.type_label }}</td>
                    <td data-label="Modified">{{ entry.modified_label }}</td>
                  </tr>
                {% endfor %}
              </tbody>
            </table>
          {% else %}
            <div class="gallery-empty">
              <p class="title is-4 mb-2">This folder is empty.</p>
              <p>No output files have been generated here yet.</p>
            </div>
          {% endif %}
        </div>

        <div class="gallery-footer">
          <div class="buttons mb-0">
            <a class="button is-medium is-light" href="{{ parent_url }}">Previous</a>
          </div>
          <nav class="pagination gallery-pagination is-centered mb-0" role="navigation" aria-label="pagination">
            <a class="pagination-previous" disabled>Previous</a>
            <a class="pagination-next" disabled>Next</a>
            <ul class="pagination-list">
              <li><a class="pagination-link is-current" aria-label="Page 1" aria-current="page">1</a></li>
            </ul>
          </nav>
          <div class="buttons mb-0">
            <a class="button is-medium is-link is-light" href="{{ refresh_url }}">Next</a>
          </div>
        </div>
      </div>
    </section>
  </div>

  <script>
    document.addEventListener('DOMContentLoaded', function () {
      var input = document.getElementById('gallery-search');
      var rows = Array.prototype.slice.call(document.querySelectorAll('.gallery-row'));
      if (!input || !rows.length) return;

      input.addEventListener('input', function () {
        var query = (input.value || '').trim().toLowerCase();
        rows.forEach(function (row) {
          var name = row.getAttribute('data-name') || '';
          row.style.display = (!query || name.indexOf(query) !== -1) ? '' : 'none';
        });
      });
    });
  </script>
</body>
</html>
"""


def _home_context(**kwargs):
    elapsed = time.time() - KERNEL_START_TS
    _drain_web_print_queue(max_items=2000)
    with WEB_TERMINAL_LOCK:
        terminal_text = "\n".join(WEB_TERMINAL_BUFFER)
    with VIDEO_QUEUE_LOCK:
        video_queue = list(VIDEO_UPLOAD_QUEUE)
    with DA3_QUEUE_LOCK:
        fps_queue = list(DA3_QUEUE)
    try:
        fps_jobs_pending = DA3_JOB_QUEUE.qsize()
    except Exception:
        fps_jobs_pending = 0
    with FRAME_QUEUE_LOCK:
        da3_task_queue_len = len(FRAME_QUEUE)
        da3_task_queue_max = FRAME_QUEUE.maxlen or 0
    with FRAME_VIDEO_STATE_LOCK:
        da3_task_pending_videos = len(FRAME_VIDEO_ORDER)
    ctx = {
        "uptime_hhmm": _format_hhmm(elapsed),
        "remaining_hhmm": _format_hhmm(TWELVE_HOURS_SEC - elapsed),
        "method": "wget",
        "url_value": "",
        "filename_value": "",
        "terminal_text": terminal_text,
        "target_fps": get_target_fps(),
        "device_count": get_device_count(),
        "max_frame_amount_in_memory": get_max_frame_amount_in_memory(),
        "batch_size": get_batch_size(),
        "video_queue_len": len(video_queue),
        "video_queue_max": VIDEO_UPLOAD_QUEUE.maxlen or 0,
        "video_queue": video_queue,
        "fps_queue_len": len(fps_queue),
        "fps_queue_max": DA3_QUEUE.maxlen or 0,
        "fps_queue": fps_queue,
        "fps_jobs_pending": fps_jobs_pending,
        "da3_ready": _is_da3_set(),
        "da3_task_queue_len": da3_task_queue_len,
        "da3_task_queue_max": da3_task_queue_max,
        "da3_task_pending_videos": da3_task_pending_videos,
        "mega_cred_set": bool(isMegaCredSet),
        "mega_email": _MEGA_EMAIL if isMegaCredSet else "",
        "mega_upload_dir": _MEGA_UPLOAD_DIR if isMegaCredSet else "/",
    }
    ctx.update(kwargs)
    return ctx


MAX_URL_DOWNLOAD_BYTES = 25 * 1024 * 1024 * 1024  # 25GB
MAX_URL_REDIRECTS = 3
ALLOWED_URL_METHODS = {'wget', 'google-drive'}


def _hostname_looks_public(hostname: str) -> bool:
    try:
        infos = socket.getaddrinfo(hostname, None)
    except socket.gaierror:
        return False

    for info in infos:
        ip_str = info[4][0]
        try:
            ip = ipaddress.ip_address(ip_str)
        except ValueError:
            return False

        if (
            ip.is_private
            or ip.is_loopback
            or ip.is_link_local
            or ip.is_reserved
            or ip.is_multicast
            or ip.is_unspecified
        ):
            return False

    return True


def _validate_fetch_url(parsed: urllib.parse.ParseResult) -> None:
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("URL must start with http:// or https://")
    if parsed.username or parsed.password:
        raise ValueError("URLs with embedded credentials are not allowed.")
    if not parsed.hostname:
        raise ValueError("Invalid URL hostname.")

    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("Invalid port in URL.") from exc
    if port not in {None, 80, 443}:
        raise ValueError("Only ports 80 and 443 are allowed.")

    if not _hostname_looks_public(parsed.hostname):
        raise ValueError("Refusing to fetch from private/reserved hosts.")


class _SafeRedirect(urllib.request.HTTPRedirectHandler):
    def __init__(self, max_redirects: int):
        super().__init__()
        self._remaining = int(max_redirects)

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        self._remaining -= 1
        if self._remaining < 0:
            raise ValueError("Too many redirects.")

        if not urllib.parse.urlparse(newurl).scheme:
            newurl = urllib.parse.urljoin(req.full_url, newurl)

        _validate_fetch_url(urllib.parse.urlparse(newurl))
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def handle_url_upload(method: str, url: str, filename: str = "") -> Path:
    method = (method or "wget").strip()
    if method not in ALLOWED_URL_METHODS:
        raise ValueError(f"Unknown method: {method}")

    url = (url or "").strip()
    if not url:
        raise ValueError("Please enter a URL.")
    if len(url) > 2048:
        raise ValueError("URL is too long.")

    parsed = urllib.parse.urlparse(url)
    _validate_fetch_url(parsed)

    safe_filename = secure_filename((filename or "").strip())
    if safe_filename and len(safe_filename) > 150:
        raise ValueError("Filename is too long.")

    cleaned = parsed._replace(fragment="").geturl()

    if method == "google-drive":
        host = (parsed.hostname or "").lower()
        if not (host.endswith("drive.google.com") or host.endswith("docs.google.com")):
            raise ValueError("Expected a Google Drive URL (drive.google.com / docs.google.com).")
        if not safe_filename:
            raise ValueError("Filename is required for google-drive.")

        out_name = f"gdrive_{uuid.uuid4().hex[:12]}_{safe_filename}"
        out_path = UPLOAD_DIR / out_name

        try:
            import gdown  # type: ignore
        except Exception as exc:
            raise ValueError("gdown is not installed. Run the pip install cell.") from exc

        result = None
        try:
            result = gdown.download(cleaned, str(out_path), quiet=True, fuzzy=True)
        except TypeError:
            result = gdown.download(cleaned, str(out_path), quiet=True)

        if not out_path.exists():
            if result and Path(str(result)).exists():
                Path(str(result)).replace(out_path)
            else:
                raise ValueError("Google Drive download failed.")

        if out_path.stat().st_size > MAX_URL_DOWNLOAD_BYTES:
            out_path.unlink(missing_ok=True)
            raise ValueError(f"Downloaded file too large (>{MAX_URL_DOWNLOAD_BYTES} bytes).")

        enqueue_video(out_path, source="url:google-drive")
        return out_path

    # wget
    guessed_name = safe_filename or Path(urllib.parse.unquote(parsed.path or "")).name
    guessed_name = secure_filename(guessed_name) or "download.bin"
    out_name = f"wget_{uuid.uuid4().hex[:12]}_{guessed_name}"
    out_path = UPLOAD_DIR / out_name

    opener = urllib.request.build_opener(_SafeRedirect(MAX_URL_REDIRECTS))
    req = urllib.request.Request(cleaned, headers={"User-Agent": "Flask-ngrok-demo/1.0"})

    total = 0
    try:
        with opener.open(req, timeout=10) as resp, open(out_path, "wb") as f:
            while True:
                chunk = resp.read(64 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > MAX_URL_DOWNLOAD_BYTES:
                    raise ValueError(f"Remote file too large (>{MAX_URL_DOWNLOAD_BYTES} bytes).")
                f.write(chunk)
    except Exception:
        try:
            out_path.unlink(missing_ok=True)
        except Exception:
            pass
        raise

    enqueue_video(out_path, source="url:wget")
    return out_path


def handle_file_upload(file_storage) -> Path:
    if file_storage is None or not getattr(file_storage, "filename", ""):
        raise ValueError("Please choose a file.")

    original_name = secure_filename(file_storage.filename) or "upload.bin"
    out_name = f"file_{uuid.uuid4().hex[:12]}_{original_name}"
    out_path = UPLOAD_DIR / out_name
    file_storage.save(out_path)
    enqueue_video(out_path, source="file-upload")
    return out_path


def _format_size_label(size_bytes: int) -> str:
    try:
        size = float(size_bytes)
    except Exception:
        size = 0.0
    units = ["B", "KB", "MB", "GB", "TB"]
    unit_idx = 0
    while size >= 1024.0 and unit_idx < len(units) - 1:
        size /= 1024.0
        unit_idx += 1
    if unit_idx == 0:
        return f"{int(size)} {units[unit_idx]}"
    if size >= 100:
        return f"{size:.0f} {units[unit_idx]}"
    if size >= 10:
        return f"{size:.1f} {units[unit_idx]}"
    return f"{size:.2f} {units[unit_idx]}"


def _format_modified_label(ts: float) -> str:
    try:
        dt = datetime.fromtimestamp(float(ts))
    except Exception:
        return "-"

    now = datetime.now()
    delta = now - dt
    seconds = max(0, int(delta.total_seconds()))
    if seconds < 60:
        return "Just now"
    if seconds < 3600:
        minutes = seconds // 60
        return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
    if seconds < 86400:
        hours = seconds // 3600
        return f"{hours} hour{'s' if hours != 1 else ''} ago"
    if seconds < 172800:
        return "Yesterday"
    if now.year == dt.year:
        return dt.strftime("%b %d")
    return dt.strftime("%b %d, %Y")


def _file_type_details(item: Path, is_dir: bool = False) -> dict:
    if is_dir:
        return {
            "kind": "folder",
            "kind_label": "Folder",
            "type_label": "Folder",
            "icon_class": "icon-folder",
            "icon_text": "DIR",
            "is_image": False,
        }

    suffix = item.suffix.lower()
    if suffix in {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp"}:
        return {
            "kind": "image",
            "kind_label": "Image",
            "type_label": "Image",
            "icon_class": "icon-image",
            "icon_text": "IMG",
            "is_image": True,
        }
    if suffix in {".zip", ".tar", ".gz", ".bz2", ".xz", ".7z", ".rar", ".npz"}:
        return {
            "kind": "archive",
            "kind_label": "Archive",
            "type_label": "Archive",
            "icon_class": "icon-archive",
            "icon_text": "ZIP",
            "is_image": False,
        }
    if suffix in {".txt", ".json", ".csv", ".md", ".yaml", ".yml", ".log"}:
        return {
            "kind": "document",
            "kind_label": "Document",
            "type_label": "Text File",
            "icon_class": "icon-doc",
            "icon_text": "TXT",
            "is_image": False,
        }
    return {
        "kind": "file",
        "kind_label": "File",
        "type_label": f"{suffix[1:].upper()} File" if suffix else "File",
        "icon_class": "icon-generic",
        "icon_text": "FILE",
        "is_image": False,
    }


def _shutdown_everything():
    try:
        stop_da3_process_workers()
    except Exception:
        pass

    try:
        stop_frame_queue_worker()
    except Exception:
        pass

    try:
        stop_mega_upload_worker()
    except Exception:
        pass

    try:
        from pyngrok import ngrok as ngrok_api

        if "public_url" in globals():
            try:
                ngrok_api.disconnect(public_url)
            except Exception:
                pass
        ngrok_api.kill()
    except Exception:
        pass

    try:
        if "server" in globals():
            server.shutdown()
    except Exception:
        pass


def _shutdown_after_response(delay_s: float = 0.5):
    time.sleep(max(0.1, float(delay_s)))
    _shutdown_everything()
    os._exit(0)


@app.get("/")
def home():
    return render_template_string(_HOME_HTML, **_home_context())


@app.get("/gallery")
def gallery():
    path_str = request.args.get("path", "")
    
    # Resolve the path, ensuring it's within OUTPUT_DIR
    try:
        if path_str:
            # Prevent directory traversal
            rel_path = Path(path_str).as_posix()
            if rel_path.startswith("/") or rel_path.startswith(".."):
                rel_path = rel_path.lstrip("/").lstrip(".")
            target_path = OUTPUT_DIR / rel_path
        else:
            target_path = OUTPUT_DIR
        
        target_path = target_path.resolve()
        
        # Ensure the path is within OUTPUT_DIR
        try:
            target_path.relative_to(OUTPUT_DIR.resolve())
        except ValueError:
            return jsonify(ok=False, error="Access denied"), 403
    except Exception:
        target_path = OUTPUT_DIR
    
    if not target_path.exists():
        return render_template_string(_GALLERY_HTML, 
            breadcrumb=[{"name": "output", "url": "/gallery"}],
            folders=[], files=[], message="Output directory does not exist yet. DA3 outputs will appear here after processing.")
    
    # Build breadcrumb
    breadcrumb = [{"name": "output", "url": "/gallery"}]
    try:
        rel_parts = target_path.relative_to(OUTPUT_DIR.resolve()).parts
        for i, part in enumerate(rel_parts):
            breadcrumb.append({
                "name": part,
                "url": f"/gallery?path={'/'.join(rel_parts[:i+1])}"
            })
    except ValueError:
        pass
    
    # List contents
    folders = []
    files = []
    entries = []
    
    try:
        for item in sorted(target_path.iterdir()):
            if item.is_dir():
                rel_path = item.relative_to(OUTPUT_DIR.resolve()).as_posix()
                stats = item.stat()
                details = _file_type_details(item, is_dir=True)
                folder_info = {
                    "name": item.name,
                    "url": f"/gallery?path={rel_path}",
                    "size_label": "--",
                    "modified_label": _format_modified_label(stats.st_mtime),
                    **details,
                }
                folders.append(folder_info)
                entries.append(folder_info)
            elif item.is_file():
                rel_path = item.relative_to(OUTPUT_DIR.resolve()).as_posix()
                stats = item.stat()
                details = _file_type_details(item, is_dir=False)
                file_info = {
                    "name": item.name,
                    "url": f"/gallery-files/{rel_path}",
                    "size_mb": round(stats.st_size / (1024 * 1024), 2) if stats.st_size > 0 else 0,
                    "size_label": _format_size_label(stats.st_size),
                    "modified_label": _format_modified_label(stats.st_mtime),
                    **details,
                }
                files.append(file_info)
                entries.append(file_info)
    except PermissionError:
        pass

    if target_path == OUTPUT_DIR.resolve():
        parent_url = "/"
    else:
        try:
            rel_parent = target_path.parent.relative_to(OUTPUT_DIR.resolve()).as_posix()
            parent_url = f"/gallery?path={rel_parent}" if rel_parent else "/gallery"
        except ValueError:
            parent_url = "/gallery"

    refresh_url = f"/gallery?path={path_str}" if path_str else "/gallery"

    return render_template_string(_GALLERY_HTML,
        breadcrumb=breadcrumb,
        folders=folders,
        files=files,
        entries=entries,
        refresh_url=refresh_url,
        parent_url=parent_url)


@app.post("/settings/target_fps")
def set_target_fps_route():
    raw = request.form.get("target_fps", "")
    try:
        fps = set_target_fps(raw)
    except Exception as exc:
        return render_template_string(_HOME_HTML, **_home_context(message=f"Invalid Target FPS: {exc}")), 400

    web_print(f"Target FPS set to {fps}")
    return render_template_string(_HOME_HTML, **_home_context(message=f"Target FPS set to {fps}"))


@app.post("/settings/device_count")
def set_device_count_route():
    raw = request.form.get("device_count", "")
    try:
        count = set_device_count(raw)
    except Exception as exc:
        return render_template_string(_HOME_HTML, **_home_context(message=f"Invalid Device Count: {exc}")), 400

    web_print(f"Device count set to {count}")
    return render_template_string(_HOME_HTML, **_home_context(message=f"Device count set to {count}"))


@app.post("/settings/max_frame_amount_in_memory")
def set_max_frame_amount_in_memory_route():
    raw = request.form.get("max_frame_amount_in_memory", "")
    try:
        count = set_max_frame_amount_in_memory(raw)
    except Exception as exc:
        return render_template_string(_HOME_HTML, **_home_context(message=f"Invalid Max Frames in Memory: {exc}")), 400

    web_print(f"Max frames in memory set to {count}")
    return render_template_string(_HOME_HTML, **_home_context(message=f"Max frames in memory set to {count}"))


@app.post("/settings/batch_size")
def set_batch_size_route():
    raw = request.form.get("batch_size", "")
    try:
        count = set_batch_size(raw)
    except Exception as exc:
        return render_template_string(_HOME_HTML, **_home_context(message=f"Invalid Batch Size: {exc}")), 400

    web_print(f"Batch size set to {count}")
    return render_template_string(_HOME_HTML, **_home_context(message=f"Batch size set to {count}"))


@app.post("/settings/mega-credentials")
def set_mega_credentials_route():
    email = request.form.get("mega_email", "").strip()
    password = request.form.get("mega_password", "").strip()
    upload_dir = request.form.get("mega_upload_dir", "").strip()

    if not email or not password or not upload_dir:
        return render_template_string(
            _HOME_HTML,
            **_home_context(message="Email, Password, and Upload Folder are required."),
        ), 400

    if set_mega_credentials(email, password, upload_dir):
        return render_template_string(_HOME_HTML, **_home_context(message="MegaCMD connected successfully."))
    else:
        return render_template_string(
            _HOME_HTML,
            **_home_context(message="Failed to connect to MegaCMD. Check credentials."),
        ), 400



@app.get("/api/terminal")
def terminal_api():
    _drain_web_print_queue(max_items=2000)
    with WEB_TERMINAL_LOCK:
        text = "\n".join(WEB_TERMINAL_BUFFER)
    return jsonify(ok=True, text=text, lines=len(WEB_TERMINAL_BUFFER))


@app.get("/api/video_queue")
def video_queue_api():
    with VIDEO_QUEUE_LOCK:
        items = list(VIDEO_UPLOAD_QUEUE)
        max_len = VIDEO_UPLOAD_QUEUE.maxlen or 0
    return jsonify(ok=True, count=len(items), max=max_len, items=items)


@app.post("/api/video_queue/pop")
def video_queue_pop_api():
    item = video_queue_pop()
    if item:
        web_print(f"Dequeued video: {item.get('stored_name')}")
    return jsonify(ok=True, item=item)


@app.post("/api/video_queue/clear")
def video_queue_clear_api():
    with VIDEO_QUEUE_LOCK:
        VIDEO_UPLOAD_QUEUE.clear()
    web_print("Video queue cleared.")
    return jsonify(ok=True)


@app.get("/api/fps_adjusted_queue")
@app.get("/api/da3_queue")
def fps_adjusted_queue_api():
    with DA3_QUEUE_LOCK:
        items = list(DA3_QUEUE)
        max_len = DA3_QUEUE.maxlen or 0
    try:
        pending = DA3_JOB_QUEUE.qsize()
    except Exception:
        pending = 0
    return jsonify(ok=True, count=len(items), max=max_len, pending=pending, items=items)


@app.post("/api/fps_adjusted_queue/pop")
@app.post("/api/da3_queue/pop")
def fps_adjusted_queue_pop_api():
    item = da3_queue_pop()
    if item:
        web_print(f"Dequeued FPS adjusted video: {item.get('stored_name')}")
    return jsonify(ok=True, item=item)


@app.post("/api/fps_adjusted_queue/clear")
@app.post("/api/da3_queue/clear")
def fps_adjusted_queue_clear_api():
    with DA3_QUEUE_LOCK:
        DA3_QUEUE.clear()
    web_print("FPS adjusted queue cleared.")
    return jsonify(ok=True)


@app.get("/api/da3_task_queue")
def da3_task_queue_api():
    limit = request.args.get("limit", "200")
    items = da3_task_queue_peek(limit)
    with FRAME_QUEUE_LOCK:
        count = len(FRAME_QUEUE)
        max_len = FRAME_QUEUE.maxlen or 0
    with FRAME_VIDEO_STATE_LOCK:
        pending_videos = len(FRAME_VIDEO_ORDER)
    return jsonify(ok=True, da3_ready=_is_da3_set(), count=count, max=max_len, pending_videos=pending_videos, items=items)


@app.post("/api/da3_task_queue/pop")
def da3_task_queue_pop_api():
    item = da3_task_queue_pop()
    if item:
        frame_start = item.get('frame_no_start', item.get('frame_no'))
        frame_end = item.get('frame_no_end', item.get('frame_no'))
        web_print(f"Dequeued DA3 task: {item.get('video_file_url')} frames {frame_start}-{frame_end}")
    return jsonify(ok=True, item=item)


@app.post("/api/da3_task_queue/clear")
def da3_task_queue_clear_api():
    with FRAME_QUEUE_LOCK:
        FRAME_QUEUE.clear()
    with FRAME_VIDEO_STATE_LOCK:
        FRAME_VIDEO_STATE.clear()
        FRAME_VIDEO_ORDER.clear()
    FRAME_QUEUE_WAKE_EVENT.set()
    web_print("Frame queue cleared.")
    return jsonify(ok=True)


@app.post("/upload/url")
def upload_url():
    method = request.form.get("method", "wget")
    url = request.form.get("url", "")
    filename = request.form.get("filename", "")

    try:
        saved_path = handle_url_upload(method, url, filename)
    except Exception as exc:
        return render_template_string(
            _HOME_HTML,
            **_home_context(
                message=f"URL fetch failed: {exc}",
                method=method,
                url_value=url,
                filename_value=filename,
            ),
        ), 400

    return render_template_string(
        _HOME_HTML,
        **_home_context(
            message=f"URL downloaded via {method}.",
            uploaded_url=url_for("get_upload", filename=saved_path.name),
            method=method,
            url_value=url,
            filename_value=filename,
        ),
    )


@app.post("/upload/file")
def upload_file():
    try:
        saved_path = handle_file_upload(request.files.get("file"))
    except Exception as exc:
        return render_template_string(_HOME_HTML, **_home_context(message=f"File upload failed: {exc}")), 400

    return render_template_string(
        _HOME_HTML,
        **_home_context(
            message="File uploaded.",
            uploaded_url=url_for("get_upload", filename=saved_path.name),
        ),
    )


@app.get("/uploads/<path:filename>")
def get_upload(filename: str):
    return send_from_directory(str(UPLOAD_DIR), filename)


@app.get("/frames/<path:filename>")
def get_frame(filename: str):
    return send_from_directory(str(FRAMES_DIR), filename)


@app.get("/gallery-files/<path:filename>")
def get_gallery_file(filename: str):
    # Prevent directory traversal
    safe_path = Path(filename).as_posix()
    if safe_path.startswith("/") or safe_path.startswith(".."):
        return jsonify(ok=False, error="Invalid path"), 400
    
    file_path = OUTPUT_DIR / filename
    try:
        file_path = file_path.resolve()
        file_path.relative_to(OUTPUT_DIR.resolve())
    except (ValueError, Exception):
        return jsonify(ok=False, error="File not found"), 404
    
    if not file_path.exists():
        return jsonify(ok=False, error="File not found"), 404
    
    return send_from_directory(str(OUTPUT_DIR), filename)


@app.errorhandler(413)
def too_large(_exc):
    return render_template_string(_HOME_HTML, **_home_context(message="Upload too large (max 25GB).")), 413


@app.get("/api/ping")
def ping():
    return jsonify(ok=True, message="pong", ts=time.time())


@app.get("/api/echo")
def echo():
    return jsonify(ok=True, msg=request.args.get("msg", ""))


@app.post("/shutdown")
def shutdown_route():
    web_print("Shutdown requested from web UI.")
    Thread(target=_shutdown_after_response, args=(0.5,), daemon=True, name="Shutdown-Worker").start()
    return """
    <!doctype html>
    <html lang="en">
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <title>Shutting Down</title>
      <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bulma@1.0.2/css/bulma.min.css">
    </head>
    <body>
      <section class="section">
        <div class="container" style="max-width: 640px;">
          <article class="message is-danger">
            <div class="message-header">
              <p>Shutdown In Progress</p>
            </div>
            <div class="message-body">
              Stopping workers, cleaning up resources, and exiting the process.
            </div>
          </article>
        </div>
      </section>
    </body>
    </html>
    """


HOST = "127.0.0.1"
PORT = _pick_port(HOST, preferred=5000)

server = _ServerThread(app, HOST, PORT)
server.start()

web_print(f"Flask running locally: http://{HOST}:{PORT}")
print(f"Flask running locally: http://{HOST}:{PORT}")
