import asyncio
import hashlib
import os
import re
import shutil
import sys
import threading
import time
from pathlib import Path

_VENDOR_DLLS = Path(__file__).resolve().parent.parent / "vendor"
if sys.platform == "win32" and _VENDOR_DLLS.is_dir():
    os.add_dll_directory(str(_VENDOR_DLLS))

import libtorrent as lt
import yt_dlp
from sqlalchemy.orm import Session

from .models import FileRecord, Job


DATA_DIR = Path("data")
DOWNLOAD_DIR = DATA_DIR / "downloads"
TORRENT_DIR = DATA_DIR / "torrents"
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
TORRENT_DIR.mkdir(parents=True, exist_ok=True)

_ORPHAN_STATES = frozenset({"queued", "processing", "finalizing", "paused"})


def _fail_orphaned_jobs(db_factory) -> None:
    db = db_factory()
    try:
        orphans = (
            db.query(Job)
            .filter(Job.status.in_(_ORPHAN_STATES))
            .all()
        )
        for job in orphans:
            job.status = "failed"
            job.progress = 0
            job.message = "Server restarted — job orphaned"
        db.commit()
    finally:
        db.close()


MAGNET_INFOHASH_RE = re.compile(r"xt=urn:btih:([a-fA-F0-9]{40}|[a-zA-Z2-7]{32})")


_STATE_LABELS = {
    lt.torrent_status.checking_files: "checking",
    lt.torrent_status.downloading_metadata: "fetching metadata",
    lt.torrent_status.downloading: "downloading",
    lt.torrent_status.finished: "finished",
    lt.torrent_status.seeding: "seeding",
    lt.torrent_status.allocating: "allocating",
    lt.torrent_status.checking_resume_data: "checking resume",
}


_session: lt.session | None = None
_session_lock = threading.Lock()
_job_stats: dict[int, dict] = {}
_stats_lock = threading.Lock()
_job_handles: dict[int, "lt.torrent_handle"] = {}
_handles_lock = threading.Lock()
_cancel_flags: set[int] = set()
_paused_flags: set[int] = set()


def get_session() -> lt.session:
    global _session
    with _session_lock:
        if _session is None:
            ses = lt.session({
                "listen_interfaces": "0.0.0.0:6881,[::]:6881",
                "enable_dht": True,
                "enable_lsd": True,
                "enable_upnp": True,
                "enable_natpmp": True,
                "alert_mask": lt.alert.category_t.error_notification,
            })
            ses.add_dht_router("router.bittorrent.com", 6881)
            ses.add_dht_router("router.utorrent.com", 6881)
            ses.add_dht_router("dht.transmissionbt.com", 6881)
            ses.add_dht_router("dht.aelitis.com", 6881)
            _session = ses
    return _session


def extract_infohash_from_magnet(magnet: str) -> str | None:
    match = MAGNET_INFOHASH_RE.search(magnet)
    if not match:
        return None
    raw = match.group(1)
    if len(raw) == 40:
        return raw.lower()
    return raw.upper()


def _bdecode(data: bytes, pos: int):
    head = data[pos:pos + 1]
    if head == b"i":
        end = data.index(b"e", pos)
        return int(data[pos + 1:end]), end + 1
    if head == b"l":
        result = []
        pos += 1
        while data[pos:pos + 1] != b"e":
            value, pos = _bdecode(data, pos)
            result.append(value)
        return result, pos + 1
    if head == b"d":
        result = {}
        pos += 1
        while data[pos:pos + 1] != b"e":
            key, pos = _bdecode(data, pos)
            value, pos = _bdecode(data, pos)
            result[key] = value
        return result, pos + 1
    colon = data.index(b":", pos)
    length = int(data[pos:colon])
    start = colon + 1
    end = start + length
    return data[start:end], end


def _extract_info_dict_bytes(data: bytes) -> bytes | None:
    if not data.startswith(b"d"):
        return None
    try:
        pos = 1
        while pos < len(data) and data[pos:pos + 1] != b"e":
            key, pos = _bdecode(data, pos)
            value_start = pos
            _, pos = _bdecode(data, pos)
            if key == b"info":
                return data[value_start:pos]
    except (ValueError, IndexError):
        return None
    return None


def infer_infohash_from_torrent_bytes(content: bytes) -> str:
    info_bytes = _extract_info_dict_bytes(content)
    if info_bytes is None:
        return hashlib.sha1(content).hexdigest()
    return hashlib.sha1(info_bytes).hexdigest()


def get_job_stats(job_id: int) -> dict:
    with _stats_lock:
        return dict(_job_stats.get(job_id, {}))


def _set_stats(job_id: int, **kwargs) -> None:
    with _stats_lock:
        _job_stats.setdefault(job_id, {}).update(kwargs)


def _clear_stats(job_id: int) -> None:
    with _stats_lock:
        _job_stats.pop(job_id, None)


def pause_job(job_id: int) -> bool:
    with _handles_lock:
        handle = _job_handles.get(job_id)
    if handle is None:
        return False
    try:
        handle.unset_flags(lt.torrent_flags.auto_managed)
        handle.pause()
    except Exception:
        return False
    _paused_flags.add(job_id)
    return True


def resume_job(job_id: int) -> bool:
    with _handles_lock:
        handle = _job_handles.get(job_id)
    if handle is None:
        return False
    try:
        handle.set_flags(lt.torrent_flags.auto_managed)
        handle.resume()
    except Exception:
        return False
    _paused_flags.discard(job_id)
    return True


def cancel_job(job_id: int) -> bool:
    _cancel_flags.add(job_id)
    return True


def _add_handle(job: Job, save_path: Path):
    session = get_session()
    if job.source_type == "magnet":
        params = lt.parse_magnet_uri(job.source_value)
        params.save_path = str(save_path)
    else:
        params = lt.add_torrent_params()
        params.ti = lt.torrent_info(job.source_value)
        params.save_path = str(save_path)
    return session.add_torrent(params)


def _record_files(db: Session, job: Job, handle, save_path: Path) -> None:
    ti = handle.torrent_file()
    if ti is None:
        return
    files = ti.files()
    for i in range(files.num_files()):
        rel_path = files.file_path(i)
        full_path = save_path / rel_path
        if not full_path.exists():
            continue
        rec = FileRecord(
            job_id=job.id,
            filename=os.path.basename(rel_path),
            stored_path=str(full_path),
            size=full_path.stat().st_size,
        )
        db.add(rec)


def _run_torrent_blocking(job_id: int, db_factory) -> None:
    db = db_factory()
    handle = None
    try:
        job = db.get(Job, job_id)
        if not job:
            return

        if job_id in _cancel_flags:
            job.status = "canceled"
            job.message = "Canceled"
            db.commit()
            return

        save_path = DOWNLOAD_DIR / f"job-{job_id}"
        save_path.mkdir(parents=True, exist_ok=True)

        try:
            handle = _add_handle(job, save_path)
        except Exception as exc:
            job.status = "failed"
            job.message = f"Failed to add torrent: {exc}"
            db.commit()
            return

        with _handles_lock:
            _job_handles[job_id] = handle

        last_payload = None
        metadata_start = time.time()
        while True:
            if job_id in _cancel_flags:
                try:
                    get_session().remove_torrent(handle, lt.session.delete_files)
                except Exception:
                    pass
                job = db.get(Job, job_id)
                if job:
                    job.status = "canceled"
                    job.message = "Canceled by user"
                    db.commit()
                return

            status = handle.status()
            state_label = _STATE_LABELS.get(status.state, str(status.state))
            progress = int(status.progress * 100)
            rate = int(status.download_rate)
            peers = int(status.num_peers)
            total = int(status.total_wanted)
            done = int(status.total_wanted_done)
            paused = job_id in _paused_flags

            metadata_ready = status.has_metadata if hasattr(status, "has_metadata") else handle.torrent_file() is not None

            if state_label == "fetching metadata" and not metadata_ready:
                if time.time() - metadata_start > 120:
                    job = db.get(Job, job_id)
                    if job:
                        job.status = "failed"
                        job.progress = 0
                        job.message = "Failed to fetch metadata (timeout after 120s)"
                        db.commit()
                    try:
                        get_session().remove_torrent(handle)
                    except Exception:
                        pass
                    return

            _set_stats(
                job_id,
                download_rate=rate,
                upload_rate=int(status.upload_rate),
                peers=peers,
                seeds=int(status.num_seeds),
                downloaded=done,
                total_size=total,
                state="paused" if paused else state_label,
                paused=paused,
            )

            finished = state_label in ("finished", "seeding") or status.is_finished

            if finished and metadata_ready and not paused:
                job = db.get(Job, job_id)
                if job:
                    job.status = "completed"
                    job.progress = 100
                    job.message = "Completed"
                    _record_files(db, job, handle, save_path)
                    db.commit()
                try:
                    get_session().remove_torrent(handle)
                except Exception:
                    pass
                return

            if paused:
                new_status = "paused"
                msg = f"Paused · {progress}%"
            elif state_label == "fetching metadata":
                new_status = "processing"
                msg = f"Fetching metadata · {peers} peers"
            elif state_label == "downloading":
                new_status = "processing"
                msg = f"{_human_rate(rate)} · {peers} peers · {_human_size(done)}/{_human_size(total)}"
            elif state_label == "checking":
                new_status = "processing"
                msg = f"Checking files · {progress}%"
            else:
                new_status = "processing" if progress < 100 else "finalizing"
                msg = f"{state_label.capitalize()} · {peers} peers"

            payload = (new_status, progress, msg)
            if payload != last_payload:
                job = db.get(Job, job_id)
                if job:
                    job.status = new_status
                    job.progress = progress
                    job.message = msg
                    db.commit()
                last_payload = payload

            db.expire_all()
            time.sleep(1)
    finally:
        with _handles_lock:
            _job_handles.pop(job_id, None)
        _cancel_flags.discard(job_id)
        _paused_flags.discard(job_id)
        db.close()
        _clear_stats(job_id)


def _human_size(n: int) -> str:
    n = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def _human_rate(n: int) -> str:
    return f"{_human_size(n)}/s"


_HAS_FFMPEG = bool(shutil.which("ffmpeg"))


class _Canceled(Exception):
    pass


def _yt_progress_hook(job_id: int, db_factory):
    last_db_write = [0.0]

    def hook(d: dict) -> None:
        if job_id in _cancel_flags:
            raise _Canceled()
        if d.get("status") == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
            done = d.get("downloaded_bytes", 0) or 0
            speed = int(d.get("speed") or 0)
            progress = int(done * 100 / total) if total else 0
            _set_stats(
                job_id,
                downloaded=done,
                total_size=total,
                download_rate=speed,
                upload_rate=0,
                peers=0,
                seeds=0,
                state="downloading",
                paused=False,
            )
            now = time.time()
            if now - last_db_write[0] < 0.5:
                return
            last_db_write[0] = now
            db = db_factory()
            try:
                job = db.get(Job, job_id)
                if job:
                    job.status = "processing"
                    job.progress = progress
                    if total:
                        job.message = (
                            f"{_human_rate(speed)} · "
                            f"{_human_size(done)}/{_human_size(total)}"
                        )
                    else:
                        job.message = f"{_human_rate(speed)} · {_human_size(done)}"
                    db.commit()
            finally:
                db.close()
        elif d.get("status") == "finished":
            db = db_factory()
            try:
                job = db.get(Job, job_id)
                if job:
                    job.status = "finalizing"
                    job.progress = 100
                    job.message = "Post-processing"
                    db.commit()
            finally:
                db.close()

    return hook


def _run_url_blocking(job_id: int, db_factory) -> None:
    db = db_factory()
    try:
        job = db.get(Job, job_id)
        if not job:
            return
        url = job.source_value
        if job_id in _cancel_flags:
            job.status = "canceled"
            job.message = "Canceled"
            db.commit()
            return
    finally:
        db.close()

    save_path = DOWNLOAD_DIR / f"job-{job_id}"
    save_path.mkdir(parents=True, exist_ok=True)

    fmt = "bestvideo*+bestaudio/best" if _HAS_FFMPEG else "best"
    ydl_opts = {
        "outtmpl": str(save_path / "%(title).180s.%(ext)s"),
        "format": fmt,
        "noplaylist": True,
        "restrictfilenames": True,
        "quiet": True,
        "no_warnings": True,
        "progress_hooks": [_yt_progress_hook(job_id, db_factory)],
        "merge_output_format": "mp4" if _HAS_FFMPEG else None,
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
    except _Canceled:
        db = db_factory()
        try:
            job = db.get(Job, job_id)
            if job:
                job.status = "canceled"
                job.message = "Canceled by user"
                db.commit()
            shutil.rmtree(save_path, ignore_errors=True)
        finally:
            db.close()
        return
    except Exception as exc:
        db = db_factory()
        try:
            job = db.get(Job, job_id)
            if job:
                job.status = "failed"
                job.message = f"Failed: {exc}".splitlines()[0][:240]
                db.commit()
        finally:
            db.close()
        return
    finally:
        with _handles_lock:
            _job_handles.pop(job_id, None)
        _cancel_flags.discard(job_id)
        _paused_flags.discard(job_id)
        _clear_stats(job_id)

    db = db_factory()
    try:
        job = db.get(Job, job_id)
        if not job:
            return
        for path in sorted(save_path.iterdir()):
            if path.is_file() and not path.name.endswith(".part"):
                rec = FileRecord(
                    job_id=job.id,
                    filename=path.name,
                    stored_path=str(path),
                    size=path.stat().st_size,
                )
                db.add(rec)
        job.status = "completed"
        job.progress = 100
        job.message = "Completed"
        db.commit()
    finally:
        db.close()


def _dispatch_blocking(job_id: int, db_factory) -> None:
    db = db_factory()
    try:
        job = db.get(Job, job_id)
        if not job:
            return
        source_type = job.source_type
    finally:
        db.close()
    if source_type == "url":
        _run_url_blocking(job_id, db_factory)
    else:
        _run_torrent_blocking(job_id, db_factory)


async def run_job(job_id: int, db_factory) -> None:
    await asyncio.to_thread(_dispatch_blocking, job_id, db_factory)
