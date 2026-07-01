import asyncio
import hashlib
import json
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session

from .database import Base, SessionLocal, engine, get_db
from .models import FileRecord, Job
from .schemas import JobFileResponse, JobResponse
from .security import sign_download, verify_download_token
from .services import (
    TORRENT_DIR,
    _fail_orphaned_jobs,
    cancel_job,
    extract_infohash_from_magnet,
    get_job_stats,
    infer_infohash_from_torrent_bytes,
    pause_job,
    resume_job,
    run_job,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    _fail_orphaned_jobs(SessionLocal)
    yield


app = FastAPI(title="StreamBridge", version="0.2.0", lifespan=lifespan)

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/", response_class=HTMLResponse)
def home():
    return (STATIC_DIR / "index.html").read_text(encoding="utf-8")


@app.post("/api/jobs/magnet", response_model=JobResponse)
async def create_magnet_job(magnet: str = Form(...), db: Session = Depends(get_db)):
    infohash = extract_infohash_from_magnet(magnet)
    if not infohash:
        raise HTTPException(status_code=400, detail="Invalid magnet link format.")

    job = Job(
        source_type="magnet",
        source_value=magnet,
        infohash=infohash,
        status="queued",
        progress=0,
        message="Queued",
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    asyncio.create_task(run_job(job.id, SessionLocal))
    return job


@app.post("/api/jobs/torrent", response_model=JobResponse)
async def create_torrent_job(
    torrent_file: UploadFile = File(...), db: Session = Depends(get_db)
):
    payload = await torrent_file.read()
    if not payload:
        raise HTTPException(status_code=400, detail="Empty torrent file.")

    infohash = infer_infohash_from_torrent_bytes(payload)
    torrent_path = TORRENT_DIR / f"{infohash}.torrent"
    torrent_path.write_bytes(payload)

    job = Job(
        source_type="torrent_file",
        source_value=str(torrent_path),
        infohash=infohash,
        status="queued",
        progress=0,
        message="Queued",
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    asyncio.create_task(run_job(job.id, SessionLocal))
    return job


@app.post("/api/jobs/url", response_model=JobResponse)
async def create_url_job(url: str = Form(...), db: Session = Depends(get_db)):
    url = url.strip()
    if not url.startswith(("http://", "https://")):
        raise HTTPException(status_code=400, detail="URL must start with http:// or https://")

    job = Job(
        source_type="url",
        source_value=url,
        infohash=hashlib.sha1(url.encode("utf-8")).hexdigest(),
        status="queued",
        progress=0,
        message="Queued",
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    asyncio.create_task(run_job(job.id, SessionLocal))
    return job


@app.get("/api/jobs", response_model=list[JobResponse])
def list_jobs(db: Session = Depends(get_db)):
    return (
        db.query(Job).order_by(Job.created_at.desc()).limit(50).all()
    )


@app.get("/api/jobs/{job_id}", response_model=JobResponse)
def get_job(job_id: int, db: Session = Depends(get_db)):
    job = db.get(Job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found.")
    return job


@app.get("/api/jobs/{job_id}/files", response_model=list[JobFileResponse])
def get_job_files(job_id: int, db: Session = Depends(get_db)):
    files = db.query(FileRecord).filter(FileRecord.job_id == job_id).all()
    response: list[JobFileResponse] = []
    for file_rec in files:
        token = sign_download(file_rec.id)
        response.append(
            JobFileResponse(
                id=file_rec.id,
                filename=file_rec.filename,
                size=file_rec.size,
                signed_url=f"/api/download/{token}",
            )
        )
    return response


_TERMINAL_STATES = {"completed", "failed", "canceled"}


@app.post("/api/jobs/{job_id}/pause")
def pause_job_endpoint(job_id: int, db: Session = Depends(get_db)):
    job = db.get(Job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found.")
    if job.source_type == "url":
        raise HTTPException(status_code=400, detail="URL downloads cannot be paused.")
    if job.status in _TERMINAL_STATES:
        raise HTTPException(status_code=400, detail=f"Cannot pause job in state '{job.status}'.")
    if not pause_job(job_id):
        raise HTTPException(status_code=409, detail="Job has no active handle yet.")
    return {"ok": True}


@app.post("/api/jobs/{job_id}/resume")
def resume_job_endpoint(job_id: int, db: Session = Depends(get_db)):
    job = db.get(Job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found.")
    if job.source_type == "url":
        raise HTTPException(status_code=400, detail="URL downloads cannot be resumed.")
    if job.status in _TERMINAL_STATES:
        raise HTTPException(status_code=400, detail=f"Cannot resume job in state '{job.status}'.")
    if not resume_job(job_id):
        raise HTTPException(status_code=409, detail="Job has no active handle.")
    return {"ok": True}


@app.post("/api/jobs/{job_id}/cancel")
def cancel_job_endpoint(job_id: int, db: Session = Depends(get_db)):
    job = db.get(Job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found.")
    if job.status in _TERMINAL_STATES:
        raise HTTPException(status_code=400, detail=f"Job already in terminal state '{job.status}'.")
    cancel_job(job_id)
    return {"ok": True}


@app.get("/api/jobs/{job_id}/events")
async def stream_job_progress(job_id: int):
    async def event_stream():
        last_payload = ""
        db = SessionLocal()
        try:
            while True:
                job = db.get(Job, job_id)
                if not job:
                    yield "event: error\ndata: Job not found\n\n"
                    return
                stats = get_job_stats(job_id)
                payload = json.dumps({
                    "status": job.status,
                    "progress": job.progress,
                    "message": job.message,
                    "download_rate": stats.get("download_rate", 0),
                    "upload_rate": stats.get("upload_rate", 0),
                    "peers": stats.get("peers", 0),
                    "seeds": stats.get("seeds", 0),
                    "downloaded": stats.get("downloaded", 0),
                    "total_size": stats.get("total_size", 0),
                    "state": stats.get("state", job.status),
                })
                if payload != last_payload:
                    yield f"event: progress\ndata: {payload}\n\n"
                    last_payload = payload
                if job.status in _TERMINAL_STATES:
                    return
                db.expire_all()
                await asyncio.sleep(1)
        finally:
            db.close()

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.get("/api/download/{token}")
def download_file(token: str, db: Session = Depends(get_db)):
    file_id = verify_download_token(token)
    if file_id is None:
        raise HTTPException(status_code=401, detail="Invalid or expired download token.")
    file_rec = db.get(FileRecord, file_id)
    if not file_rec:
        raise HTTPException(status_code=404, detail="File not found.")
    return FileResponse(path=file_rec.stored_path, filename=file_rec.filename)
