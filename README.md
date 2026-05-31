# StreamBridge MVP

This is a runnable MVP implementation based on your StreamBridge proposal.

## What is implemented

- FastAPI web service with:
  - Magnet link submission endpoint
  - `.torrent` upload endpoint
  - SQLite-backed `jobs` and `files` tables
  - Server-Sent Events (SSE) for live job progress
  - Time-limited HMAC-signed download URLs
- Simple browser UI at `/` for submitting jobs and monitoring status
- Local artifact generation to simulate completed conversion output

## Current scope

This build provides architecture scaffolding and a working end-to-end flow, but does **not** yet include:

- Real BitTorrent swarm fetching (`libtorrent`)
- Redis queue / Celery workers
- S3 object storage + CDN integration
- Malware scanning / takedown tooling
- Authentication, quotas, and full legal compliance workflow

## Run locally (Python)

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python run.py
```

Open `http://localhost:8000`.

## Run with Docker Compose

```bash
docker compose up
```

Open `http://localhost:8000`.

## Recommended next steps

1. Replace simulation logic in `app/services.py` with `python-libtorrent` worker integration.
2. Move background processing into a dedicated worker service with Redis/Celery.
3. Add object storage and signed CDN delivery for real file hosting.
4. Add safety service (AV scan + infohash blocklist + audit events).
5. Add auth and account-level quota controls.
