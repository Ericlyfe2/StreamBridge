from datetime import datetime

from pydantic import BaseModel


class JobResponse(BaseModel):
    id: int
    source_type: str
    infohash: str
    status: str
    progress: int
    message: str
    created_at: datetime
    updated_at: datetime | None = None

    class Config:
        from_attributes = True


class JobFileResponse(BaseModel):
    id: int
    filename: str
    size: int
    signed_url: str
