from datetime import datetime

from pydantic import BaseModel


class JobCreate(BaseModel):
    magnet: str


class JobResponse(BaseModel):
    id: int
    source_type: str
    infohash: str
    status: str
    progress: int
    message: str
    created_at: datetime

    class Config:
        from_attributes = True


class JobFileResponse(BaseModel):
    id: int
    filename: str
    size: int
    signed_url: str
