# video_router.py
import os
from fastapi import APIRouter, HTTPException
from sqlalchemy import create_engine, text

router = APIRouter(prefix="/videos", tags=["videos"])


DB_HOST = os.getenv("DB_HOST", "")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_NAME", "")
DB_USER = os.getenv("DB_USER", "")
DB_PASS = os.getenv("DB_PASS", "")

engine = create_engine(
    f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# get video
@router.get("/")
def list_videos():
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT id, filename, url FROM videos ORDER BY id")).fetchall()
        return [
            {"id": r.id, "filename": r.filename, "url": r.url}
            for r in rows
        ]

# based on ID 
@router.get("/{video_id}")
def get_video(video_id: int):
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT id, filename, url FROM videos WHERE id = :id"),
            {"id": video_id},
        ).fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Video not found")

        return {"id": row.id, "filename": row.filename, "url": row.url}