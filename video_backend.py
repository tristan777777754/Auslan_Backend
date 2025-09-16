# video_backend.py
from fastapi import APIRouter
from sqlalchemy import create_engine, text
import os
import boto3

router = APIRouter(prefix="/videos", tags=["videos"])

# ---------- DB Settings ----------
DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

# ---------- AWS S3 Settings ----------
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET", "demo2109bhargav")

s3 = boto3.client("s3", region_name=AWS_REGION)

# ---------- API: Get video list ----------
@router.get("/")
def get_videos():
  
    with engine.connect() as conn:
        result = conn.execute(text("SELECT id, filename, s3_key FROM videos"))
        videos = [dict(row._mapping) for row in result]
    return videos


# ---------- API: Get video presigned URL ----------
@router.get("/{video_key}")
def get_video_url(video_key: str):
   
    url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": S3_BUCKET, "Key": video_key},
        ExpiresIn=3600  # 1小時
    )
    return {"url": url}