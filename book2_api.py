# book1_backend.py
from fastapi import APIRouter
from sqlalchemy import create_engine, text
import os
import boto3
from botocore.client import Config

router = APIRouter(prefix="/book2", tags=["book1 videos"])

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

s3 = boto3.client("s3", region_name=AWS_REGION, config=Config(signature_version="s3v4"))

# ---------- API: Get all book1 videos with pre-signed URL ----------
@router.get("/")
def get_book1_videos():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT id, filename, s3_key FROM book_2_video"))
        videos = []
        for row in result:
            video = dict(row._mapping)

            # Generate pre-signed URL (7 days validity)
            url = s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": S3_BUCKET, "Key": video["s3_key"]},
                ExpiresIn=604800   # 7 days
            )
            video["url"] = url
            videos.append(video)
    return videos