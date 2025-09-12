# s3_to_mysql.py
import os
from datetime import datetime
from urllib.parse import quote

import boto3
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# -----------------------------
# Load env (optional if Render already injects env)
# -----------------------------
load_dotenv()

# -----------------------------
# S3 settings
# -----------------------------
S3_BUCKET = "demo2109bhargav"
S3_REGION = "us-east-1"   # your friend confirmed this

# If you keep all videos at a prefix inside the bucket, set it here (else leave "")
S3_PREFIX = ""            # e.g. "videos/"

# -----------------------------
# DB settings (from your screenshot)
# -----------------------------
DB_HOST = os.getenv("DB_HOST", "")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_NAME", "")
DB_USER = os.getenv("DB_USER", "")
DB_PASS = os.getenv("DB_PASS", "")

if not all([DB_HOST, DB_NAME, DB_USER, DB_PASS]):
    raise RuntimeError("Missing DB env vars: DB_HOST/DB_NAME/DB_USER/DB_PASS")

db_url = URL.create(
    "mysql+pymysql",
    username=DB_USER,
    password=DB_PASS,
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
)

engine = create_engine(
    db_url,
    pool_pre_ping=True,
    pool_recycle=3600,
    pool_timeout=30,
    pool_size=5,
    max_overflow=10,
)

# -----------------------------
# Ensure table exists (idempotent)
# -----------------------------
DDL = """
CREATE TABLE IF NOT EXISTS videos (
  id INT AUTO_INCREMENT PRIMARY KEY,
  s3_key VARCHAR(512) NOT NULL,
  filename VARCHAR(255) NOT NULL,
  url TEXT NOT NULL,
  size_bytes BIGINT NULL,
  etag VARCHAR(64) NULL,
  last_modified DATETIME NULL,
  UNIQUE KEY uk_s3_key (s3_key)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
"""

# -----------------------------
# S3 client (credentials come from env / instance role)
# -----------------------------
s3 = boto3.client("s3", region_name=S3_REGION)

def list_mp4_objects(bucket: str, prefix: str = ""):
    """Yield all .mp4 objects in a bucket (optionally under a prefix)."""
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith(".mp4"):
                yield {
                    "Key": key,
                    "Size": obj.get("Size"),
                    "ETag": obj.get("ETag", "").strip('"'),
                    "LastModified": obj.get("LastModified"),
                }

def object_url(bucket: str, region: str, key: str) -> str:
    """
    Build a public S3 URL (virtual-hosted–style).
    If keys contain spaces/unicode, we URL-encode path.
    """
    return f"https://{bucket}.s3.{region}.amazonaws.com/{quote(key)}"

def upsert_video(conn, row):
    """
    MySQL upsert on s3_key.
    """
    sql = text("""
        INSERT INTO videos (s3_key, filename, url, size_bytes, etag, last_modified)
        VALUES (:s3_key, :filename, :url, :size_bytes, :etag, :last_modified)
        ON DUPLICATE KEY UPDATE
          filename = VALUES(filename),
          url = VALUES(url),
          size_bytes = VALUES(size_bytes),
          etag = VALUES(etag),
          last_modified = VALUES(last_modified)
    """)
    conn.execute(sql, row)

def main():
    # Ensure table exists
    with engine.begin() as conn:
        conn.execute(text(DDL))

    total, inserted = 0, 0
    with engine.begin() as conn:
        for obj in list_mp4_objects(S3_BUCKET, S3_PREFIX):
            total += 1
            key = obj["Key"]
            base_name = key.split("/")[-1]      # filename at end of key
            url = object_url(S3_BUCKET, S3_REGION, key)

            row = {
                "s3_key": key,
                "filename": base_name,
                "url": url,
                "size_bytes": obj.get("Size"),
                "etag": obj.get("ETag"),
                "last_modified": obj.get("LastModified").replace(tzinfo=None) if obj.get("LastModified") else None,
            }
            upsert_video(conn, row)
            inserted += 1

    print(f"✅ Synced {inserted}/{total} .mp4 objects from s3://{S3_BUCKET}/{S3_PREFIX} into MySQL {DB_NAME}.videos")

if __name__ == "__main__":
    main()