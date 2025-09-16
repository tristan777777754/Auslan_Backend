# s3_ingest.py
import os
import re
from datetime import datetime
from typing import Dict, Iterable, Tuple

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# ---------- Settings from environment ----------
DB_HOST = os.getenv("DB_HOST", "")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_NAME", "")
DB_USER = os.getenv("DB_USER", "")
DB_PASS = os.getenv("DB_PASS", "")

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET", "demo2109bhargav")
S3_PREFIX = os.getenv("S3_PREFIX", "").strip()  # optional, can be empty

# ---------- DB engine ----------
def get_db_engine():
    db_url = URL.create(
        "mysql+pymysql",
        username=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
    )
    return create_engine(
        db_url,
        pool_pre_ping=True,
        pool_recycle=3600,
        pool_timeout=30,
        pool_size=5,
        max_overflow=10,
    )

# ---------- Ensure table exists ----------
CREATE_SQL = """
CREATE TABLE IF NOT EXISTS videos (
  id INT NULL,
  name VARCHAR(255) NULL,
  s3_bucket VARCHAR(128) NOT NULL,
  s3_key VARCHAR(1024) NOT NULL,
  s3_url VARCHAR(1024) NOT NULL,
  size BIGINT NULL,
  etag VARCHAR(128) NULL,
  last_modified DATETIME NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_s3_key (s3_key),
  INDEX idx_id (id),
  INDEX idx_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

UPSERT_SQL = """
INSERT INTO videos (s3_key, filename, url, size_bytes, etag, last_modified)
VALUES (:key, :name, :url, :size, :etag, :last_modified)
ON DUPLICATE KEY UPDATE
  filename     = VALUES(filename),
  url          = VALUES(url),
  size_bytes   = VALUES(size_bytes),
  etag         = VALUES(etag),
  last_modified= VALUES(last_modified);
"""

def ensure_table(engine):
    with engine.begin() as conn:
        conn.execute(text(CREATE_SQL))

# ---------- S3 helpers ----------
def s3_client():
    return boto3.client("s3", region_name=AWS_REGION)

def list_mp4_objects(bucket: str, prefix: str = "") -> Iterable[Dict]:
    client = s3_client()
    paginator = client.get_paginator("list_objects_v2")
    kwargs = {"Bucket": bucket}
    if prefix:
        kwargs["Prefix"] = prefix

    for page in paginator.paginate(**kwargs):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith(".mp4"):
                yield {
                    "Key": key,
                    "Size": obj.get("Size"),
                    "ETag": obj.get("ETag", "").strip('"'),
                    "LastModified": obj.get("LastModified"),
                }

def public_url(bucket: str, key: str) -> str:
    # Works for public/allow-listed buckets or when used only for display
    # (not a signed URL). For us-east-1 this format is also fine.
    return f"https://{bucket}.s3.{AWS_REGION}.amazonaws.com/{key}"

# ---------- Basic name/id parsing from filename ----------
# Examples:
#   00083_kf_rgb.mp4     -> id=83, name="00083_kf_rgb"
#   folder/00157_abc.mp4 -> id=157, name="00157_abc"
#   apple.mp4            -> id=None, name="apple"
ID_PREFIX = re.compile(r"^(\d{1,6})")  # up to 6 digits at start

def parse_id_and_name_from_key(key: str) -> Tuple[int | None, str]:
    fname = os.path.basename(key)
    stem = os.path.splitext(fname)[0]
    m = ID_PREFIX.match(stem)
    vid = int(m.group(1)) if m else None
    # normalize "name" for display/search: lower, spaces/underscores kept
    safe_name = stem.strip().lower()
    return vid, safe_name

# ---------- Main ingest ----------
def ingest_from_s3(prefix: str = "") -> Dict:
    engine = get_db_engine()
    ensure_table(engine)

    inserted = 0
    updated = 0
    scanned = 0
    errors: list[str] = []

    try:
        with engine.begin() as conn:
            for obj in list_mp4_objects(S3_BUCKET, prefix):
                scanned += 1
                key = obj["Key"]
                size = obj.get("Size")
                etag = obj.get("ETag")
                lm = obj.get("LastModified")
                lm_dt: datetime | None = lm if isinstance(lm, datetime) else None

                vid, name = parse_id_and_name_from_key(key)
                url = public_url(S3_BUCKET, key)

                try:
                    res = conn.execute(
                        text(UPSERT_SQL),
                        {
                            "key": key,
                            "name": name,  
                            "url": url,
                            "size": size,
                            "etag": etag,
                            "last_modified": lm_dt,
                        },
                    )
                    # MySQL's ON DUPLICATE KEY doesn't tell us inserted vs updated reliably.
                    # We'll just count as "inserted/updated" based on rowcount when possible.
                    if res.rowcount and res.rowcount > 0:
                        inserted += 1  # treat as success
                except Exception as e:
                    errors.append(f"{key}: {e}")

    except (BotoCoreError, ClientError) as e:
        errors.append(f"S3 error: {e}")

    return {
        "bucket": S3_BUCKET,
        "prefix": S3_PREFIX,
        "scanned": scanned,
        "upserted": inserted,
        "errors": errors,
    }