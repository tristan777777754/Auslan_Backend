# s3_toSQL.py
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
  filename VARCHAR(255) NULL,
  s3_bucket VARCHAR(128) NOT NULL,
  s3_key VARCHAR(1024) NOT NULL,
  url VARCHAR(1024) NOT NULL,
  size_bytes BIGINT NULL,
  etag VARCHAR(128) NULL,
  last_modified DATETIME NULL,
  collection VARCHAR(255) NULL,   
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_s3_key (s3_key),
  INDEX idx_id (id),
  INDEX idx_name (filename),
  INDEX idx_collection (collection)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

UPSERT_SQL = """
INSERT INTO videos (id, filename, s3_bucket, s3_key, url, size_bytes, etag, last_modified, collection)
VALUES (:id, :filename, :bucket, :key, :url, :size, :etag, :last_modified, :collection)
ON DUPLICATE KEY UPDATE
  filename     = VALUES(filename),
  url          = VALUES(url),
  size_bytes   = VALUES(size_bytes),
  etag         = VALUES(etag),
  last_modified= VALUES(last_modified),
  collection   = VALUES(collection);
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
    return f"https://{bucket}.s3.{AWS_REGION}.amazonaws.com/{key}"

# ---------- Basic name/id parsing ----------
ID_PREFIX = re.compile(r"^(\d{1,6})")

def parse_id_and_name_from_key(key: str) -> Tuple[int | None, str]:
    fname = os.path.basename(key)
    stem = os.path.splitext(fname)[0]
    m = ID_PREFIX.match(stem)
    vid = int(m.group(1)) if m else None
    safe_name = stem.strip().lower()
    return vid, safe_name

# ---------- Main ingest ----------
def ingest_from_s3(prefix: str = "", collection: str = None) -> Dict:
    engine = get_db_engine()
    ensure_table(engine)

    inserted = 0
    scanned = 0
    errors: list[str] = []

    # 如果沒給 collection，就用 prefix 當作 collection 名稱
    collection_name = collection or (prefix.rstrip("/").replace("/", "_") + "_video")

    try:
        with engine.begin() as conn:
            for obj in list_mp4_objects(S3_BUCKET, prefix):
                scanned += 1
                key = obj["Key"]
                size = obj.get("Size")
                etag = obj.get("ETag")
                lm = obj.get("LastModified")
                lm_dt: datetime | None = lm if isinstance(lm, datetime) else None

                vid, filename = parse_id_and_name_from_key(key)
                url = public_url(S3_BUCKET, key)

                try:
                    conn.execute(
                        text(UPSERT_SQL),
                        {
                            "id": vid,
                            "filename": filename,
                            "bucket": S3_BUCKET,
                            "key": key,
                            "url": url,
                            "size": size,
                            "etag": etag,
                            "last_modified": lm_dt,
                            "collection": collection_name,
                        },
                    )
                    inserted += 1
                except Exception as e:
                    errors.append(f"{key}: {e}")

    except (BotoCoreError, ClientError) as e:
        errors.append(f"S3 error: {e}")

    return {
        "bucket": S3_BUCKET,
        "prefix": prefix,
        "collection": collection_name,
        "scanned": scanned,
        "upserted": inserted,
        "errors": errors,
    }