# ingest_router.py
from fastapi import APIRouter, HTTPException, Query
from s3_toSQL import ingest_from_s3

# Router for /admin endpoints
router = APIRouter(prefix="/admin", tags=["admin"])

@router.post("/ingest-s3")
def run_ingest(prefix: str = Query(default="", description="Optional S3 prefix (e.g. converted/)")):
    """
    Trigger a one-off S3 -> MySQL import.
    If prefix is provided, only files under that prefix will be scanned.
    Example:
        POST /admin/ingest-s3?prefix=converted/
    """
    try:
        summary = ingest_from_s3(prefix=prefix)  # pass prefix to s3_toSQL
        return {"status": "ok", **summary}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))