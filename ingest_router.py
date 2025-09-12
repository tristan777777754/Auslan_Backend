# ingest_router.py
from fastapi import APIRouter, HTTPException
from s3_toSQL import ingest_from_s3

router = APIRouter(tags=["admin"])

@router.post("/admin/ingest-s3")
def run_ingest():
    """
    Trigger a one-off S3 -> MySQL import.
    Returns a small summary JSON.
    """
    try:
        summary = ingest_from_s3()
        return {"status": "ok", **summary}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))