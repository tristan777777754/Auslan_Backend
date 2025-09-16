from fastapi import APIRouter, HTTPException, Query
from s3_toSQL import ingest_from_s3

router = APIRouter(tags=["admin"])

@router.post("/ingest-s3")
def run_ingest(prefix: str = Query(default="")):
    """
    Trigger a one-off S3 -> MySQL import.
    Allows optional prefix (e.g. converted/).
    """
    try:
        summary = ingest_from_s3(prefix=prefix) 
        return {"status": "ok", **summary}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))