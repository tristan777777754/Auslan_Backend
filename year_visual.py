# year_visual.py

import os
from typing import List, Dict, Any
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.middleware import SlowAPIMiddleware
from slowapi.errors import RateLimitExceeded

# Load environment variables from .env
load_dotenv()

# Environment variable config
DB_HOST = os.getenv("DB_HOST", "")
DB_PORT = int(os.getenv("DB_PORT", ""))
DB_NAME = os.getenv("DB_NAME", "")
DB_USER = os.getenv("DB_USER", "")
DB_PASS = os.getenv("DB_PASS", "")

#print(f"Connecting to database {DB_NAME} at {DB_HOST}:{DB_PORT}...")

# Create SQLAlchemy URL
db_url = URL.create(
    "mysql+pymysql",
    username=DB_USER,
    password=DB_PASS,
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
)

# Create DB engine
engine = None
try:
    engine = create_engine(
        db_url,
        pool_pre_ping=True,
        pool_recycle=3600,
        pool_timeout=30,
        pool_size=5,
        max_overflow=10,
    )
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print(" Database connection successful")
except Exception as e:
    print(" Database connection failed:")
    engine = None

# Create FastAPI app
app = FastAPI(title="Auslan Population By Year API")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://helloauslan.me"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------
# Rate limiters
# -------------------------

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_middleware(SlowAPIMiddleware)

@app.exception_handler(RateLimitExceeded)
def _rate_limit_handler(request, exc):  # type: ignore
    return PlainTextResponse("Too Many Requests", status_code=429)

@app.get("/")
@limiter.limit("5/10second")
def root(request: Request):
    return {
        "message": "Welcome to Auslan Population By Year API",
        "endpoints": ["/population-by-year", "/debug-population-year"]
    }

@app.get("/population-by-year")
@limiter.limit("5/10second")
def get_population_by_year(request: Request) -> Dict[str, Any]:
    """
    Returns a list of population values by year from population_diffyear table.
    Format: { "yearly_population": [ { "year": "2018", "population": 100000 }, ... ] }
    """
    if not engine:
        raise HTTPException(status_code=500, detail="Database engine not available")

    sql = text("""
        SELECT Year, population
        FROM population_diffyear
        WHERE Year IS NOT NULL AND population IS NOT NULL
        ORDER BY Year
    """)

    try:
        with engine.connect() as conn:
            rows = conn.execute(sql).mappings().all()
            print(f"Retrieved {len(rows)} rows from population_diffyear")
    except SQLAlchemyError as e:
        # print(f"Error executing query: {e}")
        # raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"Error": "Internal server error."}
        )

    result: List[Dict[str, Any]] = []
    for row in rows:
        try:
            year = str(row["Year"]).strip()
            population = int(float(row["population"]))
            result.append({"year": year, "population": population})
        except Exception as e:
            #print(f"Skipping row due to error: {e}")
            return JSONResponse(
            status_code=500,
            content={"Error": "Internal server error."}
        )
            continue

    return {"yearly_population": result}

@app.get("/debug-population-year")
@limiter.limit("5/10second")
def debug_population_year(request: Request):
    """
    Debug: Returns all rows from population_diffyear (for inspection)
    """
    if not engine:
        raise HTTPException(status_code=500, detail="Database engine not available")
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT * FROM population_diffyear")).mappings().all()
            return {"rows": [dict(row) for row in rows], "count": len(rows)}
    except Exception as e:
        # return {"error": str(e)}
        return JSONResponse(
            status_code=500,
            content={"Error": "Internal server error."}
        )