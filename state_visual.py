# state_map.py
import os
from typing import List, Dict, Any
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "database-auslan.c9yoaa6kcl93.ap-southeast-2.rds.amazonaws.com")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_NAME", "myapp_db")
DB_USER = os.getenv("DB_USER", "tristan")
DB_PASS = os.getenv("DB_PASS", "Auslan47")

FRONTEND_ORIGIN = os.getenv("FRONTEND_ORIGIN", "")

# -----------------------------
# Create DB engine
# -----------------------------
db_url = URL.create(
    "mysql+pymysql",
    username=DB_USER,
    password=DB_PASS,
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
)
engine = create_engine(db_url, pool_pre_ping=True, pool_recycle=280)

# -----------------------------
# FastAPI app & CORS
# -----------------------------
app = FastAPI(title="Auslan State Map API")

allow_origins: List[str] = []
if FRONTEND_ORIGIN:
    allow_origins = [o.strip() for o in FRONTEND_ORIGIN.split(",") if o.strip()]
if "http://localhost:5173" not in allow_origins:
    allow_origins.append("http://localhost:5173")

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# Utils
# -----------------------------
BLACKLIST = {"Total", "Other Territories", "OT", "Other Territory", ""}

STATE_NORMALIZE = {
    "New South Wales": "New South Wales", "NSW": "New South Wales",
    "Victoria": "Victoria", "VIC": "Victoria",
    "Queensland": "Queensland", "QLD": "Queensland",
    "South Australia": "South Australia", "SA": "South Australia",
    "Western Australia": "Western Australia", "WA": "Western Australia",
    "Tasmania": "Tasmania", "TAS": "Tasmania",
    "Northern Territory": "Northern Territory", "NT": "Northern Territory",
    "Australian Capital Territory": "Australian Capital Territory", "ACT": "Australian Capital Territory",
}

def normalize_state(name: str) -> str:
    if not name:
        return ""
    name = name.strip()
    if name in STATE_NORMALIZE:
        return STATE_NORMALIZE[name]
    for k, v in STATE_NORMALIZE.items():
        if name.lower() == k.lower():
            return v
    return name

def fetch_state_population(year: int = 2021) -> List[Dict[str, Any]]:
    sql = text("""
        SELECT
            `2021State` AS state_name,
            COALESCE(`population`, `population_[0]`) AS population
        FROM auslan_population_state_years
        WHERE `Year` = :year
    """)
    try:
        with engine.connect() as conn:
            rows = conn.execute(sql, {"year": year}).mappings().all()
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"DB error: {str(e)}")

    data: List[Dict[str, Any]] = []
    for r in rows:
        raw_name = (r.get("state_name") or "").strip()
        if raw_name in BLACKLIST:
            continue
        name = normalize_state(raw_name)
        try:
            value = int(r.get("population") or 0)
        except (TypeError, ValueError):
            value = 0
        if not name or name in BLACKLIST:
            continue
        data.append({"name": name, "value": value})

    return sorted(data, key=lambda x: x["name"])

# -----------------------------
# Routes
# -----------------------------
@app.get("/health")
def health():
    return {"ok": True}

# Canonical route
@app.get("/map/state-pop/{year}")
def map_state_pop_year(year: int) -> Dict[str, Any]:
    return {"year": year, "states": fetch_state_population(year)}

# Default (2021) shortcut
@app.get("/map/state-pop")
def map_state_pop_default() -> Dict[str, Any]:
    year = 2021
    return {"year": year, "states": fetch_state_population(year)}