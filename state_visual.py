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
engine = create_engine(
    db_url,
    pool_pre_ping=True,
    pool_recycle=280,   # recycle before AWS default idle timeout
)

# -----------------------------
# FastAPI app & CORS
# -----------------------------
app = FastAPI(title="Auslan State Map API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          
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
    """
    Query table 'auslan_population_state_years' for a given year
    and return a list of { name: <state>, value: <population> }.
    """
    sql = text(
        """
        SELECT
            `2021State` AS state_name,
            COALESCE(`population`, `population_[0]`) AS population
        FROM auslan_population_state_years
        WHERE `Year` = :year
        """
    )
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

    data.sort(key=lambda x: x["name"])
    return data

# -----------------------------
# Routes
# -----------------------------
@app.get("/health")
def health():
    return {"ok": True}


@app.get("/state-pop")
def state_pop_default() -> List[Dict[str, Any]]:
    return fetch_state_population(year=2021)

@app.get("/state-pop/{year}")
def state_pop_by_year(year: int) -> List[Dict[str, Any]]:
    return fetch_state_population(year=year)

@app.get("/api/state-population")
def state_pop_compat() -> List[Dict[str, Any]]:
    return fetch_state_population(year=2021)


@app.get("/map/state-pop-{year}")
def map_state_pop_year(year: int) -> Dict[str, Any]:
    return {"year": year, "states": fetch_state_population(year)}

@app.get("/map/state-pop-2021")
def map_state_pop_2021() -> Dict[str, Any]:
    return {"year": 2021, "states": fetch_state_population(2021)}