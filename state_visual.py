# state_map.py
import os
from typing import List, Dict, Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv

# Load .env (DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "database-auslan.c9yoaa6kcl93.ap-southeast-2.rds.amazonaws.com")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_NAME", "myapp_db")
DB_USER = os.getenv("DB_USER", "tristan")
DB_PASS = os.getenv("DB_PASS", "Auslan47")

db_url = URL.create(
    "mysql+pymysql",
    username=DB_USER,
    password=DB_PASS,
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
)
engine = create_engine(db_url, pool_pre_ping=True)

app = FastAPI(title="Auslan State Map API")

# CORS for your frontend origin (or * while testing)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # tighten later to your frontend domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/state-pop-2021")
def state_pop_2021() -> Dict[str, Any]:
    """
    Query the `auslan_population_state_years` table for year 2021.
    Your table columns (based on your screenshot):
      - Year
      - 2021State
      - population OR population_[0]
    We COALESCE to survive either naming.
    """
    sql = text("""
        SELECT
          `2021State` AS state_name,
          COALESCE(`population`, `population_[0]`) AS population
        FROM auslan_population_state_years
        WHERE `Year` = 2021
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql).mappings().all()

    # Clean/filter rows: drop totals/other territories if present
    blacklist = {"Total", "Other Territories", "OT", "Other Territory"}
    data: List[Dict[str, Any]] = []
    for r in rows:
        name = (r["state_name"] or "").strip()
        if not name or name in blacklist:
            continue
        value = int(r["population"] or 0)
        data.append({"name": name, "value": value})

    return {"year": 2021, "states": data}