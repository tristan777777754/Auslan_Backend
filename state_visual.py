import os
from typing import List, Dict, Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "database-auslan.c9yoaa6kcl93.ap-southeast-2.rds.amazonaws.com")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_NAME", "myapp_db")
DB_USER = os.getenv("DB_USER", "tristan")
DB_PASS = os.getenv("DB_PASS", "Auslan47")

print(f"DB_HOST from env = {DB_HOST}")  # 除錯用

db_url = URL.create(
    "mysql+pymysql",
    username=DB_USER,
    password=DB_PASS,
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
)

# 初始化資料庫引擎
engine = None
try:
    engine = create_engine(
        db_url, 
        pool_pre_ping=True, 
        pool_recycle=3600,
        pool_timeout=30,
        pool_size=5,
        max_overflow=10
    )
    # 測試連線
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("Database connection successful")
except Exception as e:
    print(f"Database connection error: {e}")
    engine = None

# 創建FastAPI應用
app = FastAPI(title="Auslan State Map API")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # allow all during dev
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def map_root():
    return {"message": "Auslan State Map API", "endpoints": ["/state-pop-2021", "/test-db", "/debug-table"]}

@app.get("/state-pop-2021")
def state_pop_2021() -> Dict[str, Any]:
    """
    Query auslan_population_state_years.
    Only return { states: [ {name, value} ] }.
    """
    if not engine:
        raise HTTPException(status_code=500, detail="Database connection not available")
    
    sql = text("""
        SELECT
          `2021State` AS state_name,
          COALESCE(`population`, `population_[0]`) AS population
        FROM auslan_population_state_years
        WHERE `2021State` IS NOT NULL 
        AND `2021State` != ''
    """)

    try:
        with engine.connect() as conn:
            rows = conn.execute(sql).mappings().all()
    except SQLAlchemyError as e:
        print(f"Database query error: {e}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")

    blacklist = {"Total", "Other Territories", "OT", "Other Territory", ""}
    states: List[Dict[str, Any]] = []
    
    for r in rows:
        name = (r["state_name"] or "").strip()
        if not name or name in blacklist:
            continue
        
        try:
            value = int(float(r["population"] or 0))  # 先轉float再轉int，處理小數
        except (ValueError, TypeError):
            value = 0
            
        if value > 0:  # 只包含有效的人口數據
            states.append({"name": name, "value": value})

    # 按人口數量排序
    states.sort(key=lambda x: x["value"], reverse=True)
    
    return {"states": states}

@app.get("/test-db")
def test_db():
    """測試資料庫連線"""
    if not engine:
        return {"status": "error", "message": "Database engine not initialized"}
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as test"))
            return {"status": "success", "message": "Database connection working"}
    except Exception as e:
        return {"status": "error", "message": f"Database connection failed: {str(e)}"}

@app.get("/debug-table")
def debug_table():
    """檢查表格結構和數據"""
    if not engine:
        raise HTTPException(status_code=500, detail="Database connection not available")
    
    try:
        with engine.connect() as conn:
            # 檢查表格是否存在
            table_check = conn.execute(text("SHOW TABLES LIKE 'auslan_population_state_years'")).fetchall()
            if not table_check:
                return {"error": "Table 'auslan_population_state_years' does not exist"}
            
            # 檢查表格結構
            columns = conn.execute(text("DESCRIBE auslan_population_state_years")).mappings().all()
            
            # 檢查前10筆數據
            sample_data = conn.execute(text("SELECT * FROM auslan_population_state_years LIMIT 10")).mappings().all()
            
            # 檢查總行數
            total_rows = conn.execute(text("SELECT COUNT(*) as count FROM auslan_population_state_years")).scalar()
            
            return {
                "table_exists": True,
                "columns": [dict(col) for col in columns],
                "sample_data": [dict(row) for row in sample_data],
                "total_rows": total_rows
            }
    except Exception as e:
        return {"error": f"Debug query failed: {str(e)}"}


