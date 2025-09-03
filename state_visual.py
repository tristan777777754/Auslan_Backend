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

DB_HOST = os.getenv("DB_HOST", "")
DB_PORT = int(os.getenv("DB_PORT", ""))
DB_NAME = os.getenv("DB_NAME", "")
DB_USER = os.getenv("DB_USER", "")
DB_PASS = os.getenv("DB_PASS", "")

print(f"DB_HOST from env = {DB_HOST}")

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
    allow_origins=["*"],
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
    根據實際資料庫結構：只有 2021State 和 population_[0] 兩個欄位
    """
    if not engine:
        raise HTTPException(status_code=500, detail="Database connection not available")
    
    # 修正SQL查詢 - 使用正確的欄位名稱
    sql = text("""
        SELECT
          `2021State` AS state_name,
          `population_[0]` AS population
        FROM auslan_population_state_years
        WHERE `2021State` IS NOT NULL 
        AND `2021State` != ''
        AND `2021State` != 'Total'
        AND `population_[0]` IS NOT NULL
        AND `population_[0]` > 0
    """)

    try:
        with engine.connect() as conn:
            rows = conn.execute(sql).mappings().all()
            print(f"Found {len(rows)} rows from database")
    except SQLAlchemyError as e:
        print(f"Database query error: {e}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")

    # 清理和過濾州名
    blacklist = {
        "Total", "Other Territories", "OT", "Other Territory", 
        "Australia", "Total Australia", "", None
    }
    
    states: List[Dict[str, Any]] = []
    
    for r in rows:
        state_name = (r["state_name"] or "").strip()
        
        # 跳過黑名單中的項目
        if not state_name or state_name in blacklist:
            continue
        
        try:
            # 處理人口數據
            pop_value = r["population"]
            if pop_value is None:
                continue
                
            # 轉換為整數
            if isinstance(pop_value, str):
                # 移除逗號和其他非數字字符
                pop_value = pop_value.replace(',', '').replace(' ', '')
                value = int(float(pop_value))
            else:
                value = int(float(pop_value))
                
        except (ValueError, TypeError) as e:
            print(f"Error converting population for {state_name}: {pop_value}, error: {e}")
            continue
            
        if value > 0:
            states.append({"name": state_name, "value": value})
            print(f"Added state: {state_name} = {value}")

    # 按人口數量排序（從大到小）
    states.sort(key=lambda x: x["value"], reverse=True)
    
    print(f"Returning {len(states)} states")
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
            
            # 檢查所有數據
            all_data = conn.execute(text("SELECT * FROM auslan_population_state_years")).mappings().all()
            
            # 檢查總行數
            total_rows = conn.execute(text("SELECT COUNT(*) as count FROM auslan_population_state_years")).scalar()
            
            return {
                "table_exists": True,
                "columns": [dict(col) for col in columns],
                "all_data": [dict(row) for row in all_data],
                "total_rows": total_rows
            }
    except Exception as e:
        return {"error": f"Debug query failed: {str(e)}"}

@app.get("/raw-data")
def raw_data():
    """返回原始數據，用於除錯"""
    if not engine:
        raise HTTPException(status_code=500, detail="Database connection not available")
    
    try:
        with engine.connect() as conn:
            sql = text("SELECT `2021State`, `population_[0]` FROM auslan_population_state_years")
            rows = conn.execute(sql).mappings().all()
            
            return {
                "raw_data": [dict(row) for row in rows],
                "count": len(rows)
            }
    except Exception as e:
        return {"error": f"Raw data query failed: {str(e)}"}