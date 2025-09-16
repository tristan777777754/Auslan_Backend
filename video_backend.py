from fastapi import FastAPI, Depends
from sqlalchemy import create_engine, text
import os

app = FastAPI()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", 3306)
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

@app.get("/videos")
def get_videos():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT id, filename, url FROM videos"))
        videos = [dict(row._mapping) for row in result]
    return videos