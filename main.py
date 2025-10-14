# main.py
from fastapi import FastAPI
from violin_visual import app as violin_app
from state_visual import app as state_map_app   
from year_visual import app as year_app
from ingest_router import router as ingest_router 
from fastapi.middleware.cors import CORSMiddleware
from video_backend import router as video_router
from book1_api import router as book1_router
from book2_api import router as book2_router
app = FastAPI(title="Auslan Backend Combined")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://helloauslan.me" , "http://localhost"],          # tighten to your frontend origin in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/violin", violin_app)
app.mount("/map", state_map_app)
app.mount("/year", year_app)
app.include_router(ingest_router)
app.include_router(video_router)
app.include_router(book1_router)
app.include_router(book2_router)

@app.get("/")
def root():
    return {
        "message": "Auslan Backend API",
        "available_endpoints": ["/violin", "/map", "/year", "/health"] }

@app.get("/health")
def health():
    return {
        "status": "ok",
        "apps": ["violin", "map", "year"]  
    }