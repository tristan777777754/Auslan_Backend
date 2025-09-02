# main.py
from fastapi import FastAPI
from violin_visual import app as violin_app
from state_visual import app as state_map_app   

app = FastAPI(title="Auslan Backend Combined")

# 掛載子應用
app.mount("/violin", violin_app)
app.mount("/map", state_map_app)

@app.get("/")
def root():
    return {"message": "Auslan Backend API", "available_endpoints": ["/violin", "/map", "/health"]}

@app.get("/health")
def health():
    return {"status": "ok", "apps": ["violin", "map"]}