# main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from violin_visual import app as violin_app   
from state_visual  import app as state_app    #

app = FastAPI(title="Auslan API (root)")


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],        
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=False,    
)


app.mount("/violin", violin_app)  
app.mount("/map",    state_app) 

@app.get("/health")
def health():
    return {"ok": True}