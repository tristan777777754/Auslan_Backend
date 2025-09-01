# app.py
import os
import re
import json
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from dotenv import load_dotenv

# -------------------------
# Config / DB engine
# -------------------------
load_dotenv()  # load .env file
print("DB_HOST from env =", os.getenv("DB_HOST"))

DB_HOST = os.getenv("DB_HOST", "")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_NAME", "")
DB_USER = os.getenv("DB_USER", "")
DB_PASS = os.getenv("DB_PASS", "")

DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
    pool_recycle=280,
    future=True,
)

# -------------------------
# FastAPI app
# -------------------------
app = FastAPI(title="Auslan API", version="1.0.0")

# CORS (relax for demo; tighten for production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------
# Helpers
# -------------------------
def fetch_age_df(table: str = "auslan_age_2021") -> tuple[pd.DataFrame, str]:
    """
    Read an age table and return (DataFrame, value_col).
    Assumes columns like: Age_years | <year> Auslan (e.g., '2021 Auslan')
    """
    query = text(f"SELECT * FROM {table}")
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)

    df = df.rename(columns=lambda c: c.strip())
    # find numeric column that contains '2021' by default
    value_cols = [c for c in df.columns if "2021" in c]
    if not value_cols:
        raise ValueError("No column containing '2021' found.")
    value_col = value_cols[0]

    df[value_col] = pd.to_numeric(df[value_col], errors="coerce").fillna(0).astype(int)

    # extract start age for sorting
    def age_start(a: str) -> int:
        a = (a or "").lower()
        if "100" in a:
            return 100
        m = re.match(r"(\d+)", a)
        return int(m.group(1)) if m else -1

    if "Age_years" not in df.columns:
        raise ValueError("Column 'Age_years' not found in table.")

    df["age_start"] = df["Age_years"].map(age_start)
    return df, value_col


def build_pyramid_df(df: pd.DataFrame, value_col: str, male_ratio: float = 0.51) -> pd.DataFrame:
    """
    Clean the data for pyramid and create Male/Female columns by ratio split.
    """
    df_plot = df.copy()
    df_plot = df_plot[df_plot["Age_years"].notna() & df_plot[value_col].notna()]
    df_plot = df_plot[df_plot["Age_years"].str.lower() != "age_years"]
    df_plot = df_plot[df_plot["Age_years"].str.lower().str.contains("total") == False]
    df_plot = df_plot.sort_values("age_start", ascending=False)

    df_plot["Male"] = np.floor(df_plot[value_col] * male_ratio).astype(int)
    df_plot["Female"] = df_plot[value_col] - df_plot["Male"]
    return df_plot


def make_pyramid_figure(df_plot: pd.DataFrame, title_suffix: str) -> go.Figure:
    """
    Build Plotly Figure for population pyramid using cleaned df_plot.
    """
    male_x = -df_plot["Male"]
    female_x = df_plot["Female"]
    y_lab = df_plot["Age_years"]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=male_x,
        y=y_lab,
        orientation='h',
        name='Male',
        marker=dict(color='#3B82F6'),
        hovertemplate='Age: %{y}<br>Male: %{customdata:,}<extra></extra>',
        customdata=df_plot["Male"]
    ))
    fig.add_trace(go.Bar(
        x=female_x,
        y=y_lab,
        orientation='h',
        name='Female',
        marker=dict(color='#EC4899'),
        hovertemplate='Age: %{y}<br>Female: %{customdata:,}<extra></extra>',
        customdata=df_plot["Female"]
    ))

    total_pop = int((df_plot["Male"] + df_plot["Female"]).sum())
    fig.update_layout(
        title=None,
        barmode='overlay',
        bargap=0.15,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        width=850,
        height=600,
        showlegend = False,
        margin=dict(l=40, r=150, t=60, b=40),
    )

    max_side = int(max(df_plot["Male"].max(), df_plot["Female"].max()) * 1.1)
    fig.update_xaxes(
        range=[-max_side, max_side],
        tickvals=[-max_side, -int(max_side*0.5), 0, int(max_side*0.5), max_side],
        ticktext=[f"{max_side}", f"{int(max_side*0.5)}", "0", f"{int(max_side*0.5)}", f"{max_side}"],
        title_text="Number of people"
    )
    fig.update_yaxes(
        title_text=None,
        autorange="reversed",
    )
    return fig


# -------------------------
# General Routes (existing)
# -------------------------
@app.get("/", response_class=JSONResponse)
def root():
    return {"message": "Hello Auslan API is running!"}

@app.get("/health", response_class=PlainTextResponse)
def health():
    return "ok"

@app.get("/age-data", response_class=JSONResponse)
def get_age_data():
    """
    Cleaned Auslan age data as JSON (generic).
    """
    try:
        df, value_col = fetch_age_df()
        df_plot = build_pyramid_df(df, value_col)
        return {
            "value_column": value_col,
            "rows": df_plot[["Age_years", value_col, "age_start", "Male", "Female"]].to_dict(orient="records")
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/age-pyramid", response_class=HTMLResponse)
def age_pyramid_html():
    """
    Standalone Plotly HTML (generic)  can be embedded with <iframe>.
    """
    try:
        df, value_col = fetch_age_df()
        df_plot = build_pyramid_df(df, value_col)
        fig = make_pyramid_figure(df_plot, "Auslan Community Age Distribution (2021)")
        html = fig.to_html(include_plotlyjs="cdn", full_html=True,
                           config={"displaylogo": False,"displayModeBar": False, "responsive": True})
        return HTMLResponse(content=html)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/age-pyramid.json", response_class=JSONResponse)
def age_pyramid_json():
    """
    Plotly figure JSON (generic).
    """
    try:
        df, value_col = fetch_age_df()
        df_plot = build_pyramid_df(df, value_col)
        fig = make_pyramid_figure(df_plot, "Auslan Community Age Distribution (2021)")
        return JSONResponse(content=json.loads(pio.to_json(fig, validate=True)))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -------------------------
# Trends-only Routes (scope this viz to Trends tab)
# -------------------------
@app.get("/trends/age-data", response_class=JSONResponse)
def trends_age_data(
    table: str = Query("auslan_age_2021", description="MySQL table name"),
    male_ratio: float = Query(0.51, ge=0.0, le=1.0)
):
    """
    (Trends) Cleaned age data JSON with inferred Male/Female.
    """
    try:
        df, value_col = fetch_age_df(table=table)
        df_plot = build_pyramid_df(df, value_col, male_ratio=male_ratio)
        rows = df_plot[["Age_years", value_col, "age_start", "Male", "Female"]].to_dict(orient="records")
        return {"scope": "trends", "table": table, "value_column": value_col, "rows": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/trends/age-pyramid", response_class=HTMLResponse)
def trends_age_pyramid_html(
    table: str = Query("auslan_age_2021"),
    male_ratio: float = Query(0.51, ge=0.0, le=1.0),
    title: str = Query("Auslan Community Age Distribution (2021)")
):
    """
    (Trends) Iframe-ready Plotly HTML.
    """
    try:
        df, value_col = fetch_age_df(table=table)
        df_plot = build_pyramid_df(df, value_col, male_ratio=male_ratio)
        fig = make_pyramid_figure(df_plot, title)
        html = fig.to_html(include_plotlyjs="cdn", full_html=True,
                           config={"displaylogo": False, "responsive": True})
        return HTMLResponse(content=html)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/trends/age-pyramid.json", response_class=JSONResponse)
def trends_age_pyramid_json(
    table: str = Query("auslan_age_2021"),
    male_ratio: float = Query(0.51, ge=0.0, le=1.0),
    title: str = Query("Auslan Community Age Distribution (2021)")
):
    """
    (Trends) Plotly figure JSON (data + layout).
    """
    try:
        df, value_col = fetch_age_df(table=table)
        df_plot = build_pyramid_df(df, value_col, male_ratio=male_ratio)
        fig = make_pyramid_figure(df_plot, title)
        return JSONResponse(content=json.loads(pio.to_json(fig, validate=True)))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))