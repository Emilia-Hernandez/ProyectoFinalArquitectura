from __future__ import annotations

from pathlib import Path

import joblib
import pandas as pd
import plotly.express as px
import streamlit as st
from pyarrow.lib import ArrowInvalid

ROOT = Path("output")
STATS_PATH = ROOT / "stats"
PRED_PATH = ROOT / "predictions"
MODEL_PATH = ROOT / "models" / "linear_regression.joblib"

st.set_page_config(page_title="AlphaVantage Streaming Dashboard", layout="wide")

st.markdown(
    """
    <style>
    .stApp {
        background: linear-gradient(135deg, #f2f7ff 0%, #f9f4ef 100%);
    }
    .block-container {padding-top: 1.5rem;}
    .metric-box {
        padding: 1rem;
        border-radius: 16px;
        background: rgba(255, 255, 255, 0.8);
        border: 1px solid rgba(0, 0, 0, 0.08);
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Streaming + ML Pipeline (Spark + Kafka + Alpha Vantage)")


@st.cache_data(ttl=5)
def load_stats() -> pd.DataFrame:
    if not STATS_PATH.exists():
        return pd.DataFrame()
    latest_file = STATS_PATH / "latest.parquet"
    if latest_file.exists() and latest_file.stat().st_size > 0:
        try:
            return pd.read_parquet(latest_file)
        except (ArrowInvalid, OSError, ValueError):
            return pd.DataFrame()

    files = [f for f in sorted(STATS_PATH.glob("*.parquet")) if f.stat().st_size > 0]
    frames: list[pd.DataFrame] = []
    for parquet_file in files[-30:]:
        try:
            frames.append(pd.read_parquet(parquet_file))
        except (ArrowInvalid, OSError, ValueError):
            continue
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


@st.cache_data(ttl=5)
def load_predictions() -> pd.DataFrame:
    if not PRED_PATH.exists():
        return pd.DataFrame()
    files = [f for f in sorted(PRED_PATH.glob("*.parquet")) if f.stat().st_size > 0]
    if not files:
        return pd.DataFrame()
    frames: list[pd.DataFrame] = []
    for parquet_file in files[-30:]:
        try:
            frames.append(pd.read_parquet(parquet_file))
        except (ArrowInvalid, OSError, ValueError):
            continue
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def build_demo_predictions_from_silver(limit_rows: int = 500) -> pd.DataFrame:
    silver_path = ROOT / "silver"
    if not silver_path.exists() or not MODEL_PATH.exists():
        return pd.DataFrame()

    try:
        silver_df = pd.read_parquet(silver_path).tail(limit_rows).copy()
        if silver_df.empty:
            return pd.DataFrame()

        artifact = joblib.load(MODEL_PATH)
        features = artifact["features"]
        model = artifact["model"]

        for col in features:
            if col not in silver_df.columns:
                silver_df[col] = 0.0

        silver_df["pred_close"] = model.predict(silver_df[features])
        silver_df["abs_error"] = (silver_df["pred_close"] - silver_df["close"]).abs()
        silver_df["source_mode"] = "demo_from_silver"
        return silver_df
    except Exception:
        return pd.DataFrame()


stats_df = load_stats()
pred_df = load_predictions()
pred_source = "stream"
if pred_df.empty:
    demo_df = build_demo_predictions_from_silver()
    if not demo_df.empty:
        pred_df = demo_df
        pred_source = "demo"

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Ventanas calculadas", int(len(stats_df)) if not stats_df.empty else 0)
with col2:
    st.metric("Predicciones generadas", int(len(pred_df)) if not pred_df.empty else 0)
with col3:
    mae = float(pred_df["abs_error"].mean()) if not pred_df.empty else 0.0
    st.metric("MAE online", f"{mae:.4f}")

left, right = st.columns([1.4, 1])

with left:
    st.subheader("Precio promedio por ventana")
    if stats_df.empty:
        st.info("Aún no hay datos en output/stats. Ejecuta el pipeline de streaming.")
    else:
        stats_df = stats_df.sort_values("window_start")
        fig = px.line(
            stats_df,
            x="window_start",
            y="avg_close",
            color="symbol",
            title="avg_close (windowed)",
            markers=True,
        )
        fig.update_layout(height=380)
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Varianza por ventana")
        fig_var = px.bar(
            stats_df.tail(40),
            x="window_end",
            y="var_close",
            color="symbol",
            title="var_close (últimas ventanas)",
        )
        fig_var.update_layout(height=320)
        st.plotly_chart(fig_var, use_container_width=True)

with right:
    st.subheader("Predicción vs real")
    if pred_df.empty:
        st.info("Aún no hay datos en output/predictions. Ejecuta stream_predictor.")
    else:
        if pred_source == "demo":
            st.warning(
                "Mostrando predicciones demo generadas desde output/silver. "
                "Para ver predicciones online reales, deja corriendo `make predict`."
            )
        pred_df = pred_df.sort_values("event_time").tail(500)
        fig_pred = px.line(
            pred_df,
            x="event_time",
            y=["close", "pred_close"],
            title="close real vs predicho",
        )
        fig_pred.update_layout(height=360)
        st.plotly_chart(fig_pred, use_container_width=True)

        st.subheader("Distribución del error absoluto")
        fig_hist = px.histogram(pred_df, x="abs_error", nbins=30)
        fig_hist.update_layout(height=320)
        st.plotly_chart(fig_hist, use_container_width=True)

st.divider()
st.caption(
    "Actualiza cada 5s. Ventanas de 10s. Datos: Alpha Vantage o simulación. "
    "Revisa Spark UI en http://localhost:4040 y Redpanda Console en http://localhost:8080"
)
