from __future__ import annotations

import json
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
HARDWARE_PATH = ROOT / "logs" / "hardware_profile.json"
BENCHMARK_PATH = ROOT / "logs" / "benchmark_runs.csv"
STREAM_PROGRESS_PATH = ROOT / "logs" / "stream_progress" / "stats_snapshot.json"

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
    df = pd.concat(frames, ignore_index=True)
    if "pred_next_close" not in df.columns and "pred_close" in df.columns:
        df["pred_next_close"] = df["pred_close"]
    return df


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

        silver_df = silver_df.sort_values(["symbol", "event_time"]).reset_index(drop=True)
        silver_df["pred_next_close"] = model.predict(silver_df[features])
        silver_df["actual_next_close"] = silver_df.groupby("symbol")["close"].shift(-1)
        silver_df["abs_error"] = (silver_df["pred_next_close"] - silver_df["actual_next_close"]).abs()
        silver_df["source_mode"] = "demo_from_silver"
        return silver_df.dropna(subset=["actual_next_close", "pred_next_close"])
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=5)
def load_hardware_profile() -> dict:
    if not HARDWARE_PATH.exists():
        return {}
    try:
        return json.loads(HARDWARE_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


@st.cache_data(ttl=5)
def load_benchmark_runs() -> pd.DataFrame:
    if not BENCHMARK_PATH.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(BENCHMARK_PATH)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=5)
def load_stream_progress() -> dict:
    if not STREAM_PROGRESS_PATH.exists():
        return {}
    try:
        return json.loads(STREAM_PROGRESS_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def format_bytes(num_bytes: float | int | None) -> str:
    if not num_bytes:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(num_bytes)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} TB"


stats_df = load_stats()
pred_df = load_predictions()
pred_source = "stream"
if pred_df.empty:
    demo_df = build_demo_predictions_from_silver()
    if not demo_df.empty:
        pred_df = demo_df
        pred_source = "demo"
elif "event_time" in pred_df.columns:
    pred_df = pred_df.sort_values(["symbol", "event_time"]).reset_index(drop=True)
    if "actual_next_close" not in pred_df.columns and "close" in pred_df.columns:
        pred_df["actual_next_close"] = pred_df.groupby("symbol")["close"].shift(-1)
    if "pred_next_close" not in pred_df.columns and "pred_close" in pred_df.columns:
        pred_df["pred_next_close"] = pred_df["pred_close"]
    pred_df["abs_error"] = (pred_df["pred_next_close"] - pred_df["actual_next_close"]).abs()
    pred_df = pred_df.dropna(subset=["actual_next_close", "pred_next_close"])
hardware = load_hardware_profile()
benchmarks = load_benchmark_runs()
stream_progress = load_stream_progress()
state_ops = stream_progress.get("stateOperators", [])
state_metrics = state_ops[0] if state_ops else {}
duration_ms = stream_progress.get("durationMs", {})

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
            y=["actual_next_close", "pred_next_close"],
            title="next_close real vs predicho",
        )
        fig_pred.update_layout(height=360)
        st.plotly_chart(fig_pred, use_container_width=True)

        st.subheader("Distribución del error absoluto")
        fig_hist = px.histogram(pred_df, x="abs_error", nbins=30)
        fig_hist.update_layout(height=320)
        st.plotly_chart(fig_hist, use_container_width=True)

st.divider()
st.subheader("Métricas para comparar arquitecturas")
cmp1, cmp2, cmp3, cmp4 = st.columns(4)
with cmp1:
    st.metric("Input rate", f"{float(stream_progress.get('inputRowsPerSecond', 0.0)):.2f} rows/s")
with cmp2:
    st.metric("Processing rate", f"{float(stream_progress.get('processedRowsPerSecond', 0.0)):.2f} rows/s")
with cmp3:
    st.metric("Batch duration", f"{float(duration_ms.get('triggerExecution', 0.0)):.0f} ms")
with cmp4:
    st.metric("State memory", format_bytes(state_metrics.get("memoryUsedBytes", 0)))

cmp5, cmp6, cmp7, cmp8 = st.columns(4)
with cmp5:
    st.metric("Input rows", int(stream_progress.get("numInputRows", 0)))
with cmp6:
    st.metric("State rows", int(state_metrics.get("numRowsTotal", 0)))
with cmp7:
    st.metric("Rows updated", int(state_metrics.get("numRowsUpdated", 0)))
with cmp8:
    st.metric("Dropped by watermark", int(state_metrics.get("numRowsDroppedByWatermark", 0)))

left_cmp, right_cmp = st.columns([1.2, 1])
with left_cmp:
    st.markdown("**Perfil de máquina**")
    if not hardware:
        st.info("Corre `python -m scripts.hardware_profile` para mostrar hardware local.")
    else:
        hw_df = pd.DataFrame(
            [
                ("Plataforma", hardware.get("platform", "")),
                ("Machine", hardware.get("machine", "")),
                ("Processor", hardware.get("processor", "")),
                ("Python", hardware.get("python_version", "")),
                ("Physical cores", hardware.get("physical_cores", "")),
                ("Logical cores", hardware.get("logical_cores", "")),
                ("RAM (GB)", hardware.get("total_ram_gb", "")),
            ],
            columns=["Métrica", "Valor"],
        )
        st.dataframe(hw_df, use_container_width=True, hide_index=True)

    st.markdown("**Notas para comparación A/B**")
    st.caption(
        "El dashboard ya expone input rate, processing rate, batch duration y memoria de estado. "
        "Shuffle, GC, spill detallado y scheduler delay siguen saliendo de Spark UI."
    )

with right_cmp:
    st.markdown("**Benchmarks locales**")
    if benchmarks.empty:
        st.info("Corre `make benchmark` para guardar tiempos comparables entre máquinas.")
    else:
        latest_benchmarks = benchmarks.sort_values("timestamp_utc").tail(6).copy()
        latest_benchmarks["elapsed_seconds"] = latest_benchmarks["elapsed_seconds"].round(3)
        st.dataframe(
            latest_benchmarks[["timestamp_utc", "command", "elapsed_seconds", "return_code"]],
            use_container_width=True,
            hide_index=True,
        )

    st.markdown("**Spark UI a capturar**")
    st.write("Jobs: job execution time")
    st.write("Stages: shuffle read/write, I/O, scheduler delay, executor run time, GC, spill")
    st.write("Structured Streaming: input rate, processing rate, batch duration")

st.caption(
    "Actualiza cada 5s. Ventanas de 10s. Datos: Alpha Vantage o simulación. "
    "Revisa Spark UI en http://localhost:4040 y Redpanda Console en http://localhost:8080"
)
