# streamlit_app.py
import streamlit as st
import pandas as pd
import plotly.express as px
from query_templates import query_descriptions
from frequency_predict import FrequencyPredictor
from controller import SmartController

st.set_page_config(page_title="Smart Query Predictor", layout="wide")

st.title("Smart Query Performance Predictor")
st.markdown("Using frequency-based prediction and materialized views")

page = st.sidebar.radio(
    "Select View:",
    ["Dashboard", "Live Prediction", "Performance Analysis"]
)

# ========== PAGE 1: DASHBOARD ==========
if page == "Dashboard":
    st.header("System Overview")
    try:
        baseline_df = pd.read_csv("baselines.csv")
        smart_df = pd.read_csv("smart_performance.csv")

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Queries (smart run)", len(smart_df))
        with col2:
            st.metric("Avg Baseline Time", f"{baseline_df['baseline_time_sec'].mean():.4f}s")
        with col3:
            st.metric("Avg Smart Time", f"{smart_df['runtime_sec'].mean():.4f}s")

        st.divider()
        st.subheader("Query Distribution (Smart Run)")
        counts = smart_df["query_id"].value_counts().sort_index()
        fig = px.bar(x=counts.index, y=counts.values,
                     labels={"x": "Query ID", "y": "Count"})
        st.plotly_chart(fig, use_container_width=True)
    except FileNotFoundError:
        st.warning("Run performance.py first to generate baselines.csv and smart_performance.csv.")

# ========== PAGE 2: LIVE PREDICTION ==========
elif page == "Live Prediction":
    st.header("Live Prediction Demo")

    try:
        predictor = FrequencyPredictor("query_log.csv")
        controller = SmartController("query_log.csv")

        current_query = st.selectbox("Current query:", ["Q1", "Q2", "Q3", "Q4", "Q5"])
        inter_arrival = st.slider("Time until next query (seconds):", 1, 300, 5)

        predicted = predictor.predict_next(current_query)
        st.write(f"Predicted next query after {current_query}: {predicted}")

        if predicted:
            confidence = predictor.get_confidence(current_query, predicted)
            st.write(f"Confidence: {confidence*100:.1f}%")

            controller.measure_mv_times()
            should_build, details = controller.should_prematerialize(
                current_query, inter_arrival
            )

            if should_build and details:
                st.success(f"Materialize {details['mv_name']} "
                           f"(build {details['build_time']:.3f}s < {details['inter_arrival_time']:.1f}s)")
            else:
                st.warning("Skip materialization for this next query.")
        else:
            st.warning("No prediction available for this query (not enough history).")

    except FileNotFoundError:
        st.warning("Run query.py and performance.py first to generate query_log and MV timings.")
    except Exception as e:
        st.error(f"Error: {e}")

# ========== PAGE 3: PERFORMANCE ANALYSIS ==========
elif page == "Performance Analysis":
    st.header("Performance Analysis")

    try:
        baseline_df = pd.read_csv("baselines.csv")
        smart_df = pd.read_csv("smart_performance.csv")

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Avg Baseline Time", f"{baseline_df['baseline_time_sec'].mean():.4f}s")
        with col2:
            st.metric("Avg Smart Time", f"{smart_df['runtime_sec'].mean():.4f}s")
        with col3:
            speedup = baseline_df["baseline_time_sec"].mean() / smart_df["runtime_sec"].mean()
            st.metric("Overall Speedup", f"{speedup:.2f}x")

        st.divider()
        st.subheader("Per-query Comparison")

        base_by_q = baseline_df.groupby("query_id")["baseline_time_sec"].mean()
        smart_by_q = smart_df.groupby("query_id")["runtime_sec"].mean()
        comp = pd.DataFrame({
            "query_id": base_by_q.index,
            "baseline": base_by_q.values,
            "smart": smart_by_q.reindex(base_by_q.index).values,
        })

        fig = px.bar(
            comp.melt(id_vars="query_id", var_name="mode", value_name="time"),
            x="query_id", y="time", color="mode", barmode="group",
            labels={"time": "Time (s)", "query_id": "Query"}
        )
        st.plotly_chart(fig, use_container_width=True)

    except FileNotFoundError:
        st.warning("Run performance.py first to generate data.")
    except Exception as e:
        st.error(f"Error: {e}")
