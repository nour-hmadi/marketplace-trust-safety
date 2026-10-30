"""Pipeline Monitor page: one live panel per stage, from Kafka to the curated layer."""
import streamlit as st

import pipeline_status as ps


@st.cache_data(ttl=15, show_spinner=False)
def load(name, *args):
    """Call one status function. Return (data, error) so one stopped service never breaks the page."""
    try:
        return getattr(ps, name)(*args), None
    except Exception as error:
        return None, str(error)


def show_table(data, error, empty_text="No data yet."):
    if error:
        st.error(f"Service unreachable: {error}")
    elif data:
        st.dataframe(data, hide_index=True)
    else:
        st.info(empty_text)


def last_write(rows):
    return max((r["last write"] for r in rows), default="-") if rows else "-"


def pipeline_monitor_page():
    st.header("🔄 Pipeline Monitor")
    st.caption("Live status from each service's own API. Data moves left to right: "
               "Kafka → Airflow → HDFS raw → Spark → HDFS refined & curated.")
    if st.button("🔄 Refresh now"):
        st.cache_data.clear()

    kafka, kafka_error = load("kafka_status")
    runs, runs_error = load("airflow_runs")
    next_run, _ = load("airflow_next_run")
    raw, raw_error = load("hdfs_layer", "raw")
    spark, spark_error = load("spark_status")
    refined, refined_error = load("hdfs_layer", "refined")
    curated, curated_error = load("hdfs_layer", "curated")

    # ----- overview: one card per stage -----
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.markdown("**1 · Kafka**")
        if kafka_error:
            st.error("unreachable")
        else:
            st.metric("Waiting for Airflow", sum(r["waiting for Airflow"] for r in kafka))
            st.caption(f"{sum(r['messages in Kafka'] for r in kafka)} messages kept")
    with col2:
        st.markdown("**2 · Airflow**")
        if runs_error:
            st.error("unreachable")
        else:
            st.metric("Last ingestion", runs[0]["state"] if runs else "-")
            st.caption(f"Next run: {next_run or '-'}")
    with col3:
        st.markdown("**3 · HDFS raw**")
        if raw_error:
            st.error("unreachable")
        else:
            st.metric("Raw files", sum(r["files"] for r in raw))
            st.caption(f"Last write: {last_write(raw)}")
    with col4:
        st.markdown("**4 · Spark**")
        if spark_error:
            st.error("unreachable")
        else:
            st.metric("Workers alive", spark["alive_workers"])
            st.caption(f"{spark['cores']} cores · {spark['running_jobs']} job(s) running")
    with col5:
        st.markdown("**5 · HDFS curated**")
        if curated_error:
            st.error("unreachable")
        else:
            st.metric("Curated tables", len(curated))
            st.caption(f"Last write: {last_write(curated)}")

    # ----- action -----
    if st.button("▶ Trigger Airflow ingestion now"):
        try:
            run_id = ps.trigger_ingestion()
            st.cache_data.clear()
            st.success(f"Airflow started run {run_id}. Click Refresh in about 30 seconds.")
        except Exception as error:
            st.error(f"Could not trigger Airflow: {error}")

    # ----- details, one section per stage -----
    st.divider()
    st.subheader("1 · Kafka topics")
    st.caption("'Waiting for Airflow' = messages produced but not yet copied to HDFS.")
    show_table(kafka, kafka_error)

    st.subheader("2 · Airflow ingestion runs")
    st.caption("DAG ingest_orders runs every 10 minutes: Kafka → HDFS raw (orders, returns, complaints).")
    show_table(runs, runs_error)

    st.subheader("3 · HDFS raw layer")
    st.caption("Original data as received, never modified (audit trail). Airflow adds one file per topic per run.")
    show_table(raw, raw_error)

    st.subheader("4 · Spark jobs")
    st.caption("Spark cleans and deduplicates raw data (refined), then computes risk scores (curated).")
    show_table(spark["recent_jobs"] if spark else None, spark_error,
               "No Spark job has run since the cluster started.")

    st.subheader("5 · HDFS refined and curated layers")
    st.caption("Refined = cleaned Parquet. Curated = analyst-ready risk scores. 'Last write' = when Spark last ran.")
    left, right = st.columns(2)
    with left:
        st.markdown("**Refined**")
        show_table(refined, refined_error)
    with right:
        st.markdown("**Curated**")
        show_table(curated, curated_error)
