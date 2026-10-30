"""Run Pipeline page: trigger Airflow's processing DAG and watch each Spark job live."""
import streamlit as st

import pipeline_status as ps

ACTIVE_STATES = ("queued", "running")
STATE_LABEL = {"success": "✅ success", "running": "⏳ running", "queued": "🕒 queued",
               "scheduled": "🕒 scheduled", "failed": "❌ failed", "up_for_retry": "🔁 retrying",
               "upstream_failed": "⛔ skipped (earlier job failed)"}
JOB_INFO = {
    "normalize_orders": "Raw orders → refined: fix types, lowercase text, drop invalid amounts, remove duplicates",
    "normalize_returns": "Raw returns → refined: clean and remove duplicate returns",
    "normalize_complaints": "Raw complaints → refined: clean and remove duplicates",
    "normalize_sellers": "Seller master data → refined",
    "seller_aggregates": "Join orders, returns, complaints and sellers → metrics per seller",
    "risk_scoring": "Risk index 0–100, HIGH/MEDIUM/LOW label and fraud patterns → curated",
    "customer_activity": "Customers per seller with salted SHA-256 masked IDs → curated (investigations)",
    "governance": "Data-quality funnel, integrity and privacy checks → curated/data_quality",
}


def curated_last_write():
    files = ps.hdfs_files("/data/curated/seller_risk")
    return files[0]["written"] if files else "-"


def progress_panel(run_id, was_active):
    try:
        run = ps.dag_run(ps.PROCESSING_DAG, run_id)
        tasks = ps.task_instances(ps.PROCESSING_DAG, run_id)
    except Exception as error:
        st.error(f"Airflow unreachable: {error}")
        return

    st.caption(f"Started {run['started']} · ended {run['ended']}")
    total = len(ps.PROCESSING_JOBS)
    done = sum(1 for job in ps.PROCESSING_JOBS if tasks.get(job, {}).get("state") == "success")
    st.progress(done / total, text=f"{done}/{total} Spark jobs done · run state: {run['state']}")

    rows = []
    for step, job in enumerate(ps.PROCESSING_JOBS, start=1):
        task = tasks.get(job, {})
        duration = task.get("duration")
        rows.append({"step": step, "Spark job": job,
                     "status": STATE_LABEL.get(task.get("state"), "⚪ waiting"),
                     "seconds": round(duration) if duration else None,
                     "what it does": JOB_INFO[job]})
    st.dataframe(rows, hide_index=True)

    if run["state"] == "success":
        st.success(f"Pipeline finished. Curated risk scores rewritten at {curated_last_write()}.")
    elif run["state"] == "failed":
        st.error("Pipeline failed. Open the log of the failed job below.")

    if was_active and run["state"] not in ACTIVE_STATES:
        st.rerun()   # the run just finished: refresh the whole page once and stop polling


def log_viewer(run_id):
    with st.expander("📜 Airflow log of one job"):
        job = st.selectbox("Job", ps.PROCESSING_JOBS, key="log_job")
        try:
            task = ps.task_instances(ps.PROCESSING_DAG, run_id).get(job)
            if not task or not task.get("start_date"):
                st.info("This job has not started yet.")
                return
            text = ps.task_log(ps.PROCESSING_DAG, run_id, job, max(task.get("try_number") or 1, 1))
            st.code("\n".join(text.splitlines()[-30:]), language="text")
        except Exception as error:
            st.warning(f"Log not available: {error}")


def run_pipeline_page():
    st.header("⚡ Run Pipeline")
    st.caption("Streamlit → Airflow DAG `seller_risk_pipeline` → Flask Spark gateway → Spark → HDFS refined & curated. "
               "Airflow controls the order and the retries; Spark does the processing.")
    try:
        st.markdown(f"**Curated risk scores last written:** {curated_last_write()}")
    except Exception:
        st.markdown("**Curated risk scores last written:** HDFS unreachable")

    if st.button("⚡ Run the full pipeline now", type="primary"):
        try:
            st.session_state["pipeline_run"] = ps.trigger_dag(ps.PROCESSING_DAG)
        except Exception as error:
            st.error(f"Could not start the pipeline: {error}")

    try:
        run_id = st.session_state.get("pipeline_run") or ps.latest_run_id(ps.PROCESSING_DAG)
        if not run_id:
            st.info("The pipeline has never run. Click the button above.")
            return
        run = ps.dag_run(ps.PROCESSING_DAG, run_id)
    except Exception as error:
        st.error(f"Airflow unreachable: {error}")
        return

    st.subheader(f"Run: {run_id}")
    active = run["state"] in ACTIVE_STATES
    st.fragment(run_every="3s" if active else None)(progress_panel)(run_id, active)
    log_viewer(run_id)
