"""Governance page: cleaning funnel, integrity and privacy checks, and who can see what."""
import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard_page import load, to_local

STATUS_LABEL = {"pass": "✅ pass", "fail": "❌ fail", "warning": "⚠️ warning"}
FUNNEL_DATASETS = ["orders", "returns", "complaints"]
ACCESS_MATRIX = pd.DataFrame([
    {"data": "Seller name, region, risk label", "Trust & Safety Analyst": "✅", "Business Analyst": "✅"},
    {"data": "Risk scores, return and complaint rates", "Trust & Safety Analyst": "✅", "Business Analyst": "❌"},
    {"data": "Fraud patterns and risk trend", "Trust & Safety Analyst": "✅", "Business Analyst": "❌"},
    {"data": "Customer activity (masked IDs)", "Trust & Safety Analyst": "✅", "Business Analyst": "❌"},
    {"data": "Real customer IDs", "Trust & Safety Analyst": "❌", "Business Analyst": "❌"},
    {"data": "Event producer, pipeline monitor and runs", "Trust & Safety Analyst": "✅", "Business Analyst": "❌"},
])


def governance_page():
    st.header("🛡️ Data Governance")
    if st.button("🔄 Reload from HDFS", key="reload_governance"):
        st.cache_data.clear()
    try:
        dq = load("/data/curated/data_quality")
    except Exception as error:
        st.error(f"HDFS unreachable: {error}")
        return
    if dq.empty:
        st.info("No checks yet. Run the pipeline: its last job, governance, writes them.")
        return

    dq = dq.assign(checked_at=to_local(dq["checked_at"]))
    latest = dq[dq["checked_at"] == dq["checked_at"].max()]
    st.caption(f"Source: HDFS /data/curated/data_quality · checked {latest['checked_at'].max():%Y-%m-%d %H:%M} "
               "· written by the Spark job governance.py")

    # 1. cleaning funnel
    st.subheader("1 · Cleaning funnel: raw → refined")
    st.caption("Raw keeps every message as received (audit trail). Spark removes duplicates and invalid rows "
               "before any analysis.")
    metrics = latest.pivot_table(index="dataset", columns="metric", values="value", aggfunc="first")
    for column, dataset in zip(st.columns(len(FUNNEL_DATASETS)), FUNNEL_DATASETS):
        if dataset not in metrics.index:
            continue
        m = metrics.loc[dataset]
        stages = pd.DataFrame({
            "stage": ["received in raw", "after removing duplicates", "clean in refined"],
            "rows": [m["raw_rows"], m["raw_rows"] - m["duplicates_removed"], m["refined_rows"]],
        })
        fig = px.funnel(stages, x="rows", y="stage", title=dataset)
        fig.update_layout(height=260, margin=dict(l=10, r=10, t=40, b=10))
        column.plotly_chart(fig, key=f"funnel_{dataset}")
        share = m["duplicates_removed"] / m["raw_rows"] if m["raw_rows"] else 0
        column.caption(f"{int(m['duplicates_removed']):,} duplicates ({share:.0%}) · "
                       f"{int(m['invalid_removed']):,} invalid rows removed")

    # 2. pass / fail checks
    st.subheader("2 · Integrity and privacy checks")
    checks = latest[latest["status"] != "info"]
    st.dataframe(checks.assign(result=checks["status"].map(STATUS_LABEL))[
                     ["dataset", "metric", "value", "result", "explanation"]], hide_index=True)
    failed = int((checks["status"] == "fail").sum())
    if failed:
        st.error(f"{failed} check(s) failed.")
    else:
        st.success("All blocking checks passed. Warnings are reported but don't stop the pipeline.")

    # 3. customer privacy
    st.subheader("3 · Customer privacy")
    st.markdown("- Real customer IDs exist only in HDFS raw and refined, which no screen reads.\n"
                "- Spark replaces them with a **salted SHA-256 hash** before writing the curated layer.\n"
                "- The `unmasked_customer_ids` check above proves that no real ID reached curated.")
    sample = load("/data/curated/customer_activity")
    if not sample.empty:
        st.code("\n".join(f"C??? → {h[:16]}…" for h in sample["customer_masked"].head(3)), language="text")

    # 4. role-based access
    st.subheader("4 · Role-based access")
    st.dataframe(ACCESS_MATRIX, hide_index=True)
    st.caption("Passwords are stored as SHA-256 hashes, never in plain text. In production, a slow salted hash "
               "(bcrypt or Argon2) and a real identity provider would replace this demo login.")
