"""Risk Dashboard: curated risk scores read live from HDFS, filtered by role (column-level access)."""
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st

from data_access import read_parquet_folder

LABEL_COLORS = {"HIGH": "#e74c3c", "MEDIUM": "#f39c12", "LOW": "#27ae60"}
LABEL_ORDER = ["HIGH", "MEDIUM", "LOW"]
BUSINESS_COLUMNS = ["seller_name", "region", "risk_label"]   # everything a Business Analyst may see
LOCAL_TZ = datetime.now().astimezone().tzinfo


@st.cache_data(ttl=30, show_spinner="Reading curated data from HDFS…")
def load(folder):
    return read_parquet_folder(folder)


def to_local(series):
    """Spark writes timestamps in UTC; show them in the laptop's time zone."""
    times = pd.to_datetime(series)
    if times.dt.tz is None:
        times = times.dt.tz_localize("UTC")
    return times.dt.tz_convert(LOCAL_TZ)


def analyst_view(risk):
    st.subheader("Seller risk ranking")
    st.dataframe(
        risk[["seller_id", "seller_name", "region", "verification_status", "total_orders", "total_returns",
              "return_rate", "total_complaints", "high_severity_complaints", "risk_score", "risk_index",
              "risk_label"]],
        hide_index=True,
        column_config={
            "return_rate": st.column_config.NumberColumn("return rate", format="percent"),
            "risk_score": st.column_config.NumberColumn("raw score", format="%.0f"),
            "risk_index": st.column_config.ProgressColumn("risk index (0–100)", min_value=0, max_value=100,
                                                          format="%.1f"),
        },
    )

    st.subheader("🚨 Fraud patterns")
    patterns = load("/data/curated/fraud_patterns")
    if patterns.empty:
        st.success("No seller crosses a fraud threshold.")
    else:
        names = risk.set_index("seller_id")["seller_name"]
        patterns = patterns.assign(seller=patterns["seller_id"].map(names))
        st.dataframe(
            patterns[["seller", "pattern_type", "indicator_value", "threshold", "explanation"]],
            hide_index=True,
            column_config={
                "indicator_value": st.column_config.NumberColumn("measured", format="percent"),
                "threshold": st.column_config.NumberColumn("limit", format="percent"),
            },
        )

    st.subheader("📈 Risk trend per pipeline run")
    history = load("/data/curated/risk_history")
    if history.empty:
        st.info("No history yet. Each pipeline run adds one point per seller.")
    else:
        history = history.assign(scored_at=to_local(history["scored_at"])).sort_values("scored_at")
        history = history.assign(run=history["scored_at"].dt.strftime("%b %d, %H:%M:%S"))   # one label per run
        fig = px.line(history, x="run", y="risk_index", color="seller_name", markers=True,
                      labels={"run": "pipeline run", "risk_index": "risk index", "seller_name": "seller"})
        fig.update_xaxes(type="category")
        fig.add_hline(y=55, line_dash="dot", annotation_text="HIGH ≥ 55")
        fig.add_hline(y=30, line_dash="dot", annotation_text="MEDIUM ≥ 30")
        st.plotly_chart(fig)
        st.caption(f"{history['scored_at'].nunique()} pipeline run(s) recorded in /data/curated/risk_history.")

    st.subheader("🗺️ Average risk by region")
    region = (risk.groupby("region")
              .agg(average_risk_index=("risk_index", "mean"), sellers=("seller_id", "count"))
              .reset_index().sort_values("average_risk_index", ascending=False))
    region = region.assign(level=pd.cut(region["average_risk_index"], bins=[-1, 30, 55, 101], right=False,
                                         labels=["LOW", "MEDIUM", "HIGH"]).astype(str))
    st.plotly_chart(px.bar(region, x="region", y="average_risk_index", color="level", text_auto=".1f",
                           hover_data=["sellers"], color_discrete_map=LABEL_COLORS,
                           category_orders={"level": LABEL_ORDER}))


def business_view(risk):
    visible = risk[BUSINESS_COLUMNS]   # column-level access control: other columns never reach the screen
    order = visible["risk_label"].map({label: i for i, label in enumerate(LABEL_ORDER)})
    visible = visible.assign(_order=order).sort_values(["_order", "seller_name"]).drop(columns="_order")

    st.subheader("Sellers by risk level")
    st.dataframe(visible, hide_index=True)

    st.subheader("🗺️ Risk levels by region")
    by_region = visible.groupby(["region", "risk_label"]).size().reset_index(name="sellers")
    st.plotly_chart(px.bar(by_region, x="region", y="sellers", color="risk_label",
                           color_discrete_map=LABEL_COLORS, category_orders={"risk_label": LABEL_ORDER}))


def dashboard_page(is_ts):
    st.header("📊 Risk Dashboard")
    if st.button("🔄 Reload from HDFS"):
        st.cache_data.clear()
    try:
        risk = load("/data/curated/seller_risk")
    except Exception as error:
        st.error(f"HDFS unreachable: {error}")
        return
    if risk.empty:
        st.info("No curated data yet. Run the pipeline first.")
        return

    risk = risk.sort_values("risk_index", ascending=False)
    if "scored_at" in risk:
        st.caption(f"Source: HDFS /data/curated/seller_risk · scored {to_local(risk['scored_at']).max():%Y-%m-%d %H:%M}")

    counts = risk["risk_label"].value_counts()
    columns = st.columns(4)
    columns[0].metric("Sellers", len(risk))
    for column, label in zip(columns[1:], LABEL_ORDER):
        column.metric(f"{label} risk", int(counts.get(label, 0)))

    if is_ts:
        analyst_view(risk)
        investigation_view(risk)
    else:
        business_view(risk)


def investigation_view(risk):
    st.subheader("🔍 Seller investigation")
    st.caption("Customers of one seller, from /data/curated/customer_activity. Customer IDs are masked in Spark "
               "with a salted SHA-256 hash, so the real IDs never reach this screen.")
    names = risk.set_index("seller_id")["seller_name"]
    seller = st.selectbox("Seller", risk["seller_id"].tolist(),          # riskiest seller first
                          format_func=lambda sid: f"{sid} · {names[sid]}")
    activity = load("/data/curated/customer_activity")
    customers = activity[activity["seller_id"] == seller] if not activity.empty else activity
    if customers.empty:
        st.info("No customer activity for this seller yet. Run the pipeline first.")
        return
    customers = customers.sort_values(["counterfeit_returns", "returns"], ascending=False)
    suspicious = customers[(customers["return_rate"] >= 0.5) | (customers["counterfeit_returns"] > 0)]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Customers", len(customers))
    c2.metric("Orders", int(customers["orders"].sum()))
    c3.metric("Counterfeit returns", int(customers["counterfeit_returns"].sum()))
    c4.metric("Suspicious customers", len(suspicious))

    def flag(row):
        if row.counterfeit_returns > 0:
            return "🚨 counterfeit returns"
        return "⚠️ returns half of orders" if row.return_rate >= 0.5 else ""

    shown = customers.assign(customer=customers["customer_masked"].str[:12] + "…",
                             flag=customers.apply(flag, axis=1))
    st.dataframe(shown[["customer", "orders", "total_spent", "returns", "counterfeit_returns", "return_rate", "flag"]],
                 hide_index=True,
                 column_config={"total_spent": st.column_config.NumberColumn("total spent", format="dollar"),
                                "return_rate": st.column_config.NumberColumn("return rate", format="percent")})
    st.caption("Only returns linked to an order of this seller appear here. Unlinked (orphan) returns are a "
               "data-quality issue, reported on the Governance page.")
