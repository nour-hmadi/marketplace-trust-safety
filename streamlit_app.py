import hashlib
from datetime import datetime

import requests
import streamlit as st

from monitor_page import pipeline_monitor_page

st.set_page_config(page_title="Trust & Safety Platform", page_icon="🛡️", layout="wide")

API = "http://localhost:5000/api"

HASHES = {
    "Trust & Safety Analyst": "660a821bf16baed1e1231b7a4c87ed28b9bbffe00814890d7716bf9d40a3b3a7",
    "Business Analyst": "8e092573de6132d196649bf1c347bab309c29fe4b25c73dc61accee5ee877b37",
}

SELLERS = {
    "S55": "TechZone Store", "S56": "QuickDeals", "S57": "MegaSupplies", "S58": "BudgetFinds",
    "S59": "LuxuryGoods", "S60": "FastShip Co", "S61": "CheapGoods", "S62": "PremiumHub",
}
SELLER_OPTIONS = [f"{sid} - {name}" for sid, name in SELLERS.items()]


def sha256(text):
    return hashlib.sha256(text.encode()).hexdigest()


def seller_id(option):
    return option.split(" - ")[0]


# ---------- LOGIN ----------
def login_page():
    st.title("🛡️ Trust & Safety Platform")
    st.caption("Secure access portal. Your role decides what data you can see.")
    role = st.selectbox("Role", list(HASHES.keys()))
    password = st.text_input("Password", type="password")
    if st.button("Access Platform"):
        if sha256(password) == HASHES[role]:
            st.session_state["role"] = role
            st.rerun()
        else:
            st.error("Invalid password. Please try again.")


# ---------- EVENT PRODUCER ----------
def send_event(endpoint, payload):
    try:
        response = requests.post(f"{API}/{endpoint}", json=payload, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as error:
        st.error(f"Flask API not reachable: {error}")
        return None


def log_event(event_type, result):
    event = result["event"]
    event_id = event[{"ORDER": "order_id", "RETURN": "return_id", "COMPLAINT": "complaint_id"}[event_type]]
    st.session_state.setdefault("events", []).insert(0, {
        "time": datetime.now().strftime("%H:%M:%S"),
        "type": event_type,
        "event_id": event_id,
        "seller": event["seller_id"],
        "kafka": "✅ sent" if result["kafka_sent"] else "❌ failed",
    })


def event_producer_page():
    st.header("📡 Event Producer")
    st.caption("Streamlit → Flask API → Kafka topic. Airflow moves new messages to HDFS raw every 10 minutes.")
    st.link_button("Open Kafka UI", "http://localhost:8080")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.subheader("📦 Order")
        with st.form("order_form"):
            seller = st.selectbox("Seller", SELLER_OPTIONS, key="order_seller")
            amount = st.number_input("Amount ($)", min_value=1.0, max_value=9999.0, value=150.0, key="order_amount")
            category = st.selectbox("Category", ["electronics", "clothing", "luxury", "home_goods", "sports"], key="order_category")
            status = st.selectbox("Status", ["completed", "cancelled", "pending"], key="order_status")
            if st.form_submit_button("Send to topic: orders"):
                result = send_event("order", {"seller_id": seller_id(seller), "amount": amount,
                                              "category": category, "status": status})
                if result:
                    log_event("ORDER", result)

    with col2:
        st.subheader("↩️ Return")
        with st.form("return_form"):
            seller = st.selectbox("Seller", SELLER_OPTIONS, key="return_seller")
            reason = st.selectbox("Reason", ["counterfeit", "damaged_item", "wrong_item",
                                             "not_as_described", "changed_mind"], key="return_reason")
            if st.form_submit_button("Send to topic: returns"):
                result = send_event("return", {"seller_id": seller_id(seller), "reason": reason})
                if result:
                    log_event("RETURN", result)

    with col3:
        st.subheader("⚠️ Complaint")
        with st.form("complaint_form"):
            seller = st.selectbox("Seller", SELLER_OPTIONS, key="complaint_seller")
            severity = st.selectbox("Severity", ["high", "medium", "low"], key="complaint_severity")
            category = st.selectbox("Category", ["counterfeit_claim", "fraud", "late_delivery",
                                                 "wrong_item", "poor_quality"], key="complaint_category")
            if st.form_submit_button("Send to topic: complaints"):
                result = send_event("complaint", {"seller_id": seller_id(seller), "severity": severity,
                                                  "category": category})
                if result:
                    log_event("COMPLAINT", result)

    st.subheader("Event log (this session)")
    events = st.session_state.get("events", [])
    if events:
        st.dataframe(events, hide_index=True)
    else:
        st.info("No events sent yet.")


# ---------- MAIN ----------
def main_page():
    role = st.session_state["role"]
    is_ts = role == "Trust & Safety Analyst"

    st.sidebar.title("🛡️ Platform")
    st.sidebar.write(f"Logged in as: **{role}**")
    if st.sidebar.button("Sign out"):
        st.session_state.clear()
        st.rerun()

    st.title("🛡️ Trust & Safety Analytics Platform")

    if is_ts:
        st.success("Full access: risk scores, fraud patterns, customer investigation, event producer.")
        page = st.sidebar.radio("Navigate", ["🔄 Pipeline Monitor", "📡 Event Producer"])
        if page == "🔄 Pipeline Monitor":
            pipeline_monitor_page()
        elif page == "📡 Event Producer":
            event_producer_page()
    else:
        st.info("Restricted access: seller names, regions and risk labels only. Your dashboard arrives in Phase 6.")


if "role" not in st.session_state:
    login_page()
else:
    main_page()
