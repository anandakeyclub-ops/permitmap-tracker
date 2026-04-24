from __future__ import annotations

from pathlib import Path
import pandas as pd
import streamlit as st

base_folder = Path(__file__).resolve().parent.parent

st.set_page_config(page_title="Permit Funnel Dashboard", layout="wide")
st.title("Permit Funnel Dashboard")

contractors_path = base_folder / "config" / "contractors_master.csv"
sent_log_path = base_folder / "logs" / "sent_emails.csv"
replies_log_path = base_folder / "logs" / "replies_detected.csv"


def safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()

    try:
        return pd.read_csv(path, dtype=str, keep_default_na=False)
    except pd.errors.ParserError:
        # fallback: skip malformed rows so dashboard still loads
        return pd.read_csv(
            path,
            dtype=str,
            keep_default_na=False,
            engine="python",
            on_bad_lines="skip",
        )


contractors = safe_read_csv(contractors_path)
sent_log = safe_read_csv(sent_log_path)
replies_log = safe_read_csv(replies_log_path)

if not contractors.empty:
    contractors.columns = [c.strip() for c in contractors.columns]

# Basic metrics
total_contractors = len(contractors) if not contractors.empty else 0
prospects = (contractors["status"].str.lower() == "prospect").sum() if "status" in contractors.columns else 0
active_clients = (contractors["status"].str.lower() == "active").sum() if "status" in contractors.columns else 0
active_paid = (contractors["billing_status"].str.lower() == "active_paid").sum() if "billing_status" in contractors.columns else 0

# Funnel metrics
first_outreach_sent_count = (contractors["first_outreach_sent"].str.upper() == "TRUE").sum() if "first_outreach_sent" in contractors.columns else 0
replied_count = contractors["last_response_date"].astype(str).str.strip().ne("").sum() if "last_response_date" in contractors.columns else 0
interested_count = (contractors["current_stage"].str.lower() == "interested").sum() if "current_stage" in contractors.columns else 0
closed_count = active_paid

reply_rate = (replied_count / first_outreach_sent_count * 100) if first_outreach_sent_count else 0
close_rate = (closed_count / replied_count * 100) if replied_count else 0

# Revenue
def get_monthly_value(row):
    if str(row.get("billing_status", "")).strip().lower() == "active_paid":
        return 99
    return 0

if not contractors.empty:
    contractors["monthly_value"] = contractors.apply(get_monthly_value, axis=1)
    estimated_mrr = contractors["monthly_value"].sum()
else:
    estimated_mrr = 0

if not contractors.empty and {"state", "county", "monthly_value"}.issubset(contractors.columns):
    county_revenue = (
        contractors.groupby(["state", "county"], dropna=False)["monthly_value"]
        .sum()
        .reset_index()
        .sort_values("monthly_value", ascending=False)
    )
else:
    county_revenue = pd.DataFrame()

if not contractors.empty and "current_stage" in contractors.columns:
    stage_breakdown = (
        contractors["current_stage"]
        .replace("", "blank")
        .value_counts()
        .reset_index()
    )
    stage_breakdown.columns = ["stage", "count"]
else:
    stage_breakdown = pd.DataFrame()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Contractors", total_contractors)
col2.metric("Prospects", prospects)
col3.metric("Active Paid Clients", active_paid)
col4.metric("Estimated MRR", f"${estimated_mrr:,.0f}")

col5, col6, col7 = st.columns(3)
col5.metric("First Outreach Sent", first_outreach_sent_count)
col6.metric("Reply Rate", f"{reply_rate:.1f}%")
col7.metric("Close Rate", f"{close_rate:.1f}%")

st.subheader("Revenue by County")
if not county_revenue.empty:
    st.dataframe(county_revenue, use_container_width=True)
else:
    st.info("No county revenue data yet.")

st.subheader("Stage Breakdown")
if not stage_breakdown.empty:
    st.dataframe(stage_breakdown, use_container_width=True)
else:
    st.info("No stage data yet.")

st.subheader("Contractors")
if not contractors.empty:
    cols = [
        c for c in [
            "name", "email", "trade", "state", "county", "status",
            "billing_status", "current_stage", "next_action",
            "first_outreach_sent", "last_response_date", "monthly_value"
        ] if c in contractors.columns
    ]
    st.dataframe(contractors[cols], use_container_width=True)
else:
    st.info("No contractor data found.")

st.subheader("Recent Sends")
if not sent_log.empty:
    st.dataframe(sent_log.tail(50), use_container_width=True)
else:
    st.info("No sent email log yet.")

st.subheader("Detected Replies")
if not replies_log.empty:
    st.dataframe(replies_log.tail(50), use_container_width=True)
else:
    st.info("No reply log yet.")
