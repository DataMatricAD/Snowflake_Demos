import os
import streamlit as st
from snowflake.snowpark import Session

st.set_page_config(page_title="Usage Monitoring Dashboard", layout="wide")
st.title("Usage Monitoring Dashboard")

@st.cache_resource
def getSession():
    password = os.getenv("SNOWFLAKE_PASSWORD")
    if not password:
        raise ValueError("SNOWFLAKE_PASSWORD environment variable is not set")

    return Session.builder.configs({
        # From your Snowflake URL:
        "account": "jt*****-hk*****",
        "user": "ABHIJITSNOWDEMO",
        "password": "***************",

        # Required for ACCOUNT_USAGE
        "role": os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),

        "database": "SNOWFLAKE",
        "schema": "ACCOUNT_USAGE",
    }).create()

def run_query(session: Session, name: str):
    with open(f"queries/{name}.sql", "r", encoding="utf-8") as f:
        return session.sql(f.read()).to_pandas()

session = getSession()

tabs = st.tabs(["Credit Usage", "Storage Usage", "Longest Running Queries"])

# --- Credit Usage ---
df = run_query(session, "Credit_Usage_Query")
import charts.Credit_Usage_Query as cuq
tabs[0].plotly_chart(cuq.getChart(df), use_container_width=True)

# --- Storage Usage ---
df = run_query(session, "Storage_Usage_Query")
import charts.Storage_Usage_Query as suq
tabs[1].plotly_chart(suq.getChart(df), use_container_width=True)

# --- Longest Running Queries ---
df = run_query(session, "Longest_Running_Queries")
import charts.Longest_Running_Queries as lrq
tabs[2].plotly_chart(lrq.getChart(df), use_container_width=True)
