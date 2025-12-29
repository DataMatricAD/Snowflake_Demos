import os
import streamlit as st
from snowflake.snowpark import Session

st.set_page_config(page_title="Usage Monitoring Dashboard", layout="wide")
st.title("Usage Monitoring Dashboard")

@st.cache_resource
def getSession():
    password = os.getenv("SNOWFLAKE_PASSWORD")
    if not password:
        raise ValueError("SNOWFLAKE_PASSWORD environment variable is not set.")

    return Session.builder.configs({
        # From your Snowflake URL:
        "account": "jt*****-hk****",
        "user": "ABHIJITSNOWDEMO",
        "password": "************",

        # Recommended for ACCOUNT_USAGE
        "role": os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),

        "database": "SNOWFLAKE",
        "schema": "ACCOUNT_USAGE",
    }).create()

def read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

session = getSession()

sels = ["Credit Usage Query", "Storage Usage Query", "Longest Running Queries"]
sel = st.sidebar.selectbox("Select a chart type:", sels, index=0)
name = sel.replace(" ", "_")

tabChart, tabQuery, tabCode = st.tabs(["Chart", "Query", "Code"])

query = read_text(f"queries/{name}.sql")
tabQuery.code(query, language="sql")

code = read_text(f"charts/{name}.py")
tabCode.code(code, language="python")

df = session.sql(query).to_pandas()

if name == "Credit_Usage_Query":
    import charts.Credit_Usage_Query as cuq
    fig = cuq.getChart(df)
elif name == "Storage_Usage_Query":
    import charts.Storage_Usage_Query as suq
    fig = suq.getChart(df)
elif name == "Longest_Running_Queries":
    import charts.Longest_Running_Queries as lrq
    fig = lrq.getChart(df)
else:
    raise ValueError(f"Unknown chart type: {name}")

tabChart.plotly_chart(fig, use_container_width=True)
