import os
from snowflake.snowpark import Session
import streamlit as st

def getSession():
    return Session.builder.configs({
        "account": "jtowmqn-hk67409",   # <-- from your Snowflake URL
        "user": "ABHIJITSNOWDEMO",
        "password": "**************",
        "role": os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "database": "SNOWFLAKE",
        "schema": "ACCOUNT_USAGE",
    }).create()


st.title("Usage Monitoring Dashboard")

name = "Credit_Usage_Query"
query = open(f"queries/{name}.sql", "r").read()
df = getSession().sql(query).to_pandas()
import charts.Credit_Usage_Query as cuq
fig = cuq.getChart(df)
st.plotly_chart(fig)

name = "Storage_Usage_Query"
query = open(f"queries/{name}.sql", "r").read()
df = getSession().sql(query).to_pandas()
import charts.Storage_Usage_Query as suq
fig = suq.getChart(df)
st.plotly_chart(fig)

name = "Longest_Running_Queries"
query = open(f"queries/{name}.sql", "r").read()
df = getSession().sql(query).to_pandas()
import charts.Longest_Running_Queries as lrq
fig = lrq.getChart(df)
st.plotly_chart(fig)
