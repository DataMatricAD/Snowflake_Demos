# This code is designed to create a Streamlit application that allows users to analyze and optimize 
# Snowflake SQL queries. It includes the following features:
import os
import streamlit as st
from snowflake.snowpark import Session
from openai import OpenAI
import re

@st.cache_resource(show_spinner="Connecting...")
def getSession():
    account = st.secrets["connections"]["snowflake"]["account"]
    user = st.secrets["connections"]["snowflake"].get("user")
    password = os.environ.get("SNOWFLAKE_PASSWORD")

    if not all([account, user, password]):
        raise RuntimeError("Snowflake credentials not fully configured")

    return Session.builder.configs({
        "account": account,
        "user": user,
        "password": password
    }).create()


def getChatResponse(prompt):
    client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}]
    )
    return response.choices[0].message.content


st.title("Query Analyzer and Optimizer")
st.write("Analyze and optimize Snowflake SQL queries for corectness and performance.")

session = getSession()
tabs = st.tabs(["Query", "Plan", "Description", "Comments", "Optimization", "Encapsulation"])

# Snowflake-based menus
with tabs[0]:
    query = "select count(*) from snowflake_sample_data.tpch_sf1.lineitem"
    query = st.text_area("Query:", query, label_visibility="hidden")
    st.dataframe(session.sql(query).collect())

with tabs[1]:
    explain = f"EXPLAIN USING TABULAR\n{query}"
    st.code(explain, language="sql")
    st.dataframe(session.sql(explain).collect())

# ChatGPT-based menus
q = f"the Snowflake SQL query \n\n```\n{query}\n```"

with tabs[2]:
    st.write(getChatResponse(f"explain {q}"))

with tabs[3]:
    response = getChatResponse(f"comment {q}")
    if sql_match := re.search(r"```sql\n(.*)\n```", response, re.DOTALL):
        st.code(sql_match.group(1), language="sql")
    else: st.write(response)

with tabs[4]:
    st.write(getChatResponse(f"optimize {q}"))

with tabs[5]:
    response = getChatResponse(f"create a stored procedure with {q}")
    if sql_match := re.search(r"```sql\n(.*)\n```", response, re.DOTALL):
        st.code(sql_match.group(1), language="sql")
    else: st.write(response)
