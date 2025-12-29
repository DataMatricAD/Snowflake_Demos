import streamlit as st

def get_session():
    """
    Works locally (VS Code) using Streamlit secrets.
    In Snowflake Streamlit, you can swap this to get_active_session().
    """
    conn = st.secrets["connections"]["snowflake"]
    from snowflake.snowpark import Session  # <-- correct Session
    return Session.builder.configs({
        "account": conn["account"],
        "user": conn["user"],
        "password": conn["password"],
        "role": conn.get("role"),
        "warehouse": conn.get("warehouse"),
        "database": conn.get("database"),
        "schema": conn.get("schema"),
    }).create()
