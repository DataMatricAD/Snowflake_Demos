import streamlit as st
from snowflake.snowpark import Session

# Page config (safe to keep here for multi-page apps)
st.set_page_config(page_title="Usage Monitoring Dashboard", layout="wide")


# -----------------------------
# Snowflake session (Streamlit-native)
# -----------------------------
@st.cache_resource
def getSession() -> Session:
    """
    Returns a cached Snowpark Session using Streamlit's
    [connections.snowflake] configuration from secrets.toml
    """
    return st.connection("snowflake").session()


# -----------------------------
# File reader utility
# -----------------------------
def _read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


# -----------------------------
# Chart renderer
# -----------------------------
def showChart(name: str):
    st.title("Usage Monitoring Dashboard")

    tabChart, tabQuery, tabCode = st.tabs(["Chart", "Query", "Code"])

    # Load SQL
    query = _read_text(f"queries/{name}.sql")
    tabQuery.code(query, language="sql")

    # Load chart code
    code = _read_text(f"charts/{name}.py")
    tabCode.code(code, language="python")

    # Run query
    session = getSession()
    df = session.sql(query).to_pandas()

    # Render chart dynamically
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
        raise ValueError(f"Unknown chart name: {name}")

    tabChart.plotly_chart(fig, use_container_width=True)
