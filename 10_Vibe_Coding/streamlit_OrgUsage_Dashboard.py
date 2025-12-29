# Abhijit Das - 12/24/2025
# Fixed: works in BOTH Snowflake Native Streamlit and local Streamlit (VS Code)
# - Uses get_active_session() when available
# - Falls back to creating a Snowpark Session via st.secrets when running locally
# - Removes any accidental pytest import usage

import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta, date

from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Session as SnowparkSession


# -----------------------------
# Page configuration
# -----------------------------
st.set_page_config(
    page_title="Snowflake Organization Usage Dashboard",
    page_icon="❄️",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #29B5E8;
        text-align: center;
        margin-bottom: 1rem;
    }
    </style>
""", unsafe_allow_html=True)


# -----------------------------
# Session handling (Native + Local)
# -----------------------------
def _read_snowflake_secrets() -> dict:
    """
    Supports Streamlit secrets style:
      [connections.snowflake]
      account = "..."
      user = "..."
      password = "..."
      role = "..."
      warehouse = "..."
    """
    # Typical Streamlit TOML access for [connections.snowflake]
    if "connections" in st.secrets and "snowflake" in st.secrets["connections"]:
        return dict(st.secrets["connections"]["snowflake"])

    # Some people keep it flattened
    if "connections.snowflake" in st.secrets:
        return dict(st.secrets["connections.snowflake"])

    return {}


@st.cache_resource(show_spinner=False)
def get_snowflake_session() -> SnowparkSession:
    """
    1) Try Snowflake Native Streamlit session
    2) If not found (local run), create a Snowpark session from secrets
    """
    # Try Native Streamlit session
    try:
        s = get_active_session()
        if s is not None:
            return s
    except Exception:
        pass

    # Local fallback
    cfg = _read_snowflake_secrets()
    missing = [k for k in ["account", "user", "password"] if not cfg.get(k)]
    if missing:
        raise RuntimeError(
            "No active Snowflake session (Native Streamlit) AND missing local secrets. "
            f"Add these keys to .streamlit/secrets.toml under [connections.snowflake]: {missing}"
        )

    # Only pass known Snowpark keys if present
    allowed_keys = {"account", "user", "password", "role", "warehouse", "database", "schema", "region"}
    snowpark_cfg = {k: v for k, v in cfg.items() if k in allowed_keys and v}

    return SnowparkSession.builder.configs(snowpark_cfg).create()


session = get_snowflake_session()


# -----------------------------
# Data loading
# -----------------------------
@st.cache_data(ttl=3600, show_spinner=False)
def load_usage_data() -> pd.DataFrame:
    query = """
    SELECT
        ORGANIZATION_NAME,
        CONTRACT_NUMBER,
        ACCOUNT_NAME,
        ACCOUNT_LOCATOR,
        REGION,
        SERVICE_LEVEL,
        USAGE_DATE,
        USAGE_TYPE,
        USAGE,
        CURRENCY,
        USAGE_IN_CURRENCY,
        BALANCE_SOURCE,
        BILLING_TYPE,
        RATING_TYPE,
        SERVICE_TYPE,
        IS_ADJUSTMENT
    FROM SNOWFLAKE.ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY
    ORDER BY USAGE_DATE DESC
    """
    df = session.sql(query).to_pandas()
    df["USAGE_DATE"] = pd.to_datetime(df["USAGE_DATE"])
    return df


# -----------------------------
# UI
# -----------------------------
st.markdown('<div class="main-header">❄️ Snowflake Organization Usage Dashboard</div>', unsafe_allow_html=True)

try:
    with st.spinner("Loading usage data..."):
        df = load_usage_data()

    if df.empty:
        st.warning("No data available in the usage view.")
        st.stop()

    # Sidebar filters
    st.sidebar.header("🔍 Filters")

    min_date = df["USAGE_DATE"].min()
    max_date = df["USAGE_DATE"].max()

    selected_org = st.sidebar.selectbox("Organization", sorted(df["ORGANIZATION_NAME"].unique()))
    selected_account = st.sidebar.selectbox("Account", sorted(df[df["ORGANIZATION_NAME"] == selected_org]["ACCOUNT_NAME"].unique()))
    
    # Display summary
    st.subheader("Usage Summary")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Usage", f"${df['USAGE_IN_CURRENCY'].sum():,.2f}")
    with col2:
        st.metric("Organizations", df["ORGANIZATION_NAME"].nunique())
    with col3:
        st.metric("Accounts", df["ACCOUNT_NAME"].nunique())

except Exception as e:
    st.error(f"Error loading data: {str(e)}")
    st.stop()
