# cortex_tools.py
# This uses Cortex Search as your RAG retriever
# the LLM will then see those chunks as context.

import json
import streamlit as st

# Use the fully-qualified service name (prevents context issues)
SEARCH_SERVICE = "KG_DEMO_DB.PUBLIC.DOCS_SEARCH"
MODEL_NAME = "snowflake-arctic"   # change if your Playground shows a different one


def cortex_rag_search(question: str, limit: int = 5):
    """
    Cortex Search retriever using SYSTEM$CORTEX_SEARCH_QUERY
    because this account rejects SEARCH_PREVIEW payload objects.
    """
    conn = st.connection("snowflake")

    payload = {"query": question, "limit": int(limit)}
    payload_str = json.dumps(payload)

    # Escape single quotes for Snowflake SQL string literal
    payload_str = payload_str.replace("'", "''")

    sql = f"""
    SELECT SYSTEM$CORTEX_SEARCH_QUERY(
      '{SEARCH_SERVICE}',
      '{payload_str}'
    ) AS RESULT;
    """

    df = conn.query(sql)
    raw = df["RESULT"].iloc[0]

    # RESULT is typically JSON string
    res = json.loads(raw) if isinstance(raw, str) else raw

    flat = []
    for r in res.get("results", []):
        rec = r.get("record", {})
        flat.append({
            "doc_id":  rec.get("DOC_ID"),
            "title":   rec.get("TITLE"),
            "section": rec.get("SECTION"),
            "content": rec.get("CONTENT"),
            "score":   r.get("score"),
        })
    return flat


def cortex_analyst_summarize_sales(question: str) -> str:
    """
    Cortex COMPLETE using string prompt (this account rejects prompt-object form).
    """
    conn = st.connection("snowflake")

    # Build prompt as a plain string
    prompt = (
        "You are a Snowflake data analyst. "
        "Database KG_DEMO_DB.PUBLIC has tables CUSTOMER, PRODUCT, STORE, ORDERS. "
        "ORDERS(ORDER_ID, CUSTOMER_ID, PRODUCT_ID, STORE_ID, ORDER_DATE, QUANTITY, TOTAL_AMOUNT). "
        "Answer the question based only on this data, and if needed, propose SQL "
        "(but do not actually run it). "
        "Be concise and conversational. "
        f"Question: {question}"
    )

    # Escape single quotes for SQL
    prompt_sql = prompt.replace("'", "''")

    sql = f"""
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
      '{MODEL_NAME}',
      '{prompt_sql}'
    ) AS ANSWER;
    """

    df = conn.query(sql)
    return df["ANSWER"].iloc[0]
