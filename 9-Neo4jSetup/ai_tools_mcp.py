# This file contains functions that interact with Snowflake and Neo4j databases.

import json
import os
import streamlit as st
from neo4j import GraphDatabase
from sf_to_neo4j import build_graph_from_snowflake
from neo4j_utils import get_neo4j_driver


# ---------- Snowflake sample analytics (optional helper) ----------

def get_sample_orders(limit: int = 10):
    """
    Simple helper to fetch sample orders from Snowflake.
    Used by the UI to show basic data.
    """
    conn = st.connection("snowflake")
    sql = f"SELECT * FROM KG_DEMO_DB.PUBLIC.ORDERS LIMIT {limit}"
    return conn.query(sql)


# ---------- Snowflake Cortex: RAG search ----------

def cortex_rag_search(question: str, limit: int = 5):
    conn = st.connection("snowflake")
    service = "KG_DEMO_DB.PUBLIC.DOCS_SEARCH"

    payload_str = json.dumps({"query": question, "limit": int(limit)}).replace("'", "''")

    sql = f"""
    SELECT SYSTEM$CORTEX_SEARCH_QUERY(
      '{service}',
      '{payload_str}'
    ) AS RESULT;
    """

    df = conn.query(sql)
    raw = df["RESULT"].iloc[0]
    res = json.loads(raw) if isinstance(raw, str) else raw

    flat = []
    for r in res.get("results", []):
        scores = r.get("@scores", {}) or {}

        # Content can be CONTENT or content depending on service/settings
        content = r.get("CONTENT") or r.get("content") or ""

        # Best-effort metadata (varies by search service config)
        title = (
            r.get("TITLE")
            or r.get("title")
            or r.get("DOCUMENT_TITLE")
            or r.get("document_title")
            or r.get("SOURCE")
            or r.get("source")
            or "Doc chunk"
        )

        section = (
            r.get("SECTION")
            or r.get("section")
            or r.get("HEADING")
            or r.get("heading")
            or r.get("PAGE")
            or r.get("page")
            or "N/A"
        )

        flat.append({
            "title": title,
            "section": section,
            "content": content,
            "score": float(scores.get("cosine_similarity", 0.0)),
            "text_match": float(scores.get("text_match", 0.0)),
            # Optional: keep raw for debugging (remove later)
            # "raw": r,
        })

    return flat


# ---------- Snowflake Cortex: analyst over tables ----------

def cortex_analyst_summarize_sales(question: str) -> str:
    conn = st.connection("snowflake")

    escaped_q = question.replace("'", "''")

    prompt = (
        "You are a Snowflake data analyst. "
        "Database KG_DEMO_DB.PUBLIC has tables CUSTOMER, PRODUCT, STORE, ORDERS. "
        "ORDERS(ORDER_ID, CUSTOMER_ID, PRODUCT_ID, STORE_ID, ORDER_DATE, QUANTITY, TOTAL_AMOUNT). "
        "Answer the question based only on this data, and if needed, propose SQL "
        "(but do not actually run it). "
        "Be concise and conversational. "
        f"Question: {escaped_q}"
    )

    escaped_prompt = prompt.replace("'", "''")

    sql = f"""
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
      'snowflake-arctic',
      '{escaped_prompt}'
    ) AS ANSWER;
    """

    df = conn.query(sql)
    return df["ANSWER"].iloc[0]


# ---------- Neo4j: build graph & neighborhood ----------

def rebuild_graph_from_snowflake():
    """
    Thin wrapper over sf_to_neo4j.build_graph_from_snowflake()
    so the UI can call a single function.
    """
    build_graph_from_snowflake()


def get_customer_neighborhood(customer_id: str, depth: int = 2):
    driver = get_neo4j_driver()
    with driver.session() as session:
        query = f"""
        MATCH path = (c:Customer {{id: $cid}})-[*1..{depth}]-(n)
        RETURN path LIMIT 50
        """
        result = session.run(query, cid=customer_id)


        nodes = {}
        edges = []

        for record in result:
            path = record["path"]
            for n in path.nodes:
                nid = str(n.id)   # or n.element_id in newer Neo4j
                if nid not in nodes:
                    nodes[nid] = {
                        "id": nid,
                        "labels": list(n.labels),
                        "props": dict(n),
                    }

            for r in path.relationships:
                edges.append({
                    "start": str(r.start_node.id),
                    "end": str(r.end_node.id),
                    "type": r.type
                })

    driver.close()
    return {
        "nodes": list(nodes.values()),
        "edges": edges,
    }

