# This code defines a server that uses the MCP (Machine Control Protocol) library 
# to provide tools for semantic search over enterprise docs in Snowflake via Cortex Search, 
# answering analytics questions over Snowflake tables using Cortex COMPLETE, 
# and returning graph neighborhood around a customer from Neo4j KG. 
# The server is implemented using the `streamlit` library and the `neo4j` library. 
# The `cortex_rag_search` function uses the Snowflake Cortex Search API to retrieve semantically 
# relevant document chunks for RAG. The `cortex_analyst_summarize_sales` 
# function uses the Snowflake Cortex COMPLETE API to answer natural-language analytics 
# questions over the KG_DEMO_DB.PUBLIC schema. The `get_customer_neighborhood` function 
# returns a small neighborhood around a customer from Neo4j. 
# The `main` function runs the server using the `stdio_server` 
# context manager from the `mcp.server.stdio` module.
import asyncio
import json

import streamlit as st
import pandas as pd
from neo4j import GraphDatabase

from mcp.server import Server
from mcp.types import Tool, Result
from mcp.server.stdio import stdio_server



# ========= Snowflake / Cortex helpers =========

def cortex_rag_search(question: str, limit: int = 5):
    """
    Use Snowflake Cortex Search (DOCS_SEARCH) to retrieve
    semantically relevant document chunks for RAG.

    Requires st.connection("snowflake") to be configured
    (same as your Streamlit app's Snowflake connection).
    """
    conn = st.connection("snowflake")

    payload = json.dumps({
        "query": question,
        "limit": limit
    })

    sql = """
        SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
          'DOCS_SEARCH',
          PARSE_JSON(%(payload)s)
        ) AS RESULT
    """

    df = conn.query(sql, params={"payload": payload})
    raw = df["RESULT"].iloc[0]  # JSON string
    res = json.loads(raw)

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
    Use Snowflake Cortex COMPLETE to answer natural-language
    analytics questions over your KG_DEMO_DB.PUBLIC schema.

    This is a lightweight 'analyst' function you can expose as a tool.
    """
    conn = st.connection("snowflake")

    sql = """
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
      'snowflake-arctic',
      OBJECT_CONSTRUCT(
        'prompt', CONCAT(
          'You are a Snowflake data analyst. ',
          'Database KG_DEMO_DB.PUBLIC has tables CUSTOMER, PRODUCT, STORE, ORDERS. ',
          'ORDERS(ORDER_ID, CUSTOMER_ID, PRODUCT_ID, STORE_ID, ORDER_DATE, QUANTITY, TOTAL_AMOUNT). ',
          'Answer the question based only on this data, and if needed, propose SQL (but do not actually run it). ',
          'Be concise and conversational. ',
          'Question: ', %(question)s
        )
      )
    ) AS ANSWER;
    """

    df = conn.query(sql, params={"question": question})
    return df["ANSWER"].iloc[0]


# ========= Neo4j helpers =========
# 🔁 Update these with your actual Neo4j settings

NEO4J_URI = "neo4j+s://76060260.databases.neo4j.io"         # or "neo4j+s://<host>.databases.neo4j.io"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "iY7-J53QwICOVljzY7UrvLVuJ4LNGOyaEC57K38nkKY"


def get_neo4j_driver():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


def get_customer_neighborhood(customer_id: str, depth: int = 2):
    """
    Return a small neighborhood around a customer from Neo4j.
    Used by the neo4j_neighborhood MCP tool.
    """
    driver = get_neo4j_driver()
    with driver.session() as session:
        result = session.run(
            """
            MATCH path = (c:Customer {id: $cid})-[*1..$depth]-(n)
            RETURN path LIMIT 50
            """,
            cid=customer_id,
            depth=depth,
        )

        nodes = {}
        rels = []

        for record in result:
            path = record["path"]
            for n in path.nodes:
                nid = n.id
                if nid not in nodes:
                    nodes[nid] = {
                        "id": nid,
                        "labels": list(n.labels),
                        "props": dict(n),
                    }
            for r in path.relationships:
                rels.append({
                    "start": r.start_node.id,
                    "end": r.end_node.id,
                    "type": r.type
                })

    driver.close()
    return {
        "nodes": list(nodes.values()),
        "edges": rels,
    }


# ========= MCP server definition =========

server = Server("snowflake-neo4j-agent")


@server.list_tools()
async def list_tools():
    return [
        Tool(
            name="cortex_rag_search",
            description="Semantic search over enterprise docs in Snowflake via Cortex Search.",
            inputSchema={
                "type": "object",
                "properties": {
                    "question": {"type": "string"},
                    "limit": {"type": "integer", "default": 5}
                },
                "required": ["question"]
            },
        ),
        Tool(
            name="cortex_analyst",
            description="Answer analytics questions over Snowflake tables using Cortex COMPLETE.",
            inputSchema={
                "type": "object",
                "properties": {
                    "question": {"type": "string"}
                },
                "required": ["question"]
            },
        ),
        Tool(
            name="neo4j_neighborhood",
            description="Return graph neighborhood around a customer from Neo4j KG.",
            inputSchema={
                "type": "object",
                "properties": {
                    "customer_id": {"type": "string"},
                    "depth": {"type": "integer", "default": 2}
                },
                "required": ["customer_id"]
            },
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict):
    if name == "cortex_rag_search":
        res = cortex_rag_search(
            question=arguments["question"],
            limit=arguments.get("limit", 5),
        )
        return Result(content=[{"type": "json", "value": res}])

    if name == "cortex_analyst":
        ans = cortex_analyst_summarize_sales(arguments["question"])
        return Result(content=[{"type": "text", "text": ans}])

    if name == "neo4j_neighborhood":
        g = get_customer_neighborhood(
            customer_id=arguments["customer_id"],
            depth=arguments.get("depth", 2),
        )
        return Result(content=[{"type": "json", "value": g}])

    raise ValueError(f"Unknown tool {name}")


async def main():
    async with stdio_server() as (reader, writer):
        await server.run(reader, writer, initialization_options={})

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())


