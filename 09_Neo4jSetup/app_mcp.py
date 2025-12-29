#  This script is part of a Streamlit application that demonstrates the integration of Snowflake, 
# Neo4j, and Cortex for various data analytics and knowledge graph tasks. 
# It includes tabs for different functionalities such as simple Snowflake sample data, 
# building/refreshing a knowledge graph in Neo4j, and using AI copilot capabilities 
# over Snowflake, Neo4j, and documents.
import streamlit as st
from streamlit_agraph import agraph, Node, Edge, Config

from ai_tools_mcp import (
    get_sample_orders,
    cortex_analyst_summarize_sales,
    cortex_rag_search,
    rebuild_graph_from_snowflake,
    get_customer_neighborhood,
)

st.set_page_config(page_title="Snowflake + Neo4j + Cortex PoC", layout="wide")
st.title("Snowflake + Neo4j + Cortex PoC")

tab1, tab2, tab3 = st.tabs([
    "Snowflake Analytics",
    "Neo4j Knowledge Graph",
    "AI Copilot",
])

# --- Tab 1: simple Snowflake sample data ---
with tab1:
    st.subheader("Sample Data from Snowflake (ORDERS)")
    df = get_sample_orders(limit=10)
    st.dataframe(df)


# --- Tab 2: Neo4j Knowledge Graph ---
with tab2:
    st.subheader("Build / Refresh Knowledge Graph from Snowflake")

    if st.button("Rebuild Graph in Neo4j"):
        rebuild_graph_from_snowflake()
        st.success("Graph successfully rebuilt from Snowflake!")

    st.subheader("Graph Visualization")

    limit = st.slider("Max relationships to display", 10, 200, 50)

    graph = get_customer_neighborhood(customer_id="C001", depth=2)  # or use LIMIT logic

    # Convert graph dict → agraph nodes/edges
    nodes_dict = {}
    edges = []

    for n in graph["nodes"]:
        nid = n["id"]
        label = n["props"].get("name", nid)
        nodes_dict[nid] = Node(
            id=nid,
            label=label,
            title=str(n["props"]),
            size=20,
        )

    for e in graph["edges"]:
        edges.append(
            Edge(
                source=e["start"],
                target=e["end"],
                label=e["type"],
            )
        )

    config = Config(width=1200, height=700, directed=True, physics=True)
    agraph(list(nodes_dict.values()), edges, config)


# --- Tab 3: AI Copilot (Cortex + Neo4j) ---
with tab3:
    st.subheader("AI Copilot over Snowflake + Neo4j + Docs")

    st.markdown("### 1. Cortex Analyst (Tables)")
    q1 = st.text_input("Ask about sales/customers/orders:", key="q1")
    if st.button("Ask Cortex Analyst", key="btn_analyst") and q1:
        ans = cortex_analyst_summarize_sales(q1)
        st.markdown("**Answer:**")
        st.write(ans)

    st.markdown("---")
    st.markdown("### 2. Cortex RAG (Docs)")
    q2 = st.text_input("Ask about docs/policies/manuals:", key="q2")
    limit_docs = st.slider("Number of doc chunks", 1, 10, 5, key="limit_docs")
    if st.button("Search Docs", key="btn_docs") and q2:
        chunks = cortex_rag_search(q2, limit=limit_docs)
        for c in chunks:
            st.markdown(f"#### {c['title']} (score: {c['score']:.3f})")
            st.markdown(f"**Section:** {c['section']}")
            st.write(c["content"])
            st.markdown("---")

    st.markdown("---")
    st.markdown("### 3. Neo4j Neighborhood")
    cid = st.text_input("Customer ID for neighborhood:", value="C001", key="cid")
    depth = st.slider("Depth", 1, 3, 2, key="depth_slider")
    if st.button("Get Neighborhood", key="btn_graph"):
        g = get_customer_neighborhood(cid, depth=depth)
        st.json(g)
