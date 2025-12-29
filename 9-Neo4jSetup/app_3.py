import streamlit as st
from streamlit_agraph import agraph, Node, Edge, Config
from sf_to_neo4j import build_graph_from_snowflake
from neo4j_utils import get_neo4j_driver

# Updated Streamlit App Configuration

st.set_page_config(page_title="Snowflake + Neo4j Knowledge Graph PoC", layout="wide")

st.title("Snowflake + Neo4j Knowledge Graph PoC")

tab1, tab2 = st.tabs(["Snowflake Analytics", "Neo4j Knowledge Graph"])

# --- Tab 1 (your existing Snowflake visualizations)
with tab1:
    sql = "SELECT * FROM KG_DEMO_DB.PUBLIC.ORDERS LIMIT 10"
    conn = st.connection("snowflake")
    st.write("Sample Data from Snowflake:")
    st.dataframe(conn.query(sql))

# --- Tab 2 (Neo4j)
with tab2:
    st.subheader("Build / Refresh Knowledge Graph from Snowflake")

    if st.button("Rebuild Graph in Neo4j"):
        build_graph_from_snowflake()
        st.success("Graph successfully rebuilt from Snowflake!")

    st.subheader("Graph Visualization")

    limit = st.slider("Max relationships to display", 10, 200, 50)

    driver = get_neo4j_driver()

    with driver.session() as session:
        result = session.run("""
            MATCH (c:Customer)-[:BOUGHT]->(p:Product)
            RETURN c, p
            LIMIT $limit
        """, limit=limit)

        nodes_dict = {}
        edges = []

        for record in result:
            c = record["c"]
            p = record["p"]

            c_id = str(c.id)
            p_id = str(p.id)

            if c_id not in nodes_dict:
                nodes_dict[c_id] = Node(id=c_id, label=c.get("name", c_id), title=str(dict(c)), size=20)

            if p_id not in nodes_dict:
                nodes_dict[p_id] = Node(id=p_id, label=p.get("name", p_id), title=str(dict(p)), size=20)

            edges.append(Edge(source=c_id, target=p_id, label="BOUGHT"))

    driver.close()

    config = Config(width=1200, height=700, directed=True, physics=True)
    agraph(list(nodes_dict.values()), edges, config)
