## This script loads data from Snowflake views and builds a Neo4j graph database.

import streamlit as st
import pandas as pd
from neo4j import GraphDatabase
from neo4j_utils import get_neo4j_driver


# Use Streamlit's built-in connection
def fetch_df(sql: str):
    conn = st.connection("snowflake")
    return conn.query(sql)   # returns a Pandas dataframe

def build_graph_from_snowflake():
    # 1. Load Snowflake node/relationship views
    df_c = fetch_df("SELECT * FROM KG_DEMO_DB.PUBLIC.V_CUSTOMER_NODE")
    df_p = fetch_df("SELECT * FROM KG_DEMO_DB.PUBLIC.V_PRODUCT_NODE")
    df_s = fetch_df("SELECT * FROM KG_DEMO_DB.PUBLIC.V_STORE_NODE")
    df_b = fetch_df("SELECT * FROM KG_DEMO_DB.PUBLIC.V_BOUGHT_REL")
    df_v = fetch_df("SELECT * FROM KG_DEMO_DB.PUBLIC.V_VISITED_REL")

    driver = get_neo4j_driver()

    with driver.session() as session:
        # Optional reset for PoC
        session.run("MATCH (n) DETACH DELETE n")

        # Customer nodes
        session.run("""
            UNWIND $rows AS row
            MERGE (c:Customer {id: row.ID})
            SET c.name = row.CUSTOMER_NAME,
                c.email = row.EMAIL,
                c.city = row.CITY
        """, rows=df_c.to_dict("records"))

        # Product nodes
        session.run("""
            UNWIND $rows AS row
            MERGE (p:Product {id: row.ID})
            SET p.name = row.PRODUCT_NAME,
                p.category = row.CATEGORY,
                p.price = row.PRICE
        """, rows=df_p.to_dict("records"))

        # Store nodes
        session.run("""
            UNWIND $rows AS row
            MERGE (s:Store {id: row.ID})
            SET s.name = row.STORE_NAME,
                s.city = row.CITY,
                s.region = row.REGION
        """, rows=df_s.to_dict("records"))

        # BOUGHT relationships
        session.run("""
            UNWIND $rows AS row
            MATCH (c:Customer {id: row.CUSTOMER_ID})
            MATCH (p:Product  {id: row.PRODUCT_ID})
            MERGE (c)-[r:BOUGHT]->(p)
            SET r.order_date = row.ORDER_DATE,
                r.quantity = row.QUANTITY,
                r.total_amount = row.TOTAL_AMOUNT
        """, rows=df_b.to_dict("records"))

        # VISITED relationships
        session.run("""
            UNWIND $rows AS row
            MATCH (c:Customer {id: row.CUSTOMER_ID})
            MATCH (s:Store    {id: row.STORE_ID})
            MERGE (c)-[r:VISITED]->(s)
            SET r.first_visit_date = row.FIRST_VISIT_DATE
        """, rows=df_v.to_dict("records"))

    driver.close()
