
## This code securely creates a Neo4j database connection using 
##a password from an environment variable and verifies 
## the connection by running a simple test query.

import os
from neo4j import GraphDatabase

def get_neo4j_driver():
    uri = "neo4j+s://*******.databases.neo4j.io"  # or "bolt://localhost:7687"
    user = "neo4j"
    password = os.environ.get("NEO4J_PASSWORD")
    return GraphDatabase.driver(uri, auth=(user, password))

def test_connection():
    driver = get_neo4j_driver()
    try:
        with driver.session() as session:
            result = session.run("RETURN 'Neo4j Connected' AS message")
            print(result.single()["message"])
    finally:
        driver.close()

if __name__ == "__main__":
    test_connection()