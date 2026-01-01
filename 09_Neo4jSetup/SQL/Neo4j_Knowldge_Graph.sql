-- 1. Create a dedicated DB for the PoC
CREATE OR REPLACE DATABASE KG_DEMO_DB;

-- 2. Use the database & public schema
USE DATABASE KG_DEMO_DB;
USE SCHEMA PUBLIC;


-- CUSTOMER master
CREATE OR REPLACE TABLE CUSTOMER (
    CUSTOMER_ID   STRING      NOT NULL,
    CUSTOMER_NAME STRING,
    EMAIL         STRING,
    CITY          STRING,
    CREATED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_CUSTOMER PRIMARY KEY (CUSTOMER_ID)
);

-- PRODUCT master
CREATE OR REPLACE TABLE PRODUCT (
    PRODUCT_ID    STRING      NOT NULL,
    PRODUCT_NAME  STRING,
    CATEGORY      STRING,
    PRICE         NUMBER(10,2),
    CONSTRAINT PK_PRODUCT PRIMARY KEY (PRODUCT_ID)
);

-- STORE master
CREATE OR REPLACE TABLE STORE (
    STORE_ID      STRING      NOT NULL,
    STORE_NAME    STRING,
    CITY          STRING,
    REGION        STRING,
    CONSTRAINT PK_STORE PRIMARY KEY (STORE_ID)
);

-- ORDERS fact (links CUSTOMER, PRODUCT, STORE)
CREATE OR REPLACE TABLE ORDERS (
    ORDER_ID      STRING      NOT NULL,
    CUSTOMER_ID   STRING      NOT NULL,
    PRODUCT_ID    STRING      NOT NULL,
    STORE_ID      STRING      NOT NULL,
    ORDER_DATE    TIMESTAMP_NTZ,
    QUANTITY      NUMBER(10,0),
    TOTAL_AMOUNT  NUMBER(10,2),
    CONSTRAINT PK_ORDERS PRIMARY KEY (ORDER_ID),
    CONSTRAINT FK_ORDERS_CUSTOMER FOREIGN KEY (CUSTOMER_ID) REFERENCES CUSTOMER(CUSTOMER_ID),
    CONSTRAINT FK_ORDERS_PRODUCT  FOREIGN KEY (PRODUCT_ID)  REFERENCES PRODUCT(PRODUCT_ID),
    CONSTRAINT FK_ORDERS_STORE    FOREIGN KEY (STORE_ID)    REFERENCES STORE(STORE_ID)
);


INSERT INTO CUSTOMER (CUSTOMER_ID, CUSTOMER_NAME, EMAIL, CITY) VALUES
('C001', 'Alice Johnson',   'alice@example.com',   'Los Angeles'),
('C002', 'Bob Singh',       'bob@example.com',     'San Diego'),
('C003', 'Carlos Ramirez',  'carlos@example.com',  'San Jose'),
('C004', 'Divya Patel',     'divya@example.com',   'San Francisco'),
('C005', 'Emily Chen',      'emily@example.com',   'Irvine');

INSERT INTO PRODUCT (PRODUCT_ID, PRODUCT_NAME, CATEGORY, PRICE) VALUES
('P001', 'Wireless Mouse',        'Electronics',   25.99),
('P002', 'Mechanical Keyboard',   'Electronics',   89.50),
('P003', 'Running Shoes',         'Sportswear',    120.00),
('P004', 'Water Bottle',          'Sportswear',    15.00),
('P005', 'Noise Cancelling Headphones', 'Electronics', 199.99);


INSERT INTO STORE (STORE_ID, STORE_NAME, CITY, REGION) VALUES
('S001', 'Downtown LA Store',       'Los Angeles',  'West'),
('S002', 'San Diego Mall Store',    'San Diego',    'West'),
('S003', 'Bay Area Flagship Store', 'San Francisco','West');

INSERT INTO ORDERS
    (ORDER_ID, CUSTOMER_ID, PRODUCT_ID, STORE_ID, ORDER_DATE, QUANTITY, TOTAL_AMOUNT)
VALUES
('O1001', 'C001', 'P001', 'S001', '2025-01-05 10:15:00', 1, 25.99),
('O1002', 'C001', 'P002', 'S001', '2025-01-15 14:30:00', 1, 89.50),
('O1003', 'C002', 'P003', 'S002', '2025-01-20 16:45:00', 2, 240.00),
('O1004', 'C003', 'P004', 'S003', '2025-01-22 11:10:00', 1, 15.00),
('O1005', 'C003', 'P003', 'S003', '2025-01-25 09:05:00', 1, 120.00),
('O1006', 'C004', 'P005', 'S003', '2025-02-01 13:20:00', 1, 199.99),
('O1007', 'C005', 'P001', 'S002', '2025-02-03 17:55:00', 2, 51.98),
('O1008', 'C005', 'P004', 'S002', '2025-02-10 12:40:00', 1, 15.00);


SELECT * FROM CUSTOMER;
SELECT * FROM PRODUCT;
SELECT * FROM STORE;
SELECT * FROM ORDERS;



-- Customer nodes
CREATE OR REPLACE VIEW V_CUSTOMER_NODE AS
SELECT DISTINCT
    CUSTOMER_ID        AS ID,
    CUSTOMER_NAME,
    EMAIL,
    CITY
FROM CUSTOMER;

-- Product nodes
CREATE OR REPLACE VIEW V_PRODUCT_NODE AS
SELECT DISTINCT
    PRODUCT_ID         AS ID,
    PRODUCT_NAME,
    CATEGORY,
    PRICE
FROM PRODUCT;

-- Store nodes
CREATE OR REPLACE VIEW V_STORE_NODE AS
SELECT DISTINCT
    STORE_ID           AS ID,
    STORE_NAME,
    CITY,
    REGION
FROM STORE;

--We’ll define two relationships:
-- Customer BOUGHT Product
CREATE OR REPLACE VIEW V_BOUGHT_REL AS
SELECT DISTINCT
    CUSTOMER_ID,
    PRODUCT_ID,
    ORDER_DATE,
    QUANTITY,
    TOTAL_AMOUNT
FROM ORDERS;

-- Customer VISITED Store (first time they appeared in that store)
CREATE OR REPLACE VIEW V_VISITED_REL AS
SELECT
    CUSTOMER_ID,
    STORE_ID,
    MIN(ORDER_DATE) AS FIRST_VISIT_DATE
FROM ORDERS
GROUP BY CUSTOMER_ID, STORE_ID;


SELECT * FROM V_CUSTOMER_NODE;
SELECT * FROM V_PRODUCT_NODE;
SELECT * FROM V_STORE_NODE;

SELECT * FROM V_BOUGHT_REL;
SELECT * FROM V_VISITED_REL;

--Prepare RAG data in Snowflake (Cortex Search)
--In a real project you’d load parsed PDFs / Confluence exports here.

USE DATABASE KG_DEMO_DB;
USE SCHEMA PUBLIC;

CREATE OR REPLACE TABLE DOCS (
    DOC_ID      STRING,
    TITLE       STRING,
    SECTION     STRING,
    CONTENT     STRING,
    DOC_TYPE    STRING,
    UPDATED_AT  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO DOCS (DOC_ID, TITLE, SECTION, CONTENT, DOC_TYPE) VALUES
('D001', 'Return Policy',        'overview', 'Customers may return products within 30 days...', 'policy'),
('D002', 'Product Failure Guide','risks',    'If a product shows repeated overheating...',       'manual'),
('D003', 'VIP Program',          'benefits', 'High-value customers get extended warranty...',    'program');

--Create a Cortex Search Service on DOCS
--This gives you a hybrid semantic + keyword engine inside Snowflake, ideal for RAG
CREATE OR REPLACE CORTEX SEARCH SERVICE DOCS_SEARCH
  ON CONTENT
  ATTRIBUTES TITLE, DOC_TYPE, UPDATED_AT
  WAREHOUSE = COMPUTE_WH
  TARGET_LAG = '1 hour'
  EMBEDDING_MODEL = 'snowflake-arctic-embed-l-v2.0'
AS (
  SELECT DOC_ID, TITLE, SECTION, CONTENT, DOC_TYPE, UPDATED_AT
  FROM DOCS
);

--Quick test query
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
  'DOCS_SEARCH',
  '{
     "query": "products that may injure customers",
     "limit": 5
   }'
);

--Python wrappers (RAG + Graph)
--In your Streamlit/VS Code codebase, add utility functions that your MCP tools will call.


SELECT CURRENT_REGION();

ALTER ACCOUNT SET CORTEX_REGION_GROUP = 'AZURE_US';

ALTER ACCOUNT SET CORTEX_REGION_GROUP = 'ANY_REGION';



SELECT CURRENT_REGION();


SELECT CURRENT_ACCOUNT();





SELECT * 
FROM TABLE(SNOWFLAKE.CORTEX.LIST_MODELS());



















