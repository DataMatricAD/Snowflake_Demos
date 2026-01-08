# 🏥 Snowflake Intelligence for Healthcare  
## Real-Time Clinical Operations Decisioning with Grounded RAG  
**Copy, Paste, Run & Done in less than 10 mins!**

Just run the SQL script as an **ACCOUNTADMIN** — and your real-time Healthcare AI Intelligence environment is ready.

This project demonstrates how to build a **production-grade Healthcare & Life Sciences Intelligence platform** using Snowflake Intelligence, enabling:

- ⚡ Real-time clinical operations decisioning  
- 🧠 Grounded RAG (hallucination-resistant LLMs)  
- 🔍 Vector search over clinical documents  
- 📊 Streaming features for operational AI  
- 🤖 Multi-tool Healthcare Intelligence Agent  

---

## 🚀 What This Project Demonstrates

This demo showcases comprehensive **Snowflake Intelligence capabilities** for Healthcare, including:

- **Cortex Analyst** – Text-to-SQL over governed clinical semantic views  
- **Cortex Search** – Vector search over unstructured clinical and regulatory documents  
- **Snowflake Intelligence Agent** – Multi-tool AI agent for cross-domain clinical reasoning  
- **Streaming-ready architecture** – Event logs, CDC, low-latency features  
- **Grounded RAG pipelines** – Evidence-backed, traceable, safety-aware generation  
- **Git Integration** – Automated loading of structured and unstructured healthcare data  

🎥 Demo video (Snowflake Intelligence overview):  
https://youtu.be/7T8LI5wIfDk  

🎥 Demo video (Streamlit apps from conversations):  
https://youtu.be/zNVx3hwKbyc  

---

# 🧩 Key Components

## 1. Clinical Data Infrastructure

- **Clinical Star Schema**
  - Patient, Provider, Facility, Drug, Diagnosis, Protocol, Trial, Device dimensions  
  - Streaming clinical and operational fact tables  

- **Real-Time Signals**
  - Event logs (admission, discharge, medication, vitals, alerts)  
  - CDC from clinical systems  
  - External healthcare signals  

- **Automated Data Loading**
  - Git integration pulls structured data and clinical documents  

- **Healthcare-Ready Warehouse**
  - Auto-suspend/resume  
  - Streaming-compatible compute  

**Database:** `HC_AI_DEMO`  
**Schema:** `CLINICAL_OPS`  
**Warehouse:** `HC_Intelligence_demo_wh`

---

## 2. Semantic Views (Clinical Domains)

- **Clinical Operations**  
  Patient flow, admissions, discharges, ICU utilization, alerts  

- **Safety & Quality**  
  Adverse events, protocol compliance, operational risk signals  

- **Trials & Research**  
  Trial operations, enrollment metrics, deviations  

- **Finance & Resources**  
  Cost of care, staffing, asset utilization  

Each semantic view is connected to **Cortex Analyst (Text-to-SQL)** for natural-language analytics.

---

## 3. Cortex Search Services (Clinical Knowledge)

Vector services index unstructured healthcare content:

- Clinical guidelines  
- Drug labels & safety notices  
- Trial protocols  
- SOPs and care pathways  
- Regulatory updates  
- Research articles  

All documents are parsed with **Cortex Parse** and indexed for grounded retrieval.

---

## 4. Snowflake Healthcare Intelligence Agent

The **Clinical Operations Intelligence Agent** is a multi-tool AI agent that can:

- Query governed clinical data via Cortex Analyst  
- Retrieve evidence from Cortex Search  
- Perform cross-domain healthcare reasoning  
- Analyze external healthcare content via web scraping  
- Generate secure file links  
- Produce charts and real-time insights  
- Support operational, safety, and trial intelligence use cases  

---

## 5. GitHub Integration

Repository sync enables:

- Automated onboarding of healthcare datasets  
- Ingestion of clinical documents (PDF, text, guidelines)  
- Version-controlled AI demo environments  

Unstructured documents are parsed and stored in a centralized `parsed_content` table for search indexing.

---

# 🏗 Architecture Overview

**Data Flow**

Git Repository → Internal Stage  
Streaming events → Clinical fact tables  
Master data → Clinical dimensions  
PDFs → Cortex Parse → Vector services  
Semantic views → Cortex Analyst  
Search services → Cortex Search  
AI Agent → Orchestrates SQL, search, web, and files  

This architecture supports:

- Real-time clinical signals  
- Grounded LLM decisioning  
- Exactly-once ingestion  
- End-to-end lineage  
- Low-latency AI operations  

---

# 🗂 Database Schema

### Dimension Tables
`patient_dim, provider_dim, facility_dim, drug_dim, diagnosis_dim, protocol_dim, device_dim, trial_dim, department_dim`

### Fact Tables
`clinical_event_fact` – streaming patient events  
`patient_flow_fact` – admissions, discharge, transfers  
`safety_event_fact` – adverse events and alerts  
`trial_operations_fact` – trial metrics  

### Unstructured Data
`clinical_documents`  
`regulatory_documents`  
`research_documents`  

---

# ⚙ Setup Instructions

## Single-Script Setup

Run the complete setup script:

```sql
-- Execute in Snowflake worksheet
@HC_IntelligenceDemo/sql_scripts/demo_setup.sql
