import os
import re
import uuid
from typing import Dict, List, Optional, Tuple

import streamlit as st
from snowflake.snowpark import Session
from openai import OpenAI


# =========================
# Small utilities
# =========================
def extract_sql_codeblock(text: str) -> Optional[str]:
    m = re.search(r"```sql\s*(.*?)\s*```", text, re.DOTALL | re.IGNORECASE)
    return m.group(1).strip() if m else None


def get_openai_client() -> OpenAI:
    api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key:
        raise RuntimeError("Missing OPENAI_API_KEY environment variable.")
    return OpenAI(api_key=api_key)


def _safe_ident(name: str) -> str:
    # Snowflake unquoted identifiers are uppercased; we will quote to preserve
    return '"' + name.replace('"', '""') + '"'


def _pick_default_db_schema(session: Session) -> Tuple[str, str]:
    """
    Ensures the session has a current DB/SCHEMA.
    If CURRENT_DATABASE/SCHEMA are NULL, choose something usable.
    """
    row = session.sql(
        "SELECT CURRENT_DATABASE() AS DB, CURRENT_SCHEMA() AS SC"
    ).collect()[0]
    db = row["DB"]
    sc = row["SC"]

    env_db = os.environ.get("SNOWFLAKE_DATABASE")
    env_sc = os.environ.get("SNOWFLAKE_SCHEMA")

    if not db:
        # try env
        if env_db:
            db = env_db
        else:
            # pick first visible db
            dbs = session.sql("SHOW DATABASES").collect()
            if not dbs:
                raise RuntimeError("No databases visible to this user. Set SNOWFLAKE_DATABASE or ask admin.")
            db = dbs[0]["name"]

    # Use database
    session.sql(f'USE DATABASE {_safe_ident(db)}').collect()

    if not sc:
        if env_sc:
            sc = env_sc
        else:
            # prefer PUBLIC if exists else first schema
            schemas = session.sql(f"SHOW SCHEMAS IN DATABASE {_safe_ident(db)}").collect()
            if not schemas:
                raise RuntimeError(f"No schemas visible in database {db}. Set SNOWFLAKE_SCHEMA or ask admin.")
            names = [r["name"] for r in schemas]
            sc = "PUBLIC" if "PUBLIC" in names else names[0]

    session.sql(f'USE SCHEMA {_safe_ident(sc)}').collect()
    return db, sc


def _connect_snowflake(
    account: str,
    user: str,
    password: str,
    role: Optional[str],
    warehouse: Optional[str],
) -> Session:
    cfg = {"account": account, "user": user, "password": password}
    if role:
        cfg["role"] = role
    if warehouse:
        cfg["warehouse"] = warehouse
    return Session.builder.configs(cfg).create()


# =========================
# Parsing LeetCode problem text
# =========================
def parse_schema_from_problem(problem_text: str) -> Dict[str, List[Tuple[str, str]]]:
    """
    Parses LeetCode style:

    Table: Employee
    +-------------+---------+
    | Column Name | Type    |
    ...
    | empId       | int     |
    ...

    Returns: { "Employee": [("empId","INT"), ("name","VARCHAR"), ...], ...}
    """
    tables: Dict[str, List[Tuple[str, str]]] = {}

    # Find each "Table: X" section until next "Table:" or end.
    # Column rows look like: | empId | int |
    # We map LeetCode types -> Snowflake types
    type_map = {
        "int": "INT",
        "integer": "INT",
        "bigint": "BIGINT",
        "varchar": "VARCHAR",
        "string": "VARCHAR",
        "text": "VARCHAR",
        "date": "DATE",
        "datetime": "TIMESTAMP_NTZ",
        "timestamp": "TIMESTAMP_NTZ",
        "float": "FLOAT",
        "double": "DOUBLE",
        "decimal": "NUMBER",
        "numeric": "NUMBER",
        "bool": "BOOLEAN",
        "boolean": "BOOLEAN",
    }

    # Split by Table:
    parts = re.split(r"\bTable:\s*", problem_text, flags=re.IGNORECASE)
    # First part is preamble
    for part in parts[1:]:
        # First line is table name
        lines = part.strip().splitlines()
        if not lines:
            continue
        table_name = lines[0].strip()
        # Collect column definitions within ASCII table rows after header
        cols: List[Tuple[str, str]] = []
        for ln in lines[1:]:
            m = re.match(r"^\|\s*([A-Za-z0-9_]+)\s*\|\s*([A-Za-z0-9_]+)\s*\|", ln.strip())
            if m:
                col = m.group(1).strip()
                typ = m.group(2).strip().lower()
                cols.append((col, type_map.get(typ, "VARCHAR")))
        if cols:
            tables[table_name] = cols

    return tables


def parse_input_tables(input_text: str) -> Dict[str, List[Dict[str, Optional[str]]]]:
    """
    Parses LeetCode "Input:" tables (ASCII) like:

    Employee table:
    +-------+--------+------------+--------+
    | empId | name   | supervisor | salary |
    +-------+--------+------------+--------+
    | 3     | Brad   | null       | 4000   |
    ...

    Bonus table:
    ...

    Returns:
      { "Employee": [ {"empId":"3","name":"Brad",...}, ...], "Bonus": [...] }

    Notes:
    - Values come back as strings (or None for null). We'll cast in INSERT using table types.
    """
    text = input_text.strip()
    if not text:
        return {}

    result: Dict[str, List[Dict[str, Optional[str]]]] = {}

    # Identify blocks like "<Name> table:" followed by ASCII table
    # We'll scan line by line.
    lines = text.splitlines()
    i = 0
    while i < len(lines):
        header = lines[i].strip()
        m = re.match(r"^([A-Za-z0-9_]+)\s+table:\s*$", header, flags=re.IGNORECASE)
        if not m:
            i += 1
            continue
        table_name = m.group(1)
        i += 1

        # Skip until we hit header row "| col | col |"
        # Also skip +----+ lines
        while i < len(lines) and not lines[i].strip().startswith("|"):
            i += 1
        if i >= len(lines):
            break

        # Header columns
        col_line = lines[i].strip()
        cols = [c.strip() for c in col_line.strip("|").split("|")]
        i += 1

        # Skip separator line(s)
        while i < len(lines) and lines[i].strip().startswith("+"):
            i += 1

        rows: List[Dict[str, Optional[str]]] = []
        while i < len(lines):
            ln = lines[i].strip()
            if not ln:
                i += 1
                continue
            if re.match(r"^[A-Za-z0-9_]+\s+table:\s*$", ln, flags=re.IGNORECASE):
                break
            if ln.startswith("+"):
                i += 1
                continue
            if not ln.startswith("|"):
                i += 1
                continue

            vals = [v.strip() for v in ln.strip("|").split("|")]
            if len(vals) != len(cols):
                i += 1
                continue

            row = {}
            for c, v in zip(cols, vals):
                if v.lower() == "null":
                    row[c] = None
                else:
                    row[c] = v
            rows.append(row)
            i += 1

        result[table_name] = rows

    return result


# =========================
# Snowflake temp table builder / loader
# =========================
def create_temp_tables(
    session: Session,
    schema: Dict[str, List[Tuple[str, str]]],
    unique_prefix: str,
) -> Dict[str, str]:
    """
    Creates TEMP tables with unique names per problem and returns mapping:
      logical_table_name -> temp_table_name
    """
    mapping: Dict[str, str] = {}
    for logical_name, cols in schema.items():
        temp_name = f"TEMP_{logical_name.upper()}_{unique_prefix}"
        mapping[logical_name] = temp_name

        col_ddl = ",\n".join([f'{_safe_ident(c)} {t}' for c, t in cols])
        ddl = f"CREATE OR REPLACE TEMP TABLE {_safe_ident(temp_name)} (\n{col_ddl}\n)"
        session.sql(ddl).collect()

    return mapping


def load_rows(
    session: Session,
    schema: Dict[str, List[Tuple[str, str]]],
    mapping: Dict[str, str],
    data: Dict[str, List[Dict[str, Optional[str]]]],
) -> None:
    """
    Truncates and inserts parsed rows into created temp tables.
    If a table has no data block provided, we leave it empty.
    """
    for logical, cols in schema.items():
        temp_name = mapping[logical]
        session.sql(f"TRUNCATE TABLE {_safe_ident(temp_name)}").collect()

        rows = data.get(logical, [])
        if not rows:
            continue

        col_names = [c for c, _ in cols]
        for row in rows:
            values_sql = []
            for c, t in cols:
                v = row.get(c)
                if v is None:
                    values_sql.append("NULL")
                else:
                    # basic quoting/casting
                    if t.upper() in ("INT", "BIGINT", "FLOAT", "DOUBLE", "NUMBER"):
                        # allow numeric strings
                        values_sql.append(str(v))
                    elif t.upper().startswith("TIMESTAMP") or t.upper() == "DATE":
                        # wrap in single quotes
                        values_sql.append("'" + str(v).replace("'", "''") + "'")
                    else:
                        values_sql.append("'" + str(v).replace("'", "''") + "'")

            insert_sql = (
                f"INSERT INTO {_safe_ident(temp_name)} "
                f"({', '.join(_safe_ident(c) for c in col_names)}) "
                f"VALUES ({', '.join(values_sql)})"
            )
            session.sql(insert_sql).collect()


def rewrite_sql_with_temp_tables(sql_query: str, mapping: Dict[str, str]) -> str:
    """
    Replaces logical table names with temp table names (word-boundary, case-insensitive).
    Example: Employee -> TEMP_EMPLOYEE_ABC123
    """
    rewritten = sql_query
    # Replace longer names first to avoid partial collisions
    for logical in sorted(mapping.keys(), key=len, reverse=True):
        temp = mapping[logical]
        pattern = r"\b" + re.escape(logical) + r"\b"
        rewritten = re.sub(pattern, temp, rewritten, flags=re.IGNORECASE)
    return rewritten


# =========================
# AI helpers
# =========================
def ai_generate_sql(problem_text: str, table_names: List[str]) -> str:
    client = get_openai_client()
    prompt = f"""
You are solving a LeetCode SQL problem in Snowflake.

Rules:
- Return ONLY a SQL query (no explanation).
- Use the provided table names exactly (case-insensitive ok).
- Do NOT invent additional tables/columns.
- Keep it simple and correct.

Tables available:
{", ".join(table_names)}

Problem:
{problem_text}
"""
    r = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.0,
    )
    text = r.choices[0].message.content.strip()
    return extract_sql_codeblock(text) or text


def ai_text(task: str, sql_query: str) -> str:
    client = get_openai_client()
    prompt = (
        f"Task: {task}\n\n"
        "SQL:\n"
        "```sql\n"
        f"{sql_query}\n"
        "```\n"
    )
    r = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
    )
    return r.choices[0].message.content.strip()


# =========================
# Streamlit UI
# =========================
st.set_page_config(page_title="Snowflake LeetCode SQL Assistant", layout="wide")
st.title("Snowflake LeetCode SQL Assistant")
st.caption("Paste a LeetCode SQL problem + sample input, auto-create TEMP tables, run query, and get AI explain/comment/optimize.")


# --- Sidebar: credentials ---
with st.sidebar:
    st.header("Snowflake Connection")
    st.caption("Keep your credential options here. You can use secrets/env or type them below.")

    # Let user enter (overrides secrets/env if provided)
    account_in = st.text_input("Account", value=os.environ.get("SNOWFLAKE_ACCOUNT", ""))
    user_in = st.text_input("User", value=os.environ.get("SNOWFLAKE_USER", ""))
    password_in = st.text_input("Password", type="password", value=os.environ.get("SNOWFLAKE_PASSWORD", ""))

    role_in = st.text_input("Role (optional)", value=os.environ.get("SNOWFLAKE_ROLE", ""))
    wh_in = st.text_input("Warehouse (optional)", value=os.environ.get("SNOWFLAKE_WAREHOUSE", ""))

    db_in = st.text_input("Database (optional)", value=os.environ.get("SNOWFLAKE_DATABASE", ""))
    sc_in = st.text_input("Schema (optional)", value=os.environ.get("SNOWFLAKE_SCHEMA", ""))

    connect_btn = st.button("Connect", use_container_width=True)


def get_session_from_inputs() -> Optional[Tuple[Session, str, str]]:
    """
    Uses sidebar if filled; else tries Streamlit secrets; else env.
    Returns None if still missing (don’t crash the UI).
    """
    # 1) Sidebar takes priority if Connect clicked or if already have in state
    def _nonempty(x: str) -> Optional[str]:
        x = (x or "").strip()
        return x if x else None

    # Try secrets if sidebar not filled
    account = _nonempty(account_in)
    user = _nonempty(user_in)
    password = _nonempty(password_in)

    # secrets fallback ONLY if user didn't type
    if not account or not user:
        try:
            if "connections" in st.secrets and "snowflake" in st.secrets["connections"]:
                account = account or st.secrets["connections"]["snowflake"].get("account")
                user = user or st.secrets["connections"]["snowflake"].get("user")
            elif "connections.snowflake" in st.secrets:
                account = account or st.secrets["connections.snowflake"].get("account")
                user = user or st.secrets["connections.snowflake"].get("user")
        except Exception:
            pass

    # password from other env fallbacks
    if not password:
        password = (
            os.environ.get("SNOWSQL_PWD")
            or os.environ.get("SNOWFLAKE_PWD")
            or os.environ.get("SNOWFLAKE_PASSWORD")
        )

    if not account or not user or not password:
        return None

    session = _connect_snowflake(
        account=account,
        user=user,
        password=password,
        role=_nonempty(role_in),
        warehouse=_nonempty(wh_in),
    )

    # Set DB/SCHEMA
    db, sc = _pick_default_db_schema(session)

    # If user specified db/schema, use them (if provided)
    if (db_in or "").strip():
        session.sql(f'USE DATABASE {_safe_ident(db_in.strip())}').collect()
        db = db_in.strip()
    if (sc_in or "").strip():
        session.sql(f'USE SCHEMA {_safe_ident(sc_in.strip())}').collect()
        sc = sc_in.strip()

    return session, db, sc


# Cache the session so temp tables stay valid while app runs
@st.cache_resource(show_spinner="Connecting to Snowflake...")
def cached_session(account: str, user: str, password: str, role: str, wh: str, db: str, sc: str):
    # We re-create session using resolved inputs (simple, stable cache key)
    sess = _connect_snowflake(account, user, password, role or None, wh or None)
    _pick_default_db_schema(sess)
    if db:
        sess.sql(f'USE DATABASE {_safe_ident(db)}').collect()
    if sc:
        sess.sql(f'USE SCHEMA {_safe_ident(sc)}').collect()
    return sess


# Connect flow
session: Optional[Session] = None
default_db = ""
default_schema = ""

resolved = get_session_from_inputs()
if resolved:
    # Use cache only when we can form stable keys
    # (If using secrets/env only, account_in/user_in might be empty; we still need actual values)
    # Re-resolve actual strings for caching:
    acct = (account_in or os.environ.get("SNOWFLAKE_ACCOUNT") or "").strip()
    usr = (user_in or os.environ.get("SNOWFLAKE_USER") or "").strip()
    pwd = (password_in or os.environ.get("SNOWFLAKE_PASSWORD") or os.environ.get("SNOWSQL_PWD") or "").strip()

    # If acct/usr are empty because you rely on secrets, don't force cache-key empties:
    # Just use the direct session.
    if acct and usr and pwd:
        session = cached_session(
            acct, usr, pwd,
            (role_in or "").strip(),
            (wh_in or "").strip(),
            (db_in or "").strip(),
            (sc_in or "").strip(),
        )
        default_db = (db_in or "").strip() or _pick_default_db_schema(session)[0]
        default_schema = (sc_in or "").strip() or _pick_default_db_schema(session)[1]
    else:
        session, default_db, default_schema = resolved

if not session:
    st.warning(
        "No Snowflake session yet.\n\n"
        "✅ Enter **Account / User / Password** in the sidebar (or set secrets/env), then click **Connect**.\n\n"
        "Password env options: `SNOWFLAKE_PASSWORD` or `SNOWSQL_PWD`."
    )
    st.stop()


# =========================
# Main inputs
# =========================
left, right = st.columns([1.2, 1])

with left:
    st.subheader("1) Paste Problem Statement")
    problem_text = st.text_area(
        "LeetCode problem statement (include the Table schema blocks)",
        height=300,
        placeholder="Paste the full problem statement including the 'Table:' schema sections...",
    )

with right:
    st.subheader("2) Paste Sample Input (Optional but recommended)")
    input_text = st.text_area(
        "LeetCode 'Input:' tables (ASCII tables).",
        height=300,
        placeholder="Paste the Employee table / Bonus table input blocks here...",
    )

btn_row = st.columns([1, 1, 1, 1])
with btn_row[0]:
    prepare_btn = st.button("Prepare Tables", use_container_width=True)
with btn_row[1]:
    gen_btn = st.button("Generate Solution SQL", use_container_width=True)
with btn_row[2]:
    run_btn = st.button("Run Query", use_container_width=True)
with btn_row[3]:
    reset_btn = st.button("Reset Problem Session", use_container_width=True)

# Problem session state (unique temp table prefix & mapping)
if "problem_id" not in st.session_state:
    st.session_state.problem_id = None
if "table_mapping" not in st.session_state:
    st.session_state.table_mapping = {}
if "schema" not in st.session_state:
    st.session_state.schema = {}
if "generated_sql" not in st.session_state:
    st.session_state.generated_sql = ""


if reset_btn:
    st.session_state.problem_id = None
    st.session_state.table_mapping = {}
    st.session_state.schema = {}
    st.session_state.generated_sql = ""
    st.success("Reset done. Paste a new problem and click Prepare Tables.")


def prepare_problem_tables() -> None:
    if not problem_text.strip():
        st.error("Paste a problem statement first.")
        return

    schema = parse_schema_from_problem(problem_text)
    if not schema:
        st.error("Could not detect any tables/columns. Make sure the problem includes 'Table:' schema sections.")
        return

    data = parse_input_tables(input_text) if input_text.strip() else {}

    # New unique prefix each time you prepare tables for a problem text (avoids collisions)
    problem_id = uuid.uuid4().hex[:8].upper()
    st.session_state.problem_id = problem_id
    st.session_state.schema = schema

    mapping = create_temp_tables(session, schema, unique_prefix=problem_id)
    st.session_state.table_mapping = mapping

    load_rows(session, schema, mapping, data)

    st.success(f"Prepared TEMP tables for this problem: {', '.join([f'{k}→{v}' for k,v in mapping.items()])}")


if prepare_btn:
    prepare_problem_tables()


# Show schema/mapping
if st.session_state.schema:
    with st.expander("Detected schema / temp table mapping", expanded=True):
        st.write("**Database/Schema in use:**", f"{default_db}.{default_schema}")
        for t, cols in st.session_state.schema.items():
            st.write(f"**{t}** → `{st.session_state.table_mapping.get(t,'(not created)')}`")
            st.code(", ".join([f"{c} {ty}" for c, ty in cols]), language="text")


# =========================
# Tabs: Query / Plan / Describe / Comments / Optimization / Encapsulation
# =========================
tabs = st.tabs(["Query", "Plan", "Description", "Comments", "Optimization", "Encapsulation"])

with tabs[0]:
    st.subheader("Query")
    if st.session_state.generated_sql:
        default_q = st.session_state.generated_sql
    else:
        default_q = ""

    sql_query = st.text_area(
        "SQL to run (logical table names will be rewritten to the TEMP tables automatically)",
        value=default_q,
        height=180,
        placeholder="Click 'Generate Solution SQL' or write your own SQL here...",
    )

    if gen_btn:
        if not st.session_state.schema:
            st.error("Click 'Prepare Tables' first so I know what tables exist.")
        else:
            try:
                logical_tables = list(st.session_state.schema.keys())
                gen = ai_generate_sql(problem_text, logical_tables)
                st.session_state.generated_sql = gen
                st.rerun()
            except Exception as e:
                st.error(f"AI generation failed: {e}")

    # Run query
    if run_btn:
        if not st.session_state.table_mapping:
            st.error("Click 'Prepare Tables' first (TEMP tables must exist before execution).")
        elif not sql_query.strip():
            st.error("SQL is empty.")
        else:
            try:
                # rewrite logical -> temp table names
                rewritten = rewrite_sql_with_temp_tables(sql_query, st.session_state.table_mapping)
                st.code(rewritten, language="sql")
                df = session.sql(rewritten).collect()
                st.dataframe(df, use_container_width=True)
            except Exception as e:
                st.error(f"Query execution failed: {e}")

with tabs[1]:
    st.subheader("Plan (EXPLAIN)")
    if sql_query.strip() and st.session_state.table_mapping:
        try:
            rewritten = rewrite_sql_with_temp_tables(sql_query, st.session_state.table_mapping)
            explain = f"EXPLAIN USING TABULAR\n{rewritten}"
            st.code(explain, language="sql")
            st.dataframe(session.sql(explain).collect(), use_container_width=True)
        except Exception as e:
            st.error(f"Explain failed: {e}")
    else:
        st.info("Prepare tables and provide SQL in the Query tab.")

with tabs[2]:
    st.subheader("Description (AI)")
    if sql_query.strip():
        try:
            st.write(ai_text("Explain what this query does in plain English. Mention joins/filters and expected output.", sql_query))
        except Exception as e:
            st.error(f"AI description failed: {e}")
    else:
        st.info("Enter or generate SQL first.")

with tabs[3]:
    st.subheader("Comments (AI)")
    if sql_query.strip():
        try:
            resp = ai_text("Add inline comments to improve readability. Return a commented SQL query.", sql_query)
            extracted = extract_sql_codeblock(resp) or resp
            st.code(extracted, language="sql")
        except Exception as e:
            st.error(f"AI comments failed: {e}")
    else:
        st.info("Enter or generate SQL first.")

with tabs[4]:
    st.subheader("Optimization (AI)")
    if sql_query.strip():
        try:
            st.write(ai_text("Optimize this query for Snowflake best practices. Provide reasoning and an improved SQL if applicable.", sql_query))
        except Exception as e:
            st.error(f"AI optimization failed: {e}")
    else:
        st.info("Enter or generate SQL first.")

with tabs[5]:
    st.subheader("Encapsulation (AI)")
    if sql_query.strip():
        try:
            resp = ai_text("Create a Snowflake stored procedure or view wrapper for this query, if reasonable. Return SQL only.", sql_query)
            extracted = extract_sql_codeblock(resp) or resp
            st.code(extracted, language="sql")
        except Exception as e:
            st.error(f"AI encapsulation failed: {e}")
    else:
        st.info("Enter or generate SQL first.")
