"""
Snowflake SQL Deployment Tool
"""

import streamlit as st
from datetime import datetime
import re

# Try to import Snowpark for Streamlit in Snowflake
try:
    from snowflake.snowpark.context import get_active_session
    IN_SNOWFLAKE = True
except ImportError:
    IN_SNOWFLAKE = False

# Page Setup
st.set_page_config(page_title="Snowflake Deployment Tool", page_icon="❄️", layout="wide")

# Minimal Styling
st.markdown("""
<style>
    .main .block-container { max-width: 1000px; padding-top: 2rem; }
    .stTextArea textarea { font-family: monospace; }
</style>
""", unsafe_allow_html=True)


# --- Session State Initialization ---
def init_session_state():
    defaults = {
        'connected': False,
        'selected_database': None,
        'selected_schema': None,
        'deployment_log': None,
        'deployment_status': None,
        'log_filename': None
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


# --- Get Snowflake Connection ---
def get_snowflake_session():
    """Get Snowflake session - works in both SiS and standalone Streamlit."""
    if IN_SNOWFLAKE:
        return get_active_session()
    else:
        return st.connection('snowflake').session()


def run_query(sql):
    """Run a SQL query and return results as pandas DataFrame."""
    if IN_SNOWFLAKE:
        session = get_active_session()
        return session.sql(sql).to_pandas()
    else:
        conn = st.connection('snowflake')
        return conn.query(sql)


def execute_sql(sql, is_procedure=False):
    """
    Execute a SQL statement without expecting a result set.
    Uses Snowpark session.sql().collect() for proper DDL execution.
    This is essential for DDL statements like CREATE PROCEDURE.
    
    Args:
        sql: The SQL statement to execute
        is_procedure: If True, use special handling for CREATE PROCEDURE/FUNCTION
    """
    if IN_SNOWFLAKE:
        session = get_active_session()
        
        if is_procedure:
            # For stored procedures in SiS, we need to be extra careful
            # Ensure the SQL is properly formatted
            clean_sql = sql.strip()
            
            # Try direct execution first
            try:
                result = session.sql(clean_sql).collect()
                return result
            except Exception as e:
                error_str = str(e)
                # If we get a parsing error, try alternative approaches
                if 'parse error' in error_str.lower() or 'unexpected' in error_str.lower():
                    # Try wrapping in EXECUTE IMMEDIATE (for some edge cases)
                    # This likely won't help but we log the attempt
                    raise Exception(f"Procedure creation failed: {error_str}\n\nNote: This may be a Streamlit-in-Snowflake limitation with $$ delimiters. Try deploying this procedure directly via Snowflake Worksheets.")
                raise e
        else:
            # Standard DDL execution
            result = session.sql(sql).collect()
            return result
    else:
        # Use Snowpark session for proper execution
        # session.sql().collect() handles $$ delimiters correctly
        conn = st.connection('snowflake')
        session = conn.session()
        result = session.sql(sql).collect()
        return result


def set_database(database):
    """Set the current database context."""
    quoted_db = f'"{database}"'
    if IN_SNOWFLAKE:
        # In Streamlit in Snowflake, USE statements may not be supported
        # Try the session method first, fall back to noting it was skipped
        try:
            session = get_active_session()
            session.use_database(quoted_db)
            return True, None
        except Exception as e:
            if 'Unsupported statement' in str(e) or 'USE' in str(e):
                # USE not supported in this context - will use fully qualified names
                return False, "USE DATABASE not supported in this context - using fully qualified names"
            raise e
    else:
        conn = st.connection('snowflake')
        conn.query(f'USE DATABASE {quoted_db}')
        return True, None


def set_schema(schema):
    """Set the current schema context."""
    quoted_schema = f'"{schema}"'
    if IN_SNOWFLAKE:
        # In Streamlit in Snowflake, USE statements may not be supported
        try:
            session = get_active_session()
            session.use_schema(quoted_schema)
            return True, None
        except Exception as e:
            if 'Unsupported statement' in str(e) or 'USE' in str(e):
                return False, "USE SCHEMA not supported in this context - using fully qualified names"
            raise e
    else:
        conn = st.connection('snowflake')
        conn.query(f'USE SCHEMA {quoted_schema}')
        return True, None


# --- Fetch Databases ---
def get_databases():
    """Fetch list of accessible databases."""
    try:
        result = run_query("SHOW DATABASES")
        if result is not None and len(result) > 0:
            # Find 'name' column - handle quoted column names like "name"
            name_col = None
            for col in result.columns:
                # Strip quotes and compare case-insensitively
                clean_col = col.strip('"').upper()
                if clean_col == 'NAME':
                    name_col = col
                    break
            
            if name_col:
                databases = [str(db) for db in result[name_col].tolist() if db]
                return sorted(databases), None
            else:
                return [], f"Could not find 'name' column. Available: {list(result.columns)}"
        return [], "No databases returned from Snowflake"
    except Exception as e:
        return [], str(e)


# --- Get Current Role ---
def get_current_role():
    """Get current Snowflake role for logging."""
    try:
        result = run_query("SELECT CURRENT_ROLE() AS CURRENT_ROLE")
        if result is not None and len(result) > 0:
            return str(result.iloc[0]['CURRENT_ROLE'])
        return "Unknown"
    except Exception:
        return "Unknown"


# --- Get Current User ---
def get_current_user():
    """Get current Snowflake user."""
    try:
        result = run_query("SELECT CURRENT_USER() AS CURRENT_USER")
        if result is not None and len(result) > 0:
            return str(result.iloc[0]['CURRENT_USER'])
        return "Unknown"
    except Exception:
        return "Unknown"


# --- Get Current Warehouse ---
def get_current_warehouse():
    """Get current Snowflake warehouse."""
    try:
        result = run_query("SELECT CURRENT_WAREHOUSE() AS CURRENT_WAREHOUSE")
        if result is not None and len(result) > 0:
            wh = result.iloc[0]['CURRENT_WAREHOUSE']
            return str(wh) if wh else "None"
        return "Unknown"
    except Exception:
        return "Unknown"


# --- Fetch Schemas ---
def get_schemas(database):
    """Fetch list of schemas in a database."""
    try:
        result = run_query(f'SHOW SCHEMAS IN DATABASE "{database}"')
        if result is not None and len(result) > 0:
            name_col = None
            for col in result.columns:
                if col.strip('"').upper() == 'NAME':
                    name_col = col
                    break
            if name_col:
                schemas = [str(s) for s in result[name_col].tolist() if s]
                return sorted(schemas), None
        return [], None
    except Exception as e:
        return [], str(e)


# --- SQL Parsing ---
def parse_sql(sql_script, single_statement_mode=False):
    """
    Parse SQL script into individual statements.
    
    Args:
        sql_script: The SQL script to parse
        single_statement_mode: If True, treat entire script as one statement
    
    Handles:
    - Stored procedures/functions with $$ delimiters
    - Custom delimiters like $body$, $func$, etc.
    - String literals with semicolons
    - Block comments
    """
    if single_statement_mode:
        # Return entire script as single statement
        script = sql_script.strip()
        if script:
            return [script]
        return []
    
    # Find all dollar-quoted blocks and temporarily replace them
    # Pattern matches: $$, $body$, $func$, $1$, $tag123$, etc.
    dollar_pattern = r'(\$[a-zA-Z0-9_]*\$)([\s\S]*?)\1'
    placeholders = {}
    placeholder_idx = 0
    
    def replace_dollar_block(match):
        nonlocal placeholder_idx
        placeholder = f"__DOLLAR_BLOCK_{placeholder_idx}__"
        placeholders[placeholder] = match.group(0)
        placeholder_idx += 1
        return placeholder
    
    # Replace $$ blocks with placeholders (DOTALL flag for multiline)
    protected_script = re.sub(dollar_pattern, replace_dollar_block, sql_script, flags=re.DOTALL)
    
    # Now split on semicolons (but not inside strings)
    statements = []
    current_stmt = ""
    in_string = False
    string_char = None
    i = 0
    
    while i < len(protected_script):
        char = protected_script[i]
        
        # Handle string literals
        if char in ("'",) and not in_string:
            in_string = True
            string_char = char
            current_stmt += char
        elif in_string and char == string_char:
            # Check for escaped quote ('')
            if i + 1 < len(protected_script) and protected_script[i + 1] == string_char:
                current_stmt += char + char
                i += 1
            else:
                in_string = False
                string_char = None
                current_stmt += char
        elif char == ';' and not in_string:
            # Statement terminator found
            stmt = current_stmt.strip()
            if stmt:
                # Restore dollar blocks
                for placeholder, original in placeholders.items():
                    stmt = stmt.replace(placeholder, original)
                # Check it's not just comments
                if any(line.strip() and not line.strip().startswith('--') for line in stmt.split('\n')):
                    statements.append(stmt)
            current_stmt = ""
        else:
            current_stmt += char
        i += 1
    
    # Handle last statement (no trailing semicolon)
    stmt = current_stmt.strip()
    if stmt:
        # Restore dollar blocks
        for placeholder, original in placeholders.items():
            stmt = stmt.replace(placeholder, original)
        if any(line.strip() and not line.strip().startswith('--') for line in stmt.split('\n')):
            statements.append(stmt)
    
    return statements


def get_statement_type(stmt):
    """Identify the type of SQL statement for logging."""
    stmt_upper = stmt.strip().upper()
    
    # DDL statements
    if stmt_upper.startswith('CREATE'):
        if 'PROCEDURE' in stmt_upper:
            return 'CREATE PROCEDURE'
        elif 'FUNCTION' in stmt_upper:
            return 'CREATE FUNCTION'
        elif 'TABLE' in stmt_upper:
            return 'CREATE TABLE'
        elif 'VIEW' in stmt_upper:
            return 'CREATE VIEW'
        elif 'SCHEMA' in stmt_upper:
            return 'CREATE SCHEMA'
        elif 'DATABASE' in stmt_upper:
            return 'CREATE DATABASE'
        elif 'TASK' in stmt_upper:
            return 'CREATE TASK'
        elif 'STREAM' in stmt_upper:
            return 'CREATE STREAM'
        elif 'STAGE' in stmt_upper:
            return 'CREATE STAGE'
        elif 'PIPE' in stmt_upper:
            return 'CREATE PIPE'
        return 'CREATE'
    elif stmt_upper.startswith('ALTER'):
        return 'ALTER'
    elif stmt_upper.startswith('DROP'):
        return 'DROP'
    elif stmt_upper.startswith('TRUNCATE'):
        return 'TRUNCATE'
    
    # DML statements
    elif stmt_upper.startswith('INSERT'):
        return 'INSERT'
    elif stmt_upper.startswith('UPDATE'):
        return 'UPDATE'
    elif stmt_upper.startswith('DELETE'):
        return 'DELETE'
    elif stmt_upper.startswith('MERGE'):
        return 'MERGE'
    
    # Query statements
    elif stmt_upper.startswith('SELECT'):
        return 'SELECT'
    elif stmt_upper.startswith('WITH'):
        return 'SELECT (CTE)'
    
    # Procedure/Function calls
    elif stmt_upper.startswith('CALL'):
        return 'CALL PROCEDURE'
    elif stmt_upper.startswith('EXECUTE'):
        return 'EXECUTE'
    
    # Transaction control
    elif stmt_upper.startswith('BEGIN'):
        if 'TRANSACTION' in stmt_upper:
            return 'BEGIN TRANSACTION'
        return 'BEGIN BLOCK'
    elif stmt_upper.startswith('COMMIT'):
        return 'COMMIT'
    elif stmt_upper.startswith('ROLLBACK'):
        return 'ROLLBACK'
    
    # Data loading
    elif stmt_upper.startswith('COPY'):
        return 'COPY'
    elif stmt_upper.startswith('PUT'):
        return 'PUT'
    elif stmt_upper.startswith('GET'):
        return 'GET'
    
    # Permissions
    elif stmt_upper.startswith('GRANT'):
        return 'GRANT'
    elif stmt_upper.startswith('REVOKE'):
        return 'REVOKE'
    
    # Other
    elif stmt_upper.startswith('USE'):
        return 'USE'
    elif stmt_upper.startswith('SET'):
        return 'SET'
    elif stmt_upper.startswith('SHOW'):
        return 'SHOW'
    elif stmt_upper.startswith('DESCRIBE') or stmt_upper.startswith('DESC'):
        return 'DESCRIBE'
    
    return 'SQL'


# --- Log Generation ---
def generate_log(entries, query_tag, user, role, database, schema, start, end, status):
    """Generate deployment log."""
    schema_line = f"Schema: {schema}\n" if schema else ""
    header = f"""{'='*60}
SNOWFLAKE DEPLOYMENT LOG
{'='*60}
Query Tag: {query_tag}
User: {user}
Role: {role}
Database: {database}
{schema_line}Start: {start}
End: {end}
Status: {status}
{'='*60}
"""
    return header + "\n".join(entries)


# --- Database Selection Screen ---
def show_database_selection():
    st.title("❄️ Snowflake SQL Deployment Tool")
    st.markdown("Select a database to begin deployment.")
    
    # Fetch databases
    with st.spinner("Loading databases..."):
        databases, error = get_databases()
    
    if error:
        st.error(f"**Connection Error:** {error}")
        
        with st.expander("Troubleshooting"):
            st.markdown("""
**For Streamlit in Snowflake:**
- Ensure the app has proper permissions
- Check that your role can access databases

**For Standalone Streamlit:**
- Create `.streamlit/secrets.toml` with:
```toml
[connections.snowflake]
account = "your_account"
user = "your_user"
password = "your_password"
warehouse = "your_warehouse"
```
            """)
        
        if st.button("🔄 Retry"):
            st.cache_data.clear()
            st.rerun()
        return
    
    if not databases:
        st.warning("No databases found. Check your Snowflake permissions.")
        if st.button("🔄 Refresh"):
            st.cache_data.clear()
            st.rerun()
        return
    
    # Database selection
    st.subheader("Select Database")
    selected_db = st.selectbox(
        "Database",
        options=databases,
        index=None,
        placeholder="Choose a database...",
        label_visibility="collapsed"
    )
    
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        if st.button("🔗 Connect to Database", type="primary", use_container_width=True, disabled=not selected_db):
            st.session_state.selected_database = selected_db
            st.session_state.connected = True
            st.rerun()


# --- Deployment Interface ---
def show_deployment_interface():
    database = st.session_state.selected_database
    
    st.title("❄️ Snowflake SQL Deployment Tool")
    st.success(f"✓ Connected to: **{database}**")
    
    # Sidebar - Session Info
    with st.sidebar:
        st.subheader("Session Info")
        st.text(f"User: {get_current_user()}")
        st.text(f"Role: {get_current_role()}")
        st.text(f"Warehouse: {get_current_warehouse()}")
        st.text(f"Database: {database}")
        if st.session_state.selected_schema:
            st.text(f"Schema: {st.session_state.selected_schema}")
        
        st.divider()
        if st.button("🔄 Change Database"):
            st.session_state.connected = False
            st.session_state.selected_database = None
            st.session_state.selected_schema = None
            st.session_state.deployment_log = None
            st.rerun()
    
    # Step 1: Query Tag
    st.subheader("1. Query Tag")
    query_tag = st.text_input(
        "Query Tag",
        placeholder="e.g., JIRA-1234, RELEASE-v2.1.0",
        label_visibility="collapsed"
    )
    
    if not query_tag:
        st.warning("Enter a Query Tag to continue.")
        return
    
    # Step 2: Schema Selection (Optional)
    st.subheader("2. Schema (Optional)")
    schemas, schema_error = get_schemas(database)
    if schemas:
        selected_schema = st.selectbox(
            "Schema",
            options=["-- No schema (use fully qualified names) --"] + schemas,
            index=0,
            label_visibility="collapsed"
        )
        if selected_schema != "-- No schema (use fully qualified names) --":
            st.session_state.selected_schema = selected_schema
        else:
            st.session_state.selected_schema = None
    else:
        st.session_state.selected_schema = None
        if schema_error:
            st.caption(f"⚠️ Could not load schemas: {schema_error}")
    
    # Step 3: SQL Script
    st.subheader("3. SQL Script")
    sql_script = st.text_area(
        "SQL",
        height=300,
        placeholder="-- Enter SQL statements separated by semicolons (;)\n-- Stored procedures with $$ delimiters are supported",
        label_visibility="collapsed"
    )
    
    # Single statement mode toggle
    single_stmt_mode = st.checkbox(
        "📦 Single Statement Mode",
        help="Enable this if you're deploying a single complex object (e.g., a stored procedure) that shouldn't be split by semicolons."
    )
    
    # Guidelines expander
    with st.expander("📘 SQL Guidelines & Best Practices", expanded=False):
        st.markdown("""
### Stored Procedures & Functions
When deploying stored procedures, ensure your SQL includes all required components:

```sql
CREATE OR REPLACE PROCEDURE schema.procedure_name()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$                          -- ← Opening delimiter (REQUIRED)
DECLARE
    result VARCHAR;
BEGIN
    BEGIN TRANSACTION;      -- ← If using transactions
    
    -- Your SQL logic here
    
    COMMIT;                 -- ← Required if BEGIN TRANSACTION used
    result := 'Success';
    RETURN result;          -- ← Required if RETURNS is declared
END;                        -- ← Required to close BEGIN block
$$                          -- ← Closing delimiter (REQUIRED)
```

### ✅ Checklist Before Deploying
| Component | Required When |
|-----------|--------------|
| `$$` opening delimiter | Always for procedures/functions |
| `$$` closing delimiter | Always - must match opening |
| `END;` | Always - closes the `BEGIN` block |
| `COMMIT;` | When using `BEGIN TRANSACTION` |
| `RETURN value;` | When `RETURNS` is declared |

### ⚠️ Common Errors & Solutions
| Error | Cause | Fix |
|-------|-------|-----|
| `unexpected '$'` | Missing/mismatched `$$` | Ensure both opening AND closing `$$` exist |
| `unexpected '<EOF>'` | Incomplete procedure | Add missing `END;` and closing `$$` |
| `Transaction not committed` | Missing `COMMIT` | Add `COMMIT;` before `END;` |

### 💡 Tips
- **Enable Single Statement Mode** when deploying procedures/functions
- **Test in Snowflake Worksheets** first if unsure about syntax
- **No trailing spaces** after the final `$$` or `$$;`
- **Transactions**: Always pair `BEGIN TRANSACTION` with `COMMIT` or `ROLLBACK`
        """)
    
    # Preview parsed statements
    if sql_script.strip():
        statements = parse_sql(sql_script, single_statement_mode=single_stmt_mode)
        
        if single_stmt_mode:
            st.caption(f"📦 **Single Statement Mode** - entire script will be executed as one statement")
        else:
            st.caption(f"📋 **{len(statements)} statement(s)** will be executed")
        
        with st.expander("Preview Statements", expanded=False):
            for i, stmt in enumerate(statements, 1):
                stmt_type = get_statement_type(stmt)
                st.markdown(f"**{i}. {stmt_type}**")
                st.code(stmt[:1000] + ("..." if len(stmt) > 1000 else ""), language="sql")
                if i < len(statements):
                    st.divider()
        
        # Show warnings for potentially problematic statements
        warnings = []
        errors = []  # Critical errors that will prevent successful deployment
        
        for stmt in statements:
            stmt_type = get_statement_type(stmt)
            stmt_upper = stmt.upper()
            stmt_stripped = stmt.strip()
            
            # === STORED PROCEDURE VALIDATION ===
            if stmt_type in ['CREATE PROCEDURE', 'CREATE FUNCTION']:
                # Check for $$ delimiters
                dollar_count = stmt.count('$$')
                if dollar_count == 0:
                    errors.append(f"❌ **{stmt_type}**: Missing `$$` delimiters - procedure body must be wrapped in `$$ ... $$`")
                elif dollar_count == 1:
                    errors.append(f"❌ **{stmt_type}**: Only ONE `$$` found - missing closing `$$` delimiter!")
                elif dollar_count % 2 != 0:
                    errors.append(f"❌ **{stmt_type}**: Odd number of `$$` delimiters ({dollar_count}) - check for missing opening/closing `$$`")
                
                # Check for BEGIN/END balance
                begin_count = len(re.findall(r'\bBEGIN\b', stmt_upper))
                end_count = len(re.findall(r'\bEND\b', stmt_upper))
                # Note: BEGIN TRANSACTION counts as BEGIN but doesn't need matching END
                begin_transaction_count = len(re.findall(r'\bBEGIN\s+TRANSACTION\b', stmt_upper))
                procedural_begins = begin_count - begin_transaction_count
                
                if procedural_begins > end_count:
                    errors.append(f"❌ **{stmt_type}**: Missing `END;` - found {procedural_begins} BEGIN block(s) but only {end_count} END statement(s)")
                
                # Check for COMMIT if BEGIN TRANSACTION is used
                if 'BEGIN TRANSACTION' in stmt_upper or 'BEGIN WORK' in stmt_upper:
                    if 'COMMIT' not in stmt_upper and 'ROLLBACK' not in stmt_upper:
                        errors.append(f"❌ **{stmt_type}**: Has `BEGIN TRANSACTION` but no `COMMIT` or `ROLLBACK` - transaction will not complete!")
                
                # Check for RETURN statement
                if 'RETURNS' in stmt_upper and 'RETURN ' not in stmt_upper and 'RETURN\n' not in stmt_upper:
                    warnings.append(f"⚠️ **{stmt_type}**: Declares RETURNS but no RETURN statement found")
                
                # Check that procedure ends correctly (should end with $$ or $$;)
                if dollar_count >= 2:
                    # Find content after last $$
                    last_dollar_pos = stmt.rfind('$$')
                    after_dollar = stmt[last_dollar_pos+2:].strip()
                    if after_dollar and after_dollar not in [';', '']:
                        warnings.append(f"⚠️ **{stmt_type}**: Unexpected content after closing `$$`: '{after_dollar[:20]}...'")
            
            # === GENERAL WARNINGS ===
            if stmt_type in ['DROP', 'TRUNCATE', 'DELETE']:
                warnings.append(f"⚠️ **{stmt_type}** statement detected - data may be permanently deleted")
            if 'GRANT' in stmt_upper and 'ACCOUNTADMIN' in stmt_upper:
                warnings.append(f"⚠️ **GRANT** to ACCOUNTADMIN detected - verify this is intended")
            if stmt_type == 'BEGIN TRANSACTION':
                warnings.append(f"⚠️ **Transaction** detected - ensure COMMIT/ROLLBACK is included")
        
        # Show errors first (these will cause deployment to fail)
        if errors:
            st.error("**🚫 CRITICAL: These issues will cause deployment to FAIL:**\n\n" + "\n".join(errors))
        
        if warnings:
            st.warning("**Review these items before deploying:**\n\n" + "\n".join(warnings))
    
    # Step 4: Deploy Button
    st.subheader("4. Deploy")
    
    col1, col2 = st.columns([3, 1])
    with col1:
        deploy = st.button("🚀 Run Deployment", type="primary", disabled=not sql_script.strip(), use_container_width=True)
    with col2:
        if st.session_state.deployment_log:
            if st.button("🗑️ Clear Log", use_container_width=True):
                st.session_state.deployment_log = None
                st.session_state.deployment_status = None
                st.rerun()
    
    if deploy:
        run_deployment(query_tag, database, st.session_state.selected_schema, sql_script, single_stmt_mode)
    
    # Show Results
    if st.session_state.deployment_log:
        st.divider()
        col1, col2 = st.columns([3, 1])
        with col2:
            st.download_button(
                "📥 Download Log",
                st.session_state.deployment_log,
                st.session_state.log_filename,
                "text/plain"
            )
        
        status = st.session_state.deployment_status
        if status == "SUCCESS":
            st.success("✅ Deployment Successful")
        else:
            st.error("❌ Deployment Failed")
        
        with st.expander("View Log", expanded=True):
            st.code(st.session_state.deployment_log, language="text")


# --- Deployment Execution ---
def run_deployment(query_tag, database, schema, sql_script, single_statement_mode=False):
    st.session_state.deployment_log = None
    st.session_state.deployment_status = None
    
    log_entries = []
    failed = False
    start_time = datetime.now()
    current_role = get_current_role()
    current_user = get_current_user()
    
    progress = st.progress(0)
    status = st.empty()
    
    try:
        # Connect
        status.info("Connecting to Snowflake...")
        log_entries.append(f"[{datetime.now()}] Connected to Snowflake")
        log_entries.append(f"[{datetime.now()}] User: {current_user}, Role: {current_role}")
        progress.progress(5)
        
        # Set Database
        status.info(f"Setting database: {database}")
        db_success, db_msg = set_database(database)
        if db_success:
            log_entries.append(f"[{datetime.now()}] Database set: {database}")
        else:
            log_entries.append(f"[{datetime.now()}] Database context: {database} (note: {db_msg})")
            st.info(f"ℹ️ {db_msg}. Make sure your SQL uses fully qualified object names (DATABASE.SCHEMA.OBJECT).")
        progress.progress(10)
        
        # Set Schema (if selected)
        if schema:
            status.info(f"Setting schema: {schema}")
            schema_success, schema_msg = set_schema(schema)
            if schema_success:
                log_entries.append(f"[{datetime.now()}] Schema set: {schema}")
            else:
                log_entries.append(f"[{datetime.now()}] Schema context: {schema} (note: {schema_msg})")
        progress.progress(15)
        
        # Set Query Tag (REQUIRED for SOX compliance)
        status.info(f"Setting query tag: {query_tag}")
        query_tag_set = False
        query_tag_method = None
        
        # Method 1: Try Snowpark session property
        if IN_SNOWFLAKE:
            try:
                session = get_active_session()
                session.query_tag = query_tag
                query_tag_set = True
                query_tag_method = "session property"
            except Exception:
                pass
        
        # Method 2: Try ALTER SESSION
        if not query_tag_set:
            try:
                safe_tag = query_tag.replace("'", "''")
                run_query(f"ALTER SESSION SET QUERY_TAG = '{safe_tag}'")
                query_tag_set = True
                query_tag_method = "ALTER SESSION"
            except Exception:
                pass
        
        # Method 3: Embed query tag as comment in SQL (always works)
        if not query_tag_set:
            query_tag_set = True
            query_tag_method = "SQL comment prefix"
            log_entries.append(f"[{datetime.now()}] Query tag will be embedded as SQL comment: /* QUERY_TAG: {query_tag} */")
            st.info(f"ℹ️ Query tag will be embedded as a comment in each SQL statement for traceability.")
        
        if query_tag_method != "SQL comment prefix":
            log_entries.append(f"[{datetime.now()}] Query tag set via {query_tag_method}: {query_tag}")
        
        progress.progress(20)
        
        # Parse and Execute
        statements = parse_sql(sql_script, single_statement_mode=single_statement_mode)
        if not statements:
            status.warning("No valid SQL statements found.")
            log_entries.append(f"[{datetime.now()}] No statements to execute")
        else:
            status.info(f"Executing {len(statements)} statement(s)...")
            
            for i, stmt in enumerate(statements, 1):
                pct = 20 + int((i / len(statements)) * 70)
                progress.progress(pct)
                
                stmt_type = get_statement_type(stmt)
                stmt_preview = stmt[:100].replace('\n', ' ')
                if len(stmt) > 100:
                    stmt_preview += "..."
                
                # Embed query tag as comment if that method is being used
                # EXCEPTION: Don't prepend comments for CREATE PROCEDURE/FUNCTION
                # as it can interfere with $$ delimiter parsing in SiS
                if query_tag_method == "SQL comment prefix":
                    if stmt_type in ['CREATE PROCEDURE', 'CREATE FUNCTION']:
                        # For procedures/functions, append comment at the end instead
                        # or skip it entirely to avoid $$ parsing issues
                        stmt_to_execute = stmt
                        log_entries.append(f"   Note: Query tag comment skipped for {stmt_type} to preserve $$ delimiters")
                    else:
                        stmt_to_execute = f"/* QUERY_TAG: {query_tag} | User: {current_user} | Role: {current_role} | Timestamp: {datetime.now()} */\n{stmt}"
                else:
                    stmt_to_execute = stmt
                
                try:
                    # Use execute_sql() for DDL statements (CREATE, ALTER, DROP, GRANT, etc.)
                    # These need raw cursor execution to avoid semicolon splitting issues
                    ddl_types = [
                        'CREATE', 'ALTER', 'DROP', 'TRUNCATE', 'GRANT', 'REVOKE',
                        'BEGIN', 'COMMIT', 'ROLLBACK', 'USE', 'SET'
                    ]
                    is_ddl = any(stmt_type.startswith(ddl) for ddl in ddl_types)
                    
                    # For stored procedures, strip trailing semicolon after $$
                    # Snowflake sometimes has issues with trailing ; after $$
                    final_stmt = stmt_to_execute.rstrip()
                    if final_stmt.endswith(';') and '$$' in final_stmt:
                        # Only strip if the statement ends with $$; or $$ ;
                        if final_stmt.rstrip('; \t\n').endswith('$$'):
                            final_stmt = final_stmt.rstrip('; \t\n')
                    
                    if is_ddl:
                        # DDL statements - use Snowpark session execution
                        is_procedure = stmt_type in ['CREATE PROCEDURE', 'CREATE FUNCTION']
                        result = execute_sql(final_stmt, is_procedure=is_procedure)
                        result_summary = "Executed successfully"
                    else:
                        # DML/Query statements - use standard query method
                        result = run_query(final_stmt)
                        if result is not None and hasattr(result, '__len__') and len(result) > 0:
                            rows = len(result)
                            result_summary = f"{rows} row(s) returned/affected"
                        else:
                            result_summary = "Executed successfully"
                    
                    log_entries.append(f"[{datetime.now()}] ✓ {stmt_type}: {result_summary}")
                    log_entries.append(f"   SQL: {stmt_preview}")
                    
                except Exception as e:
                    error_msg = str(e)
                    log_entries.append(f"[{datetime.now()}] ✗ {stmt_type}: FAILED")
                    log_entries.append(f"   SQL: {stmt_preview}")
                    log_entries.append(f"   Error: {error_msg}")
                    failed = True
                    st.error(f"**Error in statement {i} ({stmt_type}):**\n\n{error_msg}")
                    
                    # Show debugging info for failed statements
                    with st.expander("🔍 Debug: View Full SQL Sent to Snowflake", expanded=True):
                        st.code(final_stmt, language="sql")
                        st.caption(f"Statement length: {len(final_stmt)} characters, {final_stmt.count(chr(10)) + 1} lines")
                        
                        # Check for common issues
                        issues = []
                        if '$$' in final_stmt:
                            dollar_count = final_stmt.count('$$')
                            if dollar_count % 2 != 0:
                                issues.append(f"⚠️ Odd number of $$ delimiters ({dollar_count}) - ensure opening and closing $$ match")
                            else:
                                issues.append(f"✓ Found {dollar_count // 2} $$ block(s)")
                        if 'BEGIN' in final_stmt.upper() and 'END' not in final_stmt.upper():
                            issues.append("⚠️ Found BEGIN without END")
                        if final_stmt.count('(') != final_stmt.count(')'):
                            issues.append(f"⚠️ Mismatched parentheses: {final_stmt.count('(')} open, {final_stmt.count(')')} close")
                        
                        if issues:
                            st.markdown("**Syntax checks:**\n" + "\n".join(issues))
                    break
        
        progress.progress(100)
        
    except Exception as e:
        failed = True
        log_entries.append(f"[{datetime.now()}] ERROR: {e}")
        st.error(f"Deployment error: {e}")
    
    # Generate Log
    end_time = datetime.now()
    final_status = "FAILED" if failed else "SUCCESS"
    
    st.session_state.deployment_log = generate_log(
        log_entries, query_tag, current_user, current_role, database, schema,
        start_time.strftime("%Y-%m-%d %H:%M:%S"),
        end_time.strftime("%Y-%m-%d %H:%M:%S"),
        final_status
    )
    st.session_state.deployment_status = final_status
    st.session_state.log_filename = f"deployment_{start_time.strftime('%Y%m%d_%H%M%S')}.txt"
    
    if not failed:
        status.success("✅ Deployment complete!")
    else:
        status.error("❌ Deployment failed.")


# --- Main ---
def main():
    init_session_state()
    
    if not st.session_state.connected:
        show_database_selection()
    else:
        show_deployment_interface()


if __name__ == "__main__":
    main()
