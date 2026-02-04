# Snowflake SQL Deployment Tool

A Streamlit application for deploying SQL scripts to Snowflake with query tagging, logging, and SOX compliance support.

## Features

- Execute SQL scripts with automatic statement parsing
- Support for stored procedures and functions with `$$` delimiters
- Query tagging for audit trails
- Deployment logging with downloadable logs
- Works both standalone and as Streamlit in Snowflake (SiS)

## Quick Start

### Standalone (Local Development)

1. Create `.streamlit/secrets.toml`:
```toml
[connections.snowflake]
account = "your_account"
user = "your_user"
password = "your_password"
warehouse = "your_warehouse"
```

2. Run locally:
```bash
streamlit run streamlit_app.py
```

### Deploy to Snowflake (Container Services)

This project deploys Streamlit to Snowpark Container Services for better performance and isolation.

#### Prerequisites

1. **Compute Pool** - A compute pool for running the app (e.g., `SYSTEM_COMPUTE_POOL_CPU`)
2. **External Access Integration** - For PyPI package installation:
```sql
CREATE OR REPLACE NETWORK RULE PYPI_NETWORK_RULE
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('pypi.org', 'files.pythonhosted.org');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION PYPI_ACCESS_INTEGRATION
  ALLOWED_NETWORK_RULES = (PYPI_NETWORK_RULE)
  ENABLED = TRUE;
```

#### Deploy

```bash
# Using defaults (edit snowflake.yml for database/schema)
./deploy.sh

# Create missing database/schema/compute pool
./deploy.sh --create

# Use specific connection
./deploy.sh -c my_connection

# Override compute pool
./deploy.sh --compute-pool MY_COMPUTE_POOL

# Override EAI name
./deploy.sh --eai MY_PYPI_INTEGRATION
```

#### Clean Up

```bash
./clean.sh
./clean.sh -c my_connection
```

## Configuration

Edit `snowflake.yml` to change deployment target:
```yaml
definition_version: 2
entities:
  sql_deploy_tool:
    type: streamlit
    identifier:
      name: SQL_DEPLOY_TOOL
      database: STREAMLIT_APPS
      schema: PUBLIC
    query_warehouse: COMPUTE_WH
    main_file: streamlit_app.py
    artifacts:
      - streamlit_app.py
      - requirements.txt
```

Environment variables for `deploy.sh`:
| Variable | Default | Description |
|----------|---------|-------------|
| `COMPUTE_POOL` | `SYSTEM_COMPUTE_POOL_CPU` | Container compute pool |
| `RUNTIME_NAME` | `SYSTEM$ST_CONTAINER_RUNTIME_PY3_11` | Python runtime |
| `STAGE_NAME` | `STREAMLIT_STAGE` | Stage for app files |
| `EAI_NAME` | `PYPI_ACCESS_INTEGRATION` | External access integration |

## Usage

1. Select a database
2. Enter a query tag (e.g., `JIRA-1234`)
3. Optionally select a schema
4. Paste your SQL script
5. Enable **Single Statement Mode** for stored procedures
6. Click **Run Deployment**
7. Download the deployment log for audit records

## Project Structure

```
sql_deploy_tool/
  deploy.sh           # Deploy to Snowflake
  clean.sh            # Remove from Snowflake
  snowflake.yml       # Deployment manifest
  streamlit_app.py    # Main application
  requirements.txt    # Python dependencies
  README.md
```
