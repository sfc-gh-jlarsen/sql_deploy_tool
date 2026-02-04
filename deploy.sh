#!/bin/bash
set -e

# ============================================
# CONFIGURATION - Edit these or set via environment
# ============================================
COMPUTE_POOL="${COMPUTE_POOL:-SYSTEM_COMPUTE_POOL_CPU}"
RUNTIME_NAME="${RUNTIME_NAME:-SYSTEM\$ST_CONTAINER_RUNTIME_PY3_11}"
STAGE_NAME="${STAGE_NAME:-STREAMLIT_STAGE}"

# External Access Integration for PyPI (must exist in account)
# Create with: CREATE EXTERNAL ACCESS INTEGRATION PYPI_ACCESS_INTEGRATION ...
# See: https://docs.snowflake.com/en/developer-guide/external-network-access/creating-using-external-network-access
EAI_NAME="${EAI_NAME:-PYPI_ACCESS_INTEGRATION}"

# ============================================
# Parse arguments
# ============================================
CREATE_OBJECTS=false
CONNECTION=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --create)
            CREATE_OBJECTS=true
            shift
            ;;
        -c|--connection)
            CONNECTION="$2"
            shift 2
            ;;
        --compute-pool)
            COMPUTE_POOL="$2"
            shift 2
            ;;
        --eai)
            EAI_NAME="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: ./deploy.sh [--create] [-c <connection>] [--compute-pool <pool>] [--eai <integration>]"
            exit 1
            ;;
    esac
done

# Build connection flag for snow commands
CONN_FLAG=""
if [ -n "$CONNECTION" ]; then
    CONN_FLAG="-c $CONNECTION"
    echo "Using connection: $CONNECTION"
else
    echo "Using default connection"
fi

# Parse snowflake.yml for required values
DATABASE=$(grep -A2 'identifier:' snowflake.yml | grep 'database:' | awk '{print $2}')
SCHEMA=$(grep -A3 'identifier:' snowflake.yml | grep 'schema:' | awk '{print $2}')
APP_NAME=$(grep -A1 'identifier:' snowflake.yml | grep 'name:' | awk '{print $2}')
WAREHOUSE=$(grep 'query_warehouse:' snowflake.yml | awk '{print $2}')
MAIN_FILE=$(grep 'main_file:' snowflake.yml | awk '{print $2}')

echo ""
echo "=== SQL Deploy Tool - Container Services Deployment ==="
echo ""
echo "Configuration:"
echo "  Database:     $DATABASE"
echo "  Schema:       $SCHEMA"
echo "  App Name:     $APP_NAME"
echo "  Warehouse:    $WAREHOUSE"
echo "  Compute Pool: $COMPUTE_POOL"
echo "  Runtime:      $RUNTIME_NAME"
echo "  Stage:        $STAGE_NAME"
echo "  EAI:          $EAI_NAME"
echo ""

# Check connection
echo "Checking Snowflake connection..."
if ! snow connection test $CONN_FLAG > /dev/null 2>&1; then
    echo "ERROR: Cannot connect to Snowflake. Check your connection configuration."
    exit 1
fi
echo "Connection OK"
echo ""

# Check if database exists
echo "Checking database: $DATABASE"
DB_EXISTS=$(snow sql -q "SHOW DATABASES LIKE '$DATABASE'" $CONN_FLAG --format json 2>/dev/null | grep -c "\"name\": \"$DATABASE\"" || true)
if [ "$DB_EXISTS" -eq 0 ]; then
    if [ "$CREATE_OBJECTS" = true ]; then
        echo "  Creating database $DATABASE..."
        snow sql -q "CREATE DATABASE IF NOT EXISTS $DATABASE" $CONN_FLAG
    else
        echo "  ERROR: Database '$DATABASE' does not exist."
        echo "  Run with --create to create missing objects."
        exit 1
    fi
else
    echo "  Database exists"
fi

# Check if schema exists
echo "Checking schema: $DATABASE.$SCHEMA"
SCHEMA_EXISTS=$(snow sql -q "SHOW SCHEMAS LIKE '$SCHEMA' IN DATABASE $DATABASE" $CONN_FLAG --format json 2>/dev/null | grep -c "\"name\": \"$SCHEMA\"" || true)
if [ "$SCHEMA_EXISTS" -eq 0 ]; then
    if [ "$CREATE_OBJECTS" = true ]; then
        echo "  Creating schema $DATABASE.$SCHEMA..."
        snow sql -q "CREATE SCHEMA IF NOT EXISTS $DATABASE.$SCHEMA" $CONN_FLAG
    else
        echo "  ERROR: Schema '$DATABASE.$SCHEMA' does not exist."
        echo "  Run with --create to create missing objects."
        exit 1
    fi
else
    echo "  Schema exists"
fi

# Check if compute pool exists
echo "Checking compute pool: $COMPUTE_POOL"
POOL_EXISTS=$(snow sql -q "SHOW COMPUTE POOLS LIKE '$COMPUTE_POOL'" $CONN_FLAG --format json 2>/dev/null | grep -c "\"name\": \"$COMPUTE_POOL\"" || true)
if [ "$POOL_EXISTS" -eq 0 ]; then
    if [ "$CREATE_OBJECTS" = true ]; then
        echo "  Creating compute pool $COMPUTE_POOL..."
        snow sql -q "CREATE COMPUTE POOL IF NOT EXISTS $COMPUTE_POOL MIN_NODES = 1 MAX_NODES = 1 INSTANCE_FAMILY = CPU_X64_XS" $CONN_FLAG
    else
        echo "  ERROR: Compute pool '$COMPUTE_POOL' does not exist."
        echo "  Run with --create to create missing objects."
        exit 1
    fi
else
    echo "  Compute pool exists"
fi

echo ""
echo "All prerequisites verified."
echo ""

# Create stage if needed
echo "Creating stage if not exists..."
snow sql -q "CREATE STAGE IF NOT EXISTS $DATABASE.$SCHEMA.$STAGE_NAME" $CONN_FLAG

# Purge old files from stage folder
echo "Purging old files from stage @$DATABASE.$SCHEMA.$STAGE_NAME/$APP_NAME/..."
snow sql -q "REMOVE @$DATABASE.$SCHEMA.$STAGE_NAME/$APP_NAME/" $CONN_FLAG 2>/dev/null || true

# Upload files to stage
echo "Uploading files to stage @$DATABASE.$SCHEMA.$STAGE_NAME/$APP_NAME/..."
snow stage copy streamlit_app.py @$DATABASE.$SCHEMA.$STAGE_NAME/$APP_NAME/ --overwrite $CONN_FLAG
snow stage copy requirements.txt @$DATABASE.$SCHEMA.$STAGE_NAME/$APP_NAME/ --overwrite $CONN_FLAG

# Drop existing app if exists
echo "Dropping existing Streamlit app if exists..."
snow sql -q "DROP STREAMLIT IF EXISTS $DATABASE.$SCHEMA.$APP_NAME" $CONN_FLAG

# Create Streamlit app with container runtime
echo "Creating Streamlit app with container runtime..."
snow sql -q "
CREATE STREAMLIT $DATABASE.$SCHEMA.$APP_NAME
  FROM '@$DATABASE.$SCHEMA.$STAGE_NAME/$APP_NAME'
  MAIN_FILE = '$MAIN_FILE'
  RUNTIME_NAME = '$RUNTIME_NAME'
  COMPUTE_POOL = $COMPUTE_POOL
  QUERY_WAREHOUSE = $WAREHOUSE
  EXTERNAL_ACCESS_INTEGRATIONS = ($EAI_NAME)
" $CONN_FLAG

echo ""
echo "=== Deployment Complete ==="
echo ""
echo "Get app URL with:"
echo "  snow sql -q \"SELECT SYSTEM\\\$GET_STREAMLIT_URL('$DATABASE.$SCHEMA.$APP_NAME')\" $CONN_FLAG"
