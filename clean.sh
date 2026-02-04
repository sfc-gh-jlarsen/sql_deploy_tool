#!/bin/bash
set -e

# Parse arguments
CONNECTION=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -c|--connection)
            CONNECTION="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: ./clean.sh [-c <connection>]"
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

# Parse snowflake.yml for app location
DATABASE=$(grep -A2 'identifier:' snowflake.yml | grep 'database:' | awk '{print $2}')
SCHEMA=$(grep -A3 'identifier:' snowflake.yml | grep 'schema:' | awk '{print $2}')
APP_NAME=$(grep -A1 'identifier:' snowflake.yml | grep 'name:' | awk '{print $2}')

echo ""
echo "=== Cleaning up SQL Deploy Tool ==="
echo ""
echo "Dropping: $DATABASE.$SCHEMA.$APP_NAME"

snow sql -q "DROP STREAMLIT IF EXISTS $DATABASE.$SCHEMA.$APP_NAME" $CONN_FLAG || echo "Streamlit app not found or already dropped"

echo ""
echo "=== Cleanup Complete ==="
