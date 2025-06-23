#!/bin/bash

# Function to run complete data pipeline (DLT + Domain Consolidation + DBT)
run_pipeline() {
    echo "$(date): Starting complete QuickBooks data pipeline..."
    
    # Change to app directory
    cd /app
    
    # Run complete pipeline (DLT extraction + Domain consolidation + DBT transformations)
    echo "$(date): Running complete pipeline (DLT + Domain consolidation + DBT)..."
    python pipeline.py
    if [ $? -ne 0 ]; then
        echo "$(date): Complete pipeline failed!" >&2
        exit 1
    fi
    
    echo "$(date): Complete data pipeline finished successfully!"
}

# Function to run DBT tests separately
run_tests() {
    echo "$(date): Running DBT tests..."
    cd /app
    dbt test
    if [ $? -ne 0 ]; then
        echo "$(date): DBT tests failed!" >&2
        exit 1
    fi
    echo "$(date): Tests completed successfully!"
}

# Handle different execution modes
case "$1" in
    "cron")
        echo "$(date): Starting cron daemon..."
        # Start cron in foreground mode
        exec cron -f
        ;;
    "run"|"seed")
        echo "$(date): Running complete pipeline..."
        run_pipeline
        ;;
    "test")
        echo "$(date): Running tests only..."
        run_tests
        ;;
    "shell")
        echo "$(date): Starting interactive shell..."
        exec /bin/bash
        ;;
    *)
        # If no argument or different argument, assume direct execution
        if [ $# -eq 0 ]; then
            # Default: start cron daemon
            echo "$(date): Starting cron daemon (default)..."
            exec cron -f
        else
            # Pass through any other commands
            exec "$@"
        fi
        ;;
esac