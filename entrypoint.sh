#!/bin/bash

# Function to run the complete pipeline
run_pipeline() {
    echo "$(date): Starting QuickBooks pipeline..."
    
    # Change to app directory
    cd /app
    
    # Run DLT pipeline first
    echo "$(date): Running DLT extraction..."
    python pipeline.py
    if [ $? -ne 0 ]; then
        echo "$(date): DLT pipeline failed!" >&2
        exit 1
    fi
    
    # Then run DBT transformations
    echo "$(date): Running DBT transformations..."
    dbt run
    if [ $? -ne 0 ]; then
        echo "$(date): DBT run failed!" >&2
        exit 1
    fi
    
    # Run DBT tests
    echo "$(date): Running DBT tests..."
    dbt test
    if [ $? -ne 0 ]; then
        echo "$(date): DBT tests failed!" >&2
        exit 1
    fi
    
    echo "$(date): Pipeline completed successfully!"
}

# Handle different execution modes
case "$1" in
    "cron")
        echo "$(date): Starting cron daemon..."
        # Start cron in foreground mode
        exec cron -f
        ;;
    "run-now")
        echo "$(date): Running pipeline immediately..."
        run_pipeline
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