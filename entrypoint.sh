#!/bin/bash

# Function to run DLT extraction and DBT transformations (for initial seeding)
run_data_pipeline() {
    echo "$(date): Starting QuickBooks data pipeline..."
    
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
    
    echo "$(date): Data pipeline completed successfully!"
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

# Function to run the complete pipeline including tests
run_pipeline() {
    run_data_pipeline
    run_tests
}

# Handle different execution modes
case "$1" in
    "cron")
        echo "$(date): Starting cron daemon..."
        # Start cron in foreground mode
        exec cron -f
        ;;
    "run-now")
        echo "$(date): Running complete pipeline immediately..."
        run_pipeline
        ;;
    "seed"|"run-data")
        echo "$(date): Running data pipeline only (no tests)..."
        run_data_pipeline
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