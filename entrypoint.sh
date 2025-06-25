#!/bin/bash

# Function to run complete data pipeline using new orchestrator
run_pipeline() {
    echo "$(date): Starting multi-source data pipeline..."
    
    # Change to app directory
    cd /app
    
    # Run complete pipeline using orchestrator
    echo "$(date): Running complete pipeline (all sources + DBT + data quality)..."
    python orchestrator.py --mode full
    if [ $? -ne 0 ]; then
        echo "$(date): Complete pipeline failed!" >&2
        exit 1
    fi
    
    echo "$(date): Multi-source data pipeline finished successfully!"
}

# Function to run individual data source
run_source() {
    local source_name="$2"
    if [ -z "$source_name" ]; then
        echo "$(date): Error: Source name required for source mode" >&2
        echo "Usage: $0 source <source_name>" >&2
        exit 1
    fi
    
    echo "$(date): Running pipeline for source: $source_name"
    cd /app
    python orchestrator.py --mode source --source "$source_name"
    if [ $? -ne 0 ]; then
        echo "$(date): Source pipeline '$source_name' failed!" >&2
        exit 1
    fi
    echo "$(date): Source pipeline '$source_name' completed successfully!"
}

# Function to run DBT transformations only
run_dbt() {
    echo "$(date): Running DBT transformations only..."
    cd /app
    python orchestrator.py --mode dbt
    if [ $? -ne 0 ]; then
        echo "$(date): DBT transformations failed!" >&2
        exit 1
    fi
    echo "$(date): DBT transformations completed successfully!"
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

# Function to run data quality checks only
run_data_quality() {
    echo "$(date): Running data quality checks..."
    cd /app
    python orchestrator.py --mode data-quality
    if [ $? -ne 0 ]; then
        echo "$(date): Data quality checks failed!" >&2
        exit 1
    fi
    echo "$(date): Data quality checks completed successfully!"
}

# Handle different execution modes
case "$1" in
    "cron")
        echo "$(date): Starting cron daemon..."
        # Start cron in foreground mode
        exec cron -f
        ;;
    "run"|"seed"|"full")
        echo "$(date): Running complete pipeline..."
        run_pipeline
        ;;
    "source")
        echo "$(date): Running individual source pipeline..."
        run_source "$@"
        ;;
    "dbt")
        echo "$(date): Running DBT transformations only..."
        run_dbt
        ;;
    "test")
        echo "$(date): Running tests only..."
        run_tests
        ;;
    "data-quality")
        echo "$(date): Running data quality checks only..."
        run_data_quality
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