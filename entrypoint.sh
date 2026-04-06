#!/bin/bash

run_orchestrator() {
    local load_mode="$1"
    local source_name="${2:-}"

    cd /app

    if [ -n "$source_name" ]; then
        python orchestrator.py "--${load_mode}" --source "$source_name"
    else
        python orchestrator.py "--${load_mode}"
    fi

    if [ $? -ne 0 ]; then
        echo "$(date): Orchestrator failed for mode '${load_mode}'${source_name:+ and source '${source_name}'}!" >&2
        exit 1
    fi
}

# Function to run complete data pipeline using new orchestrator
run_pipeline() {
    echo "$(date): Starting multi-source data pipeline..."

    # "run"/"full" now means a full orchestrated incremental pass.
    echo "$(date): Running complete incremental pipeline (all enabled sources + DBT + data quality)..."
    run_orchestrator incremental

    echo "$(date): Multi-source data pipeline finished successfully!"
}

# Function to run seed data pipeline (historical data only)
run_seed_pipeline() {
    echo "$(date): Starting seed data pipeline..."

    echo "$(date): Running seed pipeline (historical data + DBT + data quality)..."
    run_orchestrator seed

    echo "$(date): Seed data pipeline finished successfully!"
}

# Function to run incremental data pipeline (latest daily data only)
run_incremental_pipeline() {
    echo "$(date): Starting incremental data pipeline..."

    echo "$(date): Running incremental pipeline (latest daily data + DBT)..."
    run_orchestrator incremental

    echo "$(date): Incremental data pipeline finished successfully!"
}

# Function to run individual data source
run_source() {
    local source_name="$2"
    local source_mode="${3:-incremental}"
    if [ -z "$source_name" ]; then
        echo "$(date): Error: Source name required for source mode" >&2
        echo "Usage: $0 source <source_name> [seed|incremental]" >&2
        exit 1
    fi

    case "$source_mode" in
        seed|incremental)
            ;;
        *)
            echo "$(date): Error: Unsupported source mode '$source_mode'. Use seed or incremental." >&2
            exit 1
            ;;
    esac

    echo "$(date): Running ${source_mode} pipeline for source: $source_name"
    run_orchestrator "$source_mode" "$source_name"
    echo "$(date): Source pipeline '$source_name' completed successfully!"
}

# Function to run DBT transformations only
run_dbt() {
    echo "$(date): Running DBT transformations only..."
    cd /app
    dbt run
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
    python -c 'from orchestrator import PipelineOrchestrator; import sys; result = PipelineOrchestrator().run_data_quality_checks(); print(result); sys.exit(0 if result.get("status") in ("completed", "success", "skipped") else 1)'
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
    "run"|"full")
        echo "$(date): Running complete pipeline..."
        run_pipeline
        ;;
    "seed")
        echo "$(date): Running seed pipeline (historical data)..."
        run_seed_pipeline
        ;;
    "incremental")
        echo "$(date): Running incremental pipeline (latest daily data)..."
        run_incremental_pipeline
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
