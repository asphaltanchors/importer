# ABOUTME: Shopify e-commerce data pipeline using DLT verified source
# ABOUTME: Supports seed (full historical) and incremental (latest updates) loading modes

import os
import sys
import argparse
from datetime import datetime, timedelta
from typing import Dict, Any

import dlt
from shopify_dlt import shopify_source

# Add pipelines directory to path for shared imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from shared import (
    setup_logging,
    validate_environment_variables,
    get_dlt_destination
)

# Configure logging
logger = setup_logging("shopify", "INFO")

# Validate required environment variables
# Note: Shopify credentials are read from .dlt/secrets.toml by DLT automatically
REQUIRED_ENV_VARS = [
    "DATABASE_URL"
]

def validate_environment():
    """Validate all required environment variables are set"""
    try:
        env_vars = validate_environment_variables(REQUIRED_ENV_VARS)
        logger.info(f"Environment validation successful")
        logger.info("Note: Shopify credentials are read from .dlt/secrets.toml")
        return env_vars
    except ValueError as e:
        logger.error(f"Environment validation failed: {e}")
        raise

def run_pipeline(mode: str = "incremental"):
    """
    Main pipeline execution function

    Args:
        mode: Loading mode - 'seed', 'incremental', or 'full'
              - 'seed': Load all historical data from start_date
              - 'incremental': Load only new/updated records since last run
              - 'full': Same as seed for Shopify (no distinction in DLT source)
    """
    logger.info(f"Starting Shopify pipeline in {mode} mode")

    try:
        # Validate environment
        env_vars = validate_environment()

        # Configure DLT pipeline using centralized destination
        postgres_config = get_dlt_destination()

        pipeline = dlt.pipeline(
            pipeline_name="shopify_pipeline",
            destination=postgres_config,
            dataset_name="raw",
        )

        # Determine start_date based on mode
        if mode in ['seed', 'full']:
            # For seed/full mode, load last 2 years of historical data
            start_date = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')
            logger.info(f"Seed mode: loading data from {start_date}")
        else:
            # For incremental mode, DLT will use its state management
            # to determine the last loaded date automatically
            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
            logger.info(f"Incremental mode: loading data from {start_date} (DLT will optimize based on state)")

        # Configure Shopify source with selected resources
        # Credentials are automatically read from .dlt/secrets.toml
        # Using the verified source with customers, orders, and products
        source = shopify_source(
            start_date=start_date,
            items_per_page=250  # Maximum allowed by Shopify API
        ).with_resources("customers", "orders", "products")

        # Run the pipeline
        logger.info("Running DLT Shopify extraction")
        load_info = pipeline.run(source)

        # Log results
        logger.info(f"Pipeline completed successfully")
        logger.info(f"Load info: {load_info}")

        # Extract row counts from load_info
        row_counts = {}
        for package in load_info.load_packages:
            for load_id in package.jobs:
                job = package.jobs[load_id]
                if hasattr(job, 'table_name') and hasattr(job, 'metrics'):
                    row_counts[job.table_name] = job.metrics.get('row_count', 0)

        if row_counts:
            logger.info(f"Rows loaded by table: {row_counts}")

        return {
            "status": "success",
            "mode": mode,
            "load_info": str(load_info),
            "row_counts": row_counts
        }

    except Exception as e:
        logger.error(f"Shopify pipeline failed: {str(e)}")
        raise

def main():
    """Main entry point when script is run directly"""
    parser = argparse.ArgumentParser(description='Shopify Data Pipeline')
    parser.add_argument(
        '--mode',
        choices=['seed', 'incremental', 'full'],
        default='incremental',
        help='Loading mode: seed (historical), incremental (latest), full (all data)'
    )

    args = parser.parse_args()

    try:
        result = run_pipeline(mode=args.mode)
        logger.info(f"Pipeline completed successfully: {result['status']}")
        print(f"\n✅ Shopify pipeline finished successfully in {result['mode']} mode!")
        if result.get('row_counts'):
            print("\nRows loaded:")
            for table, count in result['row_counts'].items():
                print(f"  - {table}: {count:,} rows")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        print(f"\n❌ Pipeline failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
