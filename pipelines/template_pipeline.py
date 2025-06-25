# ABOUTME: Template pipeline for new data source integration
# ABOUTME: Copy and modify this template for each new data source (Attio, GA, Shopify, etc.)

import os
import sys
from datetime import datetime
from typing import Dict, Any, List

import dlt
from dotenv import load_dotenv

# Add shared utilities to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))
from shared import setup_logging, validate_environment_variables, get_current_timestamp

# Load environment variables
load_dotenv()

# Configure logging for this source
logger = setup_logging("CHANGE_ME_SOURCE_NAME", "INFO")

# Validate required environment variables
REQUIRED_ENV_VARS = [
    "DATABASE_URL",
    # "CHANGE_ME_API_KEY",  # Add source-specific environment variables
    # "CHANGE_ME_BASE_URL",
]

def validate_environment():
    """Validate all required environment variables are set"""
    try:
        env_vars = validate_environment_variables(REQUIRED_ENV_VARS)
        logger.info(f"Environment validation successful for {len(REQUIRED_ENV_VARS)} variables")
        return env_vars
    except ValueError as e:
        logger.error(f"Environment validation failed: {e}")
        raise

# Define your DLT source
@dlt.source
def template_source():
    """
    Template DLT source - modify this for your specific data source
    
    Replace this with your actual data source extraction logic:
    - API calls for REST APIs
    - File processing for CSV/JSON files
    - Database queries for database sources
    """
    
    @dlt.resource(
        write_disposition="merge",  # or "replace" or "append"
        name="template_table",
        primary_key=["id"]  # Define appropriate primary key(s)
    )
    def extract_template_data():
        """
        Template extraction function - replace with actual data extraction
        
        This should yield dictionaries representing rows of data.
        Each dictionary becomes a row in the destination table.
        """
        logger.info("Starting template data extraction")
        
        # REPLACE THIS BLOCK with your actual data extraction logic
        sample_data = [
            {
                "id": 1,
                "name": "Sample Record 1",
                "created_at": get_current_timestamp(),
                "load_date": datetime.utcnow().date().isoformat()
            },
            {
                "id": 2, 
                "name": "Sample Record 2",
                "created_at": get_current_timestamp(),
                "load_date": datetime.utcnow().date().isoformat()
            }
        ]
        
        for record in sample_data:
            yield record
            
        logger.info(f"Template data extraction completed: {len(sample_data)} records")
    
    # Add more @dlt.resource functions for additional tables/endpoints
    # @dlt.resource(...)
    # def extract_other_table():
    #     ...
    
    return [extract_template_data]  # Return list of all resource functions

def run_pipeline():
    """Main pipeline execution function"""
    logger.info("Starting template pipeline execution")
    
    try:
        # Validate environment
        env_vars = validate_environment()
        
        # Configure DLT pipeline
        # Use the DATABASE_URL directly to avoid credential issues
        import dlt.destinations
        postgres_config = dlt.destinations.postgres(env_vars["DATABASE_URL"])
        
        load_pipeline = dlt.pipeline(
            pipeline_name="dqi",  # Keep consistent with main pipeline
            destination=postgres_config,
            dataset_name="raw",   # All sources go to raw schema
        )
        
        # Run the pipeline
        logger.info("Running DLT extraction")
        load_info = load_pipeline.run(template_source())
        logger.info(f"DLT pipeline completed: {load_info}")
        
        # Optional: Add source-specific post-processing here
        # run_custom_processing()
        
        logger.info("Template pipeline completed successfully")
        return {"status": "success", "load_info": str(load_info)}
        
    except Exception as e:
        logger.error(f"Template pipeline failed: {str(e)}")
        raise

def run_custom_processing():
    """
    Optional: Add custom processing logic specific to this data source
    
    Examples:
    - Data validation and cleaning
    - Custom transformations before DBT
    - API-specific error handling
    - Rate limiting management
    """
    logger.info("Running custom processing (placeholder)")
    # Add custom logic here
    pass

if __name__ == "__main__":
    """
    Main entry point when script is run directly
    
    To use this template:
    1. Copy this file to pipelines/{source_name}/pipeline.py
    2. Replace "CHANGE_ME_SOURCE_NAME" with your actual source name
    3. Replace "template" with your source name throughout
    4. Modify extract_template_data() with your actual extraction logic
    5. Add required environment variables to REQUIRED_ENV_VARS
    6. Update the source configuration in config/sources.yml
    """
    try:
        result = run_pipeline()
        print(f"Pipeline completed successfully: {result}")
        sys.exit(0)
    except Exception as e:
        print(f"Pipeline failed: {str(e)}")
        sys.exit(1)