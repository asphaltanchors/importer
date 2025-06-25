# ABOUTME: Master pipeline orchestrator for multi-source data ingestion
# ABOUTME: Runs individual source pipelines based on configuration and scheduling

import os
import sys
import argparse
import subprocess
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime

# Add pipelines directory to Python path for imports
sys.path.insert(0, str(Path(__file__).parent / "pipelines"))

from pipelines.shared import setup_logging, load_config, run_basic_quality_checks

class PipelineOrchestrator:
    """Master orchestrator for multi-source data pipeline"""
    
    def __init__(self, config_path: str = "config/sources.yml"):
        self.config_path = config_path
        self.config = self._load_orchestrator_config()
        self.logger = setup_logging("orchestrator", "INFO")
        self.results = {}
        
    def _load_orchestrator_config(self) -> Dict[str, Any]:
        """Load orchestrator configuration"""
        if os.path.exists(self.config_path):
            return load_config(self.config_path)
        else:
            # Create default configuration
            default_config = {
                "sources": {
                    "quickbooks": {
                        "enabled": True,
                        "schedule": "daily",
                        "path": "pipelines/quickbooks",
                        "priority": 1,
                        "tables": ["customers", "items", "sales_receipts", "invoices", "company_enrichment"]
                    }
                },
                "dbt": {
                    "enabled": True,
                    "run_after_sources": True,
                    "models_to_run": ["staging", "intermediate", "mart"]
                },
                "data_quality": {
                    "enabled": True,
                    "run_after_dbt": True
                }
            }
            
            # Create config directory if it doesn't exist
            os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
            
            # Save default config
            import yaml
            with open(self.config_path, 'w') as f:
                yaml.dump(default_config, f, default_flow_style=False)
            
            self.logger.info(f"Created default configuration: {self.config_path}")
            return default_config
    
    def run_source_pipeline(self, source_name: str) -> Dict[str, Any]:
        """
        Run individual source pipeline
        
        Args:
            source_name: Name of the data source
            
        Returns:
            Execution result dictionary
        """
        source_config = self.config["sources"].get(source_name)
        if not source_config:
            raise ValueError(f"Source '{source_name}' not found in configuration")
        
        if not source_config.get("enabled", False):
            self.logger.info(f"Source '{source_name}' is disabled, skipping")
            return {"status": "skipped", "reason": "disabled"}
        
        self.logger.info(f"Running pipeline for source: {source_name}")
        
        # Get pipeline path
        pipeline_path = Path(source_config["path"])
        pipeline_script = pipeline_path / "pipeline.py"
        
        if not pipeline_script.exists():
            error_msg = f"Pipeline script not found: {pipeline_script}"
            self.logger.error(error_msg)
            return {"status": "error", "message": error_msg}
        
        try:
            # Change to pipeline directory and run
            original_cwd = os.getcwd()
            os.chdir(pipeline_path)
            
            start_time = datetime.now()
            result = subprocess.run(
                [sys.executable, "pipeline.py"],
                capture_output=True,
                text=True,
                timeout=3600  # 1 hour timeout
            )
            end_time = datetime.now()
            
            # Return to original directory
            os.chdir(original_cwd)
            
            execution_time = (end_time - start_time).total_seconds()
            
            if result.returncode == 0:
                self.logger.info(f"Pipeline '{source_name}' completed successfully in {execution_time:.1f}s")
                return {
                    "status": "success",
                    "execution_time": execution_time,
                    "stdout": result.stdout,
                    "stderr": result.stderr
                }
            else:
                self.logger.error(f"Pipeline '{source_name}' failed with return code {result.returncode}")
                return {
                    "status": "error", 
                    "return_code": result.returncode,
                    "execution_time": execution_time,
                    "stdout": result.stdout,
                    "stderr": result.stderr
                }
                
        except subprocess.TimeoutExpired:
            self.logger.error(f"Pipeline '{source_name}' timed out after 1 hour")
            return {"status": "timeout", "execution_time": 3600}
        except Exception as e:
            self.logger.error(f"Error running pipeline '{source_name}': {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def run_dbt_transformations(self) -> Dict[str, Any]:
        """Run DBT transformations after all sources complete"""
        if not self.config.get("dbt", {}).get("enabled", False):
            self.logger.info("DBT transformations are disabled, skipping")
            return {"status": "skipped", "reason": "disabled"}
        
        self.logger.info("Running DBT transformations")
        
        try:
            start_time = datetime.now()
            
            # Run dbt models
            models_to_run = self.config["dbt"].get("models_to_run", ["staging", "intermediate", "mart"])
            
            for model_group in models_to_run:
                self.logger.info(f"Running DBT models: {model_group}")
                result = subprocess.run(
                    ["dbt", "run", "--select", model_group],
                    capture_output=True,
                    text=True,
                    timeout=1800  # 30 minute timeout
                )
                
                if result.returncode != 0:
                    self.logger.error(f"DBT run failed for {model_group}")
                    return {
                        "status": "error",
                        "model_group": model_group,
                        "return_code": result.returncode,
                        "stdout": result.stdout,
                        "stderr": result.stderr
                    }
            
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            self.logger.info(f"DBT transformations completed successfully in {execution_time:.1f}s")
            return {
                "status": "success",
                "execution_time": execution_time,
                "models_run": models_to_run
            }
            
        except subprocess.TimeoutExpired:
            self.logger.error("DBT transformations timed out after 30 minutes")
            return {"status": "timeout", "execution_time": 1800}
        except Exception as e:
            self.logger.error(f"Error running DBT transformations: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def run_data_quality_checks(self) -> Dict[str, Any]:
        """Run data quality checks on final mart tables"""
        if not self.config.get("data_quality", {}).get("enabled", False):
            self.logger.info("Data quality checks are disabled, skipping")
            return {"status": "skipped", "reason": "disabled"}
        
        self.logger.info("Running data quality checks")
        
        try:
            quality_results = {}
            
            # Check each enabled source
            for source_name, source_config in self.config["sources"].items():
                if source_config.get("enabled", False):
                    tables = source_config.get("tables", [])
                    if tables:
                        quality_report = run_basic_quality_checks(source_name, tables)
                        quality_results[source_name] = quality_report
            
            # Count total issues
            total_issues = sum(
                report.get("issues_found", 0) 
                for report in quality_results.values()
            )
            
            if total_issues > 0:
                self.logger.warning(f"Data quality checks found {total_issues} issues across all sources")
            else:
                self.logger.info("All data quality checks passed")
            
            return {
                "status": "completed",
                "total_issues": total_issues,
                "source_reports": quality_results
            }
            
        except Exception as e:
            self.logger.error(f"Error running data quality checks: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def run_full_pipeline(self) -> Dict[str, Any]:
        """Run complete pipeline in correct order"""
        self.logger.info("Starting full pipeline execution")
        start_time = datetime.now()
        
        pipeline_results = {
            "start_time": start_time.isoformat(),
            "sources": {},
            "dbt": {},
            "data_quality": {},
            "overall_status": "success"
        }
        
        try:
            # 1. Run all source pipelines in priority order
            sources = list(self.config["sources"].items())
            sources.sort(key=lambda x: x[1].get("priority", 999))
            
            for source_name, source_config in sources:
                if source_config.get("enabled", False):
                    result = self.run_source_pipeline(source_name)
                    pipeline_results["sources"][source_name] = result
                    
                    if result["status"] not in ["success", "skipped"]:
                        self.logger.error(f"Source pipeline '{source_name}' failed, continuing with remaining sources")
                        pipeline_results["overall_status"] = "partial_failure"
            
            # 2. Run DBT transformations if enabled and sources succeeded
            if self.config.get("dbt", {}).get("run_after_sources", False):
                dbt_result = self.run_dbt_transformations()
                pipeline_results["dbt"] = dbt_result
                
                if dbt_result["status"] not in ["success", "skipped"]:
                    pipeline_results["overall_status"] = "failure"
            
            # 3. Run data quality checks if enabled
            if self.config.get("data_quality", {}).get("run_after_dbt", False):
                quality_result = self.run_data_quality_checks()
                pipeline_results["data_quality"] = quality_result
        
        except Exception as e:
            self.logger.error(f"Unexpected error in full pipeline: {str(e)}")
            pipeline_results["overall_status"] = "error"
            pipeline_results["error"] = str(e)
        
        # Calculate total execution time
        end_time = datetime.now()
        total_time = (end_time - start_time).total_seconds()
        pipeline_results["end_time"] = end_time.isoformat()
        pipeline_results["total_execution_time"] = total_time
        
        self.logger.info(f"Full pipeline completed with status: {pipeline_results['overall_status']} in {total_time:.1f}s")
        
        return pipeline_results

def main():
    """Main entry point for orchestrator"""
    parser = argparse.ArgumentParser(description="Multi-source data pipeline orchestrator")
    parser.add_argument(
        "--mode", 
        choices=["full", "source", "dbt", "data-quality"],
        default="full",
        help="Execution mode"
    )
    parser.add_argument(
        "--source",
        help="Source name (required for --mode source)"
    )
    parser.add_argument(
        "--config",
        default="config/sources.yml",
        help="Configuration file path"
    )
    
    args = parser.parse_args()
    
    # Create orchestrator
    orchestrator = PipelineOrchestrator(args.config)
    
    try:
        if args.mode == "full":
            result = orchestrator.run_full_pipeline()
        elif args.mode == "source":
            if not args.source:
                print("Error: --source required for source mode")
                sys.exit(1)
            result = orchestrator.run_source_pipeline(args.source)
        elif args.mode == "dbt":
            result = orchestrator.run_dbt_transformations()
        elif args.mode == "data-quality":
            result = orchestrator.run_data_quality_checks()
        
        # Print summary
        print(f"\nExecution completed with status: {result.get('status', 'unknown')}")
        
        # Exit with appropriate code
        if result.get("status") in ["success", "completed", "skipped"] and result.get("overall_status", "success") != "failure":
            sys.exit(0)
        else:
            sys.exit(1)
            
    except KeyboardInterrupt:
        orchestrator.logger.info("Pipeline execution interrupted by user")
        sys.exit(130)
    except Exception as e:
        orchestrator.logger.error(f"Orchestrator failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()