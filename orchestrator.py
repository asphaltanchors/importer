# ABOUTME: Master pipeline orchestrator for multi-source data ingestion
# ABOUTME: Runs individual source pipelines based on configuration and scheduling

import os
import sys
import argparse
import subprocess
import re
import json
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime

# Add pipelines directory to Python path for imports
sys.path.insert(0, str(Path(__file__).parent / "pipelines"))

from pipelines.shared import setup_logging, load_config, run_basic_quality_checks

class PipelineOrchestrator:
    """Master orchestrator for multi-source data pipeline"""
    
    def __init__(self, config_path: str = "config/sources.yml", verbose: bool = False):
        self.config_path = config_path
        self.config = self._load_orchestrator_config()
        self.logger = setup_logging("orchestrator", "INFO")
        self.verbose = verbose
        self.results = {}
        
        # Initialize simple file tracking
        self.processed_files_path = Path("logs/processed_files.json")
        self.processed_files_path.parent.mkdir(exist_ok=True)
        self._load_processed_files()
    
    def _load_processed_files(self) -> None:
        """Load previously processed file records"""
        try:
            if self.processed_files_path.exists():
                with open(self.processed_files_path, 'r') as f:
                    self.processed_files = json.load(f)
            else:
                self.processed_files = {}
        except (json.JSONDecodeError, IOError) as e:
            self.logger.warning(f"Could not load processed files: {e}")
            self.processed_files = {}
    
    def _save_processed_files(self) -> None:
        """Save processed file records"""
        try:
            with open(self.processed_files_path, 'w') as f:
                json.dump(self.processed_files, f, indent=2)
        except IOError as e:
            self.logger.warning(f"Could not save processed files: {e}")
    
    def _is_file_already_processed(self, file_path: Path) -> bool:
        """Check if file has been processed (simple filename + mod time check)"""
        file_key = str(file_path)
        
        try:
            current_mod_time = file_path.stat().st_mtime
        except OSError:
            return False  # File doesn't exist, not processed
        
        # Check if we've seen this file before
        if file_key not in self.processed_files:
            return False
        
        # Check if modification time has changed
        previous_mod_time = self.processed_files[file_key].get("mod_time", 0)
        if current_mod_time > previous_mod_time:
            return False  # File has been modified
        
        return True  # File already processed and unchanged
    
    def _mark_file_processed(self, file_path: Path) -> None:
        """Mark a file as processed with current modification time"""
        file_key = str(file_path)
        try:
            current_mod_time = file_path.stat().st_mtime
            self.processed_files[file_key] = {
                "mod_time": current_mod_time,
                "processed_at": datetime.now().isoformat()
            }
        except OSError as e:
            self.logger.warning(f"Could not mark file as processed {file_path}: {e}")
    
    def _check_source_files_changed(self, source_name: str, mode: str) -> bool:
        """Check if any source files have changed since last run"""
        if source_name != "quickbooks":
            return True  # Always run non-QuickBooks sources for now
        
        # Get the DROPBOX_PATH from environment
        dropbox_path = os.getenv("DROPBOX_PATH")
        if not dropbox_path:
            return True  # Run if we can't determine paths
        
        files_to_check = []
        
        if mode in ["seed", "full"]:
            # Check seed files
            seed_path = Path(dropbox_path) / "seed"
            seed_files = [
                seed_path / "all_lists.xlsx",
                seed_path / "all_transactions.xlsx", 
                seed_path / "company_enrichment.jsonl"
            ]
            files_to_check.extend([f for f in seed_files if f.exists()])
        
        if mode in ["incremental", "full"]:
            # Check input files
            input_path = Path(dropbox_path) / "input"
            if input_path.exists():
                # Check all xlsx files in input directory
                input_files = list(input_path.glob("*.xlsx")) + list(input_path.glob("*.xls"))
                files_to_check.extend(input_files)
        
        if not files_to_check:
            return True  # Run if no files found

        # Sort files chronologically by extracting date from filename
        def extract_date(file_path):
            """Extract date from filename pattern: All Lists_MM_DD_YYYY_H_MM_SS.xls"""
            match = re.search(r'_(\d{2})_(\d{2})_(\d{4})_', file_path.name)
            if match:
                month, day, year = match.groups()
                return datetime.strptime(f"{year}-{month}-{day}", "%Y-%m-%d")
            return datetime.min  # Put unparseable files first

        files_to_check.sort(key=extract_date)

        # Check if any files need processing
        new_files = []
        for file_path in files_to_check:
            if not self._is_file_already_processed(file_path):
                self.logger.info(f"File needs processing: {file_path}")
                new_files.append(file_path)
        
        # Mark new files as processed
        for file_path in new_files:
            self._mark_file_processed(file_path)
        
        return len(new_files) > 0
    
    def _update_pipeline_state(self, source_name: str, mode: str, status: str) -> None:
        """Update pipeline state tracking"""
        state_file = Path("logs/pipeline_state.json")
        
        try:
            if state_file.exists():
                with open(state_file, 'r') as f:
                    pipeline_state = json.load(f)
            else:
                pipeline_state = {}
            
            # Update state for this source
            if source_name not in pipeline_state:
                pipeline_state[source_name] = {}
            
            pipeline_state[source_name][mode] = {
                "last_run": datetime.now().isoformat(),
                "status": status
            }
            
            # Save updated state
            with open(state_file, 'w') as f:
                json.dump(pipeline_state, f, indent=2)
                
        except (json.JSONDecodeError, IOError) as e:
            self.logger.warning(f"Could not update pipeline state: {e}")
    
    def _get_last_successful_run(self, source_name: str, mode: str) -> Optional[str]:
        """Get timestamp of last successful run for source/mode"""
        state_file = Path("logs/pipeline_state.json")
        
        try:
            if not state_file.exists():
                return None
                
            with open(state_file, 'r') as f:
                pipeline_state = json.load(f)
            
            source_state = pipeline_state.get(source_name, {})
            mode_state = source_state.get(mode, {})
            
            if mode_state.get("status") == "success":
                return mode_state.get("last_run")
            
            return None
            
        except (json.JSONDecodeError, IOError) as e:
            self.logger.warning(f"Could not read pipeline state: {e}")
            return None
    
    def _run_subprocess(self, cmd: List[str], context: str, working_dir: str = None, timeout: int = 3600) -> Dict[str, Any]:
        """
        Shared subprocess execution with consistent error handling and progress tracking
        
        Args:
            cmd: Command to execute
            context: Description for logging and TUI
            working_dir: Working directory (None to use current)
            timeout: Timeout in seconds
            
        Returns:
            Execution result dictionary
        """
        original_cwd = None
        if working_dir:
            original_cwd = os.getcwd()
            os.chdir(working_dir)
        
        try:
            start_time = datetime.now()
            
            if self.verbose:
                # Real-time output streaming for verbose mode
                print(f"Running command: {' '.join(cmd)}")
                print(f"Working directory: {os.getcwd()}")
                print("-" * 60)
                
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,  # Merge stderr into stdout for unified output
                    text=True,
                    bufsize=1,  # Line buffered
                    universal_newlines=True
                )
                
                stdout_lines = []
                while True:
                    output = process.stdout.readline()
                    if output == '' and process.poll() is not None:
                        break
                    if output:
                        print(output.rstrip())
                        stdout_lines.append(output)
                
                # Wait for process completion and get return code
                return_code = process.wait()
                stdout_content = ''.join(stdout_lines)
                stderr_content = ""  # Merged into stdout
                
                # Create result object compatible with subprocess.run
                class MockResult:
                    def __init__(self, returncode, stdout, stderr):
                        self.returncode = returncode
                        self.stdout = stdout
                        self.stderr = stderr
                
                result = MockResult(return_code, stdout_content, stderr_content)
                print("-" * 60)
            else:
                # Standard capture mode for non-verbose
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=timeout
                )
            
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            if result.returncode == 0:
                return {
                    "status": "success",
                    "execution_time": execution_time,
                    "stdout": result.stdout,
                    "stderr": result.stderr
                }
            else:
                # Display detailed error information
                error_details = self._format_subprocess_error(cmd, result, f"{context} failed")
                print(error_details)
                return {
                    "status": "error", 
                    "return_code": result.returncode,
                    "execution_time": execution_time,
                    "stdout": result.stdout,
                    "stderr": result.stderr
                }
                
        except subprocess.TimeoutExpired:
            self.logger.error(f"{context} timed out after {timeout/60:.1f} minutes")
            return {"status": "timeout", "execution_time": timeout}
        except Exception as e:
            if 'process' in locals():
                try:
                    process.terminate()
                    process.wait(timeout=5)
                except:
                    pass
            self.logger.error(f"Error in {context}: {str(e)}")
            return {"status": "error", "message": str(e)}
        finally:
            # Return to original directory
            if original_cwd:
                os.chdir(original_cwd)
        
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
    
    def _format_subprocess_error(self, cmd: List[str], result, context: str = "") -> str:
        """Simple error formatting - just show essential information"""
        error_lines = []
        error_lines.append(f"❌ ERROR: {context}")
        error_lines.append(f"Command: {' '.join(cmd)}")
        error_lines.append(f"Return code: {result.returncode}")
        error_lines.append("")
        
        if result.stdout and result.stdout.strip():
            error_lines.append("STDOUT:")
            error_lines.append(result.stdout.strip())
            error_lines.append("")
        
        if result.stderr and result.stderr.strip():
            error_lines.append("STDERR:")
            error_lines.append(result.stderr.strip())
        
        return '\n'.join(error_lines)
    
    
    def run_source_pipeline(self, source_name: str, mode: str = "incremental") -> Dict[str, Any]:
        """
        Run individual source pipeline
        
        Args:
            source_name: Name of the data source
            mode: Loading mode ('seed', 'incremental', 'full')
            
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
        
        # Check if source files have changed
        if not self._check_source_files_changed(source_name, mode):
            self.logger.info(f"No file changes detected for '{source_name}', skipping pipeline")
            return {"status": "skipped", "reason": "no_changes", "execution_time": 0}
        
        # Get pipeline path
        pipeline_path = Path(source_config["path"])
        pipeline_script = pipeline_path / "pipeline.py"
        
        if not pipeline_script.exists():
            error_msg = f"Pipeline script not found: {pipeline_script}"
            self.logger.error(error_msg)
            return {"status": "error", "message": error_msg}
        
        try:
            # Build command - skip DBT for individual pipelines (orchestrator runs it centrally)
            cmd = [sys.executable, "pipeline.py", "--mode", mode]
            if source_name == "quickbooks":
                cmd.append("--skip-dbt")  # QuickBooks pipeline supports skipping DBT
            context = f"Pipeline: {source_name}"

            result = self._run_subprocess(cmd, context, str(pipeline_path), timeout=3600)
            
            if result["status"] == "success":
                self.logger.info(f"Pipeline '{source_name}' completed successfully in {result['execution_time']:.1f}s")
                self._update_pipeline_state(source_name, mode, "success")
                if self.verbose and result.get("stdout"):
                    print("Pipeline output:")
                    print(result["stdout"])
            else:
                self.logger.error(f"Pipeline '{source_name}' failed")
                self._update_pipeline_state(source_name, mode, "failed")
                
            return result
                
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
            # Build DBT command with exclusions for disabled sources
            cmd = ["dbt", "run"]

            # Exclude models from disabled sources by path (all layers)
            disabled_sources = [
                source_name for source_name, source_config in self.config["sources"].items()
                if not source_config.get("enabled", False)
            ]

            if disabled_sources:
                for source in disabled_sources:
                    # Exclude staging, intermediate, and mart layers
                    cmd.extend(["--exclude", f"path:models/staging/{source}/*"])
                    cmd.extend(["--exclude", f"path:models/intermediate/{source}/*"])
                    cmd.extend(["--exclude", f"path:models/mart/*{source}*"])
                    self.logger.info(f"Excluding all {source} models from DBT run")

            context = "DBT Transformations"

            result = self._run_subprocess(cmd, context, timeout=1800)  # 30 minute timeout
            
            if result["status"] == "success":
                self.logger.info(f"DBT transformations completed successfully in {result['execution_time']:.1f}s")
                models_to_run = self.config["dbt"].get("models_to_run", ["staging", "intermediate", "mart"])
                return {
                    "status": "success",
                    "execution_time": result["execution_time"],
                    "models_run": models_to_run,
                    "stdout": result.get("stdout"),
                    "stderr": result.get("stderr")
                }
            else:
                self.logger.error("DBT transformations failed")
                return result
            
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
    
    def run_full_pipeline(self, mode: str = "incremental") -> Dict[str, Any]:
        """Run complete pipeline in correct order"""
        self.logger.info("Starting full pipeline execution")
        start_time = datetime.now()
        
        # Calculate total steps for progress tracking
        total_steps = 0
        enabled_sources = [name for name, config in self.config["sources"].items() if config.get("enabled", False)]
        total_steps += len(enabled_sources)
        if self.config.get("dbt", {}).get("enabled", False):
            total_steps += len(self.config.get("dbt", {}).get("models_to_run", []))
        if self.config.get("data_quality", {}).get("enabled", False):
            total_steps += 1
        
        
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
                    result = self.run_source_pipeline(source_name, mode)
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
        
        
        # Save processed files after pipeline completion
        self._save_processed_files()
        
        return pipeline_results
    

def main():
    """Main entry point for orchestrator"""
    parser = argparse.ArgumentParser(description="Multi-source data pipeline orchestrator")
    
    # Create mutually exclusive group for load modes
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument(
        "--seed",
        action="store_true",
        help="Load all historical data from seed/ directory"
    )
    mode_group.add_argument(
        "--incremental",
        action="store_true", 
        help="Load all available daily files from input/ directory"
    )
    
    parser.add_argument(
        "--source",
        help="Run pipeline for specific source only (for multi-source support)"
    )
    parser.add_argument(
        "--config",
        default="config/sources.yml",
        help="Configuration file path"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show detailed output including full subprocess stdout/stderr"
    )
    
    args = parser.parse_args()
    
    # Create orchestrator
    orchestrator = PipelineOrchestrator(
        args.config, 
        verbose=getattr(args, 'verbose', False)
    )
    
    try:
        # Determine load mode from new arguments
        if args.seed:
            load_mode = "seed"
        elif args.incremental:
            load_mode = "incremental"
        
        # Execute pipeline
        if args.source:
            # Run specific source only
            result = orchestrator.run_source_pipeline(args.source, load_mode)
        else:
            # Run full pipeline (all enabled sources + DBT + data quality)
            result = orchestrator.run_full_pipeline(load_mode)
        
        # Print enhanced summary
        print("\n" + "="*70)
        print("📊 PIPELINE EXECUTION SUMMARY")
        print("="*70)
        
        overall_status = result.get("overall_status", result.get("status", "unknown"))
        status_emoji = "✅" if overall_status == "success" else "❌" if overall_status in ["error", "failure"] else "⚠️" 
        print(f"Status: {status_emoji} {overall_status.upper()}")
        
        if "total_execution_time" in result:
            total_time = result["total_execution_time"]
            print(f"Total execution time: {total_time:.1f}s ({total_time/60:.1f} minutes)")
        
        # Source pipeline summary
        if "sources" in result:
            print("\n📂 Source Pipelines:")
            for source_name, source_result in result["sources"].items():
                status = source_result.get("status", "unknown")
                emoji = "✅" if status == "success" else "❌" if status == "error" else "⏭️"
                exec_time = source_result.get("execution_time", 0)
                print(f"  {emoji} {source_name}: {status} ({exec_time:.1f}s)")
        
        # DBT summary
        if "dbt" in result and result["dbt"]:
            dbt_status = result["dbt"].get("status", "unknown")
            emoji = "✅" if dbt_status == "success" else "❌" if dbt_status == "error" else "⏭️"
            exec_time = result["dbt"].get("execution_time", 0)
            print(f"\n🔄 DBT Transformations: {emoji} {dbt_status} ({exec_time:.1f}s)")
            if "models_run" in result["dbt"]:
                models = ", ".join(result["dbt"]["models_run"])
                print(f"   Models: {models}")
        
        # Data quality summary
        if "data_quality" in result and result["data_quality"]:
            dq_status = result["data_quality"].get("status", "unknown")
            emoji = "✅" if dq_status == "completed" else "❌" if dq_status == "error" else "⏭️"
            total_issues = result["data_quality"].get("total_issues", 0)
            print(f"\n🔍 Data Quality: {emoji} {dq_status} ({total_issues} issues found)")
        
        print("="*70)
        
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