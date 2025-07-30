# ABOUTME: Master pipeline orchestrator for multi-source data ingestion
# ABOUTME: Runs individual source pipelines based on configuration and scheduling

import os
import sys
import argparse
import subprocess
import re
import hashlib
import json
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime

# Add pipelines directory to Python path for imports
sys.path.insert(0, str(Path(__file__).parent / "pipelines"))

from pipelines.shared import setup_logging, load_config, run_basic_quality_checks

class PipelineOrchestrator:
    """Master orchestrator for multi-source data pipeline"""
    
    def __init__(self, config_path: str = "config/sources.yml", verbose: bool = False, use_tui: bool = False):
        self.config_path = config_path
        self.config = self._load_orchestrator_config()
        self.logger = setup_logging("orchestrator", "INFO")
        self.verbose = verbose
        self.use_tui = use_tui
        self.results = {}
        
        # TUI components
        self.dashboard = None
        self.progress_tracker = None
        self.subprocess_parser = None
        
        if self.use_tui:
            self._initialize_tui()
        
        # Initialize file hash tracking
        self.hash_file_path = Path("logs/file_hashes.json")
        self.hash_file_path.parent.mkdir(exist_ok=True)
        self._load_file_hashes()
    
    def _load_file_hashes(self) -> None:
        """Load previously stored file hashes"""
        try:
            if self.hash_file_path.exists():
                with open(self.hash_file_path, 'r') as f:
                    self.file_hashes = json.load(f)
            else:
                self.file_hashes = {}
        except (json.JSONDecodeError, IOError) as e:
            self.logger.warning(f"Could not load file hashes: {e}")
            self.file_hashes = {}
    
    def _save_file_hashes(self) -> None:
        """Save current file hashes"""
        try:
            with open(self.hash_file_path, 'w') as f:
                json.dump(self.file_hashes, f, indent=2)
        except IOError as e:
            self.logger.warning(f"Could not save file hashes: {e}")
    
    def _calculate_file_hash(self, file_path: Path) -> str:
        """Calculate SHA256 hash of file contents"""
        try:
            with open(file_path, 'rb') as f:
                return hashlib.sha256(f.read()).hexdigest()
        except IOError as e:
            self.logger.warning(f"Could not hash file {file_path}: {e}")
            return ""
    
    def _has_file_changed(self, file_path: Path) -> bool:
        """Check if file has changed since last run"""
        file_key = str(file_path)
        current_hash = self._calculate_file_hash(file_path)
        
        if not current_hash:
            return True  # Assume changed if we can't hash
        
        previous_hash = self.file_hashes.get(file_key)
        has_changed = previous_hash != current_hash
        
        # Update hash regardless
        self.file_hashes[file_key] = current_hash
        
        return has_changed
    
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
                seed_path / "company_enrichment.jsonl",
                seed_path / "historical_items.jsonl"
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
        
        # Check if any file has changed
        any_changed = False
        for file_path in files_to_check:
            if self._has_file_changed(file_path):
                self.logger.info(f"File changed: {file_path}")
                any_changed = True
        
        return any_changed
    
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
            
            if self.verbose or self.use_tui:
                # Real-time output streaming for verbose mode or TUI
                if not self.use_tui:
                    print(f"Running command: {' '.join(cmd)}")
                    print(f"Working directory: {os.getcwd()}")
                    print("-" * 60)
                
                # Report step start to TUI
                if self.progress_tracker:
                    self.progress_tracker.step_started(context)
                
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
                        if not self.use_tui:  # Only print if not using TUI
                            print(output.rstrip())
                        stdout_lines.append(output)
                        
                        # Parse output for TUI progress updates
                        if self.subprocess_parser:
                            self.subprocess_parser.parse_line(output, context)
                
                # Wait for process completion and get return code
                return_code = process.wait()
                stdout_content = ''.join(stdout_lines)
                stderr_content = ""  # Merged into stdout
                
                # Report step completion to TUI
                if self.progress_tracker:
                    status = "success" if return_code == 0 else "error"
                    self.progress_tracker.step_completed(context, status)
                
                # Create result object compatible with subprocess.run
                class MockResult:
                    def __init__(self, returncode, stdout, stderr):
                        self.returncode = returncode
                        self.stdout = stdout
                        self.stderr = stderr
                
                result = MockResult(return_code, stdout_content, stderr_content)
                if not self.use_tui:
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
        """Format subprocess error output for better debugging"""
        error_lines = []
        error_lines.append(f"{'='*70}")
        error_lines.append(f"❌ SUBPROCESS ERROR: {context}")
        error_lines.append(f"Command: {' '.join(cmd)}")
        error_lines.append(f"Return code: {result.returncode}")
        error_lines.append(f"Working directory: {os.getcwd()}")
        error_lines.append(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        error_lines.append(f"{'='*70}")
        
        # Extract key error information for quick diagnosis
        key_errors = self._extract_key_errors(result.stdout, result.stderr)
        if key_errors:
            error_lines.append("🔍 KEY ERRORS DETECTED:")
            error_lines.append("-" * 50)
            for error in key_errors[:5]:  # Show top 5 key errors
                error_lines.append(f"  • {error}")
            error_lines.append("")
        
        if result.stdout and result.stdout.strip():
            error_lines.append("📄 STDOUT:")
            error_lines.append("-" * 50)
            # Truncate if too long (unless verbose)
            stdout_lines = result.stdout.strip().split('\n')
            if not self.verbose and len(stdout_lines) > 25:
                error_lines.extend(stdout_lines[:12])
                error_lines.append(f"    ... ({len(stdout_lines) - 25} more lines, use --verbose for full output) ...")
                error_lines.extend(stdout_lines[-12:])
            else:
                error_lines.extend(stdout_lines)
            error_lines.append("")
        
        if result.stderr and result.stderr.strip():
            error_lines.append("⚠️  STDERR:")
            error_lines.append("-" * 50)
            # Truncate if too long (unless verbose)
            stderr_lines = result.stderr.strip().split('\n')
            if not self.verbose and len(stderr_lines) > 25:
                error_lines.extend(stderr_lines[:12])
                error_lines.append(f"    ... ({len(stderr_lines) - 25} more lines, use --verbose for full output) ...")
                error_lines.extend(stderr_lines[-12:])
            else:
                error_lines.extend(stderr_lines)
            error_lines.append("")
        
        # Add troubleshooting suggestions
        suggestions = self._get_troubleshooting_suggestions(cmd, result)
        if suggestions:
            error_lines.append("💡 TROUBLESHOOTING SUGGESTIONS:")
            error_lines.append("-" * 50)
            for suggestion in suggestions:
                error_lines.append(f"  • {suggestion}")
            error_lines.append("")
        
        error_lines.append(f"{'='*70}")
        return '\n'.join(error_lines)
    
    def _extract_key_errors(self, stdout: str, stderr: str) -> List[str]:
        """Extract key error messages from subprocess output"""
        key_errors = []
        combined_output = f"{stdout or ''} {stderr or ''}"
        
        # Common error patterns
        error_patterns = [
            r'ERROR:.*',
            r'FATAL:.*', 
            r'.*Error:.*',
            r'.*Exception:.*',
            r'.*Failed:.*',
            r'.*not found.*',
            r'.*Permission denied.*',
            r'.*Connection.*refused.*',
            r'.*Authentication.*failed.*',
            r'.*Timeout.*',
            r'.*Invalid.*',
            r'.*Missing.*requirement.*'
        ]
        
        for pattern in error_patterns:
            matches = re.findall(pattern, combined_output, re.IGNORECASE | re.MULTILINE)
            for match in matches[:3]:  # Limit to 3 matches per pattern
                if match.strip() and len(match.strip()) > 10:  # Skip very short matches
                    key_errors.append(match.strip())
        
        return list(dict.fromkeys(key_errors))  # Remove duplicates while preserving order
    
    def _get_troubleshooting_suggestions(self, cmd: List[str], result) -> List[str]:
        """Provide troubleshooting suggestions based on the command and error"""
        suggestions = []
        combined_output = f"{result.stdout or ''} {result.stderr or ''}".lower()
        
        # Command-specific suggestions
        if 'pipeline.py' in cmd:
            suggestions.append("Check if all required environment variables are set (.env file)")
            suggestions.append("Verify DROPBOX_PATH directory exists and contains expected files")
            suggestions.append("Ensure database connection is working (DATABASE_URL)")
            
            if 'connection' in combined_output or 'database' in combined_output:
                suggestions.append("Test database connectivity: psql $DATABASE_URL")
            
            if 'permission' in combined_output or 'access' in combined_output:
                suggestions.append("Check file/directory permissions for DROPBOX_PATH")
                
        elif 'dbt' in cmd:
            suggestions.append("Check DBT profiles.yml configuration")
            suggestions.append("Verify database schema exists and has proper permissions")
            suggestions.append("Run 'dbt debug' to test DBT configuration")
            
            if 'relation' in combined_output or 'table' in combined_output:
                suggestions.append("Check if source tables exist in the raw schema")
            
            if 'compilation' in combined_output:
                suggestions.append("Review DBT model SQL syntax for errors")
        
        # Generic suggestions based on error content
        if 'timeout' in combined_output:
            suggestions.append("Consider increasing timeout values or checking system performance")
            
        if 'memory' in combined_output or 'oom' in combined_output:
            suggestions.append("Check available system memory and consider processing smaller data batches")
        
        return suggestions
    
    def _initialize_tui(self) -> None:
        """Initialize TUI components"""
        try:
            from tui.dashboard import PipelineDashboard
            from tui.progress_tracker import ProgressTracker, SubprocessProgressParser
            
            self.dashboard = PipelineDashboard()
            self.progress_tracker = ProgressTracker(self.dashboard)
            self.subprocess_parser = SubprocessProgressParser(self.progress_tracker)
            
            # Start the progress tracker
            self.progress_tracker.start()
            
        except ImportError as e:
            print(f"Warning: TUI dependencies not available: {e}")
            print("Install TUI dependencies with: pip install rich textual")
            self.use_tui = False
    
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
            cmd = [sys.executable, "pipeline.py", "--mode", mode]
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
            # Run all dbt models in single command - let DBT handle dependencies
            cmd = ["dbt", "run"]
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
        
        # Report pipeline start to TUI
        if self.progress_tracker:
            self.progress_tracker.pipeline_started(total_steps=total_steps)
        
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
        
        # Report pipeline completion to TUI
        if self.progress_tracker:
            self.progress_tracker.pipeline_ended(pipeline_results['overall_status'])
        
        # Save file hashes after pipeline completion
        self._save_file_hashes()
        
        return pipeline_results
    
    def run_with_tui(self, mode: str = "incremental") -> Dict[str, Any]:
        """Run pipeline with TUI interface"""
        if not self.use_tui or not self.dashboard:
            return self.run_full_pipeline(mode)
        
        import asyncio
        from threading import Thread
        
        # Pipeline execution in separate thread
        pipeline_result = {}
        pipeline_exception = None
        
        def pipeline_worker():
            nonlocal pipeline_result, pipeline_exception
            try:
                pipeline_result = self.run_full_pipeline(mode)
            except Exception as e:
                pipeline_exception = e
        
        # Start pipeline in background thread
        pipeline_thread = Thread(target=pipeline_worker, daemon=True)
        pipeline_thread.start()
        
        # Run TUI dashboard
        try:
            self.dashboard.run()
        except KeyboardInterrupt:
            if self.progress_tracker:
                self.progress_tracker.log("Dashboard closed by user", "WARNING")
        
        # Wait for pipeline to complete
        pipeline_thread.join()
        
        # Stop progress tracker
        if self.progress_tracker:
            self.progress_tracker.stop()
        
        if pipeline_exception:
            raise pipeline_exception
        
        return pipeline_result

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
        "--load-mode",
        choices=["seed", "incremental", "full"],
        default="incremental", 
        help="Data loading mode: seed (historical only), incremental (latest daily), full (seed + all incremental)"
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
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show detailed output including full subprocess stdout/stderr"
    )
    parser.add_argument(
        "--tui",
        action="store_true",
        help="Use Terminal User Interface for enhanced monitoring and control"
    )
    
    args = parser.parse_args()
    
    # Create orchestrator
    orchestrator = PipelineOrchestrator(
        args.config, 
        verbose=getattr(args, 'verbose', False),
        use_tui=getattr(args, 'tui', False)
    )
    
    try:
        if args.mode == "full":
            if getattr(args, 'tui', False):
                result = orchestrator.run_with_tui(args.load_mode)
            else:
                result = orchestrator.run_full_pipeline(args.load_mode)
        elif args.mode == "source":
            if not args.source:
                print("Error: --source required for source mode")
                sys.exit(1)
            result = orchestrator.run_source_pipeline(args.source, args.load_mode)
        elif args.mode == "dbt":
            result = orchestrator.run_dbt_transformations()
        elif args.mode == "data-quality":
            result = orchestrator.run_data_quality_checks()
        
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