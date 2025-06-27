# ABOUTME: Progress tracking interface for TUI integration with pipeline orchestrator
# ABOUTME: Provides thread-safe communication between subprocess execution and TUI updates

import threading
import queue
import time
from typing import Dict, Any, Optional, Callable
from dataclasses import dataclass
from enum import Enum
from datetime import datetime

from .dashboard import PipelineStatus, PipelineDashboard


class ProgressEvent(Enum):
    """Types of progress events"""
    PIPELINE_START = "pipeline_start"
    PIPELINE_END = "pipeline_end"
    STEP_START = "step_start"
    STEP_UPDATE = "step_update"
    STEP_END = "step_end"
    LOG_MESSAGE = "log_message"
    ERROR = "error"


@dataclass
class ProgressMessage:
    """Progress update message"""
    event_type: ProgressEvent
    data: Dict[str, Any]
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


class ProgressTracker:
    """Thread-safe progress tracker for TUI integration"""
    
    def __init__(self, dashboard: Optional[PipelineDashboard] = None):
        self.dashboard = dashboard
        self.message_queue = queue.Queue()
        self.is_running = False
        self.worker_thread = None
        self.current_steps = {}
        self.pipeline_start_time = None
        
        # Callbacks for non-TUI usage
        self.callbacks: Dict[ProgressEvent, List[Callable]] = {
            event: [] for event in ProgressEvent
        }
    
    def start(self) -> None:
        """Start the progress tracking worker thread"""
        if self.is_running:
            return
            
        self.is_running = True
        self.worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self.worker_thread.start()
    
    def stop(self) -> None:
        """Stop the progress tracking worker thread"""
        self.is_running = False
        if self.worker_thread:
            self.worker_thread.join(timeout=1.0)
    
    def _worker_loop(self) -> None:
        """Main worker loop for processing progress messages"""
        while self.is_running:
            try:
                # Check for new messages with timeout
                message = self.message_queue.get(timeout=0.1)
                self._process_message(message)
                self.message_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                print(f"Error processing progress message: {e}")
    
    def _process_message(self, message: ProgressMessage) -> None:
        """Process a single progress message"""
        try:
            # Update dashboard if available
            if self.dashboard:
                self._update_dashboard(message)
            
            # Call registered callbacks
            for callback in self.callbacks.get(message.event_type, []):
                callback(message)
                
        except Exception as e:
            print(f"Error updating progress: {e}")
    
    def _update_dashboard(self, message: ProgressMessage) -> None:
        """Update the TUI dashboard based on the message"""
        event_type = message.event_type
        data = message.data
        
        if event_type == ProgressEvent.PIPELINE_START:
            self.pipeline_start_time = message.timestamp
            total_steps = data.get('total_steps', 0)
            self.dashboard.update_status(
                PipelineStatus.RUNNING,
                steps_completed=0,
                total_steps=total_steps
            )
            self.dashboard.add_log("Pipeline execution started", "SUCCESS")
            
        elif event_type == ProgressEvent.PIPELINE_END:
            status = data.get('status', 'unknown')
            pipeline_status = self._convert_status(status)
            completed_steps = len([s for s in self.current_steps.values() 
                                 if s.get('status') in ['success', 'completed']])
            
            self.dashboard.update_status(
                pipeline_status,
                steps_completed=completed_steps
            )
            
            duration = (message.timestamp - self.pipeline_start_time).total_seconds() if self.pipeline_start_time else 0
            self.dashboard.add_log(f"Pipeline completed with status: {status} (duration: {duration:.1f}s)", 
                                 "SUCCESS" if status == "success" else "ERROR")
            
        elif event_type == ProgressEvent.STEP_START:
            step_name = data.get('name')
            if step_name:
                self.dashboard.add_step(step_name)
                self.dashboard.update_step(
                    step_name,
                    status=PipelineStatus.RUNNING,
                    start_time=message.timestamp
                )
                self.current_steps[step_name] = {'status': 'running', 'start_time': message.timestamp}
                
        elif event_type == ProgressEvent.STEP_UPDATE:
            step_name = data.get('name')
            if step_name:
                update_data = {}
                if 'progress' in data:
                    update_data['progress'] = data['progress']
                if 'message' in data:
                    self.dashboard.add_log(f"{step_name}: {data['message']}", "INFO")
                
                self.dashboard.update_step(step_name, **update_data)
                
        elif event_type == ProgressEvent.STEP_END:
            step_name = data.get('name')
            status = data.get('status', 'completed')
            
            if step_name:
                pipeline_status = self._convert_status(status)
                update_data = {
                    'status': pipeline_status,
                    'end_time': message.timestamp,
                    'progress': 100.0
                }
                
                if 'error_message' in data:
                    update_data['error_message'] = data['error_message']
                
                self.dashboard.update_step(step_name, **update_data)
                self.current_steps[step_name] = {'status': status, 'end_time': message.timestamp}
                
        elif event_type == ProgressEvent.LOG_MESSAGE:
            level = data.get('level', 'INFO')
            message_text = data.get('message', '')
            self.dashboard.add_log(message_text, level)
            
        elif event_type == ProgressEvent.ERROR:
            error_message = data.get('message', 'Unknown error')
            step_name = data.get('step_name')
            
            self.dashboard.add_log(f"ERROR: {error_message}", "ERROR")
            if step_name:
                self.dashboard.update_step(
                    step_name,
                    status=PipelineStatus.ERROR,
                    error_message=error_message,
                    end_time=message.timestamp
                )
    
    def _convert_status(self, status_str: str) -> PipelineStatus:
        """Convert string status to PipelineStatus enum"""
        status_map = {
            'success': PipelineStatus.SUCCESS,
            'completed': PipelineStatus.SUCCESS,
            'error': PipelineStatus.ERROR,
            'failed': PipelineStatus.ERROR,
            'failure': PipelineStatus.ERROR,
            'timeout': PipelineStatus.TIMEOUT,
            'skipped': PipelineStatus.SKIPPED,
            'running': PipelineStatus.RUNNING,
            'pending': PipelineStatus.PENDING
        }
        return status_map.get(status_str.lower(), PipelineStatus.PENDING)
    
    # Public API for orchestrator integration
    def pipeline_started(self, total_steps: int = 0, **kwargs) -> None:
        """Report pipeline start"""
        self.message_queue.put(ProgressMessage(
            ProgressEvent.PIPELINE_START,
            {'total_steps': total_steps, **kwargs}
        ))
    
    def pipeline_ended(self, status: str, **kwargs) -> None:
        """Report pipeline completion"""
        self.message_queue.put(ProgressMessage(
            ProgressEvent.PIPELINE_END,
            {'status': status, **kwargs}
        ))
    
    def step_started(self, name: str, **kwargs) -> None:
        """Report step start"""
        self.message_queue.put(ProgressMessage(
            ProgressEvent.STEP_START,
            {'name': name, **kwargs}
        ))
    
    def step_progress(self, name: str, progress: float = None, message: str = None, **kwargs) -> None:
        """Report step progress update"""
        data = {'name': name, **kwargs}
        if progress is not None:
            data['progress'] = progress
        if message is not None:
            data['message'] = message
            
        self.message_queue.put(ProgressMessage(
            ProgressEvent.STEP_UPDATE,
            data
        ))
    
    def step_completed(self, name: str, status: str = 'completed', **kwargs) -> None:
        """Report step completion"""
        self.message_queue.put(ProgressMessage(
            ProgressEvent.STEP_END,
            {'name': name, 'status': status, **kwargs}
        ))
    
    def log(self, message: str, level: str = 'INFO', **kwargs) -> None:
        """Add a log message"""
        self.message_queue.put(ProgressMessage(
            ProgressEvent.LOG_MESSAGE,
            {'message': message, 'level': level, **kwargs}
        ))
    
    def error(self, message: str, step_name: str = None, **kwargs) -> None:
        """Report an error"""
        data = {'message': message, **kwargs}
        if step_name:
            data['step_name'] = step_name
            
        self.message_queue.put(ProgressMessage(
            ProgressEvent.ERROR,
            data
        ))
    
    def add_callback(self, event_type: ProgressEvent, callback: Callable) -> None:
        """Add a callback for progress events"""
        self.callbacks[event_type].append(callback)
    
    def remove_callback(self, event_type: ProgressEvent, callback: Callable) -> None:
        """Remove a callback for progress events"""
        if callback in self.callbacks[event_type]:
            self.callbacks[event_type].remove(callback)


class SubprocessProgressParser:
    """Parser for extracting progress information from subprocess output"""
    
    def __init__(self, progress_tracker: ProgressTracker):
        self.tracker = progress_tracker
        self.patterns = {
            'dbt_model': r'Completed model (\w+\.\w+)',
            'dlt_pipeline': r'Pipeline (\w+) completed',
            'error': r'ERROR:?\s*(.*)',
            'warning': r'WARNING:?\s*(.*)',
            'progress': r'(\d+)%\s*complete',
        }
    
    def parse_line(self, line: str, step_name: str = None) -> None:
        """Parse a single line of subprocess output for progress information"""
        import re
        
        line = line.strip()
        if not line:
            return
        
        # Check for errors
        for pattern_name, pattern in self.patterns.items():
            match = re.search(pattern, line, re.IGNORECASE)
            if match:
                if pattern_name == 'error':
                    self.tracker.error(match.group(1), step_name)
                elif pattern_name == 'warning':
                    self.tracker.log(f"WARNING: {match.group(1)}", "WARNING")
                elif pattern_name == 'progress' and step_name:
                    progress = float(match.group(1))
                    self.tracker.step_progress(step_name, progress=progress)
                elif pattern_name in ['dbt_model', 'dlt_pipeline']:
                    self.tracker.log(line, "SUCCESS")
                break
        else:
            # No patterns matched, log as regular output
            if step_name:
                self.tracker.step_progress(step_name, message=line)
            else:
                self.tracker.log(line, "INFO")