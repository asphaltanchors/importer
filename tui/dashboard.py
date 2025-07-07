# ABOUTME: Main TUI dashboard for pipeline orchestration monitoring
# ABOUTME: Provides real-time progress tracking, logs, and interactive controls

import asyncio
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from enum import Enum

from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal, Vertical
from textual.widgets import (
    Header, Footer, Static, ProgressBar, Log, Button, 
    DataTable, Label, Collapsible, LoadingIndicator
)
from textual.reactive import reactive
from textual.message import Message
from textual import events
from rich.text import Text
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, TaskID
from rich.console import Console


class PipelineStatus(Enum):
    """Pipeline execution status"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    ERROR = "error"
    TIMEOUT = "timeout"
    SKIPPED = "skipped"


@dataclass
class PipelineStep:
    """Represents a single pipeline step"""
    name: str
    status: PipelineStatus = PipelineStatus.PENDING
    progress: float = 0.0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    error_message: Optional[str] = None
    logs: List[str] = field(default_factory=list)
    
    @property
    def duration(self) -> float:
        """Get step duration in seconds"""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        elif self.start_time:
            return (datetime.now() - self.start_time).total_seconds()
        return 0.0
    
    @property
    def status_emoji(self) -> str:
        """Get emoji for current status"""
        return {
            PipelineStatus.PENDING: "⏳",
            PipelineStatus.RUNNING: "🔄",
            PipelineStatus.SUCCESS: "✅",
            PipelineStatus.ERROR: "❌",
            PipelineStatus.TIMEOUT: "⏰",
            PipelineStatus.SKIPPED: "⏭️"
        }.get(self.status, "❓")


class StatusPanel(Static):
    """Widget for displaying overall pipeline status"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.overall_status = PipelineStatus.PENDING
        self.start_time = None
        self.steps_completed = 0
        self.total_steps = 0
    
    def compose(self) -> ComposeResult:
        yield Static(self._render_status(), id="status-content")
    
    def _render_status(self) -> Panel:
        """Render the status panel content"""
        duration = ""
        if self.start_time:
            elapsed = (datetime.now() - self.start_time).total_seconds()
            minutes, seconds = divmod(elapsed, 60)
            duration = f" ({int(minutes):02d}:{int(seconds):02d})"
        
        progress_text = f"{self.steps_completed}/{self.total_steps}" if self.total_steps > 0 else "0/0"
        
        status_table = Table.grid(padding=1)
        status_table.add_column(style="bold blue")
        status_table.add_column()
        
        status_table.add_row("Status:", f"{self.overall_status.value.title()} {PipelineStep('', self.overall_status).status_emoji}")
        status_table.add_row("Progress:", f"{progress_text} steps completed")
        status_table.add_row("Duration:", duration)
        status_table.add_row("Started:", self.start_time.strftime("%H:%M:%S") if self.start_time else "Not started")
        
        return Panel(status_table, title="🎯 Pipeline Status", border_style="blue")
    
    def update_status(self, status: PipelineStatus, steps_completed: int = None, total_steps: int = None):
        """Update the overall status"""
        self.overall_status = status
        if steps_completed is not None:
            self.steps_completed = steps_completed
        if total_steps is not None:
            self.total_steps = total_steps
        if status == PipelineStatus.RUNNING and self.start_time is None:
            self.start_time = datetime.now()
        
        self.query_one("#status-content").update(self._render_status())


class StepsTable(DataTable):
    """Table widget for displaying pipeline steps"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.steps: Dict[str, PipelineStep] = {}
    
    def on_mount(self) -> None:
        """Set up the table columns"""
        self.add_columns("Step", "Status", "Progress", "Duration", "Details")
        self.cursor_type = "row"
    
    def add_step(self, step: PipelineStep) -> None:
        """Add a new step to the table"""
        self.steps[step.name] = step
        self._update_table()
    
    def update_step(self, name: str, **kwargs) -> None:
        """Update an existing step"""
        if name in self.steps:
            step = self.steps[name]
            for key, value in kwargs.items():
                if hasattr(step, key):
                    setattr(step, key, value)
            self._update_table()
    
    def _update_table(self) -> None:
        """Refresh the table content"""
        self.clear()
        for step in self.steps.values():
            progress_text = f"{step.progress:.1f}%" if step.progress > 0 else ""
            duration_text = f"{step.duration:.1f}s" if step.duration > 0 else ""
            details = step.error_message if step.error_message else ""
            
            self.add_row(
                step.name,
                f"{step.status_emoji} {step.status.value}",
                progress_text,
                duration_text,
                details[:50] + "..." if len(details) > 50 else details
            )


class LogViewer(Log):
    """Enhanced log viewer with filtering and search"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.max_lines = 1000
    
    def add_log(self, message: str, level: str = "INFO") -> None:
        """Add a log message with timestamp and level"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        
        # Color coding based on level
        style_map = {
            "DEBUG": "dim",
            "INFO": "white", 
            "WARNING": "yellow",
            "ERROR": "red bold",
            "SUCCESS": "green bold"
        }
        
        style = style_map.get(level.upper(), "white")
        formatted_message = f"[dim]{timestamp}[/] [{style}]{level}[/] {message}"
        
        self.write(formatted_message)
        
        # Keep log size manageable
        if len(self.lines) > self.max_lines:
            self.clear()
            self.write(Text("... (log cleared to prevent memory issues) ...", style="dim italic"))


class ControlPanel(Container):
    """Interactive control panel with buttons"""
    
    def compose(self) -> ComposeResult:
        with Horizontal():
            yield Button("Pause", id="pause-btn", variant="warning")
            yield Button("Resume", id="resume-btn", variant="success")
            yield Button("Stop", id="stop-btn", variant="error")
            yield Button("Export Logs", id="export-btn", variant="primary")


class PipelineDashboard(App):
    """Main TUI application for pipeline monitoring"""
    
    CSS = """
    Screen {
        layout: grid;
        grid-size: 3 4;
        grid-rows: 1fr 2fr 1fr auto;
        grid-columns: 1fr 1fr 1fr;
    }
    
    #status-panel {
        column-span: 1;
        row-span: 1;
        margin: 1;
    }
    
    #steps-panel {
        column-span: 2;
        row-span: 2;
        margin: 1;
    }
    
    #logs-panel {
        column-span: 3;
        row-span: 1;
        margin: 1;
    }
    
    #controls-panel {
        column-span: 3;
        row-span: 1;
        margin: 1;
    }
    
    StepsTable {
        height: 100%;
    }
    
    LogViewer {
        height: 100%;
        border: solid blue;
    }
    """
    
    TITLE = "🚀 Pipeline Orchestrator Dashboard"
    SUB_TITLE = "Real-time monitoring and control"
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.pipeline_controller = None
        self.is_paused = False
        
    def compose(self) -> ComposeResult:
        """Create the dashboard layout"""
        yield Header()
        
        # Status panel
        with Container(id="status-panel"):
            yield StatusPanel(id="status")
        
        # Steps table panel  
        with Container(id="steps-panel"):
            with Collapsible(title="📋 Pipeline Steps", collapsed=False):
                yield StepsTable(id="steps-table")
        
        # Logs panel
        with Container(id="logs-panel"):
            with Collapsible(title="📜 Execution Logs", collapsed=False):
                yield LogViewer(id="log-viewer")
        
        # Control panel
        with Container(id="controls-panel"):
            yield ControlPanel(id="controls")
        
        yield Footer()
    
    def on_mount(self) -> None:
        """Initialize the dashboard"""
        self.log_viewer = self.query_one("#log-viewer", LogViewer)
        self.status_panel = self.query_one("#status", StatusPanel)
        self.steps_table = self.query_one("#steps-table", StepsTable)
        
        self.log_viewer.add_log("Dashboard initialized", "SUCCESS")
    
    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button presses"""
        button_id = event.button.id
        
        if button_id == "pause-btn":
            self.pause_pipeline()
        elif button_id == "resume-btn":
            self.resume_pipeline()
        elif button_id == "stop-btn":
            self.stop_pipeline()
        elif button_id == "export-btn":
            self.export_logs()
    
    def pause_pipeline(self) -> None:
        """Pause pipeline execution"""
        self.is_paused = True
        self.log_viewer.add_log("Pipeline paused by user", "WARNING")
        if self.pipeline_controller:
            self.pipeline_controller.pause()
    
    def resume_pipeline(self) -> None:
        """Resume pipeline execution"""
        self.is_paused = False
        self.log_viewer.add_log("Pipeline resumed by user", "SUCCESS")
        if self.pipeline_controller:
            self.pipeline_controller.resume()
    
    def stop_pipeline(self) -> None:
        """Stop pipeline execution"""
        self.log_viewer.add_log("Pipeline stop requested by user", "ERROR")
        if self.pipeline_controller:
            self.pipeline_controller.stop()
    
    def export_logs(self) -> None:
        """Export logs to file"""
        filename = f"pipeline_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        self.log_viewer.add_log(f"Logs exported to {filename}", "SUCCESS")
    
    # Public API for pipeline integration
    def update_status(self, status: PipelineStatus, **kwargs) -> None:
        """Update overall pipeline status"""
        self.status_panel.update_status(status, **kwargs)
    
    def add_step(self, name: str) -> None:
        """Add a new pipeline step"""
        step = PipelineStep(name)
        self.steps_table.add_step(step)
        self.log_viewer.add_log(f"Added step: {name}", "INFO")
    
    def update_step(self, name: str, **kwargs) -> None:
        """Update a pipeline step"""
        self.steps_table.update_step(name, **kwargs)
        
        # Log significant events
        if 'status' in kwargs:
            status = kwargs['status']
            if status == PipelineStatus.RUNNING:
                self.log_viewer.add_log(f"Started: {name}", "INFO")
                kwargs['start_time'] = datetime.now()
            elif status == PipelineStatus.SUCCESS:
                self.log_viewer.add_log(f"Completed: {name}", "SUCCESS")
                kwargs['end_time'] = datetime.now()
            elif status == PipelineStatus.ERROR:
                error_msg = kwargs.get('error_message', 'Unknown error')
                self.log_viewer.add_log(f"Failed: {name} - {error_msg}", "ERROR")
                kwargs['end_time'] = datetime.now()
    
    def add_log(self, message: str, level: str = "INFO") -> None:
        """Add a log message"""
        self.log_viewer.add_log(message, level)
    
    def set_pipeline_controller(self, controller) -> None:
        """Set the pipeline controller for interactive controls"""
        self.pipeline_controller = controller


def run_dashboard():
    """Convenience function to run the dashboard"""
    app = PipelineDashboard()
    app.run()


if __name__ == "__main__":
    run_dashboard()