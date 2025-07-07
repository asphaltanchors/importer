# Project Backlog

This file tracks feature requests, improvements, and technical debt items for the QuickBooks data pipeline.

## TUI Dashboard Improvements

**Priority**: Medium  
**Status**: Planned  
**Context**: Current TUI is a monitoring-only interface that doesn't provide pipeline management capabilities

### Current State Analysis
- **Files involved**: 
  - `tui/dashboard.py` - Main TUI interface using Textual library
  - `orchestrator.py` - Pipeline orchestrator with `--tui` flag integration
  - `tui/progress_tracker.py` - Thread-safe communication layer
  - `tui/subprocess_progress_parser.py` - Parses subprocess output for progress

### Current Issues
1. **Layout problems**: Text overlaying buttons due to CSS grid issues
2. **Non-functional controls**: Pause/Resume/Stop buttons are UI-only, don't control pipeline
3. **Limited management**: No way to see pending files, kick off imports, or manage pipeline state
4. **Missing integration**: No actual process control implementation

### Desired State
Transform TUI from monitoring-only to a **pipeline management interface**:

#### Core Features Needed
1. **File Discovery**:
   - Show pending files in `/dropbox/quickbooks-csv/input/` directory
   - Display last processed files and dates
   - Show seed vs incremental file status

2. **Pipeline Control**:
   - Launch seed, incremental, or full pipeline modes
   - Actually control running processes (not just monitor)
   - Queue multiple operations

3. **Status Overview**:
   - Current pipeline state (idle, running, failed)
   - Last run results and timing
   - DBT model status and freshness

4. **Interactive Management**:
   - File selection for processing
   - Manual data quality check triggers
   - DBT model refresh controls

#### Technical Implementation Notes
- **Process Control**: Need to implement actual subprocess management with signal handling
- **State Management**: Track pipeline state across components
- **File System Integration**: Monitor Dropbox folder for new files
- **DBT Integration**: Query DBT state and model freshness
- **Configuration**: Allow runtime configuration changes

### Implementation Approach
1. **Phase 1**: Fix current layout issues and make existing TUI functional
2. **Phase 2**: Add file discovery and status overview
3. **Phase 3**: Implement actual pipeline control and management
4. **Phase 4**: Add advanced features like queueing and selective processing

### Architecture Considerations
- **Current**: `Pipeline Subprocess → Progress Parser → Progress Tracker → TUI Dashboard`
- **Needed**: `TUI Dashboard ↔ Pipeline Manager ↔ File System Monitor + DBT Interface`

### Related Files to Modify
- `tui/dashboard.py` - Main interface redesign
- `orchestrator.py` - Add management capabilities
- `pipeline.py` - Add controllable execution modes
- New: `tui/pipeline_manager.py` - Central control interface
- New: `tui/file_monitor.py` - File system monitoring
- New: `tui/dbt_interface.py` - DBT status and control

---

## Other Backlog Items

### Data Quality Improvements
**Priority**: High  
**Status**: Ongoing  
**Context**: Items tracked in `DBT_CANDIDATES.md` for dashboard integration

### Performance Optimization
**Priority**: Medium  
**Status**: Planned  
**Context**: Pipeline execution time and resource usage optimization

### Error Handling Enhancement
**Priority**: Medium  
**Status**: Planned  
**Context**: Better error recovery and user feedback