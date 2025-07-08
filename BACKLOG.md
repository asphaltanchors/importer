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

---

## 🔥 Orchestrator Architecture Overhaul *(HIGH PRIORITY)*

**Priority**: High  
**Status**: Critical - Current implementation is overly complex and brittle  
**Context**: Discovered during duplicate data debugging - orchestrator lacks intelligent state management

### Current Architecture Problems

1. **Manual Mode Selection Complexity**
   - Forces users to understand internal concepts (seed vs incremental vs full)
   - No automatic detection of database state
   - Requires deep knowledge of when to use which mode

2. **Redundant File Processing**
   - Parses files multiple times just to skip them later
   - No intelligent file state tracking
   - Re-processes already loaded files unnecessarily

3. **No State Awareness**
   - Can't detect if database is empty (needs seed)
   - Doesn't track which files have been processed
   - No detection of new/changed files

4. **Complex Configuration**
   - Too many manual parameters and flags
   - Should have saner defaults based on system state
   - Configuration complexity grows with each source

### Proposed Smart Architecture

#### **Intelligent Pipeline States**
```
Empty Database → Auto Seed Mode
Has Data + New Files → Auto Incremental Mode  
Has Data + No New Files → Skip/Status Report
Error Recovery → Auto Repair Mode
```

#### **File State Management**
- **Track processed files** by filename in database metadata table
- **Detect new files** automatically via filesystem monitoring
- **Process only newer files** based on filename timestamps
- **Skip already processed files** - no re-parsing of old data

#### **Smart Defaults System**
```python
# Current (complex)
python orchestrator.py --mode full --load-mode incremental

# Proposed (intelligent)
python orchestrator.py  # Auto-detects what needs to be done
```

#### **Database State Detection**
```sql
-- Check if database needs seeding
SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'analytics_mart';

-- Check last processed files  
SELECT source, last_file, last_processed_date FROM pipeline_metadata;

-- Detect new files needing processing
SELECT * FROM pending_files_view;
```

### Implementation Plan

#### **Phase 1: Smart State Detection** *(Week 1)*
- Add `pipeline_metadata` table to track execution state
- Implement database state detection functions
- Add file tracking and change detection
- Create automatic mode selection logic

#### **Phase 2: Simplified CLI Interface** *(Week 2)*  
- Replace complex mode flags with intelligent defaults
- Add `--force-seed`, `--force-incremental` for overrides only
- Implement `--status` command to show current state
- Add `--dry-run` to preview what would be processed

#### **Phase 3: File State Management** *(Week 3)*
- Track processed files by filename only
- Implement incremental file detection based on filename timestamps  
- Add file dependency resolution (customers before orders)
- Handle partial failures and resume capability

#### **Phase 4: Error Recovery & Robustness** *(Week 4)*
- Automatic retry logic for transient failures
- Better error classification (data vs infrastructure)
- Graceful degradation for partial source failures
- Recovery suggestions based on error patterns

### Key Benefits

1. **Zero Configuration** - Works out of the box with intelligent defaults
2. **Incremental by Design** - Only processes what's needed
3. **Self-Healing** - Automatically detects and fixes common issues
4. **Performance** - Eliminates redundant file scanning and processing
5. **Reliability** - State tracking prevents data loss and duplicate processing

### Technical Architecture

#### **New Components Needed**
```
orchestrator.py (simplified)
├── pipeline_state.py (NEW) - Database state detection
├── file_manager.py (NEW) - File tracking and change detection  
├── smart_scheduler.py (NEW) - Intelligent execution planning
└── recovery_manager.py (NEW) - Error handling and recovery
```

#### **Database Schema Additions**
```sql
CREATE TABLE pipeline_metadata (
    source_name VARCHAR,
    last_run_timestamp TIMESTAMP,
    last_run_status VARCHAR,
    files_processed JSONB,
    execution_stats JSONB
);

CREATE TABLE processed_files (
    source_name VARCHAR,
    filename VARCHAR,
    file_date DATE,  -- Extracted from filename timestamp
    processed_timestamp TIMESTAMP,
    row_count INTEGER,
    PRIMARY KEY (source_name, filename)
);
```

### Success Metrics

- **Simplicity**: `python orchestrator.py` works 90% of the time
- **Performance**: 50% reduction in unnecessary file processing
- **Reliability**: Automatic recovery from 80% of common failure scenarios
- **User Experience**: Non-technical users can run pipeline without documentation

### Migration Strategy

1. **Backwards Compatibility**: Keep existing CLI flags during transition
2. **Gradual Rollout**: Start with QuickBooks source, expand to others
3. **Fallback Mode**: Allow manual override when auto-detection fails
4. **Documentation**: Clear migration guide from old to new approach

---

## 🔔 Production Reliability Improvements *(SINGLE-USER FOCUS)*

**Priority**: High  
**Status**: Planned  
**Context**: Production cron job on VPS - focus on "boringly reliable" automation for single user

### **Critical: Failure Notifications & Status**

#### **1. Smart Failure Alerts** *(Week 1)*
**Problem**: When 3am cron fails, need immediate awareness with actionable info
- **Email notifications** on pipeline failures with error summary
- **Success confirmations** for daily runs (optional, configurable)
- **Clear error classifications**: "Data issue" vs "Infrastructure issue" vs "New files missing"
- **"Last known good" timestamp** in all notifications

**Implementation**:
```python
# Add to orchestrator.py
class NotificationManager:
    def send_failure_alert(self, error_type, error_summary, last_success_date)
    def send_success_summary(self, files_processed, execution_time)
```

#### **2. Quick Status Command** *(Week 1)*
**Problem**: Need to quickly check pipeline health without VPS login
- `python orchestrator.py --status` shows:
  - Last successful run date/time
  - Files pending processing (if any)
  - Data freshness of key mart tables
  - Quick row count health check
  - Any obvious issues

**Output Example**:
```
📊 Pipeline Status (2025-07-08 14:30:00)
✅ Last successful run: 2025-07-08 03:00:00 (11 hours ago)
📁 Pending files: None
📈 Data freshness: 
   • fct_orders: 1 day old (13,553 records)
   • fct_customers: 1 day old (7,406 records)
🔍 Health: All tables present, row counts normal
```

#### **3. Graceful Partial Failures** *(Week 2)*
**Problem**: One failing component shouldn't break everything else
- **Continue processing** other sources if one fails
- **Clear separation** between "partial success" and "total failure"
- **Recovery suggestions** based on failure type
- **Retry logic** for common transient issues (DB connection, file locks)

### **Nice-to-Have: Operational Convenience**

#### **4. Cron Job Wrapper** *(Week 3)*
**Problem**: Cron output management and scheduling convenience
- **Wrapper script** that handles logging and notifications
- **Log rotation** to prevent disk space issues
- **Timezone handling** for clear timestamps
- **Environment validation** before execution

```bash
#!/bin/bash
# cron_wrapper.sh
export TZ="America/New_York"
cd /path/to/importer
python orchestrator.py 2>&1 | tee -a logs/pipeline_$(date +%Y%m%d).log
```

#### **5. Data Staleness Monitoring** *(Week 4)*
**Problem**: Need to know when data gets too old
- **Configurable staleness thresholds** (e.g., warn if data >2 days old)
- **Automatic alerts** when thresholds exceeded
- **Business day awareness** (don't alert on weekends)

### **Explicitly NOT Building** *(Wrong for Single-User VPS)*

❌ **Resource Management** - VPS + daily cron doesn't need process pooling  
❌ **Concurrent Execution** - Sequential processing is fine for data volumes  
❌ **Circuit Breakers** - Manual retry is acceptable for single user  
❌ **Complex Observability** - Metrics dashboards overkill for one person  
❌ **Multi-tenant Features** - Will never have multiple users  
❌ **Real-time Monitoring** - Daily batch is sufficient  

### **Success Criteria**

- **Zero Surprise Failures**: Get notified immediately when something breaks
- **Quick Health Checks**: Status command answers "is my data current?" in 5 seconds
- **Reliable Automation**: Pipeline runs quietly and successfully 95%+ of the time
- **Fast Debugging**: Error messages guide you to the fix without log diving
- **Peace of Mind**: Confidence that production pipeline "just works"

### **Technical Implementation Notes**

**Notification Backends**:
- SMTP for email (simple, reliable)
- Optional: Slack webhook for instant notifications
- Configuration via environment variables

**Status Data Storage**:
```sql
CREATE TABLE pipeline_status (
    run_timestamp TIMESTAMP PRIMARY KEY,
    overall_status VARCHAR,  -- success, partial_failure, error
    sources_processed JSONB,
    execution_time_seconds INTEGER,
    files_processed JSONB,
    error_summary TEXT
);
```

**Cron Schedule Recommendation**:
```cron
# Run daily at 3 AM with wrapper
0 3 * * * /path/to/cron_wrapper.sh
```