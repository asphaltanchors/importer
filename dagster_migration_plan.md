# Meltano to Dagster Migration Plan

## Overview

This document outlines a step-by-step plan for migrating the MQI (Meltano Quickbooks Importer) project from Meltano to Dagster. The migration is structured into logical phases, each representing a potential git commit or small group of commits.

## Migration Phases

### Phase 1: Setup and Initial Configuration

#### 1. Install Dagster and dependencies
- **Description**: Set up the core Dagster infrastructure
- **Tasks**:
  - Install required packages: `dagster`, `dagster-postgres`, `dagster-dbt`, `dagster-pandas`
  - Create a `requirements-dagster.txt` file
  - Set up virtual environment for Dagster
- **Commit**: "Install Dagster dependencies and setup environment"

#### 2. Create basic Dagster project structure
- **Description**: Establish the foundational project structure
- **Tasks**:
  - Create `dagster_project` directory
  - Set up `workspace.yaml` for Dagster
  - Create initial `repository.py` file
  - Add `README.md` with migration status
  - Configure Dagster daemon for scheduling
- **Commit**: "Create basic Dagster project structure"

### Phase 2: CSV Extraction Implementation

#### 3. Implement file discovery assets
- **Description**: Create assets to discover and process input files
- **Tasks**:
  - Create assets for monitoring input directories
  - Implement file type detection (customers, invoices, sales)
  - Add metadata logging for discovered files
  - Create configuration for input/output directories
- **Commit**: "Implement file discovery assets"

#### 4. Implement CSV parsing assets
- **Description**: Create assets to parse and validate CSV files
- **Tasks**:
  - Create pandas-based CSV readers
  - Implement primary key validation based on file type
  - Add data quality checks and validation
  - Create schema definitions matching current Meltano configuration
- **Commit**: "Implement CSV parsing and validation assets"

### Phase 3: Postgres Loading Implementation

#### 5. Configure Postgres resources
- **Description**: Set up database connections and resources
- **Tasks**:
  - Create Postgres resource definitions
  - Set up connection configuration
  - Implement I/O managers for database operations
  - Configure connection pooling
- **Commit**: "Configure Postgres resources and connections"

#### 6. Implement upsert logic
- **Description**: Create assets for loading data with upsert logic
- **Tasks**:
  - Implement primary key-based upsert logic
  - Create assets for database loading
  - Add transaction management
  - Implement error handling for database operations
- **Commit**: "Implement database loading with upsert logic"

### Phase 4: dbt Integration

#### 7. Set up dbt integration
- **Description**: Integrate existing dbt models with Dagster
- **Tasks**:
  - Configure `dagster-dbt` integration
  - Link dbt models as Dagster assets
  - Set up manifest generation
  - Create dbt resource definition
- **Commit**: "Set up dbt integration with Dagster"

#### 8. Implement dbt dependencies
- **Description**: Configure proper asset dependencies for dbt
- **Tasks**:
  - Create proper asset dependencies between loading and transformation
  - Configure dbt run parameters
  - Add dbt test integration
  - Implement selective model running
- **Commit**: "Configure dbt dependencies and testing"

### Phase 5: File Management

#### 9. Implement file movement logic
- **Description**: Create assets for file archiving and management
- **Tasks**:
  - Create assets for file archiving
  - Implement success/failure handling
  - Add file metadata tracking
  - Create directory structure for processed files
- **Commit**: "Implement file archiving and management"

#### 10. Add error handling and notifications
- **Description**: Enhance error handling and add notifications
- **Tasks**:
  - Implement retry logic for failed operations
  - Add failure notifications
  - Create error logging assets
  - Implement detailed error reporting
- **Commit**: "Add comprehensive error handling and notifications"

### Phase 6: Scheduling and Orchestration

#### 11. Set up schedules and sensors
- **Description**: Configure automated execution
- **Tasks**:
  - Create file-based sensors for detecting new files
  - Implement daily schedule for regular processing
  - Add partition-based backfills
  - Configure sensor and schedule policies
- **Commit**: "Set up schedules and file sensors"

#### 12. Configure run configuration
- **Description**: Fine-tune execution configuration
- **Tasks**:
  - Set up environment-specific configurations (dev, staging, prod)
  - Implement resource limits
  - Configure concurrency settings
  - Add run tags and metadata
- **Commit**: "Configure environment-specific run configurations"

### Phase 7: Testing and Validation

#### 13. Create test suite
- **Description**: Implement comprehensive testing
- **Tasks**:
  - Create unit tests for assets
  - Implement integration tests
  - Add data validation tests
  - Set up test fixtures and mock data
- **Commit**: "Create comprehensive test suite"

#### 14. Perform parallel testing
- **Description**: Validate Dagster implementation against Meltano
- **Tasks**:
  - Run Meltano and Dagster pipelines in parallel
  - Create comparison scripts for outputs
  - Document performance differences
  - Fix any discrepancies
- **Commit**: "Implement parallel testing and validation"

### Phase 8: Deployment and Documentation

#### 15. Set up deployment
- **Description**: Configure production deployment
- **Tasks**:
  - Create deployment configuration
  - Set up monitoring
  - Implement logging infrastructure
  - Configure resource requirements
- **Commit**: "Set up production deployment configuration"

#### 16. Complete documentation
- **Description**: Create comprehensive documentation
- **Tasks**:
  - Create user guides for operations
  - Document architecture and design decisions
  - Add troubleshooting guides
  - Create runbook for common tasks
- **Commit**: "Add comprehensive documentation"

### Phase 9: Cutover and Cleanup

#### 17. Perform cutover
- **Description**: Switch production to Dagster
- **Tasks**:
  - Switch production schedules to Dagster
  - Verify initial runs
  - Monitor performance
  - Create rollback plan
- **Commit**: "Perform production cutover to Dagster"

#### 18. Clean up Meltano components
- **Description**: Remove or archive Meltano components
- **Tasks**:
  - Archive Meltano configuration
  - Remove unused dependencies
  - Update documentation
  - Archive old scripts
- **Commit**: "Clean up Meltano components and finalize migration"

## Implementation Details

### Directory Structure

The new Dagster project will have the following structure:

```
mqi/
├── dagster_project/
│   ├── assets/
│   │   ├── __init__.py
│   │   ├── csv_extraction.py
│   │   ├── postgres_loading.py
│   │   ├── file_management.py
│   │   └── dbt_assets.py
│   ├── resources/
│   │   ├── __init__.py
│   │   ├── postgres.py
│   │   └── file_system.py
│   ├── schedules/
│   │   ├── __init__.py
│   │   └── schedules.py
│   ├── sensors/
│   │   ├── __init__.py
│   │   └── file_sensors.py
│   ├── __init__.py
│   ├── repository.py
│   └── constants.py
├── transform/  # Existing dbt project
├── workspace.yaml
├── dagster.yaml
└── README.md
```

### Key Configuration Files

#### workspace.yaml
```yaml
load_from:
  - python_module:
      module_name: dagster_project
      attribute: repository
```

#### dagster.yaml
```yaml
scheduler:
  module: dagster.core.scheduler
  class: DagsterDaemonScheduler

run_coordinator:
  module: dagster.core.run_coordinator
  class: QueuedRunCoordinator
  config:
    max_concurrent_runs: 4

run_launcher:
  module: dagster.core.launcher
  class: DefaultRunLauncher

storage:
  sqlite:
    base_dir: /path/to/dagster/storage
```

## Migration Timeline

- **Phase 1-2**: Week 1
- **Phase 3-4**: Week 2
- **Phase 5-6**: Week 3
- **Phase 7-8**: Week 4
- **Phase 9**: Week 5

## Success Criteria

The migration will be considered successful when:

1. All current Meltano functionality is replicated in Dagster
2. File discovery, processing, and archiving work reliably
3. Data quality checks are in place
4. Scheduling and monitoring are operational
5. Documentation is complete
6. Performance is equal to or better than the current Meltano implementation

## Rollback Plan

If issues arise during the migration, the following rollback steps will be taken:

1. Revert to Meltano schedules
2. Document issues encountered
3. Create targeted fixes
4. Retry migration with fixes in place
