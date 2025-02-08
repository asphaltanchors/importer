# Technical Context

## Technologies Used

### Core Stack
- Python 3.8+
- Poetry (package manager)
- SQLAlchemy ORM
- PostgreSQL Database
- Pandas for data processing
- Click for CLI interface

### Key Libraries
- pandas: CSV processing and data manipulation
- sqlalchemy: Database ORM and session management
- click: Command-line interface
- pytest: Testing framework
- python-dotenv: Environment configuration

## Development Setup
1. Poetry Installation & Setup
   - Install Poetry: `curl -sSL https://install.python-poetry.org | python3 -`
   - Install dependencies: `poetry install`
   - Run commands: `poetry run importer <command>`

2. PostgreSQL database
   - Configure in .env: `DATABASE_URL="postgresql://user:password@localhost:5432/dbname"`

3. Development tools (VSCode, etc.)

## Technical Constraints

### Database Schema
- Uses QuickBooks ID as primary key when available
- Falls back to UUID for records without QuickBooks ID
- Foreign key relationships between tables
- NOT NULL constraints on critical fields
- JSON fields for flexible data storage

### Data Processing
1. Batch Processing:
   - Configurable batch size
   - Transaction management
   - Error tracking per batch
   - Progress reporting

2. Idempotent Operations:
   - Lookup before create
   - Update existing records
   - Track processed records
   - Handle duplicates

3. Error Handling:
   - Error limits
   - Error categorization
   - Detailed error messages
   - Debug logging

### Performance Considerations
- Batch size tuning
- Index optimization
- Session management
- Memory usage (pandas)
- Transaction boundaries

## Technical Decisions

### Customer Processing
1. Primary Keys:
   - Use QuickBooks ID when available
   - Generate UUID for new customers without QuickBooks ID
   - Maintain stable relationships

2. Lookup Strategy:
   - QuickBooks ID first
   - Exact name match second
   - Normalized name match third
   - Create new as last resort

3. Update Logic:
   - Update names if changed
   - Update QuickBooks ID if found
   - Preserve creation timestamps
   - Track modifications

### Data Validation
- Pre-processing validation
- Schema validation
- Business rule validation
- Error reporting

### Logging
- Structured logging
- Debug mode
- Error tracking
- Statistics collection
