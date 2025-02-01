# CSV Importer

A Python tool for importing CSV data into a database.

## Local Development Setup

1. Install Poetry (package manager):
```bash
curl -sSL https://install.python-poetry.org | python3 -
```

2. Create a `.env` file in the project root:
```bash
DATABASE_URL="postgresql://user:password@localhost:5432/dbname"
```

3. Install dependencies:
```bash
poetry install
```

## Usage

Test database connectivity:
```bash
poetry run importer test-connection
```

## Docker Setup

1. Build the Docker image:
```bash
docker build -t py-importer .
```

2. Run the container:

```bash
docker run --rm \
  --env-file .env.docker \
  py-importer poetry run importer test-connection
```

```bash
docker run -d \
  --name py-importer \
  -v $(pwd)/data/input:/data/input \
  --env-file .env \
  py-importer
```

The container will:
- Run a daily import job via cron
- Process CSV files from `/data/input`
- Archive processed files
- Log operations to `/var/log/importer/import.log`

## Project Structure

- `pyproject.toml` - Project configuration and dependencies
- `importer/` - Main package directory
  - `cli.py` - CLI commands
  - `importer.py` - Core import logic
- `scripts/` - Shell scripts for Docker operations
  - `startup.sh` - Container entrypoint script
  - `run_import.sh` - CSV import execution script
- `.env` - Environment variables (not in version control)
- `Dockerfile` - Container definition
