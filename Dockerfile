# Use Python slim image as base
FROM python:3.9-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    POETRY_VERSION=1.7.1 \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_CREATE=false

# Add Poetry to PATH
ENV PATH="$POETRY_HOME/bin:$PATH"

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        cron \
        curl \
        gcc \
        python3-dev \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -

# Set working directory
WORKDIR /app

# Copy project files
COPY pyproject.toml poetry.lock ./
COPY csv_importer ./csv_importer
COPY scripts ./scripts

# Install Python dependencies
RUN poetry install --no-dev --no-interaction

# Set up cron job and startup script
COPY scripts/run_import.sh /etc/cron.daily/run_import
COPY scripts/startup.sh /app/startup.sh
RUN chmod +x /etc/cron.daily/run_import /app/startup.sh

# Create directory for logs
RUN mkdir -p /var/log/csv_importer \
    && touch /var/log/csv_importer/import.log \
    && chmod 666 /var/log/csv_importer/import.log

# Create directory for input files
RUN mkdir -p /data/input

# Add cron job to crontab
RUN echo "0 0 * * * /etc/cron.daily/run_import >> /var/log/csv_importer/import.log 2>&1" > /etc/crontab

# Create non-root user
RUN useradd -m -u 1000 csv_user \
    && chown -R csv_user:csv_user /app /data /var/log/csv_importer

# Switch to non-root user
USER csv_user

# Volume for input files and logs
VOLUME ["/data/input", "/var/log/csv_importer"]

# Set startup script as entrypoint
ENTRYPOINT ["/app/startup.sh"]
