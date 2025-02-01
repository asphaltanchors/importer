# Use Python slim image as base
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    POETRY_VERSION=2.0.1 \
    POETRY_VIRTUALENVS_CREATE=false

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        gcc \
        python3-dev \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install supercronic
# Latest releases available at https://github.com/aptible/supercronic/releases
ENV SUPERCRONIC_URL=https://github.com/aptible/supercronic/releases/download/v0.2.33/supercronic-linux-amd64 \
    SUPERCRONIC_SHA1SUM=71b0d58cc53f6bd72cf2f293e09e294b79c666d8 \
    SUPERCRONIC=supercronic-linux-amd64

RUN curl -fsSLO "$SUPERCRONIC_URL" \
 && echo "${SUPERCRONIC_SHA1SUM}  ${SUPERCRONIC}" | sha1sum -c - \
 && chmod +x "$SUPERCRONIC" \
 && mv "$SUPERCRONIC" "/usr/local/bin/${SUPERCRONIC}" \
 && ln -s "/usr/local/bin/${SUPERCRONIC}" /usr/local/bin/supercronic

# Install Poetry
RUN pip install poetry==${POETRY_VERSION}

# Set working directory
WORKDIR /app

# Copy project files
COPY pyproject.toml ./
COPY importer ./importer
COPY scripts ./scripts

# Ensure scripts are executable
RUN chmod -R +x /app/scripts

# Install Python dependencies
RUN poetry install --no-interaction --no-ansi 

# Set up cron job and startup script
COPY scripts/run_import.sh /app/scripts/run_import.sh
COPY scripts/startup.sh /app/startup.sh
COPY crontab.txt /app/crontab.txt
RUN chmod +x /app/scripts/run_import.sh /app/startup.sh

# Create directory for logs and input files
RUN mkdir -p /var/log/importer /data/input \
    && touch /var/log/importer/import.log \
    && chmod 666 /var/log/importer/import.log

# Create non-root user
RUN useradd -m -u 1000 importer_user \
    && chown -R importer_user:importer_user /app /data /var/log/importer

# Switch to non-root user
USER importer_user

# Volume for input files and logs
VOLUME ["/data/input", "/var/log/importer"]

# Set startup script as entrypoint
ENTRYPOINT ["/app/startup.sh"]
