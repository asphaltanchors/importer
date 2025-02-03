# Build stage
FROM python:3.11-slim AS builder

# Set Poetry configuration
ENV POETRY_VERSION=2.0.1
ENV POETRY_HOME=/opt/poetry
ENV POETRY_NO_INTERACTION=1
ENV POETRY_VIRTUALENVS_IN_PROJECT=1
ENV POETRY_VIRTUALENVS_CREATE=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV POETRY_CACHE_DIR=/opt/.cache

# Install poetry
RUN pip install "poetry==${POETRY_VERSION}"

# Install supercronic
ENV SUPERCRONIC_URL=https://github.com/aptible/supercronic/releases/download/v0.2.33/supercronic-linux-amd64 \
    SUPERCRONIC_SHA1SUM=71b0d58cc53f6bd72cf2f293e09e294b79c666d8 \
    SUPERCRONIC=supercronic-linux-amd64

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && curl -fsSLO "$SUPERCRONIC_URL" \
    && echo "${SUPERCRONIC_SHA1SUM}  ${SUPERCRONIC}" | sha1sum -c - \
    && chmod +x "$SUPERCRONIC" \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy dependency files
COPY pyproject.toml ./

# Install dependencies and clear cache
RUN poetry install --no-root && rm -rf $POETRY_CACHE_DIR

# Runtime stage
FROM python:3.11-slim AS runtime

# Set environment variables
ENV VIRTUAL_ENV=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Install runtime system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Copy supercronic from builder
COPY --from=builder /supercronic-linux-amd64 /usr/local/bin/supercronic
RUN ln -s /usr/local/bin/supercronic /usr/local/bin/supercronic-linux-amd64

# Copy virtual environment from builder
COPY --from=builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}

# Set working directory
WORKDIR /app

# Copy application code
COPY importer ./importer
COPY scripts ./scripts

# Set up cron job and startup script
COPY scripts/run_import.sh /app/scripts/run_import.sh
COPY scripts/startup.sh /app/startup.sh
COPY crontab.txt /app/crontab.txt

# Ensure scripts are executable
RUN chmod +x /app/scripts/run_import.sh /app/startup.sh

# Create directory for logs and input files
RUN mkdir -p /var/log/importer /data/input /data/processed /data/failed \
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
