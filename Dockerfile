FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        postgresql-client \
        postgresql-server-dev-all \
        gcc \
        python3-dev \
        libc-dev \
    && rm -rf /var/lib/apt/lists/*

# Install supercronic
ENV SUPERCRONIC_URL=https://github.com/aptible/supercronic/releases/download/v0.2.33/supercronic-linux-amd64 \
    SUPERCRONIC_SHA1SUM=71b0d58cc53f6bd72cf2f293e09e294b79c666d8 \
    SUPERCRONIC=supercronic-linux-amd64

RUN curl -fsSLO "$SUPERCRONIC_URL" \
    && echo "${SUPERCRONIC_SHA1SUM}  ${SUPERCRONIC}" | sha1sum -c - \
    && chmod +x "$SUPERCRONIC" \
    && mv "$SUPERCRONIC" /usr/local/bin/supercronic \
    && ln -s /usr/local/bin/supercronic /usr/local/bin/supercronic-linux-amd64

WORKDIR /app

# Copy requirements and setup files
COPY requirements.txt setup.py ./

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY importer ./importer

# Install the package
RUN pip install --no-cache-dir -e .

# Copy scripts and config
COPY scripts ./scripts
COPY crontab.txt /app/crontab.txt

# Ensure scripts are executable
RUN chmod +x /app/scripts/run_import.sh /app/scripts/startup.sh

# Create non-root user
RUN useradd -m -u 1000 importer_user \
    && chown -R importer_user:importer_user /app

# Switch to non-root user
USER importer_user

# Set startup script as entrypoint
ENTRYPOINT ["/app/scripts/startup.sh"]
