FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    cron \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set default environment variables for Docker deployment
ENV DROPBOX_PATH=/dropbox/Dropbox/quickbooks-csv
ENV DBT_TARGET=prod

# Copy application code
COPY . .

# Install DBT package dependencies
RUN dbt deps

# Create necessary directories
RUN mkdir -p /var/log/cron

# Copy cron configuration
COPY cron-schedule /etc/cron.d/pipeline-cron

# Set proper permissions for cron
RUN chmod 0644 /etc/cron.d/pipeline-cron && \
    crontab /etc/cron.d/pipeline-cron

# Make entrypoint executable
RUN chmod +x entrypoint.sh

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD pgrep cron > /dev/null || exit 1

# Set entrypoint
ENTRYPOINT ["./entrypoint.sh"]

# Default command (run cron)
CMD ["cron"]
