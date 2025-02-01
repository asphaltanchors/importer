#!/bin/bash
set -e

# Start cron service
service cron start

# Keep container running
tail -f /var/log/importer/import.log
