#!/bin/bash
set -e

# Upgrade the database
airflow db upgrade

# Create admin user if it doesn't exist
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com || true

# Execute the main command (webserver or scheduler)
exec airflow "$@"