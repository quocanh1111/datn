#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    SELECT 'CREATE DATABASE airflow' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec
    SELECT 'CREATE DATABASE nessie' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'nessie')\gexec
    SELECT 'CREATE DATABASE superset' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'superset')\gexec
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname nessie <<-EOSQL
    CREATE EXTENSION IF NOT EXISTS "pgcrypto";
EOSQL

echo "Databases and extensions created successfully."