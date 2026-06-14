#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE IF NOT EXISTS airflow;
    CREATE DATABASE IF NOT EXISTS nessie;
    CREATE DATABASE IF NOT EXISTS superset;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname nessie <<-EOSQL
    CREATE EXTENSION IF NOT EXISTS "pgcrypto";
EOSQL

echo "Databases and extensions created successfully."