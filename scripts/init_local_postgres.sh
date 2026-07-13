#!/usr/bin/env bash
set -euo pipefail

DB_USER="postgres"
DB_PASSWORD="123456"
DB_NAME="token_discover"

sudo -u postgres psql <<SQL
ALTER USER ${DB_USER} WITH PASSWORD '${DB_PASSWORD}';

SELECT 'CREATE DATABASE ${DB_NAME}'
WHERE NOT EXISTS (
  SELECT FROM pg_database WHERE datname = '${DB_NAME}'
)\gexec
SQL

echo "postgres local init ok: user=${DB_USER} db=${DB_NAME}"
