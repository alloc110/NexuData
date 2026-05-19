#!/usr/bin/env bash

set -e

echo "Waiting for PostgreSQL..."

sleep 15

echo "Initializing Superset..."

superset db upgrade

superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@superset.com \
  --password admin || true

superset init

echo "Starting Superset..."

/usr/bin/run-server.sh