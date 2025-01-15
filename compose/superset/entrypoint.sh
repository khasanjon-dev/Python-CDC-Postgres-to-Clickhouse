#!/bin/bash
set -e

# Initialize Superset
superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname Admin \
  --email admin@localhost \
  --password admin || true

superset db upgrade
superset init

# Start Superset
superset run -p 8088 --with-threads --reload --debugger
