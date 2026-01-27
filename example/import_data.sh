#!/bin/bash
# This script imports data into a Redis instance from a dump file.

# Exit on error
set -e

REDIS_CONTAINER="redis"
MAX_RETRIES=30
RETRY_INTERVAL=1

# Start Redis (idempotent - does nothing if already running)
echo "Ensuring Redis container is running..."
docker compose up -d redis

# Wait for Redis to be ready
echo "Waiting for Redis to accept connections..."
for i in $(seq 1 $MAX_RETRIES); do
  if docker exec ${REDIS_CONTAINER} redis-cli ping 2>/dev/null | grep -q PONG; then
    echo "Redis is ready"
    break
  fi
  if [ $i -eq $MAX_RETRIES ]; then
    echo "Error: Redis failed to respond within ${MAX_RETRIES} seconds"
    docker compose logs redis
    exit 1
  fi
  echo "Waiting for Redis... (attempt $i/$MAX_RETRIES)"
  sleep $RETRY_INTERVAL
done

# Clear existing data
echo "Clearing existing data in Redis..."
docker exec ${REDIS_CONTAINER} redis-cli FLUSHALL

# Import data from dump.rdb
echo "Importing data from dump.resp into Redis..."
docker exec -i ${REDIS_CONTAINER} redis-cli --pipe < dump.resp