#!/bin/bash
# This script populates a Redis instance with sample data for testing.

set -e  # Exit on error

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

# Populate multiple Redis databases with sample data
echo "Populating Redis with sample data across multiple databases..."
{
  echo "SELECT 0"
  seq 1 1000000 | awk '{print "SETEX string_key_db0_" $1 " 3600 value_" $1}'
  echo "SELECT 1"
  seq 1 300000 | awk '{print "SETEX string_key_db1_" $1 " 3600 value_" $1}'
  echo "SELECT 2"
  seq 1 200000 | awk '{print "SETEX string_key_db2_" $1 " 3600 value_" $1}'
} | docker exec -i ${REDIS_CONTAINER} redis-cli --pipe

echo "Done! Redis has been populated successfully."