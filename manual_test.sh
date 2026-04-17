#!/bin/bash
# Manual testing script for EventStream API

set -e

echo "=== EventStream API Manual Test ==="
echo ""

# Start services
echo "1. Starting Docker containers..."
docker-compose up -d
echo "Waiting for services to be healthy..."
sleep 10

# Check health
echo ""
echo "2. Checking API health..."
curl -s http://localhost:8000/api/health | jq '.'

# Get sources
echo ""
echo "3. Getting available sources..."
curl -s http://localhost:8000/api/sources | jq '.'

# Get events
echo ""
echo "4. Getting recent events..."
curl -s http://localhost:8000/api/events?limit=5 | jq '.'

# Test SSE stream (timeout after 5 seconds)
echo ""
echo "5. Testing SSE stream (5 second sample)..."
timeout 5 curl -s -N http://localhost:8000/api/stream || true

echo ""
echo "=== Manual test complete ==="
echo "To stop: docker-compose down"
