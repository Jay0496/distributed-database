#!/usr/bin/env bash
set -u

echo "=== Distributed DB demo ==="

echo "[1] Bring up cluster"
docker compose up -d

echo "[2] Wait for nodes to be healthy"
sleep 10

echo "[3] Put a value (rs32)"
docker compose run --rm gateway put demo-key "hello-ta" --mode rs32

echo "[4] Get the value back"
docker compose run --rm gateway get demo-key

echo "[5] Cluster status"
docker compose run --rm gateway status

echo "[6] Kill node3"
docker compose stop node3

echo "[7] Read still succeeds with one node down"
docker compose run --rm gateway get demo-key

echo "[8] Restart node3"
docker compose start node3

echo "[9] Final status"
sleep 5
docker compose run --rm gateway status

echo "[10] Tear down"
docker compose down -v

echo "=== Demo complete ==="
EOF