#!/usr/bin/env bash
set -euo pipefail

cleanup() {
    docker compose down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

# 1. clean slate
docker compose down -v

# 2. bring up cluster
docker compose up -d

# 3. wait for healthchecks (up to 30s)
deadline=$((SECONDS + 30))
while true; do
    healthy=$(docker compose ps --format '{{.Health}}' | grep -c healthy || true)
    if [ "${healthy}" -ge 5 ]; then
        break
    fi
    if [ "${SECONDS}" -ge "${deadline}" ]; then
        echo "timeout waiting for nodes to become healthy" >&2
        exit 1
    fi
    sleep 2
done

# 4. put
put_out=$(docker compose run --rm gateway put smoke-key "hello" --mode rs32 2>/dev/null)
echo "${put_out}"
echo "${put_out}" | grep -q '"ok": true' || { echo "put failed" >&2; exit 1; }

# 5. get round-trip
get_out=$(docker compose run --rm gateway get smoke-key 2>/dev/null)
echo "${get_out}"
echo "${get_out}" | grep -q '"found": true' || { echo "get failed" >&2; exit 1; }
echo "${get_out}" | grep -q '"value": "hello"' || { echo "value mismatch" >&2; exit 1; }

# 6. kill one node
docker stop node3

# 7. get still works with one node down
get_out=$(docker compose run --rm gateway get smoke-key 2>/dev/null)
echo "${get_out}"
echo "${get_out}" | grep -q '"found": true' || { echo "get failed (1 node down)" >&2; exit 1; }
echo "${get_out}" | grep -q '"value": "hello"' || { echo "value mismatch (1 node down)" >&2; exit 1; }

# 8. kill a second node (RS tolerates 2 down)
docker stop node4

# 9. get still works with two nodes down
get_out=$(docker compose run --rm gateway get smoke-key 2>/dev/null)
echo "${get_out}"
echo "${get_out}" | grep -q '"found": true' || { echo "get failed (2 nodes down)" >&2; exit 1; }
echo "${get_out}" | grep -q '"value": "hello"' || { echo "value mismatch (2 nodes down)" >&2; exit 1; }