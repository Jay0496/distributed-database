#!/usr/bin/env bash
set -u

README="${1:-README.md}"
fail=0

for needle in "docker compose up" "put" "get" "status" "mode" "smoke"; do
    if ! grep -qi -- "$needle" "$README"; then
        echo "missing required docs entry: $needle" >&2
        fail=1
    fi
done

if [ "$fail" -ne 0 ]; then
    echo "check-docs: FAILED" >&2
    exit 1
fi

echo "check-docs: OK"
exit 0
