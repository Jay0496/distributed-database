#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python -m grpc_tools.protoc -I proto --python_out=gen --grpc_python_out=gen proto/kv.proto
python - <<'PY'
p = "gen/kv_pb2_grpc.py"
with open(p, "rb") as f:
    s = f.read()
s = s.replace(b"import kv_pb2 as", b"from . import kv_pb2 as")
with open(p, "wb") as f:
    f.write(s)
PY