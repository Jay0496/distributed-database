#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python -m grpc_tools.protoc -I proto --python_out=gen --grpc_python_out=gen proto/kv.proto
python - <<'PY'
import pathlib
p = pathlib.Path("gen/kv_pb2_grpc.py")
s = p.read_text()
s = s.replace("import kv_pb2 as", "from . import kv_pb2 as")
p.write_text(s)
PY