"""Node entrypoint."""

import argparse
import os
import sys

try:
    import grpc
except Exception:  # pragma: no cover
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    import grpc

_REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

try:
    from node.storage import Storage
    from node.raft import RaftNode
    from node.server import NodeService, GrpcTransport
except Exception:  # pragma: no cover
    sys.path.insert(0, os.path.dirname(__file__))
    from storage import Storage  # type: ignore
    from raft import RaftNode  # type: ignore
    from server import NodeService, GrpcTransport  # type: ignore

import gen.kv_pb2_grpc as kv_pb2_grpc


def parse_peers(raw):
    return [p.strip() for p in (raw or "").split(",") if p.strip()]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--peers", default="")
    args = parser.parse_args()

    data_dir = os.environ.get("DATA_DIR", "/data")
    os.makedirs(data_dir, exist_ok=True)
    storage = Storage(os.path.join(data_dir, f"{args.id}.db"))

    peers = parse_peers(args.peers)
    transport = GrpcTransport(peers)
    raft = RaftNode(args.id, peers, storage=storage, transport=transport)
    raft.start()

    print(f"starting {args.id} on {args.port}", flush=True)

    server = grpc.server(__import__("concurrent.futures", fromlist=["futures"]).ThreadPoolExecutor(max_workers=4))
    kv_pb2_grpc.add_KVServicer_to_server(NodeService(args.id, storage, raft), server)
    server.add_insecure_port(f"0.0.0.0:{args.port}")
    server.start()
    print("listening", flush=True)
    server.wait_for_termination()


if __name__ == "__main__":
    main()
"""