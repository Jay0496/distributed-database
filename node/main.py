"""Node entrypoint."""

import argparse
import os
import signal
import sys
import threading
from concurrent import futures

try:
    import grpc
except Exception:  # pragma: no cover
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    import grpc

# ensure repo root (for gen/) is importable
_REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

try:
    from node.storage import Storage
    from node.raft import RaftNode
    from node.server import NodeService, GrpcTransport
except Exception:  # pragma: no cover - running from node/ directly
    sys.path.insert(0, os.path.dirname(__file__))
    from storage import Storage  # type: ignore
    from raft import RaftNode  # type: ignore
    from server import NodeService, GrpcTransport  # type: ignore


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
    storage_path = os.path.join(data_dir, f"{args.id}.db")
    storage = Storage(storage_path)

    peers = parse_peers(args.peers)
    peer_ids = [p.split(":")[0] if ":" in p else p for p in peers]
    transport = GrpcTransport(peers)
    raft = RaftNode(args.id, peers, storage=storage, transport=transport, peer_ids=peer_ids)
    raft.start()

    print(f"starting {args.id} on {args.port}", flush=True)

    import gen.kv_pb2_grpc as kv_pb2_grpc

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    kv_pb2_grpc.add_KVServicer_to_server(NodeService(args.id, storage, raft), server)

    server.add_insecure_port(f"0.0.0.0:{args.port}")
    server.start()
    print("listening", flush=True)

    stop_event = threading.Event()

    def _shutdown(signum, frame):
        stop_event.set()

    try:
        signal.signal(signal.SIGINT, _shutdown)
        signal.signal(signal.SIGTERM, _shutdown)
    except ValueError:
        pass

    stop_event.wait()

    try:
        raft.stop()
    except Exception:
        pass
    try:
        server.stop(grace=5)
    except Exception:
        pass
    try:
        storage.close()
    except Exception:
        pass
    try:
        transport.close()
    except Exception:
        pass


if __name__ == "__main__":
    main()