"""Node entrypoint."""

import argparse
import contextlib
import os
import signal
import threading
from concurrent import futures

import grpc

from node.logging import get_logger, log_event
from node.raft import RaftNode
from node.server import AuthInterceptor, GrpcTransport, NodeService
from node.storage import Storage


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

    os.environ.setdefault("NODE_ID", args.id)
    logger = get_logger(f"node.{args.id}")

    peers = parse_peers(args.peers)
    peer_ids = [p.split(":")[0] if ":" in p else p for p in peers]
    transport = GrpcTransport(peers)
    raft = RaftNode(args.id, peers, storage=storage, transport=transport, peer_ids=peer_ids)
    raft.start()

    log_event(logger, "starting node", node_id=args.id, port=args.port)

    import gen.kv_pb2_grpc as kv_pb2_grpc

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=16),
        interceptors=(AuthInterceptor(),),
    )
    kv_pb2_grpc.add_KVServicer_to_server(NodeService(args.id, storage, raft), server)

    server.add_insecure_port(f"0.0.0.0:{args.port}")
    server.start()
    log_event(logger, "listening", port=args.port)

    from node.repair import sync_on_start

    try:
        _node_index = int(str(args.id).replace("node", "")) - 1
    except Exception:
        _node_index = 0
    if _node_index < 0:
        _node_index = 0

    def _sync():
        with contextlib.suppress(Exception):
            sync_on_start(args.id, _node_index, storage, transport, log=logger.info)

    threading.Thread(target=_sync, daemon=True, name=f"sync-{args.id}").start()

    stop_event = threading.Event()

    from node.repair import RepairSweeper

    try:
        _sweep_interval = float(os.environ.get("SWEEP_INTERVAL", "10.0"))
    except Exception:
        _sweep_interval = 10.0
    sweeper = RepairSweeper(args.id, storage, transport, interval=_sweep_interval, stop_event=stop_event)
    sweeper_thread = threading.Thread(target=sweeper.run, daemon=True, name=f"sweep-{args.id}")
    sweeper_thread.start()

    def _shutdown(signum, frame):
        stop_event.set()

    try:
        signal.signal(signal.SIGINT, _shutdown)
        signal.signal(signal.SIGTERM, _shutdown)
    except ValueError:
        pass

    stop_event.wait()

    with contextlib.suppress(Exception):
        sweeper.stop()
    with contextlib.suppress(Exception):
        sweeper_thread.join(timeout=2.0)

    with contextlib.suppress(Exception):
        raft.stop()
    with contextlib.suppress(Exception):
        server.stop(grace=5)
    with contextlib.suppress(Exception):
        storage.close()
    with contextlib.suppress(Exception):
        transport.close()


if __name__ == "__main__":
    main()