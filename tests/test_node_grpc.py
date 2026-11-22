import os
import sys
import time

import grpc
import pytest

try:
    from node.server import NodeService
    from node.storage import Storage
    from node.raft import RaftNode
    import gen.kv_pb2 as kv_pb2
    import gen.kv_pb2_grpc as kv_pb2_grpc
except Exception:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "node"))
    from server import NodeService  # type: ignore
    from storage import Storage  # type: ignore
    from raft import RaftNode  # type: ignore
    import gen.kv_pb2 as kv_pb2  # type: ignore
    import gen.kv_pb2_grpc as kv_pb2_grpc  # type: ignore

from concurrent import futures


@pytest.fixture
def grpc_server(tmp_path):
    storage = Storage(str(tmp_path / "node.db"))
    raft = RaftNode("node0", [], storage=storage)
    raft.start()
    # allow leader election
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline and not raft.is_leader():
        time.sleep(0.05)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    kv_pb2_grpc.add_KVServicer_to_server(NodeService("node0", storage, raft), server)
    port = server.add_insecure_port("0.0.0.0:0")
    server.start()
    yield port, storage, raft
    raft.stop()
    server.stop(0)
    storage.close()


def test_store_fetch_has_roundtrip(grpc_server):
    port, storage, raft = grpc_server
    channel = grpc.insecure_channel(f"localhost:{port}")
    stub = kv_pb2_grpc.KVStub(channel)

    data = b"hello world \x00\xff"
    resp = stub.StoreShard(kv_pb2.StoreShardRequest(key="k", shard_index=0, data=data, version=1))
    assert resp.ok is True

    fetched = stub.FetchShard(kv_pb2.FetchShardRequest(key="k", shard_index=0))
    assert fetched.present is True
    assert fetched.data == data

    assert stub.HasShard(kv_pb2.HasShardRequest(key="k", shard_index=0)).present is True
    assert stub.HasShard(kv_pb2.HasShardRequest(key="k", shard_index=1)).present is False

    missing = stub.FetchShard(kv_pb2.FetchShardRequest(key="k", shard_index=99))
    assert missing.present is False
    assert missing.data == b""

    channel.close()