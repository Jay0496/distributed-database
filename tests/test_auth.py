import os
import sys
import time

import grpc
import pytest

try:
    import gen.kv_pb2 as kv_pb2
    import gen.kv_pb2_grpc as kv_pb2_grpc
    from node.auth import AuthInterceptor
    from node.raft import RaftNode
    from node.server import NodeService
    from node.storage import Storage
except Exception:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "node"))
    from server import AuthInterceptor, NodeService  # type: ignore

    import gen.kv_pb2 as kv_pb2  # type: ignore
    import gen.kv_pb2_grpc as kv_pb2_grpc  # type: ignore
    from raft import RaftNode  # type: ignore
    from storage import Storage  # type: ignore

from concurrent import futures


def _start_node(tmp_path, token=None):
    if token is None:
        os.environ.pop("TOKEN", None)
    else:
        os.environ["TOKEN"] = token
    storage = Storage(str(tmp_path / "node.db"))
    raft = RaftNode("node0", [], storage=storage)
    raft.start()
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline and not raft.is_leader():
        time.sleep(0.05)

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=4),
        interceptors=(AuthInterceptor(),),
    )
    kv_pb2_grpc.add_KVServicer_to_server(NodeService("node0", storage, raft), server)
    port = server.add_insecure_port("0.0.0.0:0")
    server.start()
    return port, storage, raft, server


@pytest.fixture
def auth_server(tmp_path, monkeypatch):
    monkeypatch.setenv("TOKEN", "secret")
    port, storage, raft, server = _start_node(tmp_path, "secret")
    yield port, storage, raft, server
    raft.stop()
    server.stop(0)
    storage.close()


def test_missing_token_rejected(auth_server):
    port, storage, raft, server = auth_server
    channel = grpc.insecure_channel(f"localhost:{port}")
    stub = kv_pb2_grpc.KVStub(channel)
    with pytest.raises(grpc.RpcError) as exc:
        stub.Heartbeat(kv_pb2.HeartbeatRequest())
    assert exc.value.code() == grpc.StatusCode.UNAUTHENTICATED
    channel.close()


def test_wrong_token_rejected(auth_server):
    port, storage, raft, server = auth_server
    channel = grpc.insecure_channel(f"localhost:{port}")
    stub = kv_pb2_grpc.KVStub(channel)
    with pytest.raises(grpc.RpcError) as exc:
        stub.Heartbeat(
            kv_pb2.HeartbeatRequest(),
            metadata=[("authorization", "Bearer wrong")],
        )
    assert exc.value.code() == grpc.StatusCode.UNAUTHENTICATED
    channel.close()


def test_correct_token_accepted(auth_server):
    port, storage, raft, server = auth_server
    channel = grpc.insecure_channel(f"localhost:{port}")
    stub = kv_pb2_grpc.KVStub(channel)
    resp = stub.Heartbeat(
        kv_pb2.HeartbeatRequest(),
        metadata=[("authorization", "Bearer secret")],
    )
    assert bool(getattr(resp, "ok", False)) is True

    # HasShard works with correct token too.
    resp2 = stub.HasShard(
        kv_pb2.HasShardRequest(key="k", shard_index=0),
        metadata=[("authorization", "Bearer secret")],
    )
    assert resp2 is not None
    channel.close()


def test_no_auth_when_token_unset(tmp_path, monkeypatch):
    monkeypatch.delenv("TOKEN", raising=False)
    port, storage, raft, server = _start_node(tmp_path, None)
    channel = grpc.insecure_channel(f"localhost:{port}")
    stub = kv_pb2_grpc.KVStub(channel)
    # No header required.
    resp = stub.Heartbeat(kv_pb2.HeartbeatRequest())
    assert bool(getattr(resp, "ok", False)) is True
    channel.close()
    raft.stop()
    server.stop(0)
    storage.close()