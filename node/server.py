"""gRPC KV service implementation for a distributed database node."""

import os
import sys

try:
    import grpc
    from gen import kv_pb2, kv_pb2_grpc
except Exception:  # pragma: no cover
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    import grpc
    from gen import kv_pb2, kv_pb2_grpc


class NodeService(kv_pb2_grpc.KVServicer):
    def __init__(self, node_id, storage, raft, engine_factory=None):
        self.node_id = node_id
        self.storage = storage
        self.raft = raft
        self.engine_factory = engine_factory

    def StoreShard(self, request, context):
        self.storage.store_shard(
            request.key, int(request.shard_index), int(request.version), bytes(request.data)
        )
        return kv_pb2.StoreShardResponse(ok=True)

    def FetchShard(self, request, context):
        data = self.storage.fetch_shard(request.key, int(request.shard_index))
        present = data is not None
        if not present:
            data = b""
        return kv_pb2.FetchShardResponse(data=bytes(data), present=bool(present))

    def HasShard(self, request, context):
        present = bool(self.storage.has_shard(request.key, int(request.shard_index)))
        return kv_pb2.HasShardResponse(present=present)

    def ListKeys(self, request, context):
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "not implemented")

    def GetLeader(self, request, context):
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "not implemented")

    def Heartbeat(self, request, context):
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "not implemented")

    def AppendEntries(self, request, context):
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "not implemented")

    def RequestVote(self, request, context):
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "not implemented")