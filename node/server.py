"""gRPC KV service implementation for a distributed database node."""

from concurrent import futures

import grpc

from gen import kv_pb2_grpc
from node.auth import AuthInterceptor  # noqa: F401
from node.service import NodeService  # noqa: F401
from node.transport import GrpcTransport  # noqa: F401


def serve(node_id, storage, raft, port, max_workers=16):
    """Helper to build a grpc server (used by main.py and tests)."""
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=max_workers),
        interceptors=(AuthInterceptor(),),
    )
    kv_pb2_grpc.add_KVServicer_to_server(NodeService(node_id, storage, raft), server)
    bound = server.add_insecure_port(f"0.0.0.0:{port}")
    return server, bound
