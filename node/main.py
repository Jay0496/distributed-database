"""Node entrypoint (scaffold)."""

import argparse
from concurrent import futures

import grpc

from gen import kv_pb2_grpc


class KVService(kv_pb2_grpc.KVServicer):
    def Put(self, request, context):
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "not implemented")

    def Get(self, request, context):
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "not implemented")

    def Heartbeat(self, request, context):
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "not implemented")

    def StoreShard(self, request, context):
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "not implemented")

    def FetchShard(self, request, context):
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "not implemented")

    def HasShard(self, request, context):
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "not implemented")

    def ListKeys(self, request, context):
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "not implemented")

    def GetLeader(self, request, context):
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "not implemented")

    def AppendEntries(self, request, context):
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "not implemented")

    def RequestVote(self, request, context):
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "not implemented")

    def SetMode(self, request, context):
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "not implemented")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--peers", default="")
    args = parser.parse_args()

    print(f"starting {args.id} on {args.port}", flush=True)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    kv_pb2_grpc.add_KVServicer_to_server(KVService(), server)
    server.add_insecure_port(f"0.0.0.0:{args.port}")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    main()