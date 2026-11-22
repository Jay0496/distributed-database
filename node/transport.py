"""gRPC transport for peer nodes."""

import os

import grpc

from gen import kv_pb2, kv_pb2_grpc


class GrpcTransport:
    """gRPC-based transport to peer nodes. Tolerates peer failures."""

    def __init__(self, peers):
        if isinstance(peers, str):
            peers = [p.strip() for p in peers.split(",") if p.strip()]
        self.peers = list(peers or [])
        self._channels = {}
        self._stubs = {}
        for addr in self.peers:
            try:
                ch = grpc.insecure_channel(addr)
                self._channels[addr] = ch
                self._stubs[addr] = kv_pb2_grpc.KVStub(ch)
            except Exception:
                self._channels[addr] = None
                self._stubs[addr] = None

    def _token(self):
        t = getattr(self, "token", None)
        if t is None:
            t = os.environ.get("TOKEN", "")
        return t or ""

    def _auth_metadata(self):
        token = self._token()
        if not token:
            return None
        return [("authorization", f"Bearer {token}")]

    def _stub(self, peer_id):
        if peer_id in self._stubs:
            return self._stubs.get(peer_id)
        # allow passing host:port directly
        if peer_id not in self._channels:
            try:
                ch = grpc.insecure_channel(peer_id)
                self._channels[peer_id] = ch
                self._stubs[peer_id] = kv_pb2_grpc.KVStub(ch)
            except Exception:
                self._channels[peer_id] = None
                self._stubs[peer_id] = None
        return self._stubs.get(peer_id)

    def _call(self, peer_id, fn):
        stub = self._stub(peer_id)
        if stub is None:
            return None
        try:
            return fn(stub)
        except Exception:
            return None

    def _rpc(self, peer_id, method, request, *, timeout=2.0, metadata=None):
        stub = self._stub(peer_id)
        if stub is None:
            return None
        fn = getattr(stub, method, None)
        if fn is None:
            return None
        kwargs = {"timeout": timeout}
        if metadata is not None:
            kwargs["metadata"] = metadata
        else:
            md = self._auth_metadata()
            if md is not None:
                kwargs["metadata"] = md
        try:
            return fn(request, **kwargs)
        except Exception:
            return None

    def request_vote(self, peer_id, req_dict):
        res = self._rpc(
            peer_id,
            "RequestVote",
            kv_pb2.RequestVoteRequest(
                term=int(req_dict.get("term", 0)),
                candidate_id=req_dict.get("candidate_id", ""),
                last_log_index=int(req_dict.get("last_log_index", 0)),
                last_log_term=int(req_dict.get("last_log_term", 0)),
            ),
            timeout=2.0,
        )
        if res is None:
            return 0, False
        return int(res.term), bool(res.vote_granted)

    def append_entries(self, peer_id, req_dict):
        entries = []
        for e in req_dict.get("entries", []) or []:
            term = getattr(e, "term", 0)
            idx = getattr(e, "index", 0)
            cmd = getattr(e, "command", None)
            if cmd is None and isinstance(e, dict):
                term = e.get("term", term)
                idx = e.get("index", idx)
                cmd = e.get("command", e.get("payload", b""))
            if isinstance(cmd, str):
                cmd = cmd.encode()
            elif not isinstance(cmd, (bytes, bytearray)):
                try:
                    cmd = bytes(cmd) if cmd is not None else b""
                except Exception:
                    cmd = b""
            entries.append(
                kv_pb2.RaftLogEntry(
                    term=int(term),
                    index=int(idx) if idx is not None else 0,
                    payload=bytes(cmd) if cmd is not None else b"",
                )
            )
        res = self._rpc(
            peer_id,
            "AppendEntries",
            kv_pb2.AppendEntriesRequest(
                term=int(req_dict.get("term", 0)),
                leader_id=req_dict.get("leader_id", ""),
                prev_log_index=int(req_dict.get("prev_log_index", 0)),
                prev_log_term=int(req_dict.get("prev_log_term", 0)),
                entries=entries,
                leader_commit=int(req_dict.get("leader_commit", 0)),
            ),
            timeout=2.0,
        )
        if res is None:
            return 0, False
        return int(res.term), bool(res.success)

    def heartbeat(self, peer_id, node_id="", term=0):
        res = self._rpc(
            peer_id,
            "Heartbeat",
            kv_pb2.HeartbeatRequest(node_id=node_id, term=int(term)),
            timeout=1.0,
        )
        if res is None:
            return False, "", 0
        return bool(res.ok), res.leader, int(res.term)

    def store_shard(self, peer_id, key, shard_index, version, data):
        res = self._rpc(
            peer_id,
            "StoreShard",
            kv_pb2.StoreShardRequest(
                key=key,
                shard_index=int(shard_index),
                data=bytes(data),
                version=int(version),
            ),
            timeout=2.0,
        )
        if res is None:
            return False
        return bool(res.ok)

    def fetch_shard(self, peer_id, key, shard_index):
        res = self._rpc(
            peer_id,
            "FetchShard",
            kv_pb2.FetchShardRequest(key=key, shard_index=int(shard_index)),
            timeout=2.0,
        )
        if res is None:
            return None
        if not getattr(res, "present", False):
            return None
        return bytes(res.data)

    def has_shard(self, peer_id, key, shard_index):
        res = self._rpc(
            peer_id,
            "HasShard",
            kv_pb2.HasShardRequest(key=key, shard_index=int(shard_index)),
            timeout=2.0,
        )
        if res is None:
            return False
        return bool(res.present)

    def get_leader(self, peer_id):
        res = self._rpc(
            peer_id,
            "GetLeader",
            kv_pb2.GetLeaderRequest(),
            timeout=2.0,
        )
        if res is None:
            return None
        return res.leader, list(res.degraded), res.mode, int(res.term), int(res.key_count)

    def list_keys(self, peer_id):
        res = self._rpc(
            peer_id,
            "ListKeys",
            kv_pb2.ListKeysRequest(),
            timeout=2.0,
        )
        if res is None:
            return []
        return list(res.keys)

    def close(self):
        for ch in self._channels.values():
            try:
                if ch is not None:
                    ch.close()
            except Exception:
                pass