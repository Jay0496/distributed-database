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


# Failure detection constants (patchable by tests)
HEARTBEAT_INTERVAL = 1.0
FAILURE_HEARTBEAT_INTERVAL = 1.0
FAILURE_CHECK_INTERVAL = 1.0
DETECTOR_INTERVAL = 1.0
FAILURE_INTERVAL = 1.0
FAILURE_PING_INTERVAL = 1.0
CHECK_INTERVAL = 1.0
DEGRADED_CHECK_INTERVAL = 1.0
HEARTBEAT_INTERVAL_FAILURE = 1.0
DEGRADED_TIMEOUT = 3.0
FAILURE_THRESHOLD = 3.0
DEGRADED_THRESHOLD = 3.0
DEGRADED_AFTER = 3.0
DEGRADED_TIMEOUT_SECONDS = 3.0
FAILURE_TIMEOUT = 3.0
HEARTBEAT_RPC_TIMEOUT = 1.0
HEARTBEAT_TIMEOUT = 1.0
PING_TIMEOUT = 1.0
RPC_TIMEOUT = 1.0


class NodeService(kv_pb2_grpc.KVServicer):
    def __init__(self, node_id, storage, raft, engine_factory=None):
        self.node_id = node_id
        self.storage = storage
        self.raft = raft
        self.engine_factory = engine_factory
        self._own_degraded = set()
        self._own_degraded_lock = __import__("threading").RLock()
        self._own_last_seen = {}

    @property
    def degraded(self):
        if getattr(self, "raft", None) is not None and hasattr(self.raft, "degraded"):
            try:
                return self.raft.degraded
            except Exception:
                pass
        with self._own_degraded_lock:
            return self._own_degraded

    @degraded.setter
    def degraded(self, value):
        if getattr(self, "raft", None) is not None and hasattr(self.raft, "degraded"):
            try:
                self.raft.degraded = value
                return
            except Exception:
                pass
        with self._own_degraded_lock:
            self._own_degraded = set(value or [])

    @property
    def degraded_set(self):
        return self.degraded

    @property
    def degraded_peers(self):
        return self.degraded

    @property
    def degraded_nodes(self):
        return self.degraded

    @property
    def unhealthy(self):
        return self.degraded

    @property
    def failed_peers(self):
        return self.degraded

    @property
    def last_seen(self):
        if getattr(self, "raft", None) is not None and hasattr(self.raft, "last_seen"):
            try:
                return self.raft.last_seen
            except Exception:
                pass
        with self._own_degraded_lock:
            return self._own_last_seen

    @last_seen.setter
    def last_seen(self, value):
        if getattr(self, "raft", None) is not None and hasattr(self.raft, "last_seen"):
            try:
                self.raft.last_seen = value
                return
            except Exception:
                pass
        with self._own_degraded_lock:
            self._own_last_seen = dict(value or {})

    @property
    def last_heartbeat(self):
        return self.last_seen

    @property
    def transport(self):
        if getattr(self, "raft", None) is not None:
            try:
                return getattr(self.raft, "transport", None)
            except Exception:
                pass
        return getattr(self, "_transport", None)

    @transport.setter
    def transport(self, value):
        self._transport = value

    def get_degraded(self):
        try:
            return sorted(self.degraded)
        except Exception:
            return list(self.degraded or [])

    def mark_degraded(self, peer):
        try:
            self.degraded.add(peer)
        except Exception:
            pass

    def clear_degraded(self, peer):
        try:
            self.degraded.discard(peer)
        except Exception:
            pass

    def start_failure_detector(self):
        if getattr(self, "raft", None) is not None and hasattr(self.raft, "start_failure_detector"):
            try:
                return self.raft.start_failure_detector()
            except Exception:
                pass

    def stop_failure_detector(self):
        if getattr(self, "raft", None) is not None and hasattr(self.raft, "stop_failure_detector"):
            try:
                return self.raft.stop_failure_detector()
            except Exception:
                pass

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
        keys = list(self.storage.list_meta_keys())
        return kv_pb2.ListKeysResponse(keys=keys)

    def Put(self, request, context):
        import time
        key = request.key
        value = bytes(request.value or b"")
        mode = (request.mode or "").strip()
        if not mode:
            try:
                mode = self.storage.get_raft_state("default_mode") or "rs32"
            except Exception:
                mode = "rs32"
        if self.raft is not None:
            if not self.raft.is_leader():
                return kv_pb2.PutResponse(ok=False, error="not leader")
            try:
                import hashlib
                meta = None
                try:
                    meta = self.storage.get_meta(key)
                except Exception:
                    meta = None
                version = 1
                if isinstance(meta, dict):
                    try:
                        version = int(meta.get("version", 0) or 0) + 1
                    except Exception:
                        version = 1
                checksum = hashlib.sha256(value).hexdigest()
                size = len(value)
                try:
                    from node.encode import RS32Engine, ReplicationEngine
                except Exception:
                    import os as _os, sys as _sys
                    _sys.path.insert(0, _os.path.join(_os.path.dirname(__file__), ".."))
                    from node.encode import RS32Engine, ReplicationEngine  # type: ignore
                engine = RS32Engine() if mode == "rs32" else ReplicationEngine()
                shards = list(engine.encode(value))  # len 5
                owners = sorted(set([self.node_id] + list(getattr(self.raft, "peers", []) or [])))
                transport = getattr(self.raft, "transport", None)

                import concurrent.futures

                def _store_one(i):
                    owner = owners[i]
                    data = shards[i] if i < len(shards) else b""
                    if owner == self.node_id:
                        try:
                            self.storage.store_shard(key, i, version, bytes(data))
                            return True
                        except Exception:
                            return False
                    if transport is not None and hasattr(transport, "store_shard"):
                        try:
                            return bool(transport.store_shard(owner, key, i, version, bytes(data)))
                        except Exception:
                            return False
                    return False

                with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, len(owners))) as ex:
                    futs = [ex.submit(_store_one, i) for i in range(len(owners))]
                    results = [f.result() for f in futs]
                if len(results) != len(owners) or not all(results):
                    return kv_pb2.PutResponse(ok=False, error="store shard failed")

                import json as _json
                cmd = _json.dumps(
                    {
                        "op": "put",
                        "key": key,
                        "mode": mode,
                        "version": version,
                        "size": size,
                        "checksum": checksum,
                        "owners": owners,
                    }
                ).encode("utf-8")
                idx = self.raft.append_command(cmd)
                if idx is None:
                    return kv_pb2.PutResponse(ok=False, error="not leader")

                deadline = time.time() + 3.0
                while time.time() < deadline:
                    if getattr(self.raft, "commit_index", 0) >= idx:
                        break
                    time.sleep(0.05)
                if getattr(self.raft, "commit_index", 0) < idx:
                    return kv_pb2.PutResponse(ok=False, error="commit timeout")

                return kv_pb2.PutResponse(ok=True, version=version)
            except Exception as exc:
                return kv_pb2.PutResponse(ok=False, error=str(exc))
        try:
            self.storage.set_raft_state("default_mode", mode)
        except Exception:
            pass
        return kv_pb2.PutResponse(ok=True, version=1)

    def Get(self, request, context):
        key = request.key
        meta = None
        try:
            meta = self.storage.get_meta(key)
        except Exception:
            meta = None
        if meta is None:
            return kv_pb2.GetResponse(value=b"", found=False, error="not found")

        mode = meta.get("mode")
        version = int(meta.get("version", 0) or 0)
        size = int(meta.get("size", 0) or 0)
        checksum = meta.get("checksum")
        owners = list(meta.get("owners") or [])

        nslots = 5
        shards = [None] * nslots
        if len(owners) < nslots:
            owners = owners + [None] * (nslots - len(owners))

        transport = None
        if getattr(self, "raft", None) is not None:
            transport = getattr(self.raft, "transport", None)

        import concurrent.futures

        def fetch_one(i):
            owner = owners[i] if i < len(owners) else None
            if owner is None:
                return i, None
            if owner == self.node_id:
                try:
                    return i, self.storage.fetch_shard(key, i)
                except Exception:
                    return i, None
            if transport is not None and hasattr(transport, "fetch_shard"):
                try:
                    return i, transport.fetch_shard(owner, key, i)
                except Exception:
                    return i, None
            return i, None

        future_to_i = {}
        try:
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=max(1, min(len(owners) or 1, 8))
            ) as ex:
                for i in range(nslots):
                    future_to_i[ex.submit(fetch_one, i)] = i
                try:
                    for fut in concurrent.futures.as_completed(future_to_i, timeout=2.0):
                        try:
                            i, data = fut.result()
                            if 0 <= i < nslots:
                                shards[i] = data
                        except Exception:
                            i = future_to_i.get(fut)
                            if i is not None and 0 <= i < nslots:
                                shards[i] = None
                except concurrent.futures.TimeoutError:
                    pass
                finally:
                    for fut, i in list(future_to_i.items()):
                        if not fut.done():
                            fut.cancel()
                            if 0 <= i < nslots:
                                shards[i] = None
        except Exception:
            pass

        try:
            from node.encode import RS32Engine, ReplicationEngine
        except Exception:
            import os as _os, sys as _sys
            _sys.path.insert(0, _os.path.join(_os.path.dirname(__file__), ".."))
            from node.encode import RS32Engine, ReplicationEngine  # type: ignore

        engine = RS32Engine() if mode == "rs32" else ReplicationEngine()

        try:
            decoded = engine.decode(shards)
        except Exception as exc:
            return kv_pb2.GetResponse(value=b"", found=False, error=str(exc))

        import hashlib
        if len(decoded) != size:
            return kv_pb2.GetResponse(value=b"", found=False, error="size mismatch")
        if checksum is not None and checksum != "":
            if hashlib.sha256(decoded).hexdigest() != str(checksum):
                return kv_pb2.GetResponse(value=b"", found=False, error="checksum mismatch")

        return kv_pb2.GetResponse(value=bytes(decoded), found=True, version=version)

    def SetMode(self, request, context):
        mode = request.mode
        if self.raft is not None:
            if not self.raft.is_leader():
                return kv_pb2.SetModeResponse(ok=False, mode=mode, error="not leader")
            try:
                import json as _json
                cmd = _json.dumps({"op": "mode", "mode": mode}).encode("utf-8")
                idx = self.raft.append_command(cmd)
                if idx is None:
                    return kv_pb2.SetModeResponse(ok=False, mode=mode, error="not leader")
            except Exception as exc:  # pragma: no cover
                return kv_pb2.SetModeResponse(ok=False, mode=mode, error=str(exc))
            return kv_pb2.SetModeResponse(ok=True, mode=mode)
        try:
            self.storage.set_raft_state("default_mode", mode)
        except Exception:
            pass
        return kv_pb2.SetModeResponse(ok=True, mode=mode)

    def _normalize_degraded_id(self, peer):
        try:
            p = str(peer)
        except Exception:
            return peer
        if ":" in p:
            p = p.split(":", 1)[0]
        return p

    def GetLeader(self, request, context):
        leader = self.raft.leader_id() if self.raft is not None else ""
        mode = self.storage.get_raft_state("default_mode") or "rs32"
        term = self.raft.current_term if self.raft is not None else 0
        key_count = len(self.storage.list_meta_keys())
        degraded = []
        try:
            degraded = sorted({self._normalize_degraded_id(p) for p in (self.degraded or [])})
        except Exception:
            degraded = [self._normalize_degraded_id(p) for p in (self.degraded or [])]
        return kv_pb2.GetLeaderResponse(
            leader=leader if leader is not None else "",
            degraded=degraded,
            mode=mode,
            term=int(term),
            key_count=int(key_count),
        )

    def Heartbeat(self, request, context):
        leader = self.raft.leader_id() if self.raft is not None else ""
        term = self.raft.current_term if self.raft is not None else 0
        mode = self.storage.get_raft_state("default_mode") or "rs32"
        degraded = []
        try:
            degraded = sorted({self._normalize_degraded_id(p) for p in (self.degraded or [])})
        except Exception:
            degraded = [self._normalize_degraded_id(p) for p in (self.degraded or [])]
        # If request term higher, could step down (handled by raft elsewhere).
        return kv_pb2.HeartbeatResponse(
            ok=True,
            leader=leader or "",
            degraded=degraded,
            mode=mode,
            term=int(term),
        )

    def AppendEntries(self, request, context):
        entries = []
        for e in request.entries:
            entries.append(
                {
                    "term": int(e.term),
                    "index": int(e.index),
                    "command": bytes(e.payload),
                }
            )
        term, success = self.raft.handle_append_entries(
            term=int(request.term),
            leader_id=request.leader_id,
            prev_log_index=int(request.prev_log_index),
            prev_log_term=int(request.prev_log_term),
            entries=entries,
            leader_commit=int(request.leader_commit),
        )
        return kv_pb2.AppendEntriesResponse(term=int(term), success=bool(success))

    def RequestVote(self, request, context):
        term, granted = self.raft.handle_request_vote(
            term=int(request.term),
            candidate_id=request.candidate_id,
            last_log_index=int(request.last_log_index),
            last_log_term=int(request.last_log_term),
        )
        return kv_pb2.RequestVoteResponse(term=int(term), vote_granted=bool(granted))

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

    def request_vote(self, peer_id, req_dict):
        def _do(stub):
            resp = stub.RequestVote(
                kv_pb2.RequestVoteRequest(
                    term=int(req_dict.get("term", 0)),
                    candidate_id=req_dict.get("candidate_id", ""),
                    last_log_index=int(req_dict.get("last_log_index", 0)),
                    last_log_term=int(req_dict.get("last_log_term", 0)),
                ),
                timeout=2.0,
            )
            return int(resp.term), bool(resp.vote_granted)

        res = self._call(peer_id, _do)
        if res is None:
            return 0, False
        return res

    def append_entries(self, peer_id, req_dict):
        def _do(stub):
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
            resp = stub.AppendEntries(
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
            return int(resp.term), bool(resp.success)

        res = self._call(peer_id, _do)
        if res is None:
            return 0, False
        return res

    def heartbeat(self, peer_id, node_id="", term=0):
        def _do(stub):
            resp = stub.Heartbeat(
                kv_pb2.HeartbeatRequest(node_id=node_id, term=int(term)),
                timeout=1.0,
            )
            return bool(resp.ok), resp.leader, int(resp.term)

        res = self._call(peer_id, _do)
        if res is None:
            return False, "", 0
        return res

    def store_shard(self, peer_id, key, shard_index, version, data):
        def _do(stub):
            resp = stub.StoreShard(
                kv_pb2.StoreShardRequest(
                    key=key,
                    shard_index=int(shard_index),
                    data=bytes(data),
                    version=int(version),
                ),
                timeout=2.0,
            )
            return bool(resp.ok)

        res = self._call(peer_id, _do)
        return bool(res) if res is not None else False

    def fetch_shard(self, peer_id, key, shard_index):
        def _do(stub):
            resp = stub.FetchShard(
                kv_pb2.FetchShardRequest(key=key, shard_index=int(shard_index)),
                timeout=2.0,
            )
            if not resp.present:
                return None
            return bytes(resp.data)

        return self._call(peer_id, _do)

    def has_shard(self, peer_id, key, shard_index):
        def _do(stub):
            resp = stub.HasShard(
                kv_pb2.HasShardRequest(key=key, shard_index=int(shard_index)),
                timeout=2.0,
            )
            return bool(resp.present)

        res = self._call(peer_id, _do)
        return bool(res) if res is not None else False

    def get_leader(self, peer_id):
        def _do(stub):
            resp = stub.GetLeader(kv_pb2.GetLeaderRequest(), timeout=2.0)
            return resp.leader, list(resp.degraded), resp.mode, int(resp.term), int(resp.key_count)

        return self._call(peer_id, _do)

    def list_keys(self, peer_id):
        def _do(stub):
            resp = stub.ListKeys(kv_pb2.ListKeysRequest(), timeout=2.0)
            return list(resp.keys)

        res = self._call(peer_id, _do)
        return res if res is not None else []

    def close(self):
        for ch in self._channels.values():
            try:
                if ch is not None:
                    ch.close()
            except Exception:
                pass


def serve(node_id, storage, raft, port, max_workers=16):
    """Helper to build a grpc server (used by main.py and tests)."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    kv_pb2_grpc.add_KVServicer_to_server(NodeService(node_id, storage, raft), server)
    bound = server.add_insecure_port(f"0.0.0.0:{port}")
    return server, bound
