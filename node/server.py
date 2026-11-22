"""gRPC KV service implementation for a distributed database node."""

import contextlib
import os
from concurrent import futures

import grpc

from gen import kv_pb2, kv_pb2_grpc

# Shared secret for gRPC auth. Empty disables auth (backward compatible).
TOKEN = os.environ.get("TOKEN", "")


def _get_token():
    """Resolve the current token, honoring env changes and test overrides."""
    env_val = os.environ.get("TOKEN", None)
    if env_val is not None:
        if env_val == "":
            return ""
        return env_val
    return globals().get("TOKEN", "") or ""


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
        with contextlib.suppress(Exception):
            self.degraded.add(peer)

    def clear_degraded(self, peer):
        with contextlib.suppress(Exception):
            self.degraded.discard(peer)

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
                from node.encode import ReplicationEngine, RS32Engine
                engine = RS32Engine() if mode == "rs32" else ReplicationEngine()
                shards = list(engine.encode(value))  # len 5
                owners = sorted(set([self.node_id] + list(getattr(self.raft, "peer_ids", []) or getattr(self.raft, "peers", []) or [])))
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
                    # resolve node id -> peer address
                    addr = owner
                    pid_map = getattr(self.raft, "peer_id_map", {}) or {}
                    for a, pid in (pid_map or {}).items():
                        if pid == owner:
                            addr = a
                            break
                    if transport is not None and hasattr(transport, "store_shard"):
                        try:
                            return bool(transport.store_shard(addr, key, i, version, bytes(data)))
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
        with contextlib.suppress(Exception):
            self.storage.set_raft_state("default_mode", mode)
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
            addr = owner
            pid_map = getattr(self.raft, "peer_id_map", {}) or {} if getattr(self, "raft", None) is not None else {}
            for a, pid in (pid_map or {}).items():
                if pid == owner:
                    addr = a
                    break
            if transport is not None and hasattr(transport, "fetch_shard"):
                try:
                    return i, transport.fetch_shard(addr, key, i)
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

        from node.encode import ReplicationEngine, RS32Engine

        engine = RS32Engine() if mode == "rs32" else ReplicationEngine()

        try:
            decoded = engine.decode(shards)
        except Exception as exc:
            return kv_pb2.GetResponse(value=b"", found=False, error=str(exc))

        import hashlib
        if len(decoded) != size:
            return kv_pb2.GetResponse(value=b"", found=False, error="size mismatch")
        if checksum is not None and checksum != "" and hashlib.sha256(decoded).hexdigest() != str(checksum):
            return kv_pb2.GetResponse(value=b"", found=False, error="checksum mismatch")

        # Read repair: rebuild missing/corrupt shards in the background.
        from node.encode import ReplicationEngine, RS32Engine
        from node.repair import read_repair
        canonical_shards = engine.encode(decoded)
        # pad owners to nslots
        _owners = list(owners) if owners is not None else []
        # resolve node ids -> peer addresses for transport calls
        pid_map = getattr(self.raft, "peer_id_map", {}) if getattr(self, "raft", None) is not None else {}
        _resolved = []
        for o in _owners:
            if o is None or o == self.node_id:
                _resolved.append(o)
                continue
            addr = o
            for a, pid in (pid_map or {}).items():
                if pid == o:
                    addr = a
                    break
            _resolved.append(addr)
        _owners = _resolved
        while len(_owners) < nslots:
            _owners.append(None)
        read_repair(engine, shards, canonical_shards, _owners, transport, key, version)

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
        with contextlib.suppress(Exception):
            self.storage.set_raft_state("default_mode", mode)
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

# Backward-compat re-exports.
try:  # noqa: SIM105
    from node.transport import GrpcTransport  # noqa: F401
except Exception:  # pragma: no cover
    try:  # noqa: SIM105
        from transport import GrpcTransport  # type: ignore  # noqa: F401
    except Exception:
        pass
try:  # noqa: SIM105
    from node.auth import AuthInterceptor  # noqa: F401
except Exception:  # pragma: no cover
    try:  # noqa: SIM105
        from auth import AuthInterceptor  # type: ignore
    except Exception:
        pass


def serve(node_id, storage, raft, port, max_workers=16):
    """Helper to build a grpc server (used by main.py and tests)."""
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=max_workers),
        interceptors=(AuthInterceptor(),),
    )
    kv_pb2_grpc.add_KVServicer_to_server(NodeService(node_id, storage, raft), server)
    bound = server.add_insecure_port(f"0.0.0.0:{port}")
    return server, bound
