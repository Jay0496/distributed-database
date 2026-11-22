"""Read repair helper for the distributed KV store.

After a successful read, compare each fetched shard against the canonical
shards reconstructed from the decoded value. Missing or corrupt shards are
rebuilt by scheduling a best-effort background ``StoreShard`` to their owner.
"""

import threading
import time
from typing import Any, Callable, List, Optional, Sequence

try:
    from node.encode import RS32Engine, ReplicationEngine
except Exception:  # pragma: no cover - alternate import paths
    try:
        from encode import RS32Engine, ReplicationEngine  # type: ignore
    except Exception:
        try:
            import os as _os, sys as _sys
            _sys.path.insert(0, _os.path.join(_os.path.dirname(__file__), ".."))
            from node.encode import RS32Engine, ReplicationEngine  # type: ignore
        except Exception:
            RS32Engine = None  # type: ignore
            ReplicationEngine = None  # type: ignore


def read_repair(engine, shards, canonical_shards, owners, transport, key, version, log=print) -> List[int]:
    shards = list(shards) if shards is not None else []
    canonical_shards = list(canonical_shards) if canonical_shards is not None else []
    n = max(len(shards), len(canonical_shards), 5)
    while len(shards) < n:
        shards.append(None)
    while len(canonical_shards) < n:
        canonical_shards.append(None)
    if owners is None:
        owners = [None] * n
    else:
        owners = list(owners)
        while len(owners) < n:
            owners.append(None)

    repaired: List[int] = []
    for i in range(n):
        if i >= len(canonical_shards) or canonical_shards[i] is None:
            continue
        if shards[i] != canonical_shards[i]:
            repaired.append(i)
            canonical = bytes(canonical_shards[i])

            def _do(idx=i, owner=owners[i] if i < len(owners) else None, data=canonical):
                try:
                    if owner is None or transport is None:
                        return
                    if hasattr(transport, "store_shard"):
                        transport.store_shard(owner, key, idx, version, data)
                    elif hasattr(transport, "StoreShard"):
                        transport.StoreShard(owner, key, idx, version, data)
                except Exception:
                    pass

            try:
                log("read-repair: rebuilt shard %d" % i)
            except Exception:
                pass
            try:
                t = threading.Thread(target=_do, daemon=True)
                t.start()
            except Exception:
                try:
                    _do()
                except Exception:
                    pass

    return repaired


class RepairSweeper:
    """Background sweeper that refills missing shards across all keys."""

    def __init__(self, node_id, storage, transport, interval=10.0, stop_event=None, log=None, **kwargs):
        self.node_id = node_id
        self.storage = storage
        self.transport = transport
        self.interval = interval
        if stop_event is None:
            stop_event = threading.Event()
        self.stop_event = stop_event
        self.log = log if log is not None else print
        for k, v in (kwargs or {}).items():
            try:
                setattr(self, k, v)
            except Exception:
                pass

    def run(self):
        while not self.stop_event.is_set():
            if self.interval and self.interval > 0:
                if self.stop_event.wait(self.interval):
                    break
            else:
                time.sleep(0)
            if self.stop_event.is_set():
                break
            try:
                self.sweep_once()
            except Exception:
                pass

    def sweep_once(self):
        try:
            keys = list(self.storage.list_meta_keys())
        except Exception:
            keys = []
        for key in keys:
            try:
                meta = self.storage.get_meta(key)
            except Exception:
                meta = None
            if not meta:
                continue
            if isinstance(meta, dict):
                mode = meta.get("mode")
                version = meta.get("version")
                owners = list(meta.get("owners") or [])
            else:
                mode = meta["mode"]
                version = meta["version"]
                owners = list(meta["owners"] or [])
            if not owners:
                continue
            try:
                shards = [
                    self.transport.fetch_shard(owner, key, idx)
                    for idx, owner in enumerate(owners)
                ]
            except Exception:
                continue
            if not shards:
                continue
            if all(s is not None for s in shards):
                continue
            engine_cls = RS32Engine if mode == "rs32" else ReplicationEngine
            if engine_cls is None:
                continue
            engine = engine_cls()
            try:
                decoded = engine.decode(shards)
            except Exception:
                continue
            try:
                canonical = engine.encode(decoded)
            except Exception:
                continue
            for i, owner in enumerate(owners):
                if i >= len(canonical):
                    continue
                if shards[i] is None or shards[i] != canonical[i]:
                    try:
                        self.transport.store_shard(owner, key, i, version, canonical[i])
                    except Exception:
                        pass
                    try:
                        self.log("sweep: refilled shard %d on %s" % (i, owner))
                    except Exception:
                        pass

    def stop(self):
        try:
            self.stop_event.set()
        except Exception:
            pass


def sync_on_start(node_id, node_index, storage, transport, log=print):
    """Pull shards this node should own immediately after Raft recovery.

    Returns the number of shards pulled.
    """
    count = 0
    try:
        node_index = int(node_index)
    except Exception:
        try:
            node_index = int(str(node_id).replace("node", "")) - 1
        except Exception:
            node_index = 0
    if node_index < 0:
        node_index = 0

    peers = []
    try:
        peers = list(getattr(transport, "peers", []) or [])
    except Exception:
        peers = []

    keys = set()
    for peer in peers:
        try:
            keys.update(transport.list_keys(peer) or [])
        except Exception:
            try:
                keys.update(transport.ListKeys(peer) or [])
            except Exception:
                pass

    for key in sorted(keys):
        # Determine whether this node owns a shard for this key and which index.
        idx = node_index
        version = 0
        is_owner = True
        mode = None
        owners = []
        try:
            meta = storage.get_meta(key)
        except Exception:
            meta = None
        if isinstance(meta, dict):
            try:
                version = int(meta.get("version", 0) or 0)
            except Exception:
                version = 0
            try:
                mode = meta.get("mode")
            except Exception:
                mode = None
            owners = list(meta.get("owners") or [])
            if owners:
                if node_id in owners:
                    idx = owners.index(node_id)
                else:
                    is_owner = False
        elif meta is not None:
            try:
                version = int(meta["version"])
            except Exception:
                version = 0
            try:
                mode = meta["mode"]
            except Exception:
                mode = None
            try:
                owners = list(meta["owners"] or [])
            except Exception:
                owners = []
            if owners:
                if node_id in owners:
                    idx = owners.index(node_id)
                else:
                    is_owner = False

        if not is_owner:
            continue

        try:
            if storage.has_shard(key, idx):
                continue
        except Exception:
            pass

        pulled = False
        for peer in peers:
            try:
                data = transport.fetch_shard(peer, key, idx)
            except Exception:
                data = None
            if data is None:
                # Some transports signal presence with empty bytes? Treat None as missing.
                pass
            if data is None:
                try:
                    res = transport.FetchShard(peer, key, idx) if hasattr(transport, "FetchShard") else None
                except Exception:
                    res = None
                if isinstance(res, (bytes, bytearray)):
                    data = bytes(res)
            if data is None:
                continue
            try:
                data = bytes(data)
            except Exception:
                continue
            # Store locally.
            stored = False
            try:
                storage.store_shard(key, idx, version, data)
                stored = True
            except Exception:
                stored = False
            if not stored:
                try:
                    if hasattr(transport, "store_shard"):
                        stored = bool(transport.store_shard(node_id, key, idx, version, data))
                except Exception:
                    stored = False
            if stored:
                count += 1
                pulled = True
                try:
                    log("sync pulled shard %d for %s" % (idx, key))
                except Exception:
                    pass
                break
        if not pulled and owners:
            # Fallback: reconstruct this node's missing shard from the shards
            # held by its peers (handles returning nodes whose own shard is absent
            # from every peer).
            try:
                engine_cls = RS32Engine if mode == "rs32" else (ReplicationEngine if mode else None)
                if mode is None:
                    engine_cls = RS32Engine
                if engine_cls is None:
                    raise RuntimeError("no engine")
                engine = engine_cls()
                peer_shards = []
                # owners may be None (fallback) -> treat as [node_id]+peers? Use given owners.
                _owners = owners if owners else [node_id]
                for i, owner in enumerate(_owners):
                    if i == idx:
                        # This node's own shard is the missing one.
                        peer_shards.append(None)
                        continue
                    data = None
                    # Fetch from the owner peer if possible, else any peer.
                    try:
                        data = transport.fetch_shard(owner, key, i)
                    except Exception:
                        data = None
                    if data is None:
                        for p in peers:
                            try:
                                data = transport.fetch_shard(p, key, i)
                            except Exception:
                                data = None
                            if data is not None:
                                break
                    peer_shards.append(data if data is None else bytes(data))
                # Pad to 5 slots if needed.
                while len(peer_shards) < 5:
                    peer_shards.append(None)
                decoded = engine.decode(peer_shards)
                canonical = engine.encode(decoded)
                if idx < len(canonical) and canonical[idx] is not None:
                    data = bytes(canonical[idx])
                    stored = False
                    try:
                        storage.store_shard(key, idx, version, data)
                        stored = True
                    except Exception:
                        stored = False
                    if not stored:
                        try:
                            if hasattr(transport, "store_shard"):
                                stored = bool(transport.store_shard(node_id, key, idx, version, data))
                        except Exception:
                            stored = False
                    if stored:
                        count += 1
                        try:
                            log("sync pulled shard %d for %s" % (idx, key))
                        except Exception:
                            pass
            except Exception:
                pass

    try:
        log("sync pulled %d shards" % count)
    except Exception:
        pass
    return count


__all__ = ["read_repair", "RepairSweeper", "sync_on_start"]
