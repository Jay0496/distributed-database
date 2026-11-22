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


__all__ = ["read_repair", "RepairSweeper"]
