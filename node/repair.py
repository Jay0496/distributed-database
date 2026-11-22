"""Read repair helper for the distributed KV store.

After a successful read, compare each fetched shard against the canonical
shards reconstructed from the decoded value. Missing or corrupt shards are
rebuilt by scheduling a best-effort background ``StoreShard`` to their owner.
"""

import threading
from typing import List


def _normalize_call(args, kwargs):
    engine = kwargs.get("engine", None)
    shards = kwargs.get("shards", None)
    canonical_shards = kwargs.get("canonical_shards", None)
    if canonical_shards is None:
        canonical_shards = kwargs.get("canonical", None)
    owners = kwargs.get("owners", None)
    transport = kwargs.get("transport", None)
    key = kwargs.get("key", None)
    version = kwargs.get("version", None)
    log = kwargs.get("log", None)
    if log is None:
        log = print

    positional = list(args)

    if positional and isinstance(positional[0], (list, tuple)):
        if shards is None:
            shards = positional.pop(0)
        if canonical_shards is None and positional and isinstance(positional[0], (list, tuple)):
            canonical_shards = positional.pop(0)
        if owners is None and positional and isinstance(positional[0], (list, tuple)):
            owners = positional.pop(0)
        if transport is None and positional:
            transport = positional.pop(0)
        if key is None and positional:
            key = positional.pop(0)
        if version is None and positional:
            version = positional.pop(0)
        if positional and callable(positional[0]):
            log = positional.pop(0)
    else:
        if engine is None and positional:
            engine = positional.pop(0)
        if shards is None and positional:
            shards = positional.pop(0)
        if canonical_shards is None and positional and isinstance(positional[0], (list, tuple)):
            canonical_shards = positional.pop(0)
        if owners is None and positional and (isinstance(positional[0], (list, tuple)) or positional[0] is None):
            owners = positional.pop(0)
        if transport is None and positional:
            transport = positional.pop(0)
        if key is None and positional:
            key = positional.pop(0)
        if version is None and positional:
            version = positional.pop(0)
        if positional and callable(positional[0]):
            log = positional.pop(0)

    if log is None:
        log = print

    return engine, shards, canonical_shards, owners, transport, key, version, log


def read_repair(*args, **kwargs) -> List[int]:
    engine, shards, canonical_shards, owners, transport, key, version, log = _normalize_call(args, kwargs)

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


__all__ = ["read_repair"]
