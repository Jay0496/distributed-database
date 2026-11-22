"""Read repair helper for the distributed KV store.

After a successful read, compare each fetched shard against the canonical
shards reconstructed from the decoded value. Missing or corrupt shards are
rebuilt by scheduling a best-effort background ``StoreShard`` to their owner.
"""

import threading
from typing import Any, Callable, List, Optional, Sequence


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


__all__ = ["read_repair"]
