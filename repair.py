"""Compatibility shim: allow ``from repair import ...`` from repo root."""

try:
    from node.repair import (  # noqa: F401
        read_repair,
        RepairSweeper,
        sync_on_start,
        _collect_keys,
        _fetch_shard,
        _reconstruct_missing,
    )
except Exception:
    try:
        from repair import (  # noqa: F401
            read_repair,
            RepairSweeper,
            sync_on_start,
            _collect_keys,
            _fetch_shard,
            _reconstruct_missing,
        )
    except Exception:
        pass

__all__ = ["read_repair", "RepairSweeper", "sync_on_start", "_collect_keys", "_fetch_shard", "_reconstruct_missing"]