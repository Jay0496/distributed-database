"""Compatibility shim: allow ``from repair import ...`` from repo root."""

try:  # noqa: SIM105
    from node.repair import (  # noqa: F401
        RepairSweeper,
        _collect_keys,
        _fetch_shard,
        _reconstruct_missing,
        read_repair,
        sync_on_start,
    )
except Exception:  # noqa: SIM105
    try:  # noqa: SIM105
        from repair import (  # noqa: F401
            RepairSweeper,
            _collect_keys,
            _fetch_shard,
            _reconstruct_missing,
            read_repair,
            sync_on_start,
        )
    except Exception:
        pass

__all__ = ["read_repair", "RepairSweeper", "sync_on_start", "_collect_keys", "_fetch_shard", "_reconstruct_missing"]