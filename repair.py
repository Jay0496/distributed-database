"""Compatibility shim: allow ``from repair import ...`` from repo root."""

try:
    from node.repair import RepairSweeper, read_repair  # noqa: F401
except Exception:
    try:
        from repair import RepairSweeper, read_repair  # noqa: F401
    except Exception:
        pass

__all__ = ["read_repair", "RepairSweeper"]