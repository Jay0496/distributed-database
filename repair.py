"""Compatibility shim: allow ``from repair import ...`` from repo root."""

try:
    from node.repair import read_repair, RepairSweeper  # noqa: F401
except Exception:
    try:
        from repair import read_repair, RepairSweeper  # noqa: F401
    except Exception:
        pass

__all__ = ["read_repair", "RepairSweeper"]