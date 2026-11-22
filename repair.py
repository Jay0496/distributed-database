"""Compatibility shim: allow ``from repair import read_repair`` from repo root."""

try:
    from node.repair import read_repair  # noqa: F401
except Exception:
    from repair import read_repair  # noqa: F401

__all__ = ["read_repair"]
