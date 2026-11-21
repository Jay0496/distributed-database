"""Compatibility shim: allow ``from storage import Storage`` from repo root."""
from node.storage import Storage  # noqa: F401

__all__ = ["Storage"]
