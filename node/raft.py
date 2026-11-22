"""Compatibility shim: allow ``from node.raft import ...`` (package) and legacy module."""

from node.raft import (  # noqa: F401
    DirectTransport,
    RaftLogEntry,
    RaftNode,
)

__all__ = ["RaftNode", "DirectTransport", "RaftLogEntry"]
