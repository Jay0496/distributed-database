"""Compatibility shim: allow ``from node.raft import ...`` (package) and legacy module."""

from node.raft import (  # noqa: F401
    RaftNode,
    DirectTransport,
    RaftLogEntry,
)

__all__ = ["RaftNode", "DirectTransport", "RaftLogEntry"]
