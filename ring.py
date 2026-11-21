"""Compatibility shim: allow ``from ring import ConsistentHashRing`` from repo root."""

from gateway.ring import ConsistentHashRing  # noqa: F401

__all__ = ["ConsistentHashRing"]