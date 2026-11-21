"""Redundancy engine for the distributed KV store (interface + checksum)."""

import hashlib
import os
import sys
from abc import ABC, abstractmethod
from typing import List, Optional, Sequence

try:
    from reedsolo import RSCodec, ReedSolomonError
except Exception:  # pragma: no cover
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from reedsolo import RSCodec, ReedSolomonError  # type: ignore


class RedundancyEngine(ABC):
    """Common interface for redundancy strategies."""

    @abstractmethod
    def encode(self, data: bytes) -> List[bytes]:
        """Encode ``data`` into exactly 5 shards."""

    @abstractmethod
    def decode(self, shards: Sequence[Optional[bytes]]) -> bytes:
        """Reconstruct the original bytes from (possibly partial/corrupt) shards."""

    @abstractmethod
    def required(self) -> int:
        """Minimum number of shards required to reconstruct."""


def checksum(data: bytes) -> str:
    """Return the SHA-256 hex digest of ``data``."""
    return hashlib.sha256(bytes(data)).hexdigest()


sha256_hexdigest = checksum
sha256_checksum = checksum
compute_checksum = checksum
checksum_sha256 = checksum


__all__ = [
    "RedundancyEngine",
    "checksum",
    "sha256_hexdigest",
    "sha256_checksum",
    "compute_checksum",
    "checksum_sha256",
]
