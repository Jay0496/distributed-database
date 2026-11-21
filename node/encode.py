"""Redundancy engine for the distributed KV store.

This module implements the two redundancy strategies used by the write/read
paths:

* ``RS32Engine`` - Reed-Solomon RS(3,2): 3 data shards + 2 parity shards.
  Any 2 shards may be missing, or any 1 shard may be corrupt, and the
  original payload is still recoverable.
* ``ReplicationEngine`` - 5 full copies; survives the loss of any 4 shards.

No storage or network logic lives here; the engine operates purely on bytes.
"""

import hashlib
import os
import sys
from abc import ABC, abstractmethod
from typing import List, Optional, Sequence

try:
    from reedsolo import RSCodec, ReedSolomonError
except Exception:  # pragma: no cover - fallback import path
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


class RS32Engine(RedundancyEngine):
    """Reed-Solomon RS(3,2) engine.

    A length-prefixed payload is padded to a multiple of 3 bytes and split
    into 3 equal data shards. Parity is computed column-by-column: for each
    byte offset, the 3 data bytes form a 3-byte RS message and ``RSCodec(2)``
    produces 2 parity bytes which are appended to the two parity shards.
    """

    NSHARDS = 5
    NDATA = 3
    NPARITY = 2

    def __init__(self) -> None:
        self._codec = RSCodec(self.NPARITY)

    def required(self) -> int:
        return 3

    def encode(self, data: bytes) -> List[bytes]:
        data = bytes(data)
        # 4-byte big-endian length prefix so trailing zeros survive.
        payload = len(data).to_bytes(4, "big") + data
        # Pad to multiple of 3.
        if len(payload) % self.NDATA:
            payload = payload + b"\x00" * (self.NDATA - (len(payload) % self.NDATA))
        shard_len = len(payload) // self.NDATA
        data_shards = [bytearray(payload[i * shard_len:(i + 1) * shard_len]) for i in range(self.NDATA)]
        parity_shards = [bytearray(shard_len) for _ in range(self.NPARITY)]

        for pos in range(shard_len):
            message = bytes(data_shards[d][pos] for d in range(self.NDATA))
            codeword = self._codec.encode(message)  # length 5
            for p in range(self.NPARITY):
                parity_shards[p][pos] = codeword[self.NDATA + p]

        shards: List[bytes] = [bytes(s) for s in data_shards] + [bytes(s) for s in parity_shards]
        return shards

    def decode(self, shards: Sequence[Optional[bytes]]) -> bytes:
        # Normalize to exactly 5 entries.
        shards = list(shards)
        if len(shards) < self.NSHARDS:
            shards = shards + [None] * (self.NSHARDS - len(shards))
        elif len(shards) > self.NSHARDS:
            shards = shards[:self.NSHARDS]

        # Determine shard length from the longest present shard.
        lengths = [len(s) if s is not None else 0 for s in shards]
        shard_len = max(lengths) if lengths and max(lengths) > 0 else 0
        if shard_len == 0:
            # Nothing to decode; treat as empty payload.
            return b""

        # Pad missing/truncated shards with zeros and mark them as erasures.
        padded: List[bytearray] = []
        erasure_positions: List[int] = []
        for idx, shard in enumerate(shards):
            if shard is None:
                padded.append(bytearray(shard_len))
                erasure_positions.append(idx)
            else:
                b = bytes(shard)
                if len(b) < shard_len:
                    padded.append(bytearray(b) + bytearray(shard_len - len(b)))
                    erasure_positions.append(idx)
                else:
                    padded.append(bytearray(b[:shard_len]))

        # Reconstruct each data shard column by column.
        data_shards = [bytearray() for _ in range(self.NDATA)]
        for pos in range(shard_len):
            code = bytearray(padded[s][pos] for s in range(self.NSHARDS))
            try:
                if erasure_positions:
                    msg, _full, _errs = self._codec.decode(
                        code, erase_pos=list(erasure_positions)
                    )
                else:
                    msg, _full, _errs = self._codec.decode(code)
            except ReedSolomonError:
                # Fall back: try as erasures for any zero-length? already handled.
                raise
            for d in range(self.NDATA):
                data_shards[d].append(msg[d])

        # Concatenate data shards -> padded payload.
        padded_payload = b"".join(bytes(s) for s in data_shards)
        if len(padded_payload) < 4:
            return b""
        length = int.from_bytes(padded_payload[:4], "big")
        if length < 0 or length > len(padded_payload) - 4:
            # Clamp to available data to be safe.
            length = min(length, len(padded_payload) - 4)
        return padded_payload[4:4 + length]


class ReplicationEngine(RedundancyEngine):
    """5 full copies of the data."""

    NSHARDS = 5

    def required(self) -> int:
        return 1

    def encode(self, data: bytes) -> List[bytes]:
        data = bytes(data)
        return [bytes(data) for _ in range(self.NSHARDS)]

    def decode(self, shards: Sequence[Optional[bytes]]) -> bytes:
        for shard in shards:
            if shard is not None:
                return bytes(shard)
        return b""


def checksum(data: bytes) -> str:
    """Return the SHA-256 hex digest of ``data``."""
    return hashlib.sha256(bytes(data)).hexdigest()


# Convenience aliases for the checksum helper.
sha256_hexdigest = checksum
sha256_checksum = checksum
compute_checksum = checksum
checksum_sha256 = checksum


__all__ = [
    "RedundancyEngine",
    "RS32Engine",
    "ReplicationEngine",
    "checksum",
    "sha256_hexdigest",
    "sha256_checksum",
    "compute_checksum",
    "checksum_sha256",
]
