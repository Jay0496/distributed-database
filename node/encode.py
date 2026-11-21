"""Redundancy engine for the distributed KV store."""

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


class RS32Engine(RedundancyEngine):
    """Reed-Solomon RS(3,2) engine (encode + basic decode)."""

    NSHARDS = 5
    NDATA = 3
    NPARITY = 2

    def __init__(self) -> None:
        self._codec = RSCodec(self.NPARITY)

    def required(self) -> int:
        return 3

    def encode(self, data: bytes) -> List[bytes]:
        data = bytes(data)
        payload = len(data).to_bytes(4, "big") + data
        if len(payload) % self.NDATA:
            payload = payload + b"\x00" * (self.NDATA - (len(payload) % self.NDATA))
        shard_len = len(payload) // self.NDATA
        data_shards = [bytearray(payload[i * shard_len:(i + 1) * shard_len]) for i in range(self.NDATA)]
        parity_shards = [bytearray(shard_len) for _ in range(self.NPARITY)]

        for pos in range(shard_len):
            message = bytes(data_shards[d][pos] for d in range(self.NDATA))
            codeword = self._codec.encode(message)
            for p in range(self.NPARITY):
                parity_shards[p][pos] = codeword[self.NDATA + p]

        return [bytes(s) for s in data_shards] + [bytes(s) for s in parity_shards]

    def decode(self, shards: Sequence[Optional[bytes]]) -> bytes:
        shards = list(shards)
        shard_len = len(shards[0]) if shards and shards[0] is not None else 0
        data_shards = [bytearray() for _ in range(self.NDATA)]
        for pos in range(shard_len):
            code = bytearray(shards[s][pos] for s in range(self.NSHARDS))
            msg, _full, _errs = self._codec.decode(code)
            for d in range(self.NDATA):
                data_shards[d].append(msg[d])
        padded_payload = b"".join(bytes(s) for s in data_shards)
        length = int.from_bytes(padded_payload[:4], "big")
        return padded_payload[4:4 + length]


def checksum(data: bytes) -> str:
    return hashlib.sha256(bytes(data)).hexdigest()


sha256_hexdigest = checksum
sha256_checksum = checksum
compute_checksum = checksum
checksum_sha256 = checksum


__all__ = [
    "RedundancyEngine",
    "RS32Engine",
    "checksum",
    "sha256_hexdigest",
    "sha256_checksum",
    "compute_checksum",
    "checksum_sha256",
]
