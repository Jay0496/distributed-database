"""Consistent hash ring with virtual nodes for key -> node routing."""

import bisect
import hashlib
from typing import List


class ConsistentHashRing:
    """A consistent-hash ring mapping keys to physical nodes via vnodes."""

    def __init__(self, vnodes: int = 150) -> None:
        self.vnodes = vnodes
        self._ring: dict[int, str] = {}
        self._sorted: list[int] = []
        self._nodes: set[str] = set()

    def _hash(self, value: str | bytes) -> int:
        if isinstance(value, str):
            data = value.encode("utf-8")
        elif isinstance(value, bytes):
            data = value
        else:
            data = str(value).encode("utf-8")
        return int(hashlib.md5(data).hexdigest(), 16)

    def add_node(self, node_id: str) -> None:
        for i in range(self.vnodes):
            pos = self._hash(f"{node_id}:{i}")
            self._ring[pos] = node_id
        self._nodes.add(node_id)
        self._sorted = sorted(self._ring)

    def remove_node(self, node_id: str) -> None:
        for pos in [p for p, n in self._ring.items() if n == node_id]:
            del self._ring[pos]
        self._nodes.discard(node_id)
        self._sorted = sorted(self._ring)

    def get_node(self, key) -> str:
        if not self._ring:
            raise ValueError("consistent hash ring is empty")
        pos = self._hash(key)
        idx = bisect.bisect_left(self._sorted, pos)
        if idx == len(self._sorted):
            idx = 0
        return self._ring[self._sorted[idx]]

    def nodes(self) -> List[str]:
        return sorted(self._nodes)

    def __len__(self) -> int:
        return len(self._ring)
