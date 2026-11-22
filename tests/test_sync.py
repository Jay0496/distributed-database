
from node.encode import RS32Engine
from node.repair import sync_on_start
from node.storage import Storage


class FakeTransport:
    def __init__(self, peers, keys, shards):
        self.peers = list(peers)
        self._keys = list(keys)
        self.shards = dict(shards or {})
        self.store_calls = []

    def list_keys(self, peer):
        return list(self._keys)

    def fetch_shard(self, peer, key, idx):
        return self.shards.get((peer, key, idx), self.shards.get((key, idx), None))

    def store_shard(self, owner, key, idx, version, data):
        self.store_calls.append((owner, key, idx, version, bytes(data)))
        self.shards[(owner, key, idx)] = bytes(data)
        return True


def test_sync_on_start_pulls_missing_shard(tmp_path):
    owners = [f"node{i}" for i in range(5)]
    key = "k"
    version = 3
    value = b"secret payload"
    canonical = RS32Engine().encode(value)

    storage = Storage(str(tmp_path / "s.db"))
    storage.upsert_meta(key, "rs32", version, len(value), "checksum", owners)

    # node0 is missing its shard locally.
    peers = [f"node{i}" for i in range(1, 5)]
    shards = {}
    for peer in peers:
        shards[(peer, key, 0)] = canonical[0]
    transport = FakeTransport(peers, [key], shards)

    pulled = sync_on_start("node0", 0, storage, transport)

    assert pulled == 1
    assert storage.fetch_shard(key, 0) == canonical[0]
    # Fetched bytes match stored bytes.
    assert transport.store_calls == [] or transport.store_calls[0][4] == canonical[0]

    storage.close()


def test_sync_on_start_skips_present_shard(tmp_path):
    owners = [f"node{i}" for i in range(5)]
    key = "k"
    version = 3
    value = b"secret payload"
    canonical = RS32Engine().encode(value)

    storage = Storage(str(tmp_path / "s2.db"))
    storage.upsert_meta(key, "rs32", version, len(value), "checksum", owners)
    storage.store_shard(key, 0, version, canonical[0])

    peers = [f"node{i}" for i in range(1, 5)]
    shards = {}
    transport = FakeTransport(peers, [key], shards)

    pulled = sync_on_start("node0", 0, storage, transport)

    assert pulled == 0
    assert storage.fetch_shard(key, 0) == canonical[0]

    storage.close()


def test_sync_on_start_no_corrupt_after_sync(tmp_path):
    owners = [f"node{i}" for i in range(5)]
    key = "k"
    version = 3
    value = b"secret payload"
    canonical = RS32Engine().encode(value)

    storage = Storage(str(tmp_path / "s3.db"))
    storage.upsert_meta(key, "rs32", version, len(value), "checksum", owners)

    peers = [f"node{i}" for i in range(1, 5)]
    shards = {}
    for peer in peers:
        shards[(peer, key, 0)] = canonical[0]
    transport = FakeTransport(peers, [key], shards)

    sync_on_start("node0", 0, storage, transport)

    assert storage.fetch_shard(key, 0) == canonical[0]
    assert storage.fetch_shard(key, 0) != b"\xff" * len(canonical[0])

    storage.close()
