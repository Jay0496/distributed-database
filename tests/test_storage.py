import os
import sys
import threading

try:
    from node.storage import Storage
except Exception:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "node"))
    from storage import Storage


def test_schema_idempotent(tmp_path):
    p = str(tmp_path / "s.db")
    s = Storage(p)
    s.close()
    s2 = Storage(p)
    s2.close()


def test_raft_state_roundtrip(tmp_path):
    s = Storage(str(tmp_path / "s.db"))
    assert s.get_raft_state("term") is None
    s.set_raft_state("term", "5")
    assert s.get_raft_state("term") == "5"
    s.set_raft_state("term", "6")
    assert s.get_raft_state("term") == "6"
    s.close()


def test_log_append_truncate_reopen(tmp_path):
    p = str(tmp_path / "s.db")
    s = Storage(p)
    i1 = s.append_log(1, b"cmd1")
    i2 = s.append_log(1, b"cmd2")
    assert i1 == 1 and i2 == 2
    assert s.get_log(1) == {"idx": 1, "term": 1, "command": b"cmd1"}
    assert [e["idx"] for e in s.get_log_from(1)] == [1, 2]
    assert s.last_log()["idx"] == 2
    s.truncate_log(2)
    assert s.get_log(2) is None
    assert [e["idx"] for e in s.get_log_from(1)] == [1]
    s.close()
    s2 = Storage(p)
    assert [e["idx"] for e in s2.get_log_from(1)] == [1]
    s2.close()


def test_meta_owners_json(tmp_path):
    s = Storage(str(tmp_path / "s.db"))
    s.upsert_meta("k", "ec", 3, 10, "abc", ["n1", "n2"])
    m = s.get_meta("k")
    assert m["owners"] == ["n1", "n2"]
    assert m["mode"] == "ec"
    assert m["version"] == 3
    assert m["size"] == 10
    assert m["checksum"] == "abc"
    assert s.list_meta_keys() == ["k"]
    s.upsert_meta("k", "repl", 4, 20, "def", ["n3"])
    assert s.get_meta("k")["owners"] == ["n3"]
    assert s.get_meta("missing") is None
    s.close()


def test_shards_has_fetch_list_for_node(tmp_path):
    s = Storage(str(tmp_path / "s.db"))
    s.store_shard("keyA", 0, 1, b"blob0")
    s.store_shard("keyA", 1, 1, b"blob1")
    s.store_shard("keyB", 1, 2, b"blobB1")
    assert s.has_shard("keyA", 0)
    assert not s.has_shard("keyA", 2)
    assert s.fetch_shard("keyA", 0) == b"blob0"
    assert s.fetch_shard("keyA", 9) is None
    assert sorted(s.list_for_node(1)) == ["keyA", "keyB"]
    assert s.list_for_node(0) == ["keyA"]
    s.store_shard("keyA", 0, 2, b"blob0v2")
    assert s.fetch_shard("keyA", 0) == b"blob0v2"
    s.delete_shard("keyA", 0)
    assert not s.has_shard("keyA", 0)
    s.close()


def test_reopen_durability(tmp_path):
    p = str(tmp_path / "s.db")
    s = Storage(p)
    s.set_raft_state("x", "y")
    s.append_log(2, b"data")
    s.upsert_meta("m", "ec", 1, 5, "cs", ["a"])
    s.store_shard("sh", 2, 1, b"shard")
    s.close()
    s2 = Storage(p)
    assert s2.get_raft_state("x") == "y"
    assert s2.get_log(1)["command"] == b"data"
    assert s2.get_meta("m")["owners"] == ["a"]
    assert s2.fetch_shard("sh", 2) == b"shard"
    s2.close()


def test_concurrent_writes(tmp_path):
    p = str(tmp_path / "s.db")
    s = Storage(p)
    errors = []

    def worker(i):
        try:
            for j in range(5):
                s.append_log(1, ("t%d-%d" % (i, j)).encode())
                s.set_raft_state("k%d" % i, str(j))
                s.store_shard("key%d" % i, j % 3, 1, ("b%d%d" % (i, j)).encode())
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert len(s.get_log_from(1)) == 100
    s.close()
    s2 = Storage(p)
    assert len(s2.get_log_from(1)) == 100
    s2.close()
