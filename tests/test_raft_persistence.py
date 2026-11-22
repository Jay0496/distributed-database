import json
import os
import sys
import time

try:
    from node.raft import RaftLogEntry, RaftNode
    from node.storage import Storage
except Exception:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "node"))
    from raft import RaftLogEntry, RaftNode  # type: ignore
    from storage import Storage  # type: ignore


def test_state_and_log_roundtrip(tmp_path):
    p = str(tmp_path / "a.db")
    s = Storage(p)
    node = RaftNode("n1", [], storage=s)
    node.current_term = 7
    node.voted_for = "n2"
    node.commit_index = 2
    # persist via hooks
    node._persist_state()
    # append log entries
    e1 = RaftLogEntry(term=7, index=0, command=b"cmd1")
    node._persist_append(e1)
    e2 = RaftLogEntry(term=7, index=0, command=b"cmd2")
    node._persist_append(e2)
    node.log = [RaftLogEntry(term=0, index=0, command=b""), e1, e2]
    s.close()

    s2 = Storage(p)
    node2 = RaftNode("n1", [], storage=s2)
    assert node2.current_term == 7
    assert node2.voted_for == "n2"
    assert node2.commit_index == 2
    assert [e.index for e in node2.log] == [0, 1, 2]
    assert node2.log[1].command == b"cmd1"
    assert node2.log[2].command == b"cmd2"
    node2.stop() if hasattr(node2, "stop") else None
    s2.close()


def test_put_commit_fsm_meta(tmp_path):
    p = str(tmp_path / "b.db")
    s = Storage(p)
    node = RaftNode("n1", [], storage=s)
    node.start()
    # wait for leader
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline and not node.is_leader():
        time.sleep(0.05)
    assert node.is_leader()
    cmd = json.dumps({"op": "put", "key": "k", "mode": "ec", "version": 3, "size": 10, "checksum": "abc", "owners": ["n1"]}).encode()
    idx = node.append_command(cmd)
    assert idx is not None
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline and node.commit_index < idx:
        time.sleep(0.05)
    assert node.commit_index >= idx
    m = s.get_meta("k")
    assert m is not None
    assert m["mode"] == "ec"
    assert m["version"] == 3
    node.stop()
    s.close()

    # reopen
    s2 = Storage(p)
    node2 = RaftNode("n1", [], storage=s2)
    # replay on startup
    assert node2.last_applied >= node2.commit_index or node2.commit_index == 0
    m2 = s2.get_meta("k")
    assert m2 is not None and m2["mode"] == "ec"
    node2.stop() if hasattr(node2, "stop") else None
    s2.close()


def test_replay_on_startup(tmp_path):
    p = str(tmp_path / "c.db")
    s = Storage(p)
    node = RaftNode("n1", [], storage=s)
    node.start()
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline and not node.is_leader():
        time.sleep(0.05)
    cmd = json.dumps({"op": "put", "key": "k2", "mode": "repl", "version": 1, "size": 5, "checksum": "cs", "owners": ["a"]}).encode()
    idx = node.append_command(cmd)
    assert idx is not None
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline and node.commit_index < idx:
        time.sleep(0.05)
    node.stop()
    s.close()

    s2 = Storage(p)
    node2 = RaftNode("n1", [], storage=s2)
    assert node2.last_applied == node2.commit_index
    assert s2.get_meta("k2") is not None
    node2.stop() if hasattr(node2, "stop") else None
    s2.close()


def test_truncation_matches_storage(tmp_path):
    p = str(tmp_path / "d.db")
    s = Storage(p)
    node = RaftNode("n1", [], storage=s)
    # append some log rows directly via hook
    e1 = RaftLogEntry(term=1, index=0, command=b"c1")
    node._persist_append(e1)
    e2 = RaftLogEntry(term=1, index=0, command=b"c2")
    node._persist_append(e2)
    node.log = [RaftLogEntry(term=0, index=0, command=b""), e1, e2]
    # truncate from index 2
    node._persist_truncate(2)
    del node.log[2:]
    # verify
    assert [e["idx"] for e in s.get_log_from(1)] == [1]
    assert [e.index for e in node.log] == [0, 1]
    # reopen
    s.close()
    s2 = Storage(p)
    node2 = RaftNode("n1", [], storage=s2)
    assert [e.index for e in node2.log] == [0, 1]
    s2.close()


def test_mode_op(tmp_path):
    p = str(tmp_path / "e.db")
    s = Storage(p)
    node = RaftNode("n1", [], storage=s)
    node.start()
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline and not node.is_leader():
        time.sleep(0.05)
    cmd = json.dumps({"op": "mode", "mode": "ec"}).encode()
    idx = node.append_command(cmd)
    assert idx is not None
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline and node.commit_index < idx:
        time.sleep(0.05)
    assert s.get_raft_state("default_mode") == "ec"
    node.stop()
    s.close()