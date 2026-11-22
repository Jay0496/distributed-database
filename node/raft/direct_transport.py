"""In-process direct transport between RaftNode instances."""

import contextlib

_NODE_REGISTRY: dict[str, object] = {}


class DirectTransport:
    def __init__(self, nodes=None):
        self.nodes = nodes if nodes is not None else _NODE_REGISTRY

    def _node(self, peer_id):
        n = self.nodes.get(peer_id) if hasattr(self.nodes, "get") else None
        if n is None:
            n = _NODE_REGISTRY.get(peer_id)
        return n

    def request_vote(self, peer_id, req_dict):
        node = self._node(peer_id)
        if node is None:
            return 0, False
        return node.handle_request_vote(
            req_dict["term"],
            req_dict["candidate_id"],
            req_dict["last_log_index"],
            req_dict["last_log_term"],
        )

    def append_entries(self, peer_id, req_dict):
        node = self._node(peer_id)
        if node is None:
            return 0, False
        return node.handle_append_entries(
            req_dict["term"],
            req_dict["leader_id"],
            req_dict["prev_log_index"],
            req_dict["prev_log_term"],
            req_dict.get("entries", []),
            req_dict["leader_commit"],
        )

    def store_shard(self, peer_id, key, shard_index, version, data):
        node = self._node(peer_id)
        if node is None:
            return False
        try:
            node.storage.store_shard(key, int(shard_index), int(version), bytes(data))
            return True
        except Exception:
            return False

    def fetch_shard(self, peer_id, key, shard_index):
        node = self._node(peer_id)
        if node is None:
            return None
        try:
            return node.storage.fetch_shard(key, int(shard_index))
        except Exception:
            return None

    def has_shard(self, peer_id, key, shard_index):
        node = self._node(peer_id)
        if node is None:
            return False
        try:
            return bool(node.storage.has_shard(key, int(shard_index)))
        except Exception:
            return False

    def list_keys(self, peer_id):
        node = self._node(peer_id)
        if node is None:
            return []
        try:
            if hasattr(node, "storage") and node.storage is not None:
                return list(node.storage.list_meta_keys())
        except Exception:
            pass
        try:
            if hasattr(node, "list_meta_keys"):
                return list(node.list_meta_keys())
        except Exception:
            pass
        return []

    def heartbeat(self, peer_id, node_id="", term=0):
        node = self._node(peer_id)
        if node is None:
            return False, "", 0
        try:
            if hasattr(node, "_running") and node._running is False:
                return False, "", 0
            lid = None
            if hasattr(node, "leader_id"):
                try:
                    lid = node.leader_id()
                except Exception:
                    lid = None
            if lid is None and hasattr(node, "_leader_id"):
                lid = getattr(node, "_leader_id", None)
            cur_term = 0
            if hasattr(node, "current_term"):
                try:
                    cur_term = int(node.current_term)
                except Exception:
                    cur_term = 0
            # if node is a NodeService-like object
            if hasattr(node, "raft") and getattr(node, "raft", None) is not None:
                r = node.raft
                try:
                    if hasattr(r, "_running") and r._running is False:
                        return False, "", 0
                except Exception:
                    pass
                with contextlib.suppress(Exception):
                    lid = r.leader_id() if hasattr(r, "leader_id") else lid
                with contextlib.suppress(Exception):
                    cur_term = int(r.current_term)
            return True, (lid if lid is not None else ""), int(cur_term or 0)
        except Exception:
            return False, "", 0


def __getattr__(name):
    try:
        from . import node as _node
        return getattr(_node, name)
    except Exception as exc:
        raise AttributeError(name) from exc


__all__ = ["DirectTransport", "_NODE_REGISTRY"]
