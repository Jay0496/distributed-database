from node.transport import GrpcTransport


def test_node_id_resolves_to_peer_address():
    transport = GrpcTransport("node1:50051,node2:50052,node3:50053")
    assert transport._stub("node1") is transport._stub("node1:50051")
    assert transport._stub("node3") is transport._stub("node3:50053")


def test_unknown_node_id_without_port_returns_none():
    transport = GrpcTransport("node1:50051")
    assert transport._stub("node9") is None
