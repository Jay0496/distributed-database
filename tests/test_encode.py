import hashlib
import itertools
import os
import random
import sys

try:
    from node.encode import (
        RS32Engine,
        ReplicationEngine,
        checksum,
        RedundancyEngine,
    )
except Exception:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "node"))
    from encode import (  # type: ignore
        RS32Engine,
        ReplicationEngine,
        checksum,
        RedundancyEngine,
    )


def _random_payloads(n=100, seed=42):
    rng = random.Random(seed)
    out = []
    for _ in range(n):
        size = rng.randint(0, 200)
        out.append(bytes(rng.randrange(256) for _ in range(size)))
    # include edge cases
    out.append(b"")
    out.append(b"\x00" * 5)
    out.append(b"\x00" * 7)  # not multiple of 3 after prefix
    out.append(bytes([255] * 100))
    return out


def test_checksum_helper():
    data = b"hello world"
    assert checksum(data) == hashlib.sha256(data).hexdigest()


def test_rs_required():
    assert RS32Engine().required() == 3


def test_repl_required():
    assert ReplicationEngine().required() == 1


def test_rs_roundtrip_no_loss():
    for payload in _random_payloads():
        eng = RS32Engine()
        shards = eng.encode(payload)
        assert len(shards) == 5
        assert eng.decode(shards) == payload


def test_rs_drop_any_two():
    for payload in _random_payloads():
        eng = RS32Engine()
        shards = eng.encode(payload)
        for combo in itertools.combinations(range(5), 2):
            corrupted = list(shards)
            for i in combo:
                corrupted[i] = None
            assert eng.decode(corrupted) == payload, (combo, payload)


def test_rs_corrupt_one():
    rng = random.Random(7)
    for payload in _random_payloads():
        eng = RS32Engine()
        shards = eng.encode(payload)
        # corrupt each shard position once
        for target in range(5):
            corrupted = [bytes(s) for s in shards]
            # flip a few bytes
            b = bytearray(corrupted[target])
            if len(b) == 0:
                continue
            for _ in range(min(3, len(b))):
                pos = rng.randrange(len(b))
                b[pos] ^= 0xFF
            corrupted[target] = bytes(b)
            assert eng.decode(corrupted) == payload, (target, payload)


def test_repl_survives_any_one_shard():
    for payload in _random_payloads():
        eng = ReplicationEngine()
        shards = eng.encode(payload)
        for keep in range(5):
            corrupted = [None] * 5
            corrupted[keep] = shards[keep]
            assert eng.decode(corrupted) == payload, (keep, payload)


def test_repl_roundtrip():
    for payload in _random_payloads():
        eng = ReplicationEngine()
        shards = eng.encode(payload)
        assert eng.decode(shards) == payload


def test_interface_abstract():
    # RedundancyEngine should not be instantiable directly
    try:
        RedundancyEngine()  # type: ignore
        instantiated = True
    except TypeError:
        instantiated = False
    assert not instantiated
