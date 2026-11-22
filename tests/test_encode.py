import hashlib
import itertools
import random

import pytest

from node.encode import (
    RS32Engine,
    ReplicationEngine,
    RedundancyEngine,
    checksum,
)

SIZES = [0, 1, 2, 3, 5, 100, 1000]


def _payload(size, seed=0xC0FFEE):
    rng = random.Random(seed + size * 31)
    return bytes(rng.randrange(256) for _ in range(size))


def _engine(mode):
    if mode == "rs":
        return RS32Engine()
    if mode == "repl":
        return ReplicationEngine()
    raise ValueError(mode)


# ---------------------------------------------------------------------------
# checksum helper
# ---------------------------------------------------------------------------


def test_checksum_helper():
    data = b"hello world"
    assert checksum(data) == hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# required() values
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "engine,expected",
    [(RS32Engine(), 3), (ReplicationEngine(), 1)],
)
def test_required_values(engine, expected):
    assert engine.required() == expected


def test_interface_abstract():
    with pytest.raises(TypeError):
        RedundancyEngine()  # type: ignore


# ---------------------------------------------------------------------------
# round-trip (table-driven over sizes and modes)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("mode", ["rs", "repl"])
@pytest.mark.parametrize("size", SIZES)
def test_roundtrip(mode, size):
    data = _payload(size)
    eng = _engine(mode)
    shards = eng.encode(data)
    assert len(shards) == 5
    assert eng.decode(shards) == data


# ---------------------------------------------------------------------------
# RS: drop any 2 shards
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("size", SIZES)
@pytest.mark.parametrize("combo", list(itertools.combinations(range(5), 2)))
def test_rs_drop_any_two(size, combo):
    data = _payload(size)
    eng = RS32Engine()
    shards = eng.encode(data)
    corrupted = list(shards)
    for i in combo:
        corrupted[i] = None
    assert eng.decode(corrupted) == data


# ---------------------------------------------------------------------------
# RS: corrupt 1 shard
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("size", SIZES)
@pytest.mark.parametrize("target", range(5))
def test_rs_corrupt_one(size, target):
    data = _payload(size)
    eng = RS32Engine()
    shards = eng.encode(data)
    corrupted = [bytes(s) for s in shards]
    b = bytearray(corrupted[target])
    assert len(b) > 0
    b[0] ^= 0xFF
    corrupted[target] = bytes(b)
    assert eng.decode(corrupted) == data


# ---------------------------------------------------------------------------
# Replication: survives a single available shard
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("size", SIZES)
@pytest.mark.parametrize("keep", range(5))
def test_repl_survives_single_shard(size, keep):
    data = _payload(size)
    eng = ReplicationEngine()
    shards = eng.encode(data)
    corrupted = [None] * 5
    corrupted[keep] = shards[keep]
    assert eng.decode(corrupted) == data


# ---------------------------------------------------------------------------
# Replication: survives loss of any single shard
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("size", SIZES)
@pytest.mark.parametrize("drop", range(5))
def test_repl_drop_one(size, drop):
    data = _payload(size)
    eng = ReplicationEngine()
    shards = eng.encode(data)
    corrupted = list(shards)
    corrupted[drop] = None
    assert eng.decode(corrupted) == data
