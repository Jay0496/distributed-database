# Raft Consensus

Raft provides leader election, replicated log, and a deterministic state
machine. This maps to the proposal's **Raft Consensus Layer** and the course's
**reliability**, **concurrency**, and **fault tolerance** principles.

The implementation is split into mixins composed by `RaftNode`:

| Concern | File | Class |
|---|---|---|
| Facade | `node/raft/node.py` | `RaftNode` |
| Election | `node/raft/election.py` | `RaftElection` |
| Replication | `node/raft/replication.py` | `RaftReplication` |
| Persistence/FSM | `node/raft/persistence.py` | `RaftPersistence` |
| Failure detection | `node/raft/failure_detector.py` | `RaftFailureDetector` |
| In-process transport | `node/raft/direct_transport.py` | `DirectTransport` |

## State machine

```mermaid
stateDiagram-v2
    [*] --> Follower
    Follower --> Candidate: election timeout (150-300 ms)
    Candidate --> Candidate: split vote (new term)
    Candidate --> Leader: majority votes (3/5)
    Candidate --> Follower: higher term or AppendEntries
    Leader --> Follower: higher term observed
    Follower --> Follower: heartbeat from leader
```

## Leader election

```mermaid
sequenceDiagram
    autonumber
    participant A as Node A
    participant P1 as Peer B
    participant P2 as Peer C
    participant P3 as Peer D
    participant P4 as Peer E

    Note over A: state = FOLLOWER<br/>election_deadline expires
    A->>A: state = CANDIDATE, term += 1, voted_for = self
    A->>A: persist current_term/voted_for
    par RequestVote fan-out
        A->>P1: RequestVote(term, lastLogIndex, lastLogTerm)
        A->>P2: RequestVote(...)
        A->>P3: RequestVote(...)
        A->>P4: RequestVote(...)
    end
    P1-->>A: (term, granted=true)
    P2-->>A: (term, granted=true)
    P3-->>A: (term, granted=false)
    P4-->>A: (term, granted=false)
    Note over A: votes = 1 + 2 = 3 >= majority(3)<br/>state = LEADER
    A->>A: init next_index/match_index
    A->>P1: AppendEntries (empty, heartbeat)
    A->>P2: AppendEntries (empty, heartbeat)
    A->>P3: AppendEntries (empty, heartbeat)
    A->>P4: AppendEntries (empty, heartbeat)
```

Code: `node/raft/election.py::RaftElection._start_election`,
`handle_request_vote`, `_reset_election_timer`, `_run`. Majority formula:
`(len(peers) + 1) // 2 + 1`.

## Log replication and commit

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant L as Leader
    participant F1 as Follower 1
    participant F2 as Follower 2
    participant F3 as Follower 3
    participant F4 as Follower 4

    C->>L: Put(key, value, mode)
    L->>L: engine.encode(value) → 5 shards
    L->>F1: StoreShard(0)
    L->>F2: StoreShard(1)
    L->>F3: StoreShard(2)
    L->>F4: StoreShard(3)
    L->>L: StoreShard(4) local
    Note over L: all 5 shards acked
    L->>L: append_command({op:put, key, mode, version, size, checksum, owners})
    par AppendEntries fan-out
        L->>F1: AppendEntries(prev, entries, leader_commit)
        L->>F2: AppendEntries(...)
        L->>F3: AppendEntries(...)
        L->>F4: AppendEntries(...)
    end
    F1-->>L: success
    F2-->>L: success
    F3-->>L: success
    Note over L: match_index majority (3/5)
    L->>L: commit_index = idx, apply FSM
    L-->>C: PutResponse(ok, version)
    Note over L,F4: next heartbeat carries leader_commit
```

Code: `node/raft/replication.py::RaftReplication.append_command`,
`_replicate_once`, `_advance_commit_index`; `handle_append_entries`.

## Persistence and FSM replay

```mermaid
flowchart TD
    A[Node starts] --> B[RaftNode.__init__]
    B --> C[load_from_storage]
    C --> D[read current_term / voted_for / commit_index]
    D --> E[read raft_log rows 1..N]
    E --> F[replay committed entries 1..commit_index]
    F --> G[apply FSM]
    G --> H{op}
    H -->|put| I[storage.upsert_meta]
    H -->|mode| J[storage.set_raft_state default_mode]
    H -->|noop| K[no-op]
    I --> L[ready]
    J --> L
    K --> L
```

Code: `node/raft/persistence.py::RaftPersistence.load_from_storage`,
`_replay_committed_locked`, `_default_apply`, `_apply_entry`,
`_persist_state`, `_persist_append`, `_persist_truncate`.

FSM command shape (JSON):
`{"op":"put","key":...,"mode":...,"version":...,"size":...,"checksum":...,"owners":[...]}`
or `{"op":"mode","mode":"rs32|replication"}`.

## Safety properties

- At most one leader per term (voted_for persisted).
- Commit requires a majority (3/5) of `match_index`.
- Followers truncate conflicting suffixes and reject mismatched
  `prev_log_index/term`.
- Higher terms force step-down.

## Verified by

- `tests/test_raft_election.py::test_election_converges_single_leader`
- `tests/test_raft_election.py::test_no_split_brain_sampled`
- `tests/test_raft_election.py::test_leader_failover`
- `tests/test_raft_replication.py::test_put_replicates_to_majority_and_commits`
- `tests/test_raft_replication.py::test_commit_requires_quorum`
- `tests/test_raft_persistence.py::test_replay_on_startup`
- `tests/test_raft_submodules.py::test_submodule_mixins_are_importable`
