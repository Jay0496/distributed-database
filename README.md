# distributed-database

A 5-node distributed NoSQL key-value store. Data is erasure-coded (RS 3+2 by
default) or replicated across nodes, coordinated by a Raft leader, with
automatic repair and read-repair.

## Architecture

- **5 storage nodes** (`node1`..`node5`) expose a gRPC KV service on ports
  `50051`..`50055`.
- A **gateway CLI** discovers the Raft leader and forwards `put`/`get`/
  `status`/`mode` commands.
- Writes are split into 5 shards (RS 3+2 or 5-way replication) and stored on
  5 nodes; reads reconstruct the value from any 3 shards.
- **Raft** provides leader election and a replicated state machine for
  metadata/mode changes.
- **Failure detection + repair sweep** refill missing shards after node
  restarts or failures.
- Storage is SQLite-backed and persists across container restarts.

## Quick start

```sh
# 1. Build and start the cluster (nodes only)
docker compose up -d

# 2. Wait for all nodes to report healthy
docker compose ps

# 3. Put a value (RS 3+2)
docker compose run --rm gateway put demo-key "hello-ta" --mode rs32

# 4. Read it back
docker compose run --rm gateway get demo-key

# 5. Cluster status
docker compose run --rm gateway status

# 6. Change default mode
docker compose run --rm gateway mode repl

# 7. Kill a node and read still succeeds
docker compose stop node3
docker compose run --rm gateway get demo-key

# 8. Restart the node
docker compose start node3
```

Gateway is shipped under the `tools` profile; `docker compose run --rm gateway ...`
starts an ephemeral gateway container automatically. To keep a long-lived
gateway container instead:

```sh
docker compose --profile tools up -d
docker compose exec gateway python -m gateway.main status
```

## Smoke test

```sh
bash scripts/smoke.sh
```

PowerShell equivalent:

```powershell
powershell -File scripts/smoke.ps1
```

The smoke test brings the cluster up, performs a put/get round-trip, kills
two nodes and verifies reads still succeed, restarts them, and checks status.

## Demo steps

See `scripts/demo.sh` for a labeled, end-to-end demo:

```sh
bash scripts/demo.sh
```

## Documentation

Detailed design docs with Mermaid diagrams live in [`docs/`](docs/):

- [Diagrams index](docs/DIAGRAMS.md)
- [Architecture & deployment](docs/ARCHITECTURE.md)
- [Raft consensus](docs/RAFT.md)
- [Write path](docs/WRITE_PATH.md) / [Read path](docs/READ_PATH.md)
- [Redundancy & encoding](docs/REDUNDANCY.md)
- [Failure recovery](docs/FAILURE_RECOVERY.md)
- [Routing](docs/ROUTING.md) / [Data model](docs/DATA_MODEL.md)
- [Security](docs/SECURITY.md) / [API](docs/API.md)
- [Testing](docs/TESTING.md) / [Benchmarks](docs/BENCHMARKS.md) / [Demo](docs/DEMO.md)

## Logs

Nodes emit structured JSON logs (one object per line) to stdout. Look for:

- `starting node` / `listening`
- `sync pulled shard ...`
- `read-repair: rebuilt shard ...`
- `sweep: refilled shard ...`

Logs never contain payload bytes, values, tokens, authorization headers, or
secrets. Sensitive fields are redacted to `[REDACTED]`.

## Status JSON example

```json
{"leader": "node1", "degraded": [], "mode": "rs32", "key_count": 1}
```

## Economy proof

```sh
for v in node{1..5}_data; do echo -n "$v: "; docker volume inspect $v -f '{{.Mountpoint}}' | xargs du -sh; done
```

The repository stays small: source + tests + proto only, no large binaries.

## Known limitations

- Gateway runs under the `tools` profile; it is not started by plain
  `docker compose up -d`.
- Reads require at least 3 of 5 shards to be available.
- Mode changes and writes require a Raft leader; during leader failover
  writes may briefly fail and should be retried.
- Repair sweep runs on a fixed interval (default 10s); very recent writes
  may not be repaired instantly.
- Shared-token auth via gRPC metadata (`TOKEN`); no TLS, JWT, or per-user roles. Intended for trusted local/demo environments.
- Node healthchecks are TCP-only and do not validate application readiness
  beyond port binding.
