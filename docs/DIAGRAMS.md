# Diagrams Index

All diagrams in this documentation use [Mermaid](https://mermaid.js.org/) and
render directly on GitHub/GitLab or any Mermaid-enabled Markdown viewer.

| Doc | Diagram | Purpose |
|---|---|---|
| [ARCHITECTURE](ARCHITECTURE.md) | System context + Docker topology | How the 5 nodes, gateway, and volumes fit together |
| [RAFT](RAFT.md) | State machine, election sequence, replication sequence, persistence flow | Consensus internals |
| [WRITE_PATH](WRITE_PATH.md) | PUT sequence | End-to-end write with erasure coding + Raft commit |
| [READ_PATH](READ_PATH.md) | GET sequence | End-to-end read with reconstruction + verification |
| [REDUNDANCY](REDUNDANCY.md) | RS(3,2) layout, replication layout, economy comparison | Storage efficiency trade-off |
| [FAILURE_RECOVERY](FAILURE_RECOVERY.md) | Detection sequence, sweep flowchart, startup sync flowchart | Fault tolerance and recovery |
| [ROUTING](ROUTING.md) | Consistent-hash ring | Key → node routing and minimal remap |
| [DATA_MODEL](DATA_MODEL.md) | ER diagram | SQLite schema |
| [SECURITY](SECURITY.md) | Auth sequence | Shared-token enforcement |
| [API](API.md) | Service class diagram | gRPC surface + CLI |
| [TESTING](TESTING.md) | Test pyramid | Unit → integration → smoke |
| [DEMO](DEMO.md) | Demo sequence | Scripted demo walkthrough |

## Rendering locally

- GitHub/GitLab render Mermaid in Markdown automatically.
- VS Code: install the "Markdown Preview Mermaid Support" extension.
- CLI: `npx @mermaid-js/mermaid-cli -i docs/ARCHITECTURE.md -o architecture.png`
  (extracts the first diagram only; render each doc separately).
