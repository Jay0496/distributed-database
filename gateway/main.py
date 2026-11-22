"""Gateway CLI."""

import argparse
import json
import os
import sys

_REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

try:
    from gateway.client import GatewayClient
except Exception:  # pragma: no cover
    sys.path.insert(0, os.path.dirname(__file__))
    from client import GatewayClient  # type: ignore


def _peers_from_args(args):
    raw = getattr(args, "peers", None)
    if raw:
        return raw
    raw = os.environ.get("PEERS") or os.environ.get("GATEWAY_PEERS")
    if raw:
        return raw
    return "localhost:50051,localhost:50052,localhost:50053,localhost:50054,localhost:50055"


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(prog="gateway")
    parser.add_argument("--peers", default=None)
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("status")

    args = parser.parse_args(argv)
    peers = _peers_from_args(args)
    client = GatewayClient(peers)

    if args.command == "status":
        print(json.dumps(client.status()))
        return
    if args.command is None:
        parser.print_help()
        return
    print(f"unknown command: {args.command}", file=sys.stderr)
    sys.exit(2)


if __name__ == "__main__":
    main()
