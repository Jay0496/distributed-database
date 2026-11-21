"""Gateway CLI (scaffold)."""

import argparse
import sys


def main() -> None:
    parser = argparse.ArgumentParser(prog="gateway")
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("put")
    sub.add_parser("get")
    sub.add_parser("status")
    sub.add_parser("mode")
    parser.parse_args()
    print("gateway starting (scaffold)")
    sys.exit(0)


if __name__ == "__main__":
    main()