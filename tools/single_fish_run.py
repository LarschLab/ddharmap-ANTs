#!/usr/bin/env python3
"""Execute a declarative, deterministic single-fish pipeline DAG."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from codeants_2pf_hcr.single_fish_run import (
    build_run_plan,
    build_run_status,
    execute_run,
    load_run_spec,
    verify_run,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name in ("plan", "run", "resume", "status", "verify"):
        command = subparsers.add_parser(name)
        command.add_argument("--spec", required=True, type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    spec = load_run_spec(args.spec)
    if args.command == "plan":
        payload = build_run_plan(spec, resume=False)
    elif args.command == "run":
        payload = execute_run(spec, resume=False)
    elif args.command == "resume":
        payload = execute_run(spec, resume=True)
    elif args.command == "status":
        payload = build_run_status(spec)
    else:
        payload = verify_run(spec)
    print(json.dumps(payload, indent=2, sort_keys=True))
    if args.command in {"run", "resume"}:
        return 0 if payload["status"] == "pass" else 1
    if args.command == "verify":
        return 0 if payload["ok"] else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
