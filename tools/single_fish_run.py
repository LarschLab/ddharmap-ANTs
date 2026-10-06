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
    audit_run_artifacts,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name in ("plan", "run", "resume", "status", "verify"):
        command = subparsers.add_parser(name)
        command.add_argument("--spec", required=True, type=Path)
    artifacts = subparsers.add_parser("audit-artifacts", help="Read-only real-table scientific checks")
    artifacts.add_argument("--spec", required=True, type=Path)
    artifacts.add_argument("--report", type=Path)
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
    elif args.command == "verify":
        payload = verify_run(spec)
    else:
        payload = audit_run_artifacts(spec)
        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(json.dumps(payload, indent=2) + "\n")
    print(json.dumps(payload, indent=2, sort_keys=True))
    if args.command in {"run", "resume"}:
        return 0 if payload["status"] == "pass" else 1
    if args.command in {"verify", "audit-artifacts"}:
        return 0 if payload["ok"] else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
