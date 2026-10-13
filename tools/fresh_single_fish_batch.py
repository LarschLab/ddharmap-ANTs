#!/usr/bin/env python3
"""Prepare or execute an unattended, control-gated fresh native fish batch."""
import argparse
import json
from pathlib import Path
from codeants_2pf_hcr.fresh_batch import prepare_fresh_batch, run_fresh_batch


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    prepare = sub.add_parser("prepare")
    for name in ("data-root", "repo-root", "validation-root", "batch"):
        prepare.add_argument("--" + name, type=Path, required=True)
    prepare.add_argument("--run-id", required=True)
    run = sub.add_parser("run")
    run.add_argument("--batch", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "prepare":
        result = prepare_fresh_batch(data_root=args.data_root, repo_root=args.repo_root,
                                     validation_root=args.validation_root, run_id=args.run_id, batch_path=args.batch)
    else:
        result = run_fresh_batch(args.batch)
    print(json.dumps(result, indent=2))
    return 1 if result.get("status") == "fail" else 0


if __name__ == "__main__":
    raise SystemExit(main())
