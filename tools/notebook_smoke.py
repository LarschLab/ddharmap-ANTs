#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from pathlib import Path

from codeants_2pf_hcr import SmokeValidationError, resolve_fish_context, run_smoke_tier


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run staged smoke checks for notebook output contracts."
    )
    parser.add_argument("--fish-id", required=True, help="Fish identifier, e.g. L395_f11.")
    parser.add_argument("--tier", choices=("A", "B", "C", "D"), default="A", help="Smoke tier to run.")
    parser.add_argument("--data-mode", choices=("local", "nas"), default="local", help="Fish context data mode.")
    parser.add_argument("--owner", default="Matilde", help="Owner folder used for nas mode fish resolution.")
    parser.add_argument("--local-root", default=None, help="Optional override for local data root.")
    parser.add_argument("--nas-root", default=None, help="Optional override for NAS root.")
    parser.add_argument("--out-reg", default=None, help="Optional direct override for functional registration output folder.")
    parser.add_argument("--json", action="store_true", help="Print JSON result payload.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.out_reg:
        out_reg = Path(args.out_reg)
    else:
        ctx = resolve_fish_context(
            fish_id=args.fish_id,
            owner=args.owner,
            data_mode=args.data_mode,
            local_root=args.local_root,
            nas_root=args.nas_root,
        )
        out_reg = ctx.out_reg

    try:
        result = run_smoke_tier(
            out_reg=out_reg,
            tier=args.tier,
            fish_id=args.fish_id,
        )
    except SmokeValidationError as exc:
        print(f"[smoke] FAIL tier={args.tier} fish={args.fish_id}: {exc}")
        raise SystemExit(1) from exc

    checks = result["checks"]
    print(
        f"[smoke] PASS tier={result['tier']} fish={args.fish_id} checks={result['n_checks']} out_reg={result['out_reg']}"
    )
    for item in checks:
        label = item["name"]
        rows = item["rows"]
        path = item["path"]
        print(f"  - {label}: rows={rows} path={path}")
    if args.json:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
