#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path

from codeants_2pf_hcr import organize


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Organize staged analysis outputs into stable folders.")
    parser.add_argument("--analysis-dir", required=True, help="Path to fish 03_analysis directory.")
    parser.add_argument("--apply", action="store_true", help="Apply moves. Without this flag, runs as dry-run.")
    parser.add_argument(
        "--move-unknown",
        action="store_true",
        help="Move unknown top-level files into functional/derived/unclassified.",
    )
    parser.add_argument("--quiet", action="store_true", help="Suppress per-file output.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    organize(
        Path(args.analysis_dir),
        apply=bool(args.apply),
        move_unknown=bool(args.move_unknown),
        verbose=not bool(args.quiet),
    )


if __name__ == "__main__":
    main()
