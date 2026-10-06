#!/usr/bin/env python3
"""Create a verified, read-only snapshot of one staged single-fish run."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from codeants_2pf_hcr.freeze_run import freeze_single_fish_run


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fish-id", required=True)
    parser.add_argument("--pipeline-root", required=True, type=Path)
    parser.add_argument("--manifest-dir", required=True, type=Path)
    parser.add_argument("--destination", required=True, type=Path)
    parser.add_argument("--scientific-commit", help="Commit that produced the scientific run; utility HEAD is recorded separately.")
    parser.add_argument("--protection", choices=("permissions", "read-only-image"), default="permissions")
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="Git checkout whose commit and dirty state are recorded.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    metadata = freeze_single_fish_run(
        fish_id=args.fish_id,
        pipeline_root=args.pipeline_root,
        manifest_dir=args.manifest_dir,
        destination=args.destination,
        repo_root=args.repo_root,
        scientific_commit=args.scientific_commit,
        protection=args.protection,
    )
    print(json.dumps(metadata, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
