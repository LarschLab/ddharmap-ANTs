#!/usr/bin/env python3
"""Predict missing fish polarity from raw in-vivo 2P anatomy for manual review."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from codeants_2pf_hcr.orientation import AnatomyPolarityConfig, run_anatomy_polarity_prediction


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--microscopy-root", required=True, action="append", type=Path)
    parser.add_argument(
        "--target-microscopy-root",
        action="append",
        type=Path,
        help="Limit --all-fish targets to these roots while retaining all --microscopy-root paths as references.",
    )
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--exclude-fish-prefix", action="append", default=[])
    parser.add_argument("--target-fish-id", action="append")
    parser.add_argument("--all-fish", action="store_true", help="Score every fish under the microscopy roots, including known labels.")
    parser.add_argument("--calibration-fraction", type=float, default=1.0)
    args = parser.parse_args()
    summary = run_anatomy_polarity_prediction(
        microscopy_roots=args.microscopy_root,
        target_microscopy_roots=args.target_microscopy_root,
        output_dir=args.output_dir,
        exclude_fish_prefixes=args.exclude_fish_prefix,
        target_fish_ids=args.target_fish_id,
        include_all_fish=args.all_fish,
        config=AnatomyPolarityConfig(calibration_fraction=args.calibration_fraction),
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
