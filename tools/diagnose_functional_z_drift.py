#!/usr/bin/env python3
"""Run time-resolved NCC scoring at previously selected per-plane scales."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from codeants_2pf_hcr.z_drift import FunctionalZDriftConfig, run_functional_z_drift_diagnostic


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fish-id", required=True)
    parser.add_argument("--motion-corrected-dir", required=True, type=Path)
    parser.add_argument("--anatomy-stack-path", required=True, type=Path)
    parser.add_argument("--preprocessing-metadata-path", required=True, type=Path)
    provenance = parser.add_mutually_exclusive_group(required=True)
    provenance.add_argument("--functional-reference-manifest-path", type=Path)
    provenance.add_argument(
        "--fish-dir",
        type=Path,
        help="Validate polarity and Block-0 exclusion directly from fish metadata without creating functional references.",
    )
    parser.add_argument("--scale-cache-path", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--intervals", type=int, default=6)
    parser.add_argument("--sampled-frames", type=int, default=80)
    parser.add_argument("--top-correlated-frames", type=int, default=20)
    parser.add_argument("--use-cv2", action="store_true")
    args = parser.parse_args()
    manifest = run_functional_z_drift_diagnostic(
        fish_id=args.fish_id,
        motion_corrected_dir=args.motion_corrected_dir,
        anatomy_stack_path=args.anatomy_stack_path,
        preprocessing_metadata_path=args.preprocessing_metadata_path,
        functional_reference_manifest_path=args.functional_reference_manifest_path,
        fish_dir=args.fish_dir,
        scale_cache_path=args.scale_cache_path,
        output_dir=args.output_dir,
        config=FunctionalZDriftConfig(
            intervals_per_session=args.intervals,
            sampled_frames_per_interval=args.sampled_frames,
            top_correlated_frames=args.top_correlated_frames,
            use_cv2=args.use_cv2,
        ),
    )
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
