"""Render a read-only, self-contained HCR/anatomy match inspector."""

from __future__ import annotations

import argparse
from pathlib import Path

from codeants_2pf_hcr.hcr_match_inspector import render_hcr_match_inspector


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fish-id", required=True)
    parser.add_argument("--fish-dir", required=True, type=Path)
    parser.add_argument("--pipeline-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    print(render_hcr_match_inspector(
        fish_id=args.fish_id,
        fish_dir=args.fish_dir,
        pipeline_root=args.pipeline_root,
        output_path=args.output,
    ))


if __name__ == "__main__":
    main()
