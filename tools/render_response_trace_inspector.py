"""Render a read-only, self-contained all-neuron response trace inspector."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from codeants_2pf_hcr.response_trace_inspector import render_response_trace_inspector


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fish-id", required=True)
    parser.add_argument("--fish-dir", required=True, type=Path)
    parser.add_argument("--score-dir", required=True, type=Path, help="score-activity-bpi registration output directory")
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--suite2p-dir", type=Path, default=None)
    args = parser.parse_args()
    meta = render_response_trace_inspector(
        fish_id=args.fish_id,
        fish_dir=args.fish_dir,
        score_dir=args.score_dir,
        output_dir=args.output_dir,
        suite2p_dir=args.suite2p_dir,
    )
    print(json.dumps({k: meta[k] for k in ("fish_id", "n_all_rois", "review_group_counts", "html", "html_bytes")}, indent=2))


if __name__ == "__main__":
    main()
