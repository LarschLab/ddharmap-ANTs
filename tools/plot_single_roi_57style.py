#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path

from codeants_2pf_hcr.plots.analysis import plot_single_roi_57style


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot a [57]-style full-session trace for one ROI and summarize the motion-window AUC call."
    )
    parser.add_argument("--fish-root", type=Path, required=True)
    parser.add_argument("--plane-idx", type=int, required=True)
    parser.add_argument("--func-label", type=int, required=True)
    parser.add_argument("--gene", type=str, default=None)
    parser.add_argument("--onset-delay-sec", type=float, default=10.0)
    parser.add_argument("--remove-interblock-gaps", action="store_true", default=True)
    parser.add_argument("--keep-interblock-gaps", dest="remove_interblock_gaps", action="store_false")
    parser.add_argument("--f-path", type=Path, default=None)
    parser.add_argument("--source-label", type=str, default=None)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--trial-csv", type=Path, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    out = plot_single_roi_57style(
        fish_root=args.fish_root,
        plane_idx=args.plane_idx,
        func_label=args.func_label,
        output=args.output,
        gene=args.gene,
        onset_delay_sec=args.onset_delay_sec,
        remove_interblock_gaps=args.remove_interblock_gaps,
        f_path=args.f_path,
        source_label=args.source_label,
        trial_csv=args.trial_csv,
    )
    print(f"[analysis] saved {out}")


if __name__ == "__main__":
    main()
