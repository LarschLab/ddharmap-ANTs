#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path

from codeants_2pf_hcr.plots.qa import DEFAULT_DATA_ROOT, DEFAULT_FUNCTIONAL_IMAGE, build_best_plane_modality_merge_grid


DEFAULT_FISH_ID = "L396_f04"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a standalone best-plane modality merge grid in shared 2P anatomy space."
    )
    parser.add_argument("--fish-id", default=DEFAULT_FISH_ID, help=f"Fish identifier. Default: {DEFAULT_FISH_ID}")
    parser.add_argument("--data-root", default=str(DEFAULT_DATA_ROOT), help=f"Root analysis directory. Default: {DEFAULT_DATA_ROOT}")
    parser.add_argument("--output", default=None, help="Optional output image path.")
    parser.add_argument("--functional-image", default=str(DEFAULT_FUNCTIONAL_IMAGE), help="Optional functional image override.")
    parser.add_argument("--dpi", type=int, default=300, help="Output DPI. Default: 300")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    out = build_best_plane_modality_merge_grid(
        fish_id=args.fish_id,
        data_root=Path(args.data_root),
        functional_image=Path(args.functional_image),
        output=Path(args.output) if args.output else None,
        dpi=args.dpi,
    )
    print(f"[qa] saved {out}")


if __name__ == "__main__":
    main()
