#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path

from codeants_2pf_hcr.plots.qa import DEFAULT_DATA_ROOT, build_round_channel_mip_grid


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a 3x3 round/channel 2P-space MIP composite from aligned confocal volumes."
    )
    parser.add_argument("--fish-id", required=True, help="Fish identifier, e.g. L396_f04.")
    parser.add_argument("--data-root", default=str(DEFAULT_DATA_ROOT), help=f"Root analysis directory. Default: {DEFAULT_DATA_ROOT}")
    parser.add_argument("--output", default=None, help="Optional output image path.")
    parser.add_argument("--z-min", type=int, default=None, help="Inclusive minimum Z slice.")
    parser.add_argument("--z-max", type=int, default=None, help="Inclusive maximum Z slice.")
    parser.add_argument("--norm-mode", choices=("robust_asinh", "legacy_percentile"), default="robust_asinh")
    parser.add_argument("--norm-black-quantile", type=float, default=10.0)
    parser.add_argument("--norm-white-quantile", type=float, default=99.5)
    parser.add_argument("--norm-gain", type=float, default=10.0)
    parser.add_argument("--norm-soft-clip", type=float, default=4.0)
    parser.add_argument("--dpi", type=int, default=300)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    out = build_round_channel_mip_grid(
        fish_id=args.fish_id,
        data_root=Path(args.data_root),
        output=Path(args.output) if args.output else None,
        z_min=args.z_min,
        z_max=args.z_max,
        norm_mode=args.norm_mode,
        norm_black_quantile=args.norm_black_quantile,
        norm_white_quantile=args.norm_white_quantile,
        norm_gain=args.norm_gain,
        norm_soft_clip=args.norm_soft_clip,
        dpi=args.dpi,
    )
    print(f"[qa] saved {out}")


if __name__ == "__main__":
    main()
