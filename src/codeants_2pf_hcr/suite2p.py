"""Suite2p stage helpers for notebook cells [23a], [25], and trace consumers."""

from __future__ import annotations

from pathlib import Path
import re
from typing import Any

import numpy as np
import pandas as pd


def _find_suite2p_file(plane_dir: str | Path, kind: str) -> Path | None:
    plane_dir = Path(plane_dir)
    exact = plane_dir / f"{kind}.npy"
    if exact.exists():
        return exact
    plane_match = re.search(r"plane(\d+)", str(plane_dir.name))
    if plane_match:
        tagged_hits = sorted(plane_dir.glob(f"*plane{int(plane_match.group(1))}_{kind}.npy"))
        if tagged_hits:
            return tagged_hits[0]
    hits: list[Path] = []
    for pattern in (f"*_{kind}.npy", f"*{kind}.npy"):
        hits.extend(sorted(plane_dir.glob(pattern)))
    hits = sorted(set(hits))
    return hits[0] if hits else None


def _resolve_suite2p_plane_dir(detail_df: pd.DataFrame, plane_idx: int, suite2p_root: str | Path | None) -> Path | None:
    if "func_source" in detail_df.columns:
        srcs = (
            detail_df.loc[detail_df["plane_idx"] == int(plane_idx), "func_source"]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )
        for src in srcs:
            path = Path(src)
            if path.exists():
                return path
    if suite2p_root is not None:
        candidate = Path(suite2p_root) / f"plane{int(plane_idx)}"
        if candidate.exists():
            return candidate
    return None


def infer_frame_rate_from_detail(detail_df: pd.DataFrame, *, suite2p_root: str | Path | None = None) -> float | None:
    fs_vals: list[float] = []
    plane_vals = detail_df["plane_idx"].dropna().astype(int).unique().tolist() if "plane_idx" in detail_df.columns else []
    for plane_idx in sorted(plane_vals):
        plane_dir = _resolve_suite2p_plane_dir(detail_df, plane_idx, suite2p_root)
        if plane_dir is None:
            continue
        ops_path = _find_suite2p_file(plane_dir, "ops")
        if ops_path is None:
            continue
        try:
            ops = np.load(ops_path, allow_pickle=True).item()
        except Exception:
            continue
        if isinstance(ops, dict) and ops.get("fs") is not None:
            fs_vals.append(float(ops["fs"]))
    if not fs_vals:
        return None
    first = float(fs_vals[0])
    if any(abs(float(val) - first) > 1e-6 for val in fs_vals[1:]):
        raise RuntimeError(f"inconsistent Suite2p frame rates across planes: {fs_vals}")
    return first


def load_suite2p_dff_map(
    detail_df: pd.DataFrame,
    *,
    suite2p_root: str | Path | None = None,
    dfof_baseline_pct: float = 10.0,
    dfof_eps: float = 1e-6,
) -> dict[int, dict[str, Any]]:
    s2p_map: dict[int, dict[str, Any]] = {}
    plane_vals = detail_df["plane_idx"].dropna().astype(int).unique().tolist() if "plane_idx" in detail_df.columns else []
    for plane_idx in sorted(plane_vals):
        plane_dir = _resolve_suite2p_plane_dir(detail_df, plane_idx, suite2p_root)
        if plane_dir is None:
            continue
        f_path = _find_suite2p_file(plane_dir, "F")
        if f_path is None:
            continue
        f_raw = np.load(f_path, allow_pickle=True).astype(np.float32)
        f0 = np.percentile(f_raw, float(dfof_baseline_pct), axis=1, keepdims=True)
        dff = (f_raw - f0) / (f0 + float(dfof_eps))
        s2p_map[int(plane_idx)] = {"dff": dff, "plane_dir": str(plane_dir), "F_path": str(f_path)}
    return s2p_map


__all__ = [
    "infer_frame_rate_from_detail",
    "load_suite2p_dff_map",
]
