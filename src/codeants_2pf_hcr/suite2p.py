"""Suite2p stage helpers for notebook cells [23a], [25], and trace consumers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import pathlib
import re
from typing import Any

import numpy as np
import pandas as pd

from .context import func_orientation_effective, func_orientation_mode
from .spatial import apply_func_orientation


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


def _load_ops_npy(path: str | Path) -> Any:
    try:
        return np.load(path, allow_pickle=True).item()
    except NotImplementedError:
        orig_windows = pathlib.WindowsPath
        orig_pure = pathlib.PureWindowsPath
        pathlib.WindowsPath = pathlib.PosixPath
        pathlib.PureWindowsPath = pathlib.PurePosixPath
        try:
            return np.load(path, allow_pickle=True).item()
        finally:
            pathlib.WindowsPath = orig_windows
            pathlib.PureWindowsPath = orig_pure


def _plane_num_from_name(name: str | Path) -> int | None:
    match = re.search(r"plane(\d+)", str(Path(name).name))
    return int(match.group(1)) if match else None


def _build_labels_from_stat(stat: np.ndarray, iscell: np.ndarray, ops: dict[str, Any]) -> tuple[np.ndarray, np.ndarray]:
    ly = int(ops.get("Ly", 0))
    lx = int(ops.get("Lx", 0))
    labels = np.zeros((ly, lx), dtype=np.uint32)
    keep = np.asarray(iscell)[:, 0].astype(bool)
    for roi_idx in np.where(keep)[0]:
        roi = stat[roi_idx]
        if isinstance(roi, dict):
            ypix = np.asarray(roi.get("ypix", []), dtype=np.int64)
            xpix = np.asarray(roi.get("xpix", []), dtype=np.int64)
            overlap = roi.get("overlap", None)
        else:
            ypix = np.asarray(roi["ypix"], dtype=np.int64)
            xpix = np.asarray(roi["xpix"], dtype=np.int64)
            dtype_names = getattr(getattr(roi, "dtype", None), "names", None)
            overlap = roi["overlap"] if dtype_names and "overlap" in dtype_names else None
        if overlap is not None:
            valid = ~np.asarray(overlap, dtype=bool)
            ypix = ypix[valid]
            xpix = xpix[valid]
        if ypix.size and xpix.size:
            labels[ypix, xpix] = roi_idx + 1
    return labels, keep


@dataclass(frozen=True)
class Suite2pStageConfig:
    use_suite2p_labels: bool = True
    plane_glob: str = "plane*"
    flip_x: bool | None = None
    dfof_baseline_pct: float = 10.0
    dfof_eps: float = 1e-6
    verbose: bool = True


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
            ops = _load_ops_npy(ops_path)
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


def load_suite2p_stage(
    *,
    plane_refs: list[dict[str, Any]],
    suite2p_root: str | Path,
    fish_id: str | None = None,
    polarity: str | None = None,
    polarity_source: str | None = None,
    config: Suite2pStageConfig | None = None,
    assert_fish_compatible: Any = None,
) -> dict[str, Any]:
    cfg = config or Suite2pStageConfig()
    default_root = Path(suite2p_root)
    resolved_root = default_root
    if callable(assert_fish_compatible):
        try:
            ok_root = assert_fish_compatible(resolved_root, key="SUITE2P_ROOT", allow_none=False, strict=False)
        except Exception:
            ok_root = True
        if not ok_root:
            if cfg.verbose:
                print(f"[Suite2p] stale SUITE2P_ROOT detected; resetting to {default_root}")
            resolved_root = default_root

    if not plane_refs:
        raise RuntimeError("plane_refs missing; run the functional reference cell first.")

    orient_mode = func_orientation_mode(polarity)
    orient_effective = func_orientation_effective(polarity)
    if cfg.verbose:
        print(
            f"[Suite2p] Base functional orientation={orient_mode} "
            f"(effective {orient_effective}, polarity={polarity}, source={polarity_source})"
        )

    plane_dirs = [path for path in resolved_root.glob(cfg.plane_glob) if path.is_dir()]
    plane_dirs = sorted(
        plane_dirs,
        key=lambda path: (_plane_num_from_name(path.name) if _plane_num_from_name(path.name) is not None else path.name),
    )

    plane_ref_map: dict[int, int] = {}
    for idx, plane_ref in enumerate(plane_refs):
        label = str(plane_ref.get("label", ""))
        match = re.search(r"plane(\d+)", label)
        if match:
            plane_ref_map[int(match.group(1))] = idx
            continue
        plane_idx = plane_ref.get("index")
        if plane_idx is not None:
            plane_idx_int = int(plane_idx)
            plane_ref_map.setdefault(plane_idx_int, idx)

    suite2p_planes: list[dict[str, Any]] = []
    suite2p_by_ref_idx: dict[int, dict[str, Any]] = {}
    func_labels: list[np.ndarray | None] = [None] * len(plane_refs)
    suite2p_sources: list[dict[str, Any]] = []

    if not plane_dirs and cfg.verbose:
        print(f"[Suite2p] No plane dirs found under {resolved_root} (glob={cfg.plane_glob})")

    for pd_idx, plane_dir in enumerate(plane_dirs):
        plane_num = _plane_num_from_name(plane_dir.name)
        ref_idx = plane_ref_map.get(plane_num)
        if ref_idx is None and pd_idx < len(plane_refs):
            ref_idx = pd_idx

        paths = {kind: _find_suite2p_file(plane_dir, kind) for kind in ("F", "Fneu", "spks", "stat", "ops", "iscell")}
        missing = [kind for kind, path in paths.items() if path is None]
        if missing:
            suite2p_sources.append(
                {
                    "plane_dir": str(plane_dir),
                    "plane_num": plane_num,
                    "ref_idx": ref_idx,
                    "status": "missing",
                    "missing": ",".join(missing),
                    "F": str(paths.get("F")) if paths.get("F") else None,
                    "Fneu": str(paths.get("Fneu")) if paths.get("Fneu") else None,
                    "spks": str(paths.get("spks")) if paths.get("spks") else None,
                    "stat": str(paths.get("stat")) if paths.get("stat") else None,
                    "ops": str(paths.get("ops")) if paths.get("ops") else None,
                    "iscell": str(paths.get("iscell")) if paths.get("iscell") else None,
                }
            )
            if cfg.verbose:
                print(f"[Suite2p] Missing {missing} in {plane_dir}; skipping")
            continue

        f_raw = np.load(paths["F"], allow_pickle=True)
        fneu = np.load(paths["Fneu"], allow_pickle=True)
        spks = np.load(paths["spks"], allow_pickle=True)
        stat = np.load(paths["stat"], allow_pickle=True)
        ops = _load_ops_npy(paths["ops"])
        iscell = np.load(paths["iscell"], allow_pickle=True)

        labels, keep = _build_labels_from_stat(stat, iscell, ops)
        labels = apply_func_orientation(labels, polarity=polarity, flip_x=True)

        flip_x = bool(cfg.flip_x) if cfg.flip_x is not None else False
        if flip_x:
            labels = labels[:, ::-1]
        flip_x_src = "manual" if cfg.flip_x is not None else "disabled"

        f_raw = np.asarray(f_raw, dtype=np.float32)
        f0 = np.percentile(f_raw, float(cfg.dfof_baseline_pct), axis=1, keepdims=True)
        dff = (f_raw - f0) / (f0 + float(cfg.dfof_eps))

        plane_info = {
            "plane_dir": plane_dir,
            "plane_num": plane_num,
            "ref_idx": ref_idx,
            "labels": labels,
            "iscell_keep": keep,
            "F": f_raw,
            "Fneu": fneu,
            "spks": spks,
            "stat": stat,
            "ops": ops,
            "iscell": iscell,
            "dff": dff,
            "flip_x": flip_x,
            "func_orient": orient_mode,
        }
        suite2p_planes.append(plane_info)

        if ref_idx is not None:
            func_labels[int(ref_idx)] = labels
            suite2p_by_ref_idx[int(ref_idx)] = plane_info
            plane_refs[int(ref_idx)]["suite2p"] = plane_info

        n_cells = int(keep.sum())
        suite2p_sources.append(
            {
                "plane_dir": str(plane_dir),
                "plane_num": plane_num,
                "ref_idx": ref_idx,
                "status": "loaded",
                "missing": None,
                "n_rois": int(len(keep)),
                "n_cells": n_cells,
                "flip_x": flip_x,
                "flip_x_src": flip_x_src,
                "orient": orient_mode,
                "F": str(paths.get("F")) if paths.get("F") else None,
                "Fneu": str(paths.get("Fneu")) if paths.get("Fneu") else None,
                "spks": str(paths.get("spks")) if paths.get("spks") else None,
                "stat": str(paths.get("stat")) if paths.get("stat") else None,
                "ops": str(paths.get("ops")) if paths.get("ops") else None,
                "iscell": str(paths.get("iscell")) if paths.get("iscell") else None,
            }
        )
        if cfg.verbose:
            print(
                f"[Suite2p] {plane_dir.name}: rois={len(keep)} cells={n_cells} "
                f"ref_idx={ref_idx} flip_x={plane_info['flip_x']} orient={plane_info['func_orient']}"
            )

    df_sum = pd.DataFrame(
        [
            {"key": "SUITE2P_ROOT", "value": str(resolved_root), "exists": resolved_root.exists()},
            {"key": "SUITE2P_PLANE_GLOB", "value": str(cfg.plane_glob), "exists": None},
            {"key": "N_PLANE_DIRS", "value": len(plane_dirs), "exists": None},
        ]
    )
    df_src = pd.DataFrame(suite2p_sources)

    if cfg.use_suite2p_labels and cfg.verbose:
        print(f"[Suite2p] Loaded labels for {sum(label is not None for label in func_labels)} plane(s) into func_labels.")

    return {
        "suite2p_root": resolved_root,
        "suite2p_planes": suite2p_planes,
        "suite2p_by_ref_idx": suite2p_by_ref_idx,
        "func_labels": func_labels,
        "suite2p_fish_id": fish_id,
        "df_sum": df_sum,
        "df_src": df_src,
    }


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
    "Suite2pStageConfig",
    "infer_frame_rate_from_detail",
    "load_suite2p_stage",
    "load_suite2p_dff_map",
]
