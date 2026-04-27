"""Matching helpers for single-fish notebook cells [34a], [50i], and [50]."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any, Callable

import numpy as np
import pandas as pd
from scipy.optimize import linear_sum_assignment
from scipy.spatial import cKDTree
from scipy.spatial.distance import cdist
from skimage.measure import regionprops_table
from skimage.transform import AffineTransform, SimilarityTransform, warp

from .single_fish_notebook_stages import run_single_fish_cell_50_stage


ArrayLike = Any


@dataclass(frozen=True)
class MatchingConfig:
    require_overlap: bool = True
    min_overlap: int = 1
    max_dist_um: float = float("inf")
    match_policy_version: str = "roi_all_vs_anat_all_candidate_gated_v1"


@dataclass(frozen=True)
class FunctionalAnatomyDebugConfig:
    debug_enabled: bool = True
    min_overlap: int = 1
    require_overlap: bool = True
    max_link_dist_px: float = 50.0


@dataclass(frozen=True)
class FunctionalRoiIdentityConfig:
    active_class: str = "Active neurons"
    inactive_class: str = "Low-quality traces"
    identity_none: str = "no identity assigned"
    claim_matched: str = "matched unique anatomy"
    claim_duplicate: str = "duplicate anatomy claim lost"
    claim_unmatched: str = "no anatomy claim"
    plane_unavailable: str = "plane unavailable"
    func_match_ok: str = "anatomy match"
    func_no_slot: str = "no 1-to-1 anatomy slot"
    func_no_overlap: str = "no anatomy overlap candidate"
    func_lost_overlap: str = "overlap candidate lost in 1-to-1 assignment"
    func_too_far: str = "anatomy centroid distance above threshold"
    func_no_anat: str = "no anatomy labels on plane"
    min_overlap_func_anat: int = 1
    require_overlap_func_anat: bool = True
    max_dist_func_anat: float = float("inf")
    match_policy_version: str = "roi_all_vs_anat_all_candidate_gated_v1"
    default_gene_order: tuple[str, ...] = ("sst1.1", "sst1.2", "npy", "tac3b", "pth2", "cfos", "cort")


@dataclass(frozen=True)
class HcrActivityExportConfig:
    recompute_conf_func_pairs: bool = True
    active_class: str = "Active neurons"
    inactive_class: str = "Low-quality traces"
    response_summary_responsive: str = "Responsive neurons"
    response_summary_low: str = "Low activity"
    response_summary_unavailable: str = "Response unavailable"
    response_class_unavailable: str = "response unavailable"
    hcr_activity_match_policy: str = "hcr_anat_first_local_geometry_response_v5_identified_priority"
    selection_rule: str = "hcr_matched_anat_label_then_local_geometry_prefer_responsive_then_low"
    hcr_out_of_plane: str = "out-of-plane anatomy label"
    hcr_in_plane_responsive: str = "in-plane responsive ROI"
    hcr_in_plane_low: str = "in-plane low-activity ROI"
    hcr_in_plane_unavailable: str = "in-plane response unavailable"
    hcr_in_plane_no_func: str = "in-plane no functional ROI candidate"
    min_overlap_func_anat: int = 1
    require_overlap_func_anat: bool = True
    max_dist_func_anat: float = float("inf")


def gene_from_mask(path_str: str | Path | None) -> str:
    name = Path(path_str).name if path_str is not None else ""
    match = re.search(r"channel\d+_(.+?)_cp_masks", name)
    gene = match.group(1) if match else name
    return gene.replace("sst1_", "sst1.")


def resolve_plane_transform(plane_ref: dict[str, Any] | None) -> Any:
    if not isinstance(plane_ref, dict):
        return None
    for key in ("tform", "func_to_anat_tform", "transform", "affine_tform"):
        tform = plane_ref.get(key)
        if tform is not None:
            return tform
    try:
        ncc_xy = plane_ref.get("ncc_xy")
        if isinstance(ncc_xy, dict) and "x0" in ncc_xy and "y0" in ncc_xy:
            return SimilarityTransform(translation=(int(ncc_xy["x0"]), int(ncc_xy["y0"])))
    except Exception:
        pass
    return None


def compute_centroids(mask: ArrayLike) -> pd.DataFrame:
    props = regionprops_table(np.asarray(mask), properties=("label", "centroid"))
    df = pd.DataFrame(props)
    df = df.rename(columns={"centroid-0": "z", "centroid-1": "y", "centroid-2": "x"})
    if "label" not in df.columns:
        return pd.DataFrame(columns=["label", "z", "y", "x"])
    return df[df["label"] != 0].reset_index(drop=True)


def idx_to_um(df: pd.DataFrame, vox: dict[str, Any]) -> np.ndarray:
    dz = float(vox["dz"])
    dy = float(vox["dy"])
    dx = float(vox["dx"])
    return np.column_stack([df["z"].to_numpy() * dz, df["y"].to_numpy() * dy, df["x"].to_numpy() * dx])


def nearest_neighbor_match(points_src_um: ArrayLike, points_dst_um: ArrayLike) -> tuple[np.ndarray, np.ndarray]:
    tree = cKDTree(np.asarray(points_dst_um))
    dists, nn = tree.query(np.asarray(points_src_um), k=1)
    return np.asarray(dists), np.asarray(nn)


def hungarian_match(
    points_src_um: ArrayLike,
    points_dst_um: ArrayLike,
    *,
    max_cost: float = float("inf"),
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    costs = cdist(np.asarray(points_src_um), np.asarray(points_dst_um))
    if np.isfinite(float(max_cost)):
        costs[costs > float(max_cost)] = float(max_cost)
    row_ind, col_ind = linear_sum_assignment(costs)
    dists = costs[row_ind, col_ind]
    return np.asarray(dists), np.asarray(col_ind), np.asarray(row_ind)


def summarize_distances(dists: ArrayLike, valid_mask: ArrayLike) -> dict[str, float | int]:
    dists_arr = np.asarray(dists)
    valid_arr = np.asarray(valid_mask, dtype=bool)
    if dists_arr.size == 0:
        return {
            "n": 0,
            "mean": 0.0,
            "median": 0.0,
            "p90": 0.0,
            "max": 0.0,
            "within_gate": 0,
            "within_gate_frac": 0.0,
        }
    return {
        "n": int(dists_arr.size),
        "mean": float(np.mean(dists_arr)),
        "median": float(np.median(dists_arr)),
        "p90": float(np.percentile(dists_arr, 90)),
        "max": float(np.max(dists_arr)),
        "within_gate": int(valid_arr.sum()),
        "within_gate_frac": float(valid_arr.mean()),
    }


def build_anat_identity_lookup_df(
    hcr_match_results: list[dict[str, Any]] | None,
    gene_order: list[str] | None = None,
    gene_from_mask_func: Callable[[str | Path | None], str] | None = None,
) -> pd.DataFrame:
    order = list(gene_order or [])
    order_map = {str(gene): idx for idx, gene in enumerate(order)}
    infer_gene = gene_from_mask_func if callable(gene_from_mask_func) else gene_from_mask

    def ordered_gene_tuple(genes: set[str]) -> tuple[str, ...]:
        clean = {str(g) for g in genes if g is not None and str(g) and str(g).lower() != "nan"}
        return tuple(sorted(clean, key=lambda gene: (order_map.get(gene, 10**6), gene)))

    anat_gene_sets: dict[int, set[str]] = {}
    for result in hcr_match_results or []:
        final_pairs = result.get("final_pairs") if isinstance(result, dict) else None
        if final_pairs is None or getattr(final_pairs, "empty", True):
            continue
        gene = str(infer_gene(result.get("mask_path", "unknown")))
        anat_series = pd.to_numeric(final_pairs.get("twoP_label", pd.Series(dtype=float)), errors="coerce").dropna().astype(int)
        for anat_label in anat_series.unique():
            anat_gene_sets.setdefault(int(anat_label), set()).add(gene)

    rows = []
    for anat_label, genes in sorted(anat_gene_sets.items()):
        gene_tuple = ordered_gene_tuple(genes)
        rows.append(
            {
                "anat_label": int(anat_label),
                "identity_label": "/".join(gene_tuple) if gene_tuple else pd.NA,
                "identity_gene_count": int(len(gene_tuple)),
                "identity_genes": list(gene_tuple),
            }
        )
    return pd.DataFrame(rows)


def build_hcr_mask_fate_df(
    hcr_match_results: list[dict[str, Any]] | None,
    *,
    gene_from_mask_func: Callable[[str | Path | None], str] | None = None,
    reject_reason_far: str = "too far / no overlap anatomy",
    reject_reason_iou: str = "1-to-1 anatomy relation, IoU below threshold",
) -> pd.DataFrame:
    infer_gene = gene_from_mask_func if callable(gene_from_mask_func) else gene_from_mask
    rows: list[dict[str, Any]] = []

    def _pick_candidate(df: pd.DataFrame) -> pd.Series | None:
        if df.empty:
            return None
        work = df.copy()
        if "distance_um" in work.columns:
            work["distance_um"] = pd.to_numeric(work["distance_um"], errors="coerce")
            work = work.sort_values(["distance_um"], ascending=[True], na_position="last")
        return work.iloc[0]

    for result in hcr_match_results or []:
        if not isinstance(result, dict):
            continue
        mask_path = result.get("mask_path")
        mask_name = Path(str(mask_path)).name if mask_path is not None else ""
        gene = str(infer_gene(mask_path if mask_path is not None else "unknown"))
        matches = pd.DataFrame(result.get("matches", pd.DataFrame())).copy()
        df_conf = pd.DataFrame(result.get("df_conf", pd.DataFrame())).copy()

        conf_labels: list[int] = []
        if not df_conf.empty and "label" in df_conf.columns:
            conf_labels.extend(pd.to_numeric(df_conf["label"], errors="coerce").dropna().astype(int).tolist())
        if not matches.empty and "conf_label" in matches.columns:
            conf_labels.extend(pd.to_numeric(matches["conf_label"], errors="coerce").dropna().astype(int).tolist())
        if not conf_labels:
            continue

        if matches.empty or "conf_label" not in matches.columns:
            for conf_label in sorted(set(int(v) for v in conf_labels)):
                rows.append(
                    {
                        "gene": gene,
                        "conf_mask_name": mask_name,
                        "conf_label": int(conf_label),
                        "anat_unmatched_reason": reject_reason_far,
                        "twoP_label": pd.NA,
                        "distance_um": np.nan,
                        "within_gate": False,
                        "pair_type": pd.NA,
                        "quality": pd.NA,
                        "iou": np.nan,
                        "overlap_voxels": np.nan,
                    }
                )
            continue

        matches["conf_label"] = pd.to_numeric(matches["conf_label"], errors="coerce").astype("Int64")
        matches = matches.dropna(subset=["conf_label"]).copy()
        if "twoP_label" in matches.columns:
            matches["twoP_label"] = pd.to_numeric(matches["twoP_label"], errors="coerce").astype("Int64")
        else:
            matches["twoP_label"] = pd.Series(pd.NA, index=matches.index, dtype="Int64")
        if "distance_um" in matches.columns:
            matches["distance_um"] = pd.to_numeric(matches["distance_um"], errors="coerce")
        else:
            matches["distance_um"] = np.nan
        if "iou" in matches.columns:
            matches["iou"] = pd.to_numeric(matches["iou"], errors="coerce")
        else:
            matches["iou"] = np.nan
        if "overlap_voxels" in matches.columns:
            matches["overlap_voxels"] = pd.to_numeric(matches["overlap_voxels"], errors="coerce")
        else:
            matches["overlap_voxels"] = np.nan
        if "within_gate" in matches.columns:
            within_gate = matches["within_gate"]
            if pd.api.types.is_bool_dtype(within_gate):
                matches["within_gate"] = within_gate.fillna(False).astype(bool)
            elif pd.api.types.is_numeric_dtype(within_gate):
                matches["within_gate"] = within_gate.fillna(0).astype(float) != 0
            else:
                matches["within_gate"] = within_gate.astype(str).str.strip().str.lower().isin({"1", "true", "t", "yes", "y"})
        else:
            matches["within_gate"] = False
        if "pair_type" not in matches.columns:
            matches["pair_type"] = pd.NA
        if "quality" not in matches.columns:
            matches["quality"] = pd.NA

        for conf_label in sorted(set(int(v) for v in conf_labels)):
            sub = matches[matches["conf_label"].astype("Int64") == int(conf_label)].copy()
            good = sub[
                sub["within_gate"]
                & sub["pair_type"].astype(str).eq("1-1")
                & sub["quality"].astype(str).eq("good")
            ].copy()
            iou_bad = sub[
                sub["within_gate"]
                & sub["pair_type"].astype(str).eq("1-1")
                & ~sub["quality"].astype(str).eq("good")
            ].copy()
            chosen = _pick_candidate(good)
            reason: Any = pd.NA
            if chosen is None:
                chosen = _pick_candidate(iou_bad)
                if chosen is not None:
                    reason = reject_reason_iou
                else:
                    chosen = _pick_candidate(sub)
                    reason = reject_reason_far
            rows.append(
                {
                    "gene": gene,
                    "conf_mask_name": mask_name,
                    "conf_label": int(conf_label),
                    "anat_unmatched_reason": reason,
                    "twoP_label": chosen.get("twoP_label", pd.NA) if chosen is not None else pd.NA,
                    "distance_um": float(chosen.get("distance_um", np.nan)) if chosen is not None and pd.notna(chosen.get("distance_um", np.nan)) else np.nan,
                    "within_gate": bool(chosen.get("within_gate", False)) if chosen is not None else False,
                    "pair_type": chosen.get("pair_type", pd.NA) if chosen is not None else pd.NA,
                    "quality": chosen.get("quality", pd.NA) if chosen is not None else pd.NA,
                    "iou": float(chosen.get("iou", np.nan)) if chosen is not None and pd.notna(chosen.get("iou", np.nan)) else np.nan,
                    "overlap_voxels": float(chosen.get("overlap_voxels", np.nan)) if chosen is not None and pd.notna(chosen.get("overlap_voxels", np.nan)) else np.nan,
                }
            )

    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(
            columns=[
                "gene",
                "conf_mask_name",
                "conf_label",
                "anat_unmatched_reason",
                "twoP_label",
                "distance_um",
                "within_gate",
                "pair_type",
                "quality",
                "iou",
                "overlap_voxels",
            ]
        )
    out = out.sort_values(["gene", "conf_mask_name", "conf_label"]).reset_index(drop=True)
    out["conf_label"] = pd.to_numeric(out["conf_label"], errors="coerce").astype("Int64")
    out["twoP_label"] = pd.to_numeric(out["twoP_label"], errors="coerce").astype("Int64")
    out["within_gate"] = out["within_gate"].fillna(False).astype(bool)
    return out


def _ensure_uint_labels(arr: ArrayLike) -> np.ndarray:
    out = np.asarray(arr)
    if out.dtype.kind == "u":
        return out.astype(np.uint32, copy=False)
    return np.asarray(out, dtype=np.uint32)


def resample_labels_nn(
    labels_2d: ArrayLike,
    tform: Any | None = None,
    *,
    output_shape: tuple[int, int] | list[int] | np.ndarray,
) -> np.ndarray:
    labels = _ensure_uint_labels(labels_2d)
    if labels.ndim != 2:
        raise ValueError(f"Expected 2D label image, got shape {labels.shape!r}")
    shape = tuple(int(v) for v in tuple(output_shape))
    if len(shape) != 2:
        raise ValueError(f"Expected 2D output_shape, got {output_shape!r}")
    xform = tform if tform is not None else AffineTransform()
    warped = warp(
        labels.astype(np.float32, copy=False),
        xform.inverse,
        output_shape=shape,
        order=0,
        mode="constant",
        cval=0.0,
        preserve_range=True,
    )
    return _ensure_uint_labels(warped)


def _series_to_int_list(series: pd.Series) -> list[int]:
    vals = pd.to_numeric(series, errors="coerce").dropna().astype(int).tolist()
    return sorted(set(int(v) for v in vals))


def _as_bool_array(values: Any) -> np.ndarray:
    s = pd.Series(values)
    if pd.api.types.is_bool_dtype(s):
        return s.fillna(False).astype(bool).to_numpy()
    if pd.api.types.is_numeric_dtype(s):
        return (s.fillna(0).astype(float) != 0).to_numpy()
    return s.astype(str).str.strip().str.lower().isin({"1", "true", "t", "yes", "y"}).to_numpy()


def _bool_from_any(value: Any) -> bool:
    try:
        if pd.isna(value):
            return False
    except Exception:
        pass
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, (int, np.integer)):
        return bool(value)
    if isinstance(value, (float, np.floating)):
        return bool(np.isfinite(value) and float(value) != 0.0)
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def compute_label_overlap(
    conf_labels_2p: ArrayLike,
    twop_labels: ArrayLike,
    *,
    min_overlap_voxels: int = 1,
) -> pd.DataFrame:
    conf_arr = _ensure_uint_labels(conf_labels_2p)
    anat_arr = _ensure_uint_labels(twop_labels)
    if conf_arr.shape != anat_arr.shape:
        raise ValueError("Label volumes must share shape")
    a = conf_arr.ravel()
    b = anat_arr.ravel()
    mask = (a != 0) & (b != 0)
    if not mask.any():
        return pd.DataFrame(columns=["conf_label", "twoP_label", "overlap_voxels"], dtype=int)
    a = a[mask].astype(np.int64, copy=False)
    b = b[mask].astype(np.int64, copy=False)
    key = (a << 32) | b
    uniq, counts = np.unique(key, return_counts=True)
    conf = (uniq >> 32).astype(np.int64)
    twop = (uniq & ((1 << 32) - 1)).astype(np.int64)
    df = pd.DataFrame({"conf_label": conf, "twoP_label": twop, "overlap_voxels": counts.astype(int)})
    if int(min_overlap_voxels) > 1:
        df = df[df["overlap_voxels"] >= int(min_overlap_voxels)].reset_index(drop=True)
    return df


def _regionprops_centroids_2d(label_img: ArrayLike) -> pd.DataFrame:
    props = regionprops_table(np.asarray(label_img, dtype=np.int32), properties=("label", "centroid"))
    df = pd.DataFrame(props).rename(columns={"centroid-0": "cy", "centroid-1": "cx"})
    if "label" not in df.columns:
        return pd.DataFrame(columns=["label", "cy", "cx"])
    return df[df["label"] != 0].reset_index(drop=True)


def _centroid_df(labels_2d: np.ndarray, y_name: str, x_name: str) -> pd.DataFrame:
    df = _regionprops_centroids_2d(labels_2d).rename(columns={"cy": y_name, "cx": x_name})
    return df.loc[:, ["label", y_name, x_name]]


def harmonize_functional_labels_to_anatomy(
    labels_raw: ArrayLike,
    plane_ref: dict[str, Any] | None,
    anat_shape: tuple[int, int] | list[int] | np.ndarray,
    *,
    tform_for_plane_func: Callable[[dict[str, Any]], Any] | None = None,
    resample_labels_nn_func: Callable[..., np.ndarray] | None = None,
) -> dict[str, Any]:
    labels = _ensure_uint_labels(labels_raw)
    if labels.ndim == 3 and labels.shape[-1] in (1, 3, 4):
        labels = labels[..., 0]
    target_shape = tuple(int(v) for v in tuple(anat_shape))
    result = {
        "labels": None,
        "status": "ok",
        "status_detail": None,
        "func_shape": tuple(labels.shape),
        "anat_shape": target_shape,
        "transform_applied": False,
    }
    if labels.ndim != 2:
        result["status"] = f"func_shape_not_2D ({labels.shape})"
        return result

    try:
        if callable(tform_for_plane_func):
            tform = tform_for_plane_func(plane_ref or {})
        else:
            tform = resolve_plane_transform(plane_ref)
    except Exception as exc:
        result["status"] = f"transform_resolve_failed ({exc})"
        return result

    resampler = resample_labels_nn_func if callable(resample_labels_nn_func) else resample_labels_nn
    needs_resample = (tform is not None) or (tuple(labels.shape) != target_shape)
    if not needs_resample:
        result["labels"] = labels
        return result

    try:
        warped = resampler(labels, tform if tform is not None else AffineTransform(), output_shape=target_shape)
    except Exception as exc:
        result["status"] = f"resample_failed ({exc})"
        return result

    warped = _ensure_uint_labels(warped)
    result["transform_applied"] = tform is not None
    result["func_shape"] = tuple(warped.shape)
    if tuple(warped.shape) != target_shape:
        result["status"] = f"shape_mismatch ({tuple(warped.shape)} vs {target_shape})"
        return result
    result["labels"] = warped
    return result


def _resolve_best_z(plane_ref: dict[str, Any]) -> int:
    try:
        return int(plane_ref.get("best_z", -1))
    except Exception:
        return -1


def _keep_mask(plane_data: dict[str, Any]) -> np.ndarray:
    keep = plane_data.get("iscell_keep")
    if keep is None:
        iscell = plane_data.get("iscell")
        if iscell is None:
            return np.zeros(0, dtype=bool)
        keep = np.asarray(iscell)[:, 0]
    return _as_bool_array(keep).astype(bool, copy=False)


def _build_labels_from_indices(plane_data: dict[str, Any], roi_indices: np.ndarray) -> np.ndarray:
    stat = plane_data.get("stat")
    ops = plane_data.get("ops") or {}
    if stat is None:
        return np.zeros((0, 0), dtype=np.uint32)
    ly = int(ops.get("Ly", 0))
    lx = int(ops.get("Lx", 0))
    labels = np.zeros((ly, lx), dtype=np.uint32)
    for roi_idx in np.asarray(roi_indices, dtype=int):
        roi = stat[int(roi_idx)]
        if isinstance(roi, dict):
            ypix = np.asarray(roi.get("ypix", []), dtype=np.int64)
            xpix = np.asarray(roi.get("xpix", []), dtype=np.int64)
            overlap = roi.get("overlap")
        else:
            ypix = np.asarray(roi["ypix"], dtype=np.int64)
            xpix = np.asarray(roi["xpix"], dtype=np.int64)
            dtype_names = getattr(getattr(roi, "dtype", None), "names", None)
            overlap = roi["overlap"] if dtype_names and "overlap" in dtype_names else None
        if overlap is not None:
            keep = ~np.asarray(overlap, dtype=bool)
            ypix = ypix[keep]
            xpix = xpix[keep]
        if ypix.size and xpix.size:
            labels[ypix, xpix] = int(roi_idx) + 1
    return labels


def _orient_labels(
    arr: np.ndarray,
    plane_data: dict[str, Any],
    *,
    apply_func_orientation_func: Callable[[np.ndarray], np.ndarray] | None = None,
) -> np.ndarray:
    out = np.asarray(arr)
    if callable(apply_func_orientation_func):
        out = np.asarray(apply_func_orientation_func(out))
    if bool(plane_data.get("flip_x", False)):
        out = out[:, ::-1]
    return _ensure_uint_labels(out)


def _resample_func_labels(
    labels_raw: np.ndarray,
    plane_ref: dict[str, Any],
    anat_shape: tuple[int, int],
    *,
    tform_for_plane_func: Callable[[dict[str, Any]], Any] | None = None,
    resample_labels_nn_func: Callable[..., np.ndarray] | None = None,
) -> np.ndarray:
    result = harmonize_functional_labels_to_anatomy(
        labels_raw,
        plane_ref,
        anat_shape,
        tform_for_plane_func=tform_for_plane_func,
        resample_labels_nn_func=resample_labels_nn_func,
    )
    labels = result.get("labels")
    if labels is None:
        raise RuntimeError(str(result.get("status", "resample_failed")))
    return _ensure_uint_labels(labels)


def _pair_df_for_plane(
    func_warped: np.ndarray,
    anat_slice: np.ndarray,
    fdf: pd.DataFrame,
    adf: pd.DataFrame,
    *,
    dx_um: float,
    dy_um: float,
    require_overlap: bool,
    min_overlap: int,
    max_dist_um: float,
) -> tuple[pd.DataFrame, dict[int, int], dict[int, int]]:
    overlap_df = compute_label_overlap(func_warped, anat_slice, min_overlap_voxels=1)
    if overlap_df.empty:
        overlap_df = pd.DataFrame(columns=["func_label", "anat_label", "overlap_px"])
    else:
        overlap_df = overlap_df.rename(
            columns={"conf_label": "func_label", "twoP_label": "anat_label", "overlap_voxels": "overlap_px"}
        )
        overlap_df["func_label"] = pd.to_numeric(overlap_df["func_label"], errors="coerce").astype("Int64")
        overlap_df["anat_label"] = pd.to_numeric(overlap_df["anat_label"], errors="coerce").astype("Int64")
        overlap_df["overlap_px"] = pd.to_numeric(overlap_df["overlap_px"], errors="coerce").astype("Int64")
        overlap_df = overlap_df.dropna().astype({"func_label": int, "anat_label": int, "overlap_px": int})

    overlap_any_count = overlap_df.groupby("func_label")["anat_label"].nunique().astype(int).to_dict() if not overlap_df.empty else {}
    overlap_valid_df = overlap_df.copy()
    if require_overlap:
        overlap_valid_df = overlap_valid_df[overlap_valid_df["overlap_px"] >= int(min_overlap)].copy()
    overlap_valid_count = (
        overlap_valid_df.groupby("func_label")["anat_label"].nunique().astype(int).to_dict() if not overlap_valid_df.empty else {}
    )

    if require_overlap:
        pair_df = overlap_valid_df.copy()
        if not pair_df.empty:
            pair_df = pair_df.merge(
                fdf[["label", "anat_cx", "anat_cy", "raw_cx", "raw_cy", "roi_idx"]],
                left_on="func_label",
                right_on="label",
                how="left",
            ).drop(columns=["label"])
            pair_df = pair_df.merge(
                adf[["label", "anat_label_cx", "anat_label_cy"]],
                left_on="anat_label",
                right_on="label",
                how="left",
            ).drop(columns=["label"])
        else:
            pair_df = pd.DataFrame(columns=["func_label", "anat_label", "overlap_px"])
    else:
        fdf_tmp = fdf[["label", "anat_cx", "anat_cy", "raw_cx", "raw_cy", "roi_idx"]].rename(columns={"label": "func_label"})
        adf_tmp = adf[["label", "anat_label_cx", "anat_label_cy"]].rename(columns={"label": "anat_label"})
        fdf_tmp["__k"] = 1
        adf_tmp["__k"] = 1
        pair_df = fdf_tmp.merge(adf_tmp, on="__k", how="inner").drop(columns="__k")
        pair_df = pair_df.merge(overlap_df, on=["func_label", "anat_label"], how="left")
        pair_df["overlap_px"] = pd.to_numeric(pair_df.get("overlap_px", 0), errors="coerce").fillna(0).astype(int)

    if pair_df.empty:
        pair_df = pd.DataFrame(columns=["func_label", "anat_label", "overlap_px", "dist_um"])
        return pair_df, overlap_any_count, overlap_valid_count

    dx = (pd.to_numeric(pair_df["anat_cx"], errors="coerce") - pd.to_numeric(pair_df["anat_label_cx"], errors="coerce")) * float(dx_um)
    dy = (pd.to_numeric(pair_df["anat_cy"], errors="coerce") - pd.to_numeric(pair_df["anat_label_cy"], errors="coerce")) * float(dy_um)
    pair_df["dist_um"] = np.sqrt(dx * dx + dy * dy)

    if np.isfinite(float(max_dist_um)) and float(max_dist_um) > 0:
        pair_df = pair_df[pd.to_numeric(pair_df["dist_um"], errors="coerce") <= float(max_dist_um)].copy()
    return pair_df.reset_index(drop=True), overlap_any_count, overlap_valid_count


def build_plane_centroid_matches(
    func_labels: ArrayLike,
    anat_slice: ArrayLike,
    *,
    plane_ref: dict[str, Any] | None = None,
    vox_x: float = 1.0,
    vox_y: float = 1.0,
    max_link_dist_px: float = 50.0,
    require_overlap: bool = True,
    min_overlap: int = 1,
    tform_for_plane_func: Callable[[dict[str, Any]], Any] | None = None,
    resample_labels_nn_func: Callable[..., np.ndarray] | None = None,
) -> dict[str, Any]:
    anat_labels = _ensure_uint_labels(anat_slice)
    harmonized = harmonize_functional_labels_to_anatomy(
        func_labels,
        plane_ref,
        anat_labels.shape,
        tform_for_plane_func=tform_for_plane_func,
        resample_labels_nn_func=resample_labels_nn_func,
    )
    func_warped = harmonized.get("labels")
    result = {
        "status": harmonized.get("status", "ok"),
        "func_warped": func_warped,
        "links_df": pd.DataFrame(
            columns=["fx_anat_px", "fy_anat_px", "ax_px", "ay_px", "dist_px", "dist_um", "func_label", "anat_label", "overlap_px"]
        ),
        "n_func": 0,
        "n_anat": 0,
        "pairs_raw": 0,
        "pairs_keep_dist": 0,
        "pairs_overlap_gt0": 0,
        "pairs_final": 0,
    }
    if func_warped is None:
        return result

    fdf = _regionprops_centroids_2d(func_warped)
    adf = _regionprops_centroids_2d(anat_labels)
    result["n_func"] = int(len(fdf))
    result["n_anat"] = int(len(adf))
    if fdf.empty or adf.empty:
        result["status"] = f"empty_labels (func={len(fdf)}, anat={len(adf)})"
        return result

    fpts = fdf[["cx", "cy"]].to_numpy()
    apts = adf[["cx", "cy"]].to_numpy()
    dists = np.sqrt(((fpts[:, None, :] - apts[None, :, :]) ** 2).sum(axis=2))
    row_ind, col_ind = linear_sum_assignment(dists)
    result["pairs_raw"] = int(len(row_ind))
    keep = dists[row_ind, col_ind] <= float(max_link_dist_px)
    row_ind = row_ind[keep]
    col_ind = col_ind[keep]
    result["pairs_keep_dist"] = int(len(row_ind))

    links = []
    for r_idx, c_idx in zip(row_ind, col_ind):
        fxp = float(fpts[r_idx, 0])
        fyp = float(fpts[r_idx, 1])
        axp = float(apts[c_idx, 0])
        ayp = float(apts[c_idx, 1])
        dx_um = (fxp - axp) * float(vox_x)
        dy_um = (fyp - ayp) * float(vox_y)
        links.append(
            {
                "fx_anat_px": fxp,
                "fy_anat_px": fyp,
                "ax_px": axp,
                "ay_px": ayp,
                "dist_px": float(dists[r_idx, c_idx]),
                "dist_um": float(np.sqrt(dx_um * dx_um + dy_um * dy_um)),
                "func_label": int(fdf.iloc[r_idx]["label"]),
                "anat_label": int(adf.iloc[c_idx]["label"]),
            }
        )
    links_df = pd.DataFrame(links)

    overlap_df = compute_label_overlap(func_warped, anat_labels, min_overlap_voxels=1)
    overlap_df = (
        overlap_df.rename(columns={"conf_label": "func_label", "twoP_label": "anat_label", "overlap_voxels": "overlap_px"})
        if not overlap_df.empty
        else overlap_df
    )
    if links_df.empty:
        result["pairs_overlap_gt0"] = 0
        result["pairs_final"] = 0
        result["links_df"] = result["links_df"].iloc[0:0].copy()
        return result

    links_df = (
        links_df.merge(overlap_df, on=["func_label", "anat_label"], how="left")
        if not overlap_df.empty
        else links_df.assign(overlap_px=0)
    )
    links_df["overlap_px"] = links_df["overlap_px"].fillna(0).astype(int)
    result["pairs_overlap_gt0"] = int((links_df["overlap_px"] > 0).sum())
    if require_overlap:
        links_df = links_df[links_df["overlap_px"] >= int(min_overlap)].reset_index(drop=True)
    result["pairs_final"] = int(len(links_df))
    result["links_df"] = links_df
    return result


def build_functional_anatomy_debug_df(
    plane_refs: list[dict[str, Any]],
    anat_labels_all: ArrayLike,
    *,
    load_func_labels_for_plane_func: Callable[[int], tuple[ArrayLike | None, str | None, str | None] | tuple[ArrayLike | None, str | None]] | None,
    vox_x: float = 1.0,
    vox_y: float = 1.0,
    max_link_dist_px: float = 50.0,
    require_overlap: bool = True,
    min_overlap: int = 1,
    tform_for_plane_func: Callable[[dict[str, Any]], Any] | None = None,
    resample_labels_nn_func: Callable[..., np.ndarray] | None = None,
) -> pd.DataFrame:
    anat_all = _ensure_uint_labels(anat_labels_all)
    z_size = int(anat_all.shape[0]) if anat_all.ndim == 3 else 1
    rows: list[dict[str, Any]] = []
    for p_idx, plane_ref in enumerate(plane_refs or []):
        plane_label = str(plane_ref.get("label", f"plane{p_idx}"))
        best_z = _resolve_best_z(plane_ref)
        row = {
            "plane_idx": int(p_idx),
            "plane": plane_label,
            "best_z": int(best_z),
            "n_func": np.nan,
            "n_anat": np.nan,
            "pairs_raw": np.nan,
            "pairs_keep_dist": np.nan,
            "pairs_overlap_gt0": np.nan,
            "pairs_final": np.nan,
            "func_source": None,
            "status": "ok",
        }
        if best_z < 0 or best_z >= z_size:
            row["status"] = f"best_z_out_of_bounds ({best_z})"
            rows.append(row)
            continue
        loaded = load_func_labels_for_plane_func(int(p_idx)) if callable(load_func_labels_for_plane_func) else (None, None, None)
        if isinstance(loaded, tuple) and len(loaded) == 3:
            func_labels, func_label_name, func_src = loaded
        elif isinstance(loaded, tuple) and len(loaded) == 2:
            func_labels, func_src = loaded
            func_label_name = None
        else:
            func_labels, func_label_name, func_src = None, None, None
        if func_labels is None:
            row["status"] = "no_functional_labels"
            rows.append(row)
            continue
        row["func_source"] = func_src if func_src is not None else func_label_name
        anat_slice = anat_all[best_z] if anat_all.ndim == 3 else anat_all
        match_result = build_plane_centroid_matches(
            func_labels,
            anat_slice,
            plane_ref=plane_ref,
            vox_x=vox_x,
            vox_y=vox_y,
            max_link_dist_px=max_link_dist_px,
            require_overlap=require_overlap,
            min_overlap=min_overlap,
            tform_for_plane_func=tform_for_plane_func,
            resample_labels_nn_func=resample_labels_nn_func,
        )
        row["n_func"] = int(match_result["n_func"])
        row["n_anat"] = int(match_result["n_anat"])
        row["pairs_raw"] = int(match_result["pairs_raw"])
        row["pairs_keep_dist"] = int(match_result["pairs_keep_dist"])
        row["pairs_overlap_gt0"] = int(match_result["pairs_overlap_gt0"])
        row["pairs_final"] = int(match_result["pairs_final"])
        row["status"] = str(match_result["status"])
        rows.append(row)
    return pd.DataFrame(
        rows,
        columns=[
            "plane_idx",
            "plane",
            "best_z",
            "n_func",
            "n_anat",
            "pairs_raw",
            "pairs_keep_dist",
            "pairs_overlap_gt0",
            "pairs_final",
            "func_source",
            "status",
        ],
    )


def _vox_xy_um(vox_anat: dict[str, Any] | None) -> tuple[float, float]:
    vox = vox_anat if isinstance(vox_anat, dict) else {}
    try:
        vox_x = float(vox.get("X", vox.get(2, vox.get("2", 1.0))))
        vox_y = float(vox.get("Y", vox.get(1, vox.get("1", 1.0))))
    except Exception:
        vox_x, vox_y = 1.0, 1.0
    return vox_x, vox_y


def build_functional_anatomy_debug_stage(
    *,
    plane_refs: list[dict[str, Any]] | None,
    anat_labels_path: str | Path | None,
    vox_anat: dict[str, Any] | None = None,
    load_func_labels_for_plane_func: Callable[[int], tuple[ArrayLike | None, str | None, str | None] | tuple[ArrayLike | None, str | None]] | None = None,
    config: FunctionalAnatomyDebugConfig | None = None,
    imread_any_func: Callable[[str | Path], np.ndarray] | None = None,
    ensure_uint_labels_func: Callable[[Any], np.ndarray] | None = None,
    tform_for_plane_func: Callable[[dict[str, Any]], Any] | None = None,
    resample_labels_nn_func: Callable[..., np.ndarray] | None = None,
) -> dict[str, Any]:
    cfg = config or FunctionalAnatomyDebugConfig()
    log_lines: list[str] = []
    debug_df = pd.DataFrame()
    bindings = {"df_f2a_debug": debug_df}
    if not bool(cfg.debug_enabled):
        log_lines.append("[34a] Set DEBUG_F2A=True to enable debug summary.")
        return {"status": "disabled", "bindings": bindings, "debug_df": debug_df, "log_lines": log_lines}
    if not plane_refs:
        log_lines.append("[34a] No plane_refs available.")
        return {"status": "missing_plane_refs", "bindings": bindings, "debug_df": debug_df, "log_lines": log_lines}
    anat_path = Path(anat_labels_path) if anat_labels_path not in (None, "", False) else None
    if anat_path is None or not anat_path.exists():
        log_lines.append("[34a] ANAT_LABELS_PATH missing; cannot debug.")
        return {"status": "missing_anatomy_labels", "bindings": bindings, "debug_df": debug_df, "log_lines": log_lines}

    ensure = ensure_uint_labels_func if callable(ensure_uint_labels_func) else _ensure_uint_labels
    if callable(imread_any_func):
        imread_local = imread_any_func
    else:
        from .spatial import imread_any as imread_local

    try:
        anat_labels_all = ensure(imread_local(anat_path))
    except Exception as exc:
        log_lines.append(f"[34a] Could not load anatomy labels: {exc}")
        return {"status": "load_error", "bindings": bindings, "debug_df": debug_df, "log_lines": log_lines}

    vox_x, vox_y = _vox_xy_um(vox_anat)
    debug_df = build_functional_anatomy_debug_df(
        list(plane_refs),
        anat_labels_all,
        load_func_labels_for_plane_func=load_func_labels_for_plane_func,
        vox_x=vox_x,
        vox_y=vox_y,
        max_link_dist_px=float(cfg.max_link_dist_px),
        require_overlap=bool(cfg.require_overlap),
        min_overlap=int(cfg.min_overlap),
        tform_for_plane_func=tform_for_plane_func,
        resample_labels_nn_func=resample_labels_nn_func,
    )
    bindings = {"df_f2a_debug": debug_df}
    if debug_df.empty:
        log_lines.append("[34a] No planes to summarize.")
        return {"status": "empty", "bindings": bindings, "debug_df": debug_df, "log_lines": log_lines}

    log_lines.append(
        "[34a] Functional↔Anatomy debug summary table "
        f"(MAX_LINK_DIST_PX={cfg.max_link_dist_px}, "
        f"REQUIRE_OVERLAP_FUNC_ANAT={bool(cfg.require_overlap)}, "
        f"MIN_OVERLAP_FUNC_ANAT={int(cfg.min_overlap)})"
    )
    return {"status": "ok", "bindings": bindings, "debug_df": debug_df, "log_lines": log_lines}


def build_functional_roi_master_df(
    suite2p_by_ref_idx: dict[int, dict[str, Any]],
    plane_refs: list[dict[str, Any]],
    anat_labels_all: ArrayLike,
    *,
    dx_um: float = 1.0,
    dy_um: float = 1.0,
    fish_id: str | None = None,
    active_class: str = "Active neurons",
    inactive_class: str = "Low-quality traces",
    require_overlap: bool = True,
    min_overlap: int = 1,
    max_dist_um: float = float("inf"),
    plane_unavailable: str = "plane unavailable",
    func_match_ok: str = "anatomy match",
    func_no_slot: str = "no 1-to-1 anatomy slot",
    func_no_overlap: str = "no anatomy overlap candidate",
    func_lost_overlap: str = "overlap candidate lost in 1-to-1 assignment",
    func_too_far: str = "anatomy centroid distance above threshold",
    func_no_anat: str = "no anatomy labels on plane",
    claim_matched: str = "matched unique anatomy",
    claim_duplicate: str = "duplicate anatomy claim lost",
    claim_unmatched: str = "no anatomy claim",
    ensure_uint_labels_func: Callable[[Any], np.ndarray] | None = None,
    apply_func_orientation_func: Callable[[np.ndarray], np.ndarray] | None = None,
    tform_for_plane_func: Callable[[dict[str, Any]], Any] | None = None,
    resample_labels_nn_func: Callable[..., np.ndarray] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    ensure = ensure_uint_labels_func if callable(ensure_uint_labels_func) else _ensure_uint_labels
    anat_labels_all = ensure(anat_labels_all)
    z_size = int(anat_labels_all.shape[0]) if getattr(anat_labels_all, "ndim", 0) == 3 else 0
    overlap_bonus = 1_000_000.0

    detail_rows: list[dict[str, Any]] = []
    plane_meta_rows: list[dict[str, Any]] = []

    for p_idx in sorted(int(k) for k in suite2p_by_ref_idx.keys()):
        plane_data = suite2p_by_ref_idx.get(int(p_idx), {})
        plane_ref = plane_refs[int(p_idx)] if 0 <= int(p_idx) < len(plane_refs) else {}
        plane_label = str(plane_ref.get("label", f"plane{p_idx}"))
        best_z = _resolve_best_z(plane_ref)
        keep = _keep_mask(plane_data)
        roi_indices = np.arange(int(keep.size), dtype=int)
        func_source = plane_data.get("plane_dir")

        labels_raw = _build_labels_from_indices(plane_data, roi_indices)
        labels_raw = _orient_labels(labels_raw, plane_data, apply_func_orientation_func=apply_func_orientation_func)
        raw_df = _centroid_df(labels_raw, "raw_cy", "raw_cx")
        if not raw_df.empty:
            raw_df["roi_idx"] = raw_df["label"].astype(int) - 1

        base_template = {
            "fish_id": fish_id,
            "plane": plane_label,
            "plane_idx": int(p_idx),
            "best_z": best_z if best_z >= 0 else pd.NA,
            "func_source": str(func_source) if func_source is not None else None,
        }

        if best_z < 0 or best_z >= z_size:
            plane_meta_rows.append(
                {
                    "plane": plane_label,
                    "plane_idx": int(p_idx),
                    "best_z": plane_ref.get("best_z", np.nan),
                    "n_rois_requested": int(len(roi_indices)),
                    "n_rois_rendered": int(len(raw_df)),
                    "n_anat_total": np.nan,
                    "status": f"best_z_out_of_bounds ({plane_ref.get('best_z', np.nan)})",
                    "func_source": func_source,
                }
            )
            for rr in raw_df.itertuples(index=False):
                func_label = int(rr.label)
                is_active = bool(keep[func_label - 1]) if 0 < func_label <= len(keep) else False
                detail_rows.append(
                    {
                        **base_template,
                        "activity_class": active_class if is_active else inactive_class,
                        "is_active": is_active,
                        "func_label": func_label,
                        "roi_idx": int(rr.roi_idx),
                        "centroid_x_func": float(getattr(rr, "raw_cx", np.nan)),
                        "centroid_y_func": float(getattr(rr, "raw_cy", np.nan)),
                        "centroid_x_anat": np.nan,
                        "centroid_y_anat": np.nan,
                        "selected_anat_label": pd.NA,
                        "selected_dist_um": np.nan,
                        "selected_overlap_px": np.nan,
                        "n_overlap_candidates_any": 0,
                        "n_overlap_candidates_valid": 0,
                        "matched_anat_plane": False,
                        "plane_match_outcome": plane_unavailable,
                        "claim_outcome": claim_unmatched,
                        "has_unique_anat_match": False,
                        "anat_label": pd.NA,
                    }
                )
            continue

        anat_slice = ensure(anat_labels_all[best_z])
        try:
            func_warped = _resample_func_labels(
                labels_raw,
                plane_ref,
                anat_slice.shape,
                tform_for_plane_func=tform_for_plane_func,
                resample_labels_nn_func=resample_labels_nn_func,
            )
        except Exception as exc:
            plane_meta_rows.append(
                {
                    "plane": plane_label,
                    "plane_idx": int(p_idx),
                    "best_z": int(best_z),
                    "n_rois_requested": int(len(roi_indices)),
                    "n_rois_rendered": int(len(raw_df)),
                    "n_anat_total": int(np.count_nonzero(np.unique(anat_slice))),
                    "status": str(exc),
                    "func_source": func_source,
                }
            )
            continue
        fdf = _centroid_df(func_warped, "anat_cy", "anat_cx")
        if not fdf.empty and not raw_df.empty:
            fdf = fdf.merge(raw_df[["label", "raw_cx", "raw_cy", "roi_idx"]], on="label", how="left")
        adf = _centroid_df(anat_slice, "anat_label_cy", "anat_label_cx")
        n_anat = int(len(adf))
        plane_meta_rows.append(
            {
                "plane": plane_label,
                "plane_idx": int(p_idx),
                "best_z": int(best_z),
                "n_rois_requested": int(len(roi_indices)),
                "n_rois_rendered": int(len(fdf)),
                "n_anat_total": n_anat,
                "status": "no_anatomy_labels_on_plane" if n_anat == 0 else ("no_nonzero_functional_labels_after_warp" if fdf.empty else "ok"),
                "func_source": func_source,
            }
        )
        if fdf.empty:
            continue

        pair_df, overlap_any_count, overlap_valid_count = _pair_df_for_plane(
            func_warped,
            anat_slice,
            fdf,
            adf,
            dx_um=float(dx_um),
            dy_um=float(dy_um),
            require_overlap=bool(require_overlap),
            min_overlap=int(min_overlap),
            max_dist_um=float(max_dist_um),
        )

        func_labels = fdf["label"].astype(int).tolist()
        assigned_map: dict[int, int] = {}
        assigned_pair_lookup: dict[tuple[int, int], pd.Series] = {}
        if (not pair_df.empty) and n_anat > 0:
            anat_labels = adf["label"].astype(int).tolist()
            func_index = {label: idx for idx, label in enumerate(func_labels)}
            anat_index = {label: idx for idx, label in enumerate(anat_labels)}
            dummy_cols = len(func_labels)
            cost = np.zeros((len(func_labels), len(anat_labels) + dummy_cols), dtype=float)
            cost[:, : len(anat_labels)] = 1e12
            pair_lookup = {(int(r.func_label), int(r.anat_label)): r for r in pair_df.itertuples(index=False)}
            for (func_label, anat_label), row in pair_lookup.items():
                score = float(row.dist_um) - overlap_bonus * float(row.overlap_px)
                cost[func_index[func_label], anat_index[anat_label]] = score
            row_ind, col_ind = linear_sum_assignment(cost)
            for r_idx, c_idx in zip(row_ind.tolist(), col_ind.tolist()):
                if c_idx >= len(anat_labels):
                    continue
                func_label = func_labels[r_idx]
                anat_label = anat_labels[c_idx]
                if (func_label, anat_label) in pair_lookup:
                    assigned_map[func_label] = anat_label
                    assigned_pair_lookup[(func_label, anat_label)] = pair_lookup[(func_label, anat_label)]

        for rr in fdf.itertuples(index=False):
            func_label = int(rr.label)
            roi_idx = int(getattr(rr, "roi_idx", func_label - 1))
            is_active = bool(keep[func_label - 1]) if 0 < func_label <= len(keep) else False
            row = {
                **base_template,
                "activity_class": active_class if is_active else inactive_class,
                "is_active": is_active,
                "func_label": func_label,
                "roi_idx": roi_idx,
                "centroid_x_func": float(getattr(rr, "raw_cx", np.nan)),
                "centroid_y_func": float(getattr(rr, "raw_cy", np.nan)),
                "centroid_x_anat": np.nan,
                "centroid_y_anat": np.nan,
                "selected_anat_label": pd.NA,
                "selected_dist_um": np.nan,
                "selected_overlap_px": np.nan,
                "n_overlap_candidates_any": int(overlap_any_count.get(func_label, 0)),
                "n_overlap_candidates_valid": int(overlap_valid_count.get(func_label, 0)),
                "matched_anat_plane": n_anat > 0,
                "plane_match_outcome": func_no_anat if n_anat == 0 else func_no_overlap,
                "claim_outcome": claim_unmatched,
                "has_unique_anat_match": False,
                "anat_label": pd.NA,
            }
            if func_label in assigned_map:
                anat_label = int(assigned_map[func_label])
                pair_row = assigned_pair_lookup[(func_label, anat_label)]
                anat_row = adf.loc[adf["label"].astype(int) == anat_label].iloc[0]
                row["centroid_x_anat"] = float(getattr(anat_row, "anat_label_cx", np.nan))
                row["centroid_y_anat"] = float(getattr(anat_row, "anat_label_cy", np.nan))
                row["selected_anat_label"] = anat_label
                row["selected_dist_um"] = float(pair_row.dist_um)
                row["selected_overlap_px"] = float(pair_row.overlap_px)
                row["plane_match_outcome"] = func_match_ok
                row["claim_outcome"] = claim_matched
                row["has_unique_anat_match"] = True
                row["anat_label"] = anat_label
            else:
                has_any = int(overlap_any_count.get(func_label, 0)) > 0
                has_valid = int(overlap_valid_count.get(func_label, 0)) > 0
                if n_anat == 0:
                    row["plane_match_outcome"] = func_no_anat
                elif not has_any:
                    row["plane_match_outcome"] = func_no_overlap
                elif not has_valid:
                    row["plane_match_outcome"] = func_too_far if np.isfinite(float(max_dist_um)) else func_no_slot
                else:
                    row["plane_match_outcome"] = func_lost_overlap
                    row["claim_outcome"] = claim_duplicate
            detail_rows.append(row)

    detail_df = pd.DataFrame(detail_rows)
    plane_meta_df = pd.DataFrame(plane_meta_rows)
    return detail_df, plane_meta_df


def _decorate_hcr_candidates(
    candidate_df: pd.DataFrame,
    *,
    response_lookup_df: pd.DataFrame | None,
    active_class: str,
    inactive_class: str,
) -> pd.DataFrame:
    out = candidate_df.copy()
    if out.empty:
        return out
    out["response_is_active"] = out.get("is_active", False).map(_bool_from_any).astype(bool)
    out["response_class"] = np.where(out["response_is_active"], "responsive", "response unavailable")
    out["response_summary_class"] = np.where(out["response_is_active"], active_class, inactive_class)
    if response_lookup_df is None or response_lookup_df.empty:
        return out

    lookup = response_lookup_df.copy()
    lookup["plane_idx_key"] = pd.to_numeric(lookup.get("plane_idx", lookup.get("plane", np.nan)), errors="coerce").astype("Int64")
    lookup["func_label_key"] = pd.to_numeric(lookup["func_label"], errors="coerce").astype("Int64")
    keep_cols = ["plane_idx_key", "func_label_key", "response_is_active", "response_class", "response_summary_class"]
    lookup = lookup[keep_cols].drop_duplicates(subset=["plane_idx_key", "func_label_key"], keep="last")

    out["plane_idx_key"] = pd.to_numeric(out["plane_idx"], errors="coerce").astype("Int64")
    out["func_label_key"] = pd.to_numeric(out["func_label"], errors="coerce").astype("Int64")
    out = out.merge(
        lookup.rename(
            columns={
                "response_is_active": "_response_is_active",
                "response_class": "_response_class",
                "response_summary_class": "_response_summary_class",
            }
        ),
        on=["plane_idx_key", "func_label_key"],
        how="left",
    )
    out["response_is_active"] = out["_response_is_active"].where(out["_response_is_active"].notna(), out["response_is_active"])
    out["response_class"] = out["_response_class"].where(out["_response_class"].notna(), out["response_class"])
    out["response_summary_class"] = out["_response_summary_class"].where(
        out["_response_summary_class"].notna(), out["response_summary_class"]
    )
    # CSV-backed response lookups can surface bool-like strings ("True"/"False");
    # normalize explicitly so candidate ranking never treats string truthiness as True.
    out["response_is_active"] = pd.Series(_as_bool_array(out["response_is_active"]), index=out.index).astype(bool)
    return out.drop(columns=["_response_is_active", "_response_class", "_response_summary_class"], errors="ignore")


def build_hcr_activity_tables(
    suite2p_by_ref_idx: dict[int, dict[str, Any]],
    plane_refs: list[dict[str, Any]],
    anat_labels_all: ArrayLike,
    hcr_match_results: list[dict[str, Any]] | None,
    *,
    dx_um: float = 1.0,
    dy_um: float = 1.0,
    fish_id: str | None = None,
    active_class: str = "Active neurons",
    inactive_class: str = "Low-quality traces",
    require_overlap: bool = True,
    min_overlap: int = 1,
    max_dist_um: float = float("inf"),
    out_of_plane: str = "out-of-plane anatomy label",
    in_plane_active: str = "in-plane active ROI",
    in_plane_inactive: str = "in-plane low-quality-trace ROI",
    in_plane_no_func: str = "in-plane no functional ROI candidate",
    match_policy_version: str = "hcr_anat_first_local_geometry_v2",
    selection_rule: str = "hcr_matched_anat_label_then_local_geometry_first_func",
    gene_from_mask_func: Callable[[str | Path | None], str] | None = None,
    response_lookup_df: pd.DataFrame | None = None,
    ensure_uint_labels_func: Callable[[Any], np.ndarray] | None = None,
    apply_func_orientation_func: Callable[[np.ndarray], np.ndarray] | None = None,
    tform_for_plane_func: Callable[[dict[str, Any]], Any] | None = None,
    resample_labels_nn_func: Callable[..., np.ndarray] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    ensure = ensure_uint_labels_func if callable(ensure_uint_labels_func) else _ensure_uint_labels
    infer_gene = gene_from_mask_func if callable(gene_from_mask_func) else gene_from_mask
    anat_labels_all = ensure(anat_labels_all)
    z_size = int(anat_labels_all.shape[0]) if getattr(anat_labels_all, "ndim", 0) == 3 else 0

    plane_candidate_rows: list[dict[str, Any]] = []
    plane_presence_rows: list[dict[str, Any]] = []
    plane_meta_rows: list[dict[str, Any]] = []

    for p_idx in sorted(int(k) for k in suite2p_by_ref_idx.keys()):
        plane_data = suite2p_by_ref_idx.get(int(p_idx), {})
        plane_ref = plane_refs[int(p_idx)] if 0 <= int(p_idx) < len(plane_refs) else {}
        plane_label = str(plane_ref.get("label", f"plane{p_idx}"))
        best_z = _resolve_best_z(plane_ref)
        keep = _keep_mask(plane_data)
        roi_indices = np.arange(int(keep.size), dtype=int)
        func_source = plane_data.get("plane_dir")

        labels_raw = _build_labels_from_indices(plane_data, roi_indices)
        labels_raw = _orient_labels(labels_raw, plane_data, apply_func_orientation_func=apply_func_orientation_func)
        raw_df = _centroid_df(labels_raw, "raw_cy", "raw_cx")
        if not raw_df.empty:
            raw_df["roi_idx"] = raw_df["label"].astype(int) - 1

        if best_z < 0 or best_z >= z_size:
            plane_meta_rows.append(
                {
                    "plane": plane_label,
                    "plane_idx": int(p_idx),
                    "best_z": plane_ref.get("best_z", np.nan),
                    "n_rois_requested": int(len(roi_indices)),
                    "n_rois_rendered": int(len(raw_df)),
                    "n_anat_total": np.nan,
                    "status": f"best_z_out_of_bounds ({plane_ref.get('best_z', np.nan)})",
                    "func_source": func_source,
                }
            )
            continue

        anat_slice = ensure(anat_labels_all[best_z])
        try:
            func_warped = _resample_func_labels(
                labels_raw,
                plane_ref,
                anat_slice.shape,
                tform_for_plane_func=tform_for_plane_func,
                resample_labels_nn_func=resample_labels_nn_func,
            )
        except Exception as exc:
            plane_meta_rows.append(
                {
                    "plane": plane_label,
                    "plane_idx": int(p_idx),
                    "best_z": int(best_z),
                    "n_rois_requested": int(len(roi_indices)),
                    "n_rois_rendered": int(len(raw_df)),
                    "n_anat_total": int(np.count_nonzero(np.unique(anat_slice))),
                    "status": str(exc),
                    "func_source": func_source,
                }
            )
            continue
        fdf = _centroid_df(func_warped, "anat_cy", "anat_cx")
        if not fdf.empty and not raw_df.empty:
            fdf = fdf.merge(raw_df[["label", "raw_cx", "raw_cy", "roi_idx"]], on="label", how="left")
        adf = _centroid_df(anat_slice, "anat_label_cy", "anat_label_cx")
        plane_meta_rows.append(
            {
                "plane": plane_label,
                "plane_idx": int(p_idx),
                "best_z": int(best_z),
                "n_rois_requested": int(len(roi_indices)),
                "n_rois_rendered": int(len(fdf)),
                "n_anat_total": int(len(adf)),
                "status": "no_anatomy_labels_on_plane" if adf.empty else ("no_nonzero_functional_labels_after_warp" if fdf.empty else "ok"),
                "func_source": func_source,
            }
        )
        if not adf.empty:
            for anat_label in adf["label"].astype(int).tolist():
                plane_presence_rows.append({"plane": plane_label, "plane_idx": int(p_idx), "best_z": int(best_z), "anat_label": int(anat_label)})
        if fdf.empty or adf.empty:
            continue

        pair_df, _, _ = _pair_df_for_plane(
            func_warped,
            anat_slice,
            fdf,
            adf,
            dx_um=float(dx_um),
            dy_um=float(dy_um),
            require_overlap=bool(require_overlap),
            min_overlap=int(min_overlap),
            max_dist_um=float(max_dist_um),
        )
        if pair_df.empty:
            continue
        for row in pair_df.itertuples(index=False):
            func_label = int(row.func_label)
            roi_idx = int(row.roi_idx)
            is_active = bool(keep[func_label - 1]) if 0 < func_label <= len(keep) else False
            plane_candidate_rows.append(
                {
                    "fish_id": fish_id,
                    "plane": plane_label,
                    "plane_idx": int(p_idx),
                    "best_z": int(best_z),
                    "func_label": func_label,
                    "roi_idx": roi_idx,
                    "anat_label": int(row.anat_label),
                    "overlap_px_func_anat": int(row.overlap_px),
                    "dist_func_anat_um": float(row.dist_um),
                    "centroid_x_func": float(getattr(row, "raw_cx", np.nan)),
                    "centroid_y_func": float(getattr(row, "raw_cy", np.nan)),
                    "centroid_x_anat": float(getattr(row, "anat_label_cx", np.nan)),
                    "centroid_y_anat": float(getattr(row, "anat_label_cy", np.nan)),
                    "is_active": is_active,
                    "activity_class": active_class if is_active else inactive_class,
                    "func_source": str(func_source) if func_source is not None else None,
                }
            )

    plane_candidates_df = pd.DataFrame(plane_candidate_rows)
    plane_presence_df = pd.DataFrame(plane_presence_rows)
    plane_meta_df = pd.DataFrame(plane_meta_rows)

    accepted_rows: list[dict[str, Any]] = []
    for result in hcr_match_results or []:
        final_pairs = result.get("final_pairs") if isinstance(result, dict) else None
        if final_pairs is None or getattr(final_pairs, "empty", True):
            continue
        gene = str(infer_gene(result.get("mask_path", "unknown")))
        conf_mask = result.get("mask_path")
        pair_df = pd.DataFrame(final_pairs).copy()
        if pair_df.empty or "twoP_label" not in pair_df.columns:
            continue
        pair_df["anat_label"] = pd.to_numeric(pair_df["twoP_label"], errors="coerce").astype("Int64")
        if "conf_label" in pair_df.columns:
            pair_df["conf_label"] = pd.to_numeric(pair_df["conf_label"], errors="coerce").astype("Int64")
        for anat_label in sorted(pair_df["anat_label"].dropna().astype(int).unique().tolist()):
            group = pair_df[pair_df["anat_label"].astype("Int64") == anat_label].copy()
            conf_labels = _series_to_int_list(group.get("conf_label", pd.Series(dtype=float))) if "conf_label" in group.columns else []
            dist_conf = np.nan
            for col in ("dist_conf_anat_um", "dist_um", "distance_um"):
                if col in group.columns:
                    vals = pd.to_numeric(group[col], errors="coerce").dropna()
                    if not vals.empty:
                        dist_conf = float(vals.min())
                        break
            accepted_rows.append(
                {
                    "fish_id": fish_id,
                    "gene": gene,
                    "conf_mask": conf_mask,
                    "anat_label": int(anat_label),
                    "conf_labels": conf_labels,
                    "conf_label_count": int(len(conf_labels)),
                    "primary_conf_label": int(conf_labels[0]) if conf_labels else pd.NA,
                    "conf_label": int(conf_labels[0]) if conf_labels else pd.NA,
                    "dist_conf_anat_um": dist_conf,
                }
            )

    if not accepted_rows:
        empty = pd.DataFrame()
        return empty, empty, empty, empty, plane_meta_df

    candidate_rows: list[dict[str, Any]] = []
    status_rows: list[dict[str, Any]] = []
    analysis_rows: list[dict[str, Any]] = []

    plane_candidates_df = _decorate_hcr_candidates(
        plane_candidates_df,
        response_lookup_df=response_lookup_df,
        active_class=active_class,
        inactive_class=inactive_class,
    )

    for accepted in accepted_rows:
        anat_label = int(accepted["anat_label"])
        represented_planes = plane_presence_df.loc[plane_presence_df["anat_label"] == anat_label, "plane_idx"].dropna().astype(int).unique().tolist()
        group = plane_candidates_df[plane_candidates_df.get("anat_label", pd.Series(dtype=float)) == anat_label].copy()
        if not group.empty:
            group = group.sort_values(
                [
                    "response_is_active",
                    "overlap_px_func_anat",
                    "dist_func_anat_um",
                    "plane_idx",
                    "func_label",
                ],
                ascending=[False, False, True, True, True],
            ).reset_index(drop=True)
            group["candidate_rank_for_anat"] = np.arange(1, len(group) + 1, dtype=int)
        for row in group.to_dict("records"):
            candidate_rows.append({**accepted, **row})

        if not represented_planes:
            functional_status = out_of_plane
            selected = None
        elif group.empty:
            functional_status = in_plane_no_func
            selected = None
        else:
            responsive = group[group["response_is_active"].astype(bool)].copy()
            if not responsive.empty:
                selected = responsive.iloc[0]
                functional_status = in_plane_active
            else:
                selected = group.iloc[0]
                functional_status = in_plane_active if bool(selected.get("is_active", False)) else in_plane_inactive

        status_row = {
            **accepted,
            "represented_on_func_plane": bool(represented_planes),
            "represented_plane_count": int(len(represented_planes)),
            "represented_plane_indices": represented_planes,
            "functional_status": functional_status,
            "match_policy_version": match_policy_version,
            "selection_rule": selection_rule,
        }
        if selected is not None:
            status_row.update(
                {
                    "selected_plane": int(selected["plane_idx"]),
                    "selected_plane_label": selected["plane"],
                    "selected_func_label": int(selected["func_label"]),
                    "selected_roi_idx": int(selected["roi_idx"]),
                    "selected_overlap_px": float(selected["overlap_px_func_anat"]),
                    "selected_dist_um": float(selected["dist_func_anat_um"]),
                    "selected_func_source": selected.get("func_source"),
                    "is_active": bool(selected.get("is_active", False)),
                    "activity_class": selected.get("activity_class"),
                }
            )
        else:
            status_row.update(
                {
                    "selected_plane": pd.NA,
                    "selected_plane_label": None,
                    "selected_func_label": pd.NA,
                    "selected_roi_idx": pd.NA,
                    "selected_overlap_px": np.nan,
                    "selected_dist_um": np.nan,
                    "selected_func_source": None,
                    "is_active": False,
                    "activity_class": inactive_class,
                }
            )
        status_rows.append(status_row)

        if selected is not None and bool(selected.get("response_is_active", selected.get("is_active", False))):
            analysis_rows.append(
                {
                    **accepted,
                    "func_label": int(selected["func_label"]),
                    "plane": int(selected["plane_idx"]),
                    "plane_label": selected["plane"],
                    "roi_idx": int(selected["roi_idx"]),
                    "is_active": True,
                    "activity_class": selected.get("response_summary_class", selected.get("activity_class", active_class)),
                    "response_is_active": bool(selected.get("response_is_active", True)),
                    "response_class": selected.get("response_class", "responsive"),
                    "response_summary_class": selected.get("response_summary_class", active_class),
                    "suite2p_is_cell": bool(selected.get("is_active", False)),
                    "suite2p_activity_class": selected.get("activity_class"),
                    "dist_conf_anat_um": accepted.get("dist_conf_anat_um", np.nan),
                    "dist_func_anat_um": float(selected["dist_func_anat_um"]),
                    "overlap_px_func_anat": float(selected["overlap_px_func_anat"]),
                    "represented_on_func_plane": bool(represented_planes),
                    "functional_status": in_plane_active,
                    "selection_rule": selection_rule,
                    "selection_rank_gene_anat": 1,
                    "is_selected_for_analysis": True,
                    "match_policy_version": match_policy_version,
                }
            )

    status_df = pd.DataFrame(status_rows).sort_values(["gene", "anat_label"]).reset_index(drop=True)
    candidate_df = pd.DataFrame(candidate_rows)
    if not candidate_df.empty and "gene" not in candidate_df.columns:
        candidate_df["gene"] = pd.NA
    raw_df = candidate_df.copy()
    analysis_df = pd.DataFrame(analysis_rows).sort_values(["gene", "anat_label", "plane", "func_label"]).reset_index(drop=True)
    return status_df, raw_df, analysis_df, candidate_df, plane_meta_df


__all__ = [
    "FunctionalAnatomyDebugConfig",
    "FunctionalRoiIdentityConfig",
    "HcrActivityExportConfig",
    "MatchingConfig",
    "build_anat_identity_lookup_df",
    "build_hcr_mask_fate_df",
    "build_functional_anatomy_debug_df",
    "build_functional_anatomy_debug_stage",
    "build_functional_roi_master_df",
    "build_hcr_activity_tables",
    "build_plane_centroid_matches",
    "compute_centroids",
    "compute_label_overlap",
    "gene_from_mask",
    "harmonize_functional_labels_to_anatomy",
    "hungarian_match",
    "idx_to_um",
    "nearest_neighbor_match",
    "resample_labels_nn",
    "resolve_plane_transform",
    "run_single_fish_cell_50_stage",
    "summarize_distances",
]
