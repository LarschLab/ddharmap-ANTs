"""Matching helpers for ROI-centric and HCR-centric notebook stages."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any, Callable

import numpy as np
import pandas as pd
from scipy.optimize import linear_sum_assignment
from skimage.measure import regionprops_table


ArrayLike = Any


@dataclass(frozen=True)
class MatchingConfig:
    require_overlap: bool = True
    min_overlap: int = 1
    max_dist_um: float = float("inf")
    match_policy_version: str = "roi_all_vs_anat_all_candidate_gated_v1"


def gene_from_mask(path_str: str | Path | None) -> str:
    name = Path(path_str).name if path_str is not None else ""
    match = re.search(r"channel\d+_(.+?)_cp_masks", name)
    gene = match.group(1) if match else name
    return gene.replace("sst1_", "sst1.")


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


def _ensure_uint_labels(arr: ArrayLike) -> np.ndarray:
    out = np.asarray(arr)
    if out.dtype.kind == "u":
        return out.astype(np.uint32, copy=False)
    return np.asarray(out, dtype=np.uint32)


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


def _centroid_df(labels_2d: np.ndarray, y_name: str, x_name: str) -> pd.DataFrame:
    props = regionprops_table(np.asarray(labels_2d, dtype=np.int32), properties=("label", "centroid"))
    df = pd.DataFrame(props).rename(columns={"centroid-0": y_name, "centroid-1": x_name})
    if "label" not in df.columns:
        return pd.DataFrame(columns=["label", y_name, x_name])
    return df[df["label"] != 0].reset_index(drop=True)


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
    if not callable(tform_for_plane_func) or not callable(resample_labels_nn_func):
        return _ensure_uint_labels(labels_raw)
    try:
        tform = tform_for_plane_func(plane_ref)
    except Exception:
        tform = None
    if tform is None:
        return _ensure_uint_labels(labels_raw)
    warped = resample_labels_nn_func(labels_raw, tform, output_shape=anat_shape)
    return _ensure_uint_labels(warped)


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
        func_warped = _resample_func_labels(
            labels_raw,
            plane_ref,
            anat_slice.shape,
            tform_for_plane_func=tform_for_plane_func,
            resample_labels_nn_func=resample_labels_nn_func,
        )
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
        func_warped = _resample_func_labels(
            labels_raw,
            plane_ref,
            anat_slice.shape,
            tform_for_plane_func=tform_for_plane_func,
            resample_labels_nn_func=resample_labels_nn_func,
        )
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
    raw_df = candidate_df.copy()
    analysis_df = pd.DataFrame(analysis_rows).sort_values(["gene", "anat_label", "plane", "func_label"]).reset_index(drop=True)
    return status_df, raw_df, analysis_df, candidate_df, plane_meta_df


__all__ = [
    "MatchingConfig",
    "build_anat_identity_lookup_df",
    "build_functional_roi_master_df",
    "build_hcr_activity_tables",
    "compute_label_overlap",
    "gene_from_mask",
]
