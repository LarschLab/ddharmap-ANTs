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
from skimage.transform import AffineTransform, SimilarityTransform, resize, warp

from .single_fish_notebook_stages import (
    run_single_fish_cell_44_stage,
    run_single_fish_cell_46_stage,
    run_single_fish_cell_50_stage,
    run_single_fish_cell_50i_stage,
)


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
    if plane_ref.get("tform_src") == "ants_rigid_affine":
        ants_transform = plane_ref.get("ants_transform")
        if isinstance(ants_transform, dict) and ants_transform.get("type") == "ants_transformlist":
            return ants_transform
        transformlist = plane_ref.get("ants_transformlist")
        if transformlist:
            return {
                "type": "ants_transformlist",
                "method": "ants_rigid_affine",
                "transformlist": list(transformlist),
                "moving_shape": tuple(plane_ref.get("ref_scaled_shape", ())),
            }
    affine_transform = plane_ref.get("affine_transform")
    if isinstance(affine_transform, dict) and affine_transform.get("type") == "skimage_affine":
        return affine_transform
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


def _ants_point_invert_flags(transformlist: list[str | Path], *, direction: str) -> list[bool]:
    if direction not in {"moving_to_fixed", "fixed_to_moving"}:
        raise ValueError(f"Unsupported point transform direction: {direction!r}")
    invert_matrix = direction == "fixed_to_moving"
    return [bool(invert_matrix and str(path).lower().endswith(".mat")) for path in transformlist]


def transform_points_between_spaces(
    x: ArrayLike,
    y: ArrayLike,
    tform: Any | None = None,
    *,
    direction: str = "moving_to_fixed",
) -> tuple[np.ndarray, np.ndarray]:
    """Transform 2D points between functional/moving and anatomy/fixed spaces."""
    if direction not in {"moving_to_fixed", "fixed_to_moving"}:
        raise ValueError(f"Unsupported point transform direction: {direction!r}")

    x_arr = np.asarray(x, dtype=float)
    y_arr = np.asarray(y, dtype=float)
    out_shape = np.broadcast_shapes(x_arr.shape, y_arr.shape)
    x_b = np.broadcast_to(x_arr, out_shape)
    y_b = np.broadcast_to(y_arr, out_shape)
    if tform is None:
        return x_b.copy(), y_b.copy()

    xr = x_b.reshape(-1)
    yr = y_b.reshape(-1)

    if isinstance(tform, dict) and tform.get("type") == "ants_transformlist":
        transformlist = [str(path) for path in list(tform.get("transformlist", []))]
        if not transformlist:
            return x_b.copy(), y_b.copy()
        try:
            import ants
        except Exception as exc:  # pragma: no cover - depends on optional native package
            raise ImportError("ANTsPy is required to transform points with ants_rigid_affine transforms") from exc

        pts = pd.DataFrame({"x": xr.astype(float, copy=False), "y": yr.astype(float, copy=False)})
        out = ants.apply_transforms_to_points(
            2,
            pts,
            transformlist,
            whichtoinvert=_ants_point_invert_flags(transformlist, direction=direction),
        )
        return (
            pd.to_numeric(out["x"], errors="coerce").to_numpy(dtype=float).reshape(out_shape),
            pd.to_numeric(out["y"], errors="coerce").to_numpy(dtype=float).reshape(out_shape),
        )

    if isinstance(tform, dict) and tform.get("type") == "skimage_affine":
        tform = AffineTransform(matrix=np.asarray(tform.get("matrix", np.eye(3)), dtype=float))

    pts = np.column_stack([xr, yr]).astype(float, copy=False)
    xform = tform if direction == "moving_to_fixed" else getattr(tform, "inverse", None)
    if xform is None:
        return x_b.copy(), y_b.copy()
    out = np.asarray(xform(pts), dtype=float)
    if out.ndim != 2 or out.shape[1] < 2:
        raise ValueError("Point transform returned an invalid coordinate array")
    return out[:, 0].reshape(out_shape), out[:, 1].reshape(out_shape)


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


def attach_identity_to_functional_roi_geometry_df(
    geometry_df: pd.DataFrame,
    anat_identity_df: pd.DataFrame,
    *,
    passthrough_df: pd.DataFrame | None = None,
    identity_none: str = "no identity assigned",
) -> pd.DataFrame:
    """Attach anatomy identity labels to staged ROI/anatomy geometry rows."""
    out = geometry_df.copy()
    if out.empty:
        return out

    key_columns = ("plane_idx", "func_label")
    identity_columns = {
        "identity_label",
        "identity_gene_count",
        "has_identity_assigned",
        "identity_display_label",
    }

    if passthrough_df is not None and not passthrough_df.empty and all(column in out.columns for column in key_columns):
        passthrough = passthrough_df.copy()
        if all(column in passthrough.columns for column in key_columns):
            out["_plane_idx_key"] = pd.to_numeric(out["plane_idx"], errors="coerce").astype("Int64").astype(str)
            out["_func_label_key"] = pd.to_numeric(out["func_label"], errors="coerce").astype("Int64").astype(str)
            passthrough["_plane_idx_key"] = pd.to_numeric(passthrough["plane_idx"], errors="coerce").astype("Int64").astype(str)
            passthrough["_func_label_key"] = pd.to_numeric(passthrough["func_label"], errors="coerce").astype("Int64").astype(str)
            passthrough_columns = [
                column
                for column in passthrough.columns
                if column not in out.columns
                and column not in identity_columns
                and not column.endswith("_key")
            ]
            if passthrough_columns:
                out = out.merge(
                    passthrough[["_plane_idx_key", "_func_label_key", *passthrough_columns]],
                    on=["_plane_idx_key", "_func_label_key"],
                    how="left",
                )
            out = out.drop(columns=["_plane_idx_key", "_func_label_key"], errors="ignore")

    lookup = anat_identity_df.copy() if anat_identity_df is not None else pd.DataFrame()
    if not lookup.empty and "anat_label" in lookup.columns:
        lookup["_anat_label_key"] = pd.to_numeric(lookup["anat_label"], errors="coerce").astype("Int64").astype(str)
        out["_anat_label_key"] = pd.to_numeric(out.get("anat_label", pd.Series(pd.NA, index=out.index)), errors="coerce").astype("Int64").astype(str)
        keep = ["_anat_label_key", "identity_label"]
        if "identity_gene_count" in lookup.columns:
            keep.append("identity_gene_count")
        lookup = lookup[keep].drop_duplicates(subset=["_anat_label_key"], keep="last")
        out = out.merge(
            lookup.rename(
                columns={
                    "identity_label": "_identity_label",
                    "identity_gene_count": "_identity_gene_count",
                }
            ),
            on="_anat_label_key",
            how="left",
        )
        out["identity_label"] = out["_identity_label"]
        if "identity_gene_count" in out.columns or "identity_gene_count" in (passthrough_df.columns if passthrough_df is not None else ()):
            out["identity_gene_count"] = out.get("_identity_gene_count", pd.Series(pd.NA, index=out.index))
        out = out.drop(columns=["_anat_label_key", "_identity_label", "_identity_gene_count"], errors="ignore")
    else:
        out["identity_label"] = pd.NA
        if "identity_gene_count" in out.columns or "identity_gene_count" in (passthrough_df.columns if passthrough_df is not None else ()):
            out["identity_gene_count"] = pd.NA

    has_identity = out["identity_label"].notna() & out["identity_label"].astype(str).str.strip().ne("")
    out["has_identity_assigned"] = has_identity.astype(bool)
    if "identity_display_label" in out.columns or "identity_display_label" in (passthrough_df.columns if passthrough_df is not None else ()):
        out["identity_display_label"] = out["identity_label"].where(has_identity, identity_none).astype(str)

    if passthrough_df is not None and not passthrough_df.empty:
        ordered_columns = [column for column in passthrough_df.columns if column in out.columns]
        out = out.loc[:, ordered_columns]

    return out


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
    if isinstance(tform, dict) and tform.get("type") == "ants_transformlist":
        return _resample_labels_ants_nn(labels, tform, output_shape=shape)
    if isinstance(tform, dict) and tform.get("type") == "skimage_affine":
        return _resample_labels_skimage_affine_nn(labels, tform, output_shape=shape)
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


def resample_image(
    image_2d: ArrayLike,
    tform: Any | None = None,
    *,
    output_shape: tuple[int, int] | list[int] | np.ndarray,
    order: int = 1,
) -> np.ndarray:
    image = np.asarray(image_2d, dtype=np.float32)
    if image.ndim != 2:
        raise ValueError(f"Expected 2D image, got shape {image.shape!r}")
    shape = tuple(int(v) for v in tuple(output_shape))
    if len(shape) != 2:
        raise ValueError(f"Expected 2D output_shape, got {output_shape!r}")
    if isinstance(tform, dict) and tform.get("type") == "ants_transformlist":
        return _resample_image_ants(image, tform, output_shape=shape)
    if isinstance(tform, dict) and tform.get("type") == "skimage_affine":
        return _resample_image_skimage_affine(image, tform, output_shape=shape, order=order)
    xform = tform if tform is not None else AffineTransform()
    warped = warp(
        image,
        xform.inverse,
        output_shape=shape,
        order=int(order),
        mode="constant",
        cval=0.0,
        preserve_range=True,
    )
    return np.asarray(warped, dtype=np.float32)


def _skimage_affine_moving_shape(tform: dict[str, Any]) -> tuple[int, int] | None:
    moving_shape_raw = tform.get("moving_shape")
    if isinstance(moving_shape_raw, (tuple, list)) and len(moving_shape_raw) >= 2:
        return (int(moving_shape_raw[-2]), int(moving_shape_raw[-1]))
    return None


def _resample_labels_skimage_affine_nn(labels: np.ndarray, tform: dict[str, Any], *, output_shape: tuple[int, int]) -> np.ndarray:
    labels_moving = labels
    moving_shape = _skimage_affine_moving_shape(tform)
    if moving_shape is not None and tuple(labels.shape) != moving_shape:
        labels_moving = resize(
            labels.astype(np.float32, copy=False),
            moving_shape,
            order=0,
            preserve_range=True,
            anti_aliasing=False,
        ).astype(np.uint32)
    xform = AffineTransform(matrix=np.asarray(tform.get("matrix", np.eye(3)), dtype=float))
    warped = warp(
        labels_moving.astype(np.float32, copy=False),
        xform.inverse,
        output_shape=tuple(output_shape),
        order=0,
        mode="constant",
        cval=0.0,
        preserve_range=True,
    )
    return _ensure_uint_labels(warped)


def _resample_image_skimage_affine(
    image: np.ndarray,
    tform: dict[str, Any],
    *,
    output_shape: tuple[int, int],
    order: int = 1,
) -> np.ndarray:
    moving_np = np.asarray(image, dtype=np.float32)
    moving_shape = _skimage_affine_moving_shape(tform)
    if moving_shape is not None and tuple(moving_np.shape) != moving_shape:
        moving_np = resize(
            moving_np,
            moving_shape,
            order=int(order),
            preserve_range=True,
            anti_aliasing=int(order) > 0,
        ).astype(np.float32)
    xform = AffineTransform(matrix=np.asarray(tform.get("matrix", np.eye(3)), dtype=float))
    warped = warp(
        moving_np,
        xform.inverse,
        output_shape=tuple(output_shape),
        order=int(order),
        mode="constant",
        cval=0.0,
        preserve_range=True,
    )
    return np.asarray(warped, dtype=np.float32)


def _resample_labels_ants_nn(labels: np.ndarray, tform: dict[str, Any], *, output_shape: tuple[int, int]) -> np.ndarray:
    transformlist = list(tform.get("transformlist", []))
    if not transformlist:
        raise ValueError("ANTs transform dictionary is missing transformlist")
    try:
        import ants
    except Exception as exc:  # pragma: no cover - depends on optional native package
        raise ImportError("ANTsPy is required to resample labels with ants_rigid_affine transforms") from exc

    moving_shape_raw = tform.get("moving_shape")
    moving_shape: tuple[int, int] | None = None
    if isinstance(moving_shape_raw, (tuple, list)) and len(moving_shape_raw) >= 2:
        moving_shape = (int(moving_shape_raw[-2]), int(moving_shape_raw[-1]))
    labels_moving = labels
    if moving_shape is not None and tuple(labels.shape) != moving_shape:
        labels_moving = resize(
            labels.astype(np.float32, copy=False),
            moving_shape,
            order=0,
            preserve_range=True,
            anti_aliasing=False,
        ).astype(np.uint32)

    fixed = ants.from_numpy(np.zeros(tuple(output_shape), dtype=np.float32))
    moving = ants.from_numpy(labels_moving.astype(np.float32, copy=False))

    fixed_spacing = tuple(float(v) for v in tform.get("fixed_spacing", (1.0, 1.0)))
    moving_spacing = tuple(float(v) for v in tform.get("moving_spacing", fixed_spacing))
    fixed.set_spacing(fixed_spacing)
    moving.set_spacing(moving_spacing)
    fixed.set_origin(tuple(float(v) for v in tform.get("fixed_origin", (0.0, 0.0))))
    moving.set_origin(tuple(float(v) for v in tform.get("moving_origin", (0.0, 0.0))))
    fixed.set_direction(np.asarray(tform.get("fixed_direction", np.eye(2)), dtype=float))
    moving.set_direction(np.asarray(tform.get("moving_direction", np.eye(2)), dtype=float))

    warped = ants.apply_transforms(
        fixed=fixed,
        moving=moving,
        transformlist=transformlist,
        interpolator="nearestNeighbor",
    )
    return _ensure_uint_labels(np.asarray(warped.numpy()))


def _resample_image_ants(image: np.ndarray, tform: dict[str, Any], *, output_shape: tuple[int, int]) -> np.ndarray:
    transformlist = list(tform.get("transformlist", []))
    if not transformlist:
        raise ValueError("ANTs transform dictionary is missing transformlist")
    try:
        import ants
    except Exception as exc:  # pragma: no cover - depends on optional native package
        raise ImportError("ANTsPy is required to resample images with ants_rigid_affine transforms") from exc

    moving_shape_raw = tform.get("moving_shape")
    moving_shape: tuple[int, int] | None = None
    if isinstance(moving_shape_raw, (tuple, list)) and len(moving_shape_raw) >= 2:
        moving_shape = (int(moving_shape_raw[-2]), int(moving_shape_raw[-1]))
    moving_np = np.asarray(image, dtype=np.float32)
    if moving_shape is not None and tuple(moving_np.shape) != moving_shape:
        moving_np = resize(
            moving_np,
            moving_shape,
            order=1,
            preserve_range=True,
            anti_aliasing=True,
        ).astype(np.float32)

    fixed = ants.from_numpy(np.zeros(tuple(output_shape), dtype=np.float32))
    moving = ants.from_numpy(moving_np)

    fixed_spacing = tuple(float(v) for v in tform.get("fixed_spacing", (1.0, 1.0)))
    moving_spacing = tuple(float(v) for v in tform.get("moving_spacing", fixed_spacing))
    fixed.set_spacing(fixed_spacing)
    moving.set_spacing(moving_spacing)
    fixed.set_origin(tuple(float(v) for v in tform.get("fixed_origin", (0.0, 0.0))))
    moving.set_origin(tuple(float(v) for v in tform.get("moving_origin", (0.0, 0.0))))
    fixed.set_direction(np.asarray(tform.get("fixed_direction", np.eye(2)), dtype=float))
    moving.set_direction(np.asarray(tform.get("moving_direction", np.eye(2)), dtype=float))

    warped = ants.apply_transforms(
        fixed=fixed,
        moving=moving,
        transformlist=transformlist,
        interpolator="linear",
    )
    return np.asarray(warped.numpy(), dtype=np.float32)


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


def _maybe_bool_or_na(value: Any) -> bool | Any:
    try:
        if pd.isna(value):
            return pd.NA
    except Exception:
        pass
    return bool(_bool_from_any(value))


def _to_int_or_na(value: Any) -> int | Any:
    try:
        if pd.isna(value):
            return pd.NA
    except Exception:
        pass
    try:
        return int(value)
    except Exception:
        return pd.NA


def _to_float_or_nan(value: Any) -> float:
    try:
        out = float(value)
    except Exception:
        return float("nan")
    return out if np.isfinite(out) else float("nan")


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


def resolve_anatomy_label_z(plane_ref: dict[str, Any] | None, z_size: int, *, best_z: int | None = None) -> int:
    """Resolve the anatomy-label stack page corresponding to a functional best-Z plane."""
    z_count = int(z_size)
    if z_count <= 0:
        return -1
    if not isinstance(plane_ref, dict):
        plane_ref = {}
    if plane_ref.get("anat_label_z") not in (None, "", False):
        try:
            return int(plane_ref["anat_label_z"])
        except Exception:
            pass
    bz = _resolve_best_z(plane_ref) if best_z is None else int(best_z)
    mode = str(
        plane_ref.get(
            "anat_label_z_mode",
            plane_ref.get("anat_labels_z_mode", plane_ref.get("anat_label_stack_z_mode", "direct")),
        )
    ).strip().lower()
    if mode in {"reverse", "reversed", "inverted", "invert", "flipz", "flip_z"}:
        return int(z_count - 1 - bz)
    return int(bz)


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
        anat_label_z = resolve_anatomy_label_z(plane_ref, z_size, best_z=best_z)
        if anat_all.ndim == 3 and (anat_label_z < 0 or anat_label_z >= z_size):
            row["status"] = f"anat_label_z_out_of_bounds ({anat_label_z})"
            rows.append(row)
            continue
        anat_slice = anat_all[anat_label_z] if anat_all.ndim == 3 else anat_all
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

        anat_label_z = resolve_anatomy_label_z(plane_ref, z_size, best_z=best_z)
        if anat_label_z < 0 or anat_label_z >= z_size:
            plane_meta_rows.append(
                {
                    "plane": plane_label,
                    "plane_idx": int(p_idx),
                    "best_z": plane_ref.get("best_z", np.nan),
                    "n_rois_requested": int(len(roi_indices)),
                    "n_rois_rendered": int(len(raw_df)),
                    "n_anat_total": np.nan,
                    "status": f"anat_label_z_out_of_bounds ({anat_label_z})",
                    "func_source": func_source,
                }
            )
            continue

        anat_slice = ensure(anat_labels_all[anat_label_z])
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


def summarize_functional_anatomy_geometry_metrics(
    master_df: pd.DataFrame,
    *,
    method: str | None = None,
    fish_id: str | None = None,
) -> pd.DataFrame:
    """Summarize ROI->anatomy geometry metrics for one completed placement backend."""
    if master_df is None or master_df.empty:
        return pd.DataFrame(
            columns=[
                "fish_id",
                "plane_idx",
                "plane",
                "method",
                "n_rois",
                "n_unique_anat_match",
                "unique_match_frac",
                "unmatched_frac",
                "median_selected_dist_um",
                "median_selected_overlap_px",
                "n_overlap_candidates_any",
                "n_overlap_candidates_valid",
                "n_identity_assigned",
            ]
        )
    df = master_df.copy()
    if "fish_id" not in df.columns:
        df["fish_id"] = fish_id
    if fish_id is not None:
        df["fish_id"] = df["fish_id"].fillna(str(fish_id))
    if "plane_idx" not in df.columns:
        df["plane_idx"] = pd.NA
    if "plane" not in df.columns:
        df["plane"] = pd.NA
    if "has_unique_anat_match" in df.columns:
        matched = _as_bool_array(df["has_unique_anat_match"])
    else:
        matched = np.zeros(len(df), dtype=bool)
    if "has_identity_assigned" in df.columns:
        identity_assigned = _as_bool_array(df["has_identity_assigned"])
    else:
        identity_assigned = np.zeros(len(df), dtype=bool)
    df["_matched_bool"] = matched
    df["_identity_bool"] = identity_assigned

    rows: list[dict[str, Any]] = []
    group_cols = ["fish_id", "plane_idx", "plane"]
    for keys, sub in df.groupby(group_cols, dropna=False):
        fish_key, plane_idx, plane = keys
        n_rois = int(len(sub))
        n_match = int(sub["_matched_bool"].sum())
        dist = pd.to_numeric(sub.get("selected_dist_um", pd.Series(dtype=float)), errors="coerce")
        overlap = pd.to_numeric(sub.get("selected_overlap_px", pd.Series(dtype=float)), errors="coerce")
        any_candidates = pd.to_numeric(sub.get("n_overlap_candidates_any", pd.Series(dtype=float)), errors="coerce").fillna(0)
        valid_candidates = pd.to_numeric(sub.get("n_overlap_candidates_valid", pd.Series(dtype=float)), errors="coerce").fillna(0)
        rows.append(
            {
                "fish_id": fish_key,
                "plane_idx": plane_idx,
                "plane": plane,
                "method": method,
                "n_rois": n_rois,
                "n_unique_anat_match": n_match,
                "unique_match_frac": float(n_match / n_rois) if n_rois else np.nan,
                "unmatched_frac": float((n_rois - n_match) / n_rois) if n_rois else np.nan,
                "median_selected_dist_um": float(dist.dropna().median()) if not dist.dropna().empty else np.nan,
                "median_selected_overlap_px": float(overlap.dropna().median()) if not overlap.dropna().empty else np.nan,
                "n_overlap_candidates_any": int(any_candidates.sum()),
                "n_overlap_candidates_valid": int(valid_candidates.sum()),
                "n_identity_assigned": int(sub["_identity_bool"].sum()),
            }
        )
    return pd.DataFrame(rows)


def annotate_session_anat_label_duplicates(
    master_df: pd.DataFrame,
    *,
    fish_col: str = "fish_id",
    session_col: str = "session_label",
    anat_col: str = "selected_anat_label",
    match_col: str = "has_unique_anat_match",
    overlap_col: str = "selected_overlap_px",
    dist_col: str = "selected_dist_um",
    plane_col: str = "plane_idx",
    func_col: str = "func_label",
) -> pd.DataFrame:
    """Flag same-anatomy-label ROI duplicates within fish/session after geometry matching."""
    if master_df is None or master_df.empty:
        out = pd.DataFrame() if master_df is None else master_df.copy()
        out["dedup_group_key"] = pd.Series(dtype=object)
        out["dedup_rank_within_session_anat"] = pd.Series(dtype="Int64")
        out["is_multiplane_duplicate_roi"] = pd.Series(dtype=bool)
        out["is_retained_after_multiplane_dedup"] = pd.Series(dtype=bool)
        out["dedup_outcome"] = pd.Series(dtype=object)
        return out

    out = master_df.copy()
    if fish_col not in out.columns:
        out[fish_col] = pd.NA
    if session_col not in out.columns:
        out[session_col] = "unknown"
    if anat_col not in out.columns:
        out[anat_col] = pd.NA
    if match_col in out.columns:
        matched = _as_bool_array(out[match_col])
    else:
        matched = pd.to_numeric(out[anat_col], errors="coerce").notna().to_numpy(dtype=bool)

    anat_key = pd.to_numeric(out[anat_col], errors="coerce").astype("Int64")
    fish_key = out[fish_col].astype("string").fillna("unknown")
    session_key = out[session_col].astype("string").fillna("unknown")
    eligible = pd.Series(matched, index=out.index) & anat_key.notna()

    out["dedup_group_key"] = pd.NA
    out.loc[eligible, "dedup_group_key"] = (
        fish_key[eligible].astype(str)
        + "|"
        + session_key[eligible].astype(str)
        + "|anat:"
        + anat_key[eligible].astype(str)
    )
    out["dedup_rank_within_session_anat"] = pd.Series(pd.NA, index=out.index, dtype="Int64")
    out["is_multiplane_duplicate_roi"] = False
    out["is_retained_after_multiplane_dedup"] = True
    out["dedup_outcome"] = "not eligible"
    out.loc[eligible, "dedup_outcome"] = "retained unique"

    if not bool(eligible.any()):
        return out

    work = pd.DataFrame(index=out.index[eligible])
    work["_group"] = out.loc[eligible, "dedup_group_key"].astype(str)
    work["_overlap"] = pd.to_numeric(out.loc[eligible, overlap_col], errors="coerce") if overlap_col in out.columns else np.nan
    work["_dist"] = pd.to_numeric(out.loc[eligible, dist_col], errors="coerce") if dist_col in out.columns else np.nan
    work["_plane"] = pd.to_numeric(out.loc[eligible, plane_col], errors="coerce") if plane_col in out.columns else np.nan
    work["_func"] = pd.to_numeric(out.loc[eligible, func_col], errors="coerce") if func_col in out.columns else np.nan
    work["_overlap_sort"] = work["_overlap"].fillna(-np.inf)
    work["_dist_sort"] = work["_dist"].fillna(np.inf)
    work["_plane_sort"] = work["_plane"].fillna(np.inf)
    work["_func_sort"] = work["_func"].fillna(np.inf)
    work = work.sort_values(
        ["_group", "_overlap_sort", "_dist_sort", "_plane_sort", "_func_sort"],
        ascending=[True, False, True, True, True],
    )
    ranks = work.groupby("_group", sort=False).cumcount() + 1
    counts = work.groupby("_group")["_group"].transform("size")
    out.loc[work.index, "dedup_rank_within_session_anat"] = pd.Series(ranks.to_numpy(), index=work.index, dtype="Int64")
    duplicate_member = counts.to_numpy() > 1
    out.loc[work.index, "is_multiplane_duplicate_roi"] = duplicate_member
    retained = ranks.to_numpy() == 1
    out.loc[work.index, "is_retained_after_multiplane_dedup"] = retained
    out.loc[work.index[duplicate_member & retained], "dedup_outcome"] = "retained best geometry"
    out.loc[work.index[duplicate_member & ~retained], "dedup_outcome"] = "duplicate anatomy label within session"
    return out


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
    if "plane_idx_key" not in lookup.columns:
        plane_values = lookup["plane_idx"] if "plane_idx" in lookup.columns else lookup.get("plane", pd.Series(np.nan, index=lookup.index))
        lookup["plane_idx_key"] = pd.to_numeric(plane_values, errors="coerce").astype("Int64")
    else:
        lookup["plane_idx_key"] = pd.to_numeric(lookup["plane_idx_key"], errors="coerce").astype("Int64")
    if "func_label_key" not in lookup.columns:
        lookup["func_label_key"] = pd.to_numeric(lookup["func_label"], errors="coerce").astype("Int64")
    else:
        lookup["func_label_key"] = pd.to_numeric(lookup["func_label_key"], errors="coerce").astype("Int64")
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

        anat_label_z = resolve_anatomy_label_z(plane_ref, z_size, best_z=best_z)
        if anat_label_z < 0 or anat_label_z >= z_size:
            plane_meta_rows.append(
                {
                    "plane": plane_label,
                    "plane_idx": int(p_idx),
                    "best_z": plane_ref.get("best_z", np.nan),
                    "n_rois_requested": int(len(roi_indices)),
                    "n_rois_rendered": int(len(raw_df)),
                    "n_anat_total": np.nan,
                    "status": f"anat_label_z_out_of_bounds ({anat_label_z})",
                    "func_source": func_source,
                }
            )
            continue

        anat_slice = ensure(anat_labels_all[anat_label_z])
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


def _hcr_status_uid(df: pd.DataFrame) -> pd.Series:
    if df is None or df.empty:
        return pd.Series(dtype=str)
    gene = df.get("gene", pd.Series("", index=df.index)).astype(str)
    conf_mask = df.get("conf_mask", pd.Series("", index=df.index)).astype(str)
    anat = pd.to_numeric(df.get("anat_label", pd.Series(np.nan, index=df.index)), errors="coerce").fillna(-1).astype(int).astype(str)
    primary = pd.to_numeric(df.get("primary_conf_label", pd.Series(np.nan, index=df.index)), errors="coerce").fillna(-1).astype(int).astype(str)
    return gene + "||" + conf_mask + "||" + anat + "||" + primary


def hcr_response_lookup_from_roi_master_df(
    master_df: pd.DataFrame,
    *,
    fish_id: str | None = None,
) -> pd.DataFrame:
    if master_df is None or master_df.empty:
        raise RuntimeError("response-aware ROI table is empty")
    out = master_df.copy()
    if fish_id is not None and "fish_id" in out.columns:
        out = out[out["fish_id"].astype(str) == str(fish_id)].copy()
    required = {"plane_idx", "func_label", "response_is_active", "response_class", "response_summary_class"}
    missing = sorted(required - set(out.columns))
    if missing:
        raise RuntimeError(f"response-aware ROI table is missing columns {missing}")
    if "suite2p_is_cell" not in out.columns:
        out["suite2p_is_cell"] = out.get("is_active", pd.Series(False, index=out.index))
    if "suite2p_activity_class" not in out.columns:
        out["suite2p_activity_class"] = out.get("activity_class", pd.Series(pd.NA, index=out.index, dtype="object"))
    lookup = out[
        [
            "plane_idx",
            "func_label",
            "response_is_active",
            "response_class",
            "response_summary_class",
            "suite2p_is_cell",
            "suite2p_activity_class",
        ]
    ].copy()
    lookup["plane_idx_key"] = pd.to_numeric(lookup["plane_idx"], errors="coerce").astype("Int64")
    lookup["func_label_key"] = pd.to_numeric(lookup["func_label"], errors="coerce").astype("Int64")
    lookup = lookup.drop(columns=["plane_idx", "func_label"])
    lookup = lookup.drop_duplicates(subset=["plane_idx_key", "func_label_key"], keep="last").reset_index(drop=True)
    lookup["response_is_active"] = lookup["response_is_active"].map(_bool_from_any).astype(bool)
    lookup["response_class"] = lookup["response_class"].fillna("response unavailable").astype(str)
    lookup["response_summary_class"] = lookup["response_summary_class"].fillna("Response unavailable").astype(str)
    lookup["suite2p_is_cell"] = lookup["suite2p_is_cell"].map(_maybe_bool_or_na)
    lookup["suite2p_activity_class"] = lookup["suite2p_activity_class"].where(lookup["suite2p_activity_class"].notna(), pd.NA)
    return lookup


def _attach_hcr_response_columns(candidate_df: pd.DataFrame, response_lookup_df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame() if candidate_df is None else candidate_df.copy()
    if out.empty:
        if "plane_idx" not in out.columns:
            out["plane_idx"] = pd.Series(dtype="Int64")
        if "func_label" not in out.columns:
            out["func_label"] = pd.Series(dtype="Int64")
        out["plane_idx_key"] = pd.to_numeric(out["plane_idx"], errors="coerce").astype("Int64")
        out["func_label_key"] = pd.to_numeric(out["func_label"], errors="coerce").astype("Int64")
        out["response_is_active"] = pd.Series(dtype=bool)
        out["response_class"] = pd.Series(dtype=object)
        out["response_summary_class"] = pd.Series(dtype=object)
        out["suite2p_is_cell"] = pd.Series(dtype=object)
        out["suite2p_activity_class"] = pd.Series(dtype=object)
        out["legacy_is_active"] = pd.Series(dtype=object)
        out["legacy_activity_class"] = pd.Series(dtype=object)
        return out

    legacy_is_active = out.get("is_active", pd.Series(pd.NA, index=out.index, dtype="object")).copy()
    legacy_activity_class = out.get("activity_class", pd.Series(pd.NA, index=out.index, dtype="object")).copy()
    prior_response_is_active = out.get("response_is_active", pd.Series(pd.NA, index=out.index, dtype="object")).copy()
    prior_response_class = out.get("response_class", pd.Series(pd.NA, index=out.index, dtype="object")).copy()
    prior_response_summary_class = out.get("response_summary_class", pd.Series(pd.NA, index=out.index, dtype="object")).copy()
    prior_suite2p_is_cell = out.get("suite2p_is_cell", pd.Series(pd.NA, index=out.index, dtype="object")).copy()
    prior_suite2p_activity_class = out.get("suite2p_activity_class", pd.Series(pd.NA, index=out.index, dtype="object")).copy()

    out["plane_idx_key"] = pd.to_numeric(out.get("plane_idx", pd.Series(np.nan, index=out.index)), errors="coerce").astype("Int64")
    out["func_label_key"] = pd.to_numeric(out.get("func_label", pd.Series(np.nan, index=out.index)), errors="coerce").astype("Int64")
    lookup = response_lookup_df.rename(
        columns={
            "response_is_active": "_lookup_response_is_active",
            "response_class": "_lookup_response_class",
            "response_summary_class": "_lookup_response_summary_class",
            "suite2p_is_cell": "_lookup_suite2p_is_cell",
            "suite2p_activity_class": "_lookup_suite2p_activity_class",
        }
    )
    out = out.merge(lookup, on=["plane_idx_key", "func_label_key"], how="left")
    out["response_is_active"] = out.get("_lookup_response_is_active", pd.Series(pd.NA, index=out.index)).where(
        out.get("_lookup_response_is_active", pd.Series(pd.NA, index=out.index)).notna(), prior_response_is_active
    )
    out["response_class"] = out.get("_lookup_response_class", pd.Series(pd.NA, index=out.index)).where(
        out.get("_lookup_response_class", pd.Series(pd.NA, index=out.index)).notna(), prior_response_class
    )
    out["response_summary_class"] = out.get("_lookup_response_summary_class", pd.Series(pd.NA, index=out.index)).where(
        out.get("_lookup_response_summary_class", pd.Series(pd.NA, index=out.index)).notna(), prior_response_summary_class
    )
    out["suite2p_is_cell"] = out.get("_lookup_suite2p_is_cell", pd.Series(pd.NA, index=out.index)).where(
        out.get("_lookup_suite2p_is_cell", pd.Series(pd.NA, index=out.index)).notna(), prior_suite2p_is_cell
    )
    out["suite2p_activity_class"] = out.get("_lookup_suite2p_activity_class", pd.Series(pd.NA, index=out.index)).where(
        out.get("_lookup_suite2p_activity_class", pd.Series(pd.NA, index=out.index)).notna(), prior_suite2p_activity_class
    )
    out["suite2p_is_cell"] = out["suite2p_is_cell"].where(out["suite2p_is_cell"].notna(), legacy_is_active)
    out["suite2p_activity_class"] = out["suite2p_activity_class"].where(out["suite2p_activity_class"].notna(), legacy_activity_class)
    out["response_is_active"] = out["response_is_active"].map(_bool_from_any).astype(bool)
    out["response_class"] = out["response_class"].fillna("response unavailable").astype(str)
    out["response_summary_class"] = out["response_summary_class"].fillna("Response unavailable").astype(str)
    out["suite2p_is_cell"] = out["suite2p_is_cell"].map(_maybe_bool_or_na)
    out["suite2p_activity_class"] = out["suite2p_activity_class"].where(out["suite2p_activity_class"].notna(), pd.NA)
    out = out.drop(
        columns=[
            column
            for column in (
                "_lookup_response_is_active",
                "_lookup_response_class",
                "_lookup_response_summary_class",
                "_lookup_suite2p_is_cell",
                "_lookup_suite2p_activity_class",
            )
            if column in out.columns
        ]
    )
    out["legacy_is_active"] = legacy_is_active.map(_maybe_bool_or_na)
    out["legacy_activity_class"] = legacy_activity_class.where(legacy_activity_class.notna(), pd.NA)
    out["is_active"] = out["response_is_active"]
    out["activity_class"] = out["response_summary_class"]
    return out


def _hcr_candidate_sort_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df.copy()
    out = df.copy()
    out["_rank_sort"] = pd.to_numeric(out.get("candidate_rank_for_anat", pd.Series(np.nan, index=out.index)), errors="coerce").astype(float).fillna(np.inf)
    out["_dist_sort"] = pd.to_numeric(out.get("dist_func_anat_um", pd.Series(np.nan, index=out.index)), errors="coerce").astype(float).fillna(np.inf)
    out["_overlap_sort"] = pd.to_numeric(out.get("overlap_px_func_anat", pd.Series(np.nan, index=out.index)), errors="coerce").astype(float).fillna(0.0)
    out["_plane_sort"] = pd.to_numeric(out.get("plane_idx", pd.Series(np.nan, index=out.index)), errors="coerce").astype(float).fillna(np.inf)
    out["_func_sort"] = pd.to_numeric(out.get("func_label", pd.Series(np.nan, index=out.index)), errors="coerce").astype(float).fillna(np.inf)
    out = out.sort_values(
        ["_rank_sort", "_dist_sort", "_overlap_sort", "_plane_sort", "_func_sort"],
        ascending=[True, True, False, True, True],
        na_position="last",
    )
    return out.drop(columns=["_rank_sort", "_dist_sort", "_overlap_sort", "_plane_sort", "_func_sort"])


def _hcr_candidate_key(row: dict[str, Any] | pd.Series | None) -> tuple[int, int] | None:
    if row is None:
        return None
    plane = _to_int_or_na(row.get("plane_idx", pd.NA))
    func_label = _to_int_or_na(row.get("func_label", pd.NA))
    if pd.isna(plane) or pd.isna(func_label):
        return None
    return int(plane), int(func_label)


def _hcr_empty_payload() -> dict[str, Any]:
    return {
        "plane": pd.NA,
        "plane_label": None,
        "func_label": pd.NA,
        "roi_idx": pd.NA,
        "overlap_px": np.nan,
        "dist_um": np.nan,
        "func_source": None,
        "response_is_active": False,
        "response_class": "response unavailable",
        "response_summary_class": "Response unavailable",
        "suite2p_is_cell": pd.NA,
        "suite2p_activity_class": pd.NA,
    }


def _hcr_candidate_payload(row: dict[str, Any] | pd.Series | None) -> dict[str, Any]:
    if row is None:
        return _hcr_empty_payload()
    payload = {
        "plane": _to_int_or_na(row.get("plane_idx", pd.NA)),
        "plane_label": None if pd.isna(row.get("plane", pd.NA)) else str(row.get("plane")),
        "func_label": _to_int_or_na(row.get("func_label", pd.NA)),
        "roi_idx": _to_int_or_na(row.get("roi_idx", pd.NA)),
        "overlap_px": _to_float_or_nan(row.get("overlap_px_func_anat", np.nan)),
        "dist_um": _to_float_or_nan(row.get("dist_func_anat_um", np.nan)),
        "func_source": row.get("func_source", None),
        "response_is_active": bool(_bool_from_any(row.get("response_is_active", False))),
        "response_class": str(row.get("response_class", "response unavailable")) if row.get("response_class", None) is not None else "response unavailable",
        "response_summary_class": str(row.get("response_summary_class", "Response unavailable")) if row.get("response_summary_class", None) is not None else "Response unavailable",
        "suite2p_is_cell": _maybe_bool_or_na(row.get("suite2p_is_cell", pd.NA)),
        "suite2p_activity_class": row.get("suite2p_activity_class", pd.NA),
    }
    if payload["plane_label"] in {"nan", "None"}:
        payload["plane_label"] = None
    if payload["response_class"] in {"nan", "None"}:
        payload["response_class"] = "response unavailable"
    if payload["response_summary_class"] in {"nan", "None"}:
        payload["response_summary_class"] = "Response unavailable"
    return payload


def finalize_hcr_activity_export_tables(
    status_df_legacy: pd.DataFrame,
    raw_df_legacy: pd.DataFrame,
    candidate_df: pd.DataFrame,
    response_lookup_df: pd.DataFrame,
    *,
    config: HcrActivityExportConfig | None = None,
    fish_id: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    cfg = config or HcrActivityExportConfig()
    response_summary_responsive = cfg.response_summary_responsive
    response_summary_low = cfg.response_summary_low
    response_summary_unavailable = cfg.response_summary_unavailable
    response_class_unavailable = cfg.response_class_unavailable

    status_work = pd.DataFrame() if status_df_legacy is None else status_df_legacy.copy()
    raw_work = pd.DataFrame() if raw_df_legacy is None else raw_df_legacy.copy()
    candidate_work = pd.DataFrame() if candidate_df is None else candidate_df.copy()
    if status_work.empty:
        return status_work, raw_work, pd.DataFrame(), candidate_work

    candidate_work = _attach_hcr_response_columns(candidate_work, response_lookup_df)
    raw_work = _attach_hcr_response_columns(raw_work, response_lookup_df)
    status_work["hcr_status_uid"] = _hcr_status_uid(status_work)
    raw_work["hcr_status_uid"] = _hcr_status_uid(raw_work)

    status_rows: list[dict[str, Any]] = []
    raw_rows: list[dict[str, Any]] = []
    analysis_rows: list[dict[str, Any]] = []

    for _, status_old in status_work.iterrows():
        uid = str(status_old.get("hcr_status_uid", ""))
        group = raw_work[raw_work["hcr_status_uid"].astype(str) == uid].copy()
        group = _hcr_candidate_sort_df(group)
        cand = group[group["plane_idx_key"].notna() & group["func_label_key"].notna()].copy()
        cand = _hcr_candidate_sort_df(cand)

        responsive_cand = cand[cand["response_is_active"].astype(bool)].copy()
        low_cand = cand[cand["response_summary_class"].astype(str) == response_summary_low].copy()
        unavailable_cand = cand[cand["response_summary_class"].astype(str) == response_summary_unavailable].copy()
        nonresponsive_cand = cand[cand["response_is_active"].astype(bool) == False].copy()

        geom_row = cand.iloc[0].to_dict() if not cand.empty else None
        response_row = responsive_cand.iloc[0].to_dict() if not responsive_cand.empty else None
        low_row = low_cand.iloc[0].to_dict() if not low_cand.empty else None
        unavailable_row = unavailable_cand.iloc[0].to_dict() if not unavailable_cand.empty else None
        nonresponsive_row = nonresponsive_cand.iloc[0].to_dict() if not nonresponsive_cand.empty else None
        selected_row = response_row if response_row is not None else (low_row if low_row is not None else (unavailable_row if unavailable_row is not None else geom_row))

        geom_key = _hcr_candidate_key(geom_row)
        response_key = _hcr_candidate_key(response_row)
        low_key = _hcr_candidate_key(low_row)
        unavailable_key = _hcr_candidate_key(unavailable_row)
        nonresponsive_key = _hcr_candidate_key(nonresponsive_row)
        selected_key = _hcr_candidate_key(selected_row)

        represented = bool(_bool_from_any(status_old.get("represented_on_func_plane", False)))
        if not represented:
            try:
                represented = int(status_old.get("represented_plane_count", 0) or 0) > 0
            except Exception:
                represented = False
        if not represented:
            functional_status = cfg.hcr_out_of_plane
        elif cand.empty:
            functional_status = cfg.hcr_in_plane_no_func
        elif response_row is not None:
            functional_status = cfg.hcr_in_plane_responsive
        elif low_row is not None:
            functional_status = cfg.hcr_in_plane_low
        else:
            functional_status = cfg.hcr_in_plane_unavailable

        selected_payload = _hcr_candidate_payload(selected_row)
        geom_payload = _hcr_candidate_payload(geom_row)
        response_payload = _hcr_candidate_payload(response_row)
        low_payload = _hcr_candidate_payload(low_row)
        unavailable_payload = _hcr_candidate_payload(unavailable_row)
        nonresponsive_payload = _hcr_candidate_payload(nonresponsive_row)

        status_new = status_old.to_dict()
        status_new["functional_status_legacy"] = str(status_old.get("functional_status", pd.NA))
        status_new["match_policy_version"] = cfg.hcr_activity_match_policy
        status_new["selection_rule"] = cfg.selection_rule
        status_new["functional_status"] = functional_status
        status_new["response_is_active"] = bool(selected_payload["response_is_active"])
        status_new["response_class"] = selected_payload["response_class"]
        status_new["response_summary_class"] = selected_payload["response_summary_class"]
        status_new["is_active"] = bool(selected_payload["response_is_active"])
        status_new["activity_class"] = selected_payload["response_summary_class"]
        status_new["selected_plane"] = selected_payload["plane"]
        status_new["selected_plane_label"] = selected_payload["plane_label"]
        status_new["selected_func_label"] = selected_payload["func_label"]
        status_new["selected_roi_idx"] = selected_payload["roi_idx"]
        status_new["selected_is_active"] = bool(selected_payload["response_is_active"])
        status_new["selected_activity_class"] = selected_payload["response_summary_class"]
        status_new["selected_overlap_px"] = selected_payload["overlap_px"]
        status_new["selected_dist_um"] = selected_payload["dist_um"]
        status_new["selected_func_source"] = selected_payload["func_source"]
        status_new["selected_response_is_active"] = bool(selected_payload["response_is_active"])
        status_new["selected_response_class"] = selected_payload["response_class"]
        status_new["selected_response_summary_class"] = selected_payload["response_summary_class"]
        status_new["selected_suite2p_is_cell"] = selected_payload["suite2p_is_cell"]
        status_new["selected_suite2p_activity_class"] = selected_payload["suite2p_activity_class"]

        for prefix, payload in (
            ("selected_geometry", geom_payload),
            ("selected_response", response_payload),
            ("selected_low", low_payload),
            ("selected_nonresponsive", nonresponsive_payload),
            ("selected_unavailable", unavailable_payload),
        ):
            status_new[f"{prefix}_plane"] = payload["plane"]
            status_new[f"{prefix}_plane_label"] = payload["plane_label"]
            status_new[f"{prefix}_func_label"] = payload["func_label"]
            status_new[f"{prefix}_roi_idx"] = payload["roi_idx"]
            status_new[f"{prefix}_overlap_px"] = payload["overlap_px"]
            status_new[f"{prefix}_dist_um"] = payload["dist_um"]
            status_new[f"{prefix}_func_source"] = payload["func_source"]
            if prefix == "selected_geometry":
                status_new[f"{prefix}_response_is_active"] = bool(payload["response_is_active"])
                status_new[f"{prefix}_response_class"] = payload["response_class"]
                status_new[f"{prefix}_response_summary_class"] = payload["response_summary_class"]
                status_new[f"{prefix}_suite2p_is_cell"] = payload["suite2p_is_cell"]
                status_new[f"{prefix}_suite2p_activity_class"] = payload["suite2p_activity_class"]

        status_new["n_func_candidates_total"] = int(len(cand))
        status_new["n_responsive_candidates"] = int(len(responsive_cand))
        status_new["n_low_candidates"] = int(len(low_cand))
        status_new["n_response_unavailable_candidates"] = int(len(unavailable_cand))
        status_new["has_any_responsive_candidate"] = bool(response_row is not None)
        status_new["has_any_low_candidate"] = bool(low_row is not None)
        status_new["has_any_response_unavailable_candidate"] = bool(unavailable_row is not None)
        status_new["has_responsive_alternative"] = bool(response_key is not None and geom_key is not None and response_key != geom_key)
        status_new["has_low_alternative"] = bool(low_key is not None and geom_key is not None and low_key != geom_key)
        status_new["has_unavailable_alternative"] = bool(unavailable_key is not None and geom_key is not None and unavailable_key != geom_key)
        status_new["selected_for_trace_export"] = bool(response_row is not None)
        status_new["n_active_candidates"] = int(len(responsive_cand))
        status_new["n_inactive_candidates"] = int(len(nonresponsive_cand))
        status_new["has_any_active_candidate"] = bool(response_row is not None)
        status_new["has_any_inactive_candidate"] = bool(nonresponsive_row is not None)
        status_new["has_active_alternative"] = bool(response_key is not None and geom_key is not None and response_key != geom_key)
        status_new["has_inactive_alternative"] = bool(nonresponsive_key is not None and geom_key is not None and nonresponsive_key != geom_key)
        status_new["selected_plane_active"] = response_payload["plane"]
        status_new["selected_plane_label_active"] = response_payload["plane_label"]
        status_new["selected_func_label_active"] = response_payload["func_label"]
        status_new["selected_roi_idx_active"] = response_payload["roi_idx"]
        status_new["selected_overlap_px_active"] = response_payload["overlap_px"]
        status_new["selected_dist_um_active"] = response_payload["dist_um"]
        status_new["selected_func_source_active"] = response_payload["func_source"]
        status_new["selected_plane_inactive"] = nonresponsive_payload["plane"]
        status_new["selected_plane_label_inactive"] = nonresponsive_payload["plane_label"]
        status_new["selected_func_label_inactive"] = nonresponsive_payload["func_label"]
        status_new["selected_roi_idx_inactive"] = nonresponsive_payload["roi_idx"]
        status_new["selected_overlap_px_inactive"] = nonresponsive_payload["overlap_px"]
        status_new["selected_dist_um_inactive"] = nonresponsive_payload["dist_um"]
        status_new["selected_func_source_inactive"] = nonresponsive_payload["func_source"]
        status_rows.append(status_new)

        if group.empty:
            group = pd.DataFrame([status_old.to_dict()])
        for _, raw_row in group.iterrows():
            raw_new = raw_row.to_dict()
            rr_key = _hcr_candidate_key(raw_row)
            rr_is_candidate = rr_key is not None
            rr_summary = str(raw_row.get("response_summary_class", response_summary_unavailable)) if raw_row.get("response_summary_class", None) is not None else response_summary_unavailable
            if not rr_is_candidate:
                bucket = "no functional ROI candidate"
            elif bool(_bool_from_any(raw_row.get("response_is_active", False))):
                bucket = "responsive"
            elif rr_summary == response_summary_low:
                bucket = "low activity"
            else:
                bucket = "response unavailable"
            raw_new["match_policy_version"] = cfg.hcr_activity_match_policy
            raw_new["selection_rule"] = cfg.selection_rule
            raw_new["functional_status"] = functional_status
            raw_new["functional_status_legacy"] = str(status_old.get("functional_status", pd.NA))
            raw_new["response_is_active"] = bool(_bool_from_any(raw_row.get("response_is_active", False))) if rr_is_candidate else False
            raw_new["response_class"] = str(raw_row.get("response_class", response_class_unavailable)) if rr_is_candidate else response_class_unavailable
            raw_new["response_summary_class"] = rr_summary if rr_is_candidate else response_summary_unavailable
            raw_new["is_active"] = raw_new["response_is_active"]
            raw_new["activity_class"] = raw_new["response_summary_class"]
            raw_new["candidate_response_bucket"] = bucket
            raw_new["selected_response_is_active"] = bool(selected_payload["response_is_active"])
            raw_new["selected_response_class"] = selected_payload["response_class"]
            raw_new["selected_response_summary_class"] = selected_payload["response_summary_class"]
            raw_new["is_selected_best_any"] = bool(rr_is_candidate and geom_key is not None and rr_key == geom_key)
            raw_new["is_selected_best_geometry"] = bool(rr_is_candidate and geom_key is not None and rr_key == geom_key)
            raw_new["is_selected_best_response"] = bool(rr_is_candidate and response_key is not None and rr_key == response_key)
            raw_new["is_selected_best_low"] = bool(rr_is_candidate and low_key is not None and rr_key == low_key)
            raw_new["is_selected_best_unavailable"] = bool(rr_is_candidate and unavailable_key is not None and rr_key == unavailable_key)
            raw_new["is_selected_canonical"] = bool(rr_is_candidate and selected_key is not None and rr_key == selected_key)
            raw_new["is_selected_best_active"] = bool(rr_is_candidate and response_key is not None and rr_key == response_key)
            raw_new["is_selected_best_inactive"] = bool(rr_is_candidate and nonresponsive_key is not None and rr_key == nonresponsive_key)
            raw_new["is_selected_for_analysis"] = bool(rr_is_candidate and response_key is not None and rr_key == response_key)
            raw_rows.append(raw_new)

        if response_row is not None:
            analysis_rows.append(
                {
                    "fish_id": status_old.get("fish_id", fish_id),
                    "gene": str(status_old.get("gene", "unknown")),
                    "conf_mask": status_old.get("conf_mask", None),
                    "conf_label": _to_int_or_na(status_old.get("primary_conf_label", status_old.get("conf_label", pd.NA))),
                    "conf_labels": status_old.get("conf_labels", None),
                    "conf_label_count": _to_int_or_na(status_old.get("conf_label_count", pd.NA)),
                    "anat_label": _to_int_or_na(status_old.get("anat_label", pd.NA)),
                    "func_label": response_payload["func_label"],
                    "plane": response_payload["plane"],
                    "plane_label": response_payload["plane_label"],
                    "roi_idx": response_payload["roi_idx"],
                    "is_active": True,
                    "activity_class": response_summary_responsive,
                    "response_is_active": True,
                    "response_class": response_payload["response_class"],
                    "response_summary_class": response_payload["response_summary_class"],
                    "suite2p_is_cell": response_payload["suite2p_is_cell"],
                    "suite2p_activity_class": response_payload["suite2p_activity_class"],
                    "dist_conf_anat_um": _to_float_or_nan(status_old.get("dist_conf_anat_um", np.nan)),
                    "dist_func_anat_um": response_payload["dist_um"],
                    "overlap_px_func_anat": response_payload["overlap_px"],
                    "represented_on_func_plane": represented,
                    "functional_status": functional_status,
                    "functional_status_legacy": str(status_old.get("functional_status", pd.NA)),
                    "n_func_candidates_total": int(len(cand)),
                    "n_responsive_candidates": int(len(responsive_cand)),
                    "n_low_candidates": int(len(low_cand)),
                    "n_response_unavailable_candidates": int(len(unavailable_cand)),
                    "has_any_responsive_candidate": bool(response_row is not None),
                    "has_any_low_candidate": bool(low_row is not None),
                    "has_any_response_unavailable_candidate": bool(unavailable_row is not None),
                    "has_responsive_alternative": bool(response_key is not None and geom_key is not None and response_key != geom_key),
                    "has_low_alternative": bool(low_key is not None and geom_key is not None and low_key != geom_key),
                    "selected_geometry_plane": geom_payload["plane"],
                    "selected_geometry_func_label": geom_payload["func_label"],
                    "selection_rule": cfg.selection_rule,
                    "selection_rank_gene_anat": 1,
                    "is_selected_for_analysis": True,
                    "match_policy_version": cfg.hcr_activity_match_policy,
                }
            )

    status_df = pd.DataFrame(status_rows)
    raw_df = pd.DataFrame(raw_rows)
    analysis_df = pd.DataFrame(analysis_rows)
    if analysis_df.empty:
        analysis_df = pd.DataFrame(
            columns=[
                "fish_id",
                "gene",
                "conf_mask",
                "conf_label",
                "conf_labels",
                "conf_label_count",
                "anat_label",
                "func_label",
                "plane",
                "plane_label",
                "roi_idx",
                "is_active",
                "activity_class",
                "response_is_active",
                "response_class",
                "response_summary_class",
                "suite2p_is_cell",
                "suite2p_activity_class",
                "dist_conf_anat_um",
                "dist_func_anat_um",
                "overlap_px_func_anat",
                "represented_on_func_plane",
                "functional_status",
                "functional_status_legacy",
                "n_func_candidates_total",
                "n_responsive_candidates",
                "n_low_candidates",
                "n_response_unavailable_candidates",
                "has_any_responsive_candidate",
                "has_any_low_candidate",
                "has_any_response_unavailable_candidate",
                "has_responsive_alternative",
                "has_low_alternative",
                "selected_geometry_plane",
                "selected_geometry_func_label",
                "selection_rule",
                "selection_rank_gene_anat",
                "is_selected_for_analysis",
                "match_policy_version",
                "roi_reuse_count_within_gene",
                "roi_reused_within_gene",
            ]
        )

    if not candidate_work.empty:
        if "gene" not in candidate_work.columns:
            candidate_work["gene"] = pd.NA
        candidate_work["candidate_response_bucket"] = np.select(
            [
                candidate_work["response_is_active"].astype(bool),
                candidate_work["response_summary_class"].astype(str) == response_summary_low,
            ],
            ["responsive", "low activity"],
            default="response unavailable",
        )

    int_cols_status = [
        "anat_label",
        "conf_label",
        "primary_conf_label",
        "conf_label_count",
        "represented_plane_count",
        "n_func_candidates_total",
        "n_responsive_candidates",
        "n_low_candidates",
        "n_response_unavailable_candidates",
        "n_active_candidates",
        "n_inactive_candidates",
        "selected_plane",
        "selected_func_label",
        "selected_roi_idx",
        "selected_geometry_plane",
        "selected_geometry_func_label",
        "selected_geometry_roi_idx",
        "selected_response_plane",
        "selected_response_func_label",
        "selected_response_roi_idx",
        "selected_low_plane",
        "selected_low_func_label",
        "selected_low_roi_idx",
        "selected_nonresponsive_plane",
        "selected_nonresponsive_func_label",
        "selected_nonresponsive_roi_idx",
        "selected_unavailable_plane",
        "selected_unavailable_func_label",
        "selected_unavailable_roi_idx",
        "selected_plane_active",
        "selected_func_label_active",
        "selected_roi_idx_active",
        "selected_plane_inactive",
        "selected_func_label_inactive",
        "selected_roi_idx_inactive",
    ]
    for col in int_cols_status:
        if col in status_df.columns:
            status_df[col] = pd.to_numeric(status_df[col], errors="coerce").astype("Int64")
    if not status_df.empty and status_df.get("selected_plane", pd.Series(dtype=object)).notna().any() and status_df.get("selected_func_label", pd.Series(dtype=object)).notna().any():
        reuse_df = status_df[status_df["selected_plane"].notna() & status_df["selected_func_label"].notna()].copy()
        reuse_counts = reuse_df.groupby(["gene", "selected_plane", "selected_func_label"]).size().rename("selected_roi_reuse_count").reset_index()
        status_df = status_df.merge(reuse_counts, on=["gene", "selected_plane", "selected_func_label"], how="left")
        status_df["selected_roi_reuse_count"] = pd.to_numeric(status_df.get("selected_roi_reuse_count", pd.Series(0, index=status_df.index)), errors="coerce").fillna(0).astype(int)
        status_df["selected_roi_reused_within_gene"] = status_df["selected_roi_reuse_count"] > 1
    elif not status_df.empty:
        status_df["selected_roi_reuse_count"] = 0
        status_df["selected_roi_reused_within_gene"] = False

    for col in ("plane_idx", "best_z", "func_label", "roi_idx", "anat_label", "candidate_rank_for_anat"):
        if col in raw_df.columns:
            raw_df[col] = pd.to_numeric(raw_df[col], errors="coerce").astype("Int64")
    for col in ("conf_label", "conf_label_count", "anat_label", "func_label", "plane", "roi_idx", "selection_rank_gene_anat"):
        if col in analysis_df.columns:
            analysis_df[col] = pd.to_numeric(analysis_df[col], errors="coerce").astype("Int64")
    if not analysis_df.empty:
        reuse_counts = analysis_df.groupby(["gene", "plane", "func_label"]).size().rename("roi_reuse_count_within_gene").reset_index()
        analysis_df = analysis_df.merge(reuse_counts, on=["gene", "plane", "func_label"], how="left")
        analysis_df["roi_reuse_count_within_gene"] = pd.to_numeric(analysis_df.get("roi_reuse_count_within_gene", pd.Series(0, index=analysis_df.index)), errors="coerce").fillna(0).astype(int)
        analysis_df["roi_reused_within_gene"] = analysis_df["roi_reuse_count_within_gene"] > 1

    status_df = status_df.sort_values(["gene", "anat_label"]).reset_index(drop=True)
    raw_sort_cols = [
        "gene",
        "anat_label",
        "is_selected_best_geometry",
        "is_selected_for_analysis",
        "candidate_rank_for_anat",
        "plane_idx",
        "func_label",
    ]
    raw_df = raw_df.sort_values(
        [column for column in raw_sort_cols if column in raw_df.columns],
        ascending=[True, True, False, False, True, True, True][: len([column for column in raw_sort_cols if column in raw_df.columns])],
        na_position="last",
    ).reset_index(drop=True)
    analysis_df = analysis_df.sort_values(["gene", "anat_label", "plane", "func_label"]).reset_index(drop=True)
    if not candidate_work.empty:
        candidate_work = candidate_work.sort_values(["anat_label", "plane_idx", "func_label"]).reset_index(drop=True)
    return status_df, raw_df, analysis_df, candidate_work


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
    "finalize_hcr_activity_export_tables",
    "hcr_response_lookup_from_roi_master_df",
    "build_plane_centroid_matches",
    "annotate_session_anat_label_duplicates",
    "compute_centroids",
    "compute_label_overlap",
    "gene_from_mask",
    "harmonize_functional_labels_to_anatomy",
    "hungarian_match",
    "idx_to_um",
    "nearest_neighbor_match",
    "resample_image",
    "resample_labels_nn",
    "resolve_anatomy_label_z",
    "resolve_plane_transform",
    "run_single_fish_cell_44_stage",
    "run_single_fish_cell_46_stage",
    "run_single_fish_cell_50_stage",
    "run_single_fish_cell_50i_stage",
    "summarize_distances",
    "summarize_functional_anatomy_geometry_metrics",
    "transform_points_between_spaces",
]
