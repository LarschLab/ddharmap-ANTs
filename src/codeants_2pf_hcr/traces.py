"""Trace export helpers for notebook cells [51], [56], [56h], and [57]."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd


def _as_bool_series(series_in: Any) -> pd.Series:
    s = pd.Series(series_in)
    if pd.api.types.is_bool_dtype(s):
        return s.fillna(False).astype(bool)
    if pd.api.types.is_numeric_dtype(s):
        return s.fillna(0).astype(float) != 0
    return s.astype(str).str.strip().str.lower().isin({"1", "true", "t", "yes", "y"})


@dataclass(frozen=True)
class TraceExportConfig:
    recompute: bool = True
    match_policy_version: str = "hcr_anat_first_local_geometry_response_v4_suite2p_gate"
    conf_func_csv_name: str = "conf_to_func_pairs.csv"
    trace_npy_pattern: str = "plane{plane_idx}_dff.npy"
    meta_csv_name: str = "suite2p_dff_traces_meta.csv"


def prepare_pairs_for_unique_cells(pairs_df: pd.DataFrame, *, strict: bool = True, tag: str = "[stim]") -> pd.DataFrame:
    req_cols = ["gene", "conf_mask", "conf_label", "anat_label", "func_label", "plane"]
    missing = [col for col in req_cols if col not in pairs_df.columns]
    if missing:
        msg = f"{tag} mapping missing required columns for unique anat-cell traces: {missing}"
        if strict:
            raise RuntimeError(msg)
        return pairs_df.iloc[0:0].copy()

    out = pairs_df.copy()
    if "is_selected_for_analysis" in out.columns:
        selected = _as_bool_series(out["is_selected_for_analysis"])
        n_not_selected = int((~selected).sum())
        if strict and n_not_selected > 0:
            raise RuntimeError(
                f"{tag} mapping contains {n_not_selected} non-selected rows. "
                "Use the dedup analysis mapping from [50] (conf_to_func_pairs.csv)."
            )
        out = out[selected].copy()

    gene_clean = (
        out["gene"]
        .astype("string")
        .str.strip()
        .replace({"": pd.NA, "nan": pd.NA, "none": pd.NA, "null": pd.NA})
    )
    out["gene"] = gene_clean
    for col in ("conf_label", "anat_label", "func_label", "plane"):
        out[col] = pd.to_numeric(out[col], errors="coerce")
    required_nonnull = out[["gene", "conf_label", "anat_label", "func_label", "plane"]].notna().all(axis=1)
    n_drop = int((~required_nonnull).sum())
    if strict and n_drop > 0:
        raise RuntimeError(
            f"{tag} mapping has {n_drop} rows with missing required fields after parsing. "
            "Re-run [50] to regenerate clean mapping."
        )
    out = out[required_nonnull].copy()
    if out.empty:
        return out
    out["conf_label"] = out["conf_label"].astype(int)
    out["anat_label"] = out["anat_label"].astype(int)
    out["func_label"] = out["func_label"].astype(int)
    out["plane"] = out["plane"].astype(int)
    return out


def resolve_conf_func_csv_analysis(
    *,
    out_reg: str | Path,
    run_config: dict[str, Any] | None = None,
    conf_func_csv: str | Path | None = None,
    fish_id: str | None = None,
) -> Path:
    out_reg_path = Path(out_reg)
    default_conf = out_reg_path / "conf_to_func_pairs.csv"
    rc = run_config if isinstance(run_config, dict) else {}
    override = rc.get("CONF_FUNC_CSV_ANALYSIS", conf_func_csv)
    if override is None:
        return default_conf
    override_path = Path(str(override))
    if fish_id is not None:
        fish = str(fish_id)
        if fish in str(override_path):
            return override_path
        try:
            if override_path.resolve(strict=False) == default_conf.resolve(strict=False):
                return override_path
        except Exception:
            pass
        if override_path.name.startswith("conf_to_func_pairs") and override_path.parent == out_reg_path:
            return override_path
        return default_conf
    return override_path


def export_suite2p_trace_metadata(
    *,
    fish_id: str,
    out_dir: str | Path,
    suite2p_by_ref_idx: dict[int, dict[str, Any]] | None,
    conf_func_csv: str | Path,
    config: TraceExportConfig | None = None,
    gene_from_mask_func: Callable[[str | Path | None], str] | None = None,
) -> dict[str, Any]:
    cfg = config or TraceExportConfig()
    out_dir_path = Path(out_dir)
    conf_func_path = Path(conf_func_csv)
    meta_path = out_dir_path / cfg.meta_csv_name
    match_policy = str(cfg.match_policy_version)
    empty_meta = pd.DataFrame()

    if (not bool(cfg.recompute)) and meta_path.exists():
        try:
            meta_df_cached = pd.read_csv(meta_path)
            if "fish_id" in meta_df_cached.columns:
                meta_df_cached = meta_df_cached[meta_df_cached["fish_id"].astype(str) == str(fish_id)].copy()
            meta_versions = set(meta_df_cached.get("match_policy_version", pd.Series(dtype=str)).dropna().astype(str).unique().tolist())
            trace_paths = []
            if "trace_path" in meta_df_cached.columns:
                trace_paths = [str(p) for p in meta_df_cached["trace_path"].dropna().astype(str).unique()]
            if ("fish_id" in meta_df_cached.columns) and meta_df_cached.empty:
                cache_message = "[Suite2p] cache metadata does not contain current fish_id rows; recomputing export."
            elif ("match_policy_version" not in meta_df_cached.columns) or (match_policy not in meta_versions):
                cache_message = f"[Suite2p] trace cache uses stale matching policy; recomputing export for {match_policy}."
            else:
                missing_trace_paths = [p for p in trace_paths if not Path(p).exists()]
                if missing_trace_paths:
                    cache_message = (
                        f"[Suite2p] cache metadata found but {len(missing_trace_paths)} trace files are missing; "
                        "recomputing export."
                    )
                else:
                    return {
                        "status": "cached",
                        "message": f"[Suite2p] Reusing cached trace export: {meta_path} (rows={len(meta_df_cached)})",
                        "meta_csv": meta_path,
                        "meta_df": meta_df_cached,
                    }
        except Exception as exc:
            cache_message = f"[Suite2p] Failed to read cached trace metadata ({exc}); recomputing export."
    else:
        cache_message = None

    if not suite2p_by_ref_idx:
        return {
            "status": "missing_suite2p",
            "message": "Suite2p data not loaded; run the Suite2p cell first.",
            "meta_csv": meta_path,
            "meta_df": empty_meta,
        }
    if not conf_func_path.exists():
        return {
            "status": "missing_mapping",
            "message": f"Missing mapping CSV: {conf_func_path} (run [50] first).",
            "meta_csv": meta_path,
            "meta_df": empty_meta,
        }

    pairs = pd.read_csv(conf_func_path)
    if "fish_id" in pairs.columns:
        pairs = pairs[pairs["fish_id"].astype(str) == str(fish_id)].copy()
    pair_versions = set(pairs.get("match_policy_version", pd.Series(dtype=str)).dropna().astype(str).unique().tolist())
    if pairs.empty:
        return {
            "status": "empty_mapping",
            "message": "Mapping CSV is empty after fish filter.",
            "meta_csv": meta_path,
            "meta_df": empty_meta,
        }
    if ("match_policy_version" not in pairs.columns) or (match_policy not in pair_versions):
        return {
            "status": "stale_mapping",
            "message": f"[Suite2p] Mapping CSV uses stale matching policy; rerun [50] to regenerate {conf_func_path}.",
            "meta_csv": meta_path,
            "meta_df": empty_meta,
        }
    required_pair_response_cols = {"response_is_active", "response_class", "response_summary_class"}
    missing_pair_response_cols = sorted(required_pair_response_cols - set(pairs.columns))
    if missing_pair_response_cols:
        return {
            "status": "missing_response_columns",
            "message": (
                f"[Suite2p] Mapping CSV missing response-aware columns {missing_pair_response_cols}; "
                "rerun [50] after [50ia]."
            ),
            "meta_csv": meta_path,
            "meta_df": empty_meta,
        }
    if "func_label" not in pairs.columns:
        return {
            "status": "missing_func_label",
            "message": "Mapping CSV missing func_label; cannot map to Suite2p.",
            "meta_csv": meta_path,
            "meta_df": empty_meta,
        }

    pairs = pairs[pairs["func_label"].notna()].copy()
    if "plane" in pairs.columns:
        pairs = pairs[pairs["plane"].notna()].copy()
        pairs["plane"] = pairs["plane"].astype(int)
    pairs["func_label"] = pairs["func_label"].astype(int)
    if pairs.empty:
        return {
            "status": "empty_func_mapping",
            "message": "No functional matches with func_label.",
            "meta_csv": meta_path,
            "meta_df": empty_meta,
        }

    if "gene" not in pairs.columns:
        if "conf_mask" in pairs.columns and callable(gene_from_mask_func):
            pairs["gene"] = pairs["conf_mask"].apply(gene_from_mask_func)
        else:
            pairs["gene"] = "unknown"

    n_in = len(pairs)
    if "anat_label" in pairs.columns and pairs["anat_label"].notna().any():
        pairs = pairs[pairs["anat_label"].notna()].copy()
        pairs["anat_label"] = pairs["anat_label"].astype(int)
        pairs["_sort_dist_func"] = pd.to_numeric(
            pairs.get("dist_func_anat_um", pd.Series(np.nan, index=pairs.index)),
            errors="coerce",
        ).fillna(np.inf)
        pairs["_sort_overlap"] = pd.to_numeric(
            pairs.get("overlap_px_func_anat", pairs.get("overlap_px", pd.Series(np.nan, index=pairs.index))),
            errors="coerce",
        ).fillna(0)
        pairs["_sort_dist_conf"] = pd.to_numeric(
            pairs.get("dist_conf_anat_um", pd.Series(np.nan, index=pairs.index)),
            errors="coerce",
        ).fillna(np.inf)
        pairs = pairs.sort_values(
            ["gene", "anat_label", "_sort_dist_func", "_sort_overlap", "_sort_dist_conf", "plane", "func_label"],
            ascending=[True, True, True, False, True, True, True],
        )
        pairs = pairs.drop_duplicates(subset=["gene", "anat_label"], keep="first")
        pairs = pairs.drop(columns=["_sort_dist_func", "_sort_overlap", "_sort_dist_conf"])
    n_after_anat = len(pairs)
    pairs = pairs.drop_duplicates(subset=["plane", "func_label", "gene"])
    n_after_roi = len(pairs)
    n_roi_reuse = int(max(0, n_after_anat - n_after_roi))

    out_dir_path.mkdir(parents=True, exist_ok=True)
    meta_rows = []
    plane_messages = []
    for p_idx, plane in suite2p_by_ref_idx.items():
        p_idx = int(p_idx)
        sub = pairs[pairs["plane"] == p_idx].copy() if "plane" in pairs.columns else pairs.copy()
        if sub.empty:
            continue
        dff = plane.get("dff", None)
        if dff is None:
            plane_messages.append(f"[Suite2p] plane {p_idx}: missing dff; skip")
            continue
        roi_idx = sub["func_label"].to_numpy() - 1
        valid = (roi_idx >= 0) & (roi_idx < dff.shape[0])
        if not valid.any():
            continue
        sub = sub.loc[valid].reset_index(drop=True)
        roi_idx = roi_idx[valid]
        traces = dff[roi_idx]
        out_path = out_dir_path / cfg.trace_npy_pattern.format(plane_idx=p_idx)
        np.save(out_path, traces)
        sub["roi_idx"] = roi_idx
        sub["trace_row"] = np.arange(traces.shape[0])
        sub["trace_path"] = str(out_path)
        meta_rows.append(sub)
        plane_messages.append(f"[Suite2p] plane {p_idx}: saved dF/F traces n={traces.shape[0]} -> {out_path}")

    if not meta_rows:
        return {
            "status": "no_traces",
            "message": "No matched traces to save.",
            "meta_csv": meta_path,
            "meta_df": empty_meta,
            "log_lines": ([cache_message] if cache_message else []) + plane_messages,
        }

    meta_df = pd.concat(meta_rows, ignore_index=True)
    meta_df["match_policy_version"] = match_policy
    meta_df.to_csv(meta_path, index=False)
    summary = (
        f"[dedup] mapping rows {n_in} -> {n_after_anat} (gene+anat) -> "
        f"{n_after_roi} (unique plane+func+gene); reused_roi_rows={n_roi_reuse}"
    )
    saved = f"[Suite2p] saved metadata: {meta_path} (rows={len(meta_df)})"
    log_lines = ([cache_message] if cache_message else []) + [summary] + plane_messages + [saved]
    return {
        "status": "exported",
        "message": saved,
        "meta_csv": meta_path,
        "meta_df": meta_df,
        "log_lines": log_lines,
    }


__all__ = [
    "TraceExportConfig",
    "export_suite2p_trace_metadata",
    "prepare_pairs_for_unique_cells",
    "resolve_conf_func_csv_analysis",
]
