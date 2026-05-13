"""Cohort Suite2p [23c] response-overview builders."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import os
import pickle
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .activity import ActivityConfig, build_response_bpi_tables, build_suite2p_response_seed_table
from .context import default_local_root, resolve_func_polarity
from .suite2p import (
    Suite2pStageConfig,
    Suite2pStimulusLockedDiagnosticConfig,
    build_suite2p_stimulus_locked_diagnostic,
    load_suite2p_stage,
)


def _default_nas_root() -> Path:
    if os.name == "nt":
        return Path(r"\\nasdcsr.unil.ch\RECHERCHE\FAC\FBM\CIG\jlarsch\default\D2c\07_Data")
    return Path("/Volumes/jlarsch/default/D2c/07_Data")


def parse_fish_ids_csv(fish_ids_csv: str) -> list[str]:
    """Parse comma-separated fish IDs, preserving order and dropping duplicates."""
    seen: set[str] = set()
    fish_ids: list[str] = []
    for part in str(fish_ids_csv or "").split(","):
        fish_id = part.strip()
        if not fish_id or fish_id in seen:
            continue
        seen.add(fish_id)
        fish_ids.append(fish_id)
    return fish_ids


@dataclass(frozen=True)
class CohortSuite2p23cConfig:
    fish_ids_csv: str = "L758_f02,L758_f03,L758_f04,L758_f06,L758_f07"
    data_mode: str = "local"
    matching_metadata_csv_override: str | Path | None = None
    cohort_outdir_override: str | Path | None = None
    pre_sec: float = 20.0
    post_sec: float = 50.0
    stim_time_scale: float = 1.0
    measure_start_block: int | str = 1
    measure_start_event: str = "start"
    remove_interblock_gaps: bool = True
    onset_delay_sec: float = 0.0
    window_edge_policy: str = "pad_nan"
    min_valid_frac: float = 0.5
    zscore_min_baseline_points: int = 20
    zscore_min_baseline_std: float = 1e-6
    suite2p_cells_only: bool = True
    min_trials_per_stimulus: int = 1
    heatmap_vmin: float = 0.0
    heatmap_vmax: float = 5.0
    stim_bar_alpha: float = 0.28
    response_zero_band: float = 0.50
    response_min_trials_per_class: int = 3
    response_denom_eps: float = 1e-6
    response_edge_policy: str = "pad_nan"
    response_min_valid_frac: float = 0.5
    response_stim_onset_delay_sec: float = 10.0
    response_min_auc: float = 0.05
    response_null_q: float = 0.99
    response_null_bootstrap_n: int = 2000
    response_null_min_windows: int = 20
    response_null_step_sec: float = 0.5
    response_rng_seed: int = 50
    skip_build_if_cached: bool = True
    force_build: bool = False
    verbose: bool = True


def cohort_suite2p_23c_cache_paths(outdir: str | Path) -> dict[str, Path]:
    outdir = Path(outdir)
    return {
        "fish_session_summary_csv": outdir / "cohort_23c_fish_session_summary.csv",
        "trace_summary_csv": outdir / "cohort_23c_responsive_trace_summary.csv",
        "response_counts_csv": outdir / "cohort_23c_response_counts.csv",
        "payload_pkl": outdir / "cohort_23c_trace_heatmap_payload.pkl",
    }


def _resolve_environment(cfg: CohortSuite2p23cConfig) -> dict[str, Any]:
    data_mode = str(cfg.data_mode).strip().lower()
    data_mode = "local" if data_mode == "local" else "nas"
    data_root = Path(default_local_root()) if data_mode == "local" else _default_nas_root()
    matching_metadata_csv = (
        Path(cfg.matching_metadata_csv_override)
        if cfg.matching_metadata_csv_override
        else (data_root / "matchingMetadata.csv" if data_mode == "local" else data_root / "Danin" / "matchingMetadata.csv")
    )
    outdir_default = (
        data_root / "cohort_outputs" / "suite2p_23c_response_overview"
        if data_mode == "local"
        else Path.cwd() / "cohort_outputs" / "suite2p_23c_response_overview"
    )
    outdir = Path(cfg.cohort_outdir_override) if cfg.cohort_outdir_override else outdir_default
    outdir.mkdir(parents=True, exist_ok=True)
    return {
        "DATA_MODE": data_mode,
        "DATA_ROOT": data_root,
        "MATCHING_METADATA_CSV": matching_metadata_csv,
        "COHORT_23C_FISH_IDS": parse_fish_ids_csv(cfg.fish_ids_csv),
        "COHORT_23C_OUTDIR": outdir,
        "COHORT_23C_OUTDIR_DEFAULT": outdir_default,
    }


def resolve_cohort_suite2p_23c_fish_dir(data_root: str | Path, fish_id: str) -> Path:
    """Resolve an owner-free fish folder from the cohort data root."""
    root = Path(data_root)
    fish_id_str = str(fish_id)
    candidates = [root / fish_id_str]
    if root.exists():
        owner_dirs = [path for path in sorted(root.iterdir()) if path.is_dir()]
        candidates.extend(path / fish_id_str for path in owner_dirs)
        candidates.extend(path / "Microscopy" / fish_id_str for path in owner_dirs)
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return root / fish_id_str


def _session_shade_color(base: tuple[float, float, float, float], session_index: int, n_sessions: int) -> tuple[float, float, float, float]:
    rgb = np.asarray(base[:3], dtype=float)
    if n_sessions <= 1:
        factor = 1.0
    else:
        factor = 0.68 + (0.32 * float(session_index) / float(max(1, n_sessions - 1)))
    shaded = 1.0 - ((1.0 - rgb) * factor)
    return (float(shaded[0]), float(shaded[1]), float(shaded[2]), float(base[3] if len(base) > 3 else 1.0))


def build_fish_session_color_map(trace_mean_df: pd.DataFrame) -> dict[str, Any]:
    import matplotlib.pyplot as plt

    if not isinstance(trace_mean_df, pd.DataFrame) or trace_mean_df.empty:
        return {}
    fish_ids = list(dict.fromkeys(trace_mean_df["fish_id"].astype(str).tolist()))
    cmap = plt.get_cmap("tab10") if len(fish_ids) <= 10 else plt.get_cmap("tab20")
    colors: dict[str, Any] = {}
    for fish_idx, fish_id in enumerate(fish_ids):
        base = cmap(fish_idx % cmap.N)
        sessions = list(dict.fromkeys(trace_mean_df.loc[trace_mean_df["fish_id"].astype(str) == fish_id, "session_label"].astype(str).tolist()))
        for session_idx, session in enumerate(sessions):
            colors[f"{fish_id}|{session}"] = _session_shade_color(base, session_idx, len(sessions))
    return colors


def aggregate_cohort_23c_trace_means(trace_df: pd.DataFrame, tvec: np.ndarray) -> pd.DataFrame:
    """Collapse [23c] per-ROI traces to one mean trace per fish/session/stimulus."""
    if not isinstance(trace_df, pd.DataFrame) or trace_df.empty:
        return pd.DataFrame()
    required = {"fish_id", "session_label", "stim_type", "plane_idx", "func_label", "mean_trace"}
    missing = required.difference(trace_df.columns)
    if missing:
        raise RuntimeError(f"cohort [23c] trace aggregation missing columns: {sorted(missing)}")
    rows: list[dict[str, Any]] = []
    tvec_arr = np.asarray(tvec, dtype=np.float32)
    for keys, group in trace_df.groupby(["fish_id", "session_label", "stim_type"], sort=False):
        fish_id, session_label, stim_type = keys
        traces = []
        for value in group["mean_trace"].tolist():
            arr = np.asarray(value, dtype=np.float32)
            if arr.shape == tvec_arr.shape:
                traces.append(arr)
        if not traces:
            continue
        stack = np.vstack(traces).astype(np.float32, copy=False)
        mean_trace = np.nanmean(stack, axis=0).astype(np.float32, copy=False)
        post_mask = tvec_arr >= 0
        rows.append(
            {
                "fish_id": str(fish_id),
                "session_label": str(session_label),
                "stim_type": str(stim_type),
                "n_trace_rows": int(len(group)),
                "n_responsive_rois": int(group[["plane_idx", "func_label"]].drop_duplicates().shape[0]),
                "mean_trace": mean_trace,
                "mean_z_pre": float(np.nanmean(mean_trace[~post_mask])) if np.any(~post_mask) else np.nan,
                "mean_z_post": float(np.nanmean(mean_trace[post_mask])) if np.any(post_mask) else np.nan,
                "peak_z_post": float(np.nanmax(mean_trace[post_mask])) if np.any(post_mask) else np.nan,
            }
        )
    return pd.DataFrame(rows)


def _activity_config(cfg: CohortSuite2p23cConfig) -> ActivityConfig:
    return ActivityConfig(
        zero_band=float(cfg.response_zero_band),
        min_trials_per_class=int(cfg.response_min_trials_per_class),
        denom_eps=float(cfg.response_denom_eps),
        edge_policy=str(cfg.response_edge_policy),
        min_valid_frac=float(cfg.response_min_valid_frac),
        stim_onset_delay_sec=float(cfg.response_stim_onset_delay_sec),
        response_min_auc=float(cfg.response_min_auc),
        response_null_q=float(cfg.response_null_q),
        response_null_bootstrap_n=int(cfg.response_null_bootstrap_n),
        response_null_min_windows=int(cfg.response_null_min_windows),
        response_null_step_sec=float(cfg.response_null_step_sec),
        response_rng_seed=int(cfg.response_rng_seed),
    )


def _diagnostic_config(cfg: CohortSuite2p23cConfig) -> Suite2pStimulusLockedDiagnosticConfig:
    return Suite2pStimulusLockedDiagnosticConfig(
        pre_sec=float(cfg.pre_sec),
        post_sec=float(cfg.post_sec),
        stim_time_scale=float(cfg.stim_time_scale),
        measure_start_block=cfg.measure_start_block,
        measure_start_event=str(cfg.measure_start_event),
        remove_interblock_gaps=bool(cfg.remove_interblock_gaps),
        onset_delay_sec=float(cfg.onset_delay_sec),
        window_edge_policy=str(cfg.window_edge_policy),
        min_valid_frac=float(cfg.min_valid_frac),
        zscore_min_baseline_points=int(cfg.zscore_min_baseline_points),
        zscore_min_baseline_std=float(cfg.zscore_min_baseline_std),
        suite2p_cells_only=bool(cfg.suite2p_cells_only),
        min_trials_per_stimulus=int(cfg.min_trials_per_stimulus),
        heatmap_vmin=float(cfg.heatmap_vmin),
        heatmap_vmax=float(cfg.heatmap_vmax),
        stim_bar_alpha=float(cfg.stim_bar_alpha),
        filter_trace_panels_response_active=True,
        save_figures=False,
        show_figures=False,
        verbose=bool(cfg.verbose),
    )


def _merge_response_calls(
    *,
    fish_dir: Path,
    fish_id: str,
    suite2p_by_ref_idx: dict[int, dict[str, Any]],
    trace_df: pd.DataFrame,
    activity_config: ActivityConfig,
) -> tuple[pd.DataFrame, pd.DataFrame, str | None]:
    try:
        seed_df, dff_map = build_suite2p_response_seed_table(suite2p_by_ref_idx, fish_id=str(fish_id))
        if seed_df.empty:
            return trace_df.copy(), pd.DataFrame(), "response seed table empty"
        response_result = build_response_bpi_tables(
            seed_df,
            fish_dir=fish_dir,
            fish_id=str(fish_id),
            suite2p_dff_map=dff_map,
            config=activity_config,
        )
        scored = response_result["scored_bpi_df"].copy()
        if scored.empty:
            return trace_df.copy(), scored, "response scored table empty"
        response_cols = [
            "plane_idx",
            "func_label",
            "response_is_active",
            "response_class",
            "response_summary_class",
            "bout_response_pass",
            "cont_response_pass",
            "bpi_category",
        ]
        lookup_cols = [column for column in response_cols if column in scored.columns]
        if not {"plane_idx", "func_label"}.issubset(lookup_cols):
            return trace_df.copy(), scored, "response scored table missing ROI keys"
        lookup = scored[lookup_cols].drop_duplicates(subset=["plane_idx", "func_label"], keep="last")
        out = trace_df.merge(lookup, on=["plane_idx", "func_label"], how="left")
        if "response_is_active" in out.columns:
            out["response_is_active"] = out["response_is_active"].fillna(False).astype(bool)
        return out, scored, None
    except Exception as exc:
        return trace_df.copy(), pd.DataFrame(), str(exc)


def _source_summary_rows(
    *,
    fish_id: str,
    source_df: pd.DataFrame,
    trace_df: pd.DataFrame,
    response_error: str | None,
    status: str,
    notes: str,
) -> list[dict[str, Any]]:
    if not isinstance(source_df, pd.DataFrame) or source_df.empty:
        return [
            {
                "fish_id": str(fish_id),
                "session_label": None,
                "status": status,
                "notes": notes,
                "response_error": response_error,
            }
        ]
    rows: list[dict[str, Any]] = []
    roi_df = trace_df.copy() if isinstance(trace_df, pd.DataFrame) else pd.DataFrame()
    if not roi_df.empty and "response_is_active" not in roi_df.columns:
        roi_df["response_is_active"] = False
    for session, group in source_df.groupby("session_label", dropna=False, sort=False):
        session_str = str(session)
        roi_sub = roi_df[roi_df["session_label"].astype(str) == session_str].copy() if not roi_df.empty else pd.DataFrame()
        n_response = 0
        if not roi_sub.empty and "response_is_active" in roi_sub.columns:
            n_response = int(roi_sub.loc[roi_sub["response_is_active"].astype(bool), ["plane_idx", "func_label"]].drop_duplicates().shape[0])
        n_z = int(pd.to_numeric(group.get("n_zscore_valid", pd.Series(dtype=float)), errors="coerce").fillna(0).sum())
        stimuli = sorted({item.strip() for value in group.get("stimulus_types", pd.Series(dtype=str)).dropna().astype(str) for item in value.split(",") if item.strip()})
        rows.append(
            {
                "fish_id": str(fish_id),
                "session_label": session_str,
                "status": status,
                "notes": notes,
                "n_planes": int(group["plane_idx"].nunique()) if "plane_idx" in group.columns else int(len(group)),
                "n_suite2p_cells": int(pd.to_numeric(group.get("n_suite2p_cells", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()),
                "n_zscore_valid": n_z,
                "n_response_active_rois": int(n_response),
                "response_fraction": float(n_response / n_z) if n_z else np.nan,
                "stimuli_detected": ", ".join(stimuli),
                "response_error": response_error,
            }
        )
    return rows


def load_cohort_suite2p_23c_outputs_from_disk(outdir: str | Path) -> dict[str, Any]:
    paths = cohort_suite2p_23c_cache_paths(outdir)
    payload: dict[str, Any] = {}
    if paths["fish_session_summary_csv"].exists():
        payload["fish_session_summary_df"] = pd.read_csv(paths["fish_session_summary_csv"])
    if paths["trace_summary_csv"].exists():
        payload["trace_summary_df"] = pd.read_csv(paths["trace_summary_csv"])
    if paths["response_counts_csv"].exists():
        try:
            payload["response_counts_df"] = pd.read_csv(paths["response_counts_csv"])
        except pd.errors.EmptyDataError:
            payload["response_counts_df"] = pd.DataFrame()
    if paths["payload_pkl"].exists():
        with paths["payload_pkl"].open("rb") as handle:
            payload.update(pickle.load(handle))
    return payload


def build_cohort_suite2p_23c_stage(config: CohortSuite2p23cConfig | None = None) -> dict[str, Any]:
    cfg = config or CohortSuite2p23cConfig()
    env = _resolve_environment(cfg)
    outdir = Path(env["COHORT_23C_OUTDIR"])
    paths = cohort_suite2p_23c_cache_paths(outdir)

    if bool(cfg.skip_build_if_cached) and not bool(cfg.force_build):
        loaded = load_cohort_suite2p_23c_outputs_from_disk(outdir)
        if {"trace_mean_df", "tvec", "heatmap_payloads"}.issubset(loaded):
            bindings = dict(env)
            bindings.update(loaded)
            bindings["cohort_suite2p_23c_cache_paths"] = paths
            if cfg.verbose:
                print(f"[cohort-23c] reusing cached outputs from {outdir}")
            return {"bindings": bindings, "state": {"cohort_build_skipped": True}, "config": asdict(cfg)}

    fish_ids = list(env["COHORT_23C_FISH_IDS"])
    if not fish_ids:
        raise RuntimeError("[cohort-23c] no fish IDs configured")

    diag_cfg = _diagnostic_config(cfg)
    activity_cfg = _activity_config(cfg)
    tvec_ref: np.ndarray | None = None
    stim_order: list[str] = []
    duration_by_stim: dict[str, list[float]] = {}
    all_active_trace_rows: list[pd.DataFrame] = []
    summary_rows: list[dict[str, Any]] = []
    response_count_rows: list[pd.DataFrame] = []
    heatmap_payloads: dict[str, dict[str, Any]] = {}

    for fish_id in fish_ids:
        fish_dir = resolve_cohort_suite2p_23c_fish_dir(env["DATA_ROOT"], fish_id)
        suite2p_root = fish_dir / "03_analysis" / "functional" / "suite2P"
        if not fish_dir.exists():
            summary_rows.extend(_source_summary_rows(fish_id=fish_id, source_df=pd.DataFrame(), trace_df=pd.DataFrame(), response_error=None, status="skip", notes="fish_dir missing"))
            continue
        if not suite2p_root.exists():
            summary_rows.extend(_source_summary_rows(fish_id=fish_id, source_df=pd.DataFrame(), trace_df=pd.DataFrame(), response_error=None, status="skip", notes="suite2P dir missing"))
            continue
        polarity, polarity_source = resolve_func_polarity(
            fish_id,
            env["MATCHING_METADATA_CSV"],
            fish_dir=fish_dir,
        )
        try:
            s2p_state = load_suite2p_stage(
                plane_refs=[],
                suite2p_root=suite2p_root,
                fish_id=fish_id,
                polarity=polarity,
                polarity_source=polarity_source,
                config=Suite2pStageConfig(verbose=False),
            )
            result = build_suite2p_stimulus_locked_diagnostic(
                fish_dir=fish_dir,
                fish_id=fish_id,
                suite2p_by_ref_idx=s2p_state["suite2p_by_ref_idx"],
                config=diag_cfg,
            )
            if tvec_ref is None:
                tvec_ref = np.asarray(result["tvec"], dtype=np.float32)
            elif tvec_ref.shape != np.asarray(result["tvec"]).shape or not np.allclose(tvec_ref, np.asarray(result["tvec"]), atol=1e-6):
                raise RuntimeError("fish [23c] tvec does not match cohort tvec")
            for stim in result["stim_order"]:
                if stim not in stim_order:
                    stim_order.append(str(stim))
            for stim, duration in result["duration_by_stim"].items():
                duration_by_stim.setdefault(str(stim), []).append(float(duration))
            trace_df, scored_df, response_error = _merge_response_calls(
                fish_dir=fish_dir,
                fish_id=fish_id,
                suite2p_by_ref_idx=s2p_state["suite2p_by_ref_idx"],
                trace_df=result["trace_df"],
                activity_config=activity_cfg,
            )
            if not scored_df.empty:
                scored_out = scored_df.copy()
                scored_out["fish_id"] = str(fish_id)
                count_cols = [
                    column
                    for column in ("fish_id", "response_summary_class", "response_class", "bpi_category")
                    if column in scored_out.columns
                ]
                response_count_rows.append(scored_out.groupby(count_cols, dropna=False).size().reset_index(name="n_rois"))
            status = "ok"
            notes = "response-active traces"
            active_trace_df = pd.DataFrame()
            if response_error is None and "response_is_active" in trace_df.columns:
                active_trace_df = trace_df[trace_df["response_is_active"].astype(bool)].copy()
            if active_trace_df.empty:
                active_trace_df = trace_df.copy()
                notes = "response unavailable fallback: all valid Suite2p-cell traces"
                status = "warn" if response_error else "ok"
            if not active_trace_df.empty:
                active_trace_df["fish_id"] = str(fish_id)
                all_active_trace_rows.append(active_trace_df)
            heatmap_payloads[str(fish_id)] = {
                "matrix": result["full_session_heatmap_matrix"],
                "row_df": result["full_session_heatmap_rows"],
                "stimulus_spans": result["full_session_stimulus_spans"],
                "block_starts": result["full_session_block_starts"],
                "session_colors": result["session_colors"],
            }
            summary_rows.extend(
                _source_summary_rows(
                    fish_id=fish_id,
                    source_df=result["source_df"],
                    trace_df=trace_df,
                    response_error=response_error,
                    status=status,
                    notes=notes,
                )
            )
            if cfg.verbose:
                print(f"[cohort-23c] {fish_id}: {status}; {notes}")
        except Exception as exc:
            summary_rows.extend(
                _source_summary_rows(
                    fish_id=fish_id,
                    source_df=pd.DataFrame(),
                    trace_df=pd.DataFrame(),
                    response_error=None,
                    status="skip",
                    notes=str(exc),
                )
            )
            if cfg.verbose:
                print(f"[cohort-23c] {fish_id}: skip -> {exc}")

    if tvec_ref is None:
        raise RuntimeError("[cohort-23c] no valid fish produced Suite2p [23c] traces")
    trace_panel_df = pd.concat(all_active_trace_rows, ignore_index=True) if all_active_trace_rows else pd.DataFrame()
    trace_mean_df = aggregate_cohort_23c_trace_means(trace_panel_df, tvec_ref)
    fish_session_colors = build_fish_session_color_map(trace_mean_df)
    duration_summary = {
        stim: float(np.nanmedian(np.asarray(values, dtype=float)))
        for stim, values in duration_by_stim.items()
        if values and np.isfinite(np.nanmedian(np.asarray(values, dtype=float)))
    }
    fish_session_summary_df = pd.DataFrame(summary_rows)
    response_counts_df = pd.concat(response_count_rows, ignore_index=True) if response_count_rows else pd.DataFrame()
    if response_counts_df.empty:
        response_counts_df = pd.DataFrame(columns=["fish_id"])
    trace_summary_df = trace_mean_df.drop(columns=["mean_trace"], errors="ignore").copy()

    fish_session_summary_df.to_csv(paths["fish_session_summary_csv"], index=False)
    trace_summary_df.to_csv(paths["trace_summary_csv"], index=False)
    response_counts_df.to_csv(paths["response_counts_csv"], index=False)
    with paths["payload_pkl"].open("wb") as handle:
        pickle.dump(
            {
                "trace_mean_df": trace_mean_df,
                "trace_panel_df": trace_panel_df,
                "tvec": tvec_ref,
                "stim_order": stim_order,
                "duration_by_stim": duration_summary,
                "fish_session_colors": fish_session_colors,
                "heatmap_payloads": heatmap_payloads,
            },
            handle,
        )

    bindings = dict(env)
    bindings.update(
        {
            "fish_session_summary_df": fish_session_summary_df,
            "trace_summary_df": trace_summary_df,
            "response_counts_df": response_counts_df,
            "trace_mean_df": trace_mean_df,
            "trace_panel_df": trace_panel_df,
            "tvec": tvec_ref,
            "stim_order": stim_order,
            "duration_by_stim": duration_summary,
            "fish_session_colors": fish_session_colors,
            "heatmap_payloads": heatmap_payloads,
            "cohort_suite2p_23c_cache_paths": paths,
        }
    )
    return {"bindings": bindings, "state": {"cohort_build_skipped": False}, "config": asdict(cfg)}


__all__ = [
    "CohortSuite2p23cConfig",
    "aggregate_cohort_23c_trace_means",
    "build_cohort_suite2p_23c_stage",
    "build_fish_session_color_map",
    "cohort_suite2p_23c_cache_paths",
    "load_cohort_suite2p_23c_outputs_from_disk",
    "parse_fish_ids_csv",
    "resolve_cohort_suite2p_23c_fish_dir",
]
