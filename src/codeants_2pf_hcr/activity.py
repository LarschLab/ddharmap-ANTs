"""Response/BPI helpers shared across late-stage notebook cells."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .stimulus import (
    StimulusConfig,
    build_null_window_start_map,
    build_prestim_trial_windows,
    classify_bpi_stim_type,
    classify_stim_type,
    compute_zscore_stats,
    effective_motion_window,
    map_stimulus_context_to_trace_grid,
    resolve_plane_stimulus_contexts,
)
from .suite2p import infer_frame_rate_from_detail, load_suite2p_dff_map


def run_single_fish_cell_50ia_stage(namespace: dict[str, Any]) -> None:
    from .single_fish_notebook_stages import run_single_fish_cell_50ia_stage as _run_stage

    _run_stage(namespace)


@dataclass(frozen=True)
class ActivityConfig:
    active_class: str = "Active neurons"
    inactive_class: str = "Low-quality traces"
    response_bout: str = "bout-responsive"
    response_cont: str = "continuous-responsive"
    response_both: str = "both-responsive"
    response_low: str = "low activity"
    response_unavailable: str = "response unavailable"
    # Legacy configuration compatibility; newly scored rows never emit this class.
    bpi_weak: str = "weak-response"
    zero_band: float = 0.50
    min_trials_per_class: int = 3
    denom_eps: float = 1e-6
    edge_policy: str = "pad_nan"
    min_valid_frac: float = 0.5
    stim_onset_delay_sec: float = 10.0
    response_window_sec: float = 12.5
    response_min_auc: float = 0.05
    response_null_q: float = 0.99
    response_null_bootstrap_n: int = 2000
    response_null_min_windows: int = 20
    response_null_step_sec: float = 0.5
    response_rng_seed: int = 50
    stim_time_scale: float = 1.0
    measure_start_block: int | str = 1
    measure_start_event: str = "start"
    remove_interblock_gaps: bool = True
    dfof_baseline_pct: float = 10.0
    dfof_eps: float = 1e-6
    zscore_min_points: int = 200
    zscore_sigma_eps: float = 1e-6


@dataclass(frozen=True)
class SingleFishBpiDiagnosticsConfig:
    bpi_index_col: str = "bpi"
    zero_band: float | None = None
    n_activity_bins: int = 6
    response_low: str = "low activity"
    response_unavailable: str = "response unavailable"
    response_summary_responsive: str = "Responsive neurons"
    response_summary_low: str = "Low activity"
    response_summary_unavailable: str = "Response unavailable"


def _as_bool_series(series_in: Any) -> pd.Series:
    s = pd.Series(series_in)
    if pd.api.types.is_bool_dtype(s):
        return s.fillna(False).astype(bool)
    if pd.api.types.is_numeric_dtype(s):
        return s.fillna(0).astype(float) != 0
    return s.astype(str).str.strip().str.lower().isin({"1", "true", "t", "yes", "y"})


def _suite2p_keep_mask(plane: dict[str, Any], n_rois: int) -> np.ndarray:
    if plane.get("iscell") is not None:
        arr = np.asarray(plane.get("iscell"))
        if arr.ndim >= 2 and arr.shape[0] >= int(n_rois):
            return arr[: int(n_rois), 0].astype(bool)
    if plane.get("iscell_keep") is not None:
        arr = np.asarray(plane.get("iscell_keep"), dtype=bool)
        if arr.shape[0] >= int(n_rois):
            return arr[: int(n_rois)].astype(bool)
    return np.ones(int(n_rois), dtype=bool)


def build_suite2p_response_seed_table(
    suite2p_by_ref_idx: dict[int, dict[str, Any]],
    *,
    fish_id: str | None = None,
    active_class: str = "Active neurons",
    inactive_class: str = "Low-quality traces",
) -> tuple[pd.DataFrame, dict[int, dict[str, Any]]]:
    """Build a pre-identity ROI table and dF/F map from loaded Suite2p planes."""
    rows: list[dict[str, Any]] = []
    dff_map: dict[int, dict[str, Any]] = {}
    for plane_idx in sorted(int(k) for k in suite2p_by_ref_idx.keys()):
        plane = suite2p_by_ref_idx.get(int(plane_idx), {}) or {}
        dff = plane.get("dff")
        if dff is None:
            continue
        dff_arr = np.asarray(dff, dtype=np.float32)
        if dff_arr.ndim != 2:
            continue
        n_rois = int(dff_arr.shape[0])
        keep = _suite2p_keep_mask(plane, n_rois)
        dff_map[int(plane_idx)] = {
            "dff": dff_arr,
            "plane_dir": plane.get("plane_dir"),
            "ops": plane.get("ops", {}),
        }
        for roi_idx in range(n_rois):
            is_cell = bool(keep[int(roi_idx)]) if int(roi_idx) < len(keep) else False
            rows.append(
                {
                    "fish_id": fish_id,
                    "plane": f"plane{int(plane_idx)}",
                    "plane_idx": int(plane_idx),
                    "func_label": int(roi_idx) + 1,
                    "roi_idx": int(roi_idx),
                    "func_source": str(plane.get("plane_dir")) if plane.get("plane_dir") is not None else None,
                    "activity_class": active_class if is_cell else inactive_class,
                    "is_active": is_cell,
                }
            )
    return pd.DataFrame(rows), dff_map


def _extract_window(trace: np.ndarray, idx0: int, idx1: int, *, mode: str, min_valid_frac: float) -> tuple[np.ndarray | None, int, int]:
    n_frames = int(trace.shape[0])
    win_len = int(idx1 - idx0)
    if win_len <= 0:
        return None, 0, 0
    if mode == "strict":
        if idx0 < 0 or idx1 > n_frames:
            return None, 0, win_len
        seg = trace[idx0:idx1]
        n_valid = int(np.isfinite(seg).sum())
        return seg, n_valid, win_len
    seg = np.full(win_len, np.nan, dtype=np.float32)
    src0 = max(int(idx0), 0)
    src1 = min(int(idx1), n_frames)
    if src1 > src0:
        dst0 = src0 - int(idx0)
        seg[dst0 : dst0 + (src1 - src0)] = trace[src0:src1]
    n_valid = int(np.isfinite(seg).sum())
    min_required = max(1, int(np.ceil(float(min_valid_frac) * float(win_len))))
    if n_valid < min_required:
        return None, n_valid, win_len
    return seg, n_valid, win_len


def _merge_scored_response_table(
    detail: pd.DataFrame,
    scored_bpi_df: pd.DataFrame,
    *,
    cfg: ActivityConfig,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    response_summary_unavailable = "Response unavailable"
    bpi_unavailable = cfg.response_unavailable
    merge_cols = [
        "plane_idx",
        "func_label",
        "n_bout_trials",
        "n_cont_trials",
        "n_bout_trials_z",
        "n_cont_trials_z",
        "mean_bout_dff",
        "mean_cont_dff",
        "response_magnitude_auc",
        "response_absolute_magnitude_auc",
        "mean_bout_zdff",
        "mean_cont_zdff",
        "mean_bout_auc_dff",
        "mean_cont_auc_dff",
        "bout_null_q99_auc",
        "cont_null_q99_auc",
        "bout_null_upper_auc",
        "cont_null_upper_auc",
        "bout_null_lower_auc",
        "cont_null_lower_auc",
        "bout_positive_response_pass",
        "bout_negative_response_pass",
        "cont_positive_response_pass",
        "cont_negative_response_pass",
        "bout_response_pass",
        "cont_response_pass",
        "bout_response_valence",
        "cont_response_valence",
        "response_valence_pattern",
        "response_has_suppression",
        "response_valence_complete",
        "response_is_active",
        "response_class",
        "response_summary_class",
        "response_auc_threshold",
        "response_null_quantile",
        "response_null_upper_quantile",
        "response_null_lower_quantile",
        "response_null_bootstrap_n",
        "denom",
        "denom_z",
        "bpi",
        "bpi_z",
        "signed_condition_contrast",
        "signed_condition_contrast_denom",
        "signed_condition_contrast_data_available",
        "activity_mag",
        "bpi_preference",
        "bpi_status",
        "bpi_data_available",
        "bpi_category",
        "bpi_zero_band",
        "bpi_activity_threshold",
    ]
    available_cols = [col for col in merge_cols if col in scored_bpi_df.columns]
    scored = scored_bpi_df[available_cols].copy()
    scored["plane_idx"] = pd.to_numeric(scored["plane_idx"], errors="coerce").astype("Int64")
    scored["func_label"] = pd.to_numeric(scored["func_label"], errors="coerce").astype("Int64")
    scored = scored.dropna(subset=["plane_idx", "func_label"]).drop_duplicates(
        subset=["plane_idx", "func_label"],
        keep="last",
    )
    detail = detail.merge(scored, on=["plane_idx", "func_label"], how="left")
    detail["response_is_active"] = _as_bool_series(detail.get("response_is_active", False)).astype(bool)
    if "bout_response_pass" not in detail.columns:
        detail["bout_response_pass"] = False
    if "cont_response_pass" not in detail.columns:
        detail["cont_response_pass"] = False
    bool_defaults = [
        "bout_positive_response_pass",
        "bout_negative_response_pass",
        "cont_positive_response_pass",
        "cont_negative_response_pass",
        "bout_response_pass",
        "cont_response_pass",
        "response_has_suppression",
        "response_valence_complete",
        "signed_condition_contrast_data_available",
    ]
    for col in bool_defaults:
        if col not in detail.columns:
            detail[col] = False
        detail[col] = _as_bool_series(detail[col]).astype(bool)
    for col in ["bout_response_valence", "cont_response_valence", "response_valence_pattern"]:
        if col not in detail.columns:
            detail[col] = "unavailable"
        detail[col] = detail[col].fillna("unavailable")
    for col in [
        "response_absolute_magnitude_auc",
        "bout_null_upper_auc",
        "cont_null_upper_auc",
        "bout_null_lower_auc",
        "cont_null_lower_auc",
        "signed_condition_contrast",
        "signed_condition_contrast_denom",
    ]:
        if col not in detail.columns:
            detail[col] = np.nan
    detail["response_class"] = detail["response_class"].fillna(cfg.response_unavailable)
    if "bpi_preference" not in detail.columns:
        detail["bpi_preference"] = "unavailable"
    detail["response_summary_class"] = detail["response_summary_class"].fillna(response_summary_unavailable)
    detail["bpi_category"] = detail["bpi_category"].fillna(bpi_unavailable)
    if "bpi_status" not in detail.columns:
        detail["bpi_status"] = "response_unavailable"
    detail["bpi_status"] = detail["bpi_status"].fillna("response_unavailable")
    if "bpi_data_available" not in detail.columns:
        detail["bpi_data_available"] = False
    detail["bpi_data_available"] = _as_bool_series(detail.get("bpi_data_available", False)).astype(bool)
    if "bpi_zero_band" not in detail.columns:
        detail["bpi_zero_band"] = float(cfg.zero_band)
    detail["bpi_zero_band"] = detail["bpi_zero_band"].fillna(float(cfg.zero_band))
    if "bpi_activity_threshold" not in detail.columns:
        detail["bpi_activity_threshold"] = float(cfg.response_min_auc)
    detail["bpi_activity_threshold"] = detail["bpi_activity_threshold"].fillna(float(cfg.response_min_auc))
    if "response_auc_threshold" not in detail.columns:
        detail["response_auc_threshold"] = float(cfg.response_min_auc)
    detail["response_auc_threshold"] = detail["response_auc_threshold"].fillna(float(cfg.response_min_auc))
    if "response_null_quantile" not in detail.columns:
        detail["response_null_quantile"] = float(cfg.response_null_q)
    detail["response_null_quantile"] = detail["response_null_quantile"].fillna(float(cfg.response_null_q))
    two_sided_tail = (1.0 - float(cfg.response_null_q)) / 2.0
    if "response_null_upper_quantile" not in detail.columns:
        detail["response_null_upper_quantile"] = 1.0 - two_sided_tail
    detail["response_null_upper_quantile"] = detail["response_null_upper_quantile"].fillna(1.0 - two_sided_tail)
    if "response_null_lower_quantile" not in detail.columns:
        detail["response_null_lower_quantile"] = two_sided_tail
    detail["response_null_lower_quantile"] = detail["response_null_lower_quantile"].fillna(two_sided_tail)
    if "response_null_bootstrap_n" not in detail.columns:
        detail["response_null_bootstrap_n"] = int(cfg.response_null_bootstrap_n)
    detail["response_null_bootstrap_n"] = detail["response_null_bootstrap_n"].fillna(int(cfg.response_null_bootstrap_n))
    summary_df = (
        detail.groupby(["response_summary_class", "bpi_category"], as_index=False)
        .size()
        .rename(columns={"size": "n_rois"})
    )
    summary_df["n_response_total"] = summary_df.groupby("response_summary_class")["n_rois"].transform("sum")
    summary_df["n_all_segmented"] = int(len(detail))
    summary_df["pct_within_response_class"] = np.where(
        summary_df["n_response_total"] > 0,
        100.0 * summary_df["n_rois"] / summary_df["n_response_total"],
        np.nan,
    )
    summary_df["pct_of_all_segmented"] = np.where(
        summary_df["n_all_segmented"] > 0,
        100.0 * summary_df["n_rois"] / summary_df["n_all_segmented"],
        np.nan,
    )
    return detail, summary_df


def _compute_bootstrap_null_quantiles(
    dff: np.ndarray,
    stim_events: list[dict[str, Any]],
    null_starts_by_duration: dict[int, np.ndarray],
    *,
    fps: float,
    n_boot: int,
    q: float,
    seed: int,
) -> dict[str, np.ndarray]:
    n_roi = int(dff.shape[0])
    out = {
        "bout": np.full(n_roi, np.nan, dtype=np.float32),
        "continuous": np.full(n_roi, np.nan, dtype=np.float32),
        "bout_upper": np.full(n_roi, np.nan, dtype=np.float32),
        "continuous_upper": np.full(n_roi, np.nan, dtype=np.float32),
        "bout_lower": np.full(n_roi, np.nan, dtype=np.float32),
        "continuous_lower": np.full(n_roi, np.nan, dtype=np.float32),
    }
    if dff is None or getattr(dff, "ndim", 0) != 2 or not stim_events:
        return out

    csum = np.concatenate(
        [np.zeros((n_roi, 1), dtype=np.float32), np.cumsum(np.asarray(dff, dtype=np.float32), axis=1, dtype=np.float32)],
        axis=1,
    )
    auc_cache: dict[int, np.ndarray] = {}
    for class_idx, stim_class in enumerate(["bout", "continuous"]):
        class_events = [event for event in stim_events if str(event.get("stim_class", "")) == stim_class]
        if not class_events:
            continue
        boot = np.zeros((n_roi, int(n_boot)), dtype=np.float32)
        usable = True
        rng = np.random.default_rng(int(seed) + int(class_idx))
        for event in class_events:
            duration = int(event["duration_frames"])
            starts = null_starts_by_duration.get(duration)
            if starts is None or len(starts) < 2:
                usable = False
                break
            if duration not in auc_cache:
                sums = csum[:, starts + duration] - csum[:, starts]
                auc_cache[duration] = sums / float(fps)
            auc_mat = auc_cache[duration]
            baseline_choices = rng.integers(0, auc_mat.shape[1], size=int(n_boot))
            response_choices = rng.integers(0, auc_mat.shape[1] - 1, size=int(n_boot))
            response_choices += response_choices >= baseline_choices
            boot += auc_mat[:, response_choices] - auc_mat[:, baseline_choices]
        if not usable:
            continue
        boot /= float(len(class_events))
        two_sided_tail = (1.0 - float(q)) / 2.0
        out[stim_class] = np.quantile(boot, float(q), axis=1).astype(np.float32)
        out[f"{stim_class}_upper"] = np.quantile(boot, 1.0 - two_sided_tail, axis=1).astype(np.float32)
        out[f"{stim_class}_lower"] = np.quantile(boot, two_sided_tail, axis=1).astype(np.float32)
    return out


def _build_activity_stimulus_context(
    ctx: dict[str, Any],
    *,
    fps: float,
    cfg: ActivityConfig,
) -> dict[str, Any]:
    df_evt = ctx["df_evt"]
    df_stim = ctx["df_stim"]
    raw_prestim = build_prestim_trial_windows(df_evt, fps, cfg.stim_onset_delay_sec, tag="[activity]")
    raw_by_trial = {(str(item["block"]), int(item["stim_idx"])): item for item in raw_prestim}
    response_frames = int(round(float(cfg.response_window_sec) * float(fps)))
    if response_frames <= 0:
        raise ValueError("response_window_sec must be positive")
    # The planned acquisition grid and mapped raw log may have different block offsets.
    # Preserve the measured pause duration, but anchor it to the same onset used for scoring.
    prestim_trial_windows = []
    for _, stimulus in df_stim.iterrows():
        key = (str(stimulus["block"]), int(stimulus["stim_idx"]))
        raw_window = raw_by_trial.get(key)
        if raw_window is None:
            raise RuntimeError(f"missing prestimulus pause for {key}")
        end = int(round(float(stimulus["start"]) * float(fps)))
        duration = int(raw_window["duration_frames"])
        prestim_trial_windows.append({
            "block": key[0], "stim_idx": key[1],
            "idx0": end - duration, "idx1": end, "duration_frames": duration,
        })
    prestim_by_trial = {
        (str(window["block"]), int(window["stim_idx"])): window
        for window in prestim_trial_windows
    }
    baseline_windows = sorted((int(item["idx0"]), int(item["idx1"])) for item in prestim_trial_windows)
    df_stim_work = df_stim.copy()
    df_stim_work["presented_stim_mode"] = df_stim_work["type"].map(classify_stim_type)
    df_stim_work["bpi_stim_class"] = df_stim_work["type"].map(classify_bpi_stim_type)
    if df_stim_work.empty:
        raise RuntimeError("No presented stimuli available for response/BPI computation")

    presented_stim_events: list[dict[str, Any]] = []
    for _, row in df_stim_work.iterrows():
        t0, t1, duration = effective_motion_window(
            row.get("start", np.nan),
            row.get("duration", np.nan),
            row.get("end", np.nan),
            float(cfg.stim_onset_delay_sec),
        )
        if not np.isfinite(t0) or not np.isfinite(t1) or not np.isfinite(duration):
            continue
        idx0 = int(round(t0 * float(fps)))
        idx1 = idx0 + response_frames
        if idx1 <= idx0:
            continue
        bpi_stim_class = row.get("bpi_stim_class", None)
        trial_key = (str(row.get("block")), int(row.get("stim_idx")))
        prestim = prestim_by_trial.get(trial_key)
        if prestim is None:
            raise RuntimeError(f"missing prestimulus pause for {trial_key}")
        baseline_idx1 = int(round(float(row["start"]) * float(fps)))
        baseline_idx0 = baseline_idx1 - response_frames
        if baseline_idx0 < int(prestim["idx0"]) or baseline_idx1 > int(prestim["idx1"]):
            raise RuntimeError(f"prestimulus pause is shorter than the matched response window for {trial_key}")
        next_prestim = [int(window["idx0"]) for window in prestim_trial_windows if int(window["idx0"]) > baseline_idx1]
        if next_prestim and idx1 > min(next_prestim):
            raise RuntimeError(f"response window overlaps the next prestimulus pause for {trial_key}")
        used_for_bpi = str(bpi_stim_class) in {"bout", "continuous"}
        presented_stim_events.append(
            {
                "block": row.get("block", pd.NA),
                "stim_idx": int(row.get("stim_idx", -1)) if pd.notna(row.get("stim_idx", np.nan)) else -1,
                "stim_type": str(row.get("type", "")),
                "stim_class": str(bpi_stim_class) if used_for_bpi else "not_used_for_bpi",
                "presented_stim_mode": str(row.get("presented_stim_mode", "")),
                "bpi_stim_class": str(bpi_stim_class) if used_for_bpi else "",
                "used_for_bpi": bool(used_for_bpi),
                "idx0": idx0,
                "idx1": idx1,
                "duration_s": float(idx1 - idx0) / float(fps),
                "baseline_idx0": baseline_idx0,
                "baseline_idx1": baseline_idx1,
                "duration_frames": int(idx1 - idx0),
                "session_label": ctx.get("session_label", pd.NA),
            }
        )
    stim_events = [event for event in presented_stim_events if bool(event["used_for_bpi"])]
    if not stim_events:
        raise RuntimeError("No LB/RB or LC/RC events available for response/BPI computation")

    step_frames = max(1, int(round(float(cfg.response_null_step_sec) * float(fps))))
    null_starts_by_duration = build_null_window_start_map(
        prestim_trial_windows,
        [event["duration_frames"] for event in stim_events],
        step_frames=step_frames,
        min_windows=int(cfg.response_null_min_windows),
    )
    return {
        "fps": float(fps),
        "df_evt": df_evt,
        "df_stim": df_stim,
        "stim_events": stim_events,
        "presented_stim_events": presented_stim_events,
        "stim_source": str(ctx.get("log_path")),
        "meta_path": ctx.get("meta_path"),
        "session_label": ctx.get("session_label"),
        "baseline_windows": baseline_windows,
        "prestim_trial_windows": prestim_trial_windows,
        "null_starts_by_duration": null_starts_by_duration,
    }


def build_scored_stimulus_windows_table(
    *,
    fish_id: str,
    df_stim: pd.DataFrame,
    stim_events: Sequence[Mapping[str, Any]],
    fps_by_session: Mapping[str, float] | None = None,
    stim_events_by_plane: Mapping[int, Sequence[Mapping[str, Any]]] | None = None,
    df_stim_by_plane: Mapping[int, pd.DataFrame] | None = None,
    fps_by_plane: Mapping[int, float] | None = None,
) -> pd.DataFrame:
    """Record exact presented windows and their response/BPI eligibility.

    This is provenance for read-only QC: every recorded B1/B2 stimulus window
    is mapped through the same onset-delay rule. ``used_for_bpi`` identifies
    the balanced LB/RB versus LC/RC subset consumed by
    :func:`build_response_bpi_tables`.
    """
    rows: list[dict[str, Any]] = []
    source = df_stim.copy() if isinstance(df_stim, pd.DataFrame) else pd.DataFrame()
    fps_lookup = dict(fps_by_session or {})
    event_groups = (
        [(int(plane), event) for plane, events in stim_events_by_plane.items() for event in events]
        if stim_events_by_plane else [(None, event) for event in stim_events]
    )
    for plane_idx, event in event_groups:
        session = str(event.get("session_label", ""))
        block = event.get("block", pd.NA)
        stim_idx = event.get("stim_idx", -1)
        candidates = (df_stim_by_plane or {}).get(plane_idx, source).copy()
        if "session_label" in candidates:
            candidates = candidates[candidates["session_label"].astype(str) == session]
        if "block" in candidates and pd.notna(block):
            candidates = candidates[candidates["block"].astype(str) == str(block)]
        if "stim_idx" in candidates and pd.notna(stim_idx):
            candidates = candidates[pd.to_numeric(candidates["stim_idx"], errors="coerce") == int(stim_idx)]
        if "type" in candidates:
            candidates = candidates[candidates["type"].astype(str) == str(event.get("stim_type", ""))]
        recorded = candidates.iloc[0] if not candidates.empty else pd.Series(dtype=object)
        fps = float((fps_by_plane or {}).get(plane_idx, fps_lookup.get(session, np.nan)))
        idx0 = int(event["idx0"])
        idx1 = int(event["idx1"])
        rows.append(
            {
                "fish_id": str(fish_id),
                "plane_idx": plane_idx,
                "session_label": session,
                "block": block,
                "stim_idx": int(stim_idx),
                "stim_type": str(event.get("stim_type", "")),
                "stim_class": str(event.get("stim_class", "")),
                "presented_stim_mode": str(event.get("presented_stim_mode", event.get("stim_class", ""))),
                "bpi_stim_class": str(event.get("bpi_stim_class", event.get("stim_class", ""))),
                "used_for_bpi": bool(event.get("used_for_bpi", True)),
                "recorded_onset_sec": pd.to_numeric(recorded.get("start", np.nan), errors="coerce"),
                "recorded_offset_sec": pd.to_numeric(recorded.get("end", np.nan), errors="coerce"),
                "prestim_baseline_start_frame": event.get("baseline_idx0", pd.NA),
                "prestim_baseline_end_frame": event.get("baseline_idx1", pd.NA),
                "prestim_baseline_duration_frames": int(event["duration_frames"]) if "baseline_idx0" in event else pd.NA,
                "response_window_policy": "fixed_from_movement_onset_matched_prestim",
                "recorded_onset_frame": int(round(float(recorded.get("start")) * fps)) if pd.notna(recorded.get("start", np.nan)) and np.isfinite(fps) else pd.NA,
                "recorded_offset_frame": int(round(float(recorded.get("end")) * fps)) if pd.notna(recorded.get("end", np.nan)) and np.isfinite(fps) else pd.NA,
                "scored_onset_frame": idx0,
                "scored_offset_frame": idx1,
                "scored_onset_sec": idx0 / fps if np.isfinite(fps) and fps > 0 else np.nan,
                "scored_offset_sec": idx1 / fps if np.isfinite(fps) and fps > 0 else np.nan,
                "fps": fps,
                "scored_duration_frames": int(event["duration_frames"]),
                "scored_duration_sec": float(event["duration_s"]),
                "trace_grid_block_start_frame": recorded.get("trace_grid_block_start_frame", pd.NA),
                "trace_grid_frames_per_block": recorded.get("trace_grid_frames_per_block", pd.NA),
                "trace_grid_timing_source": recorded.get("trace_grid_timing_source", pd.NA),
                "timing_coordinate": "suite2p_full_trace",
            }
        )
    return pd.DataFrame.from_records(rows)


def build_response_bpi_tables(
    detail_df: pd.DataFrame,
    *,
    fish_dir: str | Path,
    fish_id: str,
    suite2p_root: str | Path | None = None,
    suite2p_dff_map: dict[int, dict[str, Any]] | None = None,
    precomputed_scored_bpi_df: pd.DataFrame | None = None,
    config: ActivityConfig | None = None,
    experiment_log_csv: str | Path | None = None,
    experiment_meta_csv: str | Path | None = None,
    frame_rate: float | None = None,
) -> dict[str, Any]:
    cfg = config or ActivityConfig()
    detail = detail_df.copy()
    drop_cols = [
        "n_bout_trials",
        "n_cont_trials",
        "n_bout_trials_z",
        "n_cont_trials_z",
        "mean_bout_dff",
        "mean_cont_dff",
        "mean_bout_zdff",
        "mean_cont_zdff",
        "mean_bout_auc_dff",
        "mean_cont_auc_dff",
        "response_magnitude_auc",
        "response_absolute_magnitude_auc",
        "bout_null_q99_auc",
        "cont_null_q99_auc",
        "bout_null_upper_auc",
        "cont_null_upper_auc",
        "bout_null_lower_auc",
        "cont_null_lower_auc",
        "bout_positive_response_pass",
        "bout_negative_response_pass",
        "cont_positive_response_pass",
        "cont_negative_response_pass",
        "bout_response_pass",
        "cont_response_pass",
        "bout_response_valence",
        "cont_response_valence",
        "response_valence_pattern",
        "response_has_suppression",
        "response_valence_complete",
        "response_is_active",
        "response_class",
        "response_summary_class",
        "response_auc_threshold",
        "response_null_quantile",
        "response_null_upper_quantile",
        "response_null_lower_quantile",
        "response_null_bootstrap_n",
        "denom",
        "denom_z",
        "bpi",
        "bpi_z",
        "signed_condition_contrast",
        "signed_condition_contrast_denom",
        "signed_condition_contrast_data_available",
        "activity_mag",
        "bpi_status",
        "bpi_preference",
        "bpi_data_available",
        "bpi_category",
        "bpi_zero_band",
        "bpi_activity_threshold",
    ]
    detail = detail.drop(columns=[col for col in drop_cols if col in detail.columns], errors="ignore")
    detail["activity_class"] = detail["activity_class"].astype(str).replace({"Inactive neurons": cfg.inactive_class})
    detail["suite2p_activity_class"] = detail["activity_class"].astype(str)
    detail["suite2p_is_cell"] = _as_bool_series(detail.get("is_active", False)).astype(bool)
    detail["plane_idx"] = pd.to_numeric(detail["plane_idx"], errors="coerce").astype("Int64")
    detail["func_label"] = pd.to_numeric(detail["func_label"], errors="coerce").astype("Int64")

    if precomputed_scored_bpi_df is not None and not precomputed_scored_bpi_df.empty:
        required_precomputed = {
            "plane_idx",
            "func_label",
            "response_is_active",
            "response_class",
            "bout_null_upper_auc",
            "cont_null_upper_auc",
            "bout_null_lower_auc",
            "cont_null_lower_auc",
            "response_null_upper_quantile",
            "response_null_lower_quantile",
        }
        missing_precomputed = sorted(required_precomputed - set(precomputed_scored_bpi_df.columns))
        if missing_precomputed:
            raise RuntimeError(f"precomputed response table missing columns: {missing_precomputed}")
        detail_out, summary_df = _merge_scored_response_table(
            detail,
            precomputed_scored_bpi_df,
            cfg=cfg,
        )
        return {
            "detail_df": detail_out,
            "scored_bpi_df": precomputed_scored_bpi_df.copy(),
            "summary_df": summary_df,
            "fps": float(frame_rate) if frame_rate is not None else float("nan"),
            "fps_by_session": {},
            "df_evt": pd.DataFrame(),
            "df_stim": pd.DataFrame(),
            "stim_events": [],
            "stim_source": "precomputed",
            "baseline_windows": [],
            "prestim_trial_windows": [],
            "s2p_map": suite2p_dff_map or {},
        }

    if suite2p_dff_map is None:
        s2p_map = load_suite2p_dff_map(
            detail,
            suite2p_root=suite2p_root,
            dfof_baseline_pct=float(cfg.dfof_baseline_pct),
            dfof_eps=float(cfg.dfof_eps),
        )
    else:
        s2p_map = {
            int(plane_idx): {
                **(plane_data if isinstance(plane_data, dict) else {}),
                "dff": np.asarray((plane_data or {}).get("dff"), dtype=np.float32),
            }
            for plane_idx, plane_data in suite2p_dff_map.items()
            if isinstance(plane_data, dict) and plane_data.get("dff") is not None
        }
    plane_indices = sorted(set(detail["plane_idx"].dropna().astype(int).tolist()))
    stim_cfg = StimulusConfig(
        stim_time_scale=float(cfg.stim_time_scale),
        measure_start_block=cfg.measure_start_block,
        measure_start_event=cfg.measure_start_event,
        remove_interblock_gaps=bool(cfg.remove_interblock_gaps),
        onset_delay_sec=float(cfg.stim_onset_delay_sec),
    )
    raw_plane_stim_contexts = resolve_plane_stimulus_contexts(
        fish_dir=fish_dir,
        fish_id=fish_id,
        plane_indices=plane_indices,
        experiment_log_csv=experiment_log_csv,
        experiment_meta_csv=experiment_meta_csv,
        frame_rate=frame_rate,
        config=stim_cfg,
    )
    inferred_fps = frame_rate
    if inferred_fps is None:
        inferred_fps = infer_frame_rate_from_detail(detail, suite2p_root=suite2p_root)

    prepared_contexts: dict[int, dict[str, Any]] = {}
    prepared_by_key: dict[tuple[Any, str, int], dict[str, Any]] = {}
    for plane_idx in plane_indices:
        ctx = raw_plane_stim_contexts.get(int(plane_idx))
        if ctx is None:
            continue
        plane_data = s2p_map.get(int(plane_idx))
        dff = None if plane_data is None else plane_data.get("dff")
        if dff is None or np.asarray(dff).ndim != 2:
            continue
        fps_value = ctx.get("frame_rate")
        if fps_value is None or float(fps_value) <= 0:
            fps_value = inferred_fps
        if fps_value is None or float(fps_value) <= 0:
            raise RuntimeError("could not determine frame rate for response/BPI computation")
        n_frames = int(np.asarray(dff).shape[1])
        key = (ctx.get("session_label"), str(ctx.get("log_path")), n_frames)
        if key not in prepared_by_key:
            trace_context = map_stimulus_context_to_trace_grid(
                ctx,
                n_frames=n_frames,
                fps=float(fps_value),
                onset_delay_sec=float(cfg.stim_onset_delay_sec),
                tag="[activity]",
            )
            prepared_by_key[key] = _build_activity_stimulus_context(
                trace_context,
                fps=float(fps_value),
                cfg=cfg,
            )
        prepared_contexts[int(plane_idx)] = prepared_by_key[key]
    if not prepared_contexts:
        raise RuntimeError("could not resolve stimulus metadata for response/BPI computation")

    zstats_by_plane: dict[int, dict[str, np.ndarray]] = {}
    null_q_by_plane: dict[int, dict[str, np.ndarray]] = {}
    for plane_idx, plane_data in s2p_map.items():
        dff = plane_data.get("dff")
        if dff is None:
            continue
        stim_context = prepared_contexts.get(int(plane_idx))
        if stim_context is None:
            continue
        zstats_by_plane[int(plane_idx)] = compute_zscore_stats(
            dff,
            stim_context["baseline_windows"],
            min_points=int(cfg.zscore_min_points),
            sigma_eps=float(cfg.zscore_sigma_eps),
        )
        null_q_by_plane[int(plane_idx)] = _compute_bootstrap_null_quantiles(
            dff,
            stim_context["stim_events"],
            stim_context["null_starts_by_duration"],
            fps=float(stim_context["fps"]),
            n_boot=int(cfg.response_null_bootstrap_n),
            q=float(cfg.response_null_q),
            seed=int(cfg.response_rng_seed) + int(plane_idx) * 100,
        )

    unique_contexts = list(prepared_by_key.values())
    fps_values = [float(ctx["fps"]) for ctx in unique_contexts]
    fps = fps_values[0] if fps_values and all(abs(val - fps_values[0]) <= 1e-6 for val in fps_values) else float("nan")
    df_evt = pd.concat(
        [
            ctx["df_evt"].assign(session_label=ctx.get("session_label"), stim_source=ctx.get("stim_source"))
            for ctx in unique_contexts
        ],
        ignore_index=True,
    )
    df_stim = pd.concat(
        [
            ctx["df_stim"].assign(session_label=ctx.get("session_label"), stim_source=ctx.get("stim_source"))
            for ctx in unique_contexts
        ],
        ignore_index=True,
    )
    stim_events = [event for ctx in unique_contexts for event in ctx["stim_events"]]
    stim_source = ";".join(dict.fromkeys(str(ctx.get("stim_source")) for ctx in unique_contexts))
    baseline_windows = unique_contexts[0]["baseline_windows"] if len(unique_contexts) == 1 else []
    prestim_trial_windows = unique_contexts[0]["prestim_trial_windows"] if len(unique_contexts) == 1 else []

    roi_rows = detail.drop_duplicates(subset=["plane_idx", "func_label"], keep="first").copy()
    roi_rows = roi_rows[roi_rows[["plane_idx", "func_label"]].notna().all(axis=1)].copy()

    response_summary_responsive = "Responsive neurons"
    response_summary_low = "Low activity"
    response_summary_unavailable = "Response unavailable"
    bpi_unavailable = cfg.response_unavailable
    cell_rows: list[dict[str, Any]] = []

    for _, row in roi_rows.iterrows():
        plane_idx = int(row["plane_idx"])
        func_label = int(row["func_label"])
        roi_idx = int(func_label) - 1
        base_row = {
            "plane_idx": plane_idx,
            "func_label": func_label,
            "roi_idx": roi_idx,
            "plane": row.get("plane", pd.NA),
            "anat_label": pd.NA,
            "identity_display_label": pd.NA,
            "bpi_status": "ok",
            "n_bout_trials": 0,
            "n_cont_trials": 0,
            "n_bout_trials_z": 0,
            "n_cont_trials_z": 0,
            "mean_bout_dff": np.nan,
            "mean_cont_dff": np.nan,
            "mean_bout_zdff": np.nan,
            "mean_cont_zdff": np.nan,
            "response_magnitude_auc": np.nan,
            "response_absolute_magnitude_auc": np.nan,
            "mean_bout_auc_dff": np.nan,
            "mean_cont_auc_dff": np.nan,
            "bout_null_q99_auc": np.nan,
            "cont_null_q99_auc": np.nan,
            "bout_null_upper_auc": np.nan,
            "cont_null_upper_auc": np.nan,
            "bout_null_lower_auc": np.nan,
            "cont_null_lower_auc": np.nan,
            "bout_positive_response_pass": False,
            "bout_negative_response_pass": False,
            "cont_positive_response_pass": False,
            "cont_negative_response_pass": False,
            "bout_response_pass": False,
            "cont_response_pass": False,
            "bout_response_valence": "unavailable",
            "cont_response_valence": "unavailable",
            "response_valence_pattern": "unavailable",
            "response_has_suppression": False,
            "response_valence_complete": False,
            "response_is_active": False,
            "response_class": cfg.response_unavailable,
            "response_summary_class": response_summary_unavailable,
            "response_auc_threshold": float(cfg.response_min_auc),
            "response_null_quantile": float(cfg.response_null_q),
            "response_null_upper_quantile": 1.0 - (1.0 - float(cfg.response_null_q)) / 2.0,
            "response_null_lower_quantile": (1.0 - float(cfg.response_null_q)) / 2.0,
            "response_null_bootstrap_n": int(cfg.response_null_bootstrap_n),
            "denom": np.nan,
            "denom_z": np.nan,
            "bpi": np.nan,
            "bpi_z": np.nan,
            "signed_condition_contrast": np.nan,
            "signed_condition_contrast_denom": np.nan,
            "signed_condition_contrast_data_available": False,
            "activity_mag": np.nan,
            "bpi_preference": "unavailable",
            "bpi_data_available": False,
            "bpi_category": bpi_unavailable,
            "bpi_zero_band": float(cfg.zero_band),
            "bpi_activity_threshold": float(cfg.response_min_auc),
        }
        plane_data = s2p_map.get(plane_idx)
        if plane_data is None:
            base_row["bpi_status"] = "missing_plane"
            cell_rows.append(base_row)
            continue
        dff = plane_data.get("dff")
        if dff is None:
            base_row["bpi_status"] = "missing_dff"
            cell_rows.append(base_row)
            continue
        if roi_idx < 0 or roi_idx >= dff.shape[0]:
            base_row["bpi_status"] = "roi_idx_out_of_range"
            cell_rows.append(base_row)
            continue
        suite2p_is_cell = bool(_as_bool_series(pd.Series([row.get("suite2p_is_cell", False)])).iloc[0])
        if not suite2p_is_cell:
            base_row["bpi_status"] = "low_quality_trace"
            cell_rows.append(base_row)
            continue
        stim_context = prepared_contexts.get(plane_idx)
        if stim_context is None:
            base_row["bpi_status"] = "missing_stimulus_context"
            cell_rows.append(base_row)
            continue

        trace = np.asarray(dff[roi_idx], dtype=np.float32)
        zstats = zstats_by_plane.get(plane_idx)
        trace_z = None
        if zstats is not None and roi_idx < len(zstats["valid"]) and bool(zstats["valid"][roi_idx]):
            trace_z = (trace - float(zstats["mu"][roi_idx])) / float(zstats["sigma"][roi_idx])

        bout_resp: list[float] = []
        cont_resp: list[float] = []
        bout_auc: list[float] = []
        cont_auc: list[float] = []
        bout_z: list[float] = []
        cont_z: list[float] = []
        plane_fps = float(stim_context["fps"])
        for event in stim_context["stim_events"]:
            seg, _, _ = _extract_window(
                trace,
                int(event["idx0"]),
                int(event["idx1"]),
                mode=str(cfg.edge_policy),
                min_valid_frac=float(cfg.min_valid_frac),
            )
            if seg is None:
                continue
            baseline, _, _ = _extract_window(
                trace, int(event["baseline_idx0"]), int(event["baseline_idx1"]),
                mode="strict", min_valid_frac=float(cfg.min_valid_frac),
            )
            if baseline is None:
                continue
            baseline_mean = float(np.nanmean(baseline))
            if not np.isfinite(baseline_mean):
                continue
            resp_mean = float(np.nanmean(seg) - baseline_mean)
            resp_auc = float(np.nansum(seg - baseline_mean) / plane_fps)
            if not np.isfinite(resp_mean) or not np.isfinite(resp_auc):
                continue
            if event["stim_class"] == "bout":
                bout_resp.append(resp_mean)
                bout_auc.append(resp_auc)
            elif event["stim_class"] == "continuous":
                cont_resp.append(resp_mean)
                cont_auc.append(resp_auc)
            if trace_z is not None:
                seg_z, _, _ = _extract_window(
                    trace_z,
                    int(event["idx0"]),
                    int(event["idx1"]),
                    mode=str(cfg.edge_policy),
                    min_valid_frac=float(cfg.min_valid_frac),
                )
                baseline_z, _, _ = _extract_window(
                    trace_z, int(event["baseline_idx0"]), int(event["baseline_idx1"]),
                    mode="strict", min_valid_frac=float(cfg.min_valid_frac),
                )
                if seg_z is not None and baseline_z is not None:
                    resp_z = float(np.nanmean(seg_z) - np.nanmean(baseline_z))
                    if np.isfinite(resp_z):
                        if event["stim_class"] == "bout":
                            bout_z.append(resp_z)
                        elif event["stim_class"] == "continuous":
                            cont_z.append(resp_z)

        n_b = int(np.isfinite(np.asarray(bout_resp, dtype=float)).sum())
        n_c = int(np.isfinite(np.asarray(cont_resp, dtype=float)).sum())
        n_b_z = int(np.isfinite(np.asarray(bout_z, dtype=float)).sum())
        n_c_z = int(np.isfinite(np.asarray(cont_z, dtype=float)).sum())
        base_row["n_bout_trials"] = n_b
        base_row["n_cont_trials"] = n_c
        base_row["n_bout_trials_z"] = n_b_z
        base_row["n_cont_trials_z"] = n_c_z

        b = float(np.nanmean(bout_resp)) if n_b > 0 else np.nan
        c = float(np.nanmean(cont_resp)) if n_c > 0 else np.nan
        b_auc_mean = float(np.nanmean(bout_auc)) if bout_auc else np.nan
        c_auc_mean = float(np.nanmean(cont_auc)) if cont_auc else np.nan
        bout_magnitude = max(0.0, b_auc_mean) if np.isfinite(b_auc_mean) else np.nan
        cont_magnitude = max(0.0, c_auc_mean) if np.isfinite(c_auc_mean) else np.nan
        if np.isfinite(bout_magnitude) and np.isfinite(cont_magnitude):
            base_row["response_magnitude_auc"] = 0.5 * (bout_magnitude + cont_magnitude)
        if np.isfinite(b_auc_mean) and np.isfinite(c_auc_mean):
            base_row["response_absolute_magnitude_auc"] = 0.5 * (abs(b_auc_mean) + abs(c_auc_mean))
            signed_denom = abs(b_auc_mean) + abs(c_auc_mean)
            base_row["signed_condition_contrast_denom"] = float(signed_denom)
            if signed_denom > float(cfg.denom_eps):
                base_row["signed_condition_contrast"] = float((b_auc_mean - c_auc_mean) / signed_denom)
                base_row["signed_condition_contrast_data_available"] = True
        b_z_mean = float(np.nanmean(bout_z)) if n_b_z > 0 else np.nan
        c_z_mean = float(np.nanmean(cont_z)) if n_c_z > 0 else np.nan
        denom = bout_magnitude + cont_magnitude
        denom_z = b_z_mean + c_z_mean if np.isfinite(b_z_mean) and np.isfinite(c_z_mean) else np.nan
        base_row["mean_bout_dff"] = b
        base_row["mean_cont_dff"] = c
        base_row["mean_bout_auc_dff"] = b_auc_mean
        base_row["mean_cont_auc_dff"] = c_auc_mean
        base_row["mean_bout_zdff"] = b_z_mean
        base_row["mean_cont_zdff"] = c_z_mean
        base_row["denom"] = float(denom) if np.isfinite(denom) else np.nan
        base_row["denom_z"] = float(denom_z) if np.isfinite(denom_z) else np.nan
        if np.isfinite(b_z_mean) and np.isfinite(c_z_mean):
            base_row["activity_mag"] = float((abs(b_z_mean) + abs(c_z_mean)) / 2.0)

        plane_null = null_q_by_plane.get(plane_idx, {})
        bout_null = plane_null.get("bout", np.full(dff.shape[0], np.nan, dtype=np.float32))
        cont_null = plane_null.get("continuous", np.full(dff.shape[0], np.nan, dtype=np.float32))
        bout_null_upper = plane_null.get("bout_upper", np.full(dff.shape[0], np.nan, dtype=np.float32))
        cont_null_upper = plane_null.get("continuous_upper", np.full(dff.shape[0], np.nan, dtype=np.float32))
        bout_null_lower = plane_null.get("bout_lower", np.full(dff.shape[0], np.nan, dtype=np.float32))
        cont_null_lower = plane_null.get("continuous_lower", np.full(dff.shape[0], np.nan, dtype=np.float32))
        if roi_idx < len(bout_null):
            base_row["bout_null_q99_auc"] = float(bout_null[roi_idx]) if np.isfinite(bout_null[roi_idx]) else np.nan
        if roi_idx < len(cont_null):
            base_row["cont_null_q99_auc"] = float(cont_null[roi_idx]) if np.isfinite(cont_null[roi_idx]) else np.nan
        if roi_idx < len(bout_null_upper):
            base_row["bout_null_upper_auc"] = (
                float(bout_null_upper[roi_idx]) if np.isfinite(bout_null_upper[roi_idx]) else np.nan
            )
        if roi_idx < len(cont_null_upper):
            base_row["cont_null_upper_auc"] = (
                float(cont_null_upper[roi_idx]) if np.isfinite(cont_null_upper[roi_idx]) else np.nan
            )
        if roi_idx < len(bout_null_lower):
            base_row["bout_null_lower_auc"] = (
                float(bout_null_lower[roi_idx]) if np.isfinite(bout_null_lower[roi_idx]) else np.nan
            )
        if roi_idx < len(cont_null_lower):
            base_row["cont_null_lower_auc"] = (
                float(cont_null_lower[roi_idx]) if np.isfinite(cont_null_lower[roi_idx]) else np.nan
            )

        bout_assessable = (
            n_b >= int(cfg.min_trials_per_class)
            and np.isfinite(b_auc_mean)
            and np.isfinite(base_row["bout_null_upper_auc"])
            and np.isfinite(base_row["bout_null_lower_auc"])
        )
        cont_assessable = (
            n_c >= int(cfg.min_trials_per_class)
            and np.isfinite(c_auc_mean)
            and np.isfinite(base_row["cont_null_upper_auc"])
            and np.isfinite(base_row["cont_null_lower_auc"])
        )
        bout_positive_pass = (
            bout_assessable
            and float(b_auc_mean) >= max(float(cfg.response_min_auc), float(base_row["bout_null_upper_auc"]))
        )
        bout_negative_pass = (
            bout_assessable
            and float(b_auc_mean) <= min(-float(cfg.response_min_auc), float(base_row["bout_null_lower_auc"]))
        )
        cont_positive_pass = (
            cont_assessable
            and float(c_auc_mean) >= max(float(cfg.response_min_auc), float(base_row["cont_null_upper_auc"]))
        )
        cont_negative_pass = (
            cont_assessable
            and float(c_auc_mean) <= min(-float(cfg.response_min_auc), float(base_row["cont_null_lower_auc"]))
        )
        bout_pass = bool(bout_positive_pass or bout_negative_pass)
        cont_pass = bool(cont_positive_pass or cont_negative_pass)
        base_row["bout_positive_response_pass"] = bool(bout_positive_pass)
        base_row["bout_negative_response_pass"] = bool(bout_negative_pass)
        base_row["cont_positive_response_pass"] = bool(cont_positive_pass)
        base_row["cont_negative_response_pass"] = bool(cont_negative_pass)
        base_row["bout_response_pass"] = bool(bout_pass)
        base_row["cont_response_pass"] = bool(cont_pass)
        base_row["bout_response_valence"] = (
            (
                "activated" if bout_positive_pass else "suppressed" if bout_negative_pass else "not-significant"
            )
            if bout_assessable else "unavailable"
        )
        base_row["cont_response_valence"] = (
            (
                "activated" if cont_positive_pass else "suppressed" if cont_negative_pass else "not-significant"
            )
            if cont_assessable else "unavailable"
        )
        base_row["response_valence_pattern"] = (
            f'{base_row["bout_response_valence"]}|{base_row["cont_response_valence"]}'
        )
        base_row["response_has_suppression"] = bool(bout_negative_pass or cont_negative_pass)
        base_row["response_valence_complete"] = bool(bout_assessable and cont_assessable)
        base_row["response_is_active"] = bool(bout_pass or cont_pass)

        if bout_pass and cont_pass:
            base_row["response_class"] = cfg.response_both
            base_row["response_summary_class"] = response_summary_responsive
        elif bout_pass:
            base_row["response_class"] = cfg.response_bout
            base_row["response_summary_class"] = response_summary_responsive
        elif cont_pass:
            base_row["response_class"] = cfg.response_cont
            base_row["response_summary_class"] = response_summary_responsive
        elif bout_assessable and cont_assessable:
            base_row["response_class"] = cfg.response_low
            base_row["response_summary_class"] = response_summary_low
        else:
            base_row["response_class"] = cfg.response_unavailable
            base_row["response_summary_class"] = response_summary_unavailable
        base_row["bpi_category"] = base_row["response_class"]

        base_row["bpi_preference"] = "unavailable" if base_row["response_is_active"] else "not-responsive"
        if n_b < int(cfg.min_trials_per_class) or n_c < int(cfg.min_trials_per_class):
            base_row["bpi_status"] = "insufficient_trials"
            cell_rows.append(base_row)
            continue
        if not np.isfinite(denom) or denom <= float(cfg.denom_eps):
            base_row["bpi_status"] = "low_denom"
            cell_rows.append(base_row)
            continue
        bpi = float((bout_magnitude - cont_magnitude) / denom)
        if not np.isfinite(bpi):
            base_row["bpi_status"] = "nonfinite_bpi"
            cell_rows.append(base_row)
            continue
        base_row["bpi"] = bpi
        base_row["bpi_data_available"] = True
        if n_b_z >= int(cfg.min_trials_per_class) and n_c_z >= int(cfg.min_trials_per_class):
            if np.isfinite(denom_z) and abs(float(denom_z)) > float(cfg.denom_eps):
                bpi_z = float((b_z_mean - c_z_mean) / denom_z)
                if np.isfinite(bpi_z):
                    base_row["bpi_z"] = bpi_z

        if not bool(base_row["response_is_active"]):
            base_row["bpi_preference"] = "not-responsive"
        elif bpi > float(cfg.zero_band):
            base_row["bpi_preference"] = "bout-preferential"
        elif bpi < -float(cfg.zero_band):
            base_row["bpi_preference"] = "continuous-preferential"
        else:
            base_row["bpi_preference"] = "non-preferential"
        cell_rows.append(base_row)

    scored_bpi_df = pd.DataFrame(cell_rows)
    if scored_bpi_df.empty:
        detail["bpi_category"] = bpi_unavailable
        detail["bpi_data_available"] = False
        detail["response_is_active"] = False
        detail["response_class"] = cfg.response_unavailable
        detail["response_summary_class"] = response_summary_unavailable
        summary_df = pd.DataFrame()
    else:
        merge_cols = [
            "plane_idx",
            "func_label",
            "n_bout_trials",
            "n_cont_trials",
            "n_bout_trials_z",
            "n_cont_trials_z",
            "mean_bout_dff",
            "mean_cont_dff",
            "mean_bout_zdff",
            "mean_cont_zdff",
            "mean_bout_auc_dff",
            "response_magnitude_auc",
            "response_absolute_magnitude_auc",
            "mean_cont_auc_dff",
            "bout_null_q99_auc",
            "cont_null_q99_auc",
            "bout_null_upper_auc",
            "cont_null_upper_auc",
            "bout_null_lower_auc",
            "cont_null_lower_auc",
            "bout_positive_response_pass",
            "bout_negative_response_pass",
            "cont_positive_response_pass",
            "cont_negative_response_pass",
            "bout_response_pass",
            "cont_response_pass",
            "bout_response_valence",
            "cont_response_valence",
            "response_valence_pattern",
            "response_has_suppression",
            "response_valence_complete",
            "response_is_active",
            "response_class",
            "response_summary_class",
            "response_auc_threshold",
            "response_null_quantile",
            "response_null_upper_quantile",
            "response_null_lower_quantile",
            "response_null_bootstrap_n",
            "denom",
            "denom_z",
            "bpi",
            "bpi_z",
            "signed_condition_contrast",
            "signed_condition_contrast_denom",
            "signed_condition_contrast_data_available",
            "activity_mag",
            "bpi_status",
            "bpi_data_available",
            "bpi_preference",
            "bpi_category",
            "bpi_zero_band",
            "bpi_activity_threshold",
        ]
        detail = detail.merge(scored_bpi_df[merge_cols], on=["plane_idx", "func_label"], how="left")
        detail["response_is_active"] = _as_bool_series(detail.get("response_is_active", False)).astype(bool)
        for col in [
            "bout_positive_response_pass",
            "bout_negative_response_pass",
            "cont_positive_response_pass",
            "cont_negative_response_pass",
            "bout_response_pass",
            "cont_response_pass",
            "response_has_suppression",
            "response_valence_complete",
            "signed_condition_contrast_data_available",
        ]:
            detail[col] = _as_bool_series(detail.get(col, False)).astype(bool)
        for col in ["bout_response_valence", "cont_response_valence", "response_valence_pattern"]:
            detail[col] = detail[col].fillna("unavailable")
        detail["response_class"] = detail["response_class"].fillna(cfg.response_unavailable)
        detail["response_summary_class"] = detail["response_summary_class"].fillna(response_summary_unavailable)
        detail["bpi_preference"] = detail["bpi_preference"].fillna("unavailable")
        detail["bpi_category"] = detail["bpi_category"].fillna(bpi_unavailable)
        detail["bpi_status"] = detail["bpi_status"].fillna("response_unavailable")
        detail["bpi_data_available"] = _as_bool_series(detail.get("bpi_data_available", False)).astype(bool)
        detail["bpi_zero_band"] = detail["bpi_zero_band"].fillna(float(cfg.zero_band))
        detail["bpi_activity_threshold"] = detail["bpi_activity_threshold"].fillna(float(cfg.response_min_auc))
        detail["response_auc_threshold"] = detail["response_auc_threshold"].fillna(float(cfg.response_min_auc))
        detail["response_null_quantile"] = detail["response_null_quantile"].fillna(float(cfg.response_null_q))
        two_sided_tail = (1.0 - float(cfg.response_null_q)) / 2.0
        detail["response_null_upper_quantile"] = detail["response_null_upper_quantile"].fillna(1.0 - two_sided_tail)
        detail["response_null_lower_quantile"] = detail["response_null_lower_quantile"].fillna(two_sided_tail)
        detail["response_null_bootstrap_n"] = detail["response_null_bootstrap_n"].fillna(int(cfg.response_null_bootstrap_n))

        summary_df = (
            detail.groupby(["response_summary_class", "bpi_category"], as_index=False)
            .size()
            .rename(columns={"size": "n_rois"})
        )
        summary_df["n_response_total"] = summary_df.groupby("response_summary_class")["n_rois"].transform("sum")
        summary_df["n_all_segmented"] = int(len(detail))
        summary_df["pct_within_response_class"] = np.where(
            summary_df["n_response_total"] > 0,
            100.0 * summary_df["n_rois"] / summary_df["n_response_total"],
            np.nan,
        )
        summary_df["pct_of_all_segmented"] = np.where(
            summary_df["n_all_segmented"] > 0,
            100.0 * summary_df["n_rois"] / summary_df["n_all_segmented"],
            np.nan,
        )

    return {
        "detail_df": detail,
        "scored_bpi_df": scored_bpi_df,
        "summary_df": summary_df,
        "fps": float(fps),
        "fps_by_session": {str(ctx.get("session_label", "")): float(ctx["fps"]) for ctx in unique_contexts},
        "stim_events_by_plane": {plane: context["stim_events"] for plane, context in prepared_contexts.items()},
        "presented_stim_events_by_plane": {plane: context["presented_stim_events"] for plane, context in prepared_contexts.items()},
        "df_stim_by_plane": {plane: context["df_stim"] for plane, context in prepared_contexts.items()},
        "fps_by_plane": {plane: float(context["fps"]) for plane, context in prepared_contexts.items()},
        "df_evt": df_evt,
        "df_stim": df_stim,
        "stim_events": stim_events,
        "presented_stim_events": [event for ctx in unique_contexts for event in ctx["presented_stim_events"]],
        "stim_source": stim_source,
        "prestim_trial_windows_by_plane": {plane: context["prestim_trial_windows"] for plane, context in prepared_contexts.items()},
        "baseline_windows": baseline_windows,
        "prestim_trial_windows": prestim_trial_windows,
        "s2p_map": s2p_map,
    }


def build_presented_stimulus_response_tables(
    response_result: dict[str, Any],
    roi_keys: pd.DataFrame,
    *,
    fish_id: str,
    config: ActivityConfig | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Screen every presented condition without changing balanced BPI calls.

    Per-condition AUCs use the same delayed motion windows and prestimulus
    null pool as BPI. Bonferroni-adjusted lower and upper null quantiles
    control one family spanning every presented stimulus and both response
    tails; no condition is added to the BPI pools.
    """
    cfg = config or ActivityConfig()
    keys = roi_keys[["plane_idx", "func_label"]].drop_duplicates().copy()
    events_by_plane = response_result["presented_stim_events_by_plane"]
    types = sorted({str(event["stim_type"]) for events in events_by_plane.values() for event in events})
    if not types:
        raise RuntimeError("no presented stimulus events available for response screening")
    familywise_tail_alpha = (1.0 - float(cfg.response_null_q)) / (2.0 * len(types))
    null_upper_quantile = 1.0 - familywise_tail_alpha
    null_lower_quantile = familywise_tail_alpha
    s2p_map = response_result["s2p_map"]
    fps_by_plane = response_result["fps_by_plane"]
    rows: list[dict[str, Any]] = []

    for plane_idx, plane_keys in keys.groupby("plane_idx", sort=True):
        plane_idx = int(plane_idx)
        dff = np.asarray(s2p_map[plane_idx]["dff"], dtype=np.float32)
        roi_indices = plane_keys["func_label"].astype(int).to_numpy() - 1
        events = events_by_plane.get(plane_idx, [])
        session = str(events[0].get("session_label", "")) if events else ""
        fps = float(fps_by_plane[plane_idx])
        prestim = response_result["prestim_trial_windows_by_plane"][plane_idx]
        null_starts = build_null_window_start_map(
            prestim,
            [int(event["duration_frames"]) for event in events],
            step_frames=max(1, int(round(float(cfg.response_null_step_sec) * fps))),
            min_windows=int(cfg.response_null_min_windows),
        )
        csum = np.concatenate(
            [np.zeros((dff.shape[0], 1), dtype=np.float32), np.cumsum(dff, axis=1, dtype=np.float32)],
            axis=1,
        )
        by_type = {stim_type: [event for event in events if str(event["stim_type"]) == stim_type] for stim_type in types}
        for type_idx, stim_type in enumerate(types):
            type_events = by_type[stim_type]
            observed: list[list[float]] = [[] for _ in roi_indices]
            for event in type_events:
                for pos, roi_idx in enumerate(roi_indices):
                    segment, _, _ = _extract_window(
                        dff[int(roi_idx)], int(event["idx0"]), int(event["idx1"]),
                        mode=str(cfg.edge_policy), min_valid_frac=float(cfg.min_valid_frac),
                    )
                    baseline, _, _ = _extract_window(
                        dff[int(roi_idx)], int(event["baseline_idx0"]), int(event["baseline_idx1"]),
                        mode="strict", min_valid_frac=float(cfg.min_valid_frac),
                    )
                    if segment is not None and baseline is not None:
                        auc = float(np.nansum(segment - np.nanmean(baseline)) / fps)
                        if np.isfinite(auc):
                            observed[pos].append(auc)
            null_upper = np.full(len(roi_indices), np.nan, dtype=np.float32)
            null_lower = np.full(len(roi_indices), np.nan, dtype=np.float32)
            if type_events and all(len(null_starts.get(int(event["duration_frames"]), [])) >= 2 for event in type_events):
                rng = np.random.default_rng(int(cfg.response_rng_seed) + plane_idx * 100 + type_idx)
                boot = np.zeros((len(roi_indices), int(cfg.response_null_bootstrap_n)), dtype=np.float32)
                for event in type_events:
                    duration = int(event["duration_frames"])
                    starts = null_starts[duration]
                    baseline_choices = rng.integers(0, len(starts), size=boot.shape[1])
                    response_choices = rng.integers(0, len(starts) - 1, size=boot.shape[1])
                    response_choices += response_choices >= baseline_choices
                    baseline_start, response_start = starts[baseline_choices], starts[response_choices]
                    boot += ((csum[roi_indices[:, None], response_start + duration] - csum[roi_indices[:, None], response_start])
                             - (csum[roi_indices[:, None], baseline_start + duration] - csum[roi_indices[:, None], baseline_start])) / fps
                boot /= float(len(type_events))
                null_upper = np.quantile(boot, null_upper_quantile, axis=1).astype(np.float32)
                null_lower = np.quantile(boot, null_lower_quantile, axis=1).astype(np.float32)
            for pos, roi_idx in enumerate(roi_indices):
                n_valid = len(observed[pos])
                mean_auc = float(np.mean(observed[pos])) if n_valid else np.nan
                upper_threshold = float(null_upper[pos])
                lower_threshold = float(null_lower[pos])
                available = (
                    n_valid >= int(cfg.min_trials_per_class)
                    and np.isfinite(upper_threshold)
                    and np.isfinite(lower_threshold)
                )
                positive_pass = bool(
                    available and mean_auc >= max(float(cfg.response_min_auc), upper_threshold)
                )
                negative_pass = bool(
                    available and mean_auc <= min(-float(cfg.response_min_auc), lower_threshold)
                )
                rows.append({
                    "fish_id": str(fish_id),
                    "plane_idx": plane_idx,
                    "func_label": int(roi_idx) + 1,
                    "session_label": session,
                    "stim_type": stim_type,
                    "n_valid_trials": n_valid,
                    "mean_auc_dff": mean_auc,
                    "null_auc_threshold": upper_threshold,
                    "null_auc_upper_threshold": upper_threshold,
                    "null_auc_lower_threshold": lower_threshold,
                    "response_auc_floor": float(cfg.response_min_auc),
                    "response_null_quantile": float(cfg.response_null_q),
                    "null_quantile_familywise": null_upper_quantile,
                    "null_upper_quantile_familywise": null_upper_quantile,
                    "null_lower_quantile_familywise": null_lower_quantile,
                    "response_available": bool(available),
                    "positive_response_pass": positive_pass,
                    "negative_response_pass": negative_pass,
                    "response_pass": bool(positive_pass or negative_pass),
                    "response_valence": (
                        "activated" if positive_pass else "suppressed" if negative_pass
                        else "not-significant" if available else "unavailable"
                    ),
                })

    per_stim = pd.DataFrame(rows)
    any_response = (
        per_stim.groupby(["fish_id", "plane_idx", "func_label"], as_index=False)
        .agg(any_presented_response_is_active=("response_pass", "any"),
             all_presented_responses_available=("response_available", "all"),
             n_presented_stimuli_tested=("response_available", "sum"),
             n_presented_stimuli_passed=("response_pass", "sum"),
             n_presented_stimuli_activated=("positive_response_pass", "sum"),
             n_presented_stimuli_suppressed=("negative_response_pass", "sum"))
    )
    any_response["any_presented_response_has_suppression"] = any_response["n_presented_stimuli_suppressed"] > 0
    any_response["any_presented_response_state"] = np.where(
        any_response["any_presented_response_is_active"], "responsive",
        np.where(any_response["all_presented_responses_available"], "nonresponsive", "unavailable"),
    )
    return per_stim, any_response


def prepare_single_fish_bpi_diagnostics_stage(
    bpi_cells_df: pd.DataFrame,
    *,
    fish_id: str,
    master_detail_csv: str | Path | None = None,
    config: SingleFishBpiDiagnosticsConfig | None = None,
) -> dict[str, Any]:
    cfg = config or SingleFishBpiDiagnosticsConfig()
    if not isinstance(bpi_cells_df, pd.DataFrame) or bpi_cells_df.empty:
        raise RuntimeError("[56g] bpi_cells_df missing/empty; run [56h] first.")

    log_lines: list[str] = []
    df = bpi_cells_df.copy()
    if "fish_id" in df.columns:
        df = df[df["fish_id"].astype(str) == str(fish_id)].copy()
        if df.empty:
            raise RuntimeError("[56g] bpi_cells_df has no rows for current fish; run [56h].")
    if "gene" not in df.columns:
        raise RuntimeError("[56g] bpi_cells_df missing required column: gene")
    if "plane_idx" not in df.columns and "plane" in df.columns:
        df["plane_idx"] = pd.to_numeric(df["plane"], errors="coerce").astype("Int64")
    if "plane_idx" not in df.columns or "func_label" not in df.columns:
        raise RuntimeError(
            "[56g] bpi_cells_df missing plane_idx/func_label required for response-aware low-activity annotation."
        )

    if {"mean_bout_zdff", "mean_cont_zdff"}.issubset(df.columns):
        bout_col = "mean_bout_zdff"
        cont_col = "mean_cont_zdff"
        activity_label = "z-scored dF/F"
    elif {"mean_bout_dff", "mean_cont_dff"}.issubset(df.columns):
        bout_col = "mean_bout_dff"
        cont_col = "mean_cont_dff"
        activity_label = "dF/F"
        log_lines.append("[56g] WARNING: z-scored response columns missing in bpi_cells_df; falling back to raw dF/F.")
    else:
        raise RuntimeError("[56g] bpi_cells_df missing both z-scored and raw bout/continuous response columns.")

    bpi_col = str(cfg.bpi_index_col)
    if bpi_col not in df.columns:
        fallback_bpi = "bpi_z" if "bpi_z" in df.columns else ("bpi" if "bpi" in df.columns else None)
        if fallback_bpi is None:
            raise RuntimeError("[56g] bpi_cells_df missing bpi columns.")
        log_lines.append(f"[56g] BPI_INDEX_COL={bpi_col} missing; using {fallback_bpi}")
        bpi_col = fallback_bpi

    for col in [bpi_col, bout_col, cont_col]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["activity_mag"] = (df[bout_col].abs() + df[cont_col].abs()) / 2.0
    df["bpi_metric"] = df[bpi_col]
    df["abs_bpi"] = df["bpi_metric"].abs()
    df = df[
        np.isfinite(df["activity_mag"])
        & np.isfinite(df["bpi_metric"])
        & np.isfinite(df[bout_col])
        & np.isfinite(df[cont_col])
    ].copy()
    if df.empty:
        raise RuntimeError("[56g] no finite cells after filtering.")

    need_lookup = ("response_class" not in df.columns) or ("response_summary_class" not in df.columns)
    if need_lookup:
        if master_detail_csv is None:
            raise RuntimeError("[56g] Missing response-aware ROI table path; rerun [50ia] first.")
        master_path = Path(master_detail_csv)
        if not master_path.exists():
            raise RuntimeError(f"[56g] Missing response-aware ROI table: {master_path}. Run [50ia] first.")
        detail_df = pd.read_csv(master_path)
        if "fish_id" in detail_df.columns:
            detail_df = detail_df[detail_df["fish_id"].astype(str) == str(fish_id)].copy()
        required_detail_cols = {
            "plane_idx",
            "func_label",
            "response_class",
            "response_summary_class",
            "response_is_active",
        }
        missing_detail_cols = sorted(required_detail_cols - set(detail_df.columns))
        if missing_detail_cols:
            raise RuntimeError(
                f"[56g] Master ROI table missing response columns {missing_detail_cols}; rerun [50ia]."
            )

        prior_response_class = df.get(
            "response_class",
            pd.Series(pd.NA, index=df.index, dtype="object"),
        ).copy()
        prior_response_summary_class = df.get(
            "response_summary_class",
            pd.Series(pd.NA, index=df.index, dtype="object"),
        ).copy()
        prior_response_is_active = df.get(
            "response_is_active",
            pd.Series(pd.NA, index=df.index, dtype="object"),
        ).copy()
        detail_lookup = detail_df[
            ["plane_idx", "func_label", "response_class", "response_summary_class", "response_is_active"]
        ].copy()
        detail_lookup["plane_idx_key"] = pd.to_numeric(detail_lookup["plane_idx"], errors="coerce").astype("Int64")
        detail_lookup["func_label_key"] = pd.to_numeric(detail_lookup["func_label"], errors="coerce").astype("Int64")
        detail_lookup = (
            detail_lookup.drop(columns=["plane_idx", "func_label"])
            .drop_duplicates(subset=["plane_idx_key", "func_label_key"], keep="last")
            .reset_index(drop=True)
        )
        detail_lookup = detail_lookup.rename(
            columns={
                "response_class": "_lookup_response_class",
                "response_summary_class": "_lookup_response_summary_class",
                "response_is_active": "_lookup_response_is_active",
            }
        )
        df["plane_idx_key"] = pd.to_numeric(df["plane_idx"], errors="coerce").astype("Int64")
        df["func_label_key"] = pd.to_numeric(df["func_label"], errors="coerce").astype("Int64")
        df = df.merge(detail_lookup, on=["plane_idx_key", "func_label_key"], how="left")
        lookup_response_class = df.get(
            "_lookup_response_class",
            pd.Series(pd.NA, index=df.index, dtype="object"),
        )
        lookup_response_summary_class = df.get(
            "_lookup_response_summary_class",
            pd.Series(pd.NA, index=df.index, dtype="object"),
        )
        lookup_response_is_active = df.get(
            "_lookup_response_is_active",
            pd.Series(pd.NA, index=df.index, dtype="object"),
        )
        df["response_class"] = lookup_response_class.where(lookup_response_class.notna(), prior_response_class)
        df["response_summary_class"] = lookup_response_summary_class.where(
            lookup_response_summary_class.notna(),
            prior_response_summary_class,
        )
        df["response_is_active"] = lookup_response_is_active.where(
            lookup_response_is_active.notna(),
            prior_response_is_active,
        )
        df = df.drop(
            columns=[
                col
                for col in [
                    "_lookup_response_class",
                    "_lookup_response_summary_class",
                    "_lookup_response_is_active",
                ]
                if col in df.columns
            ]
        )

    df["response_class"] = df["response_class"].fillna(cfg.response_unavailable).astype(str)
    if "response_summary_class" not in df.columns:
        df["response_summary_class"] = np.where(
            df["response_class"].isin({"bout-responsive", "continuous-responsive", "both-responsive"}),
            cfg.response_summary_responsive,
            np.where(
                df["response_class"].eq(cfg.response_low),
                cfg.response_summary_low,
                cfg.response_summary_unavailable,
            ),
        )
    else:
        df["response_summary_class"] = (
            df["response_summary_class"].fillna(cfg.response_summary_unavailable).astype(str)
        )
    if "response_is_active" in df.columns:
        df["response_is_active"] = _as_bool_series(df["response_is_active"]).astype(bool)
    else:
        df["response_is_active"] = df["response_summary_class"].eq(cfg.response_summary_responsive)

    if cfg.zero_band is not None:
        zero_band = float(cfg.zero_band)
    elif "bpi_zero_band" in df.columns:
        zero_vals = pd.to_numeric(df["bpi_zero_band"], errors="coerce").to_numpy(dtype=float)
        zero_vals = zero_vals[np.isfinite(zero_vals)]
        zero_band = float(np.nanmedian(zero_vals)) if zero_vals.size else 0.10
    else:
        zero_band = 0.10

    df["is_bpi_near_zero"] = df["bpi_metric"].abs() <= float(zero_band)
    df["is_low_activity"] = df["response_summary_class"].eq(cfg.response_summary_low)
    df["is_responsive"] = df["response_summary_class"].eq(cfg.response_summary_responsive)
    df["is_response_unavailable"] = df["response_summary_class"].eq(cfg.response_summary_unavailable)
    df["interpretation"] = np.select(
        [
            df["is_bpi_near_zero"] & df["is_low_activity"],
            df["is_bpi_near_zero"] & df["is_responsive"],
            df["is_bpi_near_zero"] & df["is_response_unavailable"],
        ],
        [
            "near-zero BPI + low activity",
            "near-zero BPI + responsive",
            "near-zero BPI + response unavailable",
        ],
        default="non-zero BPI",
    )

    n_total = int(len(df))
    n_nz = int(df["is_bpi_near_zero"].sum())
    n_nz_low = int((df["is_bpi_near_zero"] & df["is_low_activity"]).sum())
    n_nz_resp = int((df["is_bpi_near_zero"] & df["is_responsive"]).sum())
    n_nz_unavailable = int((df["is_bpi_near_zero"] & df["is_response_unavailable"]).sum())

    binned_df = pd.DataFrame()
    n_bins = min(int(cfg.n_activity_bins), int(df["activity_mag"].nunique()))
    if n_bins >= 2:
        try:
            tmp = df.copy()
            tmp["_activity_bin"] = pd.qcut(df["activity_mag"], q=n_bins, duplicates="drop")
            binned_df = (
                tmp.groupby("_activity_bin", observed=False)
                .agg(
                    activity_mid=("activity_mag", "median"),
                    median_abs_bpi=("abs_bpi", "median"),
                    n=("abs_bpi", "size"),
                )
                .reset_index(drop=True)
            )
        except Exception:
            binned_df = pd.DataFrame()

    breakdown_df = (
        df.groupby(["gene", "interpretation", "response_summary_class"], observed=False)
        .size()
        .rename("n")
        .reset_index()
    )

    log_lines.append(
        f"[56g] cells={n_total}; BPI column={bpi_col}; activity columns=({bout_col}, {cont_col}); "
        f"|BPI|<= {zero_band:.3f}: {n_nz}; near-zero+low-activity={n_nz_low}; "
        f"near-zero+responsive={n_nz_resp}; near-zero+response-unavailable={n_nz_unavailable}; "
        "low-activity source=response_summary_class"
    )

    return {
        "bindings": {
            "bpi_activity_df": df.copy(),
            "bpi_activity_bins_df": binned_df.copy(),
            "bpi_activity_breakdown_df": breakdown_df.copy(),
            "BPI_ACTIVITY_FISH_ID": str(fish_id),
        },
        "df": df,
        "binned_df": binned_df,
        "breakdown_df": breakdown_df,
        "activity_label": activity_label,
        "bout_col": bout_col,
        "cont_col": cont_col,
        "bpi_index_col": bpi_col,
        "bpi_zero_band": float(zero_band),
        "summary_counts": {
            "n_total": n_total,
            "n_near_zero": n_nz,
            "n_near_zero_low": n_nz_low,
            "n_near_zero_responsive": n_nz_resp,
            "n_near_zero_response_unavailable": n_nz_unavailable,
        },
        "log_lines": log_lines,
    }


__all__ = [
    "ActivityConfig",
    "SingleFishBpiDiagnosticsConfig",
    "build_response_bpi_tables",
    "build_presented_stimulus_response_tables",
    "build_suite2p_response_seed_table",
    "prepare_single_fish_bpi_diagnostics_stage",
    "run_single_fish_cell_50ia_stage",
]
