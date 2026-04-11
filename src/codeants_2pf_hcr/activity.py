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
    build_prestim_baseline_windows,
    build_prestim_trial_windows,
    build_stim_tables,
    classify_stim_type,
    compute_zscore_stats,
    effective_motion_window,
    find_experiment_log,
    find_metadata_csv,
    load_events_df,
    load_metadata_params,
    parse_float,
)
from .suite2p import infer_frame_rate_from_detail, load_suite2p_dff_map


@dataclass(frozen=True)
class ActivityConfig:
    active_class: str = "Active neurons"
    inactive_class: str = "Low-quality traces"
    response_bout: str = "bout-responsive"
    response_cont: str = "continuous-responsive"
    response_both: str = "both-responsive"
    response_low: str = "low activity"
    response_unavailable: str = "response unavailable"
    bpi_weak: str = "weak-response"
    zero_band: float = 0.10
    min_trials_per_class: int = 3
    denom_eps: float = 1e-6
    edge_policy: str = "pad_nan"
    min_valid_frac: float = 0.5
    stim_onset_delay_sec: float = 10.0
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


def _as_bool_series(series_in: Any) -> pd.Series:
    s = pd.Series(series_in)
    if pd.api.types.is_bool_dtype(s):
        return s.fillna(False).astype(bool)
    if pd.api.types.is_numeric_dtype(s):
        return s.fillna(0).astype(float) != 0
    return s.astype(str).str.strip().str.lower().isin({"1", "true", "t", "yes", "y"})


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
            if starts is None or len(starts) == 0:
                usable = False
                break
            if duration not in auc_cache:
                sums = csum[:, starts + duration] - csum[:, starts]
                auc_cache[duration] = sums / float(fps)
            auc_mat = auc_cache[duration]
            choices = rng.integers(0, auc_mat.shape[1], size=int(n_boot))
            boot += auc_mat[:, choices]
        if not usable:
            continue
        boot /= float(len(class_events))
        out[stim_class] = np.quantile(boot, float(q), axis=1).astype(np.float32)
    return out


def _resolve_stim_context_for_activity(
    detail_df: pd.DataFrame,
    *,
    fish_dir: str | Path,
    fish_id: str,
    config: ActivityConfig,
    experiment_log_csv: str | Path | None = None,
    experiment_meta_csv: str | Path | None = None,
    frame_rate: float | None = None,
    suite2p_root: str | Path | None = None,
) -> tuple[float, pd.DataFrame, pd.DataFrame, str]:
    meta_path = Path(experiment_meta_csv) if experiment_meta_csv else find_metadata_csv(fish_dir, fish_id)
    fps = frame_rate
    if fps is None and meta_path is not None and meta_path.exists():
        params = load_metadata_params(meta_path)
        fps = parse_float(params.get("framerate", params.get("frame_rate", params.get("fps"))))
    if fps is None or float(fps) <= 0:
        fps = infer_frame_rate_from_detail(detail_df, suite2p_root=suite2p_root)
    if fps is None or float(fps) <= 0:
        raise RuntimeError("could not determine frame rate for response/BPI computation")

    log_path = Path(experiment_log_csv) if experiment_log_csv else find_experiment_log(fish_dir, fish_id)
    if log_path is None or not log_path.exists():
        raise RuntimeError("experiment log not found for response/BPI computation")
    df_evt = load_events_df(log_path)
    df_evt["time"] = df_evt["time"].astype(float) * float(config.stim_time_scale)
    df_evt, df_stim = build_stim_tables(
        df_evt,
        fps=float(fps),
        onset_delay_sec=float(config.stim_onset_delay_sec),
        remove_interblock_gaps=bool(config.remove_interblock_gaps),
        measure_start_block=config.measure_start_block,
        measure_start_event=config.measure_start_event,
    )
    return float(fps), df_evt, df_stim, str(log_path)


def build_response_bpi_tables(
    detail_df: pd.DataFrame,
    *,
    fish_dir: str | Path,
    fish_id: str,
    suite2p_root: str | Path | None = None,
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
        "bout_null_q99_auc",
        "cont_null_q99_auc",
        "bout_response_pass",
        "cont_response_pass",
        "response_is_active",
        "response_class",
        "response_summary_class",
        "response_auc_threshold",
        "response_null_quantile",
        "response_null_bootstrap_n",
        "denom",
        "denom_z",
        "bpi",
        "bpi_z",
        "activity_mag",
        "bpi_status",
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

    fps, df_evt, df_stim, stim_source = _resolve_stim_context_for_activity(
        detail,
        fish_dir=fish_dir,
        fish_id=fish_id,
        config=cfg,
        experiment_log_csv=experiment_log_csv,
        experiment_meta_csv=experiment_meta_csv,
        frame_rate=frame_rate,
        suite2p_root=suite2p_root,
    )
    baseline_windows = build_prestim_baseline_windows(df_evt, fps, cfg.stim_onset_delay_sec, tag="[activity]")
    prestim_trial_windows = build_prestim_trial_windows(df_evt, fps, cfg.stim_onset_delay_sec, tag="[activity]")

    df_stim_work = df_stim.copy()
    df_stim_work["stim_class"] = df_stim_work["type"].map(classify_stim_type)
    df_stim_work = df_stim_work[df_stim_work["stim_class"].isin(["bout", "continuous"])].copy()
    if df_stim_work.empty:
        raise RuntimeError("No pure bout/continuous stimuli available for response/BPI computation")

    stim_events: list[dict[str, Any]] = []
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
        idx1 = int(round(t1 * float(fps)))
        if idx1 <= idx0:
            continue
        stim_events.append(
            {
                "block": row.get("block", pd.NA),
                "stim_idx": int(row.get("stim_idx", -1)) if pd.notna(row.get("stim_idx", np.nan)) else -1,
                "stim_type": str(row.get("type", "")),
                "stim_class": str(row.get("stim_class", "")),
                "idx0": idx0,
                "idx1": idx1,
                "duration_s": float(duration),
                "duration_frames": int(idx1 - idx0),
            }
        )
    if not stim_events:
        raise RuntimeError("Stimulus events could not be constructed for response/BPI computation")

    step_frames = max(1, int(round(float(cfg.response_null_step_sec) * float(fps))))
    null_starts_by_duration = build_null_window_start_map(
        prestim_trial_windows,
        [event["duration_frames"] for event in stim_events],
        step_frames=step_frames,
        min_windows=int(cfg.response_null_min_windows),
    )
    s2p_map = load_suite2p_dff_map(
        detail,
        suite2p_root=suite2p_root,
        dfof_baseline_pct=float(cfg.dfof_baseline_pct),
        dfof_eps=float(cfg.dfof_eps),
    )

    zstats_by_plane: dict[int, dict[str, np.ndarray]] = {}
    null_q_by_plane: dict[int, dict[str, np.ndarray]] = {}
    for plane_idx, plane_data in s2p_map.items():
        dff = plane_data.get("dff")
        if dff is None:
            continue
        zstats_by_plane[int(plane_idx)] = compute_zscore_stats(
            dff,
            baseline_windows,
            min_points=int(cfg.zscore_min_points),
            sigma_eps=float(cfg.zscore_sigma_eps),
        )
        null_q_by_plane[int(plane_idx)] = _compute_bootstrap_null_quantiles(
            dff,
            stim_events,
            null_starts_by_duration,
            fps=float(fps),
            n_boot=int(cfg.response_null_bootstrap_n),
            q=float(cfg.response_null_q),
            seed=int(cfg.response_rng_seed) + int(plane_idx) * 100,
        )

    roi_rows = detail.drop_duplicates(subset=["plane_idx", "func_label"], keep="first").copy()
    roi_rows = roi_rows[roi_rows[["plane_idx", "func_label"]].notna().all(axis=1)].copy()

    response_summary_responsive = "Responsive neurons"
    response_summary_low = "Low activity"
    response_summary_unavailable = "Response unavailable"
    bpi_low = cfg.response_low
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
            "anat_label": row.get("anat_label", pd.NA),
            "identity_display_label": row.get("identity_display_label", pd.NA),
            "bpi_status": "ok",
            "n_bout_trials": 0,
            "n_cont_trials": 0,
            "n_bout_trials_z": 0,
            "n_cont_trials_z": 0,
            "mean_bout_dff": np.nan,
            "mean_cont_dff": np.nan,
            "mean_bout_zdff": np.nan,
            "mean_cont_zdff": np.nan,
            "mean_bout_auc_dff": np.nan,
            "mean_cont_auc_dff": np.nan,
            "bout_null_q99_auc": np.nan,
            "cont_null_q99_auc": np.nan,
            "bout_response_pass": False,
            "cont_response_pass": False,
            "response_is_active": False,
            "response_class": cfg.response_unavailable,
            "response_summary_class": response_summary_unavailable,
            "response_auc_threshold": float(cfg.response_min_auc),
            "response_null_quantile": float(cfg.response_null_q),
            "response_null_bootstrap_n": int(cfg.response_null_bootstrap_n),
            "denom": np.nan,
            "denom_z": np.nan,
            "bpi": np.nan,
            "bpi_z": np.nan,
            "activity_mag": np.nan,
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
        for event in stim_events:
            seg, _, _ = _extract_window(
                trace,
                int(event["idx0"]),
                int(event["idx1"]),
                mode=str(cfg.edge_policy),
                min_valid_frac=float(cfg.min_valid_frac),
            )
            if seg is None:
                continue
            resp_mean = float(np.nanmean(seg))
            resp_auc = float(np.nansum(seg) / float(fps))
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
                if seg_z is not None:
                    resp_z = float(np.nanmean(seg_z))
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
        b_z_mean = float(np.nanmean(bout_z)) if n_b_z > 0 else np.nan
        c_z_mean = float(np.nanmean(cont_z)) if n_c_z > 0 else np.nan
        denom = b + c if np.isfinite(b) and np.isfinite(c) else np.nan
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
        if roi_idx < len(bout_null):
            base_row["bout_null_q99_auc"] = float(bout_null[roi_idx]) if np.isfinite(bout_null[roi_idx]) else np.nan
        if roi_idx < len(cont_null):
            base_row["cont_null_q99_auc"] = float(cont_null[roi_idx]) if np.isfinite(cont_null[roi_idx]) else np.nan

        bout_pass = (
            n_b >= int(cfg.min_trials_per_class)
            and np.isfinite(b_auc_mean)
            and np.isfinite(base_row["bout_null_q99_auc"])
            and float(b_auc_mean) >= max(float(cfg.response_min_auc), float(base_row["bout_null_q99_auc"]))
        )
        cont_pass = (
            n_c >= int(cfg.min_trials_per_class)
            and np.isfinite(c_auc_mean)
            and np.isfinite(base_row["cont_null_q99_auc"])
            and float(c_auc_mean) >= max(float(cfg.response_min_auc), float(base_row["cont_null_q99_auc"]))
        )
        base_row["bout_response_pass"] = bool(bout_pass)
        base_row["cont_response_pass"] = bool(cont_pass)
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
        elif n_b >= int(cfg.min_trials_per_class) and n_c >= int(cfg.min_trials_per_class):
            base_row["response_class"] = cfg.response_low
            base_row["response_summary_class"] = response_summary_low
        else:
            base_row["response_class"] = cfg.response_unavailable
            base_row["response_summary_class"] = response_summary_unavailable

        if n_b < int(cfg.min_trials_per_class) or n_c < int(cfg.min_trials_per_class):
            base_row["bpi_status"] = "insufficient_trials"
            cell_rows.append(base_row)
            continue
        if not np.isfinite(denom) or abs(float(denom)) <= float(cfg.denom_eps):
            base_row["bpi_status"] = "low_denom"
            cell_rows.append(base_row)
            continue
        bpi = float((b - c) / denom)
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

        if bout_pass and cont_pass:
            if float(bpi) > float(cfg.zero_band):
                base_row["bpi_category"] = cfg.response_bout
            elif float(bpi) < -float(cfg.zero_band):
                base_row["bpi_category"] = cfg.response_cont
            else:
                base_row["bpi_category"] = cfg.response_both
        elif bout_pass:
            base_row["bpi_category"] = cfg.bpi_weak if abs(float(bpi)) <= float(cfg.zero_band) else cfg.response_bout
        elif cont_pass:
            base_row["bpi_category"] = cfg.bpi_weak if abs(float(bpi)) <= float(cfg.zero_band) else cfg.response_cont
        else:
            base_row["bpi_category"] = bpi_low if base_row["response_class"] == cfg.response_low else bpi_unavailable
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
            "mean_cont_auc_dff",
            "bout_null_q99_auc",
            "cont_null_q99_auc",
            "bout_response_pass",
            "cont_response_pass",
            "response_is_active",
            "response_class",
            "response_summary_class",
            "response_auc_threshold",
            "response_null_quantile",
            "response_null_bootstrap_n",
            "denom",
            "denom_z",
            "bpi",
            "bpi_z",
            "activity_mag",
            "bpi_status",
            "bpi_data_available",
            "bpi_category",
            "bpi_zero_band",
            "bpi_activity_threshold",
        ]
        detail = detail.merge(scored_bpi_df[merge_cols], on=["plane_idx", "func_label"], how="left")
        detail["response_is_active"] = _as_bool_series(detail.get("response_is_active", False)).astype(bool)
        detail["bout_response_pass"] = _as_bool_series(detail.get("bout_response_pass", False)).astype(bool)
        detail["cont_response_pass"] = _as_bool_series(detail.get("cont_response_pass", False)).astype(bool)
        detail["response_class"] = detail["response_class"].fillna(cfg.response_unavailable)
        detail["response_summary_class"] = detail["response_summary_class"].fillna(response_summary_unavailable)
        detail["bpi_category"] = detail["bpi_category"].fillna(bpi_unavailable)
        detail["bpi_status"] = detail["bpi_status"].fillna("response_unavailable")
        detail["bpi_data_available"] = _as_bool_series(detail.get("bpi_data_available", False)).astype(bool)
        detail["bpi_zero_band"] = detail["bpi_zero_band"].fillna(float(cfg.zero_band))
        detail["bpi_activity_threshold"] = detail["bpi_activity_threshold"].fillna(float(cfg.response_min_auc))
        detail["response_auc_threshold"] = detail["response_auc_threshold"].fillna(float(cfg.response_min_auc))
        detail["response_null_quantile"] = detail["response_null_quantile"].fillna(float(cfg.response_null_q))
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
        "df_evt": df_evt,
        "df_stim": df_stim,
        "stim_events": stim_events,
        "stim_source": stim_source,
        "baseline_windows": baseline_windows,
        "prestim_trial_windows": prestim_trial_windows,
        "s2p_map": s2p_map,
    }


__all__ = [
    "ActivityConfig",
    "build_response_bpi_tables",
]
