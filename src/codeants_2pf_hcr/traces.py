"""Trace export helpers for notebook cells [51], [56], [56h], and [57]."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any, Callable

import numpy as np
import pandas as pd

from .stimulus import (
    build_stim_tables,
    effective_motion_window,
    find_experiment_log,
    find_metadata_csv,
    load_events_df,
    load_metadata_params,
    parse_float,
    parse_unilateral_stim,
)
from .suite2p import infer_frame_rate_from_detail


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


@dataclass(frozen=True)
class MotionAucPlotConfig:
    onset_delay_sec: float = 10.0
    stim_time_scale: float = 1.0
    measure_start_block: int | str = 1
    measure_start_event: str = "start"
    remove_interblock_gaps: bool = True
    min_valid_frac: float = 0.5
    min_trials_per_panel: int = 1
    dfof_baseline_pct: float = 10.0
    dfof_eps: float = 1e-6
    response_low_class: str = "low activity"
    response_unavailable_class: str = "response unavailable"
    active_class: str = "Active neurons"
    inactive_class: str = "Low-quality traces"


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


def _find_suite2p_plane_dir_for_auc(detail_df: pd.DataFrame, plane_idx: int, *, suite2p_root: str | Path | None) -> Path | None:
    plane_idx_int = int(plane_idx)
    if "func_source" in detail_df.columns:
        srcs = (
            detail_df.loc[detail_df["plane_idx"] == plane_idx_int, "func_source"]
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
        fallback = Path(suite2p_root) / f"plane{plane_idx_int}"
        if fallback.exists():
            return fallback
    return None


def _find_suite2p_npy(plane_dir: str | Path, kind: str) -> Path | None:
    plane_dir_p = Path(plane_dir)
    exact = plane_dir_p / f"{kind}.npy"
    if exact.exists():
        return exact
    plane_match = re.search(r"plane(\d+)", str(plane_dir_p.name))
    if plane_match:
        tagged_hits = sorted(plane_dir_p.glob(f"*plane{int(plane_match.group(1))}_{kind}.npy"))
        if tagged_hits:
            return tagged_hits[0]
    hits: list[Path] = []
    for pattern in (f"*_{kind}.npy", f"*{kind}.npy"):
        hits.extend(sorted(plane_dir_p.glob(pattern)))
    hits = sorted(set(hits))
    return hits[0] if hits else None


def _block_key_for_auc(block_label: str) -> int | str:
    match = re.search(r"\d+", str(block_label))
    return int(match.group(0)) if match else str(block_label)


def _load_suite2p_dff_map_for_auc(
    detail_df: pd.DataFrame,
    *,
    suite2p_root: str | Path | None,
    baseline_pct: float,
    eps: float,
) -> dict[int, dict[str, Any]]:
    s2p_map: dict[int, dict[str, Any]] = {}
    plane_vals = detail_df["plane_idx"].dropna().astype(int).unique().tolist() if "plane_idx" in detail_df.columns else []
    for plane_idx in sorted(plane_vals):
        plane_dir = _find_suite2p_plane_dir_for_auc(detail_df, plane_idx, suite2p_root=suite2p_root)
        if plane_dir is None:
            continue
        f_path = _find_suite2p_npy(plane_dir, "F")
        if f_path is None:
            continue
        f_raw = np.asarray(np.load(f_path, allow_pickle=True), dtype=np.float32)
        f0 = np.percentile(f_raw, float(baseline_pct), axis=1, keepdims=True)
        dff = (f_raw - f0) / (f0 + float(eps))
        s2p_map[int(plane_idx)] = {
            "dff": dff,
            "plane_dir": str(plane_dir),
            "F_path": str(f_path),
        }
    return s2p_map


def _load_midline_bundle(midline_json: str | Path, *, fish_id: str | None) -> tuple[dict[str, Any], Path]:
    midline_path = Path(midline_json)
    if not midline_path.exists():
        raise RuntimeError(f"[56i] Missing midline JSON: {midline_path}")
    try:
        bundle = json.loads(midline_path.read_text())
    except Exception as exc:
        raise RuntimeError(f"[56i] Failed to parse midline JSON: {midline_path}") from exc
    if not isinstance(bundle, dict):
        raise RuntimeError(f"[56i] Midline JSON must contain an object bundle: {midline_path}")
    bundle_fish = str(bundle.get("fish_id", "")).strip()
    current = str(fish_id or "").strip()
    if bundle_fish and current and bundle_fish != current:
        raise RuntimeError(
            f"[56i] midline bundle fish_id={bundle_fish!r} does not match current fish_id={current!r}."
        )
    return bundle, midline_path


def _infer_midline_space(bundle: dict[str, Any]) -> str:
    src_label = ((bundle.get("base", {}) or {}).get("source_label", None))
    label = str(src_label).strip().lower() if src_label is not None else ""
    if label in {"warped", "tform-preview", "warped-raw"}:
        return "anat"
    return "func"


def _annotate_midline_side(
    df_in: pd.DataFrame,
    bundle: dict[str, Any],
    *,
    x_col: str,
    y_col: str,
    plane_col: str = "plane_idx",
) -> pd.DataFrame:
    out = df_in.copy()
    signed = np.full(len(out), np.nan, dtype=float)
    per_plane = bundle.get("per_plane", {}) if isinstance(bundle.get("per_plane", {}), dict) else {}
    side_labels = bundle.get("side_labels", {}) if isinstance(bundle.get("side_labels", {}), dict) else {}
    pos_label = str(side_labels.get("positive", "left")).strip().lower()
    neg_label = str(side_labels.get("negative", "right")).strip().lower()
    if pos_label not in {"left", "right"}:
        pos_label = "left"
    if neg_label not in {"left", "right"}:
        neg_label = "right" if pos_label == "left" else "left"
    try:
        band = float(bundle.get("manual", {}).get("uncertain_band_px", 0.0))
    except Exception:
        band = 0.0

    for plane_idx, idx in out.groupby(plane_col).groups.items():
        params = per_plane.get(str(int(plane_idx)), per_plane.get(int(plane_idx), None))
        if not isinstance(params, dict):
            continue
        try:
            x0 = float(params.get("x0", np.nan))
            y0 = float(params.get("y0", np.nan))
            th = np.deg2rad(float(params.get("theta_deg", np.nan)))
        except Exception:
            continue
        if not (np.isfinite(x0) and np.isfinite(y0) and np.isfinite(th)):
            continue
        nx = -np.sin(th)
        ny = np.cos(th)
        xv = pd.to_numeric(out.loc[idx, x_col], errors="coerce").to_numpy(dtype=float)
        yv = pd.to_numeric(out.loc[idx, y_col], errors="coerce").to_numpy(dtype=float)
        signed[idx] = (xv - x0) * nx + (yv - y0) * ny

    out["midline_signed_dist_px"] = signed
    out["midline_side"] = "unknown"
    finite = np.isfinite(signed)
    out.loc[finite & (signed > band), "midline_side"] = pos_label
    out.loc[finite & (signed < -band), "midline_side"] = neg_label
    out.loc[finite & (np.abs(signed) <= band), "midline_side"] = "midline"
    out["midline_uncertain"] = np.abs(signed) <= band
    return out


def _build_stim_tables_for_auc(
    detail_df: pd.DataFrame,
    *,
    fish_dir: str | Path,
    fish_id: str,
    config: MotionAucPlotConfig,
    experiment_log_csv: str | Path | None,
    experiment_meta_csv: str | Path | None,
    suite2p_root: str | Path | None,
) -> tuple[float, pd.DataFrame, pd.DataFrame, Path | None, Path]:
    meta_path = Path(experiment_meta_csv) if experiment_meta_csv else find_metadata_csv(fish_dir, fish_id)
    fps = None
    if meta_path is not None and meta_path.exists():
        params = load_metadata_params(meta_path)
        fps = parse_float(params.get("framerate", params.get("frame_rate", params.get("fps"))))
    if fps is None or float(fps) <= 0:
        fps = infer_frame_rate_from_detail(detail_df, suite2p_root=suite2p_root)
    if fps is None or float(fps) <= 0:
        raise RuntimeError("[56i] could not determine frame rate for AUC computation")

    log_path = Path(experiment_log_csv) if experiment_log_csv else find_experiment_log(fish_dir, fish_id)
    if log_path is None or not log_path.exists():
        raise RuntimeError("[56i] experiment log not found for AUC computation")
    df_evt_local = load_events_df(log_path)
    df_evt_local["time"] = df_evt_local["time"].astype(float) * float(config.stim_time_scale)

    if config.measure_start_block is not None:
        block_label = (
            config.measure_start_block
            if isinstance(config.measure_start_block, str)
            else f"B{int(config.measure_start_block)}"
        )
        start_event = f"{block_label}_{config.measure_start_event}"
        start_match = df_evt_local.loc[df_evt_local["event"] == start_event, "time"]
        if len(start_match):
            t0 = float(start_match.iloc[0])
        else:
            block_rows = df_evt_local[df_evt_local["event"].str.startswith(f"{block_label}_")]
            t0 = float(block_rows["time"].min()) if not block_rows.empty else None
        if t0 is not None:
            df_evt_local["time"] = df_evt_local["time"] - t0
            df_evt_local = df_evt_local[df_evt_local["time"] >= 0].reset_index(drop=True)

    if bool(config.remove_interblock_gaps):
        block_codes: list[str] = []
        for event in df_evt_local["event"]:
            match = re.match(r"^(B\d+)_", str(event))
            if match:
                block_codes.append(match.group(1))
        block_codes = sorted(set(block_codes), key=_block_key_for_auc)

        blocks = []
        for block in block_codes:
            block_events = df_evt_local[df_evt_local["event"].str.startswith(f"{block}_")]
            if block_events.empty:
                continue
            block_start = None
            block_end = None
            start_rows = block_events.loc[block_events["event"] == f"{block}_start", "time"]
            if len(start_rows):
                block_start = float(start_rows.iloc[0])
            end_rows = block_events.loc[block_events["event"] == f"{block}_end", "time"]
            if len(end_rows):
                block_end = float(end_rows.iloc[0])
            if block_start is None:
                block_start = float(block_events["time"].min())
            pause_rows = block_events.loc[block_events["event"] == f"{block}_interblock_pause", "time"]
            pause_time = float(pause_rows.iloc[0]) if len(pause_rows) else None
            if block_end is None:
                block_end = pause_time
            elif pause_time is not None:
                block_end = pause_time
            if block_end is None:
                block_end = float(block_events["time"].max())
            blocks.append({"block": block, "start": block_start, "end": block_end})

        if blocks:
            blocks = sorted(blocks, key=lambda block: float(block["start"]))
            shift_map: dict[str, float] = {}
            shift = 0.0
            prev_end = None
            for block in blocks:
                orig_start = float(block["start"])
                orig_end = float(block["end"])
                if prev_end is not None:
                    shift += max(0.0, orig_start - prev_end)
                shift_map[str(block["block"])] = shift
                prev_end = orig_end

            def _shift_evt_time(row: pd.Series) -> float:
                match = re.match(r"^(B\d+)_", str(row["event"]))
                if not match:
                    return float(row["time"])
                return float(row["time"]) - shift_map.get(match.group(1), 0.0)

            df_evt_local["time"] = df_evt_local.apply(_shift_evt_time, axis=1)
            df_evt_local = df_evt_local.sort_values("time").reset_index(drop=True)

    stims = []
    for row in df_evt_local.itertuples(index=False):
        event = str(row.event)
        match = re.match(r"^(B\d+)_stim(\d+)_(.+)$", event)
        if not match:
            continue
        block = match.group(1)
        stim_idx = int(match.group(2))
        stim_type = match.group(3)
        t0 = float(row.time)
        post_name = f"{block}_poststim{stim_idx}_pause"
        end_match = df_evt_local.loc[df_evt_local["event"] == post_name, "time"]
        if len(end_match):
            end_time = float(end_match.iloc[0])
        else:
            after = df_evt_local[
                (df_evt_local["time"] > t0)
                & df_evt_local["event"].str.startswith(f"{block}_")
            ].sort_values("time")
            end_time = float(after["time"].iloc[0]) if not after.empty else np.nan
        stims.append(
            {
                "block": block,
                "stim_idx": stim_idx,
                "type": stim_type,
                "start": t0,
                "end": end_time,
                "duration": end_time - t0 if np.isfinite(end_time) else np.nan,
            }
        )

    df_stim_local = pd.DataFrame(stims)
    if df_stim_local.empty:
        raise RuntimeError("[56i] no stimuli parsed from experiment log")

    stim_side_mode = df_stim_local["type"].apply(lambda s: pd.Series(parse_unilateral_stim(str(s))))
    stim_side_mode.columns = ["stim_side", "stim_mode"]
    df_stim_local = pd.concat([df_stim_local, stim_side_mode], axis=1)
    eff = df_stim_local.apply(
        lambda row: pd.Series(
            effective_motion_window(
                row.get("start", np.nan),
                row.get("duration", np.nan),
                row.get("end", np.nan),
                float(config.onset_delay_sec),
            ),
            index=["motion_start", "motion_end", "motion_duration"],
        ),
        axis=1,
    )
    df_stim_local = pd.concat([df_stim_local, eff], axis=1)
    df_stim_local["trial_id"] = df_stim_local.apply(
        lambda row: f"{row['block']}_stim{int(row['stim_idx'])}_{row['type']}",
        axis=1,
    )
    return float(fps), df_evt_local, df_stim_local, meta_path, Path(log_path)


def _compute_roi_trial_auc_table(
    detail_df: pd.DataFrame,
    s2p_map: dict[int, dict[str, Any]],
    stim_trials_df: pd.DataFrame,
    *,
    fps: float,
    min_valid_frac: float,
) -> pd.DataFrame:
    rows = []
    trials = stim_trials_df.copy()
    trials["idx0"] = np.rint(trials["motion_start"].astype(float) * float(fps)).astype(int)
    trials["idx1"] = np.rint(trials["motion_end"].astype(float) * float(fps)).astype(int)
    trials = trials[trials["idx1"] > trials["idx0"]].reset_index(drop=True)

    for plane_idx in sorted(detail_df["plane_idx"].dropna().astype(int).unique().tolist()):
        pdata = s2p_map.get(int(plane_idx))
        if pdata is None or pdata.get("dff") is None:
            continue
        dff = np.asarray(pdata["dff"], dtype=np.float32)
        n_frames = int(dff.shape[1])
        plane_rois = detail_df.loc[detail_df["plane_idx"] == int(plane_idx), ["func_label"]].copy()
        plane_rois["func_label"] = pd.to_numeric(plane_rois["func_label"], errors="coerce")
        plane_rois = plane_rois.dropna(subset=["func_label"]).copy()
        plane_rois["func_label"] = plane_rois["func_label"].astype(int)
        plane_rois["roi_idx"] = plane_rois["func_label"] - 1
        plane_rois = plane_rois[(plane_rois["roi_idx"] >= 0) & (plane_rois["roi_idx"] < dff.shape[0])].copy()
        if plane_rois.empty:
            continue

        func_labels = plane_rois["func_label"].to_numpy(dtype=int)
        roi_idx = plane_rois["roi_idx"].to_numpy(dtype=int)
        for stim in trials.itertuples(index=False):
            idx0 = int(stim.idx0)
            idx1 = int(stim.idx1)
            win_len = idx1 - idx0
            if win_len <= 0:
                continue
            src0 = max(idx0, 0)
            src1 = min(idx1, n_frames)
            if src1 <= src0:
                continue
            seg = dff[roi_idx, src0:src1]
            n_valid = np.isfinite(seg).sum(axis=1)
            min_pts = max(1, int(np.ceil(float(min_valid_frac) * float(win_len))))
            auc_vals = np.full(func_labels.shape, np.nan, dtype=np.float32)
            ok = n_valid >= min_pts
            if np.any(ok):
                auc_vals[ok] = np.nansum(seg[ok], axis=1).astype(np.float32) / float(fps)
            rows.append(
                pd.DataFrame(
                    {
                        "plane_idx": int(plane_idx),
                        "func_label": func_labels,
                        "trial_id": stim.trial_id,
                        "stim_side": stim.stim_side,
                        "stim_mode": stim.stim_mode,
                        "auc_dff": auc_vals,
                        "n_valid_frames": n_valid.astype(int),
                        "window_frames": int(win_len),
                    }
                )
            )
    if not rows:
        return pd.DataFrame(columns=["plane_idx", "func_label", "trial_id", "stim_side", "stim_mode", "auc_dff"])
    return pd.concat(rows, ignore_index=True)


def _compute_laterality_for_auc(roi_side: str | None, stim_side: str | None) -> str | float:
    if roi_side not in {"left", "right"} or stim_side not in {"left", "right"}:
        return np.nan
    return "ipsi" if roi_side == stim_side else "contra"


def build_single_fish_motion_auc_plot_tables(
    *,
    fish_dir: str | Path,
    fish_id: str,
    out_reg: str | Path,
    run_config: dict[str, Any] | None = None,
    suite2p_root: str | Path | None = None,
    detail_csv: str | Path | None = None,
    hcr_status_csv: str | Path | None = None,
    midline_json: str | Path | None = None,
    experiment_log_csv: str | Path | None = None,
    experiment_meta_csv: str | Path | None = None,
    points_csv: str | Path | None = None,
    counts_csv: str | Path | None = None,
    roi_panel_csv: str | Path | None = None,
    write_csv: bool = True,
    config: MotionAucPlotConfig | None = None,
) -> dict[str, Any]:
    cfg = config or MotionAucPlotConfig(
        onset_delay_sec=float((run_config or {}).get("STIM_ONSET_DELAY_SEC", 10.0)),
        remove_interblock_gaps=bool((run_config or {}).get("REMOVE_INTERBLOCK_GAPS", True)),
    )
    rc = run_config if isinstance(run_config, dict) else {}
    out_reg_p = Path(out_reg)
    detail_csv_p = Path(detail_csv) if detail_csv is not None else (out_reg_p / "functional_roi_activity_identity.csv")
    hcr_status_csv_p = Path(hcr_status_csv) if hcr_status_csv is not None else (out_reg_p / "hcr_activity_status.csv")
    midline_json_p = Path(midline_json) if midline_json is not None else (out_reg_p / "midline_params_func_ref.json")
    points_csv_p = Path(points_csv) if points_csv is not None else (out_reg_p / "motion_auc_plot_points.csv")
    counts_csv_p = Path(counts_csv) if counts_csv is not None else (out_reg_p / "motion_auc_plot_counts.csv")
    roi_panel_csv_p = Path(roi_panel_csv) if roi_panel_csv is not None else (out_reg_p / "motion_auc_roi_panels.csv")
    gene_order = list(rc.get("GENE_ORDER", ["sst1.1", "sst1.2", "npy", "tac3b", "pth2", "cfos", "cort"]))

    if not detail_csv_p.exists():
        raise RuntimeError(f"[56i] Missing master ROI table: {detail_csv_p} (run [50i] first).")
    if not hcr_status_csv_p.exists():
        raise RuntimeError(f"[56i] Missing HCR activity status table: {hcr_status_csv_p} (run [50] first).")

    detail_df = pd.read_csv(detail_csv_p)
    status_df = pd.read_csv(hcr_status_csv_p)
    if "fish_id" in detail_df.columns:
        detail_df = detail_df[detail_df["fish_id"].astype(str) == str(fish_id)].copy()
    if "fish_id" in status_df.columns:
        status_df = status_df[status_df["fish_id"].astype(str) == str(fish_id)].copy()
    if detail_df.empty:
        raise RuntimeError("[56i] functional_roi_activity_identity.csv is empty for the current fish.")
    if status_df.empty:
        raise RuntimeError("[56i] hcr_activity_status.csv is empty for the current fish.")

    bundle, bundle_path = _load_midline_bundle(midline_json_p, fish_id=fish_id)
    coord_space = _infer_midline_space(bundle)
    if coord_space == "anat" and {"centroid_x_anat", "centroid_y_anat"}.issubset(detail_df.columns):
        x_col = "centroid_x_anat"
        y_col = "centroid_y_anat"
    elif {"centroid_x_func", "centroid_y_func"}.issubset(detail_df.columns):
        x_col = "centroid_x_func"
        y_col = "centroid_y_func"
    else:
        raise RuntimeError("[56i] master ROI table missing centroid columns required for midline assignment.")

    detail_side_df = _annotate_midline_side(detail_df, bundle, x_col=x_col, y_col=y_col, plane_col="plane_idx")
    detail_side_df = detail_side_df.rename(columns={"midline_side": "roi_side"})
    detail_side_df["roi_side_valid"] = detail_side_df["roi_side"].isin({"left", "right"})
    detail_side_df["func_label"] = pd.to_numeric(detail_side_df["func_label"], errors="coerce")
    detail_side_df["plane_idx"] = pd.to_numeric(detail_side_df["plane_idx"], errors="coerce")
    detail_side_df = detail_side_df.dropna(subset=["plane_idx", "func_label"]).copy()
    detail_side_df["plane_idx"] = detail_side_df["plane_idx"].astype(int)
    detail_side_df["func_label"] = detail_side_df["func_label"].astype(int)
    required_response_cols = {"response_class", "response_is_active", "bpi_category"}
    missing_response_cols = sorted(required_response_cols - set(detail_side_df.columns))
    if missing_response_cols:
        raise RuntimeError(
            f"[56i] master ROI table is missing response-aware columns {missing_response_cols}; rerun [50ia] and refresh functional_roi_activity_identity.csv before plotting AUC."
        )
    detail_side_df["response_is_active"] = detail_side_df["response_is_active"].map(lambda v: bool(v) if pd.notna(v) else False)
    detail_side_df["response_class"] = detail_side_df["response_class"].fillna(cfg.response_unavailable_class).astype(str)
    if "suite2p_is_cell" not in detail_side_df.columns:
        detail_side_df["suite2p_is_cell"] = detail_side_df.get("is_active", False)
    detail_side_df["suite2p_is_cell"] = detail_side_df["suite2p_is_cell"].map(lambda v: bool(v) if pd.notna(v) else False)
    suite2p_class_default = pd.Series(
        np.where(detail_side_df["suite2p_is_cell"], cfg.active_class, cfg.inactive_class),
        index=detail_side_df.index,
    )
    if "suite2p_activity_class" not in detail_side_df.columns:
        detail_side_df["suite2p_activity_class"] = suite2p_class_default
    else:
        detail_side_df["suite2p_activity_class"] = detail_side_df["suite2p_activity_class"].where(
            detail_side_df["suite2p_activity_class"].notna(),
            suite2p_class_default,
        )
    detail_side_df["suite2p_activity_class"] = detail_side_df["suite2p_activity_class"].astype(str).replace({"Inactive neurons": cfg.inactive_class})
    detail_auc_df = detail_side_df[detail_side_df["suite2p_is_cell"]].copy()
    if detail_auc_df.empty:
        raise RuntimeError("[56i] No high-quality Suite2p traces available for AUC plotting after applying the suite2p_is_cell gate.")

    fps, _, df_stim_local, meta_path, log_path = _build_stim_tables_for_auc(
        detail_side_df,
        fish_dir=fish_dir,
        fish_id=fish_id,
        config=cfg,
        experiment_log_csv=experiment_log_csv,
        experiment_meta_csv=experiment_meta_csv,
        suite2p_root=suite2p_root,
    )
    stim_trials = df_stim_local[
        df_stim_local["stim_side"].notna()
        & df_stim_local["stim_mode"].isin({"bout", "continuous"})
        & np.isfinite(df_stim_local["motion_start"])
        & np.isfinite(df_stim_local["motion_end"])
    ].copy()
    if stim_trials.empty:
        raise RuntimeError("[56i] No unilateral stimulus windows available for AUC plotting.")

    s2p_map = _load_suite2p_dff_map_for_auc(
        detail_auc_df,
        suite2p_root=suite2p_root,
        baseline_pct=float(cfg.dfof_baseline_pct),
        eps=float(cfg.dfof_eps),
    )
    if not s2p_map:
        raise RuntimeError("[56i] Could not load Suite2p traces from disk for AUC plotting.")

    roi_trial_auc_df = _compute_roi_trial_auc_table(
        detail_auc_df,
        s2p_map,
        stim_trials,
        fps=fps,
        min_valid_frac=float(cfg.min_valid_frac),
    )
    if roi_trial_auc_df.empty:
        raise RuntimeError("[56i] No ROI AUC values could be computed from the motion windows.")

    roi_trial_auc_df = roi_trial_auc_df.merge(
        detail_auc_df[
            [
                "plane_idx",
                "func_label",
                "response_class",
                "response_is_active",
                "bpi_category",
                "suite2p_activity_class",
                "suite2p_is_cell",
                "roi_side",
                "roi_side_valid",
            ]
        ].drop_duplicates(subset=["plane_idx", "func_label"]),
        on=["plane_idx", "func_label"],
        how="left",
    )
    roi_trial_auc_df["laterality"] = [
        _compute_laterality_for_auc(roi_side, stim_side)
        for roi_side, stim_side in roi_trial_auc_df[["roi_side", "stim_side"]].itertuples(index=False)
    ]

    roi_panel_df = (
        roi_trial_auc_df.dropna(subset=["laterality"])
        .groupby(
            [
                "plane_idx",
                "func_label",
                "response_class",
                "response_is_active",
                "bpi_category",
                "suite2p_activity_class",
                "suite2p_is_cell",
                "roi_side",
                "roi_side_valid",
                "stim_mode",
                "laterality",
            ],
            as_index=False,
        )
        .agg(
            auc_dff=("auc_dff", "mean"),
            n_trials=("trial_id", "nunique"),
        )
    )
    roi_panel_df = roi_panel_df[roi_panel_df["n_trials"] >= int(cfg.min_trials_per_panel)].copy()
    roi_panel_df = roi_panel_df.merge(
        detail_auc_df[["plane_idx", "func_label", "identity_display_label", "has_identity_assigned"]].drop_duplicates(subset=["plane_idx", "func_label"]),
        on=["plane_idx", "func_label"],
        how="left",
    )

    panel_keys = pd.DataFrame(
        [
            {"laterality": "ipsi", "stim_mode": "bout"},
            {"laterality": "ipsi", "stim_mode": "continuous"},
            {"laterality": "contra", "stim_mode": "bout"},
            {"laterality": "contra", "stim_mode": "continuous"},
        ]
    )
    all_points_df = roi_panel_df.copy()
    all_points_df["group"] = "All neurons"
    all_points_df["population"] = "all_neurons"
    all_points_df["group_type"] = "all"
    all_points_df["point_label_id"] = all_points_df.apply(
        lambda row: f"roi:p{int(row['plane_idx'])}:f{int(row['func_label'])}",
        axis=1,
    )

    all_counts_base = (
        detail_side_df[
            ["plane_idx", "func_label", "response_class", "response_is_active", "roi_side", "roi_side_valid"]
        ]
        .drop_duplicates(subset=["plane_idx", "func_label"])
        .assign(_join_key=1)
        .merge(panel_keys.assign(_join_key=1), on="_join_key", how="inner")
        .drop(columns=["_join_key"])
    )
    all_counts_base = all_counts_base.merge(
        roi_panel_df[["plane_idx", "func_label", "laterality", "stim_mode", "auc_dff"]],
        on=["plane_idx", "func_label", "laterality", "stim_mode"],
        how="left",
    )
    all_counts_base["panel_usable"] = np.isfinite(all_counts_base["auc_dff"])
    counts_all = (
        all_counts_base.groupby(["laterality", "stim_mode"], as_index=False)
        .agg(
            n_responsive_used=("panel_usable", lambda s: int(((s.astype(bool)) & (all_counts_base.loc[s.index, "response_is_active"].astype(bool))).sum())),
            n_low_used=("panel_usable", lambda s: int(((s.astype(bool)) & (all_counts_base.loc[s.index, "response_class"].astype(str).eq(cfg.response_low_class))).sum())),
            n_total=("panel_usable", "size"),
        )
    )
    counts_all["n_other"] = counts_all["n_total"] - counts_all["n_responsive_used"] - counts_all["n_low_used"]
    counts_all["group"] = "All neurons"

    onplane_status_df = status_df[status_df["functional_status"].astype(str) != "out-of-plane anatomy label"].copy()
    onplane_status_df["selected_plane_idx"] = pd.to_numeric(onplane_status_df["selected_plane"], errors="coerce")
    onplane_status_df["selected_func_label_int"] = pd.to_numeric(onplane_status_df["selected_func_label"], errors="coerce")
    status_panel_df = (
        onplane_status_df.assign(_join_key=1)
        .merge(panel_keys.assign(_join_key=1), on="_join_key", how="inner")
        .drop(columns=["_join_key"])
    )
    status_panel_df = status_panel_df.merge(
        roi_panel_df[
            [
                "plane_idx",
                "func_label",
                "laterality",
                "stim_mode",
                "auc_dff",
                "n_trials",
                "roi_side",
                "roi_side_valid",
                "response_class",
                "response_is_active",
                "bpi_category",
                "suite2p_activity_class",
                "suite2p_is_cell",
            ]
        ],
        left_on=["selected_plane_idx", "selected_func_label_int", "laterality", "stim_mode"],
        right_on=["plane_idx", "func_label", "laterality", "stim_mode"],
        how="left",
        suffixes=("", "_roi"),
    )
    status_panel_df["panel_usable"] = np.isfinite(status_panel_df["auc_dff"])
    status_panel_df["selected_response_is_active"] = status_panel_df["response_is_active"].map(lambda v: bool(v) if pd.notna(v) else False)
    status_panel_df["selected_response_class"] = status_panel_df["response_class"].fillna(cfg.response_unavailable_class).astype(str)

    gene_points_df = status_panel_df[status_panel_df["panel_usable"]].copy()
    gene_points_df = gene_points_df.sort_values(
        ["gene", "selected_plane_idx", "selected_func_label_int", "laterality", "stim_mode", "anat_label", "conf_label"]
    )
    n_gene_point_rows_raw = int(len(gene_points_df))
    gene_points_df = gene_points_df.drop_duplicates(
        subset=["gene", "selected_plane_idx", "selected_func_label_int", "laterality", "stim_mode"],
        keep="first",
    ).copy()
    n_gene_point_rows_dedup = int(len(gene_points_df))
    gene_points_df["group"] = gene_points_df["gene"].astype(str)
    gene_points_df["population"] = "identified_gene"
    gene_points_df["group_type"] = "gene"
    gene_points_df["plane_idx"] = gene_points_df["selected_plane_idx"].astype(int)
    gene_points_df["func_label"] = gene_points_df["selected_func_label_int"].astype(int)
    gene_points_df["response_class"] = gene_points_df["selected_response_class"]
    gene_points_df["response_is_active"] = gene_points_df["selected_response_is_active"].astype(bool)
    gene_points_df["bpi_category"] = gene_points_df["bpi_category"].fillna(cfg.response_unavailable_class).astype(str)
    gene_points_df["point_label_id"] = gene_points_df.apply(
        lambda row: f"gene:{row['gene']}:p{int(row['plane_idx'])}:f{int(row['func_label'])}",
        axis=1,
    )

    counts_gene = (
        status_panel_df.groupby(["gene", "laterality", "stim_mode"], as_index=False)
        .agg(
            n_responsive_used=("panel_usable", lambda s: int(((s.astype(bool)) & (status_panel_df.loc[s.index, "selected_response_is_active"].astype(bool))).sum())),
            n_low_used=("panel_usable", lambda s: int(((s.astype(bool)) & (status_panel_df.loc[s.index, "selected_response_class"].astype(str).eq(cfg.response_low_class))).sum())),
            n_total=("panel_usable", "size"),
        )
    )
    counts_gene["n_other"] = counts_gene["n_total"] - counts_gene["n_responsive_used"] - counts_gene["n_low_used"]
    counts_gene = counts_gene.rename(columns={"gene": "group"})

    genes_present = sorted(set(onplane_status_df["gene"].dropna().astype(str).tolist()))
    ordered_genes = [gene for gene in gene_order if gene in genes_present]
    ordered_genes.extend([gene for gene in genes_present if gene not in ordered_genes])
    group_order = ["All neurons"] + ordered_genes

    plot_df = pd.concat(
        [
            all_points_df[
                [
                    "group",
                    "population",
                    "group_type",
                    "plane_idx",
                    "func_label",
                    "laterality",
                    "stim_mode",
                    "auc_dff",
                    "n_trials",
                    "response_class",
                    "response_is_active",
                    "bpi_category",
                    "point_label_id",
                ]
            ],
            gene_points_df[
                [
                    "group",
                    "population",
                    "group_type",
                    "plane_idx",
                    "func_label",
                    "laterality",
                    "stim_mode",
                    "auc_dff",
                    "n_trials",
                    "response_class",
                    "response_is_active",
                    "bpi_category",
                    "point_label_id",
                ]
            ],
        ],
        ignore_index=True,
    )
    plot_df["group"] = pd.Categorical(plot_df["group"], categories=group_order, ordered=True)
    plot_df["bpi_category"] = plot_df["bpi_category"].fillna(cfg.response_unavailable_class).astype(str)
    plot_df = plot_df.sort_values(
        ["laterality", "stim_mode", "group", "response_class", "plane_idx", "func_label"]
    ).reset_index(drop=True)

    counts_df = pd.concat(
        [
            counts_all[["group", "laterality", "stim_mode", "n_responsive_used", "n_low_used", "n_other", "n_total"]],
            counts_gene[["group", "laterality", "stim_mode", "n_responsive_used", "n_low_used", "n_other", "n_total"]],
        ],
        ignore_index=True,
    )
    counts_df["group"] = pd.Categorical(counts_df["group"], categories=group_order, ordered=True)
    counts_df = counts_df.sort_values(["laterality", "stim_mode", "group"]).reset_index(drop=True)
    for col in ["n_responsive_used", "n_low_used", "n_other", "n_total"]:
        counts_df[col] = pd.to_numeric(counts_df[col], errors="coerce").fillna(0).astype(int)
    counts_df["frac_responsive_used"] = np.where(counts_df["n_total"] > 0, counts_df["n_responsive_used"] / counts_df["n_total"], 0.0)
    counts_df["frac_low_used"] = np.where(counts_df["n_total"] > 0, counts_df["n_low_used"] / counts_df["n_total"], 0.0)
    counts_df["frac_other"] = np.where(counts_df["n_total"] > 0, counts_df["n_other"] / counts_df["n_total"], 0.0)

    if write_csv:
        out_reg_p.mkdir(parents=True, exist_ok=True)
        roi_panel_df.to_csv(roi_panel_csv_p, index=False)
        plot_df.to_csv(points_csv_p, index=False)
        counts_df.to_csv(counts_csv_p, index=False)

    messages = [
        (
            f"[56i] motion-window AUC computed from {len(stim_trials)} unilateral trials "
            f"({len(stim_trials[stim_trials['stim_mode'] == 'bout'])} bout, "
            f"{len(stim_trials[stim_trials['stim_mode'] == 'continuous'])} continuous); "
            f"fps={fps:.3f} Hz"
        ),
        f"[56i] experiment log -> {log_path}",
    ]
    if meta_path is not None:
        messages.append(f"[56i] metadata -> {meta_path}")
    messages.extend(
        [
            f"[56i] midline -> {bundle_path} (space={coord_space}, centroid columns={x_col},{y_col})",
            f"[56i] ROI panel table -> {roi_panel_csv_p}",
            f"[56i] plot points -> {points_csv_p}",
            f"[56i] plot counts -> {counts_csv_p}",
            (
                f"[56i] plotted points: all neurons={len(all_points_df)}; "
                f"gene rows raw={n_gene_point_rows_raw}; gene rows deduped={n_gene_point_rows_dedup}"
            ),
        ]
    )

    return {
        "fps": float(fps),
        "df_stim": df_stim_local,
        "roi_panel_df": roi_panel_df,
        "points_df": plot_df,
        "counts_df": counts_df,
        "ordered_genes": ordered_genes,
        "messages": messages,
        "points_csv": points_csv_p,
        "counts_csv": counts_csv_p,
        "roi_panel_csv": roi_panel_csv_p,
    }


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
    "MotionAucPlotConfig",
    "TraceExportConfig",
    "build_single_fish_motion_auc_plot_tables",
    "export_suite2p_trace_metadata",
    "prepare_pairs_for_unique_cells",
    "resolve_conf_func_csv_analysis",
]
