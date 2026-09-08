"""Suite2p stage helpers for notebook cells [23a], [25], and trace consumers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import pathlib
import re
from typing import Any

import numpy as np
import pandas as pd

from .stimulus import StimulusConfig, build_prestim_baseline_windows, compute_zscore_stats, resolve_plane_stimulus_contexts


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


def build_suite2p_all_roi_labels(stat: np.ndarray, ops: dict[str, Any]) -> np.ndarray:
    """Rasterize every Suite2p ROI, preserving its one-based stat.npy label."""
    include_all = np.ones((len(stat), 1), dtype=bool)
    labels, _ = _build_labels_from_stat(stat, include_all, ops)
    return labels


@dataclass(frozen=True)
class Suite2pStageConfig:
    use_suite2p_labels: bool = True
    plane_glob: str = "plane*"
    flip_x: bool | None = None
    # Suite2p's stat.npy pixel coordinates may predate a later canonical-TIFF
    # preprocessing run.  This is deliberately independent of the frame of
    # the TIFF paths recorded in ops.npy.
    stat_xy_frame: str | None = None
    dfof_baseline_pct: float = 10.0
    dfof_eps: float = 1e-6
    verbose: bool = True


@dataclass(frozen=True)
class Suite2pStimulusLockedDiagnosticConfig:
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
    line_alpha: float = 0.22
    line_width: float = 0.8
    heatmap_vmin: float = 0.0
    heatmap_vmax: float = 5.0
    stim_bar_alpha: float = 0.28
    filter_trace_panels_response_active: bool = True
    save_figures: bool = True
    show_figures: bool = True
    figure_dpi: int = 300
    verbose: bool = True


def _extract_trace_window(
    trace: np.ndarray,
    idx0: int,
    idx1: int,
    *,
    mode: str,
    min_valid_frac: float,
) -> np.ndarray | None:
    arr = np.asarray(trace, dtype=np.float32)
    win_len = int(idx1 - idx0)
    if win_len <= 0:
        return None
    mode_clean = str(mode).strip().lower()
    if mode_clean == "strict":
        if idx0 < 0 or idx1 > arr.shape[0]:
            return None
        seg = arr[int(idx0) : int(idx1)]
    elif mode_clean in {"pad_nan", "pad", "nan"}:
        seg = np.full(win_len, np.nan, dtype=np.float32)
        src0 = max(int(idx0), 0)
        src1 = min(int(idx1), int(arr.shape[0]))
        if src1 > src0:
            dst0 = src0 - int(idx0)
            seg[dst0 : dst0 + (src1 - src0)] = arr[src0:src1]
    else:
        raise ValueError(f"unsupported window extraction mode: {mode}")
    n_valid = int(np.isfinite(seg).sum())
    min_required = max(1, int(np.ceil(float(min_valid_frac) * float(win_len))))
    return seg if n_valid >= min_required else None


def _session_palette(session_labels: list[str]) -> dict[str, Any]:
    import matplotlib.pyplot as plt

    labels = list(dict.fromkeys([str(label) for label in session_labels]))
    cmap = plt.get_cmap("tab10") if len(labels) <= 10 else plt.get_cmap("tab20")
    return {label: cmap(idx % cmap.N) for idx, label in enumerate(labels)}


def _block_key(block: str) -> tuple[int, str]:
    match = re.match(r"^B(\d+)$", str(block).strip())
    if match:
        return int(match.group(1)), str(block)
    return 10**9, str(block)


def _logged_block_starts(df_evt: pd.DataFrame, blocks: list[str]) -> dict[str, float]:
    out: dict[str, float] = {}
    for block in blocks:
        block_events = df_evt[df_evt["event"].astype(str).str.startswith(f"{block}_")].copy()
        if block_events.empty:
            continue
        start_rows = block_events.loc[block_events["event"].astype(str) == f"{block}_start", "time"]
        if len(start_rows):
            out[str(block)] = float(start_rows.iloc[0])
        else:
            out[str(block)] = float(pd.to_numeric(block_events["time"], errors="coerce").min())
    return out


def _event_blocks(df_evt: pd.DataFrame) -> list[str]:
    if df_evt.empty or "event" not in df_evt.columns:
        return []
    blocks: list[str] = []
    for event in df_evt["event"].astype(str):
        match = re.match(r"^(B\d+)_", event)
        if match:
            blocks.append(match.group(1))
    return sorted(set(blocks), key=_block_key)


def _schedule_block_starts(block_table: pd.DataFrame | None) -> tuple[list[str], dict[str, float]]:
    if not isinstance(block_table, pd.DataFrame) or block_table.empty:
        return [], {}
    required = {"block", "start"}
    if not required.issubset(block_table.columns):
        return [], {}
    work = block_table.copy()
    work["block"] = work["block"].astype(str)
    work["start"] = pd.to_numeric(work["start"], errors="coerce")
    work = work[work["block"].ne("") & work["start"].notna()].copy()
    if work.empty:
        return [], {}
    work = work.sort_values("block", key=lambda col: col.map(_block_key))
    blocks = list(dict.fromkeys(work["block"].tolist()))
    starts = {str(row.block): float(row.start) for row in work.itertuples(index=False)}
    return blocks, starts


def _remap_stimulus_tables_to_frame_grid(
    df_evt: pd.DataFrame,
    df_stim: pd.DataFrame,
    *,
    n_frames: int,
    fps: float,
    block_table: pd.DataFrame | None = None,
    tag: str = "[23c]",
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Place parsed stimulus blocks on equal Suite2p frame-count boundaries."""
    schedule_blocks, schedule_starts = _schedule_block_starts(block_table)
    if df_stim.empty or "block" not in df_stim.columns:
        return df_evt.copy(), df_stim.copy(), {"timing_mode": "frame_grid", "n_blocks": 0}
    if schedule_blocks:
        blocks = schedule_blocks
        logged_starts = schedule_starts
        timing_source = "planned_schedule"
    else:
        event_blocks = _event_blocks(df_evt)
        stim_blocks = sorted({str(value) for value in df_stim["block"].dropna().astype(str)}, key=_block_key)
        blocks = event_blocks or stim_blocks
        logged_starts = _logged_block_starts(df_evt, blocks)
        timing_source = "experiment_log"
    n_blocks = len(blocks)
    if n_blocks <= 0:
        return df_evt.copy(), df_stim.copy(), {"timing_mode": "frame_grid", "n_blocks": 0}
    if int(n_frames) <= 0:
        raise RuntimeError(f"{tag} cannot compute frame-grid block starts from empty Suite2p traces")
    if int(n_frames) % int(n_blocks) != 0:
        raise RuntimeError(
            f"{tag} Suite2p frame count ({int(n_frames)}) is not divisible by parsed block count ({int(n_blocks)}); "
            "cannot place block starts on an equal frame grid."
        )
    frames_per_block = int(n_frames) // int(n_blocks)
    missing = [block for block in blocks if block not in logged_starts or not np.isfinite(logged_starts[block])]
    if missing:
        raise RuntimeError(f"{tag} could not resolve logged start time for block(s): {', '.join(missing)}")

    block_start_frames = {block: idx * frames_per_block for idx, block in enumerate(blocks)}
    block_start_seconds = {block: float(frame) / float(fps) for block, frame in block_start_frames.items()}

    evt = df_evt.copy()

    def remap_event_time(row: pd.Series) -> float:
        match = re.match(r"^(B\d+)_", str(row["event"]))
        if not match:
            return float(row["time"])
        block = match.group(1)
        if block not in block_start_seconds:
            return float(row["time"])
        return block_start_seconds[block] + (float(row["time"]) - float(logged_starts[block]))

    evt["time"] = evt.apply(remap_event_time, axis=1)
    evt = evt.sort_values("time").reset_index(drop=True)

    stim = df_stim.copy()
    for col in ("start", "end", "motion_start", "motion_end"):
        if col not in stim.columns:
            continue
        remapped: list[float] = []
        for row in stim.itertuples(index=False):
            block = str(getattr(row, "block"))
            value = getattr(row, col)
            if block in block_start_seconds and pd.notna(value):
                remapped.append(block_start_seconds[block] + (float(value) - float(logged_starts[block])))
            else:
                remapped.append(float(value) if pd.notna(value) else np.nan)
        stim[col] = remapped

    return evt, stim, {
        "timing_mode": "frame_grid",
        "timing_source": timing_source,
        "n_blocks": int(n_blocks),
        "frames_per_block": int(frames_per_block),
        "block_start_frames": block_start_frames,
    }


def build_suite2p_stimulus_locked_diagnostic(
    *,
    fish_dir: str | Path,
    fish_id: str,
    suite2p_by_ref_idx: dict[int, dict[str, Any]],
    experiment_log_csv: str | Path | None = None,
    experiment_meta_csv: str | Path | None = None,
    frame_rate: float | None = None,
    config: Suite2pStimulusLockedDiagnosticConfig | None = None,
) -> dict[str, Any]:
    cfg = config or Suite2pStimulusLockedDiagnosticConfig()
    if not suite2p_by_ref_idx:
        raise RuntimeError("[23c] Suite2p data not loaded; run [23a] first.")
    plane_indices = sorted(int(plane) for plane in suite2p_by_ref_idx.keys())
    stim_cfg = StimulusConfig(
        stim_time_scale=float(cfg.stim_time_scale),
        measure_start_block=cfg.measure_start_block,
        measure_start_event=cfg.measure_start_event,
        remove_interblock_gaps=bool(cfg.remove_interblock_gaps),
        onset_delay_sec=float(cfg.onset_delay_sec),
    )
    plane_contexts = resolve_plane_stimulus_contexts(
        fish_dir=fish_dir,
        fish_id=fish_id,
        plane_indices=plane_indices,
        experiment_log_csv=experiment_log_csv,
        experiment_meta_csv=experiment_meta_csv,
        frame_rate=frame_rate,
        config=stim_cfg,
    )

    trace_rows: list[dict[str, Any]] = []
    source_rows: list[dict[str, Any]] = []
    all_stim_types: list[str] = []
    session_labels_seen: list[str] = []
    tvec_ref: np.ndarray | None = None
    duration_by_stim: dict[str, list[float]] = {}
    full_session_traces: list[np.ndarray] = []
    full_session_rows: list[dict[str, Any]] = []
    full_session_spans: list[dict[str, Any]] = []
    full_session_block_starts: list[dict[str, Any]] = []

    for plane_idx in plane_indices:
        plane = suite2p_by_ref_idx.get(int(plane_idx), {})
        dff = plane.get("dff")
        if dff is None:
            continue
        dff_arr = np.asarray(dff, dtype=np.float32)
        if dff_arr.ndim != 2 or dff_arr.size == 0:
            continue
        ctx = plane_contexts.get(int(plane_idx))
        if ctx is None:
            continue
        fps_value = ctx.get("frame_rate")
        if fps_value is None or float(fps_value) <= 0:
            ops = plane.get("ops", {}) if isinstance(plane.get("ops", {}), dict) else {}
            fps_value = ops.get("fs", None)
        if fps_value is None or float(fps_value) <= 0:
            raise RuntimeError(f"[23c] could not resolve frame rate for plane {plane_idx}")
        fps = float(fps_value)
        n_pre = int(round(float(cfg.pre_sec) * fps))
        n_post = int(round(float(cfg.post_sec) * fps))
        if n_pre <= 0 or n_post <= 0:
            raise RuntimeError("[23c] pre/post diagnostic windows must be positive")
        tvec = (np.arange(-n_pre, n_post, dtype=np.float32) / fps).astype(np.float32)
        if tvec_ref is None:
            tvec_ref = tvec
        elif tvec_ref.shape != tvec.shape or not np.allclose(tvec_ref, tvec, atol=1e-6):
            raise RuntimeError("[23c] inconsistent frame rates or diagnostic windows across sessions")

        schedule_blocks = ctx.get("planned_schedule_blocks")
        schedule_stimuli = ctx.get("planned_schedule_stimuli")
        use_schedule_timing = isinstance(schedule_blocks, pd.DataFrame) and not schedule_blocks.empty
        df_evt_source = ctx.get("df_evt_full", ctx["df_evt"]) if use_schedule_timing else ctx["df_evt"]
        df_stim_source = schedule_stimuli if isinstance(schedule_stimuli, pd.DataFrame) and not schedule_stimuli.empty else ctx["df_stim"]
        df_evt, df_stim, timing_meta = _remap_stimulus_tables_to_frame_grid(
            df_evt_source.copy(),
            df_stim_source.copy(),
            n_frames=int(dff_arr.shape[1]),
            fps=fps,
            block_table=schedule_blocks if use_schedule_timing else None,
            tag="[23c]",
        )
        if df_stim.empty:
            continue
        stim_types = [str(value) for value in (ctx.get("stimulus_types") or df_stim["type"].astype(str).unique().tolist())]
        for stim_type in stim_types:
            if stim_type not in all_stim_types:
                all_stim_types.append(stim_type)

        session_label = str(ctx.get("session_label") or "r1")
        session_labels_seen.append(session_label)
        baseline_windows = build_prestim_baseline_windows(
            df_evt,
            fps,
            float(cfg.onset_delay_sec),
            tag="[23c]",
        )
        zstats = compute_zscore_stats(
            dff_arr,
            baseline_windows,
            min_points=int(cfg.zscore_min_baseline_points),
            sigma_eps=float(cfg.zscore_min_baseline_std),
        )
        keep = np.ones(dff_arr.shape[0], dtype=bool)
        if bool(cfg.suite2p_cells_only):
            iscell = plane.get("iscell", None)
            if iscell is not None:
                keep = np.asarray(iscell)[:, 0].astype(bool)
            elif plane.get("iscell_keep") is not None:
                keep = np.asarray(plane.get("iscell_keep"), dtype=bool)
        valid_roi = keep & np.asarray(zstats["valid"], dtype=bool)
        source_rows.append(
            {
                "plane_idx": int(plane_idx),
                "session_label": session_label,
                "fps": fps,
                "log_path": str(ctx.get("log_path")),
                "meta_path": str(ctx.get("meta_path")) if ctx.get("meta_path") is not None else None,
                "stimulus_types_source": ctx.get("stimulus_types_source"),
                "stimulus_types": ", ".join(stim_types),
                "session_mapping_source": ctx.get("session_mapping_source"),
                "n_stimuli": int(len(df_stim)),
                "stimulus_timing_mode": timing_meta.get("timing_mode"),
                "stimulus_timing_source": timing_meta.get("timing_source"),
                "n_blocks": timing_meta.get("n_blocks"),
                "frames_per_block": timing_meta.get("frames_per_block"),
                "n_rois": int(dff_arr.shape[0]),
                "n_suite2p_cells": int(keep.sum()),
                "n_zscore_valid": int(valid_roi.sum()),
            }
        )
        if not valid_roi.any():
            continue

        z = (dff_arr - np.asarray(zstats["mu"])[:, np.newaxis]) / np.asarray(zstats["sigma"])[:, np.newaxis]
        plane_row_start = len(full_session_traces)
        for roi_idx in np.where(valid_roi)[0]:
            full_session_traces.append(z[int(roi_idx)].astype(np.float32, copy=False))
            full_session_rows.append(
                {
                    "fish_id": str(fish_id),
                    "plane_idx": int(plane_idx),
                    "func_label": int(roi_idx) + 1,
                    "roi_idx": int(roi_idx),
                    "session_label": session_label,
                    "n_frames": int(dff_arr.shape[1]),
                }
            )
        plane_row_end = len(full_session_traces)
        if plane_row_end > plane_row_start:
            block_start_frames = timing_meta.get("block_start_frames", {})
            if isinstance(block_start_frames, dict):
                for block, frame in block_start_frames.items():
                    try:
                        frame_int = int(frame)
                    except Exception:
                        continue
                    if frame_int < 0 or frame_int > int(dff_arr.shape[1]):
                        continue
                    full_session_block_starts.append(
                        {
                            "session_label": session_label,
                            "plane_idx": int(plane_idx),
                            "block": str(block),
                            "frame": int(frame_int),
                            "row_start": int(plane_row_start),
                            "row_end": int(plane_row_end),
                        }
                    )
            for stim_row in df_stim.itertuples(index=False):
                stim_type_value = str(getattr(stim_row, "type"))
                start_s = pd.to_numeric(getattr(stim_row, "start", np.nan), errors="coerce")
                end_s = pd.to_numeric(getattr(stim_row, "end", np.nan), errors="coerce")
                duration_s = pd.to_numeric(getattr(stim_row, "duration", np.nan), errors="coerce")
                if not np.isfinite(start_s):
                    continue
                if not np.isfinite(end_s) and np.isfinite(duration_s):
                    end_s = float(start_s) + float(duration_s)
                if not np.isfinite(end_s):
                    continue
                start_frame = int(round(float(start_s) * fps))
                end_frame = int(round(float(end_s) * fps))
                start_frame = max(0, min(start_frame, int(dff_arr.shape[1])))
                end_frame = max(0, min(end_frame, int(dff_arr.shape[1])))
                if end_frame <= start_frame:
                    continue
                full_session_spans.append(
                    {
                        "stim_type": stim_type_value,
                        "session_label": session_label,
                        "plane_idx": int(plane_idx),
                        "frame_start": int(start_frame),
                        "frame_end": int(end_frame),
                        "row_start": int(plane_row_start),
                        "row_end": int(plane_row_end),
                    }
                )
        for stim_type in stim_types:
            stim_sub = df_stim[df_stim["type"].astype(str) == str(stim_type)].copy()
            if len(stim_sub) < int(cfg.min_trials_per_stimulus):
                continue
            durations = pd.to_numeric(stim_sub.get("duration", pd.Series(dtype=float)), errors="coerce").dropna().astype(float).tolist()
            duration_by_stim.setdefault(str(stim_type), []).extend(durations)
            trial_starts = pd.to_numeric(stim_sub["start"], errors="coerce").dropna().astype(float).tolist()
            if not trial_starts:
                continue
            for roi_idx in np.where(valid_roi)[0]:
                segs: list[np.ndarray] = []
                dff_segs: list[np.ndarray] = []
                trace = z[int(roi_idx)]
                dff_trace = dff_arr[int(roi_idx)]
                for start_s in trial_starts:
                    onset_idx = int(round(float(start_s) * fps))
                    seg = _extract_trace_window(
                        trace,
                        onset_idx - n_pre,
                        onset_idx + n_post,
                        mode=cfg.window_edge_policy,
                        min_valid_frac=float(cfg.min_valid_frac),
                    )
                    if seg is not None:
                        segs.append(seg)
                        dff_seg = _extract_trace_window(
                            dff_trace,
                            onset_idx - n_pre,
                            onset_idx + n_post,
                            mode=cfg.window_edge_policy,
                            min_valid_frac=float(cfg.min_valid_frac),
                        )
                        if dff_seg is not None:
                            dff_segs.append(dff_seg)
                if len(segs) < int(cfg.min_trials_per_stimulus):
                    continue
                stack = np.vstack(segs).astype(np.float32, copy=False)
                mean_trace = np.nanmean(stack, axis=0).astype(np.float32, copy=False)
                mean_dff_trace = np.nanmean(np.vstack(dff_segs), axis=0) if dff_segs else np.full_like(mean_trace, np.nan)
                post_mask = tvec >= 0
                trace_rows.append(
                    {
                        "fish_id": str(fish_id),
                        "plane_idx": int(plane_idx),
                        "func_label": int(roi_idx) + 1,
                        "roi_idx": int(roi_idx),
                        "session_label": session_label,
                        "stim_type": str(stim_type),
                        "n_trials": int(len(trial_starts)),
                        "n_valid_trials": int(len(segs)),
                        "mean_trace": mean_trace,
                        "mean_dff_trace": mean_dff_trace.astype(np.float32, copy=False),
                        "mean_z_pre": float(np.nanmean(mean_trace[~post_mask])) if np.any(~post_mask) else np.nan,
                        "mean_z_post": float(np.nanmean(mean_trace[post_mask])) if np.any(post_mask) else np.nan,
                        "mean_dff_post": float(np.nanmean(mean_dff_trace[post_mask])) if np.any(post_mask) else np.nan,
                        "peak_z_post": float(np.nanmax(mean_trace[post_mask])) if np.any(post_mask) else np.nan,
                    }
                )

    trace_df = pd.DataFrame(trace_rows)
    source_df = pd.DataFrame(source_rows)
    if tvec_ref is None or trace_df.empty:
        raise RuntimeError("[23c] no stimulus-locked Suite2p traces could be computed")
    full_session_rows_df = pd.DataFrame(full_session_rows)
    full_session_spans_df = pd.DataFrame(full_session_spans)
    full_session_block_starts_df = pd.DataFrame(full_session_block_starts)
    if full_session_traces:
        max_frames = max(int(trace.shape[0]) for trace in full_session_traces)
        full_session_matrix = np.full((len(full_session_traces), max_frames), np.nan, dtype=np.float32)
        for row_idx, trace in enumerate(full_session_traces):
            full_session_matrix[row_idx, : int(trace.shape[0])] = trace
    else:
        full_session_matrix = np.empty((0, 0), dtype=np.float32)
    session_colors = _session_palette(session_labels_seen)
    duration_summary = {
        key: float(np.nanmedian(np.asarray(vals, dtype=float)))
        for key, vals in duration_by_stim.items()
        if len(vals) and np.isfinite(np.nanmedian(np.asarray(vals, dtype=float)))
    }
    return {
        "trace_df": trace_df,
        "source_df": source_df,
        "tvec": tvec_ref,
        "stim_order": all_stim_types,
        "session_colors": session_colors,
        "duration_by_stim": duration_summary,
        "full_session_heatmap_matrix": full_session_matrix,
        "full_session_heatmap_rows": full_session_rows_df,
        "full_session_stimulus_spans": full_session_spans_df,
        "full_session_block_starts": full_session_block_starts_df,
    }


def run_suite2p_stimulus_locked_diagnostic_stage(
    *,
    fish_dir: str | Path,
    fish_id: str,
    suite2p_by_ref_idx: dict[int, dict[str, Any]],
    out_qa: str | Path | None = None,
    experiment_log_csv: str | Path | None = None,
    experiment_meta_csv: str | Path | None = None,
    frame_rate: float | None = None,
    config: Suite2pStimulusLockedDiagnosticConfig | None = None,
    activity_config: Any | None = None,
) -> dict[str, Any]:
    cfg = config or Suite2pStimulusLockedDiagnosticConfig()
    result = build_suite2p_stimulus_locked_diagnostic(
        fish_dir=fish_dir,
        fish_id=fish_id,
        suite2p_by_ref_idx=suite2p_by_ref_idx,
        experiment_log_csv=experiment_log_csv,
        experiment_meta_csv=experiment_meta_csv,
        frame_rate=frame_rate,
        config=cfg,
    )
    from .plots.analysis import render_suite2p_full_session_heatmap, render_suite2p_stimulus_locked_trace_panels

    response_result: dict[str, Any] | None = None
    trace_df_for_lines = result["trace_df"]
    try:
        from .activity import ActivityConfig, build_response_bpi_tables, build_suite2p_response_seed_table

        seed_df, dff_map = build_suite2p_response_seed_table(suite2p_by_ref_idx, fish_id=str(fish_id))
        if not seed_df.empty:
            response_result = build_response_bpi_tables(
                seed_df,
                fish_dir=fish_dir,
                fish_id=str(fish_id),
                suite2p_dff_map=dff_map,
                config=activity_config or ActivityConfig(),
                experiment_log_csv=experiment_log_csv,
                experiment_meta_csv=experiment_meta_csv,
                frame_rate=frame_rate,
            )
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
            lookup = response_result["scored_bpi_df"][[c for c in response_cols if c in response_result["scored_bpi_df"].columns]].copy()
            if not lookup.empty:
                lookup = lookup.drop_duplicates(subset=["plane_idx", "func_label"], keep="last")
                result["trace_df"] = result["trace_df"].merge(lookup, on=["plane_idx", "func_label"], how="left")
                result["trace_df"]["response_is_active"] = result["trace_df"]["response_is_active"].fillna(False).astype(bool)
                trace_df_for_lines = result["trace_df"]
                if bool(cfg.filter_trace_panels_response_active):
                    active_df = trace_df_for_lines[trace_df_for_lines["response_is_active"].astype(bool)].copy()
                    if not active_df.empty:
                        trace_df_for_lines = active_df
                    elif cfg.verbose:
                        print("[23c] no response-active Suite2p neurons available for trace panels; showing all traces.")
            result["response_scored_df"] = response_result["scored_bpi_df"]
            result["response_summary_df"] = response_result["summary_df"]
    except Exception as exc:
        result["response_error"] = str(exc)
        if cfg.verbose:
            print(f"[23c] response scoring unavailable for trace-panel filtering: {exc}")

    fig_traces = render_suite2p_stimulus_locked_trace_panels(
        trace_df=trace_df_for_lines,
        tvec=result["tvec"],
        stim_order=result["stim_order"],
        session_colors=result["session_colors"],
        duration_by_stim=result["duration_by_stim"],
        line_alpha=float(cfg.line_alpha),
        line_width=float(cfg.line_width),
    )
    fig_heatmaps = render_suite2p_full_session_heatmap(
        matrix=result["full_session_heatmap_matrix"],
        row_df=result["full_session_heatmap_rows"],
        stimulus_spans=result["full_session_stimulus_spans"],
        block_starts=result["full_session_block_starts"],
        session_colors=result["session_colors"],
        vmin=float(cfg.heatmap_vmin),
        vmax=float(cfg.heatmap_vmax),
        stim_alpha=float(cfg.stim_bar_alpha),
    )
    result["trace_panel_df"] = trace_df_for_lines
    result["fig_traces"] = fig_traces
    result["fig_heatmaps"] = fig_heatmaps

    out_paths: dict[str, str] = {}
    if bool(cfg.save_figures):
        out_dir = Path(out_qa) if out_qa is not None else Path(fish_dir) / "03_analysis" / "functional" / "qa"
        out_dir.mkdir(parents=True, exist_ok=True)
        trace_png = out_dir / "suite2p_stimulus_locked_traces_23b.png"
        heat_png = out_dir / "suite2p_stimulus_locked_heatmaps_23b.png"
        fig_traces.savefig(trace_png, dpi=int(cfg.figure_dpi), bbox_inches="tight")
        fig_traces.savefig(trace_png.with_suffix(".pdf"), bbox_inches="tight")
        fig_heatmaps.savefig(heat_png, dpi=int(cfg.figure_dpi), bbox_inches="tight")
        fig_heatmaps.savefig(heat_png.with_suffix(".pdf"), bbox_inches="tight")
        summary_csv = out_dir / "suite2p_stimulus_locked_summary_23b.csv"
        source_csv = out_dir / "suite2p_stimulus_locked_sources_23b.csv"
        heatmap_matrix_npy = out_dir / "suite2p_full_session_heatmap_matrix_23c.npy"
        heatmap_rows_csv = out_dir / "suite2p_full_session_heatmap_rows_23c.csv"
        summary_out = result["trace_df"].drop(columns=["mean_trace", "mean_dff_trace"]).copy()
        summary_out.to_csv(summary_csv, index=False)
        result["source_df"].to_csv(source_csv, index=False)
        np.save(heatmap_matrix_npy, result["full_session_heatmap_matrix"])
        result["full_session_heatmap_rows"].to_csv(heatmap_rows_csv, index=False)
        response_csv = out_dir / "suite2p_response_bpi_cells_23c.csv"
        response_summary_csv = out_dir / "suite2p_response_bpi_summary_23c.csv"
        if response_result is not None:
            response_result["scored_bpi_df"].to_csv(response_csv, index=False)
            response_result["summary_df"].to_csv(response_summary_csv, index=False)
        out_paths = {
            "trace_png": str(trace_png),
            "trace_pdf": str(trace_png.with_suffix(".pdf")),
            "heatmap_png": str(heat_png),
            "heatmap_pdf": str(heat_png.with_suffix(".pdf")),
            "summary_csv": str(summary_csv),
            "source_csv": str(source_csv),
            "heatmap_matrix_npy": str(heatmap_matrix_npy),
            "heatmap_rows_csv": str(heatmap_rows_csv),
        }
        if response_result is not None:
            out_paths["response_csv"] = str(response_csv)
            out_paths["response_summary_csv"] = str(response_summary_csv)
        if cfg.verbose:
            print(f"[23c] saved stimulus-locked Suite2p diagnostics to {out_dir}")
    result["out_paths"] = out_paths
    return result


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
    input_xy_frame: str | None = None,
    config: Suite2pStageConfig | None = None,
    assert_fish_compatible: Any = None,
) -> dict[str, Any]:
    from .context import func_orientation_effective, func_orientation_mode
    from .spatial import apply_func_orientation

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

    from .spatial_contract import (
        CANONICAL_XY_FRAME,
        LEGACY_ACQUISITION_XY_FRAME,
        fish_dir_from_product_path,
        load_spatial_manifest,
    )

    fish_dir = fish_dir_from_product_path(resolved_root)
    manifest = load_spatial_manifest(fish_dir, required=False) if fish_dir is not None else None
    declared_frame = CANONICAL_XY_FRAME if manifest is not None else None
    if input_xy_frame is not None and declared_frame is not None and input_xy_frame != declared_frame:
        raise ValueError(f"Suite2P frame {input_xy_frame!r} conflicts with canonical manifest {declared_frame!r}")
    frame = cfg.stat_xy_frame or input_xy_frame or declared_frame or LEGACY_ACQUISITION_XY_FRAME
    if frame not in {CANONICAL_XY_FRAME, LEGACY_ACQUISITION_XY_FRAME}:
        raise ValueError(f"Unknown Suite2P input XY frame: {frame!r}")
    canonical_plane_paths = {
        Path(str(record["output_path"])).resolve()
        for record in (manifest or {}).get("functional_planes", [])
        if isinstance(record, dict) and record.get("output_path")
    }
    orient_mode = "none" if frame == CANONICAL_XY_FRAME else func_orientation_mode(polarity)
    orient_effective = "none" if frame == CANONICAL_XY_FRAME else func_orientation_effective(polarity)
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

    inferred_ref_indices: list[int] = []
    if not plane_refs:
        for pd_idx, plane_dir in enumerate(plane_dirs):
            plane_num = _plane_num_from_name(plane_dir.name)
            inferred_ref_indices.append(int(plane_num) if plane_num is not None else int(pd_idx))
        if cfg.verbose:
            print("[Suite2p] plane_refs missing; loading Suite2p traces by discovered plane index.")

    suite2p_planes: list[dict[str, Any]] = []
    suite2p_by_ref_idx: dict[int, dict[str, Any]] = {}
    n_func_label_slots = len(plane_refs)
    if inferred_ref_indices:
        n_func_label_slots = max(inferred_ref_indices) + 1
    func_labels: list[np.ndarray | None] = [None] * n_func_label_slots
    suite2p_sources: list[dict[str, Any]] = []

    if not plane_dirs and cfg.verbose:
        print(f"[Suite2p] No plane dirs found under {resolved_root} (glob={cfg.plane_glob})")

    for pd_idx, plane_dir in enumerate(plane_dirs):
        plane_num = _plane_num_from_name(plane_dir.name)
        ref_idx = plane_ref_map.get(plane_num)
        if ref_idx is None and pd_idx < len(plane_refs):
            ref_idx = pd_idx
        if ref_idx is None and not plane_refs:
            ref_idx = int(plane_num) if plane_num is not None else int(pd_idx)

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
        if manifest is not None:
            ops_inputs: set[Path] = set()
            for key in ("tiff_list", "filelist"):
                values = ops.get(key, []) if isinstance(ops, dict) else []
                if isinstance(values, (str, Path)):
                    values = [values]
                for value in values or []:
                    candidate = Path(str(value))
                    if not candidate.is_absolute():
                        data_paths = ops.get("data_path", []) if isinstance(ops, dict) else []
                        if isinstance(data_paths, (str, Path)):
                            data_paths = [data_paths]
                        if data_paths:
                            candidate = Path(str(data_paths[0])) / candidate
                    ops_inputs.add(candidate.resolve())
            if not ops_inputs or not ops_inputs.issubset(canonical_plane_paths):
                raise ValueError(
                    "Suite2P outputs are stale or lack canonical input provenance: "
                    f"ops inputs={sorted(str(path) for path in ops_inputs)}, "
                    f"manifest planes={sorted(str(path) for path in canonical_plane_paths)}"
                )
        iscell = np.load(paths["iscell"], allow_pickle=True)

        labels, keep = _build_labels_from_stat(stat, iscell, ops)
        if frame == LEGACY_ACQUISITION_XY_FRAME:
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
            "input_xy_frame": frame,
        }
        suite2p_planes.append(plane_info)

        if ref_idx is not None:
            func_labels[int(ref_idx)] = labels
            suite2p_by_ref_idx[int(ref_idx)] = plane_info
            if int(ref_idx) < len(plane_refs):
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
                "input_xy_frame": frame,
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
        "input_xy_frame": frame,
        "stat_xy_frame": frame,
        "output_xy_frame": CANONICAL_XY_FRAME,
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
    "Suite2pStimulusLockedDiagnosticConfig",
    "build_suite2p_all_roi_labels",
    "build_suite2p_stimulus_locked_diagnostic",
    "infer_frame_rate_from_detail",
    "load_suite2p_stage",
    "load_suite2p_dff_map",
    "run_suite2p_stimulus_locked_diagnostic_stage",
]
