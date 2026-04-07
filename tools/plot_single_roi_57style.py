#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.gridspec import GridSpec


STIM_PALETTE = {
    "LLC": "#0a5910",
    "LLB": "#34B18C",
    "RLC": "#4D0C2C",
    "RLB": "#cf368f",
    "LLC+RLC": "#281578",
    "LLC+RLB": "#26200b",
    "LLB+RLC": "#989999",
    "LLB+RLB": "#94cae3",
}

PANEL_ORDER = [
    ("ipsi", "bout"),
    ("ipsi", "continuous"),
    ("contra", "bout"),
    ("contra", "continuous"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot a [57]-style full-session trace for one ROI and summarize the [56i] motion-window AUC call."
    )
    parser.add_argument("--fish-root", type=Path, required=True)
    parser.add_argument("--plane-idx", type=int, required=True)
    parser.add_argument("--func-label", type=int, required=True)
    parser.add_argument("--gene", type=str, default=None, help="Optional label for the identified-gene point.")
    parser.add_argument("--onset-delay-sec", type=float, default=10.0)
    parser.add_argument("--remove-interblock-gaps", action="store_true", default=True)
    parser.add_argument("--keep-interblock-gaps", dest="remove_interblock_gaps", action="store_false")
    parser.add_argument("--f-path", type=Path, default=None, help="Optional explicit Suite2p F.npy path to use instead of auto-resolving from plane_idx.")
    parser.add_argument("--source-label", type=str, default=None, help="Optional label describing the trace source in the figure title/text.")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--trial-csv", type=Path, default=None)
    return parser.parse_args()


def parse_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        s = re.sub(r"[^0-9eE+\-.]", "", str(value))
        return float(s) if s else None


def block_key(block: str) -> tuple[int, str]:
    match = re.match(r"^B(\d+)$", str(block).strip())
    if match:
        return int(match.group(1)), str(block)
    return 10**9, str(block)


def find_one(directory: Path, pattern: str) -> Path:
    matches = sorted(directory.glob(pattern))
    if not matches:
        raise FileNotFoundError(f"No files matching {pattern!r} under {directory}")
    return matches[0]


def load_metadata_params(metadata_csv: Path) -> dict[str, object]:
    df = pd.read_csv(metadata_csv)
    if not {"parameter", "value"}.issubset(df.columns):
        return {}
    return {str(k).strip().lower(): v for k, v in df[["parameter", "value"]].itertuples(index=False)}


def load_events_df(log_csv: Path) -> pd.DataFrame:
    df = pd.read_csv(log_csv)
    if "timestamp" in df.columns and "time" not in df.columns:
        df = df.rename(columns={"timestamp": "time"})
    if not {"event", "time"}.issubset(df.columns):
        raise RuntimeError(f"Unexpected event log columns in {log_csv}")
    df = df[["event", "time"]].copy()
    df["event"] = df["event"].astype(str)
    df["time"] = pd.to_numeric(df["time"], errors="coerce")
    return df.dropna(subset=["event", "time"]).sort_values("time").reset_index(drop=True)


def effective_motion_window(start_s: object, duration_s: object, end_s: object, onset_delay_sec: float) -> tuple[float, float, float]:
    start = parse_float(start_s)
    dur = parse_float(duration_s)
    end = parse_float(end_s)
    if start is None or not np.isfinite(start):
        return np.nan, np.nan, np.nan
    t0 = float(start) + float(onset_delay_sec)
    if end is not None and np.isfinite(end):
        t1 = float(end)
    elif dur is not None and np.isfinite(dur):
        t1 = float(start) + float(dur)
    else:
        return t0, np.nan, np.nan
    eff_dur = t1 - t0
    if not np.isfinite(eff_dur) or eff_dur <= 0:
        return t0, t1, np.nan
    return t0, t1, eff_dur


def parse_unilateral_stim(stype: str) -> tuple[str | None, str | None]:
    parts = [p.strip().upper() for p in str(stype).split("+") if str(p).strip()]
    if len(parts) != 1:
        return None, None
    match = re.match(r"^([LR])(LB|LC)$", parts[0])
    if not match:
        return None, None
    stim_side = "left" if match.group(1) == "L" else "right"
    stim_mode = "bout" if match.group(2) == "LB" else "continuous"
    return stim_side, stim_mode


def build_stim_tables(
    log_csv: Path,
    fps: float,
    onset_delay_sec: float,
    remove_interblock_gaps: bool,
    measure_start_block: int = 1,
    measure_start_event: str = "start",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_evt = load_events_df(log_csv)

    block_label = f"B{int(measure_start_block)}"
    start_event = f"{block_label}_{measure_start_event}"
    start_match = df_evt.loc[df_evt["event"] == start_event, "time"]
    if len(start_match):
        t0 = float(start_match.iloc[0])
    else:
        block_rows = df_evt[df_evt["event"].str.startswith(f"{block_label}_")]
        t0 = float(block_rows["time"].min()) if not block_rows.empty else None
    if t0 is not None:
        df_evt["time"] = df_evt["time"] - t0
        df_evt = df_evt[df_evt["time"] >= 0].reset_index(drop=True)

    block_codes = []
    for event in df_evt["event"]:
        match = re.match(r"^(B\d+)_", str(event))
        if match:
            block_codes.append(match.group(1))
    block_codes = sorted(set(block_codes), key=block_key)

    blocks: list[dict[str, float | str]] = []
    for block in block_codes:
        block_events = df_evt[df_evt["event"].str.startswith(f"{block}_")]
        if block_events.empty:
            continue
        start_time = block_events.loc[block_events["event"] == f"{block}_start", "time"]
        end_time = block_events.loc[block_events["event"] == f"{block}_end", "time"]
        interblock = block_events.loc[block_events["event"] == f"{block}_interblock_pause", "time"]
        bstart = float(start_time.iloc[0]) if len(start_time) else float(block_events["time"].min())
        bend = float(end_time.iloc[0]) if len(end_time) else None
        ib_time = float(interblock.iloc[0]) if len(interblock) else None
        if bend is None:
            bend = ib_time
        elif ib_time is not None:
            bend = ib_time
        if bend is None:
            bend = float(block_events["time"].max())
        blocks.append({"block": block, "start": bstart, "end": bend})

    if remove_interblock_gaps and blocks:
        blocks = sorted(blocks, key=lambda item: float(item["start"]))
        shift_map: dict[str, float] = {}
        shift = 0.0
        prev_end = None
        for block in blocks:
            orig_start = float(block["start"])
            orig_end = float(block["end"])
            if prev_end is not None:
                shift += max(0.0, orig_start - prev_end)
            shift_map[str(block["block"])] = shift
            block["start"] = orig_start - shift
            block["end"] = orig_end - shift
            prev_end = orig_end

        def shift_event_time(row: pd.Series) -> float:
            match = re.match(r"^(B\d+)_", str(row["event"]))
            if not match:
                return float(row["time"])
            return float(row["time"]) - shift_map.get(match.group(1), 0.0)

        df_evt["time"] = df_evt.apply(shift_event_time, axis=1)
        df_evt = df_evt.sort_values("time").reset_index(drop=True)

    stims: list[dict[str, object]] = []
    for row in df_evt.itertuples(index=False):
        event = str(row.event)
        match = re.match(r"^(B\d+)_stim(\d+)_(.+)$", event)
        if not match:
            continue
        block = match.group(1)
        stim_idx = int(match.group(2))
        stim_type = match.group(3)
        start = float(row.time)
        post_name = f"{block}_poststim{stim_idx}_pause"
        post_match = df_evt.loc[df_evt["event"] == post_name, "time"]
        if len(post_match):
            end = float(post_match.iloc[0])
        else:
            after = df_evt[(df_evt["time"] > start) & (df_evt["event"].str.startswith(f"{block}_"))].sort_values("time")
            end = float(after["time"].iloc[0]) if not after.empty else np.nan
        stims.append(
            {
                "block": block,
                "stim_idx": stim_idx,
                "type": stim_type,
                "start": start,
                "end": end,
                "duration": end - start if np.isfinite(end) else np.nan,
            }
        )

    df_stim = pd.DataFrame(stims)
    if df_stim.empty:
        raise RuntimeError("No stimuli parsed from experiment log")

    stim_side_mode = df_stim["type"].apply(lambda s: pd.Series(parse_unilateral_stim(s)))
    stim_side_mode.columns = ["stim_side", "stim_mode"]
    df_stim = pd.concat([df_stim, stim_side_mode], axis=1)

    eff = df_stim.apply(
        lambda row: pd.Series(
            effective_motion_window(
                row.get("start", np.nan),
                row.get("duration", np.nan),
                row.get("end", np.nan),
                onset_delay_sec,
            ),
            index=["motion_start", "motion_end", "motion_duration"],
        ),
        axis=1,
    )
    df_stim = pd.concat([df_stim, eff], axis=1)
    df_stim["trial_id"] = df_stim.apply(lambda row: f"{row['block']}_stim{int(row['stim_idx'])}_{row['type']}", axis=1)
    return df_evt, df_stim


def compute_laterality(roi_side: str | None, stim_side: str | None) -> str | None:
    if roi_side not in {"left", "right"} or stim_side not in {"left", "right"}:
        return None
    return "ipsi" if roi_side == stim_side else "contra"


def compute_trial_auc(
    trace: np.ndarray,
    fps: float,
    df_stim: pd.DataFrame,
    roi_side: str | None,
    min_valid_frac: float = 0.5,
) -> pd.DataFrame:
    out_rows: list[dict[str, object]] = []
    T = int(trace.shape[0])
    unilateral = df_stim[df_stim["stim_side"].notna() & df_stim["stim_mode"].notna()].copy()
    unilateral["idx0"] = np.rint(unilateral["motion_start"].astype(float) * float(fps)).astype(int)
    unilateral["idx1"] = np.rint(unilateral["motion_end"].astype(float) * float(fps)).astype(int)
    unilateral = unilateral[unilateral["idx1"] > unilateral["idx0"]].reset_index(drop=True)
    for stim in unilateral.itertuples(index=False):
        idx0 = int(stim.idx0)
        idx1 = int(stim.idx1)
        win_len = idx1 - idx0
        if win_len <= 0:
            continue
        src0 = max(idx0, 0)
        src1 = min(idx1, T)
        if src1 <= src0:
            continue
        seg = trace[src0:src1]
        n_valid = int(np.isfinite(seg).sum())
        min_pts = max(1, int(np.ceil(float(min_valid_frac) * float(win_len))))
        auc = np.nan
        if n_valid >= min_pts:
            auc = float(np.nansum(seg) / float(fps))
        laterality = compute_laterality(roi_side, stim.stim_side)
        out_rows.append(
            {
                "trial_id": stim.trial_id,
                "type": stim.type,
                "stim_side": stim.stim_side,
                "stim_mode": stim.stim_mode,
                "laterality": laterality,
                "motion_start": float(stim.motion_start),
                "motion_end": float(stim.motion_end),
                "motion_duration": float(stim.motion_duration),
                "auc_dff": auc,
                "n_valid_frames": n_valid,
                "window_frames": int(win_len),
            }
        )
    return pd.DataFrame(out_rows)


def find_suite2p_f(plane_dir: Path, plane_idx: int) -> Path:
    tagged = sorted(plane_dir.glob(f"*plane{int(plane_idx)}_F.npy"))
    if tagged:
        return tagged[0]
    candidates = sorted(plane_dir.glob("*_F.npy"))
    if candidates:
        return candidates[0]
    plain = plane_dir / "F.npy"
    if plain.exists():
        return plain
    raise FileNotFoundError(f"Could not find an F.npy file under {plane_dir}")


def main() -> None:
    args = parse_args()
    fish_root = args.fish_root.resolve()
    registration_dir = fish_root / "03_analysis" / "functional" / "registration"
    metadata_dir = fish_root / "01_raw" / "2p" / "metadata"

    detail_csv = registration_dir / "functional_roi_activity_identity.csv"
    panel_csv = registration_dir / "motion_auc_roi_panels.csv"
    metadata_csv = find_one(metadata_dir, "*_metadata.csv")
    experiment_log_csv = find_one(metadata_dir, "*_experiment_log.csv")

    detail_df = pd.read_csv(detail_csv)
    roi_df = detail_df[
        (pd.to_numeric(detail_df["plane_idx"], errors="coerce") == int(args.plane_idx))
        & (pd.to_numeric(detail_df["func_label"], errors="coerce") == int(args.func_label))
    ].copy()
    if roi_df.empty:
        raise RuntimeError(f"ROI plane_idx={args.plane_idx}, func_label={args.func_label} not found in {detail_csv}")
    roi = roi_df.iloc[0]

    params = load_metadata_params(metadata_csv)
    fps = parse_float(params.get("framerate", params.get("frame_rate", params.get("fps"))))
    if fps is None or not np.isfinite(fps) or float(fps) <= 0:
        raise RuntimeError(f"Could not determine frame rate from {metadata_csv}")
    fps = float(fps)

    plane_dir = Path(str(roi["func_source"]))
    if args.f_path is not None:
        F_path = args.f_path.resolve()
    else:
        F_path = find_suite2p_f(plane_dir, int(args.plane_idx))
    F = np.load(F_path, allow_pickle=True).astype(np.float32)
    roi_idx = int(args.func_label) - 1
    if roi_idx < 0 or roi_idx >= F.shape[0]:
        raise RuntimeError(f"ROI index {roi_idx} out of range for {F_path} with shape {F.shape}")
    F0 = np.percentile(F, 10.0, axis=1, keepdims=True)
    dff = (F - F0) / (F0 + 1e-6)
    trace = np.asarray(dff[roi_idx], dtype=np.float32)
    time_s = np.arange(trace.shape[0], dtype=np.float32) / float(fps)

    df_evt, df_stim = build_stim_tables(
        experiment_log_csv,
        fps=fps,
        onset_delay_sec=float(args.onset_delay_sec),
        remove_interblock_gaps=bool(args.remove_interblock_gaps),
    )

    roi_side = None
    panel_roi_df = pd.DataFrame()
    if panel_csv.exists():
        panel_df = pd.read_csv(panel_csv)
        panel_roi_df = panel_df[
            (pd.to_numeric(panel_df["plane_idx"], errors="coerce") == int(args.plane_idx))
            & (pd.to_numeric(panel_df["func_label"], errors="coerce") == int(args.func_label))
        ].copy()
        if not panel_roi_df.empty and "roi_side" in panel_roi_df.columns:
            side = str(panel_roi_df["roi_side"].iloc[0]).strip().lower()
            roi_side = side if side in {"left", "right"} else None

    trial_auc_df = compute_trial_auc(trace, fps, df_stim, roi_side=roi_side, min_valid_frac=0.5)
    if args.trial_csv is not None:
        args.trial_csv.parent.mkdir(parents=True, exist_ok=True)
        trial_auc_df.to_csv(args.trial_csv, index=False)

    args.output.parent.mkdir(parents=True, exist_ok=True)

    fig = plt.figure(figsize=(14, 8.5), dpi=200)
    gs = GridSpec(2, 2, height_ratios=[3.0, 1.7], width_ratios=[2.2, 1.0], hspace=0.28, wspace=0.22, figure=fig)
    ax_trace = fig.add_subplot(gs[0, :])
    ax_auc = fig.add_subplot(gs[1, 0])
    ax_text = fig.add_subplot(gs[1, 1])

    span_labels: set[str] = set()
    for row in df_stim.itertuples(index=False):
        start = float(row.start)
        end = float(row.end)
        if not (np.isfinite(start) and np.isfinite(end) and end > start):
            continue
        color = STIM_PALETTE.get(str(row.type), "#d9d9d9")
        label = str(row.type) if str(row.type) not in span_labels else None
        ax_trace.axvspan(start, end, color=color, alpha=0.12, label=label)
        span_labels.add(str(row.type))

    unilateral_windows = df_stim[df_stim["stim_side"].notna() & df_stim["stim_mode"].notna()].copy()
    for row in unilateral_windows.itertuples(index=False):
        start = float(row.motion_start)
        end = float(row.motion_end)
        if not (np.isfinite(start) and np.isfinite(end) and end > start):
            continue
        color = STIM_PALETTE.get(str(row.type), "#777777")
        ax_trace.axvspan(start, end, color=color, alpha=0.22)
        ax_trace.axvline(start, color=color, alpha=0.35, linewidth=0.8)

    ax_trace.plot(time_s, trace, color="#222222", linewidth=1.05, alpha=0.95)
    ax_trace.axhline(0.0, color="#bdbdbd", linewidth=0.8, zorder=0)
    ax_trace.set_title("Single-ROI full-session dF/F with stimulus spans")
    ax_trace.set_xlabel("Time (s)")
    ax_trace.set_ylabel("dF/F")

    bout_threshold = max(float(roi.get("response_auc_threshold", 0.05)), float(roi.get("bout_null_q99_auc", np.nan))) if np.isfinite(float(roi.get("bout_null_q99_auc", np.nan))) else float(roi.get("response_auc_threshold", 0.05))
    cont_threshold = max(float(roi.get("response_auc_threshold", 0.05)), float(roi.get("cont_null_q99_auc", np.nan))) if np.isfinite(float(roi.get("cont_null_q99_auc", np.nan))) else float(roi.get("response_auc_threshold", 0.05))

    trial_auc_df = trial_auc_df.dropna(subset=["laterality"]).copy()
    labels = []
    mean_map: dict[tuple[str, str], float] = {}
    rng = np.random.default_rng(16)
    for idx, (laterality, stim_mode) in enumerate(PANEL_ORDER):
        sub = trial_auc_df[(trial_auc_df["laterality"] == laterality) & (trial_auc_df["stim_mode"] == stim_mode)].copy()
        vals = sub["auc_dff"].to_numpy(dtype=float)
        labels.append(f"{laterality}\n{stim_mode}")
        if vals.size:
            jitter = rng.uniform(-0.10, 0.10, size=vals.size)
            color = "#1f77b4" if stim_mode == "bout" else "#ff7f0e"
            ax_auc.scatter(np.full(vals.size, idx) + jitter, vals, s=28, color=color, alpha=0.85, zorder=3)
            mean_val = float(np.nanmean(vals))
            mean_map[(laterality, stim_mode)] = mean_val
            ax_auc.hlines(mean_val, idx - 0.22, idx + 0.22, color="black", linewidth=1.8, zorder=4)
            ax_auc.text(idx, mean_val, f"{mean_val:.3f}", fontsize=8, ha="center", va="bottom")
        else:
            mean_map[(laterality, stim_mode)] = np.nan

    ax_auc.axhline(float(roi.get("response_auc_threshold", 0.05)), color="#999999", linestyle="--", linewidth=1.0, label="min AUC threshold")
    ax_auc.axhline(bout_threshold, color="#1f77b4", linestyle=":", linewidth=1.2, label="bout effective threshold")
    ax_auc.axhline(cont_threshold, color="#ff7f0e", linestyle=":", linewidth=1.2, label="continuous effective threshold")
    ax_auc.set_xticks(range(len(PANEL_ORDER)))
    ax_auc.set_xticklabels(labels)
    ax_auc.set_ylabel("Motion-window AUC (dF/F·s)")
    ax_auc.set_title("[56i] unilateral trial AUCs for this ROI")
    ax_auc.grid(axis="y", alpha=0.25)
    ax_auc.legend(loc="upper right", fontsize=8)

    ax_text.axis("off")
    gene_label = args.gene or str(roi.get("identity_display_label", "unknown"))
    summary_lines = [
        f"fish: {fish_root.name}",
        f"gene point: {gene_label}",
        f"ROI: plane {args.plane_idx}, func {args.func_label}, roi_idx {roi_idx}",
        f"plane source: {plane_dir}",
        f"trace source: {args.source_label or F_path.name}",
        f"fps: {fps:.3f}",
        f"response_summary_class: {roi.get('response_summary_class', 'NA')}",
        f"response_class: {roi.get('response_class', 'NA')}",
        f"response_is_active: {bool(roi.get('response_is_active', False))}",
        "",
        f"mean_bout_auc_dff: {float(roi.get('mean_bout_auc_dff', np.nan)):.6f}",
        f"mean_cont_auc_dff: {float(roi.get('mean_cont_auc_dff', np.nan)):.6f}",
        f"response_auc_threshold: {float(roi.get('response_auc_threshold', 0.05)):.6f}",
        f"bout_null_q99_auc: {float(roi.get('bout_null_q99_auc', np.nan)):.6f}",
        f"cont_null_q99_auc: {float(roi.get('cont_null_q99_auc', np.nan)):.6f}",
        f"bout effective threshold: {bout_threshold:.6f}",
        f"cont effective threshold: {cont_threshold:.6f}",
        f"bout_response_pass: {bool(roi.get('bout_response_pass', False))}",
        f"cont_response_pass: {bool(roi.get('cont_response_pass', False))}",
        "",
        f"roi_side (from motion panel table): {roi_side or 'unknown'}",
        f"panel mean ipsi/bout: {mean_map[('ipsi', 'bout')]:.6f}",
        f"panel mean ipsi/continuous: {mean_map[('ipsi', 'continuous')]:.6f}",
        f"panel mean contra/bout: {mean_map[('contra', 'bout')]:.6f}",
        f"panel mean contra/continuous: {mean_map[('contra', 'continuous')]:.6f}",
    ]
    ax_text.text(
        0.0,
        1.0,
        "\n".join(summary_lines),
        va="top",
        ha="left",
        family="monospace",
        fontsize=8.6,
    )

    fig.suptitle(
        f"{fish_root.name} | single-ROI [57]-style trace for p{args.plane_idx}:f{args.func_label}"
        + (f" | {args.source_label}" if args.source_label else ""),
        fontsize=13,
        y=0.98,
    )
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    fig.savefig(args.output, dpi=200, bbox_inches="tight")

    print(f"figure -> {args.output}")
    if args.trial_csv is not None:
        print(f"trial_csv -> {args.trial_csv}")
    print(f"F_path -> {F_path}")
    print(f"metadata_csv -> {metadata_csv}")
    print(f"experiment_log_csv -> {experiment_log_csv}")
    print(
        "summary: "
        f"response_class={roi.get('response_class', 'NA')}, "
        f"mean_bout_auc={float(roi.get('mean_bout_auc_dff', np.nan)):.6f}, "
        f"mean_cont_auc={float(roi.get('mean_cont_auc_dff', np.nan)):.6f}, "
        f"bout_threshold={bout_threshold:.6f}, "
        f"cont_threshold={cont_threshold:.6f}"
    )


if __name__ == "__main__":
    main()
