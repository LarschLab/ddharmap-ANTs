"""Analysis figure builders used by trace-focused tools and notebook exports."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from ..stimulus import StimulusConfig, build_stim_tables, load_events_df, load_metadata_params, parse_float, parse_unilateral_stim


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


def _find_one(directory: Path, pattern: str) -> Path:
    matches = sorted(directory.glob(pattern))
    if not matches:
        raise FileNotFoundError(f"No files matching {pattern!r} under {directory}")
    return matches[0]


def _find_suite2p_f(plane_dir: Path) -> Path:
    for candidate in (plane_dir / "F.npy", plane_dir / "suite2p" / "plane0" / "F.npy"):
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"Could not resolve Suite2p F.npy under {plane_dir}")


def compute_laterality(roi_side: str | None, stim_side: str | None) -> str | None:
    if roi_side not in {"left", "right"} or stim_side not in {"left", "right"}:
        return None
    return "ipsi" if roi_side == stim_side else "contra"


def compute_trial_auc(trace: np.ndarray, fps: float, df_stim: pd.DataFrame, roi_side: str | None) -> pd.DataFrame:
    auc_rows = []
    for row in df_stim.itertuples(index=False):
        stim_side, stim_mode = parse_unilateral_stim(row.type)
        laterality = compute_laterality(roi_side, stim_side)
        if not np.isfinite(row.motion_start) or not np.isfinite(row.motion_end):
            continue
        idx0 = max(0, int(np.floor(float(row.motion_start) * fps)))
        idx1 = min(int(trace.shape[0]), int(np.ceil(float(row.motion_end) * fps)))
        if idx1 <= idx0:
            continue
        window = trace[idx0:idx1]
        auc_rows.append(
            {
                "block": row.block,
                "type": row.type,
                "stim_mode": stim_mode,
                "stim_side": stim_side,
                "laterality": laterality,
                "motion_start": row.motion_start,
                "motion_end": row.motion_end,
                "trial_auc": float(np.trapz(window, dx=1.0 / fps)),
            }
        )
    return pd.DataFrame(auc_rows)


def plot_single_roi_57style(
    *,
    fish_root: Path,
    plane_idx: int,
    func_label: int,
    output: Path,
    gene: str | None = None,
    onset_delay_sec: float = 10.0,
    remove_interblock_gaps: bool = True,
    f_path: Path | None = None,
    source_label: str | None = None,
    trial_csv: Path | None = None,
) -> Path:
    metadata_dir = fish_root / "01_raw" / "2p" / "metadata"
    log_csv = _find_one(metadata_dir, "*experiment_log*.csv")
    metadata_csv = _find_one(metadata_dir, "*metadata*.csv")
    params = load_metadata_params(metadata_csv)
    fps = parse_float(params.get("framerate", params.get("frame_rate", params.get("fps"))))
    if fps is None:
        raise RuntimeError(f"Could not resolve frame rate from {metadata_csv}")

    df_evt = load_events_df(log_csv)
    _, df_stim = build_stim_tables(
        df_evt,
        fps=fps,
        onset_delay_sec=onset_delay_sec,
        remove_interblock_gaps=remove_interblock_gaps,
        measure_start_block=1,
        measure_start_event="start",
    )

    if f_path is None:
        plane_dir = fish_root / "03_analysis" / "functional" / "suite2p" / f"plane{int(plane_idx)}"
        f_path = _find_suite2p_f(plane_dir)
    traces = np.asarray(np.load(f_path), dtype=np.float32)
    trace = traces[int(func_label) - 1]
    time_s = np.arange(trace.shape[0], dtype=np.float32) / float(fps)
    auc_df = compute_trial_auc(trace, float(fps), df_stim, roi_side=None)
    if trial_csv is not None:
        auc_df.to_csv(trial_csv, index=False)

    fig, axes = plt.subplots(2, 1, figsize=(14, 6), height_ratios=[3, 1], constrained_layout=True)
    axes[0].plot(time_s, trace, color="black", linewidth=1.0)
    for row in df_stim.itertuples(index=False):
        color = STIM_PALETTE.get(row.type, "#999999")
        if np.isfinite(row.start) and np.isfinite(row.end):
            axes[0].axvspan(float(row.start), float(row.end), color=color, alpha=0.18)
    title_bits = [f"plane {plane_idx}", f"ROI {func_label}"]
    if gene:
        title_bits.append(gene)
    if source_label:
        title_bits.append(source_label)
    axes[0].set_title(" | ".join(title_bits), fontsize=11)
    axes[0].set_ylabel("dF/F")
    axes[0].set_xlabel("Time (s)")

    if not auc_df.empty:
        summary = auc_df.groupby(["laterality", "stim_mode"], dropna=False)["trial_auc"].mean().reset_index()
        x = np.arange(len(summary))
        axes[1].bar(x, summary["trial_auc"].to_numpy(dtype=float), color="#4c78a8")
        axes[1].set_xticks(x, [f"{row.laterality or 'na'}\n{row.stim_mode or 'na'}" for row in summary.itertuples(index=False)])
    axes[1].set_ylabel("AUC")
    axes[1].set_title("Motion-window trial AUC", fontsize=11)

    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=300, bbox_inches="tight")
    plt.close(fig)
    return output


__all__ = ["STIM_PALETTE", "compute_laterality", "compute_trial_auc", "plot_single_roi_57style"]
