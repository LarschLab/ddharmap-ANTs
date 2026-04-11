"""Analysis figure builders used by trace-focused tools and notebook exports."""

from __future__ import annotations

from pathlib import Path
import re
from typing import Any

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


def _fish_paths_for_cohort(data_root: str | Path, owner: str, fish_id: str, data_mode: str) -> dict[str, Path]:
    root = Path(data_root)
    fish_id_s = str(fish_id)
    owner_s = str(owner)
    mode = str(data_mode).strip().lower()
    if mode == "local":
        candidates = [root / fish_id_s, root / owner_s / fish_id_s, root / owner_s / "Microscopy" / fish_id_s]
        fish_dir = next((cand for cand in candidates if cand.exists()), candidates[0])
    else:
        base = root / owner_s
        microscopy = base / "Microscopy"
        fish_dir = (microscopy if microscopy.exists() else base) / fish_id_s
    out_reg = fish_dir / "03_analysis" / "functional" / "registration"
    return {"fish_dir": fish_dir, "out_reg": out_reg}


def render_cohort_56h_by_fish(
    *,
    cell_df: pd.DataFrame,
    results: dict[str, Any],
    results_by_fish: dict[str, Any],
    results_per_cell_by_fish: dict[str, Any],
    tvec: np.ndarray,
    mode_durations: dict[str, Any],
    cohort_fish_summary_df: pd.DataFrame | None,
    gene_order: list[str],
    gene_colors: dict[str, str],
    plot_order: list[str],
    plot_titles: dict[str, str],
    min_segments: int = 3,
    min_cells: int = 1,
    out_path: str | Path | None = None,
) -> dict[str, Any]:
    if not isinstance(cell_df, pd.DataFrame) or cell_df.empty:
        raise RuntimeError("cohort_bpi_cells_df missing; run [cohort-build] first.")
    if not isinstance(results, dict) or not isinstance(results_by_fish, dict) or not isinstance(results_per_cell_by_fish, dict):
        raise RuntimeError("cohort trace payload missing; run [cohort-build] first.")
    tvec_arr = np.asarray(tvec, dtype=float)
    if tvec_arr.size == 0:
        raise RuntimeError("cohort_tvec missing; run [cohort-build] first.")

    genes_present = set()
    for fish_panel in results_by_fish.values():
        if not isinstance(fish_panel, dict):
            continue
        for panel in plot_order:
            genes_present.update(list(((fish_panel.get(panel, {}) or {}).keys())))
    genes = [g for g in gene_order if g in genes_present] + sorted([g for g in genes_present if g not in gene_order])
    if not genes:
        raise RuntimeError("No genes available for cohort [56h] plot.")

    fish_order = sorted(set([str(f) for f in results_by_fish.keys()]) | set([str(f) for f in results_per_cell_by_fish.keys()]))
    if not fish_order:
        raise RuntimeError("No fish-level trace summaries available.")

    fig, axes = plt.subplots(max(1, len(genes)), 1, figsize=(13.0, 2.7 * max(1, len(genes)) + 1.4), sharex=True, sharey=True)
    axes_arr = np.atleast_1d(axes)
    fish_cmap = plt.get_cmap("tab10") if len(fish_order) <= 10 else plt.get_cmap("tab20")
    fish_colors = {fid: fish_cmap(i % fish_cmap.N) for i, fid in enumerate(fish_order)}
    block_gap = 3.0
    x_min = -10.0
    x_max = 30.0
    mask = (tvec_arr >= x_min) & (tvec_arr <= x_max)
    if int(np.count_nonzero(mask)) < 2:
        raise RuntimeError("Invalid cohort [56h] trace range.")
    plot_tvec = tvec_arr[mask]
    block_span = float(plot_tvec[-1] - plot_tvec[0])
    offsets = {panel: (idx * (block_span + block_gap)) - float(plot_tvec[0]) for idx, panel in enumerate(plot_order)}
    centers = {panel: float(0.5 * (plot_tvec[0] + plot_tvec[-1]) + offsets[panel]) for panel in plot_order}
    xlim = (min(float(plot_tvec[0] + offsets[p]) for p in plot_order), max(float(plot_tvec[-1] + offsets[p]) for p in plot_order))

    for idx, gene in enumerate(genes):
        ax = axes_arr[idx]
        for panel in plot_order:
            x_block = plot_tvec + offsets[panel]
            ax.axvspan(float(x_block[0]), float(x_block[-1]), color="#efefef", alpha=0.12, zorder=0)
            mode = "LB" if panel.endswith("LB") else "LC"
            dur_vals = np.asarray(mode_durations.get(mode, []), dtype=float) if isinstance(mode_durations, dict) else np.asarray([], dtype=float)
            if dur_vals.size:
                d = float(np.nanmedian(dur_vals))
                if np.isfinite(d) and d > 0:
                    ax.axvspan(offsets[panel] + max(0.0, x_min), offsets[panel] + min(d, x_max), color="#cccccc", alpha=0.18, zorder=0)

            for fish_id in fish_order:
                roi_map = (((results_per_cell_by_fish.get(fish_id, {}) or {}).get(panel, {}) or {}).get(gene, {}) or {})
                if isinstance(roi_map, dict):
                    for cell_res in roi_map.values():
                        mean = np.asarray(cell_res.get("mean", []), dtype=float)
                        if mean.size == tvec_arr.size:
                            ax.plot(x_block, mean[mask], color=fish_colors[fish_id], linewidth=0.9, alpha=0.18)

            for fish_id in fish_order:
                fish_res = (((results_by_fish.get(fish_id, {}) or {}).get(panel, {}) or {}).get(gene, None))
                if fish_res is None:
                    continue
                if int(fish_res.get("n_segments", 0)) < int(min_segments) or int(fish_res.get("n_cells", 0)) < int(min_cells):
                    continue
                mean = np.asarray(fish_res.get("mean", []), dtype=float)
                if mean.size == tvec_arr.size:
                    ax.plot(x_block, mean[mask], color=fish_colors[fish_id], linewidth=1.6, alpha=0.92)

            cohort_res = ((results.get(panel, {}) or {}).get(gene, None))
            if cohort_res is not None:
                mean = np.asarray(cohort_res.get("mean", []), dtype=float)
                if mean.size == tvec_arr.size:
                    ax.plot(x_block, mean[mask], color="#111111", linewidth=2.3, alpha=0.95)
            ax.axvline(offsets[panel], color="k", linestyle="--", linewidth=0.85, alpha=0.7)

        ax.axhline(0.0, color="k", linewidth=0.8, alpha=0.6)
        ax.set_xlim(*xlim)
        ax.set_ylim(-5.0, 10.0)
        ax.set_ylabel(f"{gene}\nz-scored dF/F")
        for fidx, fid in enumerate(fish_order):
            ax.text(0.985, 0.965 - (fidx * 0.085), str(fid), transform=ax.transAxes, ha="right", va="top", fontsize=7.5, color=fish_colors[fid])
        if idx == 0:
            for panel in plot_order:
                ax.text(centers[panel], 1.005, plot_titles.get(panel, panel), transform=ax.get_xaxis_transform(), ha="center", va="bottom", fontsize=8.5)

    axes_arr[-1].set_xticks([centers[p] for p in plot_order], [plot_titles.get(p, p).replace(" × ", "\n") for p in plot_order])
    axes_arr[-1].set_xlabel("Time (s)")
    for ax in axes_arr[:-1]:
        ax.tick_params(axis="x", which="both", bottom=False, labelbottom=False)
    n_fish_ok = int(cohort_fish_summary_df.get("ok", pd.Series(dtype=bool)).sum()) if isinstance(cohort_fish_summary_df, pd.DataFrame) else len(fish_order)
    fig.suptitle(f"Cohort [56h]: per-gene stimulus traces by fish (fish n={n_fish_ok})", y=0.995)
    fig.tight_layout(rect=[0, 0.04, 1, 0.93])
    save_path = Path(out_path) if out_path is not None else None
    if save_path is not None:
        save_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(save_path, dpi=200, bbox_inches="tight")
    return {"fig": fig, "out_path": save_path, "fish_order": fish_order, "genes": genes}


def render_cohort_56g_diagnostics(
    *,
    df: pd.DataFrame,
    gene_order: list[str],
    gene_colors: dict[str, str],
    cohort_fish_summary_df: pd.DataFrame | None,
    out_path: str | Path | None = None,
) -> dict[str, Any]:
    if not isinstance(df, pd.DataFrame) or df.empty:
        raise RuntimeError("cohort_bpi_cells_df missing; run [cohort-build] first.")
    work = df.copy()
    if {"mean_bout_zdff", "mean_cont_zdff"}.issubset(work.columns):
        bout_col, cont_col, activity_label = "mean_bout_zdff", "mean_cont_zdff", "z-scored dF/F"
    elif {"mean_bout_dff", "mean_cont_dff"}.issubset(work.columns):
        bout_col, cont_col, activity_label = "mean_bout_dff", "mean_cont_dff", "dF/F"
    else:
        raise RuntimeError("cohort_bpi_cells_df missing response columns needed for [56g].")
    bpi_col = "bpi" if "bpi" in work.columns else ("bpi_z" if "bpi_z" in work.columns else None)
    if bpi_col is None:
        raise RuntimeError("cohort_bpi_cells_df missing bpi columns.")
    for col in [bpi_col, bout_col, cont_col]:
        work[col] = pd.to_numeric(work[col], errors="coerce")
    work["activity_mag"] = (work[bout_col].abs() + work[cont_col].abs()) / 2.0
    work["abs_bpi"] = work[bpi_col].abs()
    work = work[np.isfinite(work["activity_mag"]) & np.isfinite(work[bpi_col]) & np.isfinite(work[bout_col]) & np.isfinite(work[cont_col])].copy()
    if work.empty:
        raise RuntimeError("No finite rows left for [56g-cohort].")
    thr = float(np.nanquantile(work["activity_mag"].to_numpy(dtype=float), 0.20))
    work["is_bpi_near_zero"] = work[bpi_col].abs() <= 0.10
    work["is_nonresponsive"] = work["activity_mag"] <= thr
    work["interpretation"] = np.where(
        work["is_bpi_near_zero"] & work["is_nonresponsive"],
        "near-zero BPI + low activity",
        np.where(work["is_bpi_near_zero"], "near-zero BPI + high activity", "non-zero BPI"),
    )

    gene_order_plot = [g for g in gene_order if g in set(work["gene"].astype(str))] + [g for g in sorted(set(work["gene"].astype(str))) if g not in gene_order]
    fig, axes = plt.subplots(2, 2, figsize=(14, 10.5))
    ax_plane, ax_quad, ax_bins, ax_group = axes[0, 0], axes[0, 1], axes[1, 0], axes[1, 1]
    for gene in gene_order_plot:
        sub = work[work["gene"].astype(str) == str(gene)]
        if sub.empty:
            continue
        ax_plane.scatter(sub[cont_col], sub[bout_col], s=18.0, alpha=0.7, color=gene_colors.get(gene, "#777777"), edgecolors="none", label=str(gene))
        ax_quad.scatter(sub["activity_mag"], sub[bpi_col], s=18.0, alpha=0.7, color=gene_colors.get(gene, "#777777"), edgecolors="none")
    vmax = float(np.nanpercentile(np.abs(np.concatenate([work[cont_col].to_numpy(dtype=float), work[bout_col].to_numpy(dtype=float)])), 99))
    vmax = 1.0 if (not np.isfinite(vmax) or vmax <= 0) else vmax
    ax_plane.plot([-vmax, vmax], [-vmax, vmax], linestyle="--", color="black", linewidth=1.0, alpha=0.8)
    ax_plane.axhline(0.0, color="#444444", linewidth=0.8)
    ax_plane.axvline(0.0, color="#444444", linewidth=0.8)
    ax_plane.set_xlim(-vmax, vmax)
    ax_plane.set_ylim(-vmax, vmax)
    ax_plane.set_xlabel(f"Mean continuous response ({activity_label})")
    ax_plane.set_ylabel(f"Mean bout response ({activity_label})")
    ax_plane.set_title("1) Bout vs Continuous Response Plane")
    handles, labels = ax_plane.get_legend_handles_labels()
    if handles:
        ax_plane.legend(handles, labels, fontsize=7, ncol=2, loc="lower right")
    ax_quad.axhline(0.0, color="black", linestyle="--", linewidth=1.0)
    ax_quad.axhline(0.10, color="#444444", linestyle=":", linewidth=1.0)
    ax_quad.axhline(-0.10, color="#444444", linestyle=":", linewidth=1.0)
    ax_quad.axvline(thr, color="#2b2b2b", linestyle="-.", linewidth=1.0)
    ax_quad.set_xlabel(f"Activity magnitude ({activity_label})")
    ax_quad.set_ylabel(f"BPI ({bpi_col})")
    ax_quad.set_ylim(-1.02, 1.02)
    ax_quad.set_xlim(left=0)
    ax_quad.set_title("2) BPI vs Activity Magnitude")
    bins = pd.qcut(work["activity_mag"], q=min(6, int(work["activity_mag"].nunique())), duplicates="drop")
    grouped = work.assign(_bin=bins).groupby("_bin", observed=False)["abs_bpi"]
    binned_df = grouped.agg(median_abs_bpi="median", q25_abs_bpi=lambda s: s.quantile(0.25), q75_abs_bpi=lambda s: s.quantile(0.75), n_cells="size").reset_index()
    if binned_df.empty:
        ax_bins.text(0.5, 0.5, "Insufficient data for binning", transform=ax_bins.transAxes, ha="center", va="center")
    else:
        mids = np.asarray([0.5 * (float(interval.left) + float(interval.right)) for interval in binned_df["_bin"]], dtype=float)
        ax_bins.plot(mids, binned_df["median_abs_bpi"].to_numpy(dtype=float), marker="o", color="#1f77b4", linewidth=1.8)
        ax_bins.fill_between(mids, binned_df["q25_abs_bpi"].to_numpy(dtype=float), binned_df["q75_abs_bpi"].to_numpy(dtype=float), alpha=0.2, color="#1f77b4")
        ax_bins.set_ylim(0, 1.02)
        ax_bins.set_xlim(left=0)
    ax_bins.set_title("3) Activity-Binned Tuning Strength")
    ax_bins.set_xlabel(f"Activity magnitude ({activity_label}; bin mid)")
    ax_bins.set_ylabel("Median |BPI|")
    groups = ["near-zero BPI + low activity", "near-zero BPI + high activity", "non-zero BPI"]
    data = [work.loc[work["interpretation"] == label, "activity_mag"].to_numpy(dtype=float) for label in groups if np.any(work["interpretation"] == label)]
    labels_group = [f"{label}\n(n={int(np.sum(work['interpretation'] == label))})" for label in groups if np.any(work["interpretation"] == label)]
    if data:
        bp = ax_group.boxplot(data, patch_artist=True, showfliers=False)
        for patch in bp["boxes"]:
            patch.set_facecolor("#d0d0d0")
            patch.set_alpha(0.45)
        ax_group.set_xticks(np.arange(1, len(labels_group) + 1))
        ax_group.set_xticklabels(labels_group, rotation=12, ha="right")
        ax_group.axhline(thr, color="#2b2b2b", linestyle="-.", linewidth=1.0)
    else:
        ax_group.text(0.5, 0.5, "No group data", transform=ax_group.transAxes, ha="center", va="center")
    ax_group.set_title("4) Activity by BPI/Response Class")
    ax_group.set_ylabel(f"Activity magnitude ({activity_label})")
    n_fish_ok = int(cohort_fish_summary_df.get("ok", pd.Series(dtype=bool)).sum()) if isinstance(cohort_fish_summary_df, pd.DataFrame) else 0
    fig.suptitle(f"Cohort [56g]: BPI vs activity diagnostics (fish n={n_fish_ok})", fontsize=13)
    fig.tight_layout(rect=[0, 0, 1, 0.97])
    save_path = Path(out_path) if out_path is not None else None
    if save_path is not None:
        save_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(save_path, dpi=200, bbox_inches="tight")
    return {"fig": fig, "out_path": save_path, "cohort_bpi_activity_df": work, "cohort_bpi_activity_bins_df": binned_df}


def render_cohort_motion_auc(
    *,
    cohort_outdir: str | Path,
    fish_specs: list[dict[str, str]],
    data_root: str | Path,
    data_mode: str,
    gene_order: list[str],
    gene_colors: dict[str, str],
    cohort_fish_summary_df: pd.DataFrame | None,
    fish_order_hint: list[str] | None = None,
    points_df: pd.DataFrame | None = None,
    counts_df: pd.DataFrame | None = None,
    out_path: str | Path | None = None,
) -> dict[str, Any]:
    import matplotlib as mpl
    import matplotlib.colors as mcolors

    mpl.rcParams["font.family"] = "sans-serif"
    mpl.rcParams["font.sans-serif"] = ["Aptos", "Aptos (Body)", "Calibri", "Arial", "DejaVu Sans"]

    outdir = Path(cohort_outdir)
    points_csv = outdir / "cohort_motion_auc_plot_points.csv"
    counts_csv = outdir / "cohort_motion_auc_plot_counts.csv"
    pts = points_df.copy() if isinstance(points_df, pd.DataFrame) else None
    cnt = counts_df.copy() if isinstance(counts_df, pd.DataFrame) else None
    if pts is None or cnt is None:
        if points_csv.exists() and counts_csv.exists():
            pts = pd.read_csv(points_csv)
            cnt = pd.read_csv(counts_csv)
        else:
            all_points: list[pd.DataFrame] = []
            all_counts: list[pd.DataFrame] = []
            for spec in fish_specs:
                owner = str(spec["owner"])
                fish_id = str(spec["fish_id"])
                paths = _fish_paths_for_cohort(data_root, owner, fish_id, data_mode)
                fish_points_csv = paths["out_reg"] / "motion_auc_plot_points.csv"
                fish_counts_csv = paths["out_reg"] / "motion_auc_plot_counts.csv"
                if not fish_points_csv.exists() or not fish_counts_csv.exists():
                    continue
                df_pts = pd.read_csv(fish_points_csv)
                df_cnt = pd.read_csv(fish_counts_csv)
                df_pts["fish_id"] = fish_id
                df_cnt["fish_id"] = fish_id
                all_points.append(df_pts)
                all_counts.append(df_cnt)
            if not all_points:
                raise RuntimeError("[cohort-auc] No single-fish AUC data found. Run [56i] on each fish first.")
            pts = pd.concat(all_points, ignore_index=True)
            cnt = pd.concat(all_counts, ignore_index=True)
            pts.to_csv(points_csv, index=False)
            cnt.to_csv(counts_csv, index=False)

    if pts is None or cnt is None or pts.empty:
        raise RuntimeError("[cohort-auc] AUC data unavailable; cannot proceed")

    if "fish_id" not in pts.columns:
        pts["fish_id"] = "unknown"
    if "fish_id" not in cnt.columns:
        cnt["fish_id"] = "unknown"
    pts["laterality"] = pts["laterality"].astype(str).str.strip().str.lower()
    pts["stim_mode"] = pts["stim_mode"].astype(str).str.strip().str.lower()
    pts["auc_dff"] = pd.to_numeric(pts["auc_dff"], errors="coerce")
    pts["response_class"] = pts["response_class"].astype(str).str.strip().str.lower()
    _fish_auc_scale = pts.groupby("fish_id", dropna=False)["auc_dff"].transform(
        lambda s: float(np.nanquantile(np.abs(pd.to_numeric(s, errors="coerce").to_numpy(dtype=float)), 1.0))
    )
    _fish_scale_ok = np.isfinite(_fish_auc_scale.to_numpy(dtype=float)) & (_fish_auc_scale.to_numpy(dtype=float) > 1e-12)
    pts["auc_dff_norm"] = 0.0
    pts.loc[_fish_scale_ok, "auc_dff_norm"] = pts.loc[_fish_scale_ok, "auc_dff"] / _fish_auc_scale.loc[_fish_scale_ok]
    if "bpi_category" in pts.columns:
        pts["bpi_category"] = pts["bpi_category"].astype(str).str.strip().str.lower()
    pts["response_is_active"] = pts["response_is_active"].astype(str).str.strip().str.lower().isin({"1", "true", "yes", "t"})
    cnt["laterality"] = cnt["laterality"].astype(str).str.strip().str.lower()
    cnt["stim_mode"] = cnt["stim_mode"].astype(str).str.strip().str.lower()

    all_neurons_points = pts[pts["group"].astype(str) == "All neurons"].copy()
    gene_points = pts[pts["group"].astype(str) != "All neurons"].copy()
    fish_present = sorted(pts["fish_id"].dropna().astype(str).unique().tolist())
    fish_order = [f for f in (fish_order_hint or []) if f in fish_present] + [f for f in fish_present if f not in (fish_order_hint or [])]
    if not fish_order:
        fish_order = fish_present if fish_present else ["unknown"]
    ordered_genes = [g for g in gene_order if g in set(gene_points["group"].dropna().astype(str))] + [
        g for g in sorted(set(gene_points["group"].dropna().astype(str))) if g not in gene_order
    ]

    AUC_FIGURE_WIDTH = 11.34 * 0.80
    AUC_FIGURE_HEIGHT = 6
    AUC_FIGURE_DPI = 300
    AUC_BOX_WIDTH = 0.30
    AUC_POINT_SIZE_ALL = 5.0
    AUC_POINT_SIZE_GENE = 16.0
    AUC_WITHIN_FISH_JITTER = 0.010
    AUC_PAIR_MODE_OFFSET_FISH = 0.08
    AUC_POINT_ALPHA_ALL = 0.15
    AUC_POINT_ALPHA_GENE = 0.80
    AUC_PAIR_LINE_ALPHA_ALL = 0.08
    AUC_CONT_LIGHTEN = 0.80
    AUC_ROW_LABEL_FONT_SIZE = 11
    AUC_LANE_LABEL_FONT_SIZE = 8
    AUC_LANE_GUIDE_ALPHA = 0.18
    AUC_LANE_LABEL_Y_OFFSET_FRAC = -0.08
    AUC_LANE_COUNT_FONT_SIZE = 6
    AUC_LANE_COUNT_Y_OFFSET_FRAC = -0.13
    AUC_Y_PAD = 0.10
    RESPONSE_LOW_NORM = "low activity"

    intra_gap = 0.006
    inter_gap = 0.18
    group_gap = 0.12
    auc_box_width_final = max(0.30, float(AUC_BOX_WIDTH))
    pair_mode_offset_fish = max(float(AUC_PAIR_MODE_OFFSET_FISH), 0.5 * (auc_box_width_final + intra_gap))
    fish_spacing = auc_box_width_final + inter_gap + 2.0 * pair_mode_offset_fish
    fish_center_offsets = np.array([0.0], dtype=float) if len(fish_order) <= 1 else np.linspace(
        -0.5 * fish_spacing * float(len(fish_order) - 1),
        0.5 * fish_spacing * float(len(fish_order) - 1),
        len(fish_order),
    )
    fish_offset_map = {fish: float(off) for fish, off in zip(fish_order, fish_center_offsets)}
    group_half_extent = float(np.max(np.abs(fish_center_offsets))) + pair_mode_offset_fish + 0.5 * auc_box_width_final
    group_step = max(1.0, 2.0 * group_half_extent + group_gap)
    group_order_all = ["All neurons"]

    def _blend_color(color: str, frac: float = AUC_CONT_LIGHTEN) -> tuple[float, float, float] | str:
        try:
            src = np.asarray(mcolors.to_rgb(color), dtype=float)
        except Exception:
            return color
        return tuple((1.0 - frac) * src + frac * np.ones(3, dtype=float))

    def _shade_from_gene(gene_name: str, fish_name: str) -> tuple[float, float, float]:
        base_rgb = np.asarray(mcolors.to_rgb(gene_colors.get(gene_name, "#bbbbbb")), dtype=float)
        blend = 0.38 if len(fish_order) == 1 else float(np.linspace(0.62, 0.20, len(fish_order))[fish_order.index(fish_name)])
        return tuple(np.clip((1.0 - blend) * base_rgb + blend * np.ones(3, dtype=float), 0.0, 1.0))

    group_color_map = {"All neurons": "#4c4c4c"}
    for gene in ordered_genes:
        group_color_map[gene] = gene_colors.get(gene, "#666666")
    mode_color_map = {group: {"bout": group_color_map[group], "continuous": _blend_color(group_color_map[group])} for group in (group_order_all + ordered_genes)}

    cohort_points_tmp = pts.copy()
    cohort_points_tmp["fish_id_norm"] = cohort_points_tmp["fish_id"].astype(str)
    cohort_points_tmp["group_norm"] = cohort_points_tmp["group"].astype(str)
    cohort_points_tmp["laterality_norm"] = cohort_points_tmp["laterality"].astype(str).str.strip().str.lower()
    cohort_points_tmp["is_included_n"] = cohort_points_tmp["response_is_active"].astype(bool) | (
        cohort_points_tmp["response_class"].astype(str).str.strip().str.lower() == RESPONSE_LOW_NORM
    )
    cohort_n_tbl = cohort_points_tmp[cohort_points_tmp["is_included_n"]].groupby(["group_norm", "laterality_norm", "fish_id_norm"], dropna=False).size().rename("n").reset_index()
    cohort_n_map = {(str(r["group_norm"]), str(r["laterality_norm"]), str(r["fish_id_norm"])): int(r["n"]) for _, r in cohort_n_tbl.iterrows()}

    fig = plt.figure(figsize=(AUC_FIGURE_WIDTH, AUC_FIGURE_HEIGHT), dpi=AUC_FIGURE_DPI)
    gs = fig.add_gridspec(6, 8, hspace=0.35, wspace=0.25)
    ax_all_ipsi = fig.add_subplot(gs[0:3, 0:2])
    ax_gene_ipsi = fig.add_subplot(gs[0:3, 2:8])
    ax_all_contra = fig.add_subplot(gs[3:6, 0:2], sharex=ax_all_ipsi, sharey=ax_all_ipsi)
    ax_gene_contra = fig.add_subplot(gs[3:6, 2:8], sharex=ax_gene_ipsi, sharey=ax_gene_ipsi)

    def _plot_auc_block(ax: plt.Axes, source_points: pd.DataFrame, groups: list[str], group_pos: np.ndarray, laterality: str, *, is_gene_panel: bool, panel_title: str, show_ylabel: bool = False, hide_y_labels: bool = False, hide_x_labels: bool = False) -> None:
        sub = source_points[source_points["laterality"] == laterality].copy()
        if sub.empty:
            ax.text(0.5, 0.5, "No data", transform=ax.transAxes, ha="center", va="center", fontsize=9)
            return

        box_data: list[np.ndarray] = []
        box_positions: list[float] = []
        box_meta: list[tuple[str, str, str]] = []
        for g_idx, group in enumerate(groups):
            gsub = sub[sub["group"] == group].copy()
            if gsub.empty:
                continue
            gsub["fish_id_norm"] = gsub["fish_id"].astype(str)
            for fish in fish_order:
                fsub = gsub[gsub["fish_id_norm"] == str(fish)]
                if fsub.empty:
                    continue
                fish_offset = float(fish_offset_map.get(str(fish), 0.0))
                for mode in ("bout", "continuous"):
                    vals = fsub.loc[fsub["stim_mode"] == mode, "auc_dff_norm"].dropna().to_numpy(dtype=float)
                    if vals.size == 0:
                        continue
                    box_data.append(vals)
                    mode_offset = -pair_mode_offset_fish if mode == "bout" else pair_mode_offset_fish
                    box_positions.append(float(group_pos[g_idx]) + fish_offset + float(mode_offset))
                    box_meta.append((group, fish, mode))
        if not box_data:
            ax.text(0.5, 0.5, "No AUC data", transform=ax.transAxes, ha="center", va="center", fontsize=9)
            return
        bp = ax.boxplot(
            box_data,
            positions=box_positions,
            widths=auc_box_width_final,
            patch_artist=True,
            showfliers=False,
            medianprops={"color": "#1a1a1a", "linewidth": 1.0},
            whiskerprops={"color": "#707070", "linewidth": 0.8},
            capprops={"color": "#707070", "linewidth": 0.8},
            boxprops={"linewidth": 0.8, "edgecolor": "#707070"},
        )
        for patch, (group, fish, mode) in zip(bp["boxes"], box_meta):
            color = mode_color_map[group][mode]
            if is_gene_panel:
                fish_base = _shade_from_gene(group, str(fish))
                color = fish_base if mode == "bout" else _blend_color(fish_base)
            patch.set_facecolor(color)
            patch.set_alpha(0.22)
            patch.set_edgecolor(color)

        rng = np.random.default_rng(56)
        size = AUC_POINT_SIZE_GENE if is_gene_panel else AUC_POINT_SIZE_ALL
        alpha = AUC_POINT_ALPHA_GENE if is_gene_panel else AUC_POINT_ALPHA_ALL
        for g_idx, group in enumerate(groups):
            gsub = sub[sub["group"] == group].copy()
            if gsub.empty:
                continue
            gsub["fish_id_norm"] = gsub["fish_id"].astype(str)
            gsub["fish_dodge"] = gsub["fish_id_norm"].map(fish_offset_map).fillna(0.0)
            gsub["within_fish_jitter"] = rng.uniform(-AUC_WITHIN_FISH_JITTER, AUC_WITHIN_FISH_JITTER, size=len(gsub))
            key_candidates = [
                ["fish_id_norm", "group", "plane_idx", "func_label"],
                ["fish_id_norm", "group", "plane", "func_label"],
                ["fish_id_norm", "group", "cell_key"],
                ["fish_id_norm", "group", "roi_idx"],
            ]
            pair_key_cols = next((cand for cand in key_candidates if all(c in gsub.columns for c in cand)), None)
            if pair_key_cols is not None:
                pair_src = gsub[pair_key_cols + ["stim_mode", "auc_dff_norm"]].copy()
                pair_src["auc_dff_norm"] = pd.to_numeric(pair_src["auc_dff_norm"], errors="coerce")
                pair_src = pair_src[np.isfinite(pair_src["auc_dff_norm"])]
                if not pair_src.empty:
                    pair_agg = pair_src.groupby(pair_key_cols + ["stim_mode"], dropna=False)["auc_dff_norm"].mean().reset_index()
                    pair_wide = pair_agg.pivot_table(index=pair_key_cols, columns="stim_mode", values="auc_dff_norm", aggfunc="mean").reset_index()
                    if ("bout" in pair_wide.columns) and ("continuous" in pair_wide.columns):
                        pair_wide = pair_wide.dropna(subset=["bout", "continuous"])
                        if not pair_wide.empty:
                            x_center = float(group_pos[g_idx]) + pair_wide["fish_id_norm"].map(fish_offset_map).fillna(0.0).to_numpy(dtype=float)
                            pair_line_alpha = 0.35 if is_gene_panel else float(AUC_PAIR_LINE_ALPHA_ALL)
                            for xb, xc, yb, yc in zip(x_center - pair_mode_offset_fish, x_center + pair_mode_offset_fish, pair_wide["bout"].to_numpy(dtype=float), pair_wide["continuous"].to_numpy(dtype=float)):
                                ax.plot([xb, xc], [yb, yc], color="#5f5f5f", linewidth=0.55, alpha=pair_line_alpha, zorder=2)
            for mode in ("bout", "continuous"):
                msub = gsub[gsub["stim_mode"] == mode].copy()
                if msub.empty:
                    continue
                mode_offset = -pair_mode_offset_fish if mode == "bout" else pair_mode_offset_fish
                x_pos = float(group_pos[g_idx]) + float(mode_offset) + msub["fish_dodge"].to_numpy(dtype=float) + msub["within_fish_jitter"].to_numpy(dtype=float)
                y_pos = msub["auc_dff_norm"].to_numpy(dtype=float)
                resp_mask = msub["response_is_active"].to_numpy(dtype=bool)
                if resp_mask.any():
                    color_pt = mode_color_map[group][mode]
                    ax.scatter(x_pos[resp_mask], y_pos[resp_mask], s=size, facecolors=color_pt, edgecolors="none", alpha=alpha, zorder=3)
                low_mask = (~msub["response_is_active"]) & (msub["response_class"].astype(str).str.strip().str.lower() == RESPONSE_LOW_NORM)
                if low_mask.any():
                    color_pt = mode_color_map[group][mode]
                    ax.scatter(x_pos[low_mask], y_pos[low_mask], s=size, facecolors="none", edgecolors=color_pt, linewidths=0.7, alpha=min(1.0, alpha + 0.06), zorder=3)
                weak_mask = np.zeros(len(msub), dtype=bool)
                if "bpi_category" in msub.columns:
                    weak_mask = (~msub["response_is_active"]) & (msub["bpi_category"].astype(str).str.lower() == "weak-response")
                if weak_mask.any():
                    color_pt = mode_color_map[group][mode]
                    ax.scatter(x_pos[weak_mask], y_pos[weak_mask], s=size * 0.95, facecolors="none", edgecolors=color_pt, linewidths=1.0, alpha=min(1.0, alpha + 0.15), zorder=4)

        vals_panel = pd.to_numeric(sub["auc_dff_norm"], errors="coerce").to_numpy(dtype=float)
        vals_panel = vals_panel[np.isfinite(vals_panel)]
        y_min = 0.0
        y_max = float(np.nanmax(vals_panel) + float(AUC_Y_PAD)) if vals_panel.size else 1.0
        y_max = y_min + 1.0 if y_max <= y_min else y_max
        ax.axhline(0.0, color="#d0d0d0", linewidth=0.9, zorder=0)
        x_pad = float(np.max(np.abs(fish_center_offsets))) + float(pair_mode_offset_fish) + 0.20
        ax.set_xlim(float(group_pos.min()) - x_pad, float(group_pos.max()) + x_pad)
        ax.set_ylim(y_min, y_max)
        if len(fish_center_offsets) > 0:
            lane_x = [float(group_pos[g_idx]) + float(off) for g_idx, _ in enumerate(groups) for off in fish_center_offsets]
            if lane_x:
                ax.vlines(lane_x, y_min, y_max, colors="#8a8a8a", linestyles=":", linewidth=0.6, alpha=AUC_LANE_GUIDE_ALPHA, zorder=0)
        ax.set_xticks(group_pos)
        if hide_x_labels:
            ax.set_xticklabels([])
            ax.tick_params(axis="x", labelbottom=False)
        else:
            ax.set_xticklabels(groups, rotation=45, ha="right", fontsize=9)
        if panel_title.strip():
            ax.set_title(panel_title, fontsize=AUC_ROW_LABEL_FONT_SIZE)
        if show_ylabel:
            ax.set_ylabel("Mean motion-window AUC\n(within-fish q100 |AUC| normalised)", fontsize=10)
        if hide_y_labels:
            ax.tick_params(axis="y", labelleft=False)
        ax.grid(axis="y", alpha=0.2, linestyle="--")

    all_pos = np.array([0.0])
    gene_pos = np.arange(len(ordered_genes), dtype=float) * float(group_step)
    _plot_auc_block(ax_all_ipsi, all_neurons_points, group_order_all, all_pos, "ipsi", is_gene_panel=False, panel_title="Global activity", show_ylabel=True, hide_x_labels=True)
    _plot_auc_block(ax_all_contra, all_neurons_points, group_order_all, all_pos, "contra", is_gene_panel=False, panel_title="", show_ylabel=True)
    _plot_auc_block(ax_gene_ipsi, gene_points, ordered_genes, gene_pos, "ipsi", is_gene_panel=True, panel_title="Marker-specific activity", hide_y_labels=True, hide_x_labels=True)
    _plot_auc_block(ax_gene_contra, gene_points, ordered_genes, gene_pos, "contra", is_gene_panel=True, panel_title="", hide_y_labels=True)

    fig.text(0.03, 0.7, "Ipsi", ha="left", va="center", rotation=90, fontsize=AUC_ROW_LABEL_FONT_SIZE, fontweight="normal")
    fig.text(0.03, 0.3, "Contra", ha="left", va="center", rotation=90, fontsize=AUC_ROW_LABEL_FONT_SIZE, fontweight="normal")

    lane_labels = [f"F{i+1}" for i in range(len(fish_order))]
    def _annotate_lanes(ax_lane: plt.Axes, groups_for_ax: list[str], pos_for_ax: np.ndarray, laterality_for_ax: str) -> None:
        if len(groups_for_ax) == 0:
            return
        y_top = ax_lane.get_ylim()[1]
        y_span = max(1e-6, ax_lane.get_ylim()[1] - ax_lane.get_ylim()[0])
        y_lbl = y_top + float(AUC_LANE_LABEL_Y_OFFSET_FRAC) * y_span
        y_cnt = y_top + float(AUC_LANE_COUNT_Y_OFFSET_FRAC) * y_span
        for gname, gx in zip(groups_for_ax, np.asarray(pos_for_ax, dtype=float)):
            for fish_id, lbl, off in zip(fish_order, lane_labels, fish_center_offsets):
                x_lane = float(gx + float(off))
                ax_lane.text(x_lane, float(y_lbl), lbl, ha="center", va="bottom", fontsize=AUC_LANE_LABEL_FONT_SIZE, color="#4a4a4a", clip_on=False)
                n_val = int(cohort_n_map.get((str(gname), str(laterality_for_ax), str(fish_id)), 0))
                ax_lane.text(x_lane, float(y_cnt), f"n={n_val}", ha="center", va="top", fontsize=AUC_LANE_COUNT_FONT_SIZE, color="#555555", clip_on=False)
    _annotate_lanes(ax_all_ipsi, group_order_all, all_pos, "ipsi")
    _annotate_lanes(ax_gene_ipsi, ordered_genes, gene_pos, "ipsi")
    fish_key_txt = ", ".join([f"F{i+1}:{f}" for i, f in enumerate(fish_order)])
    ax_gene_ipsi.text(1.0, 1.10, fish_key_txt, transform=ax_gene_ipsi.transAxes, ha="right", va="bottom", fontsize=max(6, AUC_LANE_LABEL_FONT_SIZE - 1), color="#666666")

    n_fish_ok = int(cohort_fish_summary_df.get("ok", pd.Series(dtype=bool)).sum()) if isinstance(cohort_fish_summary_df, pd.DataFrame) else len(fish_order)
    fig.suptitle(
        f"Cohort [50l-style] Motion-Window AUC (fish n={n_fish_ok}, All neurons n={len(all_neurons_points)}, Genes n={len(gene_points)})",
        fontsize=13,
        fontweight="bold",
        y=0.995,
    )
    fig.tight_layout(rect=[0.06, 0, 1, 0.97])
    save_path = Path(out_path) if out_path is not None else (outdir / "cohort_50l_auc_ipsi_contra.png")
    save_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(save_path, dpi=AUC_FIGURE_DPI, bbox_inches="tight")
    return {"fig": fig, "out_path": save_path, "points_df": pts, "counts_df": cnt}


def render_cohort_56h_status_donut_grid(
    *,
    fish_specs: list[dict[str, str]],
    data_root: str | Path,
    data_mode: str,
    cohort_outdir: str | Path,
    cohort_fish_summary_df: pd.DataFrame | None,
    gene_order: list[str] | None = None,
) -> dict[str, Any]:
    gene_order_local = list(gene_order or ["sst1.1", "pth2", "sst1.2", "tac3b", "npy", "cfos"])
    fish_ok_set: set[str] = set()
    if isinstance(cohort_fish_summary_df, pd.DataFrame) and {"fish_id", "ok"}.issubset(cohort_fish_summary_df.columns):
        fish_ok_set = set(cohort_fish_summary_df.loc[cohort_fish_summary_df["ok"].astype(bool), "fish_id"].dropna().astype(str).tolist())
    fish_cols = []
    for spec in fish_specs:
        owner = str(spec["owner"])
        fish_id = str(spec["fish_id"])
        if fish_ok_set and fish_id not in fish_ok_set:
            continue
        paths = _fish_paths_for_cohort(data_root, owner, fish_id, data_mode)
        status_csv = paths["out_reg"] / "hcr_activity_status.csv"
        if status_csv.exists():
            fish_cols.append((owner, fish_id, status_csv))
    if not fish_cols:
        raise RuntimeError("No fish with hcr_activity_status.csv available for cohort donut grid.")
    rows = []
    counts_by_fish: dict[str, dict[str, dict[str, int]]] = {}
    for owner, fish_id, status_csv in fish_cols:
        sdf = pd.read_csv(status_csv)
        if "fish_id" in sdf.columns:
            sdf = sdf[sdf["fish_id"].astype(str) == fish_id].copy()
        sdf["gene"] = sdf.get("gene", pd.Series(dtype=object)).astype(str)
        sdf["functional_status"] = sdf.get("functional_status", pd.Series(dtype=object)).astype(str)
        per_gene: dict[str, dict[str, int]] = {}
        for gene in gene_order_local:
            sub = sdf[sdf["gene"] == gene]
            counts = {
                "responsive": int((sub["functional_status"] == "in-plane responsive ROI").sum()),
                "low": int((sub["functional_status"] == "in-plane low-activity ROI").sum()),
                "unavailable": int((sub["functional_status"] == "in-plane response unavailable").sum()),
                "no_func": int((sub["functional_status"] == "in-plane no functional ROI candidate").sum()),
                "out_of_plane": int((sub["functional_status"] == "out-of-plane anatomy label").sum()),
            }
            counts["n_labels"] = int(sum(counts.values()))
            per_gene[gene] = counts
            rows.append({"owner": owner, "fish_id": fish_id, "gene": gene, **counts})
        counts_by_fish[fish_id] = per_gene
    counts_df = pd.DataFrame(rows)
    outdir = Path(cohort_outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    counts_csv = outdir / "cohort_hcr_donut_counts_by_fish_gene.csv"
    counts_df.to_csv(counts_csv, index=False)
    fish_ids = [f for _, f, _ in fish_cols]
    fig, axes = plt.subplots(len(gene_order_local), len(fish_ids), figsize=(max(2.4 * len(fish_ids), 6.0), max(2.35 * len(gene_order_local), 8.5)), squeeze=False, subplot_kw={"aspect": "equal"})
    palette = {"responsive": "#1b9e77", "low": "#6a6a6a", "unavailable": "#fdb462", "no_func": "#d73027", "out_of_plane": "#80b1d3"}
    status_order = ["responsive", "low", "unavailable", "no_func", "out_of_plane"]
    for col_idx, fish_id in enumerate(fish_ids):
        axes[0, col_idx].set_title(str(fish_id), fontsize=11, pad=20)
    for r, gene in enumerate(gene_order_local):
        for c, fish_id in enumerate(fish_ids):
            ax = axes[r, c]
            ax.set_axis_off()
            d = (counts_by_fish.get(fish_id, {}) or {}).get(gene, {})
            vals = [float(d.get(k, 0)) for k in status_order]
            if sum(vals) <= 0:
                continue
            cols = [palette[k] for k in status_order]
            ax.pie(vals, radius=1.05, colors=cols, startangle=90, counterclock=False, wedgeprops=dict(width=0.45, edgecolor="white", linewidth=0.9))
            ax.add_artist(plt.Circle((0, 0), 0.50, fc="white", ec="white"))
            ax.text(0, 0, f"n={int(d.get('n_labels', 0))}", ha="center", va="center", fontsize=8.5, fontweight="bold")
            ax.set_xlim(-0.85, 0.85)
            ax.set_ylim(-0.78, 0.78)
    for r, gene in enumerate(gene_order_local):
        y = 0.5 * (axes[r, 0].get_position().y0 + axes[r, 0].get_position().y1)
        fig.text(0.015, y, str(gene), ha="left", va="center", fontsize=11)
    from matplotlib.patches import Patch
    fig.legend(handles=[Patch(facecolor=palette[k], edgecolor="none", label=k.replace("_", " ")) for k in status_order], loc="lower center", bbox_to_anchor=(0.53, 0.01), ncol=5, frameon=False, fontsize=8)
    fig.suptitle("Cohort [56h] HCR status donuts: fish x gene", y=0.995, fontsize=12, fontweight="bold")
    fig.tight_layout(rect=[0.05, 0.08, 1.0, 0.95])
    out_path = outdir / "cohort_56h_hcr_donut_grid_fish_by_gene.png"
    fig.savefig(out_path, dpi=300, bbox_inches="tight")
    return {"fig": fig, "out_path": out_path, "counts_df": counts_df, "fish_order": fish_ids, "gene_order": gene_order_local}


def render_cohort_50l_donut_row(
    *,
    fish_specs: list[dict[str, str]],
    data_root: str | Path,
    data_mode: str,
    cohort_outdir: str | Path,
) -> dict[str, Any]:
    import matplotlib.colors as mcolors

    COHORT_50L_RESPONSE_ORDER = ["Responsive neurons", "Low activity", "Response unavailable"]
    COHORT_50L_BPI_ORDER = ["bout-responsive", "continuous-responsive", "both-responsive", "weak-response", "low activity", "response unavailable"]
    COHORT_50L_RESPONSE_COLORS = {"Responsive neurons": "#1b9e77", "Low activity": "#8d8d8d", "Response unavailable": "#d9d9d9"}
    COHORT_50L_BPI_COLORS = {
        "bout-responsive": "#2c7fb8",
        "continuous-responsive": "#d95f0e",
        "both-responsive": "#d946ef",
        "weak-response": "#000000",
        "low activity": "#9e9e9e",
        "response unavailable": "#ececec",
    }
    COHORT_50L_BPI_SHORT = {
        "bout-responsive": "Bout-responsive",
        "continuous-responsive": "Cont.-responsive",
        "both-responsive": "Both-responsive",
        "weak-response": "Weak-response",
        "low activity": "Low activity",
        "response unavailable": "Unavailable",
    }

    COHORT_50L_ACTIVITY_RING_WIDTH = 0.35
    COHORT_50L_BPI_RING_WIDTH = COHORT_50L_ACTIVITY_RING_WIDTH * 0.375
    COHORT_50L_RING_GAP = 0.02
    COHORT_50L_ACTIVITY_LABEL_MIN_PCT = 6.0
    COHORT_50L_OUTER_RADIUS = 1.08
    COHORT_50L_VIEW_LIMIT = 1.20
    COHORT_50L_OUTER_COUNT_INSIDE_MIN = 150
    COHORT_50L_OUTER_LABEL_RADIUS = COHORT_50L_OUTER_RADIUS + 0.08
    COHORT_50L_OUTER_LABEL_GUTTER_X = COHORT_50L_VIEW_LIMIT - 0.09
    COHORT_50L_OUTER_LABEL_Y_MARGIN = 0.10
    COHORT_50L_OUTER_LABEL_MIN_GAP = 0.11
    COHORT_50L_OUTER_LABEL_TEXT_SHIFT = 0.1
    COHORT_50L_OUTER_LABEL_ELBOW_PAD = 0.04
    COHORT_50L_OUTER_LABEL_TEXT_PAD = -0.1
    COHORT_50L_OUTER_FORCE_OUTSIDE_WEDGE_DEG = 12.0
    COHORT_50L_OUTER_LEADER_LINEWIDTH = 0.8
    COHORT_50L_DPI = 300
    COHORT_50L_FIGSIZE = (11.2, 3.8)
    COHORT_50L_INNER_LABEL_FONTSIZE = 8.0
    COHORT_50L_CENTER_FONTSIZE = 10.0
    COHORT_50L_TITLE_FONTSIZE = 12.0
    COHORT_50L_COLUMN_TITLE_FONTSIZE = 11.0
    COHORT_50L_LEGEND_FONTSIZE = 8.5

    def _wedge_midpoint(wedge: Any, radius: float) -> tuple[float, float, float]:
        theta = np.deg2rad((float(wedge.theta1) + float(wedge.theta2)) / 2.0)
        return float(theta), float(radius) * np.cos(theta), float(radius) * np.sin(theta)

    def _tangent_rotation(theta_rad: float) -> float:
        theta_deg = ((float(np.rad2deg(theta_rad)) + 180.0) % 360.0) - 180.0
        rot = theta_deg - 90.0
        if rot < -90.0:
            rot += 180.0
        elif rot > 90.0:
            rot -= 180.0
        return float(rot)

    def _contrast_text_color(color: Any) -> str:
        try:
            r, g, b = mcolors.to_rgb(color)
        except Exception:
            return "black"
        luminance = 0.2126 * r + 0.7152 * g + 0.0722 * b
        return "white" if luminance < 0.52 else "black"

    def _resolve_outside_label_positions(candidates: list[dict[str, Any]], y_min: float, y_max: float, min_gap: float) -> list[dict[str, Any]]:
        if not candidates:
            return []
        resolved: list[dict[str, Any]] = []
        for side in ("left", "right"):
            side_items = [dict(item) for item in candidates if item.get("side") == side]
            if not side_items:
                continue
            side_items = sorted(side_items, key=lambda item: (float(item["target_y"]), float(item["theta"])))
            n_items = len(side_items)
            if n_items == 1:
                y_positions = np.array([float(np.clip(side_items[0]["target_y"], y_min, y_max))], dtype=float)
            else:
                span_needed = float(min_gap) * float(n_items - 1)
                if span_needed > float(y_max - y_min):
                    y_positions = np.linspace(float(y_min), float(y_max), n_items, dtype=float)
                else:
                    y_positions = np.array([float(np.clip(item["target_y"], y_min, y_max)) for item in side_items], dtype=float)
                    for idx in range(1, n_items):
                        y_positions[idx] = max(y_positions[idx], y_positions[idx - 1] + float(min_gap))
                    if y_positions[-1] > float(y_max):
                        y_positions -= float(y_positions[-1] - y_max)
                    if y_positions[0] < float(y_min):
                        y_positions += float(y_min - y_positions[0])
                    for idx in range(n_items - 2, -1, -1):
                        y_positions[idx] = min(y_positions[idx], y_positions[idx + 1] - float(min_gap))
                    if y_positions[0] < float(y_min):
                        y_positions = np.linspace(float(y_min), float(y_min) + span_needed, n_items, dtype=float)
            for item, y_text in zip(side_items, y_positions.tolist()):
                item["resolved_y"] = float(y_text)
                resolved.append(item)
        return sorted(resolved, key=lambda item: int(item["idx"]))

    def _outside_label_gap_stats(resolved_candidates: list[dict[str, Any]]) -> tuple[float, int]:
        gaps: list[float] = []
        overlap_pairs = 0
        for side in ("left", "right"):
            y_vals = sorted(float(item["resolved_y"]) for item in resolved_candidates if item.get("side") == side)
            if len(y_vals) < 2:
                continue
            side_gaps = np.diff(np.asarray(y_vals, dtype=float))
            gaps.extend(side_gaps.tolist())
            overlap_pairs += int(np.sum(side_gaps < (float(COHORT_50L_OUTER_LABEL_MIN_GAP) - 1e-9)))
        return (float(np.min(gaps)) if gaps else np.nan), int(overlap_pairs)

    def _load_master_df(master_csv: Path, fish_id: str) -> pd.DataFrame:
        detail_df = pd.read_csv(master_csv)
        if "fish_id" in detail_df.columns:
            detail_df = detail_df[detail_df["fish_id"].astype(str) == str(fish_id)].copy()
        if detail_df.empty:
            raise RuntimeError(f"[cohort-50l-donut] {fish_id}: master ROI table is empty")
        required_cols = {"response_summary_class", "bpi_category"}
        missing = sorted(required_cols - set(detail_df.columns))
        if missing:
            raise RuntimeError(f"[cohort-50l-donut] {fish_id}: missing required columns {missing}; rerun [50ia].")
        detail_df["response_summary_class"] = detail_df["response_summary_class"].astype(str)
        detail_df["bpi_category"] = detail_df["bpi_category"].astype(str)
        unknown_response = sorted(set(detail_df["response_summary_class"]) - set(COHORT_50L_RESPONSE_ORDER))
        unknown_bpi = sorted(set(detail_df["bpi_category"]) - set(COHORT_50L_BPI_ORDER))
        if unknown_response:
            raise RuntimeError(f"[cohort-50l-donut] {fish_id}: unexpected response_summary_class values {unknown_response}")
        if unknown_bpi:
            raise RuntimeError(f"[cohort-50l-donut] {fish_id}: unexpected bpi_category values {unknown_bpi}")
        return detail_df

    def _compute_counts(detail_df: pd.DataFrame, owner: str, fish_id: str, master_csv: Path) -> dict[str, Any]:
        response_counts = detail_df.groupby("response_summary_class", as_index=False).size().rename(columns={"size": "n_rois"})
        response_counts["response_order"] = response_counts["response_summary_class"].map({k: i for i, k in enumerate(COHORT_50L_RESPONSE_ORDER)}).fillna(10**6)
        response_counts = response_counts.sort_values("response_order").reset_index(drop=True)
        bpi_counts = detail_df.groupby(["response_summary_class", "bpi_category"], as_index=False).size().rename(columns={"size": "n_rois"})
        bpi_counts["response_order"] = bpi_counts["response_summary_class"].map({k: i for i, k in enumerate(COHORT_50L_RESPONSE_ORDER)}).fillna(10**6)
        bpi_counts["bpi_order"] = bpi_counts["bpi_category"].map({k: i for i, k in enumerate(COHORT_50L_BPI_ORDER)}).fillna(10**6)
        bpi_counts = bpi_counts.sort_values(["response_order", "bpi_order"]).reset_index(drop=True)
        response_full = pd.DataFrame({"response_summary_class": COHORT_50L_RESPONSE_ORDER}).merge(
            response_counts[["response_summary_class", "n_rois"]],
            on="response_summary_class",
            how="left",
        )
        response_full["n_rois"] = response_full["n_rois"].fillna(0).astype(int)
        bpi_totals = bpi_counts.groupby("bpi_category", as_index=False)["n_rois"].sum().rename(columns={"n_rois": "n_rois_total"})
        bpi_full = pd.DataFrame({"bpi_category": COHORT_50L_BPI_ORDER}).merge(bpi_totals, on="bpi_category", how="left")
        bpi_full["n_rois_total"] = bpi_full["n_rois_total"].fillna(0).astype(int)

        count_rows: list[dict[str, Any]] = []
        for order_idx, row in response_full.iterrows():
            count_rows.append(
                {
                    "owner": owner,
                    "fish_id": fish_id,
                    "master_csv": str(master_csv),
                    "ring": "inner",
                    "plot_order": int(order_idx),
                    "response_summary_class": str(row["response_summary_class"]),
                    "bpi_category": pd.NA,
                    "plot_category": str(row["response_summary_class"]),
                    "count": int(row["n_rois"]),
                    "n_total": int(len(detail_df)),
                }
            )
        for seg_idx, row in bpi_counts.reset_index(drop=True).iterrows():
            count_rows.append(
                {
                    "owner": owner,
                    "fish_id": fish_id,
                    "master_csv": str(master_csv),
                    "ring": "outer",
                    "plot_order": int(seg_idx),
                    "response_summary_class": str(row["response_summary_class"]),
                    "bpi_category": str(row["bpi_category"]),
                    "plot_category": str(row["bpi_category"]),
                    "count": int(row["n_rois"]),
                    "n_total": int(len(detail_df)),
                }
            )
        return {
            "owner": owner,
            "fish_id": fish_id,
            "master_csv": Path(master_csv),
            "detail_df": detail_df,
            "total_n": int(len(detail_df)),
            "response_counts_plot": response_counts[["response_summary_class", "n_rois"]].copy(),
            "bpi_counts_plot": bpi_counts[["response_summary_class", "bpi_category", "n_rois"]].copy(),
            "response_counts_full": response_full.copy(),
            "bpi_counts_full": bpi_full.copy(),
            "counts_long": pd.DataFrame(count_rows),
        }

    def _direct_50l_counts(master_csv: Path, fish_id: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        direct_df = _load_master_df(master_csv, fish_id)
        response_counts = direct_df.groupby("response_summary_class", as_index=False).size().rename(columns={"size": "n_rois"})
        response_counts["response_order"] = response_counts["response_summary_class"].map({k: i for i, k in enumerate(COHORT_50L_RESPONSE_ORDER)}).fillna(10**6)
        response_counts = response_counts.sort_values("response_order").reset_index(drop=True)
        bpi_counts = direct_df.groupby(["response_summary_class", "bpi_category"], as_index=False).size().rename(columns={"size": "n_rois"})
        bpi_counts["response_order"] = bpi_counts["response_summary_class"].map({k: i for i, k in enumerate(COHORT_50L_RESPONSE_ORDER)}).fillna(10**6)
        bpi_counts["bpi_order"] = bpi_counts["bpi_category"].map({k: i for i, k in enumerate(COHORT_50L_BPI_ORDER)}).fillna(10**6)
        bpi_counts = bpi_counts.sort_values(["response_order", "bpi_order"]).reset_index(drop=True)
        return (
            response_counts[["response_summary_class", "n_rois"]].reset_index(drop=True),
            bpi_counts[["response_summary_class", "bpi_category", "n_rois"]].reset_index(drop=True),
        )

    def _plot_donut(ax: plt.Axes, fish_bundle: dict[str, Any]) -> dict[str, Any]:
        response_df = fish_bundle["response_counts_plot"].copy()
        bpi_df = fish_bundle["bpi_counts_plot"].copy()
        response_df = response_df[pd.to_numeric(response_df["n_rois"], errors="coerce").fillna(0) > 0].reset_index(drop=True)
        bpi_df = bpi_df[pd.to_numeric(bpi_df["n_rois"], errors="coerce").fillna(0) > 0].reset_index(drop=True)
        if response_df.empty or bpi_df.empty:
            raise RuntimeError(f"[cohort-50l-donut] {fish_bundle['fish_id']}: no donut counts to plot")

        inner_outer_radius = COHORT_50L_OUTER_RADIUS - COHORT_50L_BPI_RING_WIDTH - COHORT_50L_RING_GAP
        outer_mid_radius = COHORT_50L_OUTER_RADIUS - (COHORT_50L_BPI_RING_WIDTH / 2.0)
        inner_mid_radius = inner_outer_radius - (COHORT_50L_ACTIVITY_RING_WIDTH / 2.0)
        centre_radius = inner_outer_radius - COHORT_50L_ACTIVITY_RING_WIDTH

        outer_wedges, _ = ax.pie(
            bpi_df["n_rois"].astype(float).tolist(),
            radius=COHORT_50L_OUTER_RADIUS,
            labels=None,
            colors=[COHORT_50L_BPI_COLORS[label] for label in bpi_df["bpi_category"].astype(str)],
            startangle=90,
            counterclock=False,
            wedgeprops=dict(width=COHORT_50L_BPI_RING_WIDTH, edgecolor="white", linewidth=1.0),
        )
        inner_wedges, _ = ax.pie(
            response_df["n_rois"].astype(float).tolist(),
            radius=inner_outer_radius,
            labels=None,
            colors=[COHORT_50L_RESPONSE_COLORS[label] for label in response_df["response_summary_class"].astype(str)],
            startangle=90,
            counterclock=False,
            wedgeprops=dict(width=COHORT_50L_ACTIVITY_RING_WIDTH, edgecolor="white", linewidth=1.0),
        )

        outer_text_labels_drawn = 0
        outside_candidates: list[dict[str, Any]] = []
        for wedge, val, color in zip(
            outer_wedges,
            bpi_df["n_rois"].astype(float).tolist(),
            [COHORT_50L_BPI_COLORS[label] for label in bpi_df["bpi_category"].astype(str)],
            strict=False,
        ):
            if float(val) <= 0:
                continue
            val_i = int(round(val))
            txt_color = _contrast_text_color(color)
            wedge_span_deg = abs(float(wedge.theta2) - float(wedge.theta1))
            if (val_i < COHORT_50L_OUTER_COUNT_INSIDE_MIN) or (wedge_span_deg <= COHORT_50L_OUTER_FORCE_OUTSIDE_WEDGE_DEG):
                theta, x_edge, y_edge = _wedge_midpoint(wedge, COHORT_50L_OUTER_RADIUS)
                _, x_target, y_target = _wedge_midpoint(wedge, COHORT_50L_OUTER_LABEL_RADIUS)
                outside_candidates.append(
                    {
                        "idx": int(outer_text_labels_drawn),
                        "theta": float(theta),
                        "x_edge": float(x_edge),
                        "y_edge": float(y_edge),
                        "target_y": float(y_target),
                        "side": ("right" if x_target >= 0 else "left"),
                        "value": int(val_i),
                    }
                )
            else:
                theta, x, y = _wedge_midpoint(wedge, outer_mid_radius)
                ax.text(
                    x,
                    y,
                    f"{val_i}",
                    ha="center",
                    va="center",
                    rotation=_tangent_rotation(theta),
                    rotation_mode="anchor",
                    fontsize=COHORT_50L_INNER_LABEL_FONTSIZE,
                    color=txt_color,
                )
            outer_text_labels_drawn += 1

        y_min = -float(COHORT_50L_VIEW_LIMIT) + float(COHORT_50L_OUTER_LABEL_Y_MARGIN)
        y_max = float(COHORT_50L_VIEW_LIMIT) - float(COHORT_50L_OUTER_LABEL_Y_MARGIN)
        resolved_outside_candidates = _resolve_outside_label_positions(
            outside_candidates,
            y_min=y_min,
            y_max=y_max,
            min_gap=COHORT_50L_OUTER_LABEL_MIN_GAP,
        )
        for item in resolved_outside_candidates:
            side_sign = 1.0 if item["side"] == "right" else -1.0
            x_text = side_sign * float(COHORT_50L_OUTER_LABEL_GUTTER_X + COHORT_50L_OUTER_LABEL_TEXT_SHIFT)
            y_text = float(item["resolved_y"])
            x_elbow = side_sign * float(COHORT_50L_OUTER_LABEL_RADIUS + COHORT_50L_OUTER_LABEL_ELBOW_PAD)
            x_text_anchor = x_text - (side_sign * float(COHORT_50L_OUTER_LABEL_TEXT_PAD))
            ax.plot(
                [float(item["x_edge"]), x_elbow, x_text_anchor],
                [float(item["y_edge"]), y_text, y_text],
                color="black",
                linewidth=COHORT_50L_OUTER_LEADER_LINEWIDTH,
                solid_capstyle="round",
                zorder=3,
            )
            ax.text(
                x_text,
                y_text,
                f"{int(item['value'])}",
                ha=("left" if side_sign > 0 else "right"),
                va="center",
                fontsize=COHORT_50L_INNER_LABEL_FONTSIZE,
                color="black",
            )

        total_response = float(response_df["n_rois"].sum())
        for wedge, label, val in zip(
            inner_wedges,
            response_df["response_summary_class"].astype(str).tolist(),
            response_df["n_rois"].astype(float).tolist(),
        ):
            if total_response <= 0 or float(val) <= 0:
                continue
            pct = 100.0 * float(val) / total_response
            if pct < COHORT_50L_ACTIVITY_LABEL_MIN_PCT:
                continue
            theta, x, y = _wedge_midpoint(wedge, inner_mid_radius)
            color = COHORT_50L_RESPONSE_COLORS.get(label, "#cccccc")
            ax.text(
                x,
                y,
                f"{label}\n{int(round(val))}",
                ha="center",
                va="center",
                rotation=_tangent_rotation(theta),
                rotation_mode="anchor",
                fontsize=COHORT_50L_INNER_LABEL_FONTSIZE,
                color=("black" if label == "Responsive neurons" else _contrast_text_color(color)),
            )

        ax.add_artist(plt.Circle((0, 0), centre_radius, fc="white", ec="white"))
        ax.text(0, 0, f"n = {int(fish_bundle['total_n'])}", ha="center", va="center", fontsize=COHORT_50L_CENTER_FONTSIZE, fontweight="bold")
        ax.set_title(str(fish_bundle["fish_id"]), fontsize=COHORT_50L_COLUMN_TITLE_FONTSIZE, pad=12)
        ax.set_xlim(-COHORT_50L_VIEW_LIMIT, COHORT_50L_VIEW_LIMIT)
        ax.set_ylim(-COHORT_50L_VIEW_LIMIT, COHORT_50L_VIEW_LIMIT)
        ax.set_axis_off()

        outside_min_gap, outside_overlap_pairs = _outside_label_gap_stats(resolved_outside_candidates)
        return {
            "n_inner_text_labels": int(sum(1 for txt in ax.texts if "\n" in txt.get_text())),
            "n_outer_text_labels": int(outer_text_labels_drawn),
            "n_outer_labels_inside": int(len(outer_wedges) - len(resolved_outside_candidates)),
            "n_outer_labels_outside": int(len(resolved_outside_candidates)),
            "outside_label_min_gap": float(outside_min_gap) if np.isfinite(outside_min_gap) else np.nan,
            "outside_label_overlap_pairs": int(outside_overlap_pairs),
            "n_outer_wedges": int(len(outer_wedges)),
            "n_inner_wedges": int(len(inner_wedges)),
        }

    bundles = []
    for spec in fish_specs:
        owner = str(spec["owner"])
        fish_id = str(spec["fish_id"])
        paths = _fish_paths_for_cohort(data_root, owner, fish_id, data_mode)
        master_csv = paths["out_reg"] / "functional_roi_activity_identity.csv"
        if not master_csv.exists():
            raise RuntimeError(f"[cohort-50l-donut] Missing master ROI table: {master_csv}")
        detail_df = _load_master_df(master_csv, fish_id)
        bundles.append(_compute_counts(detail_df, owner, fish_id, master_csv))
    if len(bundles) != 4:
        raise RuntimeError(f"[cohort-50l-donut] Expected 4 fish bundles, found {len(bundles)}.")

    outdir = Path(cohort_outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    fig, axes = plt.subplots(
        1,
        len(bundles),
        figsize=COHORT_50L_FIGSIZE,
        dpi=COHORT_50L_DPI,
        squeeze=False,
        subplot_kw={"aspect": "equal"},
    )
    axes_arr = axes.ravel()
    qa_rows = []
    for ax, bundle in zip(axes_arr, bundles):
        qa = _plot_donut(ax, bundle)
        qa_rows.append({"fish_id": bundle["fish_id"], "n_total": int(bundle["total_n"]), **qa})

    counts_df = pd.concat([bundle["counts_long"] for bundle in bundles], ignore_index=True)
    counts_csv = outdir / "cohort_50l_global_activity_donut_counts_by_fish.csv"
    counts_df.to_csv(counts_csv, index=False)
    counts_wide_df = pd.DataFrame(
        [
            {
                "owner": bundle["owner"],
                "fish_id": bundle["fish_id"],
                "n_total": int(bundle["total_n"]),
                **{f"inner__{label}": int(bundle["response_counts_full"].set_index("response_summary_class").loc[label, "n_rois"]) for label in COHORT_50L_RESPONSE_ORDER},
                **{f"outer__{label}": int(bundle["bpi_counts_full"].set_index("bpi_category").loc[label, "n_rois_total"]) for label in COHORT_50L_BPI_ORDER},
            }
            for bundle in bundles
        ]
    )

    from matplotlib.patches import Patch
    legend_handles = [Patch(facecolor=COHORT_50L_BPI_COLORS[label], edgecolor="none", label=COHORT_50L_BPI_SHORT[label]) for label in COHORT_50L_BPI_ORDER]
    legend = fig.legend(
        handles=legend_handles,
        loc="lower center",
        bbox_to_anchor=(0.5, 0.01),
        ncol=len(COHORT_50L_BPI_ORDER),
        frameon=False,
        fontsize=COHORT_50L_LEGEND_FONTSIZE,
        title="Outer ring: stimulus bias",
        title_fontsize=COHORT_50L_LEGEND_FONTSIZE,
    )
    legend._legend_box.align = "left"
    fig.suptitle("Cohort [50l] global activity distribution by fish", y=0.98, fontsize=COHORT_50L_TITLE_FONTSIZE, fontweight="bold")
    fig.tight_layout(rect=[0.01, 0.11, 0.99, 0.93])
    fig_path = outdir / "cohort_50l_global_activity_donut_row_by_fish.png"
    fig_pdf = outdir / "cohort_50l_global_activity_donut_row_by_fish.pdf"
    fig.savefig(fig_path, dpi=COHORT_50L_DPI, bbox_inches="tight")
    fig.savefig(fig_pdf, bbox_inches="tight")

    spot_bundle = bundles[0]
    spot_response_counts, spot_bpi_counts = _direct_50l_counts(spot_bundle["master_csv"], spot_bundle["fish_id"])
    pd.testing.assert_frame_equal(
        spot_bundle["response_counts_plot"].reset_index(drop=True),
        spot_response_counts.reset_index(drop=True),
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        spot_bundle["bpi_counts_plot"].reset_index(drop=True),
        spot_bpi_counts.reset_index(drop=True),
        check_dtype=False,
    )
    expected_legend_labels = [COHORT_50L_BPI_SHORT[label] for label in COHORT_50L_BPI_ORDER]
    legend_labels = [handle.get_label() for handle in legend_handles]
    if legend_labels != expected_legend_labels:
        raise RuntimeError(f"[cohort-50l-donut] Legend labels do not match [50l] order: {legend_labels}")
    legend_colors = [tuple(handle.get_facecolor()) for handle in legend_handles]
    expected_legend_colors = [tuple(mcolors.to_rgba(COHORT_50L_BPI_COLORS[label])) for label in COHORT_50L_BPI_ORDER]
    if legend_colors != expected_legend_colors:
        raise RuntimeError("[cohort-50l-donut] Legend colors do not match [50l] colors.")
    qa_df = pd.DataFrame(qa_rows)
    if len(axes_arr) != 4:
        raise RuntimeError(f"[cohort-50l-donut] Expected 4 donut axes, found {len(axes_arr)}.")
    if int(qa_df["n_outer_text_labels"].sum()) <= 0:
        raise RuntimeError("[cohort-50l-donut] Outer-ring text labels were not drawn.")
    if "outside_label_overlap_pairs" in qa_df.columns and int(qa_df["outside_label_overlap_pairs"].sum()) > 0:
        raise RuntimeError("[cohort-50l-donut] Outside-label layout still contains vertical overlaps.")
    if not fig_path.exists():
        raise RuntimeError(f"[cohort-50l-donut] Figure was not written: {fig_path}")
    if not counts_csv.exists():
        raise RuntimeError(f"[cohort-50l-donut] Counts CSV was not written: {counts_csv}")

    return {
        "fig": fig,
        "out_path": fig_path,
        "pdf_path": fig_pdf,
        "counts_csv": counts_csv,
        "counts_df": counts_df,
        "counts_wide_df": counts_wide_df,
        "fish_order": [bundle["fish_id"] for bundle in bundles],
        "legend_labels": expected_legend_labels,
        "qa_df": qa_df,
    }


__all__ = [
    "STIM_PALETTE",
    "compute_laterality",
    "compute_trial_auc",
    "plot_single_roi_57style",
    "render_cohort_56h_by_fish",
    "render_cohort_56g_diagnostics",
    "render_cohort_motion_auc",
    "render_cohort_56h_status_donut_grid",
    "render_cohort_50l_donut_row",
]
