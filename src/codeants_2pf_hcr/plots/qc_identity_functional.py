"""Read-only identity-functional review figures for QC notebook 06.

Whole-population views remain ROI-centric. Marker traces, HCR status, and the
identified-response anatomy map are explicitly HCR-centric. Responsive identity
coverage is hybrid: its denominator is the responsive ROI master and its identity
lookup comes from selected HCR pairs.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap, to_rgb
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
import numpy as np
import pandas as pd
from scipy.cluster.hierarchy import leaves_list, linkage

from ..spatial import norm01
from .analysis import render_single_fish_50l_gene_auc_panel

_ROI_KEYS = ("plane_idx", "func_label")
_GENE_COLORS = {
    "sst1.1": "#6a3d9a", "sst1.2": "#b15928", "pth2": "#1b9e77",
    "gad2": "#e6ab02", "cort": "#e7298a", "unidentified": "#bdbdbd",
}
_RESPONSE_COLORS = {
    "bout-responsive": "#2c7fb8", "continuous-responsive": "#d95f0e",
    "both-responsive": "#d946ef", "low activity": "#9e9e9e",
    "response unavailable": "#ececec",
}
_RESPONSE_MARKERS = {"bout-responsive": "o", "continuous-responsive": "s", "both-responsive": "D"}
_SIDE_COLORS = {"left": "#80b1d3", "right": "#fb8072", "midline": "#e6a817", "unknown": "#eeeeee"}
_TRACE_PAIRS = (
    ("LB_trajectory", "RB_trajectory", "LB", "RB"),
    ("LC_trajectory", "RC_trajectory", "LC", "RC"),
    ("LAB_trajectory", "RAB_trajectory", "LAB", "RAB"),
    ("Ll_RB_trajectory", "Rl_RB_trajectory", "Ll_RB", "Rl_RB"),
    ("WFCl", "WFCo", "WFCl", "WFCo"),
)


def _as_bool(series: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(series):
        return series.fillna(False).astype(bool)
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce").fillna(0).ne(0)
    return series.astype(str).str.strip().str.lower().isin({"1", "true", "t", "yes", "y"})


def _read_csv(path: str | Path, *, label: str, fish_id: str) -> tuple[Path, pd.DataFrame]:
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"Missing {label}: {resolved}")
    frame = pd.read_csv(resolved)
    if frame.empty:
        raise ValueError(f"{label} is empty: {resolved}")
    if "fish_id" in frame:
        observed = set(frame["fish_id"].dropna().astype(str).unique())
        if observed and observed != {str(fish_id)}:
            raise ValueError(f"{label} fish mismatch: expected {fish_id}, found {sorted(observed)}")
        frame = frame.loc[frame["fish_id"].astype(str).eq(str(fish_id))].copy()
    return resolved, frame


def _require(frame: pd.DataFrame, columns: Sequence[str], *, label: str) -> None:
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise ValueError(f"{label} lacks required columns {missing}")


def load_identity_functional_qc_bundle(
    *, fish_id: str, roi_master_path: str | Path, hcr_status_path: str | Path,
    hcr_pairs_path: str | Path, trace_summary_path: str | Path,
    trace_payload_path: str | Path, motion_auc_points_path: str | Path,
    motion_auc_counts_path: str | Path,
) -> dict[str, Any]:
    """Load and cross-check the persisted inputs for identity-functional review."""
    master_path, master = _read_csv(roi_master_path, label="ROI master", fish_id=fish_id)
    status_path, status = _read_csv(hcr_status_path, label="HCR activity status", fish_id=fish_id)
    pairs_path, pairs = _read_csv(hcr_pairs_path, label="selected HCR pairs", fish_id=fish_id)
    summary_path, summary = _read_csv(trace_summary_path, label="Q1 trace summary", fish_id=fish_id)
    points_path, points = _read_csv(motion_auc_points_path, label="motion AUC points", fish_id=fish_id)
    counts_path, counts = _read_csv(motion_auc_counts_path, label="motion AUC counts", fish_id=fish_id)
    _require(master, (*_ROI_KEYS, "identity_label", "response_is_active", "response_class", "bpi_category", "centroid_x_anat", "centroid_y_anat"), label="ROI master")
    _require(status, ("gene", "anat_label", "functional_status"), label="HCR activity status")
    _require(pairs, ("gene", "plane", "func_label", "response_is_active", "response_class", "is_selected_for_analysis"), label="selected HCR pairs")
    _require(summary, (*_ROI_KEYS, "session_label", "stim_type", "mean_dff_post", "any_presented_response_state"), label="Q1 trace summary")
    _require(points, ("group", "laterality", "stim_mode", "auc_dff", "accepted_midline_sidecar_sha256"), label="motion AUC points")
    _require(counts, ("group", "laterality", "stim_mode", "n_total", "accepted_midline_sidecar_sha256"), label="motion AUC counts")
    if master.duplicated(list(_ROI_KEYS)).any():
        raise ValueError("ROI master contains duplicate ROI keys")
    if summary.duplicated([*_ROI_KEYS, "stim_type"]).any():
        raise ValueError("Q1 trace summary contains duplicate ROI/stimulus keys")

    payload_path = Path(trace_payload_path).expanduser().resolve()
    if not payload_path.is_file():
        raise FileNotFoundError(f"Missing Q1 numeric trace payload: {payload_path}")
    with np.load(payload_path, allow_pickle=False) as payload:
        traces = np.asarray(payload["traces"], dtype=np.float32)
        tvec = np.asarray(payload["tvec"], dtype=np.float32)
        summary_digest = str(payload["summary_sha256"].item())
        metadata = json.loads(str(payload["plot_metadata_json"].item()))
    if summary_digest != hashlib.sha256(summary_path.read_bytes()).hexdigest():
        raise RuntimeError("Q1 trace summary and numeric payload are from different runs")
    if tvec.ndim != 1 or traces.shape != (len(summary), len(tvec)):
        raise RuntimeError("Q1 numeric trace payload does not align with its summary")
    summary = summary.copy()
    summary["mean_trace"] = list(traces)
    pairs = pairs.loc[_as_bool(pairs["is_selected_for_analysis"])].copy()
    pairs["plane_idx"] = pd.to_numeric(pairs["plane"], errors="coerce")
    pairs["func_label"] = pd.to_numeric(pairs["func_label"], errors="coerce")
    if pairs[["plane_idx", "func_label"]].isna().any(axis=None):
        raise ValueError("Selected HCR pairs contain invalid functional ROI keys")
    pairs[["plane_idx", "func_label"]] = pairs[["plane_idx", "func_label"]].astype(int)
    return {
        "fish_id": str(fish_id), "master": master, "hcr_status": status,
        "hcr_pairs": pairs, "trace_summary": summary, "tvec": tvec,
        "trace_metadata": metadata, "motion_auc_points": points,
        "motion_auc_counts": counts, "read_only": True,
        "paths": {
            "master": master_path, "hcr_status": status_path, "hcr_pairs": pairs_path,
            "trace_summary": summary_path, "trace_payload": payload_path,
            "motion_auc_points": points_path, "motion_auc_counts": counts_path,
        },
    }


def build_population_response_matrix(
    bundle: Mapping[str, Any], *, midline_rows: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """Build a complete ROI-by-stimulus matrix and hierarchical ordering."""
    summary = pd.DataFrame(bundle["trace_summary"]).copy()
    master = pd.DataFrame(bundle["master"]).copy()
    # Keep left/right members adjacent; metadata order reflects file discovery.
    stim_order = [stim for pair in _TRACE_PAIRS for stim in pair[:2]]
    summary["_value"] = pd.to_numeric(summary["mean_dff_post"], errors="coerce")
    pivot = summary.pivot(index=list(_ROI_KEYS), columns="stim_type", values="_value")
    missing = sorted(set(stim_order) - set(pivot.columns))
    if missing:
        raise ValueError(f"Q1 response matrix lacks stimuli {missing}")
    pivot = pivot[stim_order]
    complete = pivot.notna().all(axis=1)
    dropped = int((~complete).sum())
    pivot = pivot.loc[complete].copy()
    if len(pivot) < 2:
        raise ValueError("Fewer than two complete ROI response profiles are available")
    raw = pivot.to_numpy(dtype=float)
    row_mean, row_sd = raw.mean(axis=1, keepdims=True), raw.std(axis=1, keepdims=True)
    normalized = np.divide(raw - row_mean, row_sd, out=np.zeros_like(raw), where=row_sd > 1e-12)
    order = leaves_list(linkage(normalized, method="ward", optimal_ordering=False))
    annotations = pivot.reset_index()[list(_ROI_KEYS)].merge(
        master[[*_ROI_KEYS, "identity_label", "response_class", "response_is_active"]],
        on=list(_ROI_KEYS), how="left", validate="one_to_one",
    )
    if midline_rows is not None:
        _require(midline_rows, (*_ROI_KEYS, "midline_side"), label="midline ROI table")
        side = midline_rows[[*_ROI_KEYS, "midline_side"]].drop_duplicates(list(_ROI_KEYS))
        annotations = annotations.merge(side, on=list(_ROI_KEYS), how="left", validate="one_to_one")
    else:
        annotations["midline_side"] = "unknown"
    annotations["identity_bucket"] = annotations["identity_label"].fillna("unidentified").astype(str)
    annotations["response_class"] = annotations["response_class"].fillna("response unavailable").astype(str).str.lower()
    annotations["midline_side"] = annotations["midline_side"].fillna("unknown").astype(str).str.lower()
    return {
        "normalized": normalized[order], "raw": raw[order],
        "annotations": annotations.iloc[order].reset_index(drop=True),
        "stim_order": stim_order, "n_complete": len(order), "n_dropped_incomplete": dropped,
    }


def render_population_response_raster(matrix_bundle: Mapping[str, Any], *, fish_id: str) -> plt.Figure:
    """Render the all-cell similarity raster with identity/response/side strips."""
    matrix = np.asarray(matrix_bundle["normalized"], dtype=float)
    raw = np.asarray(matrix_bundle["raw"], dtype=float)
    annotations = pd.DataFrame(matrix_bundle["annotations"])
    stimuli = [str(v).replace("_trajectory", "") for v in matrix_bundle["stim_order"]]
    identities = list(dict.fromkeys([*_GENE_COLORS, *sorted(set(annotations["identity_bucket"]) - set(_GENE_COLORS))]))
    identity_colors = [_GENE_COLORS.get(v, _combo_color(v)) for v in identities]
    response_names, side_names = list(_RESPONSE_COLORS), list(_SIDE_COLORS)
    strips_data = (
        (annotations["identity_bucket"].map({v: i for i, v in enumerate(identities)}), identity_colors, "Identity"),
        (annotations["response_class"].map({v: i for i, v in enumerate(response_names)}).fillna(response_names.index("response unavailable")), list(_RESPONSE_COLORS.values()), "Response"),
        (annotations["midline_side"].map({v: i for i, v in enumerate(side_names)}).fillna(side_names.index("unknown")), list(_SIDE_COLORS.values()), "Side"),
    )
    fig = plt.figure(figsize=(15.2, 8.4))
    fig.subplots_adjust(top=0.86, bottom=0.18, left=0.045, right=0.985, wspace=0.22)
    grid = fig.add_gridspec(1, 5, width_ratios=(0.23, 0.23, 0.23, 5.0, 1.15))
    for index, (values, colors, title) in enumerate(strips_data):
        ax = fig.add_subplot(grid[0, index])
        ax.imshow(values.to_numpy()[:, None], aspect="auto", interpolation="nearest", cmap=ListedColormap(colors), vmin=0, vmax=max(1, len(colors) - 1))
        ax.set(xticks=[], yticks=[], title=title)
    ax = fig.add_subplot(grid[0, 3])
    vmax = max(1.0, float(np.nanpercentile(np.abs(matrix), 99)))
    image = ax.imshow(matrix, aspect="auto", interpolation="nearest", cmap="RdBu_r", vmin=-vmax, vmax=vmax)
    ax.set_xticks(np.arange(len(stimuli)), stimuli, rotation=35, ha="right")
    ax.set_yticks([0, len(matrix) - 1], ["1", str(len(matrix))])
    ax.set(xlabel="All recorded stimulus types")
    fig.colorbar(image, ax=ax, shrink=0.74, pad=0.01, label="Within-ROI normalized mean response")
    magnitude_ax = fig.add_subplot(grid[0, 4])
    magnitude = np.maximum(raw, 0).mean(axis=1)
    magnitude_ax.barh(np.arange(len(magnitude)), magnitude, height=1.0, color="#6baed6")
    magnitude_ax.set_ylim(len(magnitude) - 0.5, -0.5)
    magnitude_ax.set_yticks([])
    magnitude_ax.set_xlabel("Mean positive raw\nresponse companion")
    magnitude_ax.spines[["top", "right"]].set_visible(False)
    fig.suptitle(f"{fish_id}: population response structure before marker-specific interpretation", fontsize=15, fontweight="bold", y=0.975)
    fig.text(0.5, 0.915, f"n={matrix_bundle['n_complete']} complete Suite2p-cell profiles; annotations do not determine ordering", ha="center", fontsize=9.5)
    handles = [Patch(color=_GENE_COLORS.get(value, _combo_color(value)), label=value) for value in identities]
    handles += [Patch(color=_RESPONSE_COLORS[k], label=k) for k in ("bout-responsive", "continuous-responsive", "both-responsive")]
    fig.legend(handles=handles, loc="lower center", bbox_to_anchor=(0.5, 0.025), ncol=min(10, len(handles)), frameon=False, fontsize=8)
    return fig


def build_marker_trace_rows(bundle: Mapping[str, Any]) -> pd.DataFrame:
    """Join selected HCR-centric responsive pairs to the hash-bound Q1 traces."""
    pairs = pd.DataFrame(bundle["hcr_pairs"]).copy()
    pairs = pairs.loc[_as_bool(pairs["response_is_active"]), ["gene", *_ROI_KEYS]].drop_duplicates()
    summary = pd.DataFrame(bundle["trace_summary"])
    joined = pairs.merge(summary, on=list(_ROI_KEYS), how="left", validate="many_to_many", indicator=True)
    if joined["_merge"].ne("both").any():
        raise ValueError("Selected responsive HCR pairs are missing from the Q1 trace payload")
    joined = joined.drop(columns="_merge")
    if joined.duplicated(["gene", *_ROI_KEYS, "stim_type"]).any():
        raise ValueError("Marker trace join produced duplicate gene/ROI/stimulus rows")
    return joined


def render_marker_stimulus_traces(
    bundle: Mapping[str, Any], *, gene_order: Sequence[str] | None = None,
    xlim: tuple[float, float] = (-5.0, 35.0),
) -> tuple[plt.Figure, pd.DataFrame]:
    """Render HCR-centric individual-cell traces and per-marker means."""
    rows = build_marker_trace_rows(bundle)
    available = list(dict.fromkeys(rows["gene"].astype(str)))
    genes = [g for g in (gene_order or _GENE_COLORS) if g in available]
    genes.extend(g for g in available if g not in genes)
    if not genes:
        raise ValueError("No selected responsive HCR marker traces are available")
    tvec, metadata = np.asarray(bundle["tvec"], dtype=float), bundle["trace_metadata"]
    onset = float(metadata["motion_onset_delay_sec"])
    durations = {str(k): float(v) for k, v in metadata["duration_by_stim"].items()}
    fig, axes = plt.subplots(len(genes), len(_TRACE_PAIRS), figsize=(17.0, 2.25 * len(genes) + 2.0), sharex=True, sharey=True, squeeze=False)
    fig.subplots_adjust(top=0.86, bottom=0.11, left=0.07, right=0.985, wspace=0.12, hspace=0.23)
    left_color, right_color = "#377eb8", "#e41a1c"
    for row_index, gene in enumerate(genes):
        gene_rows = rows.loc[rows["gene"].astype(str).eq(gene)]
        n_cells = gene_rows[list(_ROI_KEYS)].drop_duplicates().shape[0]
        for col_index, (left_stim, right_stim, left_label, right_label) in enumerate(_TRACE_PAIRS):
            ax = axes[row_index, col_index]
            for stim, color in ((left_stim, left_color), (right_stim, right_color)):
                sub = gene_rows.loc[gene_rows["stim_type"].astype(str).eq(stim)]
                traces = np.stack(sub["mean_trace"].to_numpy()) if not sub.empty else np.empty((0, len(tvec)))
                for trace in traces:
                    ax.plot(tvec, trace, color=color, alpha=0.15, linewidth=0.55)
                if len(traces):
                    ax.plot(tvec, np.nanmean(traces, axis=0), color=color, linewidth=2.0)
            ax.axvline(0, color="#777777", linewidth=0.8)
            ax.axvline(onset, color="#222222", linewidth=0.9, linestyle="--")
            ax.axvspan(onset, min(durations.get(left_stim, onset), durations.get(right_stim, onset)), color="#d9d9d9", alpha=0.30)
            ax.set_xlim(*xlim)
            ax.spines[["top", "right"]].set_visible(False)
            if row_index == 0:
                ax.set_title(f"{left_label} vs {right_label}", fontsize=10, fontweight="bold")
            if col_index == 0:
                ax.set_ylabel(f"{gene} (n={n_cells})\nz-scored dF/F", color=_GENE_COLORS.get(gene, "#333333"), fontweight="bold")
            if row_index == len(genes) - 1:
                ax.set_xlabel("Time from stimulus appearance (s)")
    fig.suptitle(f"{bundle['fish_id']}: HCR-centric marker-stratified stimulus traces", fontsize=15, fontweight="bold", y=0.975)
    fig.text(0.5, 0.915, "Thin lines are selected responsive identity-linked cells; thick lines are marker means", ha="center", fontsize=9.5)
    fig.legend(handles=[
        Line2D([0], [0], color=left_color, linewidth=2, label="left-side member"),
        Line2D([0], [0], color=right_color, linewidth=2, label="right-side member"),
        Line2D([0], [0], color="#222222", linestyle="--", label="motion onset"),
    ], loc="lower center", bbox_to_anchor=(0.5, 0.02), ncol=3, frameon=False)
    return fig, rows


def render_marker_auc_review(
    bundle: Mapping[str, Any], *, accepted_midline_context: Mapping[str, Any],
    global_laterality_status: str,
    gene_order: Sequence[str] | None = None,
) -> plt.Figure:
    """Render persisted per-marker AUC points without rebuilding laterality."""
    if str(global_laterality_status) != "ready":
        raise RuntimeError("Marker AUC requires a ready Q7 global laterality provenance gate")
    points, counts = pd.DataFrame(bundle["motion_auc_points"]).copy(), pd.DataFrame(bundle["motion_auc_counts"]).copy()
    expected_hash = str(accepted_midline_context["sidecar_sha256"])
    for label, table in (("points", points), ("counts", counts)):
        hashes = set(table["accepted_midline_sidecar_sha256"].dropna().astype(str))
        if hashes != {expected_hash}:
            raise ValueError(f"Motion-AUC {label} do not match the accepted midline")
    available = [str(v) for v in points["group"].dropna().unique() if str(v) not in {"All neurons", "all_neurons"}]
    genes = [g for g in (gene_order or _GENE_COLORS) if g in available]
    genes.extend(g for g in available if g not in genes)
    if not genes:
        raise ValueError("No marker-specific AUC groups are available")
    values = pd.to_numeric(points.loc[points["group"].astype(str).isin(genes), "auc_dff"], errors="coerce").dropna().to_numpy()
    span = max(0.01, float(np.nanmax(values) - np.nanmin(values)))
    y_limits = (min(0.0, float(np.nanmin(values)) - 0.08 * span), float(np.nanmax(values)) + 0.14 * span)
    fig = plt.figure(figsize=(15.0, 7.0))
    grid = fig.add_gridspec(2, 2, height_ratios=(4.0, 1.25), hspace=0.12, wspace=0.10)
    colors = {gene: _GENE_COLORS.get(gene, "#666666") for gene in genes}
    for col, (laterality, title) in enumerate((("ipsi", "Ipsilateral"), ("contra", "Contralateral"))):
        ax, strip = fig.add_subplot(grid[0, col]), fig.add_subplot(grid[1, col])
        render_single_fish_50l_gene_auc_panel(
            ax, strip, points, counts, genes, np.arange(len(genes)), laterality, title,
            y_limits, gene_colors=colors, show_ylabel=(col == 0), show_count_ylabel=(col == 0),
            hide_y_ticklabels=(col == 1),
        )
    fig.suptitle(f"{bundle['fish_id']}: marker-specific motion-window AUC", fontsize=15, fontweight="bold")
    fig.subplots_adjust(top=0.88, bottom=0.10, left=0.08, right=0.98)
    return fig


def build_status_coverage_tables(bundle: Mapping[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Build HCR status counts and hybrid responsive identity coverage."""
    status = pd.DataFrame(bundle["hcr_status"]).copy()
    status_map = {
        "in-plane responsive roi": "responsive", "in-plane low-activity roi": "low activity",
        "in-plane response unavailable": "response unavailable",
        "in-plane no functional roi candidate": "no functional ROI",
        "out-of-plane anatomy label": "outside functional planes",
    }
    status["status_bucket"] = status["functional_status"].astype(str).str.strip().str.lower().map(status_map)
    if status["status_bucket"].isna().any():
        unknown = sorted(status.loc[status["status_bucket"].isna(), "functional_status"].astype(str).unique())
        raise ValueError(f"Unexpected HCR functional_status values {unknown}")
    status_counts = status.groupby(["gene", "status_bucket"], as_index=False).size().rename(columns={"size": "count"})
    master = pd.DataFrame(bundle["master"]).copy()
    master["response_is_active"] = _as_bool(master["response_is_active"])
    master["bpi_category"] = master["bpi_category"].astype(str).str.strip().str.lower()
    responsive_categories = {"bout-responsive", "continuous-responsive", "both-responsive"}
    responsive = master.loc[
        master["response_is_active"] & master["bpi_category"].isin(responsive_categories),
        [*_ROI_KEYS, "response_class"],
    ].copy()
    pairs = pd.DataFrame(bundle["hcr_pairs"])[["gene", *_ROI_KEYS]].drop_duplicates()
    combo = pairs.groupby(list(_ROI_KEYS))["gene"].agg(lambda values: "/".join(sorted(set(values.astype(str))))).rename("identity_bucket").reset_index()
    coverage_rows = responsive.merge(combo, on=list(_ROI_KEYS), how="left", validate="one_to_one")
    coverage_rows["identity_bucket"] = coverage_rows["identity_bucket"].fillna("unidentified")
    coverage = pd.DataFrame({
        "coverage": ["accepted HCR identity", "unidentified"],
        "count": [int(coverage_rows["identity_bucket"].ne("unidentified").sum()), int(coverage_rows["identity_bucket"].eq("unidentified").sum())],
    })
    return status_counts, coverage, coverage_rows


def render_status_coverage(
    bundle: Mapping[str, Any], *, gene_order: Sequence[str] | None = None,
) -> tuple[plt.Figure, dict[str, pd.DataFrame]]:
    status, coverage, coverage_rows = build_status_coverage_tables(bundle)
    available = list(dict.fromkeys(status["gene"].astype(str)))
    genes = [g for g in (gene_order or _GENE_COLORS) if g in available]
    genes.extend(g for g in available if g not in genes)
    buckets = ["responsive", "low activity", "response unavailable", "no functional ROI", "outside functional planes"]
    colors = ["#4daf4a", "#bdbdbd", "#80b1d3", "#fb8072", "#cab2d6"]
    fig, axes = plt.subplots(1, 2, figsize=(16.0, 7.0), gridspec_kw={"width_ratios": (1.55, 1.0)})
    fig.subplots_adjust(top=0.79, bottom=0.22, left=0.07, right=0.98, wspace=0.17)
    ax = axes[0]
    y, left = np.arange(len(genes)), np.zeros(len(genes), dtype=float)
    for bucket, color in zip(buckets, colors):
        indexed = status.loc[status["status_bucket"].eq(bucket)].set_index("gene")["count"]
        values = np.asarray([int(indexed.get(g, 0)) for g in genes], dtype=float)
        ax.barh(y, values, left=left, color=color, edgecolor="white", height=0.62, label=bucket)
        for idx, value in enumerate(values):
            if value >= 4:
                ax.text(left[idx] + value / 2, idx, str(int(value)), ha="center", va="center", fontsize=8)
        left += values
    ax.set_yticks(y, genes)
    ax.invert_yaxis()
    ax.set_xlabel("HCR anatomy labels")
    ax.set_title("A. HCR-centric functional status by marker", fontweight="bold")
    ax.spines[["top", "right"]].set_visible(False)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.16), ncol=3, frameon=False, fontsize=8)
    ax = axes[1]
    bottom, total = 0, int(coverage["count"].sum())
    coverage_colors = {"accepted HCR identity": "#1b9e77", "unidentified": "#d9d9d9"}
    for row in coverage.itertuples(index=False):
        ax.bar([0], [row.count], bottom=bottom, width=0.58, color=coverage_colors[row.coverage], edgecolor="white", label=row.coverage)
        ax.text(0, bottom + row.count / 2, f"{int(row.count)}\n({row.count / total:.0%})", ha="center", va="center", fontweight="bold")
        bottom += int(row.count)
    ax.set_xticks([0], ["Response-positive ROI population"])
    ax.set_ylabel("ROI count")
    ax.set_title("B. ROI-centric responsive identity coverage", fontweight="bold")
    ax.spines[["top", "right"]].set_visible(False)
    ax.legend(frameon=False, loc="upper right")
    ax.text(0, -0.11 * total, "Identity from selected HCR pairs; denominator remains ROI-centric", ha="center", color="#a33", fontweight="bold", fontsize=9)
    fig.suptitle(f"{bundle['fish_id']}: functional status and responsive-identity coverage", fontsize=15, fontweight="bold", y=0.975)
    fig.text(0.5, 0.905, "HCR-label fate and ROI-population coverage stay in separate panels", ha="center", fontsize=9.5)
    return fig, {"status_counts": status, "coverage": coverage, "coverage_rows": coverage_rows}


def build_identity_function_anatomy_rows(bundle: Mapping[str, Any]) -> pd.DataFrame:
    """Collapse selected responsive HCR genes to exact per-ROI combinations."""
    pairs = pd.DataFrame(bundle["hcr_pairs"]).copy()
    pairs = pairs.loc[_as_bool(pairs["response_is_active"])]
    combo = pairs.groupby(list(_ROI_KEYS))["gene"].agg(lambda values: "/".join(sorted(set(values.astype(str))))).rename("identity_bucket").reset_index()
    master = pd.DataFrame(bundle["master"])
    cols = [*_ROI_KEYS, "centroid_x_anat", "centroid_y_anat", "response_class"]
    rows = combo.merge(master[cols], on=list(_ROI_KEYS), how="left", validate="one_to_one", indicator=True)
    if rows["_merge"].ne("both").any() or rows[["centroid_x_anat", "centroid_y_anat"]].isna().any(axis=None):
        raise ValueError("Selected responsive HCR ROIs lack anatomy-grid coordinates")
    return rows.drop(columns="_merge")


def _combo_color(label: str) -> tuple[float, float, float]:
    genes = [v for v in str(label).split("/") if v]
    colors = np.asarray([to_rgb(_GENE_COLORS.get(g, "#777777")) for g in genes])
    return tuple(colors.mean(axis=0)) if len(colors) else to_rgb("#777777")


def render_identity_function_anatomy_map(
    bundle: Mapping[str, Any], *, accepted_midline_context: Mapping[str, Any],
    anatomy_zyx: np.ndarray, plane_z: Mapping[int, int],
) -> tuple[plt.Figure, pd.DataFrame]:
    """Render HCR-centric responsive exact-combination ROIs on anatomy planes."""
    rows = build_identity_function_anatomy_rows(bundle)
    planes = sorted(int(value) for value in plane_z)
    ncols, nrows = min(5, len(planes)), int(np.ceil(len(planes) / min(5, len(planes))))
    fig, axes = plt.subplots(nrows, ncols, figsize=(3.25 * ncols, 3.45 * nrows), squeeze=False)
    fig.subplots_adjust(top=0.84, bottom=0.16, left=0.02, right=0.99, wspace=0.04, hspace=0.18)
    lines = accepted_midline_context["review"]["lines_by_plane"]
    for ax, plane in zip(axes.flat, planes):
        if plane not in plane_z:
            raise ValueError(f"Missing anatomy Z mapping for functional plane {plane}")
        image = np.asarray(anatomy_zyx[int(plane_z[plane])])
        ax.imshow(norm01(image), cmap="gray")
        sub = rows.loc[rows["plane_idx"].astype(int).eq(plane)]
        for response_class, marker in _RESPONSE_MARKERS.items():
            selected = sub.loc[sub["response_class"].astype(str).str.lower().eq(response_class)]
            if selected.empty:
                continue
            ax.scatter(
                selected["centroid_x_anat"], selected["centroid_y_anat"], s=45,
                c=[_combo_color(v) for v in selected["identity_bucket"]], marker=marker,
                edgecolor="white", linewidth=0.55,
            )
        line = lines.get(str(plane), lines.get(plane))
        angle, length = np.deg2rad(float(line["theta_deg"])), max(image.shape) * 1.5
        ax.plot(
            [float(line["x0"]) - length * np.cos(angle), float(line["x0"]) + length * np.cos(angle)],
            [float(line["y0"]) - length * np.sin(angle), float(line["y0"]) + length * np.sin(angle)],
            color="#ffd23f", linewidth=1.3,
        )
        ax.set(xlim=(-0.5, image.shape[1] - 0.5), ylim=(image.shape[0] - 0.5, -0.5), title=f"plane {plane}: n={len(sub)}")
        ax.axis("off")
    for ax in axes.flat[len(planes):]:
        ax.axis("off")
    labels = sorted(set(rows["identity_bucket"].astype(str)), key=lambda value: (len(value.split("/")), value))
    handles = [Line2D([0], [0], marker="o", linestyle="", markerfacecolor=_combo_color(v), markeredgecolor="white", label=v) for v in labels]
    handles += [Line2D([0], [0], marker=m, linestyle="", color="#777777", label=k) for k, m in _RESPONSE_MARKERS.items()]
    fig.legend(handles=handles, loc="lower center", bbox_to_anchor=(0.5, 0.025), ncol=min(8, len(handles)), frameon=False, fontsize=8)
    fig.suptitle(f"{bundle['fish_id']}: HCR-centric identified responsive ROIs on the accepted anatomy midline", fontsize=14, fontweight="bold")
    return fig, rows


__all__ = [
    "load_identity_functional_qc_bundle", "build_population_response_matrix",
    "render_population_response_raster", "build_marker_trace_rows",
    "render_marker_stimulus_traces", "render_marker_auc_review",
    "build_status_coverage_tables", "render_status_coverage",
    "build_identity_function_anatomy_rows", "render_identity_function_anatomy_map",
]

