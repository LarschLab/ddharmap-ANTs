"""Lightweight HCR-centric figure builders."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
from matplotlib import colors as mcolors
import numpy as np
import pandas as pd


IN_PLANE_HCR_QC_STATUSES = {
    "in-plane responsive ROI",
    "in-plane low-activity ROI",
    "in-plane response unavailable",
    "in-plane no functional ROI candidate",
}


def _as_bool_series(values: Any) -> pd.Series:
    series = pd.Series(values)
    if pd.api.types.is_bool_dtype(series):
        return series.fillna(False).astype(bool)
    if pd.api.types.is_numeric_dtype(series):
        return series.fillna(0).astype(float) != 0
    return series.astype(str).str.strip().str.lower().isin({"1", "true", "t", "yes", "y"})


def _select_in_plane_hcr_status(status_df: pd.DataFrame) -> pd.DataFrame:
    if status_df.empty:
        return status_df.copy()
    work = status_df.copy()
    work["gene"] = work.get("gene", pd.Series(["unknown"] * len(work), index=work.index)).astype(str).str.strip()
    work["anat_label"] = pd.to_numeric(work.get("anat_label", np.nan), errors="coerce").astype("Int64")
    represented = _as_bool_series(work.get("represented_on_func_plane", pd.Series(False, index=work.index)))
    functional_status = work.get("functional_status", pd.Series("", index=work.index)).astype(str).str.strip()
    work = work[represented & functional_status.isin(IN_PLANE_HCR_QC_STATUSES)].copy()
    work = work.dropna(subset=["gene", "anat_label"]).copy()
    if work.empty:
        return work
    work["dist_conf"] = pd.to_numeric(work.get("dist_conf_anat_um", work.get("dist_conf", np.nan)), errors="coerce")
    work["dist_func"] = pd.to_numeric(work.get("selected_dist_um", np.nan), errors="coerce")
    work["overlap"] = pd.to_numeric(work.get("selected_overlap_px", work.get("overlap_px_func_anat", np.nan)), errors="coerce")
    work["plane_sort"] = pd.to_numeric(work.get("selected_plane", np.nan), errors="coerce")
    work["func_sort"] = pd.to_numeric(work.get("selected_func_label", np.nan), errors="coerce")
    work = work.sort_values(
        ["gene", "anat_label", "dist_conf", "dist_func", "overlap", "plane_sort", "func_sort"],
        ascending=[True, True, True, True, False, True, True],
        na_position="last",
    ).reset_index(drop=True)
    return work.drop_duplicates(subset=["gene", "anat_label"], keep="first").reset_index(drop=True)


def render_single_fish_hcr_anatomy_coexpression_summary(
    *,
    fish_id: str,
    status_csv: str | Path,
    outdir: str | Path,
    gene_order: list[str] | None = None,
    gene_colors: dict[str, str] | None = None,
) -> dict[str, Any]:
    fish_id_s = str(fish_id)
    status_csv_p = Path(status_csv)
    if not status_csv_p.exists():
        raise RuntimeError(f"[single-fish-hcr-anatomy-coexpression] Missing HCR status table: {status_csv_p}")

    status_df = pd.read_csv(status_csv_p)
    if "fish_id" in status_df.columns:
        status_df = status_df[status_df["fish_id"].astype(str) == fish_id_s].copy()
    if status_df.empty:
        raise RuntimeError(f"[single-fish-hcr-anatomy-coexpression] {fish_id_s}: HCR status table is empty")

    default_gene_order = ["sst1.1", "sst1.2", "npy", "tac3b", "pth2", "cfos", "cort"]
    default_gene_colors = {
        "sst1.1": "#d62728",
        "sst1.2": "#d61ad2",
        "npy": "#1f9d55",
        "tac3b": "#ffd400",
        "pth2": "#00bcd4",
        "cfos": "#ff7f0e",
        "cort": "#8c564b",
    }
    gene_order_local = list(gene_order or default_gene_order)
    gene_colors_local = {**default_gene_colors, **{str(k): str(v) for k, v in (gene_colors or {}).items()}}
    gene_rank = {str(g): idx for idx, g in enumerate(gene_order_local)}

    def ordered_genes(values: Any) -> tuple[str, ...]:
        return tuple(sorted({str(v).strip() for v in values if str(v).strip()}, key=lambda g: (gene_rank.get(g, 10**6), g)))

    def combo_sort_key(label: str) -> tuple[int, tuple[Any, ...]]:
        parts = [part for part in str(label).split("/") if part]
        return (len(parts), tuple((gene_rank.get(part, 10**6), part) for part in parts))

    def combo_color(label: str) -> tuple[float, float, float]:
        parts = [part for part in str(label).split("/") if part]
        if not parts:
            return mcolors.to_rgb("#bdbdbd")
        rgb = np.asarray([mcolors.to_rgb(gene_colors_local.get(part, "#777777")) for part in parts], dtype=float).mean(axis=0)
        return tuple(np.clip(0.88 * rgb + 0.12 * np.ones(3, dtype=float), 0.0, 1.0))

    in_plane = _select_in_plane_hcr_status(status_df)
    if "fish_id" in in_plane.columns:
        in_plane = in_plane[in_plane["fish_id"].astype(str) == fish_id_s].copy()
    if in_plane.empty:
        summary_df = pd.DataFrame(
            columns=["fish_id", "anat_label", "n_genes", "gene_combo_label", "is_putative_coexpression"]
        )
    else:
        summary_df = (
            in_plane.groupby("anat_label", as_index=False)["gene"]
            .agg(ordered_genes)
            .rename(columns={"gene": "gene_tuple"})
        )
        summary_df["fish_id"] = fish_id_s
        summary_df["n_genes"] = summary_df["gene_tuple"].map(len).astype(int)
        summary_df["gene_combo_label"] = summary_df["gene_tuple"].map(lambda tup: "/".join(tup))
        summary_df["is_putative_coexpression"] = summary_df["n_genes"] > 1
        summary_df = summary_df[["fish_id", "anat_label", "n_genes", "gene_combo_label", "is_putative_coexpression"]].copy()
        summary_df["anat_label"] = pd.to_numeric(summary_df["anat_label"], errors="coerce").astype("Int64")
        summary_df = summary_df.sort_values(
            ["is_putative_coexpression", "n_genes", "gene_combo_label", "anat_label"],
            ascending=[False, False, True, True],
            na_position="last",
        ).reset_index(drop=True)

    combo_counts_df = (
        summary_df[summary_df["is_putative_coexpression"].astype(bool)]
        .groupby(["gene_combo_label", "n_genes"], as_index=False)
        .size()
        .rename(columns={"size": "n_anatomy_labels"})
    )
    if combo_counts_df.empty:
        combo_counts_df = pd.DataFrame(columns=["fish_id", "gene_combo_label", "n_genes", "n_anatomy_labels"])
    else:
        combo_counts_df["fish_id"] = fish_id_s
        combo_counts_df["sort_key"] = combo_counts_df["gene_combo_label"].map(combo_sort_key)
        combo_counts_df = combo_counts_df.sort_values(
            ["n_anatomy_labels", "n_genes", "sort_key"],
            ascending=[False, False, True],
        ).drop(columns=["sort_key"]).reset_index(drop=True)
        combo_counts_df = combo_counts_df[["fish_id", "gene_combo_label", "n_genes", "n_anatomy_labels"]].copy()

    bucket_counts_df = pd.DataFrame(
        [
            {"fish_id": fish_id_s, "marker_count_bucket": "1", "n_anatomy_labels": int((summary_df.get("n_genes", pd.Series(dtype=int)) == 1).sum())},
            {"fish_id": fish_id_s, "marker_count_bucket": "2", "n_anatomy_labels": int((summary_df.get("n_genes", pd.Series(dtype=int)) == 2).sum())},
            {"fish_id": fish_id_s, "marker_count_bucket": "3+", "n_anatomy_labels": int((summary_df.get("n_genes", pd.Series(dtype=int)) >= 3).sum())},
        ]
    )

    fig, axes = plt.subplots(1, 2, figsize=(11.0, 4.2), constrained_layout=True)
    ax_bucket, ax_combo = axes
    bucket_palette = {"1": "#d9d9d9", "2": "#8da0cb", "3+": "#fc8d62"}
    ax_bucket.bar(
        bucket_counts_df["marker_count_bucket"].astype(str),
        bucket_counts_df["n_anatomy_labels"].astype(int),
        color=[bucket_palette[str(v)] for v in bucket_counts_df["marker_count_bucket"].astype(str)],
        edgecolor="#444444",
        linewidth=0.8,
    )
    ax_bucket.set_ylim(0.0, max(1, int(bucket_counts_df["n_anatomy_labels"].max())) + 1.0)
    ax_bucket.set_ylabel("Anatomy labels")
    ax_bucket.set_title("Marker count per in-plane anatomy label", fontsize=11)
    ax_bucket.spines["top"].set_visible(False)
    ax_bucket.spines["right"].set_visible(False)
    for row in bucket_counts_df.itertuples(index=False):
        ax_bucket.text(str(row.marker_count_bucket), float(row.n_anatomy_labels) + 0.05, str(int(row.n_anatomy_labels)), ha="center", va="bottom", fontsize=9)

    if combo_counts_df.empty:
        ax_combo.axis("off")
        ax_combo.text(0.5, 0.55, "No multi-marker\nin-plane anatomy labels", ha="center", va="center", fontsize=11, fontweight="bold")
        ax_combo.text(0.5, 0.33, "Possible co-expression was not detected in this fish.", ha="center", va="center", fontsize=9, color="#555555")
    else:
        combo_plot = combo_counts_df.iloc[::-1].reset_index(drop=True)
        y_pos = np.arange(len(combo_plot), dtype=float)
        ax_combo.barh(
            y_pos,
            combo_plot["n_anatomy_labels"].astype(int),
            color=[combo_color(label) for label in combo_plot["gene_combo_label"].astype(str)],
            edgecolor="#444444",
            linewidth=0.8,
        )
        ax_combo.set_yticks(y_pos)
        ax_combo.set_yticklabels(combo_plot["gene_combo_label"].astype(str), fontsize=9)
        ax_combo.set_xlim(0.0, max(1, int(combo_plot["n_anatomy_labels"].max())) + 1.0)
        ax_combo.set_xlabel("Anatomy labels")
        ax_combo.set_title("Exact multi-marker combinations", fontsize=11)
        ax_combo.spines["top"].set_visible(False)
        ax_combo.spines["right"].set_visible(False)
        for idx, row in combo_plot.iterrows():
            ax_combo.text(float(row["n_anatomy_labels"]) + 0.05, y_pos[idx], str(int(row["n_anatomy_labels"])), va="center", ha="left", fontsize=9)

    n_total = int(len(summary_df))
    n_multi = int(summary_df["is_putative_coexpression"].astype(bool).sum()) if "is_putative_coexpression" in summary_df.columns else 0
    fig.suptitle(
        f"Possible marker co-expression is summarized at the anatomy-label level (in-plane labels n={n_total}, multi-marker n={n_multi})",
        y=1.02,
        fontsize=12,
        fontweight="bold",
    )

    outdir_p = Path(outdir)
    outdir_p.mkdir(parents=True, exist_ok=True)
    out_path = outdir_p / "single_fish_hcr_anatomy_coexpression_summary.png"
    pdf_path = outdir_p / "single_fish_hcr_anatomy_coexpression_summary.pdf"
    summary_csv = outdir_p / "single_fish_hcr_anatomy_coexpression_summary.csv"
    combo_counts_csv = outdir_p / "single_fish_hcr_anatomy_coexpression_combo_counts.csv"
    fig.savefig(out_path, dpi=300, bbox_inches="tight")
    fig.savefig(pdf_path, bbox_inches="tight")
    summary_df.to_csv(summary_csv, index=False)
    combo_counts_df.to_csv(combo_counts_csv, index=False)

    return {
        "fig": fig,
        "out_path": out_path,
        "pdf_path": pdf_path,
        "summary_csv": summary_csv,
        "combo_counts_csv": combo_counts_csv,
        "summary_df": summary_df,
        "combo_counts_df": combo_counts_df,
        "bucket_counts_df": bucket_counts_df,
        "fish_id": fish_id_s,
    }


__all__ = ["render_single_fish_hcr_anatomy_coexpression_summary"]
