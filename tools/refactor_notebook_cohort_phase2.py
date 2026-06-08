#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path


NB_PATH = Path("notebooks/multi_fish_56h_56g.ipynb")


REPLACEMENTS = {
    "53a-cohort": """# [53a-cohort] Cohort 2x2 [53a]-analogue summary figure from cached cohort tables
from codeants_2pf_hcr.plots.qa import render_cohort_53a_summary

_cohort_cache_paths = cohort_cache_paths(COHORT_OUTDIR)
_ncc_curves_df = pd.read_csv(_cohort_cache_paths["cohort_53a_ncc_curves_csv"])
_diameters_df = pd.read_csv(_cohort_cache_paths["cohort_53a_diameters_csv"])
_diameter_filter_summary_df = pd.read_csv(_cohort_cache_paths["cohort_53a_diameter_filter_summary_csv"])
_func_anat_offsets_df = pd.read_csv(_cohort_cache_paths["cohort_53a_func_anat_offsets_csv"])
_hcr_offsets_df = pd.read_csv(_cohort_cache_paths["cohort_53a_hcr_offsets_csv"])
_thresholds_df = pd.read_csv(_cohort_cache_paths["cohort_53a_thresholds_csv"])

fig_53a_cohort = render_cohort_53a_summary(
    ncc_curves_df=_ncc_curves_df,
    diameters_df=_diameters_df,
    diameter_filter_summary_df=_diameter_filter_summary_df,
    func_anat_offsets_df=_func_anat_offsets_df,
    hcr_offsets_df=_hcr_offsets_df,
    thresholds_df=_thresholds_df,
    gene_order=GENE_ORDER,
    gene_colors=GENE_COLORS,
)

cohort_53a_png = COHORT_OUTDIR / "cohort_53a_summary.png"
cohort_53a_pdf = COHORT_OUTDIR / "cohort_53a_summary.pdf"
fig_53a_cohort.savefig(cohort_53a_png, dpi=300, bbox_inches="tight")
fig_53a_cohort.savefig(cohort_53a_pdf, dpi=300, bbox_inches="tight")
display(fig_53a_cohort)
plt.close(fig_53a_cohort)
print(f"[53a-cohort] saved: {cohort_53a_png}")
print(f"[53a-cohort] saved: {cohort_53a_pdf}")
""",
    "56h-cohort": """# [56h-cohort] Cohort per-gene traces by fish (from [56h]-style outputs)
from codeants_2pf_hcr.plots.analysis import render_cohort_56h_by_fish
from codeants_2pf_hcr import count_trace_genes, load_cohort_analysis_state

state_bindings = {}
if not all(k in globals() for k in [
    "cohort_bpi_cells_df",
    "cohort_results_stim_ipsi_contra",
    "cohort_results_stim_ipsi_contra_by_fish",
    "cohort_results_stim_ipsi_contra_per_cell_by_fish",
    "cohort_tvec",
]):
    state_bindings = load_cohort_analysis_state(
        COHORT_BUILD_CONFIG,
        outdir=COHORT_OUTDIR,
        load_tables=True,
        load_trace_cache=True,
        load_cohort_53a_tables=False,
        verbose=True,
    )["bindings"]

cell_df = state_bindings.get("cohort_bpi_cells_df", cohort_bpi_cells_df)
results = state_bindings.get("cohort_results_stim_ipsi_contra", cohort_results_stim_ipsi_contra)
results_by_fish = state_bindings.get("cohort_results_stim_ipsi_contra_by_fish", cohort_results_stim_ipsi_contra_by_fish)
results_per_cell_by_fish = state_bindings.get("cohort_results_stim_ipsi_contra_per_cell_by_fish", cohort_results_stim_ipsi_contra_per_cell_by_fish)
tvec = state_bindings.get("cohort_tvec", cohort_tvec)
mode_durations = state_bindings.get("cohort_mode_durations", cohort_mode_durations)
cohort_fish_summary_local = state_bindings.get("cohort_fish_summary_df", cohort_fish_summary_df)

if not isinstance(results_per_cell_by_fish, dict) or (count_trace_genes(results_per_cell_by_fish, PLOT_ORDER, nested=True) == 0):
    raise RuntimeError("cohort_results_stim_ipsi_contra_per_cell_by_fish missing or stale; rerun [cohort-build] with FORCE_COHORT_BUILD=True.")

render_56h = render_cohort_56h_by_fish(
    cell_df=cell_df,
    results=results,
    results_by_fish=results_by_fish,
    results_per_cell_by_fish=results_per_cell_by_fish,
    tvec=tvec,
    mode_durations=mode_durations,
    cohort_fish_summary_df=cohort_fish_summary_local,
    gene_order=GENE_ORDER,
    gene_colors=GENE_COLORS,
    plot_order=PLOT_ORDER,
    plot_titles=PLOT_TITLES,
    min_segments=MIN_SEGMENTS,
    min_cells=MIN_CELLS,
    out_path=COHORT_OUTDIR / "cohort_56h_per_gene_by_fish.png",
)
FIG_56H_COHORT_LAST = render_56h["fig"]
try:
    FIG_56H_COHORT_LAST.canvas.draw()
    FIG_56H_COHORT_RGBA = np.asarray(FIG_56H_COHORT_LAST.canvas.buffer_rgba()).copy()
except Exception:
    FIG_56H_COHORT_RGBA = None
plt.show()
""",
    "56h-cohort-poster": """# [56h-cohort-poster] Fish-averaged single-row poster traces
from codeants_2pf_hcr.plots.analysis import render_cohort_56h_fish_average_poster_traces
from codeants_2pf_hcr import count_trace_genes, load_cohort_analysis_state

POSTER_56H_GENE_COLORS = {
    "sst1.1": "#d62728",
    "sst1.2": "#008000",
    "cfos": "#003f8c",
    "pth2": "#00bcd4",
    "npy": "#d61ad2",
    "tac3b": "#ffd400",
}
POSTER_56H_GENE_ORDER = ["sst1.1", "sst1.2", "cfos", "pth2", "npy", "tac3b"]

state_bindings = {}
if not all(k in globals() for k in [
    "cohort_results_stim_ipsi_contra_by_fish",
    "cohort_tvec",
]):
    state_bindings = load_cohort_analysis_state(
        COHORT_BUILD_CONFIG,
        outdir=COHORT_OUTDIR,
        load_tables=True,
        load_trace_cache=True,
        load_cohort_53a_tables=False,
        verbose=True,
    )["bindings"]

results_by_fish = state_bindings.get("cohort_results_stim_ipsi_contra_by_fish", globals().get("cohort_results_stim_ipsi_contra_by_fish", {}))
tvec = state_bindings.get("cohort_tvec", globals().get("cohort_tvec", np.asarray([], dtype=np.float32)))
mode_durations = state_bindings.get("cohort_mode_durations", globals().get("cohort_mode_durations", {}))
cohort_fish_summary_local = state_bindings.get("cohort_fish_summary_df", globals().get("cohort_fish_summary_df", pd.DataFrame()))

if not isinstance(results_by_fish, dict) or (count_trace_genes(results_by_fish, PLOT_ORDER, nested=True) == 0):
    raise RuntimeError("cohort_results_stim_ipsi_contra_by_fish missing or stale; rerun [cohort-build] with FORCE_COHORT_BUILD=True.")

render_56h_poster = render_cohort_56h_fish_average_poster_traces(
    results_by_fish=results_by_fish,
    tvec=tvec,
    mode_durations=mode_durations,
    cohort_fish_summary_df=cohort_fish_summary_local,
    gene_order=POSTER_56H_GENE_ORDER,
    gene_colors=POSTER_56H_GENE_COLORS,
    plot_order=PLOT_ORDER,
    plot_titles=PLOT_TITLES,
    min_segments=MIN_SEGMENTS,
    min_cells=MIN_CELLS,
    y_limits=(-5.0, 10.0),
    out_path=COHORT_OUTDIR / "cohort_56h_fish_average_poster_traces.png",
)
COHORT_56H_POSTER_N_FISH_DF = render_56h_poster["summary_df"].copy()
FIG_56H_POSTER_LAST = render_56h_poster["fig"]
try:
    FIG_56H_POSTER_LAST.canvas.draw()
    FIG_56H_POSTER_RGBA = np.asarray(FIG_56H_POSTER_LAST.canvas.buffer_rgba()).copy()
except Exception:
    FIG_56H_POSTER_RGBA = None
display(COHORT_56H_POSTER_N_FISH_DF)
plt.show()
""",
    "56g-cohort": """# [56g-cohort] Cohort BPI/activity diagnostics (2x2)
from codeants_2pf_hcr.plots.analysis import render_cohort_56g_diagnostics
from codeants_2pf_hcr import load_cohort_analysis_state

state_bindings = {}
if "cohort_bpi_cells_df" not in globals():
    state_bindings = load_cohort_analysis_state(
        COHORT_BUILD_CONFIG,
        outdir=COHORT_OUTDIR,
        load_tables=True,
        load_trace_cache=False,
        load_cohort_53a_tables=False,
        verbose=True,
    )["bindings"]

df_56g = state_bindings.get("cohort_bpi_cells_df", cohort_bpi_cells_df)
cohort_fish_summary_local = state_bindings.get("cohort_fish_summary_df", cohort_fish_summary_df)
render_56g = render_cohort_56g_diagnostics(
    df=df_56g,
    gene_order=GENE_ORDER,
    gene_colors=GENE_COLORS,
    cohort_fish_summary_df=cohort_fish_summary_local,
    out_path=COHORT_OUTDIR / "cohort_56g.png",
)
cohort_bpi_activity_df = render_56g["cohort_bpi_activity_df"].copy()
cohort_bpi_activity_bins_df = render_56g["cohort_bpi_activity_bins_df"].copy()
FIG_56G_COHORT_LAST = render_56g["fig"]
try:
    FIG_56G_COHORT_LAST.canvas.draw()
    FIG_56G_COHORT_RGBA = np.asarray(FIG_56G_COHORT_LAST.canvas.buffer_rgba()).copy()
except Exception:
    FIG_56G_COHORT_RGBA = None
try:
    cohort_bpi_activity_breakdown_df = (
        cohort_bpi_activity_df.groupby(["gene", "interpretation"]).size().unstack(fill_value=0).reset_index()
    )
    display(cohort_bpi_activity_breakdown_df)
except Exception:
    pass
plt.show()
""",
    "cohort-auc": """# [cohort-auc] Cohort motion-window AUC pair plots (ipsi/contra × bout/continuous)
from codeants_2pf_hcr.plots.analysis import render_cohort_motion_auc
from codeants_2pf_hcr import load_cohort_analysis_state

state_bindings = {}
if not all(k in globals() for k in ["FISH_SPECS", "DATA_ROOT", "DATA_MODE", "COHORT_OUTDIR"]):
    state_bindings = load_cohort_analysis_state(
        COHORT_BUILD_CONFIG,
        outdir=COHORT_OUTDIR,
        load_tables=True,
        load_trace_cache=False,
        load_cohort_53a_tables=False,
        verbose=True,
    )["bindings"]

render_auc = render_cohort_motion_auc(
    cohort_outdir=state_bindings.get("COHORT_OUTDIR", COHORT_OUTDIR),
    fish_specs=state_bindings.get("FISH_SPECS", FISH_SPECS),
    data_root=state_bindings.get("DATA_ROOT", DATA_ROOT),
    data_mode=state_bindings.get("DATA_MODE", DATA_MODE),
    gene_order=state_bindings.get("GENE_ORDER", GENE_ORDER),
    gene_colors=state_bindings.get("GENE_COLORS", GENE_COLORS),
    cohort_fish_summary_df=state_bindings.get("cohort_fish_summary_df", globals().get("cohort_fish_summary_df", pd.DataFrame())),
    fish_order_hint=globals().get("fish_order", []),
    points_df=globals().get("cohort_motion_auc_plot_points", None),
    counts_df=globals().get("cohort_motion_auc_plot_counts", None),
    hide_global_median_labels=True,
    out_path=(state_bindings.get("COHORT_OUTDIR", COHORT_OUTDIR) / "cohort_50l_auc_ipsi_contra.png"),
)
cohort_motion_auc_plot_points = render_auc["points_df"].copy()
cohort_motion_auc_plot_counts = render_auc["counts_df"].copy()
FIG_COHORT_AUC_LAST = render_auc["fig"]
try:
    FIG_COHORT_AUC_LAST.canvas.draw()
    FIG_COHORT_AUC_RGBA = np.asarray(FIG_COHORT_AUC_LAST.canvas.buffer_rgba()).copy()
except Exception:
    FIG_COHORT_AUC_RGBA = None
plt.show()
""",
    "cohort-56h-donut-grid": """# [cohort-56h-donut-grid] Fish x Gene HCR status donuts (matches [56h] semantics)
from codeants_2pf_hcr.plots.analysis import render_cohort_56h_status_donut_grid
from codeants_2pf_hcr import load_cohort_analysis_state

state_bindings = load_cohort_analysis_state(
    COHORT_BUILD_CONFIG,
    outdir=COHORT_OUTDIR,
    load_tables=True,
    load_trace_cache=False,
    load_cohort_53a_tables=False,
    verbose=True,
)["bindings"]

render_donut_grid = render_cohort_56h_status_donut_grid(
    fish_specs=state_bindings["FISH_SPECS"],
    data_root=state_bindings["DATA_ROOT"],
    data_mode=state_bindings["DATA_MODE"],
    cohort_outdir=state_bindings["COHORT_OUTDIR"],
    cohort_fish_summary_df=state_bindings.get("cohort_fish_summary_df", pd.DataFrame()),
    gene_order=["sst1.1", "pth2", "sst1.2", "tac3b", "npy", "cfos"],
)
COHORT_HCR_DONUT_GRID_LAST = render_donut_grid["fig"]
COHORT_HCR_DONUT_COUNTS_DF = render_donut_grid["counts_df"]
COHORT_HCR_DONUT_FISH_ORDER = render_donut_grid["fish_order"]
COHORT_HCR_DONUT_GENE_ORDER = render_donut_grid["gene_order"]
try:
    COHORT_HCR_DONUT_GRID_LAST.canvas.draw()
    COHORT_HCR_DONUT_GRID_RGBA = np.asarray(COHORT_HCR_DONUT_GRID_LAST.canvas.buffer_rgba()).copy()
except Exception:
    COHORT_HCR_DONUT_GRID_RGBA = None
plt.show()
""",
    "cohort-50l-donut-row": """# [cohort-50l-donut-row] Cohort [50l] global activity donuts by fish
from codeants_2pf_hcr.plots.analysis import render_cohort_50l_donut_row
from codeants_2pf_hcr import load_cohort_analysis_state

state_bindings = load_cohort_analysis_state(
    COHORT_BUILD_CONFIG,
    outdir=COHORT_OUTDIR,
    load_tables=False,
    load_trace_cache=False,
    load_cohort_53a_tables=False,
    verbose=True,
)["bindings"]

render_donut_row = render_cohort_50l_donut_row(
    fish_specs=state_bindings["FISH_SPECS"],
    data_root=state_bindings["DATA_ROOT"],
    data_mode=state_bindings["DATA_MODE"],
    cohort_outdir=state_bindings["COHORT_OUTDIR"],
)
COHORT_50L_DONUT_ROW_LAST = render_donut_row["fig"]
COHORT_50L_DONUT_COUNTS_DF = render_donut_row["counts_df"]
COHORT_50L_DONUT_COUNTS_WIDE_DF = render_donut_row["counts_wide_df"]
COHORT_50L_DONUT_FISH_ORDER = render_donut_row["fish_order"]
COHORT_50L_DONUT_LEGEND_LABELS = render_donut_row["legend_labels"]
try:
    COHORT_50L_DONUT_ROW_LAST.canvas.draw()
    COHORT_50L_DONUT_ROW_RGBA = np.asarray(COHORT_50L_DONUT_ROW_LAST.canvas.buffer_rgba()).copy()
except Exception:
    COHORT_50L_DONUT_ROW_RGBA = None
display(COHORT_50L_DONUT_COUNTS_WIDE_DF)
plt.show()
""",
    "cohort-50l-responsive-identity-donut-row": """# [cohort-50l-responsive-identity-donut-row] Cohort responsive identity donuts by fish
from codeants_2pf_hcr.plots.analysis import render_cohort_50l_responsive_identity_donut_row
from codeants_2pf_hcr import load_cohort_analysis_state

state_bindings = load_cohort_analysis_state(
    COHORT_BUILD_CONFIG,
    outdir=COHORT_OUTDIR,
    load_tables=False,
    load_trace_cache=False,
    load_cohort_53a_tables=False,
    verbose=True,
)["bindings"]

# Manual size controls (keep rings proportional via shared donut scale)
COHORT_50L_RESPONSIVE_IDENTITY_DONUT_SCALE = 1.0
COHORT_50L_RESPONSIVE_IDENTITY_VIEW_SCALE = 1.0

render_responsive_identity_donut = render_cohort_50l_responsive_identity_donut_row(
    fish_specs=state_bindings["FISH_SPECS"],
    data_root=state_bindings["DATA_ROOT"],
    data_mode=state_bindings["DATA_MODE"],
    cohort_outdir=state_bindings["COHORT_OUTDIR"],
    gene_order=state_bindings.get("GENE_ORDER", GENE_ORDER),
    gene_colors=state_bindings.get("GENE_COLORS", GENE_COLORS),
    donut_scale=COHORT_50L_RESPONSIVE_IDENTITY_DONUT_SCALE,
    view_limit_scale=COHORT_50L_RESPONSIVE_IDENTITY_VIEW_SCALE,
)
COHORT_50L_RESPONSIVE_IDENTITY_DONUT_ROW_LAST = render_responsive_identity_donut["fig"]
COHORT_50L_RESPONSIVE_IDENTITY_DONUT_COUNTS_DF = render_responsive_identity_donut["counts_df"]
COHORT_50L_RESPONSIVE_IDENTITY_DONUT_COUNTS_WIDE_DF = render_responsive_identity_donut["counts_wide_df"]
COHORT_50L_RESPONSIVE_IDENTITY_DONUT_FISH_ORDER = render_responsive_identity_donut["fish_order"]
COHORT_50L_RESPONSIVE_IDENTITY_DONUT_IDENTITY_ORDER = render_responsive_identity_donut["identity_order"]
try:
    COHORT_50L_RESPONSIVE_IDENTITY_DONUT_ROW_LAST.canvas.draw()
    COHORT_50L_RESPONSIVE_IDENTITY_DONUT_ROW_RGBA = np.asarray(COHORT_50L_RESPONSIVE_IDENTITY_DONUT_ROW_LAST.canvas.buffer_rgba()).copy()
except Exception:
    COHORT_50L_RESPONSIVE_IDENTITY_DONUT_ROW_RGBA = None
display(COHORT_50L_RESPONSIVE_IDENTITY_DONUT_COUNTS_WIDE_DF)
plt.show()
""",
}


def _replace_source(notebook: dict, tag: str, new_source: str) -> None:
    for cell in notebook["cells"]:
        if cell.get("cell_type") != "code":
            continue
        src = "".join(cell.get("source", []))
        if src.startswith(f"# [{tag}]"):
            cell["source"] = [line + "\n" for line in new_source.rstrip("\n").split("\n")]
            return
    raise RuntimeError(f"Notebook cell [{tag}] not found")


def main() -> None:
    notebook = json.loads(NB_PATH.read_text())
    for tag, source in REPLACEMENTS.items():
        _replace_source(notebook, tag, source)
    NB_PATH.write_text(json.dumps(notebook, indent=1))


if __name__ == "__main__":
    main()
