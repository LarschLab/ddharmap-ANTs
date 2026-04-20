# Cohort Notebook Stage Map

**Purpose:** navigation map for `notebooks/multi_fish_56h_56g.ipynb`.

**Use this file when:** locating cohort stage ownership, cache/output boundaries, and notebook-vs-package responsibilities.

## Stage tags (current)

- `[cfg]`
- `[helpers]`
- `[cohort-build]`
- `[53a-cohort]`
- `[56h-cohort]`
- `[56g-cohort]`
- `[cohort-auc]`
- `[cohort-56h-donut-grid]`
- `[cohort-50l-donut-row]`
- `[cohort-50l-responsive-identity-donut-row]`

## Cohort-owned outputs

- Canonical cohort output root:
  - `cohort_outputs/multi_fish_56h_56g/`
- Build/cache stage writes cohort-level CSV/PKL artifacts under that directory.
- Downstream cohort figure stages read those artifacts; they should not redefine cohort cache semantics.
- `[53a-cohort]` thresholds cache now also includes representative HCR↔anatomy XY cohort summaries:
  - `hcr_xy_median_of_fish_medians_um` (recommended representative cohort median)
  - `hcr_xy_median_pooled_um`, `hcr_xy_n_fish`, `hcr_xy_n_pairs`
- `[53a-cohort]` thresholds cache also includes representative functional↔anatomy XY cohort summaries:
  - `func_anat_xy_median_of_fish_medians_um` (recommended representative cohort median)
  - `func_anat_xy_median_pooled_um`, `func_anat_xy_n_fish`, `func_anat_xy_n_pairs`

## Semantic boundary

- The cohort notebook is a downstream multi-fish consumer of single-fish canonical outputs.
- It does **not** redefine single-fish table semantics (identity, response, BPI authority remains in single-fish writer stages/docs).
- `[53a-cohort]` is a pooled analogue of the single-fish `[53a]` HCR↔anatomy QC panel and must preserve single-fish subset semantics (in-plane represented matched labels) rather than silently switching to responsive-only trace mappings.

## Ownership split

- Build/cache and reusable aggregation logic belong in `src/codeants_2pf_hcr/` owner modules.
- Figure construction belongs in `plots.*`.
- Notebook cells should orchestrate: explicit knobs, package calls, optional display/save.

## Late-stage renderer contract (current)

- `[53a-cohort]` is package-renderer driven via `codeants_2pf_hcr.plots.qa.render_cohort_53a_summary`.
- `[56h-cohort]` is package-renderer driven via `codeants_2pf_hcr.plots.analysis.render_cohort_56h_by_fish`.
- `[56g-cohort]` is package-renderer driven via `codeants_2pf_hcr.plots.analysis.render_cohort_56g_diagnostics`.
- `[cohort-auc]` is package-renderer driven via `codeants_2pf_hcr.plots.analysis.render_cohort_motion_auc`.
- `[cohort-56h-donut-grid]` is package-renderer driven via `codeants_2pf_hcr.plots.analysis.render_cohort_56h_status_donut_grid`.
- `[cohort-56h-donut-grid]` consumes per-fish `hcr_activity_status.csv` and `[50e]` `hcr_activity_status_summary.csv` so unmatched-mask semantics remain consistent with single-fish outputs.
- `[cohort-50l-donut-row]` is package-renderer driven via `codeants_2pf_hcr.plots.analysis.render_cohort_50l_donut_row`.
- `[cohort-50l-responsive-identity-donut-row]` is package-renderer driven via `codeants_2pf_hcr.plots.analysis.render_cohort_50l_responsive_identity_donut_row`.
- `[cohort-50l-responsive-identity-donut-row]` renderer now lays out fish in a 2-row stagger (`top, bottom, top, bottom`) across columns, leaving unused grid slots blank to reduce outer-label collisions without changing counts/output contracts.
- `[cohort-50l-responsive-identity-donut-row]` notebook cell exposes manual sizing knobs (`COHORT_50L_RESPONSIVE_IDENTITY_DONUT_SCALE`, `COHORT_50L_RESPONSIVE_IDENTITY_VIEW_SCALE`) passed to renderer (`donut_scale`, `view_limit_scale`); ring widths/radii scale proportionally from `donut_scale`.
- Late cells are thin wrappers: explicit knobs, one context/cache load (`resolve_cohort_context_stage` / `load_cohort_analysis_state`), one renderer call, optional save/display.
