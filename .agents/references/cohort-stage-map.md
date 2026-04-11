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

## Cohort-owned outputs

- Canonical cohort output root:
  - `cohort_outputs/multi_fish_56h_56g/`
- Build/cache stage writes cohort-level CSV/PKL artifacts under that directory.
- Downstream cohort figure stages read those artifacts; they should not redefine cohort cache semantics.

## Semantic boundary

- The cohort notebook is a downstream multi-fish consumer of single-fish canonical outputs.
- It does **not** redefine single-fish table semantics (identity, response, BPI authority remains in single-fish writer stages/docs).

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
- `[cohort-50l-donut-row]` is package-renderer driven via `codeants_2pf_hcr.plots.analysis.render_cohort_50l_donut_row`.
- Late cells are thin wrappers: explicit knobs, one context/cache load (`resolve_cohort_context_stage` / `load_cohort_analysis_state`), one renderer call, optional save/display.
