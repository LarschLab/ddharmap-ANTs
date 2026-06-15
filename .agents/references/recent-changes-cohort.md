# Recent Changes - Cohort Workflow

**Purpose:** rolling handoff log for meaningful cohort work, remaining breakpoints, and rerun implications.

**Use this file when:** work targets `notebooks/multi_fish_56h_56g.ipynb` or cohort ownership modules.

## Update template

### YYYY-MM-DD - short task label

- Slice goal:
  - owner-complete migration target for this session
- Passes completed in this session:
  - ordered sub-passes completed before stopping
- What changed:
  - concise description of the behavior, ownership, or workflow change
- What remains broken:
  - known failures, gaps, or deferred follow-up
- Remaining in-slice work:
  - cells, helpers, or functions still inside the same ownership slice
- Next likely breakpoint:
  - exact next stage, file, cell, test, or command to resume from or expected to fail next
- Rerun implications:
  - minimum rerun or validation sequence still needed after this change

## Notes

- Append new entries; do not rewrite unrelated history.
- Keep migration state in `current-state.md`; use this file for per-change cohort handoff detail.

### 2026-04-11 - cohort 53a representative XY median

- Slice goal:
  - expose a cohort-level representative median HCR↔anatomy XY offset for `[53a-cohort]`
- Passes completed in this session:
  - identified owning aggregation stage (`collect_cohort_53a_tables`) and downstream `[53a-cohort]` notebook consumer
  - added cohort-level HCR XY summary metrics to `thresholds_df`
  - surfaced representative median printout in notebook `[53a-cohort]` save cell
- What changed:
  - `collect_cohort_53a_tables` now writes `hcr_xy_median_of_fish_medians_um` (median of per-fish HCR XY medians) plus `hcr_xy_median_pooled_um`, `hcr_xy_n_fish`, and `hcr_xy_n_pairs` into cached `cohort_53a_thresholds.csv`
  - `notebooks/multi_fish_56h_56g.ipynb` `[53a-cohort]` now prints representative cohort median HCR→anatomy XY offset from thresholds cache after figure save
  - updated `cohort-stage-map.md` cohort output notes to document these threshold fields
- What remains broken:
  - repository baseline tests fail during collection in `tests/test_activity.py` due to unrelated import error (`prepare_pairs_for_unique_cells` from `codeants_2pf_hcr.activity`)
- Remaining in-slice work:
  - optional: add renderer-level annotation using the same representative median if panel text should carry the cohort scalar directly
- Next likely breakpoint:
  - rerun `[cohort-build]` then `[53a-cohort]` in `notebooks/multi_fish_56h_56g.ipynb` to refresh `cohort_53a_thresholds.csv` before reading the printed value
- Rerun implications:
  - minimum rerun for fresh metric: `[cohort-build]` -> `[53a-cohort]`

### 2026-04-11 - cohort 53a functional-anatomy representative XY median

- Slice goal:
  - add a representative cohort median for functional→anatomy XY offsets, analogous to HCR→anatomy
- Passes completed in this session:
  - extended `collect_cohort_53a_tables` thresholds aggregation for functional→anatomy XY metrics
  - added `[53a-cohort]` notebook printout for functional→anatomy representative median
- What changed:
  - `cohort_53a_thresholds.csv` now includes `func_anat_xy_median_of_fish_medians_um`, `func_anat_xy_median_pooled_um`, `func_anat_xy_n_fish`, and `func_anat_xy_n_pairs`
  - `[53a-cohort]` now prints representative median functional→anatomy XY offset across fish (median of fish medians), alongside fish/pair counts
  - updated `cohort-stage-map.md` to document functional↔anatomy cohort summary fields
- What remains broken:
  - repository baseline tests still fail during collection in `tests/test_activity.py` due to unrelated import error (`prepare_pairs_for_unique_cells` from `codeants_2pf_hcr.activity`)
- Remaining in-slice work:
  - optional: include these representative scalar values directly in figure annotations if desired
- Next likely breakpoint:
  - rerun `[cohort-build]` then `[53a-cohort]` to refresh thresholds cache and print the new functional→anatomy representative value
- Rerun implications:
  - minimum rerun for fresh metric: `[cohort-build]` -> `[53a-cohort]`

### 2026-04-12 - cohort 56h explicit unilateral xtick labels

- Slice goal:
  - make bottom x-axis labels in cohort trace plots explicitly state unilateral condition semantics.
- Passes completed in this session:
  - located owning package renderer (`render_cohort_56h_by_fish`) and per-gene trace notebook consumer labels.
  - updated both package and notebook label builders to emit explicit side+mode labels independent of title formatting.
- What changed:
  - `src/codeants_2pf_hcr/plots/analysis.py` now formats [56h] bottom tick labels as `Ipsi/Contra` + `bout-like/continuous` explicitly.
  - `notebooks/2PF_to_HCR.ipynb` per-gene cohort trace grid now uses explicit unilateral xtick labels rather than string-replacing title text.
- What remains broken:
  - repository test baseline still fails at collection in `tests/test_activity.py` due to unrelated missing export (`prepare_pairs_for_unique_cells` in `codeants_2pf_hcr.activity`).
- Remaining in-slice work:
  - optional harmonization: centralize single-fish/cohort unilateral label formatter to avoid drift.
- Next likely breakpoint:
  - rerun cohort [56h] plotting stage (or package caller of `render_cohort_56h_by_fish`) and inspect bottom x-axis labels in saved figures.
- Rerun implications:
  - minimum rerun: `[56h]` plotting stage for updated exports.

### 2026-04-12 - cohort AUC median labels switched to 53a-style placement

- Slice goal:
  - eliminate overlap in cohort unilateral AUC median labels by using 53a-style annotation layout.
- Passes completed in this session:
  - replaced in-panel top-band median text placement in `render_cohort_motion_auc` with above-data stacked placement.
  - added dynamic y-limit expansion so stacked labels remain fully visible.
- What changed:
  - `src/codeants_2pf_hcr/plots/analysis.py` now places `med=...` labels above the plotted AUC range, stacks upward on local x collisions, and expands y-limits to prevent label overlap/cropping.
- What remains broken:
  - repository baseline tests still fail at collection in `tests/test_activity.py` due to unrelated missing export (`prepare_pairs_for_unique_cells` in `codeants_2pf_hcr.activity`).
- Remaining in-slice work:
  - visual confirmation in downstream figure exports after rerun.
- Next likely breakpoint:
  - rerun cohort AUC plotting caller and confirm annotation readability in saved panels.
- Rerun implications:
  - minimum rerun: cohort AUC render stage that calls `render_cohort_motion_auc`.

### 2026-04-12 - shared 53a-style label placement helper for cohort medians

- Slice goal:
  - enforce consistent collision-aware median label placement between `[53a-cohort]` and `[cohort-auc]` and remove overlap drift.
- Passes completed in this session:
  - extracted shared collision-aware label placement into a package-owned plotting helper.
  - rewired `render_cohort_motion_auc` median annotation path to use the shared helper.
  - rewired `[53a-cohort]` sample-size annotation path to use the same helper behavior.
- What changed:
  - added `src/codeants_2pf_hcr/plots/annotations.py` with `place_labels_no_overlap` (53a-style stack-above with local x-collision checks and y-limit expansion).
  - `src/codeants_2pf_hcr/plots/analysis.py` now routes `med=...` labels through `place_labels_no_overlap` instead of inlined placement logic.
  - `src/codeants_2pf_hcr/plots/qa.py` now routes sample-size labels through `place_labels_no_overlap` to keep behavior consistent with AUC panel labels.
  - updated `symbol-index.md` with `codeants_2pf_hcr.plots.annotations.place_labels_no_overlap`.
- What remains broken:
  - repository baseline test collection still fails in `tests/test_activity.py` due to unrelated missing export (`prepare_pairs_for_unique_cells` in `codeants_2pf_hcr.activity`).
- Remaining in-slice work:
  - visual check in notebook exports to tune `x_neighbor_thresh`/`min_sep` only if panel density changes.
- Next likely breakpoint:
  - rerun `[53a-cohort]` and `[cohort-auc]` plotting stages in `notebooks/multi_fish_56h_56g.ipynb` and inspect label overlap/cropping.
- Rerun implications:
  - minimum rerun: `[53a-cohort]` and cohort AUC render stage.

### 2026-04-12 - cohort AUC labels use text-footprint collision stacking

- Slice goal:
  - remove residual AUC median-label overlap by making shared label placement collision-aware on rendered text footprint instead of narrow x-threshold assumptions.
- Passes completed in this session:
  - upgraded `place_labels_no_overlap` internals to detect overlap from rendered text extents and text-height-aware vertical spacing.
  - simplified `[cohort-auc]` caller wiring so label stacking does not depend on a tuned mode-offset threshold.
  - spot-checked shared helper behavior via focused regression tests covering stacking, non-stacking, and y-limit expansion.
- What changed:
  - `src/codeants_2pf_hcr/plots/annotations.py` now stacks labels when rendered horizontal extents overlap (or explicit neighbor threshold matches), and uses rendered text height in data coordinates for vertical separation and top-limit expansion.
  - `src/codeants_2pf_hcr/plots/analysis.py` now calls `place_labels_no_overlap(..., x_neighbor_thresh=0.0)` for cohort AUC medians so overlap handling is footprint-driven by default.
- What remains broken:
  - repository baseline test collection still fails in `tests/test_activity.py` due to unrelated missing export (`prepare_pairs_for_unique_cells` in `codeants_2pf_hcr.activity`).
- Remaining in-slice work:
  - visual export check after rerun to confirm final spacing in dense panels.
- Next likely breakpoint:
  - rerun `[cohort-auc]` and `[53a-cohort]` in `notebooks/multi_fish_56h_56g.ipynb` and inspect `med=...`/sample-size label readability.
- Rerun implications:
  - minimum rerun: `[cohort-auc]` and `[53a-cohort]`.

### 2026-04-12 - cohort 53a AUC global medians hidden + mode-colored median labels

- Slice goal:
  - remove ambiguous global-activity median AUC labels in cohort 53a while keeping median labels elsewhere and making mode identity explicit by color.
- Passes completed in this session:
  - extended shared annotation helper to accept per-label text styling.
  - updated `render_cohort_motion_auc` to color median labels by bout/continuous mode.
  - added renderer-level switch to suppress median labels only on global-activity AUC panels.
  - wired `[cohort-auc]` notebook/template callers to pass `hide_global_median_labels=True` for the 53a cohort notebook flow.
- What changed:
  - `src/codeants_2pf_hcr/plots/annotations.py` now accepts optional per-item text kwargs in `place_labels_no_overlap`.
  - `src/codeants_2pf_hcr/plots/analysis.py` adds `hide_global_median_labels` on `render_cohort_motion_auc`; when enabled, medians are hidden only for the global-activity panel and retained for marker-specific panels.
  - remaining median labels are now mode-colored (bout/continuous shade) with matching tinted bbox edges.
  - `notebooks/multi_fish_56h_56g.ipynb` and `tools/refactor_notebook_cohort_phase2.py` now call `render_cohort_motion_auc(..., hide_global_median_labels=True)`.
- What remains broken:
  - repository baseline tests still fail in unrelated areas (`tests/test_activity.py` import error; pre-existing `tests/test_matching.py` fixture/title expectations).
- Remaining in-slice work:
  - visual check of regenerated cohort AUC export for expected 53a behavior.
- Next likely breakpoint:
  - rerun `[cohort-auc]` in `notebooks/multi_fish_56h_56g.ipynb` and inspect global vs marker panel median-label behavior.
- Rerun implications:
  - minimum rerun: `[cohort-auc]`.

### 2026-04-14 - cohort responsive-identity donuts use staggered two-row layout

- Slice goal:
  - reduce label collisions in `[cohort-50l-responsive-identity-donut-row]` by adding vertical separation while preserving stage semantics and exported contracts.
- Passes completed in this session:
  - confirmed ownership and routing to package renderer `render_cohort_50l_responsive_identity_donut_row`.
  - replaced single-row subplot arrangement with staggered two-row placement (`top, bottom, top, bottom`) and explicit blank unused slots.
  - updated focused renderer test expectations for new figure geometry.
  - documented stage-level renderer behavior update in `cohort-stage-map.md`.
- What changed:
  - `src/codeants_2pf_hcr/plots/analysis.py` now creates a `2 x N` grid and places fish `i` at `(row=i%2, col=i)`, which yields `top, bottom, top, bottom` ordering across columns; non-used cells are hidden.
  - spacing constants were updated for the two-row geometry (`FIGURE_HEIGHT`, `LAYOUT_BOTTOM/TOP`, `LAYOUT_HSPACE`) while preserving axis limits and output filenames.
  - `tests/test_plots_analysis.py` now asserts the taller figure and expected axis count for the one-fish case.
- What remains broken:
  - repository baseline still has unrelated failures outside this slice in other test modules (pre-existing).
- Remaining in-slice work:
  - visual notebook export check to tune spacing constants only if residual collisions remain on real cohort data.
- Next likely breakpoint:
  - rerun `[cohort-50l-responsive-identity-donut-row]` in `notebooks/multi_fish_56h_56g.ipynb` and inspect inter-panel label clearance.
- Rerun implications:
  - minimum rerun: cohort responsive-identity donut stage only.

### 2026-04-12 - cohort AUC median label contrast tuning

- Slice goal:
  - improve readability of light mode-colored median labels, especially continuous-condition shades.
- Passes completed in this session:
  - tuned cohort median-label text/bbox styling in owning renderer while preserving mode-color semantics.
- What changed:
  - `src/codeants_2pf_hcr/plots/analysis.py` now renders median label text in near-black (`#111111`) for both modes, with a high-contrast light-tinted bbox (`facecolor` = strongly lightened mode color, `edgecolor` = mode color, `linewidth` = 0.9, `alpha` = 0.95).
  - keeps mode identity in the label border/fill tint while restoring text legibility on light shades.
- What remains broken:
  - repository baseline tests still fail in unrelated areas (`tests/test_activity.py` import error; pre-existing `tests/test_matching.py` fixture/title expectations).
- Remaining in-slice work:
  - visual confirmation in regenerated cohort AUC export.
- Next likely breakpoint:
  - rerun `[cohort-auc]` in `notebooks/multi_fish_56h_56g.ipynb` and inspect label contrast in continuous-mode medians.
- Rerun implications:
  - minimum rerun: `[cohort-auc]`.

### 2026-04-14 - responsive-identity donut manual size knobs with proportional ring scaling

- Slice goal:
  - allow manual donut enlargement in `[cohort-50l-responsive-identity-donut-row]` without changing inner/outer ring proportion.
- Passes completed in this session:
  - extended renderer API with explicit sizing parameters.
  - wired notebook stage knobs and mirrored template updates.
  - added focused tests for geometry metadata and proportional ring-width invariant.
- What changed:
  - `src/codeants_2pf_hcr/plots/analysis.py` `render_cohort_50l_responsive_identity_donut_row` now accepts `donut_scale` and `view_limit_scale`.
  - donut geometry now derives from base constants scaled by `donut_scale`; `OUTER_RING_WIDTH` remains `0.375 * INNER_RING_WIDTH` under all scales.
  - renderer returns `geometry` metadata (`outer_radius`, ring widths, `view_limit`, scales) to support assertions/introspection.
  - `notebooks/multi_fish_56h_56g.ipynb` stage `[cohort-50l-responsive-identity-donut-row]` now defines:
    - `COHORT_50L_RESPONSIVE_IDENTITY_DONUT_SCALE`
    - `COHORT_50L_RESPONSIVE_IDENTITY_VIEW_SCALE`
    and passes them into the renderer call.
  - `tools/refactor_notebook_cohort_phase2.py` stage template is updated to preserve these notebook knobs.
  - `tests/test_plots_analysis.py` now verifies:
    - default proportional ratio invariant (`outer_ring_width / inner_ring_width == 0.375`)
    - custom scale pass-through and expected axis view-limit behavior.
- What remains broken:
  - repository baseline still has unrelated pre-existing failures outside this slice in other modules.
- Remaining in-slice work:
  - tune default knob values on real cohort output if desired visual density differs from current defaults.
- Next likely breakpoint:
  - rerun `[cohort-50l-responsive-identity-donut-row]` and adjust the two notebook constants interactively for preferred panel fill.
- Rerun implications:
  - minimum rerun: responsive-identity donut stage only.

### 2026-04-20 - cohort AUC lane counts now use authoritative denominators

- Slice goal:
  - stop cohort `[cohort-auc]` lane/sample-size labels from dropping response-unavailable rows out of the denominator.
- Passes completed in this session:
  - traced `render_cohort_motion_auc` lane-label counts back to a plotted-points-only subset.
  - switched lane/sample-size labels to read `n_total` from the cohort count table instead of recomputing counts from plotted rows.
  - added a focused renderer test where lane labels must exceed the number of plotted points because response-unavailable rows remain in the denominator.
- What changed:
  - `src/codeants_2pf_hcr/plots/analysis.py` now derives lane/sample-size labels from `counts_df` grouped by `(group, laterality, fish_id)`, preserving response-unavailable rows in the displayed `n=...` labels.
  - plotted-point inclusion and the actual AUC panels remain unchanged.
- What remains broken:
  - live cohort notebook rerun is still needed to visually confirm updated lane labels on real cached cohort inputs.
- Remaining in-slice work:
  - optional follow-up: surface the same denominator source more explicitly in any future cohort cache QA summary.
- Next likely breakpoint:
  - rerun `[cohort-auc]` in `notebooks/multi_fish_56h_56g.ipynb` and inspect all-neuron and gene lane labels against the cached count table.
- Rerun implications:
  - minimum rerun: `[cohort-auc]` only if cached count CSVs already reflect the updated single-fish denominators; otherwise refresh stale single-fish `[50l]`/embedded `[56i]` caches first.

### 2026-04-21 - cohort Windows NAS default now resolves directly to 07_Data

- Slice goal:
  - align cohort Windows NAS path defaults with the canonical UNC data root used by single-fish setup.
- Passes completed in this session:
  - updated `src/codeants_2pf_hcr/cohort.py` `_default_nas_root()` to return `\\nasdcsr.unil.ch\RECHERCHE\FAC\FBM\CIG\jlarsch\default\D2c\07_Data` directly on Windows.
  - kept the macOS cohort NAS path unchanged.
- What changed:
  - cohort environment setup no longer conditionally falls back to the parent `D2c` directory when deriving default NAS-backed paths on Windows.
- What remains broken:
  - no live cohort notebook rerun was executed in this session.
- Remaining in-slice work:
  - rerun cohort build/setup on Windows if you want end-to-end validation against the NAS share.
- Next likely breakpoint:
  - execute the cohort config/build stage and verify `NAS_ROOT` and `DATA_ROOT` bindings.
- Rerun implications:
  - minimum rerun: cohort setup/build stage only.
### 2026-05-13 - multifish raw-orientation resolution and anatomy uint8 preprocessing

- intent:
  - use new per-fish raw metadata orientation in `multiFish.ipynb` and related cohort consumers, while keeping legacy `matchingMetadata.csv` fallback for older fish.
  - ensure multifish anatomy Cellpose consumes the same oriented `750x750` uint8 anatomy stack contract as single-fish `[14a]`.
- changed:
  - made shared orientation resolution prefer `01_raw/2p/metadata/*metadata*.csv` `fish_orientation` over `matchingMetadata.csv`, normalize `bottom-left`/`top-right` to `north`/`south`, and fail fast on missing or conflicting values.
  - updated cohort `[23c]`, cohort build polarity lookup, and multifish anatomy segmentation to use the shared resolver.
  - changed multifish anatomy segmentation to preprocess anatomy through `[14a]` before Cellpose, so raw `512x512` anatomy TIFFs no longer bypass the canonical `750x750` uint8 stage.
- validation:
  - `PYTHONPATH=src pytest tests/test_context.py tests/test_multifish.py tests/test_early_stage_extraction.py tests/test_cohort_suite2p_23c.py -q`

### 2026-05-14 - multiFish uses local dataDrive root by default

- intent:
  - stop `multiFish.ipynb` from resolving anatomy/Suite2p cohort paths through the inaccessible macOS `/Volumes/jlarsch` NAS mount.
  - make local-root discovery reusable across terminal-launched repo checkouts.
- changed:
  - switched `notebooks/multiFish.ipynb` `[cfg]` `CohortSuite2p23cConfig` and `MultiFishAnatomySegmentationConfig` to `data_mode="local"`.
  - added `/Volumes/dataDrive/dataProcessing/2p_processing` to shared local-root discovery in `src/codeants_2pf_hcr/runtime.py`.
  - added persistent shell exports in user zsh/bash startup files for `CODEANTS_2PF_HCR_LOCAL_ROOT` plus compatibility aliases.
- validation:
  - `PYTHONPATH=src pytest -q tests/test_runtime.py`
  - fresh `zsh -lc` and `bash -lc` shells report `/Volumes/dataDrive/dataProcessing/2p_processing` for all local-root env aliases.
- rerun implications:
  - restart or source the shell before launching notebooks from a terminal, then rerun `multiFish.ipynb` `[cfg]` and downstream cohort stages.

### 2026-05-14 - raw orientation ignores macOS sidecar CSVs

- intent:
  - prevent AppleDouble `._*metadata*.csv` sidecars in raw metadata folders from failing multifish anatomy orientation resolution before the real metadata CSV is read.
- changed:
  - `read_raw_metadata_polarity` now ignores hidden metadata CSV candidates, preserving fail-fast behavior for invalid visible metadata files.
  - documented that multifish anatomy segmentation reads raw orientation from non-hidden metadata CSVs.
- validation:
  - `conda run -n cellMatching env PYTHONPATH=src python -m unittest tests.test_context.ContextTests.test_resolve_func_polarity_ignores_hidden_raw_metadata_sidecars tests.test_context.ContextTests.test_resolve_func_polarity_prefers_raw_metadata_over_matching_metadata tests.test_context.ContextTests.test_resolve_func_polarity_raises_on_conflicting_raw_metadata`
- rerun implications:
  - rerun `multiFish.ipynb` `[multifish-anatomy-segmentation]` for affected fish such as `L758_f07`.

### 2026-05-26 - multiFish 23c stimulus lookup skips hidden sidecars

- intent:
  - allow `multiFish.ipynb` `[cohort-23c-build]` to reuse processed Suite2p outputs when raw metadata folders contain macOS `._*.csv` sidecars or timestamp-matched r2 logs without `_r2` in the log filename.
- changed:
  - `stimulus.find_experiment_log` and `find_metadata_csv` now ignore hidden CSV sidecars.
  - session-specific experiment-log lookup can fall back from an `_r2_metadata.csv` file to the timestamp-matched non-`_r2` `experiment_log.csv`.
  - copied the missing `L765_f04` r2 metadata/log/schedule CSV bundle from NAS to local dataDrive.
- validation:
  - `PYTHONPATH=src pytest -q tests/test_stimulus.py -k 'session_log_lookup or preprocessing_session_planes'`
  - `PYTHONPATH=src python - <<'PY' ... build_cohort_suite2p_23c_stage(force_build=True) ... PY` now includes all configured fish except `L765_f03`.
- remaining:
  - `L765_f03` still has no local or NAS Suite2p plane outputs under `03_analysis/functional/suite2P`; run/copy `[23a]` Suite2p outputs before it can enter `[cohort-23c-build]`.

### 2026-05-26 - multiFish session-aware functional-anatomy duplicate flags

- intent:
  - aggregate per-fish single-fish ROI identity tables in `multiFish.ipynb` and flag likely duplicate functional ROI representations across planes without merging across imaging sessions.
- changed:
  - added `run_multifish_functional_anatomy_match_stage` and `MultiFishFunctionalAnatomyMatchConfig`.
  - added `annotate_session_anat_label_duplicates`, which groups matched ROIs by `fish_id`, metadata-derived `session_label`, and `selected_anat_label`, ranks by geometry, and keeps all ROI rows with `is_retained_after_multiplane_dedup` flags.
  - added `multiFish.ipynb` `[multifish-functional-anatomy-match]` to write `multifish_functional_roi_activity_identity.csv`, summary CSV, and duplicate-summary CSV under `cohort_outputs/multiFish/`.
- validation:
  - `PYTHONPATH=src pytest -q tests/test_multifish.py tests/test_package_exports.py tests/test_agent_docs.py`
- rerun implications:
  - rerun each fish through single-fish `[50i]`/`[50ia]` first when `functional_roi_activity_identity.csv` is missing, then rerun `multiFish.ipynb` `[cfg]` and `[multifish-functional-anatomy-match]`.
