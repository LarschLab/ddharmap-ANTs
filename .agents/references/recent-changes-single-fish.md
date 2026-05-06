# Recent Changes - Single-Fish Workflow

**Purpose:** rolling handoff log for meaningful single-fish work, remaining breakpoints, and rerun implications.

**Use this file when:** work targets `notebooks/2PF_to_HCR.ipynb` or single-fish ownership modules.

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
- Keep migration state in `current-state.md`; use this file for per-change single-fish handoff detail.

### 2026-05-06 - single-fish notebook contract-clean migration wrappers

- Slice goal:
  - finish the remaining single-fish notebook refactor surface by removing top-level helper definitions and visible bulky alias/helper blocks from the notebook.
- Passes completed in this session:
  - moved remaining def-heavy and bulky support cells into package-owned migration wrappers: `[30]`, `[34c]`, `[38]`, `[40]`, `[41]`, `[44]`, `[46]`, `[47]`, `[47b]`, `[50i]`, `[50ia]`, `[51]`, `[54]`, and `[56d]`.
  - rewired the notebook cells to thin public package calls and added notebook-contract ownership rules for the migrated tags.
  - updated package exports and symbol/stage/current-state docs.
- What changed:
  - `notebooks/2PF_to_HCR.ipynb` now reports zero top-level helper definitions, zero figure violations, zero required-cell violations, and zero native-stage import violations.
  - the `[56d]` user-facing cell no longer contains the `FIG_53A_RGBA_LOCAL` / `FIG_56_RGBA_LOCAL` alias clutter; that legacy behavior is hidden behind the package wrapper.
- What remains broken:
  - none known from static/package validation; live notebook rerun on fish data is still needed for visual confirmation of migrated legacy stages.
- Remaining in-slice work:
  - optional future pass: replace embedded migration-source strings with explicit owner APIs where the legacy code is still too large for long-term maintenance.
- Next likely breakpoint:
  - rerun the newly wrapped notebook cells in order on an active fish and inspect any failure at the package wrapper boundary for missing upstream globals.
- Rerun implications:
  - no canonical output filenames changed; rerun the touched cells only when validating the notebook experience or refreshing their outputs.

### 2026-05-06 - [56f-qc] yellow midline anatomy display alignment

- Slice goal:
  - fix the remaining `[56f-qc]` visual mismatch where hemisphere colors were correct but the yellow midline overlay was drawn in the wrong display space.
- Passes completed in this session:
  - traced `[56f-qc*]` display image selection and midline overlay rendering separately from side assignment.
  - made anatomy-space midline QC prefer `plane_refs[*].ref_warped`/`ref_warped_raw`, matching `[22c]` preview space.
  - made `[56f-qc*]` warp Suite2p label masks into the anatomy display with the active plane transform before rendering colored ROI boundaries.
  - made yellow line drawing skip fixed-to-moving conversion when the panel display is already anatomy-space.
  - propagated the activity subset figure's `display_space` from its source payload so the second `[56f-qc-activity]` figure uses the same yellow-line rule as the first.
  - added focused regression coverage for anatomy-display selection and label-mask warping.
- What changed:
  - `[56f-qc]`, `[56f-qc-activity]`, and the second `[56f-qc-activity]` subset figure now render ROI colors and the yellow midline in the same coordinate space for anatomy-space midline bundles.
- What remains broken:
  - none known in code; active notebook kernels need restart or module reload to pick up the shim change.
- Remaining in-slice work:
  - visually confirm `[56f-qc]` on the affected fish.
- Next likely breakpoint:
  - if the yellow line is still wrong, inspect whether `plane_refs[*].ref_warped` is stale relative to the saved `midline_params_func_ref.json`.
- Rerun implications:
  - rerun `[56f-qc]` and `[56f-qc-activity]`; rerun `[20]`/`[22c]` only if the stored warped references or midline JSON are stale.

### 2026-05-06 - [56f-qc] anatomy-centroid double-transform guard

- Slice goal:
  - diagnose why the `[22c]` interactive midline still did not propagate correctly into `[56f-qc]` hemisphere assignment.
- Passes completed in this session:
  - traced `[22c]` midline bundle saving, `[56f-qc*]` midline-space inference, authoritative anatomy centroid attachment, and side annotation.
  - fixed `[56f-qc]` and `[56f-qc-activity]` so anatomy centroids are not transformed again after they are selected for anatomy-space midline assignment.
  - made `[56f-qc*]` honor explicit `midline_space` from the saved `[22c]` bundle before falling back to legacy `base.source_label` inference.
  - added focused regression coverage for the double-transform failure mode.
- What changed:
  - when `functional_roi_activity_identity.csv` supplies `centroid_x_anat`/`centroid_y_anat`, those coordinates are now used directly against anatomy-space midline parameters.
- What remains broken:
  - none known in code; affected notebook kernels still need a module reload or restart before rerunning the QC cells.
- Remaining in-slice work:
  - visually confirm `[56f-qc]` on the affected fish after rerun.
- Next likely breakpoint:
  - if separation remains wrong, inspect the saved `midline_params_func_ref.json` and `functional_roi_activity_identity.csv` for stale fish IDs or missing/noncurrent anatomy centroids.
- Rerun implications:
  - rerun `[56f-qc]` and `[56f-qc-activity]`; rerun `[22c]` only if the existing midline JSON predates explicit `midline_space` or needs manual retuning.

### 2026-05-06 - [56f-qc] authoritative anatomy-centroid midline assignment

- Slice goal:
  - fix `[56f-qc]` and `[56f-qc-activity]` hemisphere assignment when the saved midline is anatomy-space and transform fallback collapses plotted ROIs to one side.
- Passes completed in this session:
  - made `[56f-qc*]` merge `functional_roi_activity_identity.csv` anatomy centroids onto plotted ROI rows before side annotation.
  - made `[22c]` include explicit `midline_space` in newly saved midline bundles.
  - added focused regression coverage for anatomy-space centroid preference and shim wiring.
- What changed:
  - anatomy-space midline assignment now uses authoritative ROI-centric anatomy centroids when available, keeping functional centroids for plotting only.
- What remains broken:
  - none known in this slice.
- Remaining in-slice work:
  - rerun `[56f-qc]` / `[56f-qc-activity]` on the affected fish and visually confirm both left and right counts are nonzero where expected.
- Next likely breakpoint:
  - if the figure still reports one side only, inspect whether `functional_roi_activity_identity.csv` is stale or missing anatomy centroid columns for the current `FISH_ID`.
- Rerun implications:
  - minimum rerun: `[56f-qc]` and `[56f-qc-activity]`; rerun `[22c]` only when regenerating `midline_params_func_ref.json` with explicit `midline_space`.

### 2026-05-06 - [56f-qc] ANTs midline display transform fix

- Slice goal:
  - fix `[56f-qc]` and `[56f-qc-activity]` midline overlays when the saved midline is in anatomy/warped space and the active in-plane backend is ANTs.
- Passes completed in this session:
  - added a package-owned point transform helper for skimage and ANTs in-plane transforms.
  - rewired the `[56f-qc*]` migration shims to transform ROI centroids into midline space and saved midline geometry back into display space.
  - added focused transform regression tests.
- What changed:
  - `[56f-qc*]` no longer treats ANTs transform dictionaries as identity when drawing or assigning anatomy-space midlines.
- What remains broken:
  - none known in this slice.
- Remaining in-slice work:
  - none.
- Next likely breakpoint:
  - rerun notebook `[56f-qc]` / `[56f-qc-activity]` for affected fish after `[22c]`, `[23a]`, and in-plane registration state are available.
- Rerun implications:
  - visual-only QC rerun; canonical tables do not need regeneration unless side assignments were consumed by downstream cached analysis outputs.

### 2026-05-06 - [22c] Midline commit callback contract guard

- Slice goal:
  - prevent the interactive `[22c]` Commit + Save callback from regressing to a stale `_params_for_plane(plane_idx)` contract.
- Passes completed in this session:
  - added an import-time source guard for the embedded `[22c]` midline commit path.
  - added a focused regression test that verifies `_build_bundle(dy, dtheta)` passes slider values through to `_params_for_plane`.
- What changed:
  - stale `[22c]` embedded source now fails clearly before widget interaction instead of surfacing as a button-click TypeError.
- What remains broken:
  - active notebook kernels that already imported an older module still need restart or manual module reload before rerunning `[22c]`.
- Rerun implications:
  - rerun `[22c]` after refreshing the notebook kernel/module state; downstream caches are unaffected until a new midline JSON is saved.

### 2026-04-25 - single-fish [50l] composite now package-rendered and shared trace prep helpers added

- Slice goal:
  - continue the broad single-fish notebook refactor by completing the active `[50l]` ownership slice and adding shared trace/midline helper APIs for the larger `[56]` / `[56h]` / `[56f-qc*]` migration.
- Passes completed in this session:
  - added public trace helpers for midline context loading, midline-side annotation, high-confidence pair filtering, and padded trace-window extraction.
  - added `render_single_fish_50l_composite(...)` in `plots.analysis` and rewired notebook `[50l]` to call it as a thin wrapper.
  - updated exports, notebook owner contracts, focused tests, and reference docs.
- What changed:
  - `[50l]` no longer defines notebook-local helper functions and no longer imports private cache-staleness helpers directly.
  - `[50l]` still writes `compound_50j_56i_unified.png/.pdf` and preserves legacy globals such as `FIG_50L_COMPOSITE`, `FIG_50L_COMPOSITE_RGBA`, `FIG_50L_COMPOSITE_PATH`, and panel axes.
  - static required-cell contract violations are now zero; top-level notebook defs dropped from 127 to 120 in the current working tree.
- What remains broken:
  - the large `[56h]`, `[56f-qc]`, `[56f-qc-activity]`, `[56]`, and `[50]` cells still carry notebook-local helpers and should be the next broad-refactor targets.
  - live notebook visual confirmation of the package-rendered `[50l]` composite is still required on fish data.
- Remaining in-slice work:
  - `[50l]` slice is package-owned and validated by focused tests; remaining work belongs to the next trace/QC ownership slice.
- Next likely breakpoint:
  - start with `[56h]` plus the duplicated midline/trace helpers in `[56f-qc]`, `[56f-qc-activity]`, and `[56]`, using the new `traces.py` helper APIs.
- Rerun implications:
  - minimum rerun for this slice: `[50l]`; it will rebuild stale `[56i]` AUC tables when given current fish paths.

### 2026-04-21 - single-fish [34] centroid QA now matches [26] label-source and orientation policy

- Slice goal:
  - remove the QA-only drift where `[34]` could load a different functional label source than `[26]` and could reorient already oriented in-memory labels.
- Passes completed in this session:
  - updated package label resolution so in-memory Suite2p / `func_labels` arrays are treated as already oriented display-space labels.
  - added `use_suite2p_labels` threading through `show_centroid_match_qa_stage(...)` and rewired notebook `[34]` to match `[26]` source selection.
  - added focused regression coverage for in-memory provenance, file-backed raw-mask orientation, and `[34]` helper threading.
- What changed:
  - `[34]`, `[34a]`, and downstream QA consumers that reuse `_load_func_labels_for_plane` now inherit the same Suite2p-vs-fallback source policy as `[26]`.
  - `[34]` center-panel wording now explicitly describes warped functional labels in anatomy space.
- What remains broken:
  - live notebook rerun is still required to visually confirm the targeted fish now shows `[26]`/`[34]` consistency under the active `USE_SUITE2P_LABELS` setting.
- Remaining in-slice work:
  - optional follow-up only if notebook rerun shows unexpected drift in `[46]` or `[54]`; current static ownership tracing suggests canonical `[50i]` / `[50]` outputs are unaffected.
- Next likely breakpoint:
  - rerun `[23a] -> [26] -> [34] -> [34a]`, then spot-check `[46]` or `[54]` on the same fish.
- Rerun implications:
  - minimum rerun: `[23a] -> [26] -> [34] -> [34a]`; `[50i]` is only needed as a guard check if the QA rerun suggests a broader mismatch.

### 2026-04-21 - single-fish `[50f]` / `[50g]` rebuild rejected-mask fates from `[44]` and `[50l]` keeps gene AUC labels in-bounds

- Slice goal:
  - fix the post-refactor regression where `[50f]` / `[50g]` still expected notebook-local `HCR_MASK_FATE_DF`, and restore package-owned median-label y-limit behavior in `[50l]` gene panels.
- Passes completed in this session:
  - added package-owned `build_hcr_mask_fate_df(...)` in `codeants_2pf_hcr.matching` to reconstruct per-confocal-label fate rows from `[44]` `hcr_match_results`.
  - rewired notebook `[50f]` and `[50g]` to rebuild/cache `HCR_MASK_FATE_DF` from `[44]` instead of incorrectly treating `[50e]` as the producer.
  - fixed `render_single_fish_50l_gene_auc_panel(...)` so shared gene-panel y-limits stay expanded after collision-aware median-label placement.
- What changed:
  - `[50f]` / `[50g]` now fail only when `[44]` match inputs are missing; rerunning `[50e]` is no longer part of their dependency chain.
  - package-owned single-fish `[50l]` gene AUC panels no longer shrink the y-top back after `place_labels_no_overlap(...)` expands it, so top-edge `med=...` labels remain inside the panel.
- Rerun implications:
  - minimum rerun for rejected-mask QA: `[44]` -> `[50f]` / `[50g]`.
  - minimum rerun for AUC label verification: `[56i]` if stale -> `[50l]`.

### 2026-04-20 - single-fish [50l] package-owns embedded [56i] tables and gene AUC panels

- Slice goal:
  - remove the embedded `[56i]` table-builder and the remaining `[50l]` notebook-local gene AUC renderer while preserving current single-fish output contracts.
- Passes completed in this session:
  - added `build_single_fish_motion_auc_plot_tables` plus `MotionAucPlotConfig` in `traces.py` and covered the ROI/gene/count semantics with focused tests.
  - added `render_single_fish_50l_gene_auc_panel` in `plots.analysis`, exported it through package surfaces, and added focused renderer/export tests.
  - rewired notebook cell `[50l]` to call the new package owners for stale-motion-AUC rebuilds and marker-specific gene AUC rendering.
  - tightened notebook contract coverage for the `[50l]` / `[57a-responsive-identity-donut]` owner surface and updated symbol docs.
- What changed:
  - single-fish motion AUC point/count/ROI-panel tables are now built by package-owned `codeants_2pf_hcr.traces.build_single_fish_motion_auc_plot_tables(...)` instead of the notebook-local `[50l]` block.
  - single-fish marker-specific `[50l]` AUC panels are now rendered by package-owned `codeants_2pf_hcr.plots.analysis.render_single_fish_50l_gene_auc_panel(...)`.
  - notebook `[50l]` remains the first downstream consumer and keeps the same cached CSV filenames and composite export path.
- What remains broken:
  - live notebook rerun on fish data is still required to visually confirm the extracted `[50l]` composite panels after the package-owner swap.
  - pre-existing unrelated `tests/test_activity.py` import/export failure remains outside this slice.
- Remaining in-slice work:
  - optional: move the remaining donut-specific local helper logic in `[50l]` into package ownership if the cell should become fully def-free rather than package-call dominant.
- Next likely breakpoint:
  - rerun `[50l]` with stale or missing `motion_auc_plot_points.csv` / `motion_auc_plot_counts.csv` and confirm the package-owned rebuild logs plus the unchanged composite outputs.
- Rerun implications:
  - minimum rerun: `[50l]`; it now delegates stale `[56i]` table regeneration to the package builder before drawing the composite.

### 2026-04-20 - single-fish [50l] auto-invalidates stale [56i] motion AUC caches

- Slice goal:
  - stop `[50l]` from silently reusing stale `[56i]` motion AUC point/count CSVs after `[50ia]` or related upstream semantic updates.
- Passes completed in this session:
  - extracted a pure package helper for `[50l]` motion AUC cache staleness checks.
  - replaced the notebook `[50l]` missing-only gate with stale-or-missing invalidation and explicit reason logging.
  - added focused regression coverage for newer master/status/midline inputs and fresh-cache reuse.
- What changed:
  - `notebooks/2PF_to_HCR.ipynb` `[50l]` now recomputes the embedded `[56i]` motion AUC tables when either cache CSV is missing or when `functional_roi_activity_identity.csv`, `hcr_activity_status.csv`, or `midline_params_func_ref.json` is newer than either cache.
  - `[50l]` now logs the exact file relationship that made the cache stale before rebuilding.
- What remains broken:
  - manual notebook acceptance on the target fish is still required to confirm refreshed bottom-panel labels and counts on real data.
- Remaining in-slice work:
  - optional follow-up: move more of the remaining notebook-local `[56i]` build block into a package-owned helper while preserving current outputs.
- Next likely breakpoint:
  - rerun `[50ia]`, leave old `motion_auc_plot_points.csv` / `motion_auc_plot_counts.csv` in place, then rerun `[50l]` and confirm the stale-cache log plus refreshed bottom panels.
- Rerun implications:
  - `[50l]` now auto-runs the embedded `[56i]` rebuild path for stale motion AUC caches; manual `[56i]` reruns are only needed when debugging or when upstream outputs themselves are missing/broken.

### 2026-04-12 - single-fish 50l paired AUC connectors restored

- Slice goal:
  - restore within-neuron bout↔continuous connector lines in single-fish unilateral BPI/AUC panels.
- Passes completed in this session:
  - traced [50l] plotting cell where paired lookup already existed but line draw was a no-op.
  - re-enabled explicit connector rendering for paired bout/continuous rows in both duplicated [50l] plotting blocks.
- What changed:
  - `notebooks/2PF_to_HCR.ipynb` now draws within-neuron connector lines in single-fish [50l] unilateral AUC/BPI plotting logic using existing per-category alpha controls (`AUC_PAIR_LINE_ALPHA_*`), matching cohort-style behavior.
- What remains broken:
  - repository test baseline still fails at collection in `tests/test_activity.py` due to unrelated missing export (`prepare_pairs_for_unique_cells` in `codeants_2pf_hcr.activity`).
- Remaining in-slice work:
  - optional cleanup: remove the duplicated [50l] plotting block to keep one authoritative implementation path.
- Next likely breakpoint:
  - rerun notebook stage `[50l]` in `notebooks/2PF_to_HCR.ipynb` and inspect paired line visibility for all-neurons and gene panels.
- Rerun implications:
  - minimum rerun: `[56i]` (if point/count CSVs are stale) -> `[50l]`.

### 2026-04-12 - single-fish AUC median labels switched to 53a-style placement

- Slice goal:
  - prevent overlap/cropping of single-fish unilateral AUC median labels.
- Passes completed in this session:
  - replaced fixed top-band median text placement in both duplicated `[50l]` AUC plotting blocks.
  - switched to above-data stacked placement with collision-aware vertical stepping and y-limit expansion.
- What changed:
  - `notebooks/2PF_to_HCR.ipynb` now places `med=...` labels above the data cloud, stacks upward when neighboring x positions collide, and expands panel y-limits to keep labels readable.
- What remains broken:
  - repository test baseline still fails at collection in `tests/test_activity.py` due to unrelated missing export (`prepare_pairs_for_unique_cells` in `codeants_2pf_hcr.activity`).
- Remaining in-slice work:
  - visual confirmation in notebook-rendered figures after rerun.
- Next likely breakpoint:
  - rerun `[50l]` and inspect single-fish AUC panel annotation spacing.
- Rerun implications:
  - minimum rerun: `[56i]` (if point/count CSVs are stale) -> `[50l]`.

### 2026-04-12 - single-fish AUC labels wired to shared package helper

- Slice goal:
  - reduce notebook-local duplication by using package-owned 53a-style label placement in `[50l]` AUC plotting blocks.
- Passes completed in this session:
  - identified both duplicated `[50l]` AUC median-label blocks still using inline collision code.
  - replaced both inline blocks with calls to `codeants_2pf_hcr.plots.annotations.place_labels_no_overlap`.
- What changed:
  - `notebooks/2PF_to_HCR.ipynb` now imports `place_labels_no_overlap` and uses it in both `[50l]` AUC median-label paths instead of ad hoc in-cell stacking loops.
  - behavior remains 53a-style (above-band placement, local x-collision stacking, y-limit expansion) but now shares package ownership for the overlap logic.
- What remains broken:
  - repository test baseline still fails at collection in `tests/test_activity.py` due to unrelated missing export (`prepare_pairs_for_unique_cells` in `codeants_2pf_hcr.activity`).
- Remaining in-slice work:
  - complete migration by moving remaining `[50l]` figure-construction logic into a notebook-callable package renderer and leaving the notebook as orchestration only.
- Next likely breakpoint:
  - rerun `[50l]` in `notebooks/2PF_to_HCR.ipynb` (after kernel restart/import refresh) and verify median labels are de-overlapped in both all-neurons and marker panels.
- Rerun implications:
  - minimum rerun: `[56i]` (if point/count CSVs are stale) -> `[50l]`.

### 2026-04-12 - single-fish [50l] AUC labels switched to footprint-based overlap detection

- Slice goal:
  - ensure same-group bout/continuous `med=...` labels stack reliably in `[50l]` without relying on caller-tuned x-neighbor thresholds.
- Passes completed in this session:
  - updated both duplicated `[50l]` AUC label blocks to stop computing mode-offset-derived neighbor thresholds.
  - relied on shared helper’s rendered-text collision logic for overlap handling.
  - added focused regression tests for helper behavior (stacking, far-label no-stack, y-limit growth).
- What changed:
  - both `[50l]` AUC median-label paths in `notebooks/2PF_to_HCR.ipynb` now pass `x_neighbor_thresh=0.0`, letting shared rendered-footprint collision checks drive stacking.
  - helper internals in `src/codeants_2pf_hcr/plots/annotations.py` now use rendered text extents and text-height-aware spacing, improving de-overlap consistency for AUC labels.
- What remains broken:
  - repository test baseline still fails at collection in `tests/test_activity.py` due to unrelated missing export (`prepare_pairs_for_unique_cells` in `codeants_2pf_hcr.activity`).
- Remaining in-slice work:
  - visual confirmation of final `[50l]` exports after rerun.
- Next likely breakpoint:
  - rerun `[56i]` only if source point/count CSVs are stale, then rerun `[50l]` and inspect median label spacing in all-neurons and marker panels.
- Rerun implications:
  - minimum rerun: `[56i]` (if stale) -> `[50l]`.

### 2026-04-12 - single-fish [50l] median label contrast tuning

- Slice goal:
  - improve readability of light mode-tinted median labels in single-fish `[50l]` AUC panels.
- Passes completed in this session:
  - adjusted both duplicated `[50l]` median-label style blocks to use high-contrast text with tinted mode bbox.
- What changed:
  - both median-label blocks in `notebooks/2PF_to_HCR.ipynb` now use near-black label text (`#111111`) with high-contrast light-tinted bbox fill (`_blend_color_local(label_color, blend_frac=0.88)`), mode-colored bbox edge, and stronger bbox opacity (`alpha=0.95`).
  - preserves bout/continuous mode identity through border/fill tint while improving legibility on lighter continuous shades.
- What remains broken:
  - repository test baseline still fails at collection in `tests/test_activity.py` due to unrelated missing export (`prepare_pairs_for_unique_cells` in `codeants_2pf_hcr.activity`).
- Remaining in-slice work:
  - optional package migration to eliminate duplicated notebook `[50l]` blocks.
- Next likely breakpoint:
  - rerun `[50l]` and inspect continuous-mode median label contrast in all-neurons and marker panels.
- Rerun implications:
  - minimum rerun: `[56i]` (if stale) -> `[50l]`.

### 2026-04-12 - single-fish [50l] mode-colored AUC median labels

- Slice goal:
  - disambiguate bout vs continuous median AUC labels by matching label color/shade to plotted mode colors.
- Passes completed in this session:
  - updated both duplicated `[50l]` median-label blocks to pass per-label style to shared annotation helper.
  - switched block-level default label style to neutral z-order only.
- What changed:
  - `notebooks/2PF_to_HCR.ipynb` now colors each `med=...` label with `mode_color_map[group][stim_mode]` and uses matching bbox edge color for both `[50l]` median-label blocks.
  - shared helper call remains `place_labels_no_overlap`, so collision behavior stays unchanged while mode identity is explicit in the label styling.
- What remains broken:
  - repository test baseline still fails at collection in `tests/test_activity.py` due to unrelated missing export (`prepare_pairs_for_unique_cells` in `codeants_2pf_hcr.activity`).
- Remaining in-slice work:
  - optional: finish package-owned `[50l]` renderer migration to eliminate duplicated notebook blocks.
- Next likely breakpoint:
  - rerun `[50l]` in `notebooks/2PF_to_HCR.ipynb` and inspect mode-colored median labels in all-neurons and marker panels.
- Rerun implications:
  - minimum rerun: `[56i]` (if stale) -> `[50l]`.

### 2026-04-22 - single-fish anatomy-label co-expression summary stage near [57]

- Slice goal:
  - add a package-rendered single-fish `[57b-anatomy-coexpression-summary]` stage that summarizes possible multi-marker anatomy labels from the in-plane HCR status subset.
- Passes completed in this session:
  - added `render_single_fish_hcr_anatomy_coexpression_summary` in `plots.qa` with stable PNG/PDF/CSV exports.
  - inserted notebook stage `[57b-anatomy-coexpression-summary]` after `[57a-responsive-identity-donut]` as a thin package-renderer call that displays the per-anatomy summary table.
  - extended notebook contracts, package exports, and focused renderer regression tests.
- What changed:
  - new single-fish outputs are written under fish `04_plots` with stable filenames:
    - `single_fish_hcr_anatomy_coexpression_summary.png`
    - `single_fish_hcr_anatomy_coexpression_summary.pdf`
    - `single_fish_hcr_anatomy_coexpression_summary.csv`
    - `single_fish_hcr_anatomy_coexpression_combo_counts.csv`
  - semantics are HCR-centric and anatomy-label scoped: one in-plane `anat_label` with more than one distinct gene is reported as a possible co-expression candidate.
- Rerun implications:
  - minimum rerun: `[50]` -> `[57b-anatomy-coexpression-summary]`.

### 2026-04-15 - single-fish responsive identity donut stage near [57]

- Slice goal:
  - add a package-rendered single-fish responsive-identity donut bridge stage immediately before `[57]` with cohort-matched hybrid semantics.
- Passes completed in this session:
  - added `render_single_fish_50l_responsive_identity_donut` in `plots.analysis` and delegated to cohort responsive renderer internals to keep semantics identical.
  - inserted notebook stage `[57a-responsive-identity-donut]` before `[57]` with thin orchestration-only call and scale knobs.
  - extended notebook contract tags/regressions and renderer/export tests.
  - updated stage/figure/symbol/current-state references.
- What changed:
  - new single-fish outputs are written under fish `04_plots` with stable filenames:
    - `single_fish_50l_responsive_identity_donut.png`
    - `single_fish_50l_responsive_identity_donut.pdf`
    - `single_fish_50l_responsive_identity_donut_counts.csv`
    - `single_fish_50l_responsive_identity_donut_counts_wide.csv`
  - semantics remain hybrid-scoped: responsive ROI-centric denominator + selected HCR exact-combo identity mapping + `unidentified` fallback.
- What remains broken:
  - unrelated baseline collection/export issue outside this slice may still appear depending on branch state.
- Remaining in-slice work:
  - optional: migrate remaining local plotting-heavy single-fish cells to package-owned renderers.
- Next likely breakpoint:
  - rerun notebook stage `[57a-responsive-identity-donut]` using current `[50]` and `[50ia]` outputs to visually confirm layout on target fish data.
- Rerun implications:
  - minimum rerun: `[50]` + `[50ia]` (if stale) -> `[57a-responsive-identity-donut]` -> `[57]`.

### 2026-04-20 - single-fish [50l] top-left BPI panel switched to AUC-vs-BPI helper

- Slice goal:
  - move the `[50l]` whole-population top-left panel into package ownership and change it from a jittered 1D BPI strip to a true mean-AUC-vs-BPI scatter.
- Passes completed in this session:
  - added `render_single_fish_50l_bpi_panel` in `src/codeants_2pf_hcr.plots.analysis`.
  - replaced the notebook-local top-left `[50l]` plotting block with a thin helper call while preserving the existing CSV/master compatibility fallback.
  - added focused renderer tests for AUC averaging, row exclusion, response-unavailable filtering, marker styling, and zero-band handling.
- What changed:
  - `[50l]` now plots per-ROI `mean_auc_dff = 0.5 * (mean_bout_auc_dff + mean_cont_auc_dff)` on the x-axis and BPI on the y-axis using `[50ia]` response/BPI outputs.
  - filled markers remain responsive ROIs, hollow markers remain non-responsive-but-plottable rows, and `response unavailable` rows remain excluded.
- What remains broken:
  - notebook visual validation and any fish-specific stale-cache reruns still need to be done in a live notebook session.

### 2026-04-20 - single-fish [50l] removes duplicate 56i figure and fixes AUC denominators

- Slice goal:
  - keep `[50l]` focused on the composite export while fixing count-strip denominators and gene-panel connector semantics.
- Passes completed in this session:
  - removed the stale inline standalone `[56i]` figure build/save/show path from notebook cell `[50l]` while preserving cache/table regeneration.
  - switched all-neuron count-strip denominators to the full ROI-centric master table after laterality expansion instead of the `suite2p_is_cell`-gated AUC detail frame.
  - updated the local `[50l]` gene-panel connector logic so only directional categories use directional connector colors.
  - extended notebook regressions for removed standalone-output strings, denominator source, and connector-color branching.
- What changed:
  - `[50l]` now emits only `compound_50j_56i_unified.png/.pdf`; it no longer builds, saves, or displays `motion_auc_by_gene_ipsi_contra.png`.
  - all-neuron count strips now include response-unavailable ROIs in `n_total`/`n_other` even though those rows remain excluded from plotted AUC points.
  - gene-panel connectors are now blue for `bout-responsive`, orange for `continuous-responsive`, and neutral for non-directional categories.
- What remains broken:
  - live notebook rerun is still required to visually confirm updated count strips and connector colors on the target fish.
- Remaining in-slice work:
  - optional package migration to remove the remaining notebook-local `_plot_auc_block` duplication in `[50l]`.
- Next likely breakpoint:
  - rerun `[50l]` on a fish with stale `motion_auc_plot_points.csv` / `motion_auc_plot_counts.csv` and verify the composite is the only displayed/saved AUC figure.
- Rerun implications:
  - minimum rerun: `[50l]` only; it will regenerate stale `[56i]` AUC tables as needed before building the composite.

### 2026-04-20 - single-fish [50l] global AUC panels moved to package paired-point renderer

- Slice goal:
  - replace the single-fish `[50l]` all-neurons AUC violin panels with package-owned paired-point panels while keeping gene panels, cached CSV contracts, and stage flow unchanged.
- Passes completed in this session:
  - added `render_single_fish_50l_global_auc_panel` in `src/codeants_2pf_hcr/plots/analysis.py` and exported it through `plots.__init__`.
  - updated `[50l]` notebook orchestration to import/call the new renderer and removed the notebook-local all-neurons helper path.
  - extended focused plot tests for fixed BPI limits, point-only global panels, paired connectors, neutral/directional styling, and directional class-mean summaries.
- What changed:
  - `render_single_fish_50l_bpi_panel` now clamps the top-left BPI panel to `[-1, 1]`.
  - single-fish `[50l]` all-neurons ipsi/contra AUC panels now render one point per ROI per mode, draw within-ROI bout↔continuous connectors, keep non-directional classes neutral, and add larger bout/continuous class-mean summaries for directional classes only.
  - count-strip bars under the all-neurons panels remain unchanged, and gene-specific panels still use the prior renderer path.
- What remains broken:
  - live notebook visual confirmation on fish data is still required; this session only covered focused automated regressions.
- Remaining in-slice work:
  - optional follow-up: reconcile the bottom AUC legend text with the new all-neurons paired-point styling if the publication-facing legend needs to describe both global and gene panels more explicitly.
- Next likely breakpoint:
  - rerun notebook stage `[50l]` and inspect the two global AUC panels plus the fixed-range BPI panel on real data.
- Rerun implications:
  - minimum rerun: `[56i]` only if `motion_auc_plot_points.csv` / `motion_auc_plot_counts.csv` are stale, then rerun `[50l]`.
- Remaining in-slice work:
  - optional broader migration of remaining `[50l]` plotting logic into package-owned helpers/renderers.
- Next likely breakpoint:
  - rerun notebook stage `[50l]` and inspect the new top-left AUC-vs-BPI panel for layout and readability on target fish data.
- Rerun implications:
  - minimum rerun: `[50ia]` (if response/BPI CSV is stale) -> `[56i]` only if motion AUC plot CSVs are stale -> `[50l]`.

### 2026-04-21 - single-fish early/staging refactor pass for [12] [14] [20] [34a] [56g] plus notebook-only cleanup in [57]

- Slice goal:
  - convert the early spatial/anatomy cells and the first downstream owner slices into package-backed notebook wrappers, while reducing top-level notebook helper debt without changing outputs.
- Passes completed in this session:
  - added/exported `AnatomyNormalizationStageConfig` + `normalize_anatomy_stack_stage` in `context.py` and rewrote `[14]` to a thin stage wrapper.
  - added/exported `FunctionalReferenceConfig` + `build_functional_references_stage` and `FunctionalPlacementConfig` + `run_ncc_placement_stage` in `spatial.py`, then rewrote `[12]` and `[20]` to thin stage wrappers.
  - added/exported `SingleFishBpiDiagnosticsConfig` + `prepare_single_fish_bpi_diagnostics_stage` in `activity.py`, then rewrote `[56g]` so response-aware BPI/activity prep is package-owned while plotting remains local.
  - finished/exported `FunctionalAnatomyDebugConfig` + `build_functional_anatomy_debug_stage` in `matching.py`, then rewrote `[34a]` to a package stage wrapper.
  - removed the remaining local helper defs from `[56g]` and `[57]` without changing the existing plotting behavior.
  - extended focused tests for exports, early-stage extraction, activity prep, matching debug staging, and notebook regressions; reran the focused suite successfully.
- What changed:
  - notebook tags `[12]`, `[14]`, `[20]`, `[34a]`, `[56g]`, and `[57]` are now free of top-level `def` violations.
  - live single-fish contract counts moved from `164` top-level defs at session start to `160` after this pass.
  - required-cell owner contracts now cover `[12]`, `[14]`, `[20]`, `[34a]`, `[56g]`, `[50l]`, and `[57a-responsive-identity-donut]`.
- What remains broken:
  - `[50l]` still carries 7 notebook-local helper defs and remains the active downstream blocker in this ownership band.
  - matching extraction for `[50i]` and `[50]` is only partially prepared in `matching.py`; no notebook rewrite landed for those cells in this pass.
- Remaining in-slice work:
  - extract the remaining `[50l]` composite helpers/rendering into package-owned functions so the cell becomes a true thin wrapper.
  - finish the package stages for `[50i]` and `[50]` and then rewrite those notebook cells.
- Next likely breakpoint:
  - continue from `[50l]` first, then finish the matching-owned `[50i]` / `[50]` stage extraction and wrapper rewrites.
- Rerun implications:
  - minimum rerun for early checks: `[12] -> [14] -> [16] -> [20]`.
  - minimum rerun for downstream checks: `[56h] -> [56g]`, plus `[57]` for the trace plot path.

### 2026-05-05 - NCC-guided per-plane ANTs regions for [19a]

- Slice goal:
  - replace manual `[19a]` ANTs fixed-region selection with automatic NCC-guided per-plane regions using the matched functional footprint plus 10% context.
- What changed:
  - `[19a]` now writes `ants_registration_region_square.json` with one NCC-derived square per functional plane.
  - `[20]` selects the current plane's region from that JSON for `ants_rigid_affine`, while legacy single-square JSON remains supported.
- Rerun implications:
  - minimum rerun: `[16] -> [19a] -> [20]`; rerun `[22e]` afterward to visually compare NCC and ANTs placements.

### 2026-04-21 - Windows NAS default now resolves directly to 07_Data

- Slice goal:
  - fix single-fish Windows NAS setup so package-owned default path resolution points at the canonical UNC data root instead of conditionally falling back to the parent `D2c` directory.
- Passes completed in this session:
  - updated `src/codeants_2pf_hcr/context.py` `default_nas_root()` to return `\\nasdcsr.unil.ch\RECHERCHE\FAC\FBM\CIG\jlarsch\default\D2c\07_Data` directly on Windows.
  - kept the macOS path unchanged.
- What changed:
  - single-fish notebook setup stages that rely on package-default `NAS_ROOT` now resolve the same canonical Windows data root without probing `Path.exists()` on the share first.
- What remains broken:
  - no notebook rerun was done in this session, so live UNC access still needs confirmation in the user environment.
- Remaining in-slice work:
  - rerun the single-fish setup path stage on Windows if you want end-to-end confirmation against the NAS share.
- Next likely breakpoint:
  - execute notebook setup through the context stage and confirm `NAS_ROOT`, `DATA_ROOT`, and downstream derived paths for the target fish.
- Rerun implications:
  - minimum rerun: setup/path cells `[1]-[5]`.

### 2026-04-26 - top-10 single-fish notebook cells slimmed to package-backed wrappers

- Slice goal:
  - reduce the 10 largest `2PF_to_HCR.ipynb` code cells to thin orchestration wrappers while preserving current outputs and stage tags.
- Passes completed in this session:
  - moved legacy bodies for `[22c]`, `[50]`, `[50e]`, `[53]`, `[53a]`, `[56]`, `[56f-qc]`, `[56f-qc-activity]`, and `[56h]` into package-owned migration shims.
  - rewrote `[50l]` to call `plots.analysis.render_single_fish_50l_composite` directly.
  - updated public exports, symbol docs, package-export coverage, and notebook contract tests for the composite `[50l]` contract.
- What changed:
  - targeted cells now range from 9 to 60 nonblank lines and define no top-level helpers.
  - live notebook top-level-definition count dropped to 31; remaining contract violations are outside this top-10 milestone.
- What remains broken:
  - static contract still reports figure/required-cell issues in non-target cells `[50f]`, `[50g]`, `[56g]`, `[57]`, and `[57b-anatomy-coexpression-summary]`.
- Remaining in-slice work:
  - replace migration shims with explicit package stage APIs in later passes, starting with trace/midline `[56*]` and HCR status `[50e]`.
- Next likely breakpoint:
  - refactor the remaining non-target figure cells to package renderers, then retire the shimmed legacy bodies incrementally.
- Rerun implications:
  - minimum rerun for this milestone is unchanged by design; rerun the affected notebook cells as needed to regenerate their existing outputs.

### 2026-04-26 - remaining single-fish figure-contract cells slimmed

- Slice goal:
  - apply the same wrapper cleanup to `[50f]`, `[50g]`, `[56g]`, `[57]`, and `[57b-anatomy-coexpression-summary]`.
- Passes completed in this session:
  - moved legacy bodies for `[50f]`, `[50g]`, `[56g]`, and `[57]` into package-owned migration shims.
  - rewrote those notebook cells to import and call their owning `plots.*` stage runners.
  - changed `[57b-anatomy-coexpression-summary]` to directly import `render_single_fish_hcr_anatomy_coexpression_summary`.
  - updated notebook contract expectations, package exports, symbol docs, and focused regression tests.
- What changed:
  - targeted cells now range from 9 to 36 nonblank lines and define no top-level helpers.
  - `check_notebook_contract("notebooks/2PF_to_HCR.ipynb")` reports zero figure violations and zero required-cell violations.
- What remains broken:
  - live notebook rerun/visual confirmation is still required for the affected QA/trace figures.
- Remaining in-slice work:
  - replace migration shims with explicit package stage APIs in later passes once the notebook stays stable.
- Next likely breakpoint:
  - retire shimmed legacy bodies one owner at a time, starting with `plots.qa` `[50f]`/`[50g]` and `plots.analysis` `[56g]`/`[57]`.
- Rerun implications:
  - rerun only the affected cells when their existing outputs need regeneration.
