# Recent Changes - Single-Fish Workflow

**Purpose:** rolling handoff log for meaningful single-fish work, remaining breakpoints, and rerun implications.

**Use this file when:** work targets `notebooks/singleFish.ipynb` or single-fish ownership modules.

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

### 2026-06-16 - voxel-scale and affine-offset probe for HCR `[50]` audit

- Slice goal:
  - move the non-promoted HCR `[50]` disk recompute closer to the notebook/internal-control output without promoting mismatching recomputed tables.
- Passes completed in this session:
  - confirmed persisted `*_func_mask_in_2p.tif` masks and `iscell`-only label reconstruction are worse than the current all-ROI affine replay.
  - confirmed shared legacy/recomputed candidates had identical saved raw/anatomy centroid columns but different overlap and distance values, pointing to warped-label replay and notebook voxel-scale parity rather than HCR target recovery.
  - updated `_write_stage_hcr_recompute_audit` to pass anatomy `DX/DY` from `voxel_sizes.json` into `build_hcr_activity_tables`.
  - added a diagnostic single-plane affine offset replay family (`dx=-1`, `dy=+1`) across selected-best-Z affine refs, plus focused tests for the offset helper and anatomy voxel-cache lookup.
- What changed:
  - On `L395_f11`, the selected non-promoted replay is now `tforms_by_plane_csv_selected_inplane_best_z_single_plane_dx-1_dy+1_p4`.
  - Its row-delta total is now `1`: `hcr_func_candidates=148` versus legacy `148`, `conf_to_func_pairs=40` versus legacy `40`, `conf_to_func_pairs_raw=253` versus legacy `254`, and `hcr_activity_status=162` versus legacy `162`.
  - Candidate-key overlap improves to `126/148`, with `22` missing and `22` extra; all `56` legacy HCR/anatomy target groups remain represented, but `24` target groups still have different functional ROI sets.
  - `assign-hcr-identity`, `export-canonical-tables`, and strict `compare-staged --stage-name export-canonical-tables` pass on `L395_f11`; canonical HCR exports remain parity-preserving staged tables.
- What remains broken:
  - The recomputed `[50]` family still does not exactly match internal control: raw candidates are one row short, table schemas still differ, and 22 candidate keys remain swapped/missing.
  - The plane-4 offset probe is diagnostic evidence of a small affine replay/coordinate-frame mismatch, not a promoted canonical correction.
- Remaining in-slice work:
  - recover the exact notebook-side plane-ref/warped-label state used by legacy `[50]`, or encode a principled transform replay contract that eliminates the remaining one raw row and 22 key differences.
- Next likely breakpoint:
  - inspect the `p4` offset replay geometry diff rows in `hcr_func_candidate_target_geometry_by_replay_variant.csv`, especially the one extra HCR/anatomy target group and the 24 target groups with differing functional ROI sets.
- Rerun implications:
  - after HCR audit changes, rerun `assign-hcr-identity`, then `export-canonical-tables`, then `compare-staged --stage-name export-canonical-tables` for the control fish.

### 2026-06-16 - transform-replay variant scoreboard for HCR `[50]` audit

- Slice goal:
  - test whether selected `[20]` `ants_rigid_affine` transformlists can improve the non-promoted `assign-hcr-identity` HCR `[50]` disk replay, without promoting mismatching recomputed tables.
- Passes completed in this session:
  - added a SimpleITK fallback for `matching.resample_labels_nn` / `resample_image` when replaying single-file ANTs affine transformlists and ANTsPy is unavailable.
  - taught `_plane_refs_from_tforms_csv` to reconstruct selected `ants_rigid_affine` plane refs from `ncc/inplane_registration_comparison/inplane_registration_comparison.csv`, including `ants_transform`, `ants_transformlist`, and `ref_scaled_shape`.
  - added a mixed replay variant that keeps `tforms_by_plane.csv` affine matrices but uses selected `[20]` best-Z values from `inplane_registration_comparison.csv`.
  - changed `_write_stage_hcr_recompute_audit` to score available transform replay variants and write `hcr_transform_replay_variants.csv`, while selecting the closest variant for the main non-promoted recomputed outputs.
  - added `hcr_func_candidate_key_diff_by_replay_variant.csv`, `hcr_func_candidate_target_set_diff_by_replay_variant.csv`, `hcr_func_candidate_target_geometry_by_replay_variant.csv`, and key/target summary columns to the transform replay scoreboard.
  - added focused pipeline tests for selected ANTs transformlist plane-ref reconstruction, HCR candidate key-diff summaries, and per-target functional ROI set diffs.
- What changed:
  - On `L395_f11`, the audit now records three transform replay variants:
    - `tforms_by_plane_csv_selected_inplane_best_z`: row-delta total `14`; `hcr_func_candidates=154`, `conf_to_func_pairs_raw=258`, `conf_to_func_pairs=44`, `hcr_activity_status=162`; candidate-key overlap `119/148`, missing `29`, extra `35`; target overlap `56/56`, with `23` target groups sharing the exact functional ROI set and `33` target groups differing in `hcr_func_candidate_target_set_diff_by_replay_variant.csv`.
    - `tforms_by_plane_csv`: row-delta total `16`; `hcr_func_candidates=155`, `conf_to_func_pairs_raw=259`, `conf_to_func_pairs=44`, `hcr_activity_status=162`.
    - `selected_inplane_registration_comparison_ants_transformlist`: row-delta total `104`; `hcr_func_candidates=100`, `conf_to_func_pairs_raw=221`, `conf_to_func_pairs=17`, `hcr_activity_status=162`; candidate-key overlap `0/148`, missing `148`, extra `100`.
  - The main non-promoted recompute outputs now use the closer `tforms_by_plane_csv_selected_inplane_best_z` replay, and the manifest records `plane_refs_variant_count=3` plus the variants CSV path.
  - `assign-hcr-identity`, `export-canonical-tables`, and strict `compare-staged --stage-name export-canonical-tables` still pass on `L395_f11`; canonical HCR exports remain parity-preserving staged tables.
- What remains broken:
  - Neither persisted replay variant exactly reproduces legacy `[50]`; selected ANTs transformlists replayed via SimpleITK are substantially worse than the flattened affine CSV for this control fish.
  - The closest replay recovers the same HCR/anatomy target groups, but not the exact functional ROI candidate sets, so the remaining gap appears to be label-warp/candidate-set reconstruction rather than HCR final-pair recovery.
- Remaining in-slice work:
  - instrument or replay legacy notebook `[20]`/`[50]` more directly to compare in-memory `plane_refs[*].ref_match`, `ref_scaled_shape`, ANTs image metadata, and functional-label warps against both persisted variants.
- Next likely breakpoint:
  - use `hcr_func_candidate_target_set_diff_by_replay_variant.csv` to compare per-target functional ROI sets and warped label images for planes `3` and `4`, where the closest replay's missing/extra candidate keys are concentrated, or persist notebook-side `plane_refs` transform/image metadata immediately after `[20]`.
- Rerun implications:
  - after transform replay changes, rerun `PYTHONPATH=src python tools/single_fish_pipeline.py assign-hcr-identity --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict`.
  - rerun `export-canonical-tables` and `compare-staged --stage-name export-canonical-tables` before any HCR recompute promotion.

### 2026-06-16 - notebook-equivalent functional orientation in HCR `[50]` recompute audit

- Slice goal:
  - make the non-promoted `assign-hcr-identity` HCR `[50]` disk replay use the same functional orientation convention as notebook `[50]`.
- Passes completed in this session:
  - traced notebook `[50]` and confirmed it calls `build_hcr_activity_tables` with `apply_func_orientation_func=_apply_func_orientation`.
  - updated `_write_stage_hcr_recompute_audit` to pass `spatial.apply_func_orientation(..., polarity=resolved_polarity, flip_x=True)` into the HCR candidate rebuild.
  - recorded `functional_orientation_polarity` and `functional_orientation_source` in the recompute audit input manifest payload.
  - tested persisted transform alternatives with the orientation fix: `registration/tforms_by_plane.csv` forward is still best; `ncc/tforms_by_plane.csv`, inverse matrices, and a local SciPy/skimage read of saved ANTs `.mat` affine parameters do not reproduce legacy keys.
- What changed:
  - On `L395_f11`, the non-promoted recompute audit moved much closer to legacy `[50]`:
    - `hcr_func_candidates`: `155` recomputed rows versus legacy `148` (previously `73`).
    - `conf_to_func_pairs_raw`: `259` recomputed rows versus legacy `254` (previously `198` after finalization-only pass).
    - `conf_to_func_pairs`: `44` recomputed rows versus legacy `40` (previously `22`).
    - `hcr_activity_status`: still `162` rows versus legacy `162`, with `50` columns versus legacy `109`.
  - Candidate key overlap for the best replay is `119/148` legacy keys, with `29` missing and `36` extra.
  - `assign-hcr-identity`, `export-canonical-tables`, and strict `compare-staged --stage-name export-canonical-tables` still pass on `L395_f11` because recomputed HCR tables remain non-promoted.
- What remains broken:
  - HCR `[50]` recompute parity is still not exact; the remaining mismatch is now small but real and should not be promoted.
  - ANTs `.mat` files are readable with SciPy, but naive skimage affine-center replay produced zero legacy key overlap in the tested coordinate formulas, so a correct ANTs coordinate/spacing replay contract is still needed before using those files.
- Remaining in-slice work:
  - recover the exact legacy functional-label warp representation for `[50]`, likely by comparing notebook in-memory `plane_refs` after `[20]` with persisted `tforms_by_plane.csv` and saved ANTs transform metadata.
- Next likely breakpoint:
  - run or instrument the legacy notebook state around `[20]`/`[50]` to persist enough `plane_refs` transform metadata (`tform_src`, transform type, moving/fixed shape, spacing/origin/direction, and any masked-ANTs transformlist) for package replay.
- Rerun implications:
  - rerun `PYTHONPATH=src python tools/single_fish_pipeline.py assign-hcr-identity --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict` after transform replay changes.
  - rerun `export-canonical-tables` and `compare-staged --stage-name export-canonical-tables` before any HCR recompute promotion.

### 2026-06-16 - response-aware finalization for non-promoted HCR `[50]` recompute audit

- Slice goal:
  - move the staged HCR `[50]` disk-recompute audit closer to notebook export semantics without promoting mismatching recomputed tables.
- Passes completed in this session:
  - added `matching.finalize_hcr_activity_export_tables` as a package-owned response-aware finalizer for HCR status/raw/trace/candidate export tables.
  - wired the `assign-hcr-identity` recompute audit through that finalizer after lower-level `build_hcr_activity_tables` candidate generation.
  - added focused package/export tests for preserving accepted HCR labels without functional candidates in the raw export.
  - ran `assign-hcr-identity`, `export-canonical-tables`, and strict staged export comparison on `L395_f11`.
- What changed:
  - The non-promoted recompute audit now retains no-candidate accepted labels in `conf_to_func_pairs_raw_recomputed.csv` and writes response-aware status/raw/trace columns from package code.
  - On `L395_f11`, audit `conf_to_func_pairs_raw` improved from `73` rows to `198` rows versus legacy `254`; `hcr_activity_status` remains `162` rows but now has `50` columns versus legacy `109`; `conf_to_func_pairs` remains `22` rows versus legacy `40`; `hcr_func_candidates` remains `73` rows versus legacy `148`.
  - Canonical HCR exports remain parity-preserving staged tables; `export-canonical-tables` and `compare-staged --stage-name export-canonical-tables --strict` still passed on `L395_f11`.
- What remains broken:
  - The recomputed candidate geometry still does not match legacy `[50]`; the remaining count gap is upstream of response-aware finalization.
  - The exact in-memory plane-ref/transform or label-warp representation used by legacy `[50]` is still missing from the staged disk replay.
- Remaining in-slice work:
  - recover the plane-ref/functional-label replay contract needed to make `hcr_func_candidates` match legacy before promoting recomputed HCR-centric tables.
- Next likely breakpoint:
  - compare legacy in-memory `plane_refs`/functional label warps against persisted `tforms_by_plane.csv`, saved ANTs transform files, and `ROI_transformed` outputs; the current affine CSV variants do not reproduce the missing candidates, ROI-master joins recover only `47/148` legacy candidate keys, and `plane_links.csv` joins produce different functional-label keys rather than the missing legacy candidates.
- Rerun implications:
  - rerun `PYTHONPATH=src python tools/single_fish_pipeline.py assign-hcr-identity --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict` after finalizer or transform replay changes.
  - rerun `export-canonical-tables` and `compare-staged --stage-name export-canonical-tables` after any promotion/source-precedence change.

### 2026-06-16 - regenerated staged `[50e]` HCR status summary

- Slice goal:
  - move `hcr_activity_status_summary.csv` from copied legacy output to a package-owned staged recompute while keeping the larger `[50]` table family staged behind the identity boundary.
- Passes completed in this session:
  - added staged summary generation from `hcr_activity_status.csv` and HCR warp metadata `filter_stats`.
  - used `n_labels_after - len(low_conf_labels)` per mask as the high-quality mask count for unmatched rows, matching the legacy `[50e]` rule.
  - recorded `hcr_activity_status_summary_owner`, `hcr_hq_mask_counts_by_gene`, `hcr_hq_mask_count_source_count`, and `hcr_warp_filter_metadata_json` manifest inputs.
  - updated focused tests so the staged summary includes an unmatched row generated from metadata rather than copied from the source summary.
- What changed:
  - `pipeline_outputs/assign-hcr-identity/registration/hcr_activity_status_summary.csv` is now regenerated by the staged identity stage.
  - On `L395_f11`, the regenerated staged summary exactly matches the legacy registration summary; manifest counts were `{'pth2': 20, 'sst1.1': 153, 'sst1.2': 13, 'tac3b': 77}` from four warp metadata files.
  - `export-canonical-tables` and `make-figures` still pass after consuming the regenerated summary through the staged canonical bundle.
- What remains broken:
  - `hcr_activity_status.csv`, `conf_to_func_pairs.csv`, `conf_to_func_pairs_raw.csv`, and `hcr_func_candidates.csv` are still staged copies, not promoted recomputes.
  - the non-promoted HCR disk recompute audit still does not reproduce legacy `[50]` candidate/pair counts.
- Remaining in-slice work:
  - recover the exact transform/plane-ref representation needed for `matching.build_hcr_activity_tables` to match legacy `[50]` outputs.
- Next likely breakpoint:
  - inspect how legacy `plane_refs` carries `ants_transform`/`ref_scaled_shape` versus what is persisted in `tforms_by_plane.csv` and in-plane registration transform files.
- Rerun implications:
  - rerun `PYTHONPATH=src python tools/single_fish_pipeline.py assign-hcr-identity --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict` after HCR warp metadata or status tables change.
  - rerun `export-canonical-tables`, `compare-staged --stage-name export-canonical-tables`, `make-figures`, and `compare-staged --stage-name make-figures` after staged summary changes.

### 2026-06-16 - non-promoted HCR `[50]` disk-recompute audit

- Slice goal:
  - make the remaining HCR-centric recompute gap measurable from staged pipeline outputs without promoting incorrect recomputed tables.
- Passes completed in this session:
  - added a best-effort disk recompute audit to `assign-hcr-identity` using persisted `tforms_by_plane.csv`, Suite2p plane files, structural anatomy labels, HCR final-pair CSVs, and the staged scored ROI master as response lookup.
  - wrote audit outputs under `03_analysis/functional/pipeline_outputs/assign-hcr-identity/recompute-audit/`.
  - recorded `hcr_recompute_audit_status`, reason, inputs, comparison summary, and promotion flag in `assign-hcr-identity_manifest.json`.
  - kept canonical HCR exports on the parity-preserving staged `[50]` table path unless the recompute audit fully matches.
- What changed:
  - On `L395_f11`, the audit runs and records `hcr_recompute_audit_status=different`, `hcr_recompute_audit_promoted=False`.
  - Audit inputs found:
    - `registration/tforms_by_plane.csv`
    - structural anatomy Cellpose labels
    - staged scored `functional_roi_activity_identity.csv`
    - Suite2p root
    - four HCR final-pair CSVs
  - Audit comparison result on `L395_f11`:
    - `hcr_activity_status`: `162` recomputed rows vs `162` legacy rows, but only `24` columns vs legacy `109`.
    - `conf_to_func_pairs_raw`: `73` recomputed rows vs `254` legacy rows.
    - `conf_to_func_pairs`: `22` recomputed rows vs `40` legacy rows.
    - `hcr_func_candidates`: `73` recomputed rows vs `148` legacy rows.
  - `export-canonical-tables` and strict staged export comparison still pass because the mismatching recompute audit is not promoted.
- What remains broken:
  - persisted affine matrices in `tforms_by_plane.csv` plus current disk inputs do not reproduce the legacy notebook candidate geometry; the exact in-memory plane-ref/transform representation used by `[50]` is still missing from the staged recompute path.
  - HCR-centric canonical exports remain staged parity tables, not recomputed outputs.
- Remaining in-slice work:
  - recover or persist the exact transform representation consumed by notebook `[50]`, or change earlier registration stages to write a replayable transform contract that reproduces legacy candidate geometry.
- Next likely breakpoint:
  - inspect registration stage outputs around `[20]` and `plane_refs` persistence, especially whether ANTs transform lists or masked-registration transform objects were used but collapsed into `tforms_by_plane.csv`.
- Rerun implications:
  - rerun `PYTHONPATH=src python tools/single_fish_pipeline.py assign-hcr-identity --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict` after registration transform persistence changes.
  - inspect `03_analysis/functional/pipeline_outputs/assign-hcr-identity/recompute-audit/hcr_recompute_legacy_comparison.csv` before promoting recomputed HCR tables.
  - rerun `export-canonical-tables` and `compare-staged --stage-name export-canonical-tables` after any HCR table promotion logic changes.

### 2026-06-16 - staged identity/HCR table boundary at `assign-hcr-identity`

- Slice goal:
  - move ROI identity master and HCR-centric `[50]` table consumption behind the staged identity boundary so score, canonical export, and figures no longer read those tables directly from the live registration folder when staged identity outputs exist.
- Passes completed in this session:
  - staged `functional_roi_activity_identity.csv` and `anatomy_identity_lookup.csv` under `pipeline_outputs/assign-hcr-identity/registration/`.
  - added staged HCR table copying in `run_single_fish_assign_hcr_identity_stage` for `hcr_activity_status.csv`, `conf_to_func_pairs.csv`, `conf_to_func_pairs_raw.csv`, `hcr_func_candidates.csv`, and `hcr_activity_status_summary.csv`.
  - made `score-activity-bpi` prefer the staged identity master when present.
  - changed canonical source resolution so HCR-centric labels prefer `pipeline_outputs/assign-hcr-identity/registration/` when available.
  - recorded `staged_hcr_identity_table:*` outputs in the identity manifest and `staged_hcr_identity_source_labels` in the canonical export manifest.
  - updated focused tests, roadmap notes, state docs, stage map, and symbol docs.
- What changed:
  - `assign-hcr-identity` now writes staged ROI identity and HCR-centric outputs under `03_analysis/functional/pipeline_outputs/assign-hcr-identity/registration/`.
  - `score-activity-bpi` records `identity_master_source` pointing at the staged identity master when present.
  - `export-canonical-tables` now promotes those staged HCR-centric outputs into `03_analysis/functional/pipeline_outputs/export-canonical-tables/registration/` when present.
  - On `L395_f11`, `assign-hcr-identity`, `score-activity-bpi`, `compare-staged --stage-name score-activity-bpi`, `export-canonical-tables`, `compare-staged --stage-name export-canonical-tables`, `make-figures`, and `compare-staged --stage-name make-figures` all passed after the source-precedence change.
  - Strict staged export comparison matched all HCR-centric tables exactly and retained the known tolerated numeric-only differences for ROI/BPI outputs.
- What remains broken:
  - HCR-centric table generation is not yet a raw recompute from `build_hcr_activity_tables` inputs; this slice stages the current `[50]` exports behind a manifest boundary so the source can be replaced later.
  - not-yet-extracted final figure artifacts (`compound_50j_56i_unified.*`, `bpi_all_pairs.png`, `per_gene_stimulus_trace_with_hcr_status_56h.png`) are still copied from legacy `04_plots`.
- Remaining in-slice work:
  - reconstruct enough disk-backed inputs for `matching.build_hcr_activity_tables` to generate the HCR-centric table family directly under `assign-hcr-identity`.
- Next likely breakpoint:
  - inspect package loaders for Suite2p plane data, anatomy labels, plane refs/tforms, and HCR final-pair records so `[50]` table generation can move from staged copies to package recomputation.
- Rerun implications:
  - after HCR table source changes, rerun `PYTHONPATH=src python tools/single_fish_pipeline.py assign-hcr-identity --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict`.
  - rerun `PYTHONPATH=src python tools/single_fish_pipeline.py export-canonical-tables --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict`.
  - rerun `PYTHONPATH=src python tools/single_fish_pipeline.py compare-staged --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --stage-name export-canonical-tables --strict`.
  - rerun `PYTHONPATH=src python tools/single_fish_pipeline.py make-figures --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict` and `compare-staged --stage-name make-figures` after staged canonical export source changes.

### 2026-06-16 - staged `make-figures` consumes canonical exports

- Slice goal:
  - make package-rendered final figures consume staged canonical table exports rather than reaching back to legacy registration tables.
- Passes completed in this session:
  - added figure table source resolution that prefers `pipeline_outputs/export-canonical-tables/registration/` for `functional_roi_activity_identity.csv`, `conf_to_func_pairs.csv`, and `hcr_activity_status.csv` when the staged canonical export bundle is complete.
  - bridged staged canonical tables through a temporary fish-shaped registration folder only for the existing responsive-donut renderer, whose cohort delegate expects fish-like paths.
  - recorded `figure_table_source:*` inputs and `figure_table_sources` in the `make-figures` manifest.
  - added CSV ignored-column support to `StagedOutputComparisonSpec` and ignored only responsive donut count provenance columns (`owner`, `master_csv`, `conf_func_csv`) for that comparison.
  - updated focused tests, roadmap notes, and symbol docs.
- What changed:
  - `make-figures` now renders `single_fish_50l_responsive_identity_donut.*` and `single_fish_hcr_anatomy_coexpression_summary.*` from staged canonical exports when available.
  - On `L395_f11`, `make-figures_manifest.json` records figure table sources under `03_analysis/functional/pipeline_outputs/export-canonical-tables/registration/`.
  - strict staged comparison passed after the source switch:
    - `single_fish_50l_composite_png`: exact visual thumbnail match, MAE/RMS `0`/`0`.
    - `responsive_identity_donut_png`: visually tolerated, MAE/RMS `0.0138299`/`0.0655831`.
    - `responsive_identity_donut_counts`: match after ignoring source-provenance columns; biological count columns match.
    - `anatomy_coexpression_summary_png`: visually tolerated, MAE/RMS `0.00668299`/`0.0379834`.
    - `anatomy_coexpression_summary_csv`: exact semantic match.
- What remains broken:
  - the responsive donut renderer still needs a temporary fish-shaped bridge when reading arbitrary staged table paths; a future plot-owner cleanup could remove that assumption.
  - not-yet-extracted final figure artifacts (`compound_50j_56i_unified.*`, `bpi_all_pairs.png`, `per_gene_stimulus_trace_with_hcr_status_56h.png`) are still copied from legacy `04_plots`.
  - HCR-centric canonical table generation upstream remains copy-based.
- Remaining in-slice work:
  - either extract the remaining final figure families or migrate HCR-centric table writers feeding the canonical export bundle.
- Next likely breakpoint:
  - migrate the HCR-centric `[50]` identified-cell tables (`hcr_activity_status`, `conf_to_func_pairs`, candidates/status summary) into staged package output generation.
- Rerun implications:
  - rerun `score-activity-bpi`, `export-canonical-tables`, then `make-figures` when staged canonical inputs change.
  - rerun `PYTHONPATH=src python tools/single_fish_pipeline.py compare-staged --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --stage-name make-figures --strict` after figure source or comparison-spec changes.

### 2026-06-16 - staged canonical export consumes score outputs

- Slice goal:
  - move `export-canonical-tables` away from pure legacy-copy behavior for ROI/BPI tables by consuming the staged `score-activity-bpi` outputs.
- Passes completed in this session:
  - added canonical source resolution that prefers `pipeline_outputs/score-activity-bpi/registration/` for `roi_master`, `bpi_cells`, and `bpi_summary`.
  - kept HCR-centric canonical tables sourced from legacy registration CSVs until their writer stages are migrated.
  - updated focused tests, roadmap notes, and symbol docs.
  - ran real-data staged `export-canonical-tables` and `compare-staged --stage-name export-canonical-tables` on `L395_f11`.
- What changed:
  - `export-canonical-tables` now copies the staged scored ROI master and BPI diagnostics into `03_analysis/functional/pipeline_outputs/export-canonical-tables/registration/` when staged score outputs exist.
  - the manifest records `staged_score_source_labels` for the ROI/BPI table labels that came from staged score outputs.
  - On `L395_f11`, strict staged comparison passed:
    - `functional_roi_activity_identity`: tolerated numeric-only differences within configured tolerance, max absolute `6.40539`, max relative `0.00613968`.
    - `functional_roi_activity_bpi_cells`: tolerated numeric-only differences within configured tolerance, max absolute `6.40539`, max relative `0.00613968`.
    - `functional_roi_activity_bpi_summary`: exact semantic match.
    - HCR-centric tables (`hcr_activity_status`, `conf_to_func_pairs`, `conf_to_func_pairs_raw`, `hcr_func_candidates`, `hcr_activity_status_summary`) matched.
- What remains broken:
  - HCR-centric canonical exports are still copied from legacy registration outputs.
  - downstream `make-figures` still reads legacy registration tables directly for package-rendered figures rather than consuming the staged canonical export folder.
- Remaining in-slice work:
  - migrate HCR identity/activity table generation or adapt downstream figure/report consumers to read staged canonical exports.
- Next likely breakpoint:
  - change `make-figures` package-rendered figure inputs to consume `pipeline_outputs/export-canonical-tables/registration/`, or migrate HCR-centric table writers feeding canonical export.
- Rerun implications:
  - rerun `score-activity-bpi` before `export-canonical-tables` when staged BPI outputs change.
  - rerun `PYTHONPATH=src python tools/single_fish_pipeline.py export-canonical-tables --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict`.
  - then rerun `PYTHONPATH=src python tools/single_fish_pipeline.py compare-staged --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --stage-name export-canonical-tables --strict`.

### 2026-06-16 - staged `score-activity-bpi` package-generated outputs

- Slice goal:
  - move `score-activity-bpi` from copying legacy `[50ia]` BPI CSVs to generating staged response/BPI tables through the package activity owner.
- Passes completed in this session:
  - added staged BPI output generation in `run_single_fish_score_activity_bpi_stage` using `codeants_2pf_hcr.activity.build_response_bpi_tables`.
  - made the stage prefer staged `[23c]` pre-identity response calls from `pipeline_outputs/preprocess-functional/qa/suite2p_response_bpi_cells_23c.csv`, with the legacy QA path as a fallback.
  - changed the stage audit to validate the staged BPI cells, summary, and scored master table rather than the legacy registration BPI files.
  - updated focused tests, roadmap notes, and symbol docs.
  - ran real-data staged `score-activity-bpi` and `compare-staged --stage-name score-activity-bpi` on `L395_f11`.
- What changed:
  - `score-activity-bpi` now writes:
    - `03_analysis/functional/pipeline_outputs/score-activity-bpi/registration/functional_roi_activity_identity.csv`
    - `03_analysis/functional/pipeline_outputs/score-activity-bpi/registration/functional_roi_activity_bpi_cells.csv`
    - `03_analysis/functional/pipeline_outputs/score-activity-bpi/registration/functional_roi_activity_bpi_summary.csv`
  - the manifest records `scoring_owner=codeants_2pf_hcr.activity.build_response_bpi_tables`, the staged output directory, the precomputed response source, and the stimulus source.
  - the staged path uses notebook-equivalent `FUNC_ACTIVITY_BPI_ZERO_BAND=0.50`.
  - On `L395_f11`, strict staged comparison passed:
    - `functional_roi_activity_bpi_cells`: tolerated numeric-only differences within configured tolerance, max absolute `6.40539`, max relative `0.00613968`.
    - `functional_roi_activity_bpi_summary`: exact semantic match.
- What remains broken:
  - this is not yet a fully raw-trace recomputation in the common successful path, because it reuses the staged `[23c]` pre-identity response calls when available.
  - `export-canonical-tables` still copies legacy registration CSVs rather than composing canonical staged outputs from the staged score products and upstream staged tables.
- Remaining in-slice work:
  - update `export-canonical-tables` so BPI-related canonical exports come from `pipeline_outputs/score-activity-bpi/registration/` rather than legacy registration files.
- Next likely breakpoint:
  - migrate `export-canonical-tables` from copy-only behavior toward staged canonical table assembly, starting with the ROI master and BPI cells/summary.
- Rerun implications:
  - rerun `PYTHONPATH=src python tools/single_fish_pipeline.py preprocess-functional --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict --run-23c` when early Suite2p response calls change.
  - rerun `PYTHONPATH=src python tools/single_fish_pipeline.py score-activity-bpi --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict`.
  - then rerun `PYTHONPATH=src python tools/single_fish_pipeline.py compare-staged --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --stage-name score-activity-bpi --strict`.

### 2026-06-16 - staged `make-figures` package-rendered outputs

- Slice goal:
  - move final figure staging from pure legacy-copy parity toward package-owned rendering from canonical tables.
- Passes completed in this session:
  - added package-rendering behavior inside `run_single_fish_make_figures_stage` for the single-fish responsive identity donut and HCR anatomy coexpression summary.
  - added optional per-artifact visual tolerance fields to `StagedOutputComparisonSpec` and used a donut-specific RMS tolerance for Matplotlib antialiasing/layout drift.
  - updated focused tests, roadmap notes, and symbol docs.
  - ran real-data staged `make-figures` and `compare-staged --stage-name make-figures` on `L395_f11`.
- What changed:
  - `make-figures` now regenerates `single_fish_50l_responsive_identity_donut.*` from `functional_roi_activity_identity.csv` plus `conf_to_func_pairs.csv` into `03_analysis/functional/pipeline_outputs/make-figures/04_plots/`.
  - `make-figures` now regenerates `single_fish_hcr_anatomy_coexpression_summary.*` from `hcr_activity_status.csv` into the same staged folder.
  - the stage still copies not-yet-extracted figure artifacts, including `compound_50j_56i_unified.*`, `bpi_all_pairs.png`, and `per_gene_stimulus_trace_with_hcr_status_56h.png`.
  - On `L395_f11`, strict staged comparison passed:
    - `single_fish_50l_composite_png`: exact visual thumbnail match, MAE/RMS `0`/`0`.
    - `responsive_identity_donut_png`: visually tolerated, MAE/RMS `0.0138299`/`0.0655831`; source counts CSV matched semantically.
    - `anatomy_coexpression_summary_png`: visually tolerated, MAE/RMS `0.00668299`/`0.0379834`; source summary CSV matched semantically.
- What remains broken:
  - this is not full end-to-end independence: several downstream stages still audit or copy legacy products, and the composite/trace/BPI figure outputs are not yet independently rendered from staged inputs.
  - visual PNG comparison remains thumbnail-based; the inspected contact sheet for the responsive donut was written to `/tmp/L395_responsive_identity_donut_legacy_vs_staged.png` during this session.
- Remaining in-slice work:
  - extract/rerun the remaining final figure families from package-owned inputs, especially `compound_50j_56i_unified.*` if the required trace/AUC inputs can be staged cleanly.
- Next likely breakpoint:
  - either migrate `score-activity-bpi` from copying `[50ia]` BPI CSVs to recomputing them, or continue `make-figures` by rendering the composite figure from staged canonical tables and trace/AUC inputs.
- Rerun implications:
  - rerun `PYTHONPATH=src python tools/single_fish_pipeline.py make-figures --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict` after canonical registration tables or final figure renderers change.
  - then rerun `PYTHONPATH=src python tools/single_fish_pipeline.py compare-staged --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --stage-name make-figures --strict`.

### 2026-06-16 - staged `register-hcr-to-anatomy` manifest scaffold

- Slice goal:
  - continue the roadmap pipeline by making legacy HCR-to-anatomy registration products explicit and auditable without touching `notebooks/singleFish.ipynb`.
- Passes completed in this session:
  - added `HcrRegistrationRecord` and `run_single_fish_register_hcr_to_anatomy_stage` in `src/codeants_2pf_hcr/pipeline.py`.
  - added `tools/single_fish_pipeline.py register-hcr-to-anatomy`.
  - added focused tests for complete HCR aligned artifacts and strict failure when a final-pairs CSV is missing.
  - updated `symbol-index.md` and this roadmap command list for the new stage.
- What changed:
  - the staged pipeline now audits `03_analysis/confocal/aligned/` for each registered HCR mask, including intensity-in-2P NRRD, label-in-2P TIFF, intensity/mask warp metadata, matches CSV, review CSV, and final-pairs CSV.
  - the stage records registered-label shape/dtype/label count and match/review/final-pair row counts in a fish-scoped manifest.
- What remains broken:
  - this stage still reuses existing BigWarp/notebook outputs; it does not recompute HCR-to-anatomy warps from raw registration transforms.
  - downstream roadmap stages from `match-roi-to-anatomy` onward are still not implemented as standalone staged pipeline commands.
- Remaining in-slice work:
  - run the new stage on `L395_f11` and use the manifest as the input contract for `assign-hcr-identity`.
- Next likely breakpoint:
  - implement the staged `match-roi-to-anatomy` command around existing geometry-only functional ROI/anatomy outputs.
- Rerun implications:
  - rerun `PYTHONPATH=src python tools/single_fish_pipeline.py register-hcr-to-anatomy --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict --hash-files` after HCR aligned outputs or review/final-pair CSVs change.

### 2026-06-16 - staged `match-roi-to-anatomy` manifest scaffold

- Slice goal:
  - continue the staged roadmap by isolating the geometry-only ROI/anatomy matching contract from later HCR identity and activity/BPI annotation.
- Passes completed in this session:
  - added `RoiAnatomyMatchRecord` and `run_single_fish_match_roi_to_anatomy_stage` in `src/codeants_2pf_hcr/pipeline.py`.
  - added `tools/single_fish_pipeline.py match-roi-to-anatomy`.
  - added tests for complete geometry artifacts and strict failure when a per-plane centroid-match CSV is missing.
  - updated `symbol-index.md` and the roadmap command scaffold.
- What changed:
  - the staged pipeline now audits `plane_links.csv`, global/per-plane `f2a_centroid_matches*.csv`, `tforms_by_plane.csv`, and required geometry columns in `functional_roi_activity_identity.csv`.
  - the stage reports per-plane ROI count, unique anatomy-match count, unique-match fraction, and centroid-match row count without using gene identity or activity to decide geometry.
- What remains broken:
  - this stage still reuses legacy `[50i]` geometry carrier outputs and does not yet rebuild ROI/anatomy matching from registered masks.
  - `assign-hcr-identity`, `score-activity-bpi`, canonical export, report, and figure stages remain to be implemented.
- Remaining in-slice work:
  - run the new command on `L395_f11` and use the resulting manifest to seed the next identity-assignment stage contract.
- Next likely breakpoint:
  - implement staged `assign-hcr-identity` around anatomy identity lookup and registered HCR final-pair outputs.
- Rerun implications:
  - rerun `PYTHONPATH=src python tools/single_fish_pipeline.py match-roi-to-anatomy --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict --hash-files` after ROI/anatomy geometry outputs change.

### 2026-06-16 - staged `assign-hcr-identity` manifest scaffold

- Slice goal:
  - continue the roadmap by separating HCR identity attachment from geometry matching and activity/BPI scoring.
- Passes completed in this session:
  - added `HcrIdentityAssignmentRecord` and `run_single_fish_assign_hcr_identity_stage` in `src/codeants_2pf_hcr/pipeline.py`.
  - added `tools/single_fish_pipeline.py assign-hcr-identity`.
  - added tests for valid identity assignment and failure when identity is assigned to an ROI without a unique anatomy match.
  - updated `symbol-index.md` and the roadmap command scaffold.
- What changed:
  - the staged pipeline now audits `anatomy_identity_lookup.csv`, ROI-level identity columns in `functional_roi_activity_identity.csv`, and HCR final-pair CSV inputs from `03_analysis/confocal/aligned/`.
  - the stage reports lookup-label count, identity-assigned ROI count, represented lookup-label count, identity labels, and the identity-after-geometry invariant.
- What remains broken:
  - this stage still reuses legacy `[50i]` identity outputs and does not yet rebuild `anatomy_identity_lookup.csv` from registered HCR final-pair CSVs.
  - `score-activity-bpi`, canonical export, report, and figure stages remain to be implemented.
- Remaining in-slice work:
  - run the new command on `L395_f11`, then begin staged `score-activity-bpi`.
- Next likely breakpoint:
  - implement staged `score-activity-bpi` around `[50ia]` response/BPI outputs.
- Rerun implications:
  - rerun `PYTHONPATH=src python tools/single_fish_pipeline.py assign-hcr-identity --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict --hash-files` after HCR final pairs or ROI identity outputs change.

### 2026-06-16 - staged `score-activity-bpi` manifest scaffold

- Slice goal:
  - continue the roadmap by isolating response/BPI scoring as the layer that annotates fixed ROI geometry and identity.
- Passes completed in this session:
  - added `ActivityBpiScoreRecord` and `run_single_fish_score_activity_bpi_stage` in `src/codeants_2pf_hcr/pipeline.py`.
  - added `tools/single_fish_pipeline.py score-activity-bpi`.
  - added tests for coherent `[50ia]` response/BPI outputs and strict failure when `functional_roi_activity_bpi_cells.csv` row keys do not match the ROI master.
  - updated `symbol-index.md` and the roadmap command scaffold.
- What changed:
  - the staged pipeline now audits `functional_roi_activity_bpi_cells.csv`, `functional_roi_activity_bpi_summary.csv`, and the response/BPI columns merged into `functional_roi_activity_identity.csv`.
  - the stage reports ROI count, response-active count, BPI-available count, response summary classes, BPI categories, and exported BPI threshold values.
- What remains broken:
  - this stage still reuses legacy `[50ia]` outputs and does not yet recompute response/BPI from Suite2p traces and stimulus timing.
  - canonical export, report, and figure stages remain to be implemented as standalone commands.
- Remaining in-slice work:
  - run the new command on `L395_f11`, then begin `export-canonical-tables`.
- Next likely breakpoint:
  - implement staged `export-canonical-tables` to make ROI-centric and HCR-centric canonical outputs explicit before QA/report/figure stages.
- Rerun implications:
  - rerun `PYTHONPATH=src python tools/single_fish_pipeline.py score-activity-bpi --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict --hash-files` after `[50ia]` response/BPI outputs change.

### 2026-06-16 - staged `export-canonical-tables` manifest scaffold

- Slice goal:
  - continue the roadmap by making the canonical single-fish table export set explicit before QA report and figure stages.
- Passes completed in this session:
  - added `CanonicalTableExportRecord` and `run_single_fish_export_canonical_tables_stage` in `src/codeants_2pf_hcr/pipeline.py`.
  - added `tools/single_fish_pipeline.py export-canonical-tables`.
  - added tests for canonical table scope recording and strict failure when a required HCR-centric table is missing.
  - updated `symbol-index.md` and the roadmap command scaffold.
- What changed:
  - the staged pipeline now audits required ROI-centric tables (`functional_roi_activity_identity.csv`, BPI cells/summary) separately from HCR-centric identified-cell exports (`hcr_activity_status.csv`, `conf_to_func_pairs.csv`, `hcr_func_candidates.csv`) and optional HCR diagnostics.
  - manifest rows include explicit `scope` labels so downstream QA and figures do not silently substitute HCR-centric exports for ROI-centric whole-population outputs.
- What remains broken:
  - this stage still reuses legacy table outputs and does not yet write a pipeline-owned copy of canonical tables.
  - `make-qa-report`, `make-figures`, and staged-vs-legacy comparisons for later stages remain to be implemented.
- Remaining in-slice work:
  - run the new command on `L395_f11`, then begin staged QA/report or figure-output auditing.
- Next likely breakpoint:
  - implement staged `make-qa-report` as a manifest/report scaffold over existing QA outputs.
- Rerun implications:
  - rerun `PYTHONPATH=src python tools/single_fish_pipeline.py export-canonical-tables --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict --hash-files` after canonical table outputs change.

### 2026-06-16 - staged `make-qa-report` manifest scaffold

- Slice goal:
  - continue the roadmap by making the existing biologist-facing QA surface explicit before building a new consolidated report artifact.
- Passes completed in this session:
  - added `QaReportArtifactRecord` and `run_single_fish_make_qa_report_stage` in `src/codeants_2pf_hcr/pipeline.py`.
  - added `tools/single_fish_pipeline.py make-qa-report`.
  - added tests for QA artifact category recording and strict failure when a required QA image is missing.
  - updated `symbol-index.md` and the roadmap command scaffold.
- What changed:
  - the staged pipeline now audits core QA artifacts for final fish audit, Suite2p stimulus response QA, ROI/anatomy geometry QA, HCR identity/activity QA, BPI QA, and optional confocal plane coverage.
- What remains broken:
  - this stage does not yet render a new consolidated HTML/PDF/markdown QA report; it only records the current legacy QA/report surface.
  - final figure stage and later-stage staged-vs-legacy comparisons remain to be implemented.
- Remaining in-slice work:
  - run the new command on `L395_f11`, then implement `make-figures`.
- Next likely breakpoint:
  - implement staged `make-figures` around existing `04_plots` outputs.
- Rerun implications:
  - rerun `PYTHONPATH=src python tools/single_fish_pipeline.py make-qa-report --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict --hash-files` after QA/report artifacts change.

### 2026-06-16 - staged `make-figures` manifest scaffold

- Slice goal:
  - complete the first-pass staged command surface by making final `04_plots` figure artifacts explicit.
- Passes completed in this session:
  - added `FigureArtifactRecord` and `run_single_fish_make_figures_stage` in `src/codeants_2pf_hcr/pipeline.py`.
  - added `tools/single_fish_pipeline.py make-figures`.
  - added tests for final figure scope recording and strict failure when a required composite figure is missing.
  - updated `symbol-index.md` and the roadmap command scaffold.
- What changed:
  - the staged pipeline now audits final single-fish figure artifacts including `[50l]` composite PNG/PDF, responsive identity donut outputs, HCR anatomy coexpression outputs, and optional BPI/trace figures.
- What remains broken:
  - this stage still reuses legacy `04_plots` outputs and does not yet rerender figures from staged canonical tables.
  - staged-vs-legacy comparison specs are still only implemented for `preprocess-functional`.
- Remaining in-slice work:
  - run the new command on `L395_f11`, then extend staged comparison specs beyond `preprocess-functional` or begin replacing audit/reuse stages with actual recomputation.
- Next likely breakpoint:
  - add staged comparison specs for canonical tables and final figures against the frozen legacy baseline.
- Rerun implications:
  - rerun `PYTHONPATH=src python tools/single_fish_pipeline.py make-figures --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict --hash-files` after final figure outputs change.

### 2026-06-16 - staged comparison specs for canonical outputs and figures

- Slice goal:
  - move beyond stage-manifest existence toward formal old-vs-new output comparison for later single-fish products.
- Passes completed in this session:
  - extended `STAGED_OUTPUT_COMPARISON_SPECS` for `score-activity-bpi`, `export-canonical-tables`, and `make-figures`.
  - added tests proving `compare_staged_single_fish_outputs` works for canonical table and final figure stages.
  - updated the roadmap note describing available `compare-staged` groups.
- What changed:
  - `compare-staged --stage-name export-canonical-tables` now checks ROI-centric and HCR-centric canonical CSV outputs against the frozen legacy baseline using semantic CSV comparison.
  - `compare-staged --stage-name make-figures` now checks key final PNG figures visually and associated counts/summary CSVs semantically.
  - `compare-staged --stage-name score-activity-bpi` now checks `[50ia]` BPI cell and summary exports semantically.
  - `score-activity-bpi`, `export-canonical-tables`, and `make-figures` now write pipeline-owned copies under `03_analysis/functional/pipeline_outputs/<stage>/...`, and the comparison specs target those staged copies rather than the original legacy paths.
- What remains broken:
  - these comparisons prove staged output files match the frozen legacy baseline, but the later stages still populate those staged outputs by copying audited legacy products rather than recomputing them independently.
  - stages after `preprocess-functional` still need true recomputation before the comparisons can prove full notebook replacement.
- Remaining in-slice work:
  - run the new comparison groups on `L395_f11`.
- Next likely breakpoint:
  - add pipeline-owned output copies or recomputation for `score-activity-bpi` / canonical tables so comparison reports measure new outputs rather than reused legacy paths.
- Rerun implications:
  - rerun `compare-staged` for `score-activity-bpi`, `export-canonical-tables`, and `make-figures` after baseline freeze or staged output path changes.

### 2026-06-15 - preprocess-functional `[23a]`/`[23c]` pipeline outputs

- Slice goal:
  - move the staged `preprocess-functional` command from inventory-only toward legacy `[23a]`/`[23c]` output recreation.
- Passes completed in this session:
  - made `run_single_fish_preprocess_functional_stage` call the package-owned `load_suite2p_stage` and write `suite2p_load_summary_23a.csv` plus `suite2p_sources_23a.csv` under `03_analysis/functional/pipeline_outputs/preprocess-functional/`.
  - added `tools/single_fish_pipeline.py preprocess-functional --run-23c`.
  - made `--run-23c` write Suite2p stimulus diagnostics under `03_analysis/functional/pipeline_outputs/preprocess-functional/qa/`, not the legacy notebook QA folder.
  - aligned the staged `[23c]` activity config to the notebook cell defaults, especially `FUNC_ACTIVITY_BPI_ZERO_BAND=0.50`.
  - added a CLI regression test so `audit-inputs` fails cleanly for missing fish after the `--run-23c` wrapper fix.
- What changed:
  - running `preprocess-functional --run-23c` on `L395_f11` now recreates the early Suite2p source, summary, response/BPI, and PNG/PDF diagnostic outputs in pipeline-owned paths.
  - after config alignment, `suite2p_response_bpi_summary_23c.csv` and `suite2p_stimulus_locked_sources_23b.csv` are byte-identical to the legacy notebook outputs for `L395_f11`.
  - `suite2p_response_bpi_cells_23c.csv` has identical shape/columns and matching text/boolean class fields; remaining differences are numeric float deltas, with the largest `bpi_z` delta observed at `6.405388980470434`.
  - `suite2p_stimulus_locked_summary_23b.csv` has identical shape/columns and only numeric deltas in `mean_z_pre`, `mean_z_post`, and `peak_z_post` with max `7.62939453125e-06`.
  - rendered `[23c]` PNG/PDF outputs are not byte-identical to legacy, despite matching source tables.
- What remains broken:
  - per-cell numeric exports and rendered figures are not yet byte-identical to the legacy notebook outputs.
  - the staged path still does not rebuild functional references, anatomy preprocessing, registration, matching, identity, activity master tables, or final figures.
- Remaining in-slice work:
  - decide whether `[23c]` equivalence should be numeric-tolerance based or byte-exact, then implement a formal staged-vs-legacy comparison report for pipeline-owned outputs.
  - continue `preprocess-functional` toward functional reference generation.
- Next likely breakpoint:
  - add comparison machinery for staged outputs versus `pipeline_baselines/L395_f11/legacy_singleFish`, then move to functional reference generation.
- Rerun implications:
  - rerun `PYTHONPATH=src python tools/single_fish_pipeline.py preprocess-functional --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict --hash-files --run-23c` after any Suite2p or `[23c]` config change.

### 2026-06-15 - preprocess-functional Suite2p inventory scaffold

- Slice goal:
  - start Phase 4 staged output recreation by adding the first functional preprocessing inventory stage.
- Passes completed in this session:
  - added `run_single_fish_preprocess_functional_stage` in `src/codeants_2pf_hcr/pipeline.py`.
  - added `tools/single_fish_pipeline.py preprocess-functional`.
  - wrote `suite2p_inventory.csv` under `03_analysis/functional/pipeline_outputs/preprocess-functional/`.
  - made the Suite2p inventory resolver handle prefixed legacy filenames and WindowsPath-bearing `ops.npy` files via the existing Suite2p loader helper.
  - added focused tests for successful inventory writing and strict failure when Suite2p planes are absent.
  - ran the stage on `L395_f11`.
- What changed:
  - `L395_f11` now has `/Volumes/dataDrive/dataProcessing/2p_processing/L395_f11/03_analysis/functional/pipeline_outputs/preprocess-functional/suite2p_inventory.csv`.
  - real inventory summary: plane0 `924` ROIs / `3120` frames / `659` Suite2p cells; plane1 `958` / `3120` / `664`; plane2 `956` / `3120` / `666`; plane3 `897` / `3120` / `621`; plane4 `795` / `3120` / `522`.
- What remains broken:
  - this stage inventories existing Suite2p products only; it does not yet rebuild functional references or reproduce `[23a]`/`[23c]` outputs.
- Remaining in-slice work:
  - compare inventory semantics against notebook `[23a]` source summaries, then add functional reference generation or Suite2p diagnostic recreation.
- Next likely breakpoint:
  - continue `preprocess-functional` toward notebook `[23a]`/`[23c]` equivalence for `L395_f11`.
- Rerun implications:
  - rerun `preprocess-functional` whenever Suite2p plane outputs change.

### 2026-06-15 - legacy baseline bundle scaffold

- Slice goal:
  - implement Phase 0 baseline freezing/comparison for the new staged single-fish pipeline.
- Passes completed in this session:
  - added declared legacy output specs for canonical tables, HCR-centric tables, early `[23c]` diagnostics, trace metadata, and key `04_plots` figures.
  - added `freeze_legacy_single_fish_baseline` and `compare_legacy_single_fish_baseline` in `src/codeants_2pf_hcr/pipeline.py`.
  - exposed `freeze-legacy-baseline` and `compare-legacy-baseline` through `tools/single_fish_pipeline.py`.
  - added focused tests for baseline copy, strict missing-required-output failure, and baseline/current difference detection.
  - froze and compared the first real baseline bundle for `L395_f11`.
- What changed:
  - `L395_f11` now has a legacy control bundle at `/Volumes/dataDrive/dataProcessing/2p_processing/pipeline_baselines/L395_f11/legacy_singleFish/`.
  - the real `compare-legacy-baseline` command reported all declared outputs as matching immediately after freeze.
- What remains broken:
  - new staged processing still does not regenerate the legacy outputs; this only freezes and verifies the current legacy control bundle.
- Remaining in-slice work:
  - implement staged `preprocess-functional`/Suite2p inventory and begin comparing generated stage products to the frozen control outputs.
- Next likely breakpoint:
  - build the next executable stage in `src/codeants_2pf_hcr/pipeline.py`, probably `preprocess-functional`, using the frozen `L395_f11` bundle as the first comparison target.
- Rerun implications:
  - no notebook rerun required. If legacy outputs change intentionally, rerun `freeze-legacy-baseline --overwrite` for the affected fish.

### 2026-06-15 - staged pipeline audit scaffold

- Slice goal:
  - start implementing the single-fish pipeline roadmap without touching `notebooks/singleFish.ipynb`.
- Passes completed in this session:
  - added `src/codeants_2pf_hcr/pipeline.py` with stage contracts, path resolution, manifest serialization, and the `audit-inputs` stage.
  - added `tools/single_fish_pipeline.py audit-inputs` as a thin wrapper.
  - added focused tests for roadmap stage order, side-effect-free path resolution, audit manifest writing, strict failure behavior, and missing-fish manifest placement.
  - exported the new pipeline API lazily from `codeants_2pf_hcr`.
- What changed:
  - the new staged path now has a first executable package-owned stage that writes `audit-inputs_manifest.json`.
  - missing-fish audits write under `<data_root>/pipeline_manifests/<fish_id>/` so a failed audit does not create or mutate the missing fish folder.
- What remains broken:
  - only the first audit scaffold exists; preprocessing, registration, matching, identity, activity, comparison-to-legacy, reports, and figures are not yet implemented in the new staged path.
- Remaining in-slice work:
  - choose representative control fish, freeze baseline notebook outputs, and implement the next package-owned stage contract against those baselines.
- Next likely breakpoint:
  - Phase 0/1 continuation from `.agents/references/single-fish-pipeline-roadmap.md`, likely baseline bundle capture plus `preprocess-functional` scaffold.
- Rerun implications:
  - no legacy notebook rerun required; new command can be tried with `PYTHONPATH=src python tools/single_fish_pipeline.py audit-inputs --fish-id <fish> --local-root <root>`.

### 2026-06-15 - staged single-fish pipeline roadmap

- Slice goal:
  - record the planned migration from notebook-led single-fish processing to a staged package/CLI pipeline while preserving `notebooks/singleFish.ipynb` as an internal control.
- Passes completed in this session:
  - added `.agents/references/single-fish-pipeline-roadmap.md`.
- What changed:
  - the roadmap now defines the target pipeline shape, stage contracts, manifests/state tracking, staged output recreation, contract tests, read-only QA reports, parallel old/new validation, and promotion criteria.
- What remains broken:
  - roadmap only; no pipeline implementation or validation changes were made.
- Remaining in-slice work:
  - choose representative control fish and freeze baseline notebook output bundles.
- Next likely breakpoint:
  - start Phase 0 from `.agents/references/single-fish-pipeline-roadmap.md`.
- Rerun implications:
  - none.

### 2026-06-15 - preserve anatomy XY in `[14a]` uint8 preprocessing

- Slice goal:
  - fix the in-plane anatomy handedness entering `[19a]` for `L395_f11`.
- Passes completed in this session:
  - changed `AnatomyUint8PreprocessingConfig` so `[14a]` preserves anatomy XY by default and only applies the functional orientation transform when explicitly requested.
  - bumped the anatomy uint8 cache version so existing functional-flipped `*_uint8.tif` sidecars are rebuilt.
  - updated focused tests and reference docs for the corrected `[14a]` cache contract.
- What changed:
  - `[14a]` still performs signed-to-uint8 conversion and default `750x750` Y/X sizing, but it no longer applies the functional flip to anatomy unless configured.
- What remains broken:
  - existing generated outputs for affected fish must be rerun; this code change does not rewrite prior registration products.
- Remaining in-slice work:
  - none in package code.
- Next likely breakpoint:
  - visual QA in `[22e]` after rerunning spatial stages.
- Rerun implications:
  - minimum rerun: `[14a] -> [8] -> [16] -> [19a] -> [20]`; rerun `[22e]` afterward to visually compare NCC and ANTs placements.
- Evidence:
  - saved functional refs matched `flipX` of the motion-corrected functional source, but their NCC scores were highest against the raw anatomy NRRD, not the `[14a]` `raw_flipX` cache.

### 2026-05-13 - single-fish notebook rename

- Slice goal:
  - rename the single-fish notebook entrypoint from `notebooks/2PF_to_HCR.ipynb` to `notebooks/singleFish.ipynb`.
- Passes completed in this session:
  - moved the notebook file.
  - updated routing docs, current state docs, smoke defaults, tests, maintenance wrappers, and BigWarp cache comments that point at the notebook path.
- What changed:
  - current single-fish workflow references should use `notebooks/singleFish.ipynb`.
- What remains broken:
  - none known from focused tests.
- Remaining in-slice work:
  - none.
- Next likely breakpoint:
  - none.
- Rerun implications:
  - no notebook rerun required for the path-only rename.

### 2026-05-12 - inferred 23c session split without preprocessing metadata

- Slice goal:
  - restore `[23c]` session 1 plotting for fish with explicit `r1`/`r2` stimulus logs but missing preprocessing metadata.
- Passes completed in this session:
  - added an equal contiguous split fallback to per-plane stimulus-context resolution.
  - propagated session-mapping provenance into `[23c]` source tables.
  - added focused resolver and Suite2p diagnostic regression tests.
- What changed:
  - when preprocessing metadata is absent and explicit session logs split evenly across requested planes, planes are assigned by sorted session order, e.g. `0-4 -> r1` and `5-9 -> r2` for ten planes.
- What remains broken:
  - none known from focused tests; saved `[23c]` figures/CSVs remain stale until rerun.
- Remaining in-slice work:
  - rerun `[23a] -> [23c]` for affected fish.
- Next likely breakpoint:
  - inspect `suite2p_stimulus_locked_sources_23b.csv` and confirm both `r1` and `r2` rows after rerun.
- Rerun implications:
  - rerun `[23c]` to regenerate `suite2p_stimulus_locked_traces_23b.*`, `suite2p_stimulus_locked_heatmaps_23b.*`, and source/summary CSVs.

### 2026-05-12 - planned-schedule 23c block timing fix

- Slice goal:
  - fix `[23c]` full-session heatmap block/stimulus placement so no-stimulus metadata blocks stay uncolored.
- Passes completed in this session:
  - added planned-schedule block/stimulus tables to stimulus metadata resolution.
  - made `[23c]` frame-grid timing use all planned-schedule blocks when available, including rest/baseline blocks.
  - added focused regression coverage for a B0 rest block followed by B1/B2 stimulus blocks.
- What changed:
  - `[23c]` heatmaps now keep scheduled rest blocks in the frame grid and draw stimulus spans only for `kind == "stimulus"` schedule rows.
- What remains broken:
  - none known from focused tests; live notebook rerun is still needed to refresh saved 23c figures.
- Remaining in-slice work:
  - none.
- Next likely breakpoint:
  - rerun `[23a] -> [23c]` for affected fish and inspect the refreshed heatmap block boundaries.
- Rerun implications:
  - rerun `[23c]` to regenerate `suite2p_stimulus_locked_heatmaps_23b.png/.pdf` and pre-identity response CSVs if downstream cells should consume the refreshed diagnostic.

### 2026-05-12 - early full-session 23c heatmap

- Slice goal:
  - make `[23c]` a first-pass fish response quality check immediately after Suite2p loading.
- Passes completed in this session:
  - replaced the `[23c]` heatmap output with a full-experiment Suite2p cell heatmap.
  - moved notebook `[23a]` and `[23c]` before `[14]`, with `[23c]` directly after `[23a]`.
  - allowed `[23a]` Suite2p loading to run before `plane_refs` exist by keying traces by discovered Suite2p plane index.
  - updated focused tests and figure/stage docs.
- What changed:
  - `[23c]` heatmaps now use frame on the X axis, all valid Suite2p-cell rows, a white-to-black activity scale, and transparent stimulus spans.
  - functional response QC can now run before anatomy/functional preprocessing cells.
- What remains broken:
  - none known from focused tests; live notebook rerun is still needed to refresh the saved 23c figures for a fish.
- Remaining in-slice work:
  - none.
- Next likely breakpoint:
  - stale saved `suite2p_stimulus_locked_heatmaps_23b.png/.pdf` files until `[23c]` is rerun.
- Rerun implications:
  - rerun `[23a] -> [23c]` before `[14]` to refresh the early fish response QC figure.

### 2026-05-09 - local root and multi-session plane metadata

- Slice goal:
  - update single-fish path/stimulus ownership for the new local data root and two-session functional acquisitions.
- Passes completed in this session:
  - made local root discovery prefer `/Users/ddharmap/dataProcessing/2p_processing`.
  - made local Cellpose model discovery prefer lowercase `cellpose/models` when present.
  - added preprocessing-metadata-driven plane-to-session stimulus context resolution.
  - updated `[50ia]` response/BPI and `[56i]` motion-AUC table construction to use per-plane session logs/metadata.
- What changed:
  - fish such as `L758_f02` now resolve planes `0-4` to r1 metadata/logs and planes `5-9` to r2 metadata/logs when preprocessing metadata records those sessions.
- What remains broken:
  - none known from package validation; live notebook rerun is still needed to refresh canonical CSVs for affected fish.
- Remaining in-slice work:
  - rerun affected single-fish notebooks from stimulus/response stages onward.
- Next likely breakpoint:
  - stale `[50ia]` or `[56i]` outputs that were generated before per-plane session metadata was applied.
- Rerun implications:
  - rerun `[50ia]` to refresh `functional_roi_activity_identity.csv` response/BPI columns, then rerun `[56i]` and downstream figures that consume motion-AUC or response-aware tables.

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

### 2026-05-09 - functional orientation stops writing full movie caches by default

- Slice goal:
  - prevent single-fish pipeline bloat from generated full-stack `_flipX.tif` functional movies while keeping oriented references, Suite2p masks, and activity outputs aligned.
- Passes completed in this session:
  - changed `[10]`/`orient_functional_stacks_stage` to audit legacy oriented movie caches by default instead of writing them.
  - changed `[12]`/`build_functional_references_stage` to build oriented 2D references from original motion-corrected stacks while preserving legacy `_flipX` reference names.
  - updated voxel mapping so original functional source paths and legacy flipped aliases share functional voxel metadata.
- What changed:
  - new runs no longer create multi-GB full oriented functional movie TIFFs unless `SAVE_ORIENTED_FUNCTIONAL_STACKS=True` is set explicitly in `[10]`.
- What remains broken:
  - existing `_flipX.tif` movie caches are audit-only in this pass; no deletion or archive action is automatic.
- Rerun implications:
  - rerun `[8] -> [10] -> [12]` to refresh voxel/source bindings and reference generation behavior; downstream spatial stages only need reruns when references are regenerated.

### 2026-05-09 - single-fish `[14a]` anatomy preprocessing now matches functional orientation

- Slice goal:
  - make `[14a]` preprocess 2P anatomy into the same gross orientation as functional data and standardize the output canvas to `750x750` pixels in Y/X.
- Passes completed in this session:
  - extended `preprocess_anatomy_uint8_stage` with metadata-driven functional orientation, default Y/X resizing to `750x750`, and cache metadata invalidation for old uint8 TIFFs.
  - updated the notebook `[14a]` wrapper to pass resolved `POLARITY` and `POLARITY_SOURCE`.
  - added focused regression coverage for intensity conversion, north-polarity orientation, default `750x750` output, and stale-cache rebuild.
- What changed:
  - `[14a]` outputs are now signed-16-bit-corrected uint8 TIFFs that match functional orientation and use a fixed `750x750` anatomy pixel grid before voxel inference.
- What remains broken:
  - no live notebook rerun was done in this session.
- Rerun implications:
  - minimum rerun: `[14a] -> [7]/[8] -> downstream spatial registration stages` for fish that still have old preprocessed anatomy caches.

### 2026-05-09 - anatomy uint8 preprocessing is rerun-idempotent

- Slice goal:
  - prevent reruns from selecting an existing preprocessed anatomy TIFF as a raw source and creating chained `*_uint8_uint8.tif` files with doubled orientation transforms.
- Passes completed in this session:
  - changed anatomy discovery to prefer raw `01_raw/2p/anatomy` inputs, then non-derived preprocessed anatomy sources, and only use existing uint8 outputs as fallback.
  - made `[14a]` reuse a uint8 input in place unless its sidecar metadata points to a raw source and force recompute is requested.
  - added regression tests for discovery priority, no chained uint8 output, and force recompute through metadata.
- What changed:
  - `[14a]` now writes/reuses one canonical `*_uint8.tif` per source and avoids applying functional orientation twice on notebook rerun.
- What remains broken:
  - existing stray `*_uint8_uint8.tif` files are not deleted automatically.
- Rerun implications:
  - rerun `[4] -> [14] -> [14a] -> [8]` to refresh path discovery and voxel reporting for affected fish.

### 2026-05-10 - Suite2p stimulus-locked diagnostic after ROI loading

- Slice goal:
  - add an early post-Suite2p diagnostic that checks raw stimulus-locked responses before identity matching and supports multi-session functional acquisitions.
- Passes completed in this session:
  - added companion stimulus metadata validation against parsed experiment logs.
  - added a package-owned `[23c]` Suite2p diagnostic with session-colored per-neuron trace panels and heatmaps.
  - updated public exports, symbol docs, stage map, and focused stimulus/diagnostic tests.
- What changed:
  - fish with `trial_sequence.csv` or `planned_schedule.csv` now use those metadata files as the authority for presented stimulus names and fail fast on mismatches with the log parse.
  - the new diagnostic preserves preprocessing-metadata plane-to-session mapping and uses raw stimulus names such as `WFCl` and `LAB_trajectory`.
- Rerun implications:
  - rerun `[23a] -> [23c]` to generate the new QA outputs; downstream identity stages are unchanged.

### 2026-05-11 - anatomy Z metadata and `[23c]` frame-grid block timing

- Slice goal:
  - fix two timing/geometry assumptions exposed by current single-fish runs.
- Passes completed in this session:
  - changed `[8]` voxel resolution so anatomy `Z_um` comes from `step_size_um_anatomy` in fish metadata, with fail-fast behavior for missing/conflicting metadata unless `VOX_ANAT_MANUAL['Z']` is set.
  - changed `[23c]` Suite2p diagnostics to place blocks on equal Suite2p frame-count boundaries and preserve log-relative stimulus offsets within each block.
  - added focused context and Suite2p diagnostic tests.
- What changed:
  - multi-page anatomy TIFFs no longer silently report `Z_um=1.0` when microscope metadata says otherwise.
  - appended block padding in the Suite2p trace no longer accumulates block-onset drift in `[23c]`.
- Rerun implications:
  - rerun `[8]` before downstream spatial stages for corrected anatomy Z.
  - rerun `[23a] -> [23c]` to regenerate the early Suite2p stimulus diagnostic.

### 2026-06-15 - staged `preprocess-functional` legacy comparison report

- Slice goal:
  - make the new staged pipeline compare its own `preprocess-functional` outputs against frozen legacy `singleFish` notebook controls without modifying `notebooks/singleFish.ipynb`.
- Passes completed in this session:
  - added `compare-staged` package/CLI support for declared stage-owned output pairs.
  - added semantic CSV comparison with row/column checks, text/bool equality, and configurable absolute/relative numeric tolerance.
  - added visual PNG thumbnail comparison for outputs declared as visual-equivalence artifacts.
  - added a focused unit test for a staged-output difference report and updated package exports/docs.
- What changed:
  - `tools/single_fish_pipeline.py compare-staged --stage-name preprocess-functional` now writes:
    - `03_analysis/functional/pipeline_outputs/preprocess-functional/preprocess-functional_legacy_comparison.csv`
    - `03_analysis/functional/pipeline_manifests/compare-staged-preprocess-functional_manifest.json`
  - On `L395_f11`, strict comparison against the frozen baseline now passes for `preprocess-functional`:
    - `suite2p_response_bpi_summary_23c.csv` exact match.
    - `suite2p_stimulus_locked_sources_23b.csv` exact match.
    - `suite2p_stimulus_locked_summary_23b.csv` tolerated numeric-only delta, max absolute `7.62939453125e-06`, max relative `1.9371509552001953e-06`.
    - `suite2p_response_bpi_cells_23c.csv` tolerated numeric-only delta, max absolute `6.405388980470434`, max relative `0.00613968161012499`; the large absolute `bpi_z` delta is confined to near-zero `denom_z` ratio amplification while response classes/categories match.
    - regenerated `[23b]` PNGs are visually tolerated by thumbnail comparison: heatmap MAE/RMS `0.00782034`/`0.0140424`, traces MAE/RMS `0.0116761`/`0.0332494`.
- What remains broken:
  - `preprocess-functional` is validated only on the current `L395_f11` control bundle; more control fish are still needed before broader promotion.
  - visual PNG comparison is thumbnail-based and should be supplemented by biologist-facing QA review/reporting in a later phase.
- Rerun implications:
  - rerun staged `preprocess-functional --run-23c` before `compare-staged` when Suite2p diagnostics or activity scoring code changes.

### 2026-06-15 - staged `preprocess-anatomy` manifest scaffold

- Slice goal:
  - move the next roadmap stage into the staged CLI without touching `notebooks/singleFish.ipynb`.
- Passes completed in this session:
  - added `run_single_fish_preprocess_anatomy_stage` around the package-owned `[14]`/`[14a]` helpers.
  - added `tools/single_fish_pipeline.py preprocess-anatomy`.
  - added focused tests for successful anatomy preprocessing manifest output and strict missing-anatomy failure.
  - updated package exports, roadmap, and symbol docs.
- What changed:
  - `preprocess-anatomy` resolves the fish anatomy source, runs/reuses NRRD-to-TIFF normalization, runs/reuses uint8 anatomy preprocessing, and writes `03_analysis/functional/pipeline_manifests/preprocess-anatomy_manifest.json`.
  - On `L395_f11`, strict real-data run passed with hashes for:
    - normalized anatomy stack: `03_analysis/functional/raw/converted_nrrd_to_tif/L395_f11_anatomy_2P_GCaMP_converted.tif`, shape `216x750x750`, dtype `uint8`, size `121535946`.
    - uint8 anatomy stack: `02_reg/00_preprocessing/2p_anatomy/L395_f11_anatomy_2P_GCaMP_uint8.tif`, shape `216x750x750`, dtype `uint8`, size `73532206`.
    - uint8 sidecar metadata: `02_reg/00_preprocessing/2p_anatomy/L395_f11_anatomy_2P_GCaMP_uint8.tif.json`.
- What remains broken:
  - this stage currently manifests/reuses the notebook-equivalent anatomy preprocessing products; there is not yet a frozen-baseline comparison class for anatomy preprocessing artifacts.
  - downstream HCR/confocal preprocessing, functional-to-anatomy registration, HCR-to-anatomy registration, ROI matching, identity assignment, canonical table export, and final figures are still not recreated by the staged pipeline.
- Rerun implications:
  - rerun staged `preprocess-anatomy --hash-files` when raw anatomy, NRRD conversion settings, or uint8 anatomy preprocessing settings change.

### 2026-06-15 - staged `preprocess-hcr` manifest scaffold

- Slice goal:
  - make HCR/confocal preprocessing inputs explicit before HCR-to-anatomy registration and identity assignment.
- Passes completed in this session:
  - fixed `collect_hcr_intensity_stack_paths` to ignore macOS dot/AppleDouble sidecar files such as `._*.nrrd`.
  - added `run_single_fish_preprocess_hcr_stage` around the package-owned HCR Cellpose discovery/reuse path.
  - added `tools/single_fish_pipeline.py preprocess-hcr`.
  - added focused tests for sidecar filtering, successful HCR mask manifesting, and strict missing-mask failure.
- What changed:
  - `preprocess-hcr` writes `03_analysis/functional/pipeline_manifests/preprocess-hcr_manifest.json` with HCR intensity inputs and corresponding raw Cellpose mask outputs.
  - On `L395_f11`, strict real-data run passed with four biological HCR stacks and four hashed masks:
    - `round2_channel2_sst1_2`: 23 labels, mask shape `40x922x922`.
    - `round2_channel3_tac3b`: 382 labels, mask shape `40x922x922`.
    - `round1_channel2_sst1_1`: 253 labels, mask shape `61x922x922`.
    - `round1_channel3_pth2`: 84 labels, mask shape `61x922x922`.
- What remains broken:
  - this stage currently manifests/reuses raw HCR masks; HCR-to-anatomy warp outputs and HCR↔anatomy matching/QC are still downstream.
  - no staged comparison spec exists yet for raw HCR mask outputs.
- Rerun implications:
  - rerun staged `preprocess-hcr --hash-files` when HCR intensity stacks, Cellpose masks, or HCR Cellpose configuration change.

### 2026-06-16 - staged `register-functional-to-anatomy` manifest scaffold

- Slice goal:
  - pin down functional-to-anatomy registration artifacts before ROI-to-anatomy matching is migrated.
- Passes completed in this session:
  - added `FunctionalRegistrationRecord` and `run_single_fish_register_functional_to_anatomy_stage`.
  - added `tools/single_fish_pipeline.py register-functional-to-anatomy`.
  - added focused tests for successful registration artifact manifesting and strict missing-artifact failure.
  - updated package exports, roadmap, and symbol docs.
- What changed:
  - `register-functional-to-anatomy` reads `03_analysis/functional/registration/tforms_by_plane.csv` as the authoritative plane list, then checks per-plane registered functional references, masks, registered images, and best-Z score CSVs.
  - The stage also records in-plane registration comparison/recommendation CSVs and the transform directory.
  - On `L395_f11`, strict real-data run passed for five planes with best-Z values `124`, `123`, `118`, `113`, and `107`.
  - The real manifest contains one input and 23 outputs: five registered functional references, five functional masks, five registered functional images, five best-Z score CSVs, plus comparison CSV, recommendation CSV, and transform directory. File hashes were recorded for file outputs.
- What remains broken:
  - this stage currently audits/reuses existing registration outputs; it does not yet recompute registration from functional references and anatomy.
  - HCR-to-anatomy registration, ROI-to-anatomy geometry matching, HCR identity assignment, response/BPI master-table export, QA reports, and final figures remain downstream.
- Rerun implications:
  - rerun staged `register-functional-to-anatomy --hash-files` when `tforms_by_plane.csv`, best-Z scores, registered functional images, or in-plane registration outputs change.
