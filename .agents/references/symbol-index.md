# Symbol Index

Generated manually for the current extracted package surface.

## Trusted patterns

- `context.py`: path/config normalization pattern.
- `stimulus.py`: explicit stage input/output pattern.
- `activity.py`: response/BPI stage-owned semantics pattern.
- `plots.*`: deterministic figure-builder pattern.
- `tools/`: wrapper pattern only, not business-logic authority.

## `codeants_2pf_hcr.context`

- `AnatomyNormalizationStageConfig`: Typed anatomy-conversion knob container for notebook cell `[14]`.
- `ContextStageConfig`: Typed setup/path knob container for notebook cell `[4]`.
- `FishStateStageConfig`: Typed fish-state marker configuration for notebook cell `[4a]`.
- `FinalFishAuditConfig`: Typed final contamination-audit configuration for `[99-debug-fish-audit]`.
- `VoxelStageConfig`: Typed voxel-resolution stage configuration for notebook cell `[8]`.
- `FunctionalOrientationStageConfig`: Typed functional-stack orientation configuration for notebook cell `[10]`.
- `resolve_fish_context`: Resolve fish-scoped roots, canonical output paths, and normalized run config.
- `resolve_notebook_context_stage`: Notebook-facing setup stage for `[4]` that returns context, legacy bindings, and discovered paths.
- `notebook_bindings_from_context`: Rebind package-resolved context back to legacy notebook variable names.
- `normalize_run_config`: Apply defaults and forced recompute flags.
- `build_run_config_stage`: Notebook-facing run-config rebinding stage for `[4b]`.
- `reset_fish_state`: Update the fish marker explicitly.
- `require_fish_state`: Check that the current fish matches the cached fish marker.
- `resolve_fish_state_stage`: Notebook-facing fish-state stage for `[4a]`.
- `build_fish_state_audit_df`: Build the audit dataframe used by cell `[4c]`.
- `build_context_audit_stage`: Notebook-facing audit stage for `[4c]`.
- `build_registration_helper_stage`: Publish package-owned registration helper bindings for `[6]`, including legacy image/orientation helpers consumed by QC notebook cells.
- `resolve_voxel_context_stage`: Notebook-facing voxel discovery/cache stage for `[8]` that preserves legacy voxel globals and summary dataframe outputs.
- `normalize_anatomy_stack_stage`: Notebook-facing anatomy normalization stage for `[14]` that preserves current NRRD->TIFF conversion/cache behavior and `ANAT_STACK_PATH` bindings.
- `build_voxel_debug_stage`: Notebook-facing anatomy voxel debug helper for `[8a]`.
- `orient_functional_stacks_stage`: Notebook-facing functional stack orientation/cache stage for `[10]`.
- `build_final_fish_audit_stage`: Notebook-facing final contamination audit for `[99-debug-fish-audit]`.

## `codeants_2pf_hcr.cohort`

- `CohortBuildConfig`: Typed cohort build/cache knob container for `multi_fish_56h_56g.ipynb` `[cfg]` + `[cohort-build]`.
- `cohort_cache_paths`: Resolve canonical cohort cache/output filenames under `cohort_outputs/multi_fish_56h_56g/`.
- `count_trace_genes`: Count available genes in panel-keyed trace cache payloads (flat and per-fish nested variants).
- `resolve_cohort_context_stage`: Notebook-facing cohort environment/context stage for `[cfg]`; publishes stable cohort env bindings.
- `load_cohort_analysis_state`: Cache-aware cohort analysis loader for late cohort cells; always returns stable env bindings plus cached tables/results.
- `load_cohort_outputs_from_disk`: Load cached cohort tables and trace payload keys for cache reuse checks.
- `save_cohort_outputs_to_disk`: Persist cohort summary tables, 53a cache tables, and trace cache payload.
- `build_cohort_outputs_stage`: Notebook-facing owner stage for cohort fish discovery, metadata lookup, cache reuse/invalidation, aggregation, and legacy-shaped binding export.

## `codeants_2pf_hcr.spatial`

- `FunctionalReferenceConfig`: Typed functional-reference cache/build configuration for notebook cell `[12]`.
- `FunctionalPlacementConfig`: Typed NCC XY placement configuration for notebook cell `[20]`.
- `InPlaneRegistrationComparisonConfig`: Typed in-plane method-comparison configuration for notebook cell `[20]`.
- `RegistrationSearchConfig`: Typed registration-search knob container for notebook cell `[16]`.
- `imread_any`: Read TIFF or NRRD images with minimal notebook dependencies.
- `zproject_mean`: Mean projection helper.
- `norm01`: Robust percentile normalization to `[0, 1]`.
- `local_unsharp`: Local contrast sharpening helper.
- `corrcoef_img`: Pearson correlation for image pairs.
- `top_correlated_mean`: Suite2p-like top-k frame reference builder.
- `best_z_by_ncc`: Best-z search by NCC-like scoring.
- `apply_func_orientation`: Apply the notebook’s functional orientation convention.
- `apply_square_region_mask`: Preserve values inside an anatomy-space square and set outside pixels to zero for masked registration.
- `build_functional_references_stage`: Notebook-facing functional reference stage for `[12]` that preserves `plane_refs` plus legacy `ref2d_raw` / `ref2d` bindings and cache filenames.
- `ncc_xy`: Shared NCC XY placement primitive for notebook cell `[20]`.
- `scale_image`: Resize a 2D functional reference by an empirical NCC search scale.
- `registration_metric_from_scores`: Summarize NCC scores into best-z and peak metrics.
- `run_ncc_placement_stage`: Notebook-facing NCC XY placement stage for `[20]` that updates `plane_refs` with placement metadata and warped reference aliases.
- `run_in_plane_registration_comparison_stage`: Notebook-facing `[20]` comparison stage that evaluates current NCC XY placement against optional ANTs rigid+affine placement while keeping the configured active backend explicit.
- `run_registration_search_stage`: Notebook-facing registration-search stage for `[16]` that updates `plane_refs`, persists scale/best-z caches, and rebinds legacy globals.

## `codeants_2pf_hcr.matching`

- `FunctionalAnatomyDebugConfig`: Typed functional↔anatomy debug summary configuration for notebook cell `[34a]`.
- `FunctionalRoiIdentityConfig`: Typed per-ROI identity export configuration for notebook cell `[50i]`.
- `HcrActivityExportConfig`: Typed HCR-centric activity export configuration for notebook cell `[50]`.
- `resolve_plane_transform`: Resolve the notebook’s per-plane affine/tform binding from a plane-ref record.
- `resample_labels_nn`: Apply nearest-neighbor label resampling for functional-to-anatomy plane warps and shape harmonization.
- `resample_image`: Apply intensity-image resampling for functional-to-anatomy plane warps, including ANTs transform dictionaries.
- `transform_points_between_spaces`: Transform 2D points between functional/moving and anatomy/fixed spaces for skimage and ANTs in-plane transform objects.
- `harmonize_functional_labels_to_anatomy`: Enforce the shared per-plane functional/anatomy shape contract before overlap or centroid matching.
- `compute_centroids`: Build label centroid tables for centroid-based QC and distance summaries.
- `idx_to_um`: Convert centroid-index tables to micron coordinates with explicit voxel scaling.
- `nearest_neighbor_match`: Build one-nearest-neighbor centroid matches for QC diagnostics.
- `hungarian_match`: Build Hungarian centroid matches for QC diagnostics.
- `summarize_distances`: Summarize centroid-match distance arrays for QC tables.
- `summarize_functional_anatomy_geometry_metrics`: Summarize ROI-centric master-table geometry metrics for in-plane registration method comparison reports.
- `build_plane_centroid_matches`: Build per-plane centroid-link tables and overlap-aware counts for notebook QA stages `[34]`/`[34a]`.
- `build_functional_anatomy_debug_df`: Summarize per-plane functional-to-anatomy centroid matching status for notebook debug stage `[34a]`.
- `build_functional_anatomy_debug_stage`: Notebook-facing functional↔anatomy debug stage for `[34a]` that loads anatomy labels, resolves voxel scale, and returns the debug summary bindings/log lines.
- `gene_from_mask`: Infer a gene label from a confocal mask filename.
- `build_anat_identity_lookup_df`: Build the anatomy-label to identity lookup table from HCR matches.
- `build_hcr_mask_fate_df`: Reconstruct per-confocal-label match fate rows from `[44]` `hcr_match_results` for downstream rejected-mask QA consumers such as `[50f]` and `[50g]`.
- `build_functional_roi_master_df`: Build the authoritative ROI-centric functional-to-anatomy master table for `[50i]`.
- `build_hcr_activity_tables`: Build HCR-centric functional candidate/status tables for `[50]`.
- `MatchingConfig`: Typed matching-stage knob container.

## `codeants_2pf_hcr.stimulus`

- `StimulusConfig`: Typed stimulus parsing configuration.
- `find_experiment_log`: Discover the fish experiment log CSV.
- `find_metadata_csv`: Discover the fish metadata CSV.
- `load_events_df`: Normalize event/time columns from the experiment log.
- `load_metadata_params`: Read metadata key-value pairs.
- `parse_float`: Robust numeric parser for mixed metadata values.
- `classify_stim_type`: Collapse notebook stimulus codes to bout/continuous/mixed.
- `effective_motion_window`: Compute motion-response windows after onset delay.
- `build_prestim_baseline_windows`: Build merged prestim baseline windows from `df_evt`.
- `build_prestim_trial_windows`: Build per-trial prestim windows for null sampling.
- `build_null_window_start_map`: Build null AUC bootstrap start indices by duration.
- `compute_zscore_stats`: Compute pooled-baseline z-score parameters and validity masks.
- `parse_unilateral_stim`: Parse unilateral stimulus code to side/mode.
- `build_stim_tables`: Build block and trial tables from event logs.
- `resolve_stimulus_context`: End-to-end notebook-facing stimulus loader for `[55]`.

## `codeants_2pf_hcr.activity`

- `ActivityConfig`: Typed response/BPI scoring configuration for `[50ia]`.
- `SingleFishBpiDiagnosticsConfig`: Typed BPI/activity diagnostics configuration for notebook cell `[56g]`.
- `build_response_bpi_tables`: Build response/BPI annotations and summary tables from the ROI master table.
- `prepare_single_fish_bpi_diagnostics_stage`: Notebook-facing response-aware diagnostics prep stage for `[56g]` that resolves activity/BPI columns, backfills response metadata from the ROI master table, and publishes plotting-ready bindings.

## `codeants_2pf_hcr.suite2p`

- `Suite2pStageConfig`: Typed Suite2p load/orientation configuration for notebook cell `[23a]`.
- `infer_frame_rate_from_detail`: Resolve a consistent Suite2p frame rate from per-plane ops files.
- `load_suite2p_stage`: Notebook-facing Suite2p stage loader for `[23a]` that returns legacy-shaped labels, plane maps, and source summary tables.
- `load_suite2p_dff_map`: Load Suite2p `F.npy` traces from disk and convert them to dF/F by plane.

## `codeants_2pf_hcr.segmentation`

- `AnatomyCellposeConfig`: Typed anatomy Cellpose configuration for notebook cell `[24a]`.
- `HcrCellposeConfig`: Typed HCR Cellpose segmentation configuration for notebook cell `[24]`.
- `resolve_hcr_cellpose_model_path`: Resolve the active HCR Cellpose model path from overrides, run config, or repo defaults.
- `collect_hcr_intensity_stack_paths`: Discover and filter HCR intensity stacks from `rbest`/`rn` preprocessing outputs for `[24]`.
- `deduplicate_hcr_intensity_targets`: Collapse duplicate intensity inputs that would write the same Cellpose mask output.
- `run_anatomy_cellpose_stage`: Notebook-facing anatomy Cellpose stage for `[24a]` with deferred Cellpose import and cross-platform device selection.
- `run_hcr_cellpose_stage`: Notebook-facing HCR Cellpose stage for `[24]` that prepares stack inputs, resolves anisotropy, and writes mask TIFFs.
- `resolve_functional_labels_for_plane`: Resolve per-plane functional labels for `[26]` from Suite2p, Cellpose, or legacy label sources with orientation handling.
- `resolve_native_suite2p_labels_for_plane`: Resolve the native Suite2p label image for a plane without Cellpose fallback for `[26a]`.
- `export_suite2p_native_labels_stage`: Notebook-facing Suite2p native-label export stage for `[26a]` that writes QA TIFFs and a manifest CSV.

## `codeants_2pf_hcr.hcr_warp`

- `run_hcr_external_bigwarp_label_stage`: Notebook-facing wrapper for single-fish `[43]` external-BigWarp label prep / load logic with stage-local ANTs import.
- `run_hcr_external_bigwarp_intensity_stage`: Notebook-facing wrapper for single-fish `[43b]` rn->rbest intensity prep logic with stage-local ANTs import.

## `codeants_2pf_hcr.traces`

- `TraceExportConfig`: Typed trace-export configuration for `[51]`.
- `MotionAucPlotConfig`: Typed motion-AUC table-build configuration for the `[50l]`-embedded `[56i]` owner path.
- `load_midline_context`: Load and validate the fish-scoped midline bundle for `[56]`, `[56h]`, `[56f-qc*]`, and `[50l]`/`[56i]` AUC preparation.
- `annotate_midline_side`: Add `midline_signed_dist_px`, `midline_side`, `midline_uncertain`, and `midline_space` columns using package-owned midline semantics.
- `filter_high_confidence_pairs`: Drop HCR/functional pair rows flagged by known low-confidence columns while optionally returning filter counts.
- `extract_window_with_padding`: Extract trace windows with notebook-compatible `pad_nan` and `strict` behavior.
- `build_single_fish_motion_auc_plot_tables`: Build the single-fish motion-window AUC ROI panel / plot points / plot counts tables for `[50l]` / `[56i]`, preserving current ROI-centric all-neuron and HCR-centric gene-group semantics.
- `export_suite2p_trace_metadata`: Export deduplicated HCR-selected Suite2p dF/F traces and metadata for `[51]`.
- `prepare_pairs_for_unique_cells`: Validate and normalize HCR-centric pair tables before trace analyses.
- `resolve_conf_func_csv_analysis`: Resolve the analysis-ready `conf_to_func_pairs.csv` path for `[51]`, `[56]`, `[56h]`, and `[57]`.

## `codeants_2pf_hcr.plots.qa`

- `build_best_plane_modality_merge_grid`: Render the merged best-plane modality QA panel.
- `build_round_channel_mip_grid`: Render the round/channel MIP grid.
- `collect_cohort_53a_tables`: Build pooled cohort [53a]-analogue cache tables for `multi_fish_56h_56g.ipynb` (`[cohort-build]`).
- `_select_in_plane_hcr_status_like_53a`: Build the [53a] label-level in-plane HCR status subset (one row per accepted `(gene, anat_label)` represented on functional planes) for HCR↔anatomy QC sourcing.
- `show_region_shift_square_selector_stage`: Notebook-facing region-square QA selector stage for `[22d]`, including JSON reuse/save behavior.
- `show_ants_registration_region_selector_stage`: Notebook-facing NCC-guided per-plane fixed-region square writer for `[19a]` masked ANTs in-plane registration.
- `show_inplane_registration_method_comparison_stage`: Notebook-facing regional ANTs-vs-NCC in-plane placement and ROI/anatomy-boundary review for `[22e]`, using stored `[20]` method outputs, Suite2p labels, anatomy labels, and the `[22d]` crop.
- `show_regional_match_review_stage`: Notebook-facing anatomy-space ROI/anatomy regional overlay for `[34c]`, using the `[22d]` crop and selected in-plane transform to warp functional labels forward.
- `compute_anatomy_median_xy_radius_um`: Compute anatomy-label XY diameter/radius reference (microns) for centroid-QA initialization.
- `render_cohort_53a_summary`: Render the 2x2 cohort [53a]-analogue summary figure for `multi_fish_56h_56g.ipynb` (`[53a-cohort]`).
- `render_single_fish_hcr_anatomy_coexpression_summary`: Render the single-fish `[57b-anatomy-coexpression-summary]` figure and export anatomy-label/coexpression summary tables from in-plane HCR status rows.
- `show_centroid_match_qa_stage`: Notebook-facing centroid-distance QA stage for `[34]` with threshold UI, plane switching, context rendering, and `[26]`-matched functional label source selection via `use_suite2p_labels`.
- `show_functional_label_overlay_stage`: Notebook-facing functional-label overlay stage for `[26]`.
- `show_registration_overlay_stage`: Notebook-facing interactive registration overlay stage for `[22]`.

## `codeants_2pf_hcr.plots.annotations`

- `place_labels_no_overlap`: Shared 53a-style collision-aware in-panel label placer for dense cohort annotations (stack-up on local x collisions + y-limit expansion).

## `codeants_2pf_hcr.plots.analysis`

- `plot_single_roi_57style`: Render the single-ROI `[57]` style figure and optional AUC table.
- `render_single_fish_50l_bpi_panel`: Render the single-fish `[50l]` top-left whole-population AUC-vs-BPI scatter from `[50ia]` response/BPI outputs.
- `render_single_fish_50l_gene_auc_panel`: Render the single-fish `[50l]` marker-specific ipsi/contra AUC box/point/count-strip panels from the package-owned motion AUC point/count tables.
- `render_single_fish_50l_global_auc_panel`: Render the single-fish `[50l]` all-neurons ipsi/contra AUC panels as paired bout↔continuous ROI points with class-colored directional highlights, neutral non-directional classes, directional class-mean summaries, and unchanged count strips.
- `render_single_fish_50l_composite`: Render notebook stage `[50l]` as one package-owned composite, including stale `[56i]` AUC cache rebuilding, BPI panel, response/BPI donut, AUC panels, legacy figure globals, and `compound_50j_56i_unified.png/.pdf`.
- `render_cohort_56h_by_fish`: Render cohort per-gene/per-fish [56h]-style trace panels from prebuilt cohort trace payloads.
- `render_cohort_56g_diagnostics`: Render cohort [56g] activity/BPI diagnostic 2x2 panel from `cohort_bpi_cells_df`.
- `render_cohort_motion_auc`: Render cohort [cohort-auc] motion-window AUC figure; supports cached aggregate CSV reuse or per-fish aggregation fallback, optional global-median-label suppression, and mode-colored median labels.
- `render_cohort_56h_status_donut_grid`: Render cohort fish×gene HCR-status donut grid and export summary counts table.
- `render_cohort_50l_donut_row`: Render cohort [50l]-style fish-row donut figure and export long/wide counts tables.
- `render_cohort_50l_responsive_identity_donut_row`: Render cohort responsive-only fish-row donut figure with BPI inner ring and exact HCR-derived identity outer ring (plus `unidentified`) and export long/wide counts tables.
- `render_single_fish_50l_responsive_identity_donut`: Render single-fish responsive-only donut with the same hybrid semantics as cohort responsive-identity donut and export long/wide counts tables.

## `codeants_2pf_hcr.single_fish_notebook_stages`

- `run_single_fish_cell_22c_stage`: Migration shim for heavy single-fish notebook cell `[22c]`.
- `run_single_fish_cell_30_stage`: Migration shim for single-fish segmentation-diameter QA cell `[30]`.
- `run_single_fish_cell_34c_stage`: Migration shim for single-fish regional match review cell `[34c]`.
- `run_single_fish_cell_38_stage`: Migration shim for single-fish HCR manifest/discovery cell `[38]`.
- `run_single_fish_cell_40_stage`: Migration shim for single-fish run metadata snapshot cell `[40]`.
- `run_single_fish_cell_41_stage`: Migration shim for single-fish HCR transform configuration cell `[41]`.
- `run_single_fish_cell_44_stage`: Migration shim for single-fish HCR↔anatomy matching/QC cell `[44]`.
- `run_single_fish_cell_46_stage`: Migration shim for single-fish functional↔anatomy matching summary cell `[46]`.
- `run_single_fish_cell_47_stage`: Migration shim for single-fish per-plane 3D viewer cell `[47]`.
- `run_single_fish_cell_47b_stage`: Migration shim for single-fish HCR/anatomy 3D viewer cell `[47b]`.
- `run_single_fish_cell_50_stage`: Migration shim for heavy single-fish notebook cell `[50]`.
- `run_single_fish_cell_50i_stage`: Migration shim for single-fish ROI-centric master-table writer cell `[50i]`.
- `run_single_fish_cell_50ia_stage`: Migration shim for single-fish response/BPI writer cell `[50ia]`.
- `run_single_fish_cell_50e_stage`: Migration shim for heavy single-fish notebook cell `[50e]`.
- `run_single_fish_cell_50f_stage`: Migration shim for single-fish notebook cell `[50f]`.
- `run_single_fish_cell_50g_stage`: Migration shim for single-fish notebook cell `[50g]`.
- `run_single_fish_cell_51_stage`: Migration shim for single-fish Suite2p trace export cell `[51]`.
- `run_single_fish_cell_53_stage`: Migration shim for heavy single-fish notebook cell `[53]`.
- `run_single_fish_cell_53a_stage`: Migration shim for heavy single-fish notebook cell `[53a]`.
- `run_single_fish_cell_54_stage`: Migration shim for single-fish functional warp export cell `[54]`.
- `run_single_fish_cell_56_stage`: Migration shim for heavy single-fish notebook cell `[56]`.
- `run_single_fish_cell_56d_stage`: Migration shim for single-fish `[53a]` + `[56]` side-by-side composite cell `[56d]`.
- `run_single_fish_cell_56f_qc_stage`: Migration shim for heavy single-fish notebook cell `[56f-qc]`.
- `run_single_fish_cell_56f_qc_activity_stage`: Migration shim for heavy single-fish notebook cell `[56f-qc-activity]`.
- `run_single_fish_cell_56g_stage`: Migration shim for single-fish notebook cell `[56g]`.
- `run_single_fish_cell_56h_stage`: Migration shim for heavy single-fish notebook cell `[56h]`.
- `run_single_fish_cell_57_stage`: Migration shim for single-fish notebook cell `[57]`.

## `codeants_2pf_hcr.notebook_contract`

- `NotebookContractViolation`: Static notebook-contract violation record.
- `find_top_level_defs`: Find code cells that still define top-level `def`/`class` blocks.
- `find_figure_contract_violations`: Find late figure cells that are not package-renderer driven.
- `check_notebook_contract`: Summarize static notebook and figure contract violations for smoke/tests.
