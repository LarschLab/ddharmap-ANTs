# Symbol Index

Generated manually for the current extracted package surface.

## Trusted patterns

- `context.py`: path/config normalization pattern.
- `stimulus.py`: explicit stage input/output pattern.
- `activity.py`: response/BPI stage-owned semantics pattern.
- `pipeline.py`: staged single-fish pipeline contract/manifest pattern.
- `plots.*`: deterministic figure-builder pattern.
- `tools/`: wrapper pattern only, not business-logic authority.

## `codeants_2pf_hcr.pipeline`

- `PIPELINE_STAGE_ORDER`: Roadmap-order stage names for the single-fish staged pipeline.
- `PIPELINE_MANIFEST_VERSION`: Current first-pass manifest schema version for staged pipeline manifests.
- `POST_PREPROCESSING_STAGE_NAMES`: Read-only stage-status names for existing post-preprocessing staged output folders.
- `GRANULAR_PREPROCESSING_STAGE_NAMES`: Concrete writer-stage names for preparation/segmentation slices that must not be hidden under generic `preprocess-*` command names.
- `SingleFishPipelineConfig`: Typed fish/root/strictness/dry-run knob container for staged single-fish pipeline commands.
- `PipelinePaths`: Resolved fish-scoped paths for staged pipeline commands without creating notebook output folders during path resolution.
- `StageContract`: Declarative stage contract record containing stage order, purpose, dependencies, and first-pass read-only status.
- `ManifestPathRecord`: JSON-ready path record with path, existence, kind, required status, and optional glob count/pattern.
- `StageCheckRecord`: JSON-ready semantic check record with status, expected value, observed value, and detail.
- `StageManifest`: JSON-ready stage manifest payload with fish ID, stage status, dry-run state, inputs, outputs, parameters, warnings, and errors.
- `StageOutputSpec`: Declared read-only downstream staged output with optional control/parity metadata, table-specific keyed/exact/numeric CSV comparison columns, and figure dimension/thumbnail warning thresholds for `compare-staged`.
- `PersistedManifestStatus`: JSON-ready persisted manifest trust-state record with missing/current/stale/fail status.
- `pipeline_contracts`: Return the staged single-fish contract list in roadmap order.
- `cellpose_stage_manifest_path`: Resolve modality-specific manifest paths for granular Cellpose/preparation writer stages.
- `discover_functional_motion_corrected_stacks`: Discover concrete motion-corrected functional TIFF inputs under `02_reg/00_preprocessing/2p_functional/02_motionCorrected/`.
- `discover_functional_reference_pairs`: Discover staged functional reference raw/norm TIFF pairs produced by `prepare-functional-reference-stacks`.
- `functional_reference_output_dir`: Resolve the staged functional reference output directory under `pipeline_root/prepare-functional-reference-stacks/functional/raw/`.
- `functional_to_anatomy_registration_root`: Resolve the staged functional-to-anatomy registration output root under `pipeline_root/register-functional-to-anatomy/`.
- `hcr_to_anatomy_registration_root`: Resolve the staged HCR-to-anatomy registration output root under `pipeline_root/register-hcr-to-anatomy/`.
- `roi_to_anatomy_match_root`: Resolve the staged ROI/anatomy geometry output root under `pipeline_root/match-roi-to-anatomy/`.
- `load_plane_refs_summary`: Load the compact `plane_refs_summary.json` emitted by `register-functional-to-anatomy` for recompute/audit paths.
- `run_prepare_functional_reference_stacks_stage`: Writer stage wrapping `[12]` functional reference preparation. It discovers or accepts motion-corrected functional TIFFs, resolves polarity, calls `spatial.build_functional_references_stage`, writes legacy-compatible raw/norm reference TIFF pairs, and records the generated pairs in a stage manifest.
- `run_register_functional_to_anatomy_stage`: Writer stage wrapping `[16]` NCC best-z/scale search and an NCC-only `[20]` in-plane comparison. It reconstructs in-memory `plane_refs` from staged functional reference TIFFs, consumes prepared in vivo anatomy, writes NCC caches, in-plane comparison/recommendation CSVs, warped reference TIFFs, `plane_refs_summary.json`, and a staged `registration/tforms_by_plane.csv`.
- `run_register_hcr_to_anatomy_stage`: Writer stage for HCR-to-anatomy artifacts. Default mode copies small accepted TIFF/CSV/JSON artifacts from an explicit or default `03_analysis/confocal/aligned/` source into `pipeline_root/register-hcr-to-anatomy/confocal/aligned/`, validates label/match/final-pair/metadata presence plus final-pair schema and accepted-pair semantics, records aligned intensity NRRDs as inputs, does not copy multi-GB NRRDs, and reports direct-ANTs recompute readiness from current raw HCR masks, rbest/rn HCR NRRDs, matching metadata, and transform files. `recompute_direct_ants=True` applies the notebook `label_voxel_floor_v3` prewarp HCR label filter, recomputes HCR label TIFFs, warp metadata, and HCR/anatomy match/review/final-pair CSVs from raw masks plus current ANTs transforms, and gates strict runs with accepted final-pair key parity.
- `run_match_roi_to_anatomy_stage`: Writer stage for geometry-only ROI/anatomy matches. The promoted recompute mode consumes staged `plane_refs_summary.json`, Suite2p, anatomy labels, selected accepted `ants_rigid_affine` in-plane transformlists when present, anatomy XY spacing, and notebook-equivalent functional orientation before writing `functional_roi_anatomy_matches.csv`, `functional_roi_anatomy_match_by_plane.csv`, and `functional_roi_anatomy_match_plane_meta.csv`. It strips identity/HCR/response/BPI/gene columns and refuses overwrite without `force_recompute`; control-geometry staging from an explicit `--source-root` remains available for bootstrap/diagnostic use.
- `build_single_fish_status`: Build a compact read-only trust-state summary from the dry-run input audit plus any persisted audit manifest.
- `build_single_fish_compare_staged_manifest`: Build a read-only comparison manifest for one existing post-preprocessing staged output folder.
- `run_single_fish_freeze_legacy_baseline_stage`: Freeze declared legacy/control outputs for one or all post-processing stage specs into `DATA_ROOT/pipeline_baselines/FISH_ID/legacy_singleFish/stages/`, refusing overwrite without `overwrite=True`.
- `build_single_fish_compare_legacy_baseline_manifest`: Build a read-only comparison manifest for one staged post-processing output folder against the frozen `legacy_singleFish` bundle.
- `compare_single_fish_legacy_baseline`: Build a read-only aggregate comparison payload for staged post-processing outputs against the frozen `legacy_singleFish` bundle.
- `build_single_fish_score_activity_bpi_recompute_manifest`: Build a read-only `score-activity-bpi` recompute audit manifest that calls `activity.build_response_bpi_tables` without the precomputed-output shortcut and compares in-memory activity/BPI tables to control CSVs.
- `build_single_fish_hcr_activity_replay_manifest`: Build a read-only HCR-centric `[50]` replay audit manifest from staged `plane_refs_summary.json`, Suite2p, anatomy labels, staged HCR final pairs, and a response-aware ROI master used only as lookup. It checks selected ANTs transformlist availability, runs selected-ANTs and persisted affine transform variants in memory, runs `matching.build_hcr_activity_tables` plus `matching.finalize_hcr_activity_export_tables`, compares row/key counts to accepted HCR outputs when available, and keeps promotion disabled.
- `run_single_fish_assign_hcr_identity_stage`: Write the staged identity/HCR registration CSV bundle under `pipeline_root/assign-hcr-identity/registration` after requiring staged ROI/anatomy geometry, staged functional/anatomy plane refs, Suite2p, anatomy labels, and staged HCR/anatomy artifacts, refusing overwrite without `force_recompute`. It recomputes `anatomy_identity_lookup.csv` from staged HCR final-pair CSVs using `FunctionalRoiIdentityConfig.default_gene_order`, recomputes `functional_roi_activity_identity.csv` from staged ROI/anatomy geometry plus that lookup, recomputes HCR-centric activity/status/candidate CSVs through the label-first replay, regenerates `hcr_activity_status_summary.csv`, verifies ROI/anatomy inputs stay geometry-only, and checks HCR final-pair CSV schema/accepted-pair semantics plus accepted-control parity.
- `run_single_fish_score_activity_bpi_stage`: Recompute response/BPI tables through `activity.build_response_bpi_tables(precomputed_scored_bpi_df=None)` and write staged score CSVs under `pipeline_root/score-activity-bpi/registration`. Defaults to the staged `assign-hcr-identity` identity CSV, supports explicit identity input for controlled validation, and refuses overwrite without `force_recompute`.
- `run_single_fish_export_canonical_tables_stage`: Assemble the staged canonical registration CSV bundle under `pipeline_root/export-canonical-tables/registration` from staged score outputs and staged or explicit HCR/identity roots, refusing overwrite without `force_recompute`.
- `run_single_fish_make_qa_report_stage`: Generate staged `qa_report.md` and `qa_report_summary.json` under `pipeline_root/make-qa-report` from staged canonical export inputs, including canonical table rows, staged output status, a manual review checklist, and inline image previews for existing QA/figure PNG artifacts; refuses overwrite without `force_recompute`.
- `run_single_fish_make_figures_stage`: Stage the declared figure artifacts under `pipeline_root/make-figures/04_plots` after verifying staged canonical export CSV inputs and declared `[56i]` AUC table inputs, rendering package-owned `compound_50j_56i_unified.png`, `bpi_all_pairs.png`, `per_gene_stimulus_trace_with_hcr_status_56h.png`, responsive identity donut, and HCR anatomy coexpression summary, and refusing overwrite without `force_recompute`.
- `build_single_fish_downstream_stage_manifest`: Build a read-only manifest for an existing post-preprocessing staged output folder.
- `build_single_fish_downstream_stage_manifests`: Build read-only manifests for all current post-preprocessing staged output folders.
- `build_single_fish_stage_status`: Summarize one downstream read-only stage manifest plus persisted-manifest state.
- `downstream_stage_names`: Return the supported current downstream read-only stage-status names.
- `resolve_pipeline_paths`: Resolve staged pipeline paths without creating output directories.
- `describe_manifest_path`: Build a manifest path record for a file or directory.
- `describe_glob`: Build a manifest path record for a glob under a base directory.
- `stage_manifest_path`: Resolve the fish-scoped path for a stage manifest under `03_analysis/functional/pipeline_manifests/`.
- `write_stage_manifest`: Persist a stage manifest to its fish-scoped manifest path.
- `write_cellpose_stage_manifest`: Persist granular Cellpose/preparation manifests under `03_analysis/confocal/raw/manifests/` or `03_analysis/structural/ex_vivo/manifests/`.
- `prepared_in_vivo_anatomy_path`: Resolve the default canonical in vivo anatomy preparation NRRD under `02_reg/00_preprocessing/2p_anatomy/`.
- `run_prepare_in_vivo_anatomy_stack_stage`: Writer stage wrapping `[14a]` signed-anatomy preprocessing for canonical in vivo 2P anatomy. It discovers an in vivo raw anatomy stack when no explicit source is provided, applies metadata-driven polarity, writes the registration-ready NRRD/JSON, checks uint8 and `750x750` Y/X output, and refuses existing outputs without `force_recompute`.
- `discover_ex_vivo_anatomy_stack`: Discover exactly one raw ex vivo anatomy stack under `01_raw/2p/anatomy`, requiring an explicit path when ambiguous.
- `ex_vivo_structural_root`: Resolve the ex vivo structural analysis subtree under `03_analysis/structural/ex_vivo/`.
- `prepared_ex_vivo_anatomy_path`: Resolve the default prepared ex vivo anatomy NRRD under the structural ex vivo subtree.
- `run_prepare_ex_vivo_anatomy_stack_stage`: Writer stage for converting/orienting the raw ex vivo stack into a registration-ready analysis NRRD without using a generic preprocessing command name.
- `run_segment_ex_vivo_anatomy_cellpose_stage`: Writer stage for ex vivo anatomy Cellpose masks isolated under `03_analysis/structural/ex_vivo/`.
- `run_segment_hcr_cellpose_stage`: Writer stage for HCR Cellpose masks from an explicit source such as `rbest`.
- `compare_persisted_manifest`: Compare a persisted manifest's path records against the current audit and report missing/current/stale/fail state.
- `compare_single_fish_staged_outputs`: Build a read-only aggregate comparison payload for declared existing post-preprocessing staged outputs.
- `stage_manifest_to_json`: Serialize a stage manifest to JSON text for stdout or persistence.
- `run_single_fish_audit_inputs_stage`: Run the first staged pipeline input audit in read-only/dry-run mode and return a JSON-ready manifest with labeled input records, schema checks, cross-table consistency checks, value-domain checks, and optional staged parity checks, without writing into fish folders.
- CLI wrapper: `tools/single_fish_pipeline.py` currently exposes read-only `contracts`, `audit-inputs`, `status`, `stage-status`, post-preprocessing `compare-staged`, frozen-bundle `freeze-legacy-baseline` / `compare-legacy-baseline`, read-only `audit-score-activity-bpi`, downstream writers `assign-hcr-identity`, `score-activity-bpi`, `export-canonical-tables`, `make-qa-report`, and `make-figures`, plus granular upstream writers `prepare-functional-reference-stacks`, `prepare-in-vivo-anatomy-stack`, `prepare-ex-vivo-anatomy-stack`, `segment-ex-vivo-anatomy-cellpose`, `segment-hcr-cellpose`, NCC-only `register-functional-to-anatomy`, HCR `register-hcr-to-anatomy` with opt-in `--recompute-direct-ants`, and recompute-capable `match-roi-to-anatomy`; `assign-hcr-identity` accepts `--roi-anatomy-root` and `--hcr-anatomy-root` overrides for dependency validation. `--write-manifest` persists manifests for manifest-bearing commands. `status`/`stage-status` warn when existing downstream staged outputs are older than required declared inputs or existing upstream staged dependencies. Remaining roadmap targets include full functional-to-anatomy ANTs parity, richer QA reports, and broader positive Cellpose/ex vivo validation.
- Helga helper: `tools/helga_nas_session_test.bat` prompts for `danin.dharmaperwira@unil.ch` NAS credentials in a headful SSH session, maps `Y:` with `/persistent:no`, verifies `L765_f02`, and removes the temporary mapping before exit. Use this pattern for future NAS-backed Helga GPU jobs; never store passwords in repo artifacts.
- Helga job helper: `tools/helga_l765_f02_cellpose_job.bat` uses the same temporary credential prompt pattern, then runs `segment-ex-vivo-anatomy-cellpose` on the manual-oriented `L765_f02` ex vivo NRRD and `segment-hcr-cellpose --hcr-source rbest` on Helga's CUDA Cellpose environment.

## `codeants_2pf_hcr.context`

- `AnatomyNormalizationStageConfig`: Typed anatomy-conversion knob container for notebook cell `[14]`.
- `AnatomyUint8PreprocessingConfig`: Typed signed-anatomy uint8 preprocessing knob container for notebook cell `[14a]`, including default 2P XY mirroring/orientation, anatomy Z flip for same-fish registration, cache-version, target Y/X shape, and registration-NRRD write controls.
- `ContextStageConfig`: Typed setup/path knob container for notebook cell `[4]`.
- `ExVivoAnatomyPreprocessingConfig`: Typed ex vivo 2P anatomy preprocessing knob container for the experimental ex vivo bridge path, including mirrored-2P X flip, registration-convention Z flip, target Y/X shape, cache-version, and registration-NRRD write controls.
- `FishStateStageConfig`: Typed fish-state marker configuration for notebook cell `[4a]`.
- `FinalFishAuditConfig`: Typed final contamination-audit configuration for `[99-debug-fish-audit]`.
- `ManualAnatomyOrientationConfig`: Typed manual anatomy-orientation knob container for applying brainAtlas-style preview-angle rotation/crop plus explicit rot90 and axis flips to an already preprocessed anatomy stack.
- `VoxelStageConfig`: Typed voxel-resolution stage configuration for notebook cell `[8]`.
- `FunctionalOrientationStageConfig`: Typed functional orientation/audit configuration for notebook cell `[10]`; full oriented movie stack saves are opt-in.
- `OrientationResolutionError`: Fail-fast error for missing or ambiguous fish orientation metadata.
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
- `resolve_voxel_context_stage`: Notebook-facing voxel discovery/cache stage for `[8]` that preserves legacy voxel globals, maps original functional source paths to legacy flipped aliases, treats `step_size_um_anatomy` metadata as authoritative for anatomy Z, and returns summary dataframe outputs.
- `normalize_anatomy_stack_stage`: Notebook-facing anatomy normalization stage for `[14]` that preserves current NRRD->TIFF conversion/cache behavior and `ANAT_STACK_PATH` bindings.
- `preprocess_anatomy_uint8_stage`: Notebook-facing signed 16-bit anatomy preprocessing stage for `[14a]` that mirrors/orients 2P anatomy XY by default, flips anatomy Z to match bottom-to-top confocal registration convention, resizes anatomy Y/X to `750x750`, saves the canonical uncompressed 8-bit registration NRRD `<fish_id>_anatomy_2P_GCaMP.nrrd` plus `.nrrd.json` metadata under `02_reg/00_preprocessing/2p_anatomy`, avoids duplicate TIFF image outputs, and rebinds `ANAT_STACK_PATH`.
- `infer_anatomy_stack_path`: Discover the raw in vivo anatomy stack while excluding mask/label/overlay and ex vivo-looking candidates so staged in vivo prep does not silently pick bridge-registration inputs.
- `preprocess_ex_vivo_anatomy_stage`: Experimental same-fish bridge preprocessing stage for raw ex vivo 2P anatomy stacks from `01_raw/2p/anatomy`; writes isolated pre-manual-rotation NRRD plus JSON provenance under `02_reg/00_preprocessing/2p_anatomy/ex_vivo/` without rebinding canonical in vivo `ANAT_STACK_PATH` or creating duplicate TIFF image outputs.
- `apply_manual_anatomy_orientation_stage`: Experimental helper that applies brainAtlas-style preview-angle XY rotation, optional square crop, rot90, and explicit axis flips to a preprocessed anatomy stack, then writes manual-oriented NRRD plus JSON provenance for ex vivo registration trials.
- `read_raw_metadata_polarity`: Read per-fish raw metadata orientation from `01_raw/2p/metadata/*metadata*.csv`, normalize `bottom-left`/`top-right` to `north`/`south`, and fail on conflicts.
- `read_matching_metadata_polarity`: Read the legacy fallback polarity from `matchingMetadata.csv`.
- `resolve_func_polarity`: Resolve orientation with override support, preferring raw per-fish metadata and falling back to legacy matching metadata.
- `build_voxel_debug_stage`: Notebook-facing anatomy voxel debug helper for `[8a]`.
- `orient_functional_stacks_stage`: Notebook-facing functional orientation stage for `[10]` that audits legacy full-stack caches and only writes oriented movie stacks when explicitly requested.
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

## `codeants_2pf_hcr.cohort_suite2p`

- `CohortSuite2p23cConfig`: Typed build/cache knob container for `multiFish.ipynb` Suite2p `[23c]`.
- `parse_fish_ids_csv`: Parse comma-separated fish IDs while preserving order and dropping blanks/duplicates.
- `resolve_cohort_suite2p_23c_fish_dir`: Resolve an owner-free fish folder under the configured cohort data root.
- `aggregate_cohort_23c_trace_means`: Collapse `[23c]` per-ROI traces to one mean trace per fish/session/stimulus.
- `build_fish_session_color_map`: Assign stable fish colors with session shade variants for cohort trace rendering.
- `cohort_suite2p_23c_cache_paths`: Resolve cache/output filenames under `cohort_outputs/suite2p_23c_response_overview/`.
- `load_cohort_suite2p_23c_outputs_from_disk`: Load cached cohort `[23c]` tables and trace/heatmap payloads.
- `build_cohort_suite2p_23c_stage`: Notebook-facing owner stage for cohort `[23c]` fish discovery, Suite2p loading, response/BPI scoring, trace aggregation, heatmap payload retention, and cache writing.

## `codeants_2pf_hcr.multifish`

- `MultiFishAnatomySegmentationConfig`: Typed per-fish anatomy segmentation knob container for `multiFish.ipynb`.
- `MultiFishFunctionalAnatomyMatchConfig`: Typed multi-fish functional-anatomy ROI identity aggregation and session-aware duplicate-flagging knob container for `multiFish.ipynb`.
- `multifish_anatomy_segmentation_cache_paths`: Resolve MultiFish anatomy segmentation summary outputs under `cohort_outputs/multiFish/`.
- `multifish_functional_anatomy_match_cache_paths`: Resolve MultiFish functional-anatomy ROI identity, summary, and duplicate-summary outputs under `cohort_outputs/multiFish/`.
- `run_multifish_anatomy_segmentation_stage`: Notebook-facing owner stage that resolves per-fish orientation from raw metadata with legacy fallback, preprocesses anatomy through the single-fish `[14a]` uint8/mirrored-2P-XY/`750x750` contract, then runs/reuses anatomy Cellpose `[24a]` per configured fish and writes a cohort-level segmentation summary.
- `run_multifish_functional_anatomy_match_stage`: Notebook-facing owner stage that aggregates per-fish `functional_roi_activity_identity.csv`, maps global planes to imaging sessions from preprocessing metadata, flags same-anatomy-label ROI duplicates within fish/session, and writes multiFish ROI identity plus duplicate summary CSVs.

## `codeants_2pf_hcr.spatial`

- `FunctionalReferenceConfig`: Typed functional-reference cache/build configuration for notebook cell `[12]`; oriented references can be built from original motion-corrected stacks without saving full oriented movies.
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
- `build_functional_references_stage`: Notebook-facing functional reference stage for `[12]` that preserves `plane_refs` plus legacy `ref2d_raw` / `ref2d` bindings and cache filenames while applying functional orientation to derived 2D references.
- `ncc_xy`: Shared NCC XY placement primitive for notebook cell `[20]`.
- `scale_image`: Resize a 2D functional reference by an empirical NCC search scale.
- `registration_metric_from_scores`: Summarize NCC scores into best-z and peak metrics.
- `run_ncc_placement_stage`: Notebook-facing NCC XY placement stage for `[20]` that updates `plane_refs` with placement metadata and warped reference aliases.
- `run_in_plane_registration_comparison_stage`: Notebook-facing `[20]` comparison stage that evaluates current NCC XY placement against optional ANTs rigid+affine placement while keeping the configured active backend explicit.
- `run_registration_search_stage`: Notebook-facing registration-search stage for `[16]` that updates `plane_refs`, persists scale/best-z caches, and rebinds legacy globals.

## `codeants_2pf_hcr.matching`

- `FunctionalAnatomyDebugConfig`: Typed functional↔anatomy debug summary configuration for notebook cell `[34a]`.
- `FunctionalRoiIdentityConfig`: Typed per-ROI identity export configuration for notebook cell `[50i]`.
- `attach_identity_to_functional_roi_geometry_df`: Attach a staged anatomy identity lookup to geometry-only ROI/anatomy rows while preserving pass-through trace-quality/response columns from an accepted ROI master schema.
- `HcrActivityExportConfig`: Typed HCR-centric activity export configuration for notebook cell `[50]`.
- `hcr_response_lookup_from_roi_master_df`: Normalize response-aware ROI master columns into the plane/function-label lookup used by HCR-centric identified-cell activity exports.
- `finalize_hcr_activity_export_tables`: Package-owned finalizer for notebook `[50]` HCR activity exports. It decorates HCR candidate geometry with ROI response calls and produces finalized status, raw candidate, responsive analysis, and candidate tables; it does not build `hcr_activity_status_summary.csv`.
- `resolve_plane_transform`: Resolve the notebook’s per-plane affine/tform binding from a plane-ref record.
- `resolve_anatomy_label_z`: Map an anatomy-intensity `best_z` to the corresponding anatomy-label stack page, including reversed label stacks.
- `resample_labels_nn`: Apply nearest-neighbor label resampling for functional-to-anatomy plane warps and shape harmonization, including ANTs transformlists via ANTsPy or the SimpleITK fallback for single-file affine transformlists.
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
- `build_hcr_anatomy_match_tables`: Recompute HCR/anatomy match, final-pair, review, and QC summary tables from warped HCR labels and anatomy labels using notebook-compatible overlap, distance, deduplication, pair-type, and quality rules.
- `build_functional_roi_master_df`: Build the authoritative ROI-centric functional-to-anatomy master table for `[50i]`.
- `annotate_session_anat_label_duplicates`: Mark same-anatomy-label functional ROI duplicates within each fish/session while retaining all ROI rows and ranking by geometry.
- `build_hcr_activity_tables`: Build HCR-centric functional candidate/status tables for `[50]`.
- `finalize_hcr_activity_export_tables`: Apply response-aware `[50]` export semantics to HCR status/candidate tables, retaining accepted labels without functional candidates in the raw export and producing finalized status/raw/trace/candidate tables.
- `MatchingConfig`: Typed matching-stage knob container.

## `codeants_2pf_hcr.stimulus`

- `StimulusConfig`: Typed stimulus parsing configuration.
- `normalize_session_label`: Normalize imaging-session identifiers such as `2`/`r2` for stimulus metadata lookup.
- `discover_functional_sessions`: Read preprocessing metadata to map output functional planes to imaging sessions.
- `find_experiment_log`: Discover the fish experiment log CSV, optionally constrained to an imaging session.
- `find_metadata_csv`: Discover the fish metadata CSV, optionally constrained to an imaging session.
- `load_events_df`: Normalize event/time columns from the experiment log.
- `load_metadata_params`: Read metadata key-value pairs.
- `parse_float`: Robust numeric parser for mixed metadata values.
- `resolve_presented_stimulus_metadata`: Resolve companion stimulus metadata (`trial_sequence`/`planned_schedule`), validate it against parsed experiment-log stimulus events, and expose planned-schedule block/stimulus tables when present.
- `classify_stim_type`: Collapse notebook stimulus codes to bout/continuous/mixed.
- `effective_motion_window`: Compute motion-response windows after onset delay.
- `build_prestim_baseline_windows`: Build merged prestim baseline windows from `df_evt`.
- `build_prestim_trial_windows`: Build per-trial prestim windows for null sampling.
- `build_null_window_start_map`: Build null AUC bootstrap start indices by duration.
- `compute_zscore_stats`: Compute pooled-baseline z-score parameters and validity masks.
- `parse_unilateral_stim`: Parse unilateral stimulus code to side/mode.
- `build_stim_tables`: Build block and trial tables from event logs.
- `resolve_stimulus_context`: End-to-end notebook-facing stimulus loader for `[55]`.
- `resolve_plane_stimulus_contexts`: Resolve per-plane stimulus contexts so multi-session fish use the session-specific experiment/meta CSVs.

## `codeants_2pf_hcr.activity`

- `ActivityConfig`: Typed response/BPI scoring configuration for `[50ia]`.
- `SingleFishBpiDiagnosticsConfig`: Typed BPI/activity diagnostics configuration for notebook cell `[56g]`.
- `build_suite2p_response_seed_table`: Build a pre-identity Suite2p ROI table plus in-memory dF/F map for early response scoring in `[23c]`.
- `build_response_bpi_tables`: Build response/BPI annotations and summary tables from the ROI master table.
- `prepare_single_fish_bpi_diagnostics_stage`: Notebook-facing response-aware diagnostics prep stage for `[56g]` that resolves activity/BPI columns, backfills response metadata from the ROI master table, and publishes plotting-ready bindings.

## `codeants_2pf_hcr.suite2p`

- `Suite2pStageConfig`: Typed Suite2p load/orientation configuration for notebook cell `[23a]`.
- `Suite2pStimulusLockedDiagnosticConfig`: Typed Suite2p stimulus/full-session diagnostic configuration for notebook cell `[23c]`.
- `infer_frame_rate_from_detail`: Resolve a consistent Suite2p frame rate from per-plane ops files.
- `build_suite2p_stimulus_locked_diagnostic`: Compute per-neuron, per-stimulus average Suite2p traces with session-aware stimulus metadata and frame-grid block timing for `[23c]`.
- `load_suite2p_stage`: Notebook-facing Suite2p stage loader for early `[23a]` that can run before functional reference preprocessing, returning legacy-shaped labels, plane maps, and source summary tables.
- `load_suite2p_dff_map`: Load Suite2p `F.npy` traces from disk and convert them to dF/F by plane.
- `run_suite2p_stimulus_locked_diagnostic_stage`: Notebook-facing Suite2p stimulus-locked diagnostic stage for `[23c]`.

## `codeants_2pf_hcr.segmentation`

- `AnatomyCellposeConfig`: Typed anatomy Cellpose configuration for notebook cell `[24a]`.
- `HcrCellposeConfig`: Typed HCR Cellpose segmentation configuration for notebook cell `[24]`.
- `resolve_hcr_cellpose_model_path`: Resolve the active HCR Cellpose model path from overrides, run config, or repo defaults.
- `collect_hcr_intensity_stack_paths`: Discover and filter HCR intensity stacks from `rbest`/`rn` preprocessing outputs for `[24]`, optionally restricted to one source, excluding fullbrain, channel1, mask outputs, and dot/AppleDouble sidecar files.
- `deduplicate_hcr_intensity_targets`: Collapse duplicate intensity inputs that would write the same Cellpose mask output.
- `run_anatomy_cellpose_stage`: Notebook-facing anatomy Cellpose stage for `[24a]` with deferred Cellpose import, cross-platform device selection, and an optional output root used to isolate ex vivo masks under `03_analysis/structural/ex_vivo/`.
- `run_hcr_cellpose_stage`: Notebook-facing HCR Cellpose stage for `[24]` that prepares stack inputs, resolves anisotropy, can restrict discovery to `rbest`/`rn`, and writes mask TIFFs.
- `resolve_functional_labels_for_plane`: Resolve per-plane functional labels for `[26]` from Suite2p, Cellpose, or legacy label sources with orientation handling.
- `resolve_native_suite2p_labels_for_plane`: Resolve the native Suite2p label image for a plane without Cellpose fallback for `[26a]`.
- `export_suite2p_native_labels_stage`: Notebook-facing Suite2p native-label export stage for `[26a]` that writes QA TIFFs and a manifest CSV.

## `codeants_2pf_hcr.hcr_warp`

- `HcrDirectWarpResult`: Result record for a direct ANTs HCR label warp.
- `build_direct_ants_hcr_transform_chain`: Build the notebook-equivalent direct HCR label warp transform chain: best-round rbest-to-2p warp/affine, plus rn-to-rbest warp/affine for non-best rounds.
- `run_direct_ants_hcr_label_warp`: Recompute HCR label TIFFs and warp metadata from raw HCR Cellpose masks, matching HCR intensity NRRDs, the prepared in vivo anatomy NRRD, and current ANTs transforms after notebook-equivalent prewarp small-label filtering.
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
- `render_functional_anatomy_center_overlay_qc_png`: Render center-crop anatomy-space overlays of transformed functional ROI outlines and anatomy-label outlines over the best-Z anatomy image for manual functional/anatomy registration QA.
- `render_functional_anatomy_plane_qc_row_png`: Render one functional-plane QA row per plane with functional reference, native Suite2p ROI boundaries, best-Z in vivo anatomy, anatomy-label boundaries, and the positioned functional reference in anatomy space; writes PNG plus per-plane review CSV for manual orientation/segmentation QA.
- `render_single_fish_hcr_anatomy_coexpression_summary`: Render the single-fish `[57b-anatomy-coexpression-summary]` figure and export anatomy-label/coexpression summary tables from in-plane HCR status rows; top-level/lazy exports now resolve to lightweight `plots.hcr` for staged pipeline use, while notebook QA imports may still use `plots.qa`.
- `show_centroid_match_qa_stage`: Notebook-facing centroid-distance QA stage for `[34]` with threshold UI, plane switching, context rendering, and `[26]`-matched functional label source selection via `use_suite2p_labels`.
- `show_functional_label_overlay_stage`: Notebook-facing functional-label overlay stage for `[26]`.
- `show_registration_overlay_stage`: Notebook-facing interactive registration overlay stage for `[22]`.

## `codeants_2pf_hcr.plots.annotations`

- `place_labels_no_overlap`: Shared 53a-style collision-aware in-panel label placer for dense cohort annotations (stack-up on local x collisions + y-limit expansion).

## `codeants_2pf_hcr.plots.analysis`

- `render_suite2p_full_session_heatmap`: Render the `[23c]` full-experiment Suite2p cell heatmap with frame X axis, white-to-black activity scale, and transparent stimulus spans.
- `plot_single_roi_57style`: Render the single-ROI `[57]` style figure and optional AUC table.
- `render_single_fish_50l_bpi_panel`: Render the single-fish `[50l]` top-left whole-population AUC-vs-BPI scatter from `[50ia]` response/BPI outputs.
- `render_single_fish_bpi_all_pairs_diagnostics`: Render `bpi_all_pairs.png/.pdf` from staged canonical BPI cells plus ROI identity rows, deriving plotting gene labels from `identity_label` when the ROI-centric BPI table lacks `gene`.
- `render_single_fish_50l_gene_auc_panel`: Render the single-fish `[50l]` marker-specific ipsi/contra AUC box/point/count-strip panels from the package-owned motion AUC point/count tables.
- `render_single_fish_50l_global_auc_panel`: Render the single-fish `[50l]` all-neurons ipsi/contra AUC panels as paired bout↔continuous ROI points with class-colored directional highlights, neutral non-directional classes, directional class-mean summaries, and unchanged count strips.
- `render_single_fish_50l_composite`: Render notebook stage `[50l]` as one package-owned composite, including stale `[56i]` AUC cache rebuilding, BPI panel, response/BPI donut, AUC panels, legacy figure globals, and `compound_50j_56i_unified.png/.pdf`.
- `render_single_fish_56h_per_gene_stimulus_trace_with_hcr_status`: Render `per_gene_stimulus_trace_with_hcr_status_56h.png/.pdf` plus a plotted-values CSV from `[56i]` per-gene motion AUC plot points and `hcr_activity_status.csv`; this replaces the copied legacy PNG in `make-figures` without depending on hidden notebook trace payloads.
- `render_single_fish_50l_population_response_donut_poster`: Render a standalone poster-scale `[50l]` population response donut with original response-status inner classes and collision-aware perimeter indicators.
- `render_cohort_56h_by_fish`: Render cohort per-gene/per-fish [56h]-style trace panels from prebuilt cohort trace payloads.
- `render_cohort_56h_fish_average_poster_traces`: Render a gene-row cohort [56h]-style poster trace figure with equal-weight fish-averaged gene traces and SEM across fish.
- `render_cohort_56g_diagnostics`: Render cohort [56g] activity/BPI diagnostic 2x2 panel from `cohort_bpi_cells_df`.
- `render_cohort_motion_auc`: Render cohort [cohort-auc] motion-window AUC figure; supports cached aggregate CSV reuse or per-fish aggregation fallback, optional global-median-label suppression, and mode-colored median labels.
- `render_cohort_56h_status_donut_grid`: Render cohort fish×gene HCR-status donut grid and export summary counts table.
- `render_cohort_50l_donut_row`: Render cohort [50l]-style fish-row donut figure and export long/wide counts tables.
- `render_cohort_50l_responsive_identity_donut_row`: Render cohort responsive-only fish-row donut figure with BPI inner ring and exact HCR-derived identity outer ring (plus `unidentified`) and export long/wide counts tables.
- `render_cohort_suite2p_23c_traces`: Render cohort `[23c]` responsive average traces with one line per fish/session/stimulus.
- `render_cohort_suite2p_23c_full_session_heatmaps`: Render and save per-fish `[23c]` full-session Suite2p heatmaps from cached cohort payloads.
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
