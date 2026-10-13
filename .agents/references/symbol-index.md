# Symbol Index

- `single_fish_readiness.audit_readiness`: read-only, spec-pinned remaining-fish
  readiness reports; never scientific writers or gate acceptance.
- `single_fish_readiness.expected_transform_paths`: explicit forward HCR route
  ordering in ANTs argument order.
- `single_fish_readiness.resolve_archive_path`: same-fish explicit archive-to-mirror
  path relocation, without rewriting original provenance.

Generated manually for the current extracted package surface.

## `codeants_2pf_hcr.paths`

- `is_appledouble(path)`: Check whether the basename starts with `._`.
- `is_visible_path(path)`: Exclude AppleDouble and `.DS_Store` components; retain other hidden entries.
- `visible_glob(directory, pattern)`, `visible_rglob(directory, pattern)`, `visible_iterdir(directory)`: Dependency-free, order-preserving Path iterators for operational directory scans.

## Trusted patterns

- `context.py`: path/config normalization pattern.
- `stimulus.py`: explicit stage input/output pattern.
- `activity.py`: response/BPI stage-owned semantics pattern.
- `pipeline.py`: staged single-fish pipeline contract/manifest pattern.
- `plots.*`: deterministic figure-builder pattern.
- `tools/`: general wrapper pattern only, not business-logic authority.
- `registrations/`: maintained manual registration entrypoints only, not business-logic authority.

## `codeants_2pf_hcr.qc_notebooks`

- `QCNotebookConfig`: Explicit `fish_id`, direct-containing `local_root`, and optional isolated `pipeline_root` selection for every QC notebook.
- `QCNotebookContext`: Fish-scoped, provenance-bearing review context with resolved fish, stage, and manifest paths.
- `resolve_qc_notebook_context`: Validate fish identity, direct-root layout, pipeline containment, and any persisted manifest fish/root claims before review.
- `build_qc_stage_inventory`: Build a read-only declared-stage artifact/manifest inventory for notebook `00`.
- `load_qc_csv`, `load_qc_json`, `load_qc_image`: Read persisted review evidence without changing it, with optional fish-table validation.

## `codeants_2pf_hcr.plots.qc_functional`

- `load_functional_reference_drift_qc`: Load manifest-declared functional references plus the authoritative per-fish temporal Z-drift bundle and response-evidence availability for notebook `01`.
- `build_early_response_trial_count_tables`: Compare presented events with per-cell valid-trial counts for each session and stimulus in Notebook 01.
- `load_early_response_trace_qc`, `render_early_response_stimulus_traces`: Verify and render same-run numeric [23c] trace payloads for notebook `01`, without displaying a cached PNG.
- `render_functional_reference_artifacts`, `render_functional_reference_drift_summary`: Render persisted reference and drift evidence without recomputation.
- `load_functional_registration_qc`: Load anatomy-prep, registration, transformed-label, and registration-QC manifests/tables/PNGs for notebook `02`.
- `functional_registration_plane_review`, `render_functional_registration_plane`, `render_functional_registration_all_planes`, `render_functional_registration_artifacts`, `show_functional_anatomy_zoom_viewer`: Read-only functional-registration views, including all-plane anatomy overlays and scroll-zoom inspection.

## `codeants_2pf_hcr.plots.qc_midline`

- `build_automatic_anatomy_midline_qc`, `render_automatic_anatomy_midline_qc`: Compute per-plane anatomical-axis estimates and a fish-consensus automatic midline proposal in anatomy space. These legacy read-only QC proposals cannot accept a midline or assign laterality.
- `build_automatic_native_functional_midline_qc`, `render_automatic_native_functional_midline_qc`: Compute the mounted-drive reviewed native-functional dark-corridor-plus-symmetry proposal, then fit one fixed axial angle with a separate robust depth translation for each acquisition session. It is a native-functional review artifact only and cannot provide anatomy-space laterality.
- `MidlineAnnotationSession`, `show_midline_annotation_gui`: Review-only, click-two-endpoint annotation for a midline per functional plane. Annotations remain in memory for review; this UI writes no sidecar or stage data.
- `load_accepted_anatomy_midline_context`, `build_midline_laterality_qc_tables`, `render_midline_laterality_plane_grid`, `render_midline_activity_plane_grid`: Read-only Notebook 06 `[56f-qc]` all-ROI and `[56f-qc-activity]` persisted-response-subset anatomy-space views. They fail closed without a complete, logbook-accepted, hash-bound anatomy-grid materialization; proposal lines are diagnostic only.
- `build_left_right_index_qc_table`, `render_left_right_index_plane_grid`, `render_left_right_index_histogram`: Pair hash-bound global motion-AUC rows into signed left/right indices by stimulus mode; map mode-responsive preferences on the accepted anatomy midline and show their distributions in Notebook 06. The editable LRI guide is review-only; negative AUC and unstable denominators are flagged without changing scoring.
- `load_identity_functional_qc_bundle`, `build_population_response_matrix`, `render_population_response_raster`: Load cross-checked canonical, HCR, Q1 trace, and motion-AUC artifacts and render the all-trace-available Suite2p-cell response-similarity view in Notebook 06. Clustering uses complete Suite2p-cell response profiles only; identity, response class, and accepted-midline side are annotations.
- `build_marker_trace_rows`, `render_marker_stimulus_traces`, `render_marker_auc_review`: Build HCR-centric responsive marker memberships from selected identity pairs, show individual-cell and marker-mean stimulus traces, and reuse persisted accepted-midline motion-AUC points for per-marker laterality review.
- `build_status_coverage_tables`, `render_status_coverage`, `build_identity_function_anatomy_rows`, `render_identity_function_anatomy_map`: Keep HCR-label functional fate separate from ROI-centric responsive-identity coverage, and map exact identity combinations and persisted response classes for identified responsive ROIs on all accepted anatomy planes.
- `load_native_functional_midline_annotation_session`, `load_suite2p_meanimg_midline_session`, `load_motion_corrected_mean_midline_session`: Load native-functional reference planes from persisted normalized TIFFs, metadata-oriented Suite2p `ops.npy` `meanImg` arrays, or bounded sampled means of motion-corrected movies. All are read-only review inputs; the movie fallback fails closed without explicit or metadata-validated orientation.

## `codeants_2pf_hcr.midline_review`

- `project_native_midline_to_anatomy`, `build_anatomy_midline_proposal_table`: Project persisted native-functional fixed-consensus lines through selected scale/NCC/ANTs registration and require exact registered-plane coverage. NCC-xy planes apply the composed native-to-anatomy transform once.
- `validate_logbook_midline_gate`, `load_accepted_anatomy_midline_context`: Require fish-specific logbook acceptance bound to proposal CSV/PNG hashes and verify the materialized anatomy-grid input and its source artifacts.

## `codeants_2pf_hcr.spatial`

- `anatomy_max_section_xy_radii_um`: Compute each 3-D anatomy label's maximal XY-section equivalent radius using physical XY spacing and summarize the all-label median; shared by Notebook 03/05 QC and matching.

## `codeants_2pf_hcr.midline`

- `fit_native_dark_corridor_model`, `propose_native_dark_corridor_midline`: Learn a plausible native-functional search area from manual labels, then suggest the darkest constrained corridor with optional left-right symmetry. Suggestions are review-only and never write canonical midlines or laterality.
- `fit_fish_midline_consensus`, `propose_fish_consensus_midline`: Combine a fish's local image suggestions into one fixed midline angle plus a robust, session-specific linear depth shift. The second function can retain a small, bounded local angle adjustment while keeping the predicted centre fixed. Both return review suggestions only.

## `codeants_2pf_hcr.plots.qc_geometry`

- `load_geometry_review_bundle`: Validate and load the three geometry-only CSVs written by `match-roi-to-anatomy`, rejecting duplicate keys, fish mismatch, or post-geometry semantics.
- `summarize_geometry_review`, `geometry_review_queue`: Build per-plane outcomes and reproducible matched/no-overlap/competition-lost review queues.
- `render_geometry_review_summary`, `render_geometry_placement_overview`, `render_centroid_offset_review`, `render_centroid_offset_detail`, `show_geometry_review_dashboard`: Render anatomy-context placement, centroid-offset, and interactive-filter views for notebook `03` without rerunning matching.

## `codeants_2pf_hcr.plots.qc_molecular`

- `inspect_molecular_geometry_qc`, `plot_molecular_geometry_qc`: Keep direct rbest-to-in-vivo, rbest-to-ex-vivo, ex-vivo-to-in-vivo, and later-round-to-ex-vivo evidence distinct while reviewing anatomy-space masks and pair ambiguity in notebook `04`; distinguish available masks from candidate matching that has not yet started.
- `MolecularLabelViewerData`, `load_molecular_label_viewer`, `load_hcr_match_overlay_viewer`, `show_molecular_label_viewer`: Preload same-grid in-vivo anatomy and explicitly selected HCR labels; the match-overlay loader classifies saved final-pair labels as accepted and saved-review-only labels as rejected, then provides read-only ipympl visibility toggles without callback disk I/O, matching, identity assignment, or pair updates.
- `build_cross_modality_mask_size_table`, `plot_cross_modality_mask_sizes`: Measure persisted anatomy, transformed functional, and retained HCR label bounding boxes with legacy max-minus-min physical X/Y/Z metrics; apply modality-local XY q05 drop/q95 flag policy and render all-modality plus HCR gene/round XY/Z review distributions.
- `build_hcr_anatomy_centroid_offset_table`, `plot_hcr_anatomy_centroid_offsets`: Read accepted HCR/anatomy final-pair tables and render lateral XY-distance plus signed Z-offset violins by gene, accepted-pair sample sizes, and the physical all-label maximum-XY-section anatomy-radius reference for notebook `05`; requires an explicit anatomy-grid voxel source (normally the prepared NRRD) because label TIFF metadata may be absent, and remains review-only without recomputing pair membership.
- `resolve_hcr_matching_artifact_root`, `build_hcr_anatomy_match_outcome_table`, `plot_hcr_anatomy_match_outcome_donuts`: Select one persisted HCR artifact root (staged when present, otherwise the legacy fish-local set) and render the legacy `[50]` one-ring per-gene anatomical outcome donuts from saved labels, match tables, q95 metadata, and saved plane representation; no matching or assignment is recomputed.
- `build_hcr_functional_plane_status_donut_table`, `plot_hcr_functional_plane_status_donuts`: Recreate the legacy two-ring functional-plane representation audit in notebook `04`: inner scope is within/outside/unmatched after the q95 exclusion, and the outer ring partitions saved within-plane HCR activity status or records the outside/unmatched outcome; both functions are read-only.
- `inspect_molecular_identity_qc`, `plot_molecular_identity_qc`: Enforce frozen geometry and compare geometry fields before/after identity attachment in notebook `05`.
- `build_molecular_identity_fate_table`: Read frozen ROI-centric links and HCR final pairs with required geometry-owned `plane_refs_path`; validate explicit direct/reverse anatomy-label Z provenance before classifying in-plane label fates.
- `plot_molecular_correspondence_tiles`: Render persisted HCR→anatomy→functional correspondence crops in anatomy/GCaMP context for notebook `05`, requiring geometry table and plane-reference paths to resolve label pages independently of intensity `best_z`; retains every selected frozen-geometry link by default and display-only gene, response, and flag filters, without inferred Z fallback or changed final-pair membership.
- `inspect_activity_export_qc`, `plot_activity_export_qc`: Review session-aware response/BPI provenance, canonical keys, figures, and ROI-centric versus HCR-centric scopes in notebook `06`; optional `scored_windows_path` and `reference_manifest_path` validate the persisted plane/session mapping without extending ROI schemas.
- `build_activity_bpi_gate_table`, `plot_activity_bpi_gate`: Read persisted response classes and strict BPI preferences for ROI-centric Notebook 06 review; non-BPI summaries use every response-assessable ROI, BPI panels use response-positive ROIs only, preference is color-coded, response-test class is marker-coded, and the all-ROI donut retains unavailable rows.

## `codeants_2pf_hcr.hcr_match_inspector`

- `render_hcr_match_inspector`: Render a self-contained, read-only HTML inventory of every transformed HCR label from saved metrics, candidate/final-pair tables, the geometry Z sidecar, anatomy/functional masks, and the identity-writer ROI table. It does not recompute matches or record gate decisions. The thin CLI is `tools/render_hcr_match_inspector.py`.

## `codeants_2pf_hcr.response_trace_inspector`

- `render_response_trace_inspector`: Render a self-contained, read-only HTML of baseline-shifted dF/F traces for every ROI over the persisted LB/RB/LC/RC BPI windows (16 trials and 4 means), labelled and coloured by tuning (stored `bpi_preference` among responders, |BPI| > `bpi_zero_band`), with the response test passed, valence and a review-only floor-excluded group; refuses unless trial AUCs reproduce the score table. Thin CLI `tools/render_response_trace_inspector.py`.
- `floor_excluded_mask`: ROIs that pass a persisted null threshold but are not response-positive only because |mean AUC| is below `response_auc_threshold`.

## `codeants_2pf_hcr.pipeline`

- `PIPELINE_STAGE_ORDER`: Roadmap-order stage names for the single-fish staged pipeline.
- `PIPELINE_MANIFEST_VERSION`: Current first-pass manifest schema version for staged pipeline manifests.
- `POST_PREPROCESSING_STAGE_NAMES`: Read-only stage-status names for existing post-preprocessing staged output folders.
- `GRANULAR_PREPROCESSING_STAGE_NAMES`: Concrete writer-stage names for preparation/segmentation slices that must not be hidden under generic `preprocess-*` command names.
- `STAGED_COMPARISON_STAGE_NAMES`: Declared stage names accepted by staged comparison/baseline commands, covering upstream writer surfaces plus post-processing stages.
- `SingleFishPipelineConfig`: Typed fish/root/strictness/dry-run knob container for staged single-fish pipeline commands.
- `PipelinePaths`: Resolved fish-scoped paths for staged pipeline commands without creating notebook output folders during path resolution.
- `StageContract`: Declarative stage contract record containing stage order, purpose, dependencies, and first-pass read-only status.
- `ManifestPathRecord`: JSON-ready path record with path, existence, kind, required status, and optional glob count/pattern.
- `StageCheckRecord`: JSON-ready semantic check record with status, expected value, observed value, and detail.
- `StageManifest`: JSON-ready stage manifest payload with fish ID, stage status, dry-run state, inputs, outputs, parameters, warnings, and errors.
- `StageOutputSpec`: Declared staged output with optional control/parity metadata, table-specific keyed/exact/numeric CSV comparison columns, semantic-column comparison for narrowed staged tables, generated-file/directory nonempty checks, manifest-aware upstream explicit-output path handling for status/comparison, and figure dimension/thumbnail warning thresholds for staged comparison/baseline commands.
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
- `run_prepare_functional_reference_stacks_stage`: Writer stage wrapping `[12]` functional reference preparation. It discovers or accepts motion-corrected functional TIFFs, resolves polarity, excludes the first selected TIFF block by default using preprocessing metadata, and consumes the per-product upstream spatial frame declaration so canonical inputs are not reoriented while legacy acquisition inputs receive the compatibility transform exactly once. Its manifest records both input and output XY frames. With `run_23c=True` (CLI `--run-23c`), it also persists the stage-owned early Suite2p response QC payload and records its sources and outputs in the same manifest for Notebook 01.
- `run_register_functional_to_anatomy_stage`: Writer stage wrapping `[16]` NCC placement and `[20]` in-plane registration. A version-4 `preprocessing_ncc_manifest_path` is the preferred route: it validates and reuses the canonical pooled reference, scale, full best-Z profile, and XY placement without writing legacy cache copies or repeating those searches, then runs ANTs residual refinement. The legacy staged-reference route and `ncc_cache_source_dir` remain available for older analyses.
- `run_transform_functional_rois_to_anatomy_stage`: Rasterize every Suite2p ROI (not only Suite2p `iscell` rows) and apply the exact selected composed functional-to-anatomy transform, writing native and anatomy-space TIFFs plus a per-plane transform manifest. `suite2p_stat_xy_frame` explicitly records when stat.npy remains acquisition-framed despite canonical input TIFF provenance.
- `run_make_functional_registration_qc_stage`: Render four post-transform review artifacts—plane rows, intensity overlays, center label overlays, and NCC depth profiles—plus companion CSV provenance only after transformed functional labels exist.
- `run_register_hcr_to_anatomy_stage`: Writer for one staged HCR label stack per mask plus `hcr_label_metrics.csv` and freshly computed match/review/final-pair tables. It applies table-driven small-speck and low-confidence-segmentation exclusions and the fish-derived all-anatomy maximal-section XY-radius gate before dedup. `recompute_direct_ants=True` additionally warps labels using the selected ex-vivo bridge or explicit legacy direct route without writing a filtered duplicate stack.
- `build_hcr_label_metrics`: Measure each HCR label in native space when available, otherwise explicitly in aligned 2P space, and assign small/large quality flags without editing the label image.
- `run_match_roi_to_anatomy_stage`: Writer stage for geometry-only ROI/anatomy matches. The recompute mode resolves direct-versus-reversed anatomy-label Z order against the anatomy intensity stack (or consumes an explicit mode), persists every resolved label page in `plane_refs_summary_geometry.json`, and uses those exact pages for matching. It restores the NCC preplacement before replaying an ANTs residual transform and can explicitly orient acquisition-framed Suite2p `stat.npy` labels before writing the three geometry CSVs. Missing or ambiguous label-Z provenance fails closed; control-geometry staging from an explicit `--source-root` remains available for bootstrap/diagnostic use. An explicit `supersede_accepted_control_reason` turns label/unique drift against the fish-local control into recorded warnings (control hash and reason in parameters); key parity stays strict.
- `build_single_fish_status`: Build a compact read-only trust-state summary from the dry-run input audit plus any persisted audit manifest.
- `build_single_fish_compare_staged_manifest`: Build a read-only comparison manifest for one existing declared staged output surface, including upstream writer surfaces and post-processing stages; for `make-qa-report`, also validates QA summary JSON parseability, required sections, review status, and image metadata/readability.
- `run_single_fish_freeze_legacy_baseline_stage`: Freeze declared legacy/control outputs for one or all comparison-stage specs into `DATA_ROOT/pipeline_baselines/FISH_ID/legacy_singleFish/stages/`, refusing overwrite without `overwrite=True`.
- `build_single_fish_compare_legacy_baseline_manifest`: Build a read-only comparison manifest for one staged output surface against the frozen `legacy_singleFish` bundle.
- `compare_single_fish_legacy_baseline`: Build a read-only aggregate comparison payload for declared staged outputs against the frozen `legacy_singleFish` bundle.
- `build_single_fish_score_activity_bpi_recompute_manifest`: Build a read-only `score-activity-bpi` recompute audit manifest that calls `activity.build_response_bpi_tables` without the precomputed-output shortcut and compares in-memory activity/BPI tables to control CSVs.
- `build_single_fish_hcr_activity_replay_manifest`: Build a read-only HCR-centric `[50]` replay audit manifest from the geometry-owned `plane_refs_summary_geometry.json`, Suite2p, anatomy labels, staged HCR final pairs, and a response-aware ROI master used only as lookup. It fails closed on missing or inconsistent label-Z provenance, checks selected ANTs transformlist availability, runs selected-ANTs and persisted affine transform variants in memory, runs `matching.build_hcr_activity_tables` plus `matching.finalize_hcr_activity_export_tables`, compares row/key counts to accepted HCR outputs when available, and keeps promotion disabled.
- `run_single_fish_assign_hcr_identity_stage`: Write the staged identity/HCR registration CSV bundle under `pipeline_root/assign-hcr-identity/registration` after requiring staged ROI/anatomy geometry, its geometry-owned plane-reference sidecar, Suite2p, anatomy labels, and staged HCR/anatomy artifacts, refusing overwrite without `force_recompute`. It accepts an explicit `activity_input_path` and otherwise prefers the same-root score master before the legacy source master; legacy identity/HCR CSVs are optional comparison controls. It validates the sidecar convention, resolved pages, plane coverage, and selected-label membership before recomputing `anatomy_identity_lookup.csv`, `functional_roi_activity_identity.csv`, and HCR-centric activity/status/candidate CSVs.
- `run_single_fish_score_activity_bpi_stage`: Recompute response/BPI tables from frozen ROI/anatomy geometry plus Suite2p trace provenance through `activity.build_response_bpi_tables(precomputed_scored_bpi_df=None)`, then write staged score CSVs under `pipeline_root/score-activity-bpi/registration`. It does not use molecular identity and refuses overwrite without `force_recompute`.
- `run_single_fish_export_canonical_tables_stage`: Assemble the staged canonical registration CSV bundle under `pipeline_root/export-canonical-tables/registration` from staged score outputs and staged or explicit HCR/identity roots, refusing overwrite without `force_recompute`.
- `run_single_fish_make_qa_report_stage`: Generate staged `qa_report.md`, `qa_report.html`, `qa_report.pdf`, and `qa_report_summary.json` under `pipeline_root/make-qa-report` from staged canonical export inputs, including canonical table rows, staged output status, per-plane registration/matching QA summaries from existing staged overlay/match CSVs, a manual review checklist with image readability/dimension metadata, and inline image previews for existing QA/figure PNG artifacts; refuses overwrite without `force_recompute`.
- `run_make_anatomy_midline_proposal_stage`, `run_materialize_anatomy_midline_stage`: Write projected anatomy-grid midline proposal evidence, then materialize it only after the fish-specific logbook gate and proposal CSV/PNG hashes match.
- `run_single_fish_make_motion_auc_tables_stage`: Write laterality-dependent AUC tables from canonical rows and the accepted anatomy-grid artifact; notebooks do not run this writer.
- `run_single_fish_make_figures_stage`: Stage the declared figure artifacts under `pipeline_root/make-figures/04_plots` after verifying staged canonical export CSV inputs and declared `[56i]` AUC table inputs, rendering package-owned `compound_50j_56i_unified.png`, `bpi_all_pairs.png`, `per_gene_stimulus_trace_with_hcr_status_56h.png`, responsive identity donut, and HCR anatomy coexpression summary, and refusing overwrite without `force_recompute`.
- `build_single_fish_downstream_stage_manifest`: Build a read-only manifest for an existing post-preprocessing staged output folder.
- `build_single_fish_downstream_stage_manifests`: Build read-only manifests for all current post-preprocessing staged output folders.
- `build_single_fish_stage_status`: Summarize one downstream read-only stage manifest plus persisted-manifest state.
- `downstream_stage_names`: Return the supported current downstream read-only stage-status names.
- `staged_comparison_stage_names`: Return the supported stage names for `compare-staged`, `freeze-legacy-baseline`, and `compare-legacy-baseline`.
- `_stage_output_specs_for_inventory`: Internal comparison/status view of staged outputs; for upstream stages with a same-pipeline-root persisted manifest, it follows the actual manifest output paths from explicit writer overrides instead of defaulting to canonical package paths.
- `_manifest_records_for_inventory`: Internal upstream status helper that re-describes same-pipeline-root manifest input/output paths against the current filesystem so persisted writer manifests can report `current` while still detecting stale files.
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
- `run_segment_ex_vivo_anatomy_cellpose_stage`: Writer stage for ex vivo anatomy Cellpose masks isolated under `03_analysis/structural/ex_vivo/`; manifests include runtime/device versions and model SHA-256 provenance.
- `run_segment_hcr_cellpose_stage`: Writer stage for HCR Cellpose masks from an explicit source such as `rbest`; manifests include runtime/device versions and model SHA-256 provenance while cached-mask reuse avoids importing optional Cellpose/Torch runtimes.
- `compare_persisted_manifest`: Compare a persisted manifest's path records against the current audit and report missing/current/stale/fail state.
- `compare_single_fish_staged_outputs`: Build a read-only aggregate comparison payload for declared existing staged outputs. Upstream comparisons activate only from staged `pipeline_outputs` or same-pipeline-root persisted upstream manifests, so legacy preprocessing files and unrelated persisted manifests alone do not become active staged comparisons.
- `stage_manifest_to_json`: Serialize a stage manifest to JSON text for stdout or persistence.
- `run_single_fish_audit_inputs_stage`: Run the first staged pipeline input audit in read-only/dry-run mode and return a JSON-ready manifest with labeled input records, schema checks, cross-table consistency checks, value-domain checks, and optional staged parity checks, without writing into fish folders.
- CLI wrapper: `tools/single_fish_pipeline.py` exposes HCR `register-hcr-to-anatomy` with opt-in `--recompute-direct-ants` and `--hcr-warp-route {exvivo_bridge,legacy_direct}`. The default recompute route is the explicit ex-vivo bridge; `--write-manifest` persists its manifest along with the other manifest-bearing staged commands.
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
- `preprocess_anatomy_uint8_stage`: Legacy `[14a]` signed-16-bit anatomy preprocessing remains available for explicitly acquisition-framed inputs. When the upstream spatial manifest declares the input canonical, the stage passes the NRRD through without XY orientation, Z reversal, resize, or rewrite and rebinds `ANAT_STACK_PATH` directly.
- `infer_anatomy_stack_path`: Discover the raw in vivo anatomy stack while excluding mask/label/overlay and ex vivo-looking candidates so staged in vivo prep does not silently pick bridge-registration inputs.
- `preprocess_ex_vivo_anatomy_stage`: Experimental same-fish bridge preprocessing stage for raw ex vivo 2P anatomy stacks from `01_raw/2p/anatomy`; writes isolated pre-manual-rotation NRRD plus JSON provenance under `02_reg/00_preprocessing/2p_anatomy/ex_vivo/` without rebinding canonical in vivo `ANAT_STACK_PATH` or creating duplicate TIFF image outputs.
- `apply_manual_anatomy_orientation_stage`: Experimental helper that applies brainAtlas-style preview-angle XY rotation, optional square crop, rot90, and explicit axis flips to a preprocessed anatomy stack, then writes manual-oriented NRRD plus JSON provenance for ex vivo registration trials.
- `read_raw_metadata_polarity`: Read per-fish raw metadata orientation from `01_raw/2p/metadata/*metadata*.csv`, normalize `bottom-left`/`top-right` to `north`/`south`, and fail on conflicts.
- `read_matching_metadata_polarity`: Read the legacy fallback polarity from `matchingMetadata.csv`.
- `resolve_func_polarity`: Resolve orientation with override support from raw per-fish metadata or the canonical spatial manifest. `matchingMetadata.csv` is available only through the explicit `allow_legacy_matching_metadata=True` compatibility switch.
- `build_voxel_debug_stage`: Notebook-facing anatomy voxel debug helper for `[8a]`.
- `orient_functional_stacks_stage`: Notebook-facing functional orientation stage for `[10]` that audits legacy full-stack caches and only writes oriented movie stacks when explicitly requested.
- `build_final_fish_audit_stage`: Notebook-facing final contamination audit for `[99-debug-fish-audit]`.

## `codeants_2pf_hcr.orientation`

- `AnatomyPolarityConfig`: Configuration for multi-percentile intensity/edge HOG polarity inference and held-out calibration.
- `read_anatomy_projection_variants`: Read legacy raw anatomy TIFF pages directly and build normalized XY projection variants without trusting malformed TZC series metadata.
- `build_anatomy_polarity_model`: Build canonical-orientation reference templates from manually labeled fish.
- `cross_validate_anatomy_polarity`: Hold out an entire acquisition prefix (for example `L396`) while scoring each reference fish.
- `predict_anatomy_polarity`: Compare north=`flipY` and south=`flipX`, returning a prediction only when all configured variants agree and clear the calibrated margin.
- `run_anatomy_polarity_prediction`: Write review-only validation/prediction CSVs, a summary JSON, and an all-target QC PNG; it never edits fish metadata.

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

- `FunctionalReferenceConfig`: Typed functional-reference cache/build configuration for notebook cell `[12]`; oriented references can be built from original motion-corrected stacks without saving full oriented movies, with explicit metadata-aware first-block exclusion.
- `FunctionalPlacementConfig`: Typed NCC XY placement configuration for notebook cell `[20]`.
- `InPlaneRegistrationComparisonConfig`: Typed in-plane method-comparison configuration for notebook cell `[20]`; ANTs uses deterministic seed 0 by default, and final-transform fallback is disabled unless explicitly requested by a non-pipeline caller.
- `RegistrationSearchConfig`: Typed registration-search knob container for notebook cell `[16]`.
- `imread_any`: Read TIFF or NRRD images with minimal notebook dependencies. NRRD reads prefer SimpleITK's Z/Y/X array convention; the pynrrd fallback explicitly requests C-order so anatomy Z cannot be silently interpreted as the last axis.
- `zproject_mean`: Mean projection helper.
- `norm01`: Robust percentile normalization to `[0, 1]`.
- `local_unsharp`: Local contrast sharpening helper.
- `corrcoef_img`: Pearson correlation for image pairs.
- `top_correlated_mean`: Suite2p-like top-k frame reference builder.
- `best_z_by_ncc`: Best-z search by NCC-like scoring.
- `apply_func_orientation`: Apply the direct effective functional transform (`north=flipY`, `south=flipX`) for explicitly legacy/acquisition-framed inputs.
- `apply_square_region_mask`: Preserve values inside an anatomy-space square and set outside pixels to zero for masked registration.
- `build_functional_references_stage`: Notebook-facing functional reference stage for `[12]` that preserves `plane_refs` plus legacy bindings/cache names, validates declared XY frames, skips orientation for upstream canonical inputs, and applies direct orientation only for explicit legacy/acquisition inputs.

## `codeants_2pf_hcr.suite2p_provenance`

- `check_suite2p_input_provenance`: Read-only per-plane source/frame/trace-count preflight; rejects stale or wrong-plane inputs before recipe creation.
- `resolve_reconciled_suite2p_input`: Resolve explicit recorded aliases only when ops hashes, metadata hash and audited input fingerprints still match; used by the authoritative Suite2p loader.
- `reconcile_legacy_functional_provenance`: Evidence-backed one-time JSON repair for recorded resonant legacy averaging; sample raw blocks, check motion orientation, archive/checksum original spatial manifest, preserve TIFFs and all Suite2p arrays.

## `codeants_2pf_hcr.spatial_contract`

- `load_spatial_manifest`: Validate the upstream canonical functional/anatomy XY and registration-Z contract; unknown or contradictory frames fail closed.
- `manifest_polarity`: Read polarity provenance from the canonical spatial manifest.
- `canonical_anatomy_path`: Resolve the single upstream registration-ready anatomy NRRD.
- `declared_xy_frame`: Verify that a functional or anatomy product is declared by the upstream manifest.

## `codeants_2pf_hcr.ncc_contract`

- `preprocessing_ncc_manifest_path`: Resolve the canonical per-fish preprocessing NCC manifest.
- `load_preprocessing_ncc_handoff`: Validate the versioned handoff schema, passing drift status, fish identity, selected canonical anatomy, coordinate frames, placement table, anchor profiles, and reference directory.
- `plane_refs_from_preprocessing_ncc_handoff`: Reconstruct registration-ready `plane_refs` from the saved pooled references, scales, complete best-Z profiles, and XY placements without rerunning NCC.

- `ncc_xy`: Shared NCC XY placement primitive for notebook cell `[20]`.
- `scale_image`: Resize a 2D functional reference by an empirical NCC search scale.
- `registration_metric_from_scores`: Summarize NCC scores into best-z and peak metrics.
- `load_functional_polarity`: Read north/south orientation from a matching accepted `prepare-functional-reference-stacks` manifest and fail on missing, non-pass, or wrong-fish provenance.
- `load_functional_frame_selection`: Read and validate the matching accepted preparation manifest's per-plane first-block-exclusion boundary, source-frame count, and retained-frame count; fail closed if exclusion provenance is absent or inconsistent.
- `run_functional_z_drift_diagnostic`: Build time-windowed, top-correlated functional references from the manifest-selected post-block-0 motion-corrected frames using manifest-resolved polarity, reuse persisted per-plane NCC scales, score the canonical anatomy Z stack, and write interval/profile/session-summary CSVs plus review plots whose X axis names each retained block and within-block window; it does not rerun ANTs or change canonical registration outputs.
- `run_ncc_placement_stage`: Notebook-facing NCC XY placement stage for `[20]` that updates `plane_refs` with placement metadata and warped reference aliases.
- `run_in_plane_registration_comparison_stage`: Notebook-facing `[20]` comparison stage that evaluates current NCC XY placement against optional ANTs rigid+affine placement while keeping the configured active backend explicit; per plane it selects the successful method with the higher post-transform NCC and records the reason.
- `run_registration_search_stage`: Notebook-facing registration-search stage for `[16]` that updates `plane_refs`, persists scale/best-z caches, rejects cached entries whose profile does not reproduce from the current reference, and rebinds legacy globals.

## `codeants_2pf_hcr.matching`

- `FunctionalAnatomyDebugConfig`: Typed functional↔anatomy debug summary configuration for notebook cell `[34a]`.
- `FunctionalRoiIdentityConfig`: Typed per-ROI identity export configuration for notebook cell `[50i]`.
- `attach_identity_to_functional_roi_geometry_df`: Attach a staged anatomy identity lookup to geometry-only ROI/anatomy rows while preserving pass-through trace-quality/response columns from an accepted ROI master schema.
- `HcrActivityExportConfig`: Typed HCR-centric activity export configuration for notebook cell `[50]`.
- `hcr_response_lookup_from_roi_master_df`: Normalize response-aware ROI master columns into the plane/function-label lookup used by HCR-centric identified-cell activity exports.
- `finalize_hcr_activity_export_tables`: Package-owned finalizer for notebook `[50]` HCR activity exports. It decorates HCR candidate geometry with ROI response calls and produces finalized status, raw candidate, responsive analysis, and candidate tables; it does not build `hcr_activity_status_summary.csv`.
- `resolve_plane_transform`: Resolve the notebook’s per-plane affine/tform binding from a plane-ref record; `tform_src == "ncc_xy"` with reference shapes returns the native-to-anatomy scale-plus-placement transform.
- `resolve_anatomy_label_z`: Map an anatomy-intensity `best_z` to the corresponding anatomy-label stack page, including reversed label stacks.
- `infer_anatomy_label_z_mode`: Compare anatomy-label boundaries with intensity edges across registered best-Z planes and return the direct or reversed page convention plus both aggregate scores.
- `validate_anatomy_label_z_provenance`: Fail-closed validator for explicit mode/page consistency, page range, geometry-plane coverage, and selected-label membership.
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
- `classify_stim_type`: Collapse the complete presented stimulus vocabulary to bout/continuous/mixed, including trajectory variants and whole-field `WFC*` conditions.
- `classify_bpi_stim_type`: Admit only balanced LB/RB bout and LC/RC continuous conditions to response/BPI pooling; other presented conditions remain QC-visible.
- `effective_motion_window`: Compute motion-response windows after onset delay.
- `build_prestim_baseline_windows`: Build merged prestim baseline windows from `df_evt`.
- `build_prestim_trial_windows`: Build per-trial prestim windows for null sampling.
- `build_null_window_start_map`: Build null AUC bootstrap start indices by duration.
- `compute_zscore_stats`: Compute pooled-baseline z-score parameters and validity masks.
- `parse_unilateral_stim`: Parse legacy `LLB/RLC` and current `LB/RB/LC/RC_trajectory` unilateral circular-motion codes to side/mode; exclude approach, whole-field, and mixed conditions.
- `build_stim_tables`: Build block and trial tables from event logs.
- `resolve_stimulus_context`: End-to-end notebook-facing stimulus loader for `[55]`.
- `resolve_plane_stimulus_contexts`: Resolve per-plane stimulus contexts so multi-session fish use the session-specific experiment/meta CSVs.
- `remap_stimulus_tables_to_frame_grid`: Remap block-relative stimulus/event tables onto an explicit concatenated trace frame grid while retaining block timing provenance.
- `map_stimulus_context_to_trace_grid`: Map a resolved session context, including its complete planned block schedule, onto a Suite2p trace so B0 remains context and B1/B2 scoring uses physical full-trace frames.

## `codeants_2pf_hcr.activity`

- `ActivityConfig`: Typed response/BPI scoring configuration for `[50ia]`.
- `SingleFishBpiDiagnosticsConfig`: Typed BPI/activity diagnostics configuration for notebook cell `[56g]`.
- `build_suite2p_response_seed_table`: Build a pre-identity Suite2p ROI table plus in-memory dF/F map for early response scoring in `[23c]`.
- `build_response_bpi_tables`: Build response/BPI annotations and summary tables from the ROI master table after centrally mapping each session context onto its complete Suite2p trace.
- `build_presented_stimulus_response_tables`: Build separate per-condition and any-presented-stimulus diagnostic response screens from the mapped `[23c]` motion windows, with a family-wise-adjusted prestimulus null; never changes BPI pools.
- `prepare_single_fish_bpi_diagnostics_stage`: Notebook-facing response-aware diagnostics prep stage for `[56g]` that resolves activity/BPI columns, backfills response metadata from the ROI master table, and publishes plotting-ready bindings.

## `codeants_2pf_hcr.suite2p`

- `Suite2pStageConfig`: Typed Suite2p load/orientation configuration for notebook cell `[23a]`.
- `Suite2pStimulusLockedDiagnosticConfig`: Typed Suite2p stimulus/full-session diagnostic configuration for notebook cell `[23c]`.
- `build_suite2p_all_roi_labels`: Rasterize all stat.npy ROI labels, including rows excluded by Suite2p `iscell`, for geometry-complete transform outputs.
- `infer_frame_rate_from_detail`: Resolve a consistent Suite2p frame rate from per-plane ops files.
- `build_suite2p_stimulus_locked_diagnostic`: Compute per-neuron, per-stimulus average Suite2p traces with session-aware stimulus metadata and frame-grid block timing for `[23c]`.
- `load_suite2p_stage`: Notebook-facing Suite2p stage loader for early `[23a]` that can run before functional reference preprocessing, returning legacy-shaped labels, plane maps, and source summary tables.
- `load_suite2p_dff_map`: Load Suite2p `F.npy` traces from disk and convert them to dF/F by plane.
- `run_suite2p_stimulus_locked_diagnostic_stage`: Notebook-facing Suite2p stimulus-locked diagnostic stage for `[23c]`; it persists the exact scored-window table used for the single full-session heatmap overlays.

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
- `build_ex_vivo_bridge_hcr_transform_chain`: Build the strict target-to-source ex-vivo bridge chain. It selects rbest→ex vivo→2P, direct rn→ex vivo→2P, or rn→rbest→ex vivo→2P by exact source/target transform names and fails closed when the required pair is absent.
- `run_direct_ants_hcr_label_warp`: Recompute HCR label TIFFs and warp metadata from raw HCR Cellpose masks, matching HCR intensity NRRDs, the prepared in vivo anatomy NRRD, and current ANTs transforms after notebook-equivalent prewarp small-label filtering. It supports explicit `exvivo_bridge` and `legacy_direct` routes.
- `run_hcr_external_bigwarp_label_stage`: Notebook-facing wrapper for single-fish `[43]` external-BigWarp label prep / load logic with stage-local ANTs import.
- `run_hcr_external_bigwarp_intensity_stage`: Notebook-facing wrapper for single-fish `[43b]` rn->rbest intensity prep logic with stage-local ANTs import.

## `codeants_2pf_hcr.traces`

- `TraceExportConfig`: Typed trace-export configuration for `[51]`.
- `MotionAucPlotConfig`: Typed motion-AUC table-build configuration for the `[50l]`-embedded `[56i]` owner path.
- `load_midline_context`: Load and validate the fish-scoped midline bundle for `[56]`, `[56h]`, `[56f-qc*]`, and `[50l]`/`[56i]` AUC preparation.
- `annotate_midline_side`: Add `midline_signed_dist_px`, `midline_side`, `midline_uncertain`, and `midline_space` columns using package-owned midline semantics.
- `filter_high_confidence_pairs`: Drop HCR/functional pair rows flagged by known low-confidence columns while optionally returning filter counts.
- `extract_window_with_padding`: Extract trace windows with notebook-compatible `pad_nan` and `strict` behavior.
- `build_single_fish_motion_auc_plot_tables`: Build the single-fish motion-window AUC ROI panel / plot points / plot counts tables for `[50l]` / `[56i]`, preserving current ROI-centric all-neuron and HCR-centric gene-group semantics. When passed a logbook-gated accepted anatomy-midline artifact, laterality is recomputed in anatomy space and persisted with its artifact hash.
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
- `compute_anatomy_median_xy_radius_um`: Compatibility wrapper for the all-3-D-label maximal-XY-section radius reference (microns), independent of functional-plane selection.
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
- `render_accepted_global_laterality_auc_qc`: Read-only Notebook 06 Q7 global ipsi/contra AUC plus count strips. It visibly blocks unless the persisted all-neuron outputs carry the exact hash of a complete accepted anatomy-grid midline review sidecar.
- `render_single_fish_50l_composite`: Render notebook stage `[50l]` as one package-owned composite, including stale `[56i]` AUC cache rebuilding, BPI panel, response/BPI donut, AUC panels, legacy figure globals, and `compound_50j_56i_unified.png/.pdf`.
- `render_single_fish_56h_per_gene_stimulus_trace_with_hcr_status`: Render `per_gene_stimulus_trace_with_hcr_status_56h.png/.pdf` and cell/trial-count summary CSV. Required inputs: `points_csv`, `hcr_status_csv`, `roi_panels_csv`, `response_csv`, `scored_windows_csv`, `suite2p_root`, `fish_id`, `outdir`. Uses persisted gene/ROI populations and midline sides, scoring dF/F and trial baselines, individual traces plus equal-cell means in per-gene 2×2 grids; grey low activity and dashed suppressed-only cells remain visible. Returns `trace_axes` and numeric `traces_df` for verification.
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

## `codeants_2pf_hcr.single_fish_run`

- `RunSpecError`: Invalid declarative single-fish run specification error.
- `StageSpec`: Immutable command/input/output/validator declaration for one ordered stage.
- `RunSpec`: Validated fish, path, Git, interpreter, provenance, environment, and DAG declaration.
- `load_run_spec`: Load and strictly validate a versioned JSON run specification.
- `describe_path`: Compute a deterministic SHA-256 content record for a file, symlink, or directory tree.
- `validate_spec_environment`: Verify the clean scientific-code pin, interpreter/package versions, required roots, and accepted provenance hashes.
- `build_run_plan`: Produce the ordered run or resume plan without executing a stage.
- `execute_run`: Execute the declared DAG, saving full logs and atomic per-stage provenance/hash state; resume skips only hash-current stages.
- `build_run_status`: Report saved stage state and currentness without executing commands.
- `verify_run`: Verify the pinned environment/provenance and every saved input/output hash.
- `audit_run_artifacts`: Read-only validation of real ROI key relationships, signed-response union, family-wise 23c tails, fixed response windows, BPI pool and geometry-assignment hashes; reports response/suppression counts.

## `codeants_2pf_hcr.single_fish_job`

- `run_finishing_job`: Noninteractive fish-locked resume, pinned/hash verification, real-artifact audit, optional no-op proof, and QC gallery export; explicit fresh review-gate recipes export preliminary notebooks 00–05 without accepting laterality, while full recipes require all seven. Saves atomic terminal reports without polling.
- CLI: `tools/single_fish_job.py`; usage and failure/reuse policy: `docs/single-fish-unattended-job.md`.

## `codeants_2pf_hcr.freeze_run`

- `freeze_single_fish_run`: Copy and SHA-256 verify the complete working run plus external manifests; capture scientific commit/runtime and utility checkout provenance; protect the verified snapshot with permissions or a mounted UDRO image.
- `build_inventory`: Deterministic content inventory of labeled trees, including symlink targets without following them.
- `verify_read_only_mount`: Verify macOS read-only mount status and rejection of a real file creation with EROFS.
- CLI: `tools/freeze_single_fish_run.py`; usage and protection limits: `docs/freeze-single-fish-run.md`.

## `codeants_2pf_hcr.qc_figure_export`

- `export_qc_figure_gallery`: Extract seven executed QC notebooks' PNGs (six only with explicit `review_gate`) and selected/current writer PNGs into a fish-named full-resolution gallery/contact sheets with exact provenance hashes. `manifest_paths` narrows coverage to the current fresh recipe rather than unrelated historical manifests.
- CLI: `tools/export_qc_figures.py`; source coverage and output-directory rules: `docs/qc-figure-gallery.md`.

## `codeants_2pf_hcr.fresh_run_spec`

- `build_fresh_run_spec`: Build a clean-pinned common native writer recipe using shared fish paths, explicit eligible planes, fresh best-Z/scale search by default (optional pinned NCC initializer copies), an optional recorded control-supersession reason passed to match-roi-to-anatomy, score-activity-bpi, assign-hcr-identity and export-canonical-tables, provisional aligned HCR inputs and a deferred midline review gate; never imports another fish's frozen products or review decisions.
- CLI: `tools/build_fresh_single_fish_spec.py`; policy: `docs/single-fish-unattended-job.md`.

## `codeants_2pf_hcr.fresh_batch`

- `prepare_fresh_batch`: Construct isolated writable L765_f04 control inputs with read-only source links, pin scientific CSV/matrix baselines, and create four shared native recipes.
- `run_fresh_batch`: Run the isolated control first, then remaining recipes sequentially only after its finishing/hash/artifact/no-op checks pass. Stop at any failure; save compact terminal batch/job reports without polling or automatic repair.
- CLI: `tools/fresh_single_fish_batch.py` (`prepare`, `run`).

## Manual ex-vivo HCR identity review

- `manual_identity.Dataset`, `AnnotationSession`, `prepare_dataset`: Validate hash-bound, physically aligned registered inputs and authoritative eligible masks; persist independent per-marker calls with a recoverable journal and frozen snapshots.
- `manual_identity.export_manual_identity`, `export_manual_canonical_tables`: Map approved ex-vivo positives through frozen reviewed geometry to existing in-vivo identity/ROI schemas; preserve ROI inventory and response fields; emit explicit ROI-only canonical bundles without fabricated HCR labels.
- `apps.manual_identity.create_viewer`: Optional napari/Qt fixed-mask reviewer with independent marker selection and orthogonal context.
- `run_single_fish_export_canonical_tables_stage`: Optional `manual_identity_root` selects ROI-only manual-source export; incompatible HCR source mixing is blocked.
- CLI: `tools/manual_hcr_identity.py` (`prepare`, `view`, `export`, `canonical`, `recover-lock`); setup/contracts: `docs/manual-hcr-identity-gui.md`.
# Native execution preflight

- `run_preflight.preflight_native_run`: read-only staged dependency and selected input checks.
- `run_preflight.check_geometry_trace_identity`: source-aware geometry/trace key validation.
- `run_preflight.materialize_retained_graph`: verified graph copies for declared continuations.

## Per-stimulus response scoring (2026-10-08)

- `activity.ActivityConfig`: promotes bootstrap count to ≥20 expected draws per stimulus tail (16,000 at 0.99).
- `activity.build_response_bpi_tables`: per-stimulus two-sided calls, any-side condition classes, activation-only side BPI, suppression confidence; signed pooled comparisons retained.
- `activity._compute_bootstrap_null_quantiles`: pooled and LB/RB/LC/RC event sets; optional config supplies valid-trial extraction policy; 128-ROI chunks share float32 cumsum/AUC caches.
- `plots.analysis.render_single_fish_50l_bpi_panel`: counts suppression-only exclusions and permits an all-suppressed population without invented BPI coordinates.
- `activity.NEW_RESPONSE_COLUMNS`: exact annotation/export column set; `POOLED_COMPARISON_COLUMNS` lists comparison-only sources.
- `response_trace_inspector.render_response_trace_inspector`: per-stimulus reproduction guard, suppression/mixed labels, equal-height views and top-neuron scroll sync.

## Gallery curation (2026-10-13)

- `qc_figure_export.select_curated_cards`: ordered stable source-role selection with complete exclusion/missing maps.
- `qc_figure_export.export_qc_figure_gallery`: curated publication with full inventory, provenance, optional response-trace inspector and explicit presentation artifact overrides.
- `plots.gallery.render_gallery_composite`: native PNG stacking for offline previews.
- `plots.gallery.render_registration_review_composite`, `render_geometry_offsets_composite`, `render_midline_review_composite`, `render_identity_coverage_composite`: notebook-owned original-resolution composites.
- `plots.gallery.render_hcr_pair_status_planes`: every persisted functional anatomy plane with saved accepted/rejected HCR outlines.
- `plots.gallery.hcr_status_ring_counts`, `draw_hcr_status_rings`: legacy aligned scope/status rings using only supplied HCR status rows.
- `plots.qc_identity_functional.render_marker_traces_with_hcr_status`: unchanged Q7 trace rows/curves plus HCR-label rings.
- `plots.qc_molecular.plot_activity_bpi_gate`: adds per-stimulus signed null/floor margins and selected tuning side; pooled panel comparison-only.
- `plots.qc_molecular.plot_molecular_identity_qc`: labels small counts and adds inset.
- `plots.analysis.render_single_fish_50l_gene_auc_panel`: fixed two-row median annotation band.
- `plots.analysis.render_single_fish_50l_composite`: shared global/population AUC y-limits.

- Review round 1: `plots.gallery` registration composite aligns whole row sets side by side; midline/coverage composites enlarge narrower parts only. HCR plane outlines use a two-column grid. `plot_activity_bpi_gate` consumes saved stimulus valence for colours and activation-only side counts; no binned curve. `render_marker_traces_with_hcr_status` exposes both colour legends.

## Signed AUC gallery surfaces (2026-10-13)

- `suite2p.build_suite2p_stimulus_locked_diagnostic`: display traces/row metadata only; retired four 50 s scalar summaries.
- `plots.qc_functional.load_presented_stimulus_auc_qc`: read saved [23c] signed AUC/calls and fixed any-stimulus membership.
- `plots.qc_functional.render_presented_stimulus_auc_violins`: two population rows × session columns; saved AUC/call counts. Replaces retired `render_stimulus_type_mean_dff` / `_frame` APIs.
- `plots.qc_identity_functional.load_identity_functional_qc_bundle`: requires explicit `presented_response_path` and cross-checks ROI/session keys.
- `plots.qc_identity_functional.build_population_response_matrix`: signed [23c] AUC input, same normalization/clustering/annotations.
- `qc_figure_export._provenance_rows`: seven compact gates, inline key values from saved notebook HTML tables/manifests, source links and full evidence retained.
