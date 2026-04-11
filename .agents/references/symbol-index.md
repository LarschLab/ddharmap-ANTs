# Symbol Index

Generated manually for the phase-1 extraction surface.

## Trusted patterns

- `context.py`: path/config normalization pattern.
- `stimulus.py`: explicit stage input/output pattern.
- `activity.py`: response/BPI stage-owned semantics pattern.
- `plots.*`: deterministic figure-builder pattern.
- `tools/`: wrapper pattern only, not business-logic authority.

## `codeants_2pf_hcr.context`

- `resolve_fish_context`: Resolve fish-scoped roots, canonical output paths, and normalized run config.
- `notebook_bindings_from_context`: Rebind package-resolved context back to legacy notebook variable names.
- `normalize_run_config`: Apply defaults and forced recompute flags.
- `reset_fish_state`: Update the fish marker explicitly.
- `require_fish_state`: Check that the current fish matches the cached fish marker.
- `build_fish_state_audit_df`: Build the audit dataframe used by cell `[4c]`.

## `codeants_2pf_hcr.spatial`

- `imread_any`: Read TIFF or NRRD images with minimal notebook dependencies.
- `zproject_mean`: Mean projection helper.
- `norm01`: Robust percentile normalization to `[0, 1]`.
- `local_unsharp`: Local contrast sharpening helper.
- `corrcoef_img`: Pearson correlation for image pairs.
- `top_correlated_mean`: Suite2p-like top-k frame reference builder.
- `best_z_by_ncc`: Best-z search by NCC-like scoring.
- `apply_func_orientation`: Apply the notebook’s functional orientation convention.

## `codeants_2pf_hcr.matching`

- `gene_from_mask`: Infer a gene label from a confocal mask filename.
- `build_anat_identity_lookup_df`: Build the anatomy-label to identity lookup table from HCR matches.
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
- `build_response_bpi_tables`: Build response/BPI annotations and summary tables from the ROI master table.
- `prepare_pairs_for_unique_cells`: Validate and normalize HCR-centric pair tables before trace analyses.
- `load_suite2p_dff_map`: Load Suite2p `F.npy` traces from disk and convert them to dF/F by plane.
- `infer_frame_rate_from_detail`: Resolve a consistent Suite2p frame rate from per-plane ops files.

## `codeants_2pf_hcr.plots.qa`

- `build_best_plane_modality_merge_grid`: Render the merged best-plane modality QA panel.
- `build_round_channel_mip_grid`: Render the round/channel MIP grid.

## `codeants_2pf_hcr.plots.analysis`

- `plot_single_roi_57style`: Render the single-ROI `[57]` style figure and optional AUC table.
