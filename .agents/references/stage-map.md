# Stage Map

| Notebook tag | Owning module | Public function(s) | Canonical outputs/files |
| --- | --- | --- | --- |
| `[4]` | `codeants_2pf_hcr.context` | `resolve_fish_context`, `notebook_bindings_from_context` | `FISH_DIR`, `OUTDIR`, `OUT_REG`, `OUT_QA`, `OUT_DERIVED` |
| `[4a]` | `codeants_2pf_hcr.context` | `reset_fish_state`, `require_fish_state` | `STATE_FISH_ID` |
| `[4b]` | `codeants_2pf_hcr.context` | `normalize_run_config` | `RUN_CONFIG` and mirrored knob vars |
| `[4c]` | `codeants_2pf_hcr.context` | `build_fish_state_audit_df` | fish audit dataframe |
| `[6]` | `codeants_2pf_hcr.spatial` | `imread_any`, `zproject_mean`, `norm01`, `local_unsharp`, `corrcoef_img`, `top_correlated_mean`, `best_z_by_ncc`, `apply_func_orientation` | in-memory spatial helpers |
| `[55]` | `codeants_2pf_hcr.stimulus` | `StimulusConfig`, `resolve_stimulus_context` | `df_evt`, `df_stim`, `DF_STIM_FISH_ID`, stimulus source table |
| `[50i]` | `codeants_2pf_hcr.matching` | `build_functional_roi_master_df`, `build_anat_identity_lookup_df`, `gene_from_mask` | `functional_roi_activity_identity.csv` |
| `[50ia]` | `codeants_2pf_hcr.activity` | `ActivityConfig`, `build_response_bpi_tables` | `functional_roi_activity_identity.csv`, `functional_roi_activity_bpi_cells.csv`, `functional_roi_activity_bpi_summary.csv` |
| `[50]` | `codeants_2pf_hcr.matching` | `build_hcr_activity_tables` | `hcr_activity_status.csv`, `conf_to_func_pairs.csv`, `hcr_func_candidates.csv` |
| `[56]` | `codeants_2pf_hcr.activity`, `codeants_2pf_hcr.stimulus` | `prepare_pairs_for_unique_cells`, `build_prestim_baseline_windows`, `compute_zscore_stats`, `effective_motion_window` | in-memory stimulus-locked gene summaries |
| `[56h]` | `codeants_2pf_hcr.activity`, `codeants_2pf_hcr.stimulus` | `prepare_pairs_for_unique_cells`, `build_prestim_baseline_windows`, `compute_zscore_stats`, `effective_motion_window` | combined `[56f]`/`[56e]` figure inputs |
| `[57]` | `codeants_2pf_hcr.activity`, `codeants_2pf_hcr.stimulus` | `prepare_pairs_for_unique_cells`, `effective_motion_window` | full-session mean-trace figure inputs |
| `tool build_best_plane_modality_merge_grid.py` | `codeants_2pf_hcr.plots.qa` | `build_best_plane_modality_merge_grid` | `<fish>_best_plane_modality_merge_grid.png` |
| `tool build_round_channel_mip_grid.py` | `codeants_2pf_hcr.plots.qa` | `build_round_channel_mip_grid` | `<fish>_round_channel_2p_mip_grid.png` |
| `tool plot_single_roi_57style.py` | `codeants_2pf_hcr.plots.analysis` | `plot_single_roi_57style` | single-ROI `[57]` figure and optional trial CSV |
