# Stage Map

| Notebook tag | Owning module | Public function(s) | Canonical outputs/files |
| --- | --- | --- | --- |
| `[4]` | `codeants_2pf_hcr.context` | `resolve_fish_context`, `notebook_bindings_from_context` | `FISH_DIR`, `OUTDIR`, `OUT_REG`, `OUT_QA`, `OUT_DERIVED` |
| `[4a]` | `codeants_2pf_hcr.context` | `reset_fish_state`, `require_fish_state` | `STATE_FISH_ID` |
| `[4b]` | `codeants_2pf_hcr.context` | `normalize_run_config` | `RUN_CONFIG` and mirrored knob vars |
| `[4c]` | `codeants_2pf_hcr.context` | `build_fish_state_audit_df` | fish audit dataframe |
| `[6]` | `codeants_2pf_hcr.spatial` | `imread_any`, `zproject_mean`, `norm01`, `local_unsharp`, `corrcoef_img`, `top_correlated_mean`, `best_z_by_ncc`, `apply_func_orientation` | in-memory spatial helpers |
| `[55]` | `codeants_2pf_hcr.stimulus` | `StimulusConfig`, `resolve_stimulus_context`, `build_stim_tables` | `df_evt`, `df_stim`, stimulus source table |
| `[50i]` | `notebook-local (pending extraction)` | `build_functional_roi_master_df` currently notebook-owned | `functional_roi_activity_identity.csv` |
| `[50ia]` | `notebook-local (pending extraction)` | response/BPI scoring currently notebook-owned | `functional_roi_activity_bpi_cells.csv` |
| `[50]` | `notebook-local (pending extraction)` | HCR-centric activity export currently notebook-owned | `hcr_activity_status.csv`, `conf_to_func_pairs.csv` |
| `tool build_best_plane_modality_merge_grid.py` | `codeants_2pf_hcr.plots.qa` | `build_best_plane_modality_merge_grid` | `<fish>_best_plane_modality_merge_grid.png` |
| `tool build_round_channel_mip_grid.py` | `codeants_2pf_hcr.plots.qa` | `build_round_channel_mip_grid` | `<fish>_round_channel_2p_mip_grid.png` |
| `tool plot_single_roi_57style.py` | `codeants_2pf_hcr.plots.analysis` | `plot_single_roi_57style` | single-ROI `[57]` figure and optional trial CSV |
