# Notebook Stage Map

**Purpose:** navigation map for `notebooks/2PF_to_HCR.ipynb`.

**Use this file when:** locating stage ownership, key cell tags, or expected stage outputs.

## End-to-end stages

1. **Setup and fish-scoped paths** (`[1]-[5]`)
   - Imports, fish context, run configuration.
   - Key vars: `FISH_ID`, `RUN_CONFIG`, `NAS_ROOT`, `FISH_DIR`, `OUTDIR`, `OUT_REG`, `OUT_QA`, `OUT_DERIVED`.
2. **Spatial preparation** (`[7] [9] [11] [13] [15] [19] [19a] [21] [22d] [22e]`)
   - Orientation, voxel alignment, best-z/scale search, in-plane placement comparison, regional crop selection, QA overlays.
   - `[19a]` writes NCC-guided per-plane fixed anatomy-space squares for masked ANTs in-plane registration; `[20]` uses masked ANTs as the default in-plane backend and explicitly falls back to NCC when ANTs or its region JSON is unavailable.
   - `[22e]` runs after Suite2p loading so the same regional review can include ROI and anatomy-label boundaries.
   - Key object: `plane_refs`.
3. **Functional ROI extraction** (`[23a] [25]`)
   - Suite2p load, ROI labels, `iscell` provenance, dF/F extraction.
   - Key object: `suite2p_by_ref_idx`.
4. **Segmentation/geometry QA** (`[29] [33] [34a]`)
   - QC helpers only; not identity source.
   - Native segmentation stages `[24]` and `[24a]` are package-owned and should remain orchestration-thin in the notebook.
5. **HCR discovery/warp/HCR↔anatomy QC** (`[37] [39] [41] [42] [43] [44]`)
   - HCR mask discovery, warping, QC summaries.
   - Key object: `hcr_match_results`.
   - External-BigWarp prep stages `[43]` and `[43b]` are package-owned wrappers with stage-local ANTs imports.
6. **HCR-centric identified-cell activity mapping** (`[50]`)
   - Builds `hcr_activity_status.csv`, `conf_to_func_pairs_raw.csv`, response-positive `conf_to_func_pairs.csv`.
   - Owns identified-cell activity export semantics.
7. **ROI-centric whole-population matching** (`[50h] [50i]`)
   - Authoritative ROI↔anatomy matching and identity attachment.
   - Owns geometry matching semantics.
8. **Activity/BPI annotation** (`[50ia]`)
   - Response/BPI annotations merged onto master ROI table.
   - Owns response semantics after geometry is fixed.
9. **Population figures** (`[50e] [50j] [50k]`)
   - Table-driven summaries.
   - Must filter canonical tables; they do not own semantic definitions.
10. **Trace export and stimulus-aligned analyses** (`[50l] [51] [55] [56] [56h] [56g] [57a-responsive-identity-donut] [57b-anatomy-coexpression-summary] [57]`)
    - Trace export, stimulus alignment, full-session and diagnostics figures.

## Core outputs by stage

- ROI-centric authoritative table: `functional_roi_activity_identity.csv` (`[50i]` + `[50ia]`).
- Response/BPI exports: `functional_roi_activity_bpi_cells.csv`, `functional_roi_activity_bpi_summary.csv` (`[50ia]`).
- HCR-centric identified-cell outputs: `hcr_activity_status.csv`, `conf_to_func_pairs.csv`, `hcr_func_candidates.csv` (`[50]`).
- Single-fish donut summary output: `hcr_activity_status_summary.csv` (`[50e]`), including per-gene inner/outer status counts and unmatched rows.
- Single-fish `[50l]` composite output: `compound_50j_56i_unified.png/.pdf`, package-rendered by `plots.analysis.render_single_fish_50l_composite` from `[50ia]` response/BPI outputs plus `[56i]` motion-AUC point/count tables.
- Single-fish responsive hybrid donut outputs: `single_fish_50l_responsive_identity_donut.png/.pdf`, plus long/wide counts CSVs (`[57a-responsive-identity-donut]`).
- Single-fish anatomy-label co-expression outputs: `single_fish_hcr_anatomy_coexpression_summary.png/.pdf` plus summary/combo-count CSVs (`[57b-anatomy-coexpression-summary]`).

## Concept ownership

- Whole-population identity -> `functional_roi_activity_identity.csv`.
- Identified-cell activity export -> HCR-centric outputs from `[50]`.
- Response semantics -> `[50ia]` / activity stage.
- Geometry matching -> matching stage only.
- Figure semantics -> downstream table filtering only; figures do not infer identity or response state.

## Navigation notes

- For module ownership and functions: see `symbol-index.md`.
- For table meaning and allowed usage: see `canonical-tables.md`.
- For migration caveats in downstream cells: see `current-state.md`.
