# Notebook Stage Map

**Purpose:** navigation map for `notebooks/2PF_to_HCR.ipynb`.

**Use this file when:** locating stage ownership, key cell tags, or expected stage outputs.

## End-to-end stages

1. **Setup and fish-scoped paths** (`[1]-[5]`)
   - Imports, fish context, run configuration.
   - Key vars: `FISH_ID`, `RUN_CONFIG`, `NAS_ROOT`, `FISH_DIR`, `OUTDIR`, `OUT_REG`, `OUT_QA`, `OUT_DERIVED`.
2. **Early functional response readout** (`[23a] [23c]`)
   - Suite2p trace/ROI load and full-experiment fish response quality diagnostics before anatomy or functional preprocessing.
   - `[23a]` can run without `plane_refs`; when functional references are not available yet, Suite2p planes are keyed by discovered Suite2p plane index.
   - `[23c]` places stimulus blocks on equal Suite2p frame-count boundaries while preserving experiment-log stimulus offsets within each block.
   - `[23c]` computes pre-identity Suite2p response/BPI calls for trace-panel filtering; these outputs are non-canonical and later merge into `[50ia]`.
   - Key object: `suite2p_by_ref_idx`.
3. **Spatial preparation** (`[13] [14] [14a] [7] [9] [11] [15] [19] [19a] [21] [22d] [24a] [22e]`)
   - Orientation, voxel alignment, best-z/scale search, in-plane placement comparison, regional crop selection, QA overlays.
   - `[8]` resolves anatomy Z from fish metadata `step_size_um_anatomy`; TIFF page-count Z is not authoritative for the anatomy stack.
   - `[10]` resolves functional orientation and audits legacy full-stack `_flipX.tif` caches; it no longer saves full oriented functional movies by default.
   - `[12]` builds/reuses oriented 2D functional references from original motion-corrected stacks while preserving legacy reference filenames and `plane_refs` labels.
   - `[14]`/`[14a]` run before voxel inference `[8]`; `[14a]` saves a signed-16-bit-corrected, functional-orientation-matched 8-bit anatomy TIFF with Y/X resized to `750x750` in `02_reg/00_preprocessing/2p_anatomy`, rebinds `ANAT_STACK_PATH` for downstream anatomy consumers, and is rerun-idempotent when the uint8 output already exists.
   - `[19a]` writes NCC-guided per-plane fixed anatomy-space squares for masked ANTs in-plane registration; `[20]` uses masked ANTs as the default in-plane backend and explicitly falls back to NCC when ANTs or its region JSON is unavailable.
   - `[24a]` runs before `[22e]` so the regional review can include anatomy-label boundaries; `[22e]` also runs after Suite2p loading so the same review can include ROI boundaries.
   - Key object: `plane_refs`.
4. **Functional ROI geometry QA** (`[23b] [25] [29] [33] [34a]`)
   - Suite2p orientation QC, functional labels on references, and geometry QC helpers.
   - QC helpers only; not identity source.
   - Diameter and regional-review support cells `[30]` and `[34c]` are package-owned migration wrappers.
   - Native segmentation stages `[24]` and `[24a]` are package-owned and should remain orchestration-thin in the notebook; `[24a]` is ordered earlier because `[22e]` consumes `ANAT_LABELS_PATH`.
5. **HCR discovery/warp/HCR↔anatomy QC** (`[37] [39] [41] [42] [43] [44]`)
   - HCR mask discovery, warping, QC summaries.
   - Key object: `hcr_match_results`.
   - HCR discovery/config/metadata/matching support cells `[38]`, `[40]`, `[41]`, `[44]`, `[46]`, `[47]`, and `[47b]` are now package-owned migration wrappers; notebook cells should stay def-free.
   - External-BigWarp prep stages `[43]` and `[43b]` are package-owned wrappers with stage-local ANTs imports.
6. **HCR-centric identified-cell activity mapping** (`[50]`)
   - Builds `hcr_activity_status.csv`, `conf_to_func_pairs_raw.csv`, response-positive `conf_to_func_pairs.csv`.
   - Owns identified-cell activity export semantics.
7. **ROI-centric whole-population matching** (`[50h] [50i]`)
   - Authoritative ROI↔anatomy matching and identity attachment.
   - Owns geometry matching semantics.
8. **Activity/BPI annotation** (`[50ia]`)
   - Response/BPI annotations merged onto master ROI table, reusing pre-identity `[23c]` response calls when available.
   - Owns response semantics after geometry is fixed.
9. **Population figures** (`[50e] [50j] [50k]`)
   - Table-driven summaries.
   - Must filter canonical tables; they do not own semantic definitions.
10. **Trace export and stimulus-aligned analyses** (`[50l] [51] [55] [56] [56h] [56g] [57a-responsive-identity-donut] [57b-anatomy-coexpression-summary] [57]`)
    - Trace export, stimulus alignment, full-session and diagnostics figures.
    - Late trace/figure migration wrappers include `[51]`, `[54]`, and `[56d]`; their notebook cells should contain only the public package call and display/binding code.

## Core outputs by stage

- ROI-centric authoritative table: `functional_roi_activity_identity.csv` (`[50i]` + `[50ia]`).
- Response/BPI exports: `functional_roi_activity_bpi_cells.csv`, `functional_roi_activity_bpi_summary.csv` (`[50ia]`).
- Pre-identity response/BPI diagnostics: `suite2p_response_bpi_cells_23c.csv`, `suite2p_response_bpi_summary_23c.csv` (`[23c]`, non-canonical).
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
