# Current State

**Purpose:** short status of the intentional mixed migration state.

**Use this file when:** editing downstream cells that may combine ROI-centric and HCR-centric paths.

## Current mixed state (intentional)

- ROI-centric path (`[50i]`, `[50ia]`, `[50j]`, `[50k]`) is used for authoritative whole-population outputs.
- HCR-centric identified-cell path (`[50]`, `[50e]`, `[51]`, `[56]`, `[56h]`, `[57]`) remains active for identified-cell activity workflows.
- Cohort notebook path (`notebooks/multi_fish_56h_56g.ipynb`) is a downstream multi-fish consumer of single-fish outputs.

## Cohort authority boundary

- Cohort stages aggregate and visualize outputs from single-fish canonical tables.
- Cohort notebook stages are **not** authoritative for single-fish identity/response semantics.

## Practical warnings

- `[50]` is response-aware via join to the `[50ia]` master ROI table.
- Downstream HCR-centric consumers are expected to require response-aware columns and fail fast if missing.
- Multi-session single-fish functional preprocessing can map later output planes to later imaging sessions; response/BPI and `[56i]` AUC consumers should use per-plane stimulus contexts rather than assuming one latest metadata/log CSV applies to all planes.
- `[50l]` is now package-rendered by `plots.analysis.render_single_fish_50l_composite`; its notebook cell should remain a thin wrapper around canonical `[50ia]` and `[56i]` tables.
- `[57a-responsive-identity-donut]` is a downstream hybrid figure only; it does not redefine upstream geometry/response semantics from `[50i]`/`[50ia]`.
- `[4c]` path audits may report stage-owned path keys as `pending` before their writer stage initializes them; treat `warn` as path drift/mismatch and `fail` as stale or missing canonical state.
- Notebook-visible QA image stages should not rely on an import-time `Agg` backend override; package renderers are expected to display figures explicitly in notebook contexts while remaining save-safe in headless runs.
- Optional native dependencies are expected to fail stage-locally: base package imports and early notebook setup cells should not require `SimpleITK`, `cellpose`, or `ANTsPy` at import time.
- `notebooks/2PF_to_HCR.ipynb` is static-contract clean for top-level helper definitions; remaining heavy legacy behavior for migrated cells is package-owned through public stage wrappers.
- Early anatomy preprocessing now includes `[14a]` before voxel inference `[8]`; it writes a signed-16-bit-corrected, functional-orientation-matched 8-bit anatomy TIFF with Y/X resized to `750x750` in `02_reg/00_preprocessing/2p_anatomy` and rebinds `ANAT_STACK_PATH` downstream.
- Keep path purposes separate:
  - ROI-centric for whole-population inference
  - HCR-centric for identified-cell activity reporting/export
- Avoid implicit hybrids in plotting cells.
