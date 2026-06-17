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
- Multi-session single-fish functional preprocessing can map later output planes to later imaging sessions; response/BPI and `[56i]` AUC consumers should use per-plane stimulus contexts rather than assuming one latest metadata/log CSV applies to all planes. If preprocessing metadata is missing but explicit session logs exist, stimulus-context resolution can infer an equal contiguous split across sessions and records that provenance.
- `[50l]` is now package-rendered by `plots.analysis.render_single_fish_50l_composite`; its notebook cell should remain a thin wrapper around canonical `[50ia]` and `[56i]` tables.
- `[57a-responsive-identity-donut]` is a downstream hybrid figure only; it does not redefine upstream geometry/response semantics from `[50i]`/`[50ia]`.
- The staged CLI path now stages the ROI identity master/lookup and current HCR-centric `[50]` tables at `assign-hcr-identity`, regenerates the `[50e]` `hcr_activity_status_summary.csv` from staged `hcr_activity_status.csv` plus HCR warp filter metadata, layers `score-activity-bpi` ROI/BPI outputs on that staged identity master when present, promotes staged identity/score outputs into `export-canonical-tables`, and has package-rendered final donut/coexpression figures consume the staged canonical export bundle when it is complete.
- HCR-centric staged identity outputs are currently table-level staged copies of the current `[50]` registration exports except for the regenerated `[50e]` status summary; they are not yet a raw reconstruction from `build_hcr_activity_tables` inputs plus finalized `[50]` export semantics.
- `assign-hcr-identity` now writes a non-promoted HCR disk-recompute audit under `pipeline_outputs/assign-hcr-identity/recompute-audit/`; on `L395_f11` it applies notebook-equivalent functional orientation, anatomy voxel XY scaling, package-owned response-aware export finalization, a transform-replay variant audit, HCR candidate-key diffs, and per-target functional ROI set/geometry diffs. The closest replay currently combines `tforms_by_plane.csv` affine matrices with selected `[20]` best-Z values plus a diagnostic single-plane `dx=-1`, `dy=+1` affine offset probe on plane `4`; it matches legacy candidate and responsive-pair row counts but remains one raw row short and still chooses different functional ROI sets for many target groups, so canonical HCR exports continue to use the staged parity tables.
- Staged final-figure comparisons intentionally ignore responsive-donut counts CSV provenance path columns while still comparing the biological count columns.
- `[4c]` path audits may report stage-owned path keys as `pending` before their writer stage initializes them; treat `warn` as path drift/mismatch and `fail` as stale or missing canonical state.
- Notebook-visible QA image stages should not rely on an import-time `Agg` backend override; package renderers are expected to display figures explicitly in notebook contexts while remaining save-safe in headless runs.
- Optional native dependencies are expected to fail stage-locally: base package imports and early notebook setup cells should not require `SimpleITK`, `cellpose`, or `ANTsPy` at import time.
- `notebooks/singleFish.ipynb` is static-contract clean for top-level helper definitions; remaining heavy legacy behavior for migrated cells is package-owned through public stage wrappers.
- Single-fish functional orientation no longer writes full `_flipX.tif` movie stacks by default. `[10]` audits existing legacy oriented movie caches, and `[12]` applies orientation to derived functional references from the original motion-corrected stacks.
- Early anatomy preprocessing now includes `[14a]` before voxel inference `[8]`; it writes a signed-16-bit-corrected 8-bit anatomy TIFF with anatomy XY preserved by default and Y/X resized to `750x750` in `02_reg/00_preprocessing/2p_anatomy`, rebinds `ANAT_STACK_PATH` downstream, invalidates older functional-flipped uint8 caches, and avoids chained `*_uint8_uint8.tif` outputs on rerun.
- Keep path purposes separate:
  - ROI-centric for whole-population inference
  - HCR-centric for identified-cell activity reporting/export
- Avoid implicit hybrids in plotting cells.
