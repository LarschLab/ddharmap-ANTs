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

- The current agentic staged CLI surface is post-preprocessing: assume preprocessing has already completed before running `contracts`, `audit-inputs`, `status`, `stage-status`, or `compare-staged`.
- Existing `L395_f11` staged outputs are the first baseline/control for staged parity checks. Treat them as accepted comparison evidence for this migration slice, not as cohort-wide scientific invariants.
- The only explicit write side effect in the current staged audit/status surface is `--write-manifest`, which persists a JSON manifest under `03_analysis/functional/pipeline_manifests/`; it does not write staged analysis outputs.
- Downstream staged output folders are currently read-only inventory/status surfaces. `stage-status` can inspect existing `assign-hcr-identity`, `score-activity-bpi`, `export-canonical-tables`, and `make-figures` outputs, but those stages remain declarative contracts only until writer commands are implemented.
- `compare-staged` compares existing declared post-preprocessing outputs only. It does not execute, recompute, promote, render, or freeze baseline bundles.
- `[50]` is response-aware via join to the `[50ia]` master ROI table.
- Downstream HCR-centric consumers are expected to require response-aware columns and fail fast if missing.
- Multi-session single-fish functional preprocessing can map later output planes to later imaging sessions; response/BPI and `[56i]` AUC consumers should use per-plane stimulus contexts rather than assuming one latest metadata/log CSV applies to all planes. If preprocessing metadata is missing but explicit session logs exist, stimulus-context resolution can infer an equal contiguous split across sessions and records that provenance.
- `[50l]` is now package-rendered by `plots.analysis.render_single_fish_50l_composite`; its notebook cell should remain a thin wrapper around canonical `[50ia]` and `[56i]` tables.
- `[56h]` includes an auto-selected `sst1.2` contra-continuous all-events trace diagnostic for sanity-checking a suspicious mean response; it is diagnostic only and does not change response semantics.
- `[56h]` includes a poster-only one-row average-trace figure with fixed gene colors; it is not an authoritative table or response-definition output.
- `[57a-responsive-identity-donut]` is a downstream hybrid figure only; it does not redefine upstream geometry/response semantics from `[50i]`/`[50ia]`.
- Existing staged output folders and diagnostic audit artifacts are accepted baseline evidence for `L395_f11`; do not treat them as proof that current CLI writer stages exist.
- Writer-stage naming should be concrete, not generic. Treat broad names like `preprocess-anatomy` or `preprocess-hcr` as roadmap groupings only; executable stages should name the exact side effect, such as `prepare-ex-vivo-anatomy-stack`, `segment-ex-vivo-anatomy-cellpose`, or `segment-hcr-cellpose`.
- Ex vivo structural analysis artifacts should live under `03_analysis/structural/ex_vivo/` and remain isolated from canonical in vivo anatomy segmentation outputs.
- Helga NAS jobs should use a headful SSH session when personal NAS credentials are needed. The username is `danin.dharmaperwira@unil.ch`; do not store or extract the password. Use a session-local `net use Y: ... /persistent:no` prompt, run the NAS-dependent job in that same session, then delete the temporary mapping before exit.
- For single-fish CLI commands, `--local-root` must be the directory that directly contains fish folders. On Helga for Danin/Microscopy data, use `Y:\default\D2c\07_Data\Danin\Microscopy`; using `Y:\default\D2c\07_Data` will write to the wrong `L765_f02` sibling path.
- Ex vivo Cellpose segmentation should consume the segmentation-ready manual-oriented ex vivo NRRD when present, not the raw or pre-rotation ex vivo stack. For `L765_f02`, use `02_reg\00_preprocessing\2p_anatomy\ex_vivo\L765_f02_exvivo_anatomy_2P_GCaMP_uint8_manual_oriented.nrrd`.
- `[4c]` path audits may report stage-owned path keys as `pending` before their writer stage initializes them; treat `warn` as path drift/mismatch and `fail` as stale or missing canonical state.
- Notebook-visible QA image stages should not rely on an import-time `Agg` backend override; package renderers are expected to display figures explicitly in notebook contexts while remaining save-safe in headless runs.
- Optional native dependencies are expected to fail stage-locally: base package imports and early notebook setup cells should not require `SimpleITK`, `cellpose`, or `ANTsPy` at import time.
- `notebooks/singleFish.ipynb` is static-contract clean for top-level helper definitions; remaining heavy legacy behavior for migrated cells is package-owned through public stage wrappers.
- Single-fish functional orientation no longer writes full `_flipX.tif` movie stacks by default. `[10]` audits existing legacy oriented movie caches, and `[12]` applies orientation to derived functional references from the original motion-corrected stacks.
- Early anatomy preprocessing now includes `[14a]` before voxel inference `[8]`; it writes a signed-16-bit-corrected 8-bit anatomy NRRD with 2P XY orientation mirrored by default through the same metadata-driven orientation convention used for functional references, anatomy Z flipped to match the bottom-to-top confocal registration convention, and Y/X resized to `750x750` at `02_reg/00_preprocessing/2p_anatomy/<fish_id>_anatomy_2P_GCaMP.nrrd`, stores cache/provenance metadata in `.nrrd.json`, rebinds `ANAT_STACK_PATH` downstream to that NRRD, and avoids duplicate TIFF image outputs.
- Keep path purposes separate:
  - ROI-centric for whole-population inference
  - HCR-centric for identified-cell activity reporting/export
- Avoid implicit hybrids in plotting cells.
