# Cache and Rerun Policy

**Purpose:** prevent stale outputs after matching/identity/activity logic changes.

**Use this file when:** changing matching logic, identity joins, `[50]`, `[50i]`, `[50ia]`, or downstream figure/export logic.

## Treat as stale when matching logic changes

- `hcr_activity_status.csv`
- `conf_to_func_pairs_raw.csv`
- `conf_to_func_pairs.csv`
- `hcr_func_candidates.csv`
- `functional_roi_activity_identity.csv`
- `functional_roi_activity_identity_summary.csv`
- `functional_roi_activity_identity_by_plane.csv`
- `functional_roi_activity_bpi_cells.csv`
- `functional_roi_activity_bpi_summary.csv`
- `suite2p_traces/suite2p_dff_traces_meta.csv`
- downstream figure exports derived from these tables

## Minimum rerun sequence

1. Rerun spatial prep / ROI loading stages if upstream references changed.
2. Rerun HCR warp + HCR↔anatomy matching if confocal geometry changed.
3. Rerun `[50h]`, `[50i]`, `[50ia]`.
4. Rerun `[50]` after `[50ia]` for HCR-centric activity exports.
5. Rerun downstream figure and trace-analysis stages that consume regenerated outputs.

## Safety expectations

- Native promotion of an accepted anatomy-midline sidecar verifies each original reviewed source hash against its frozen source. A changed operative JSON hash is permitted only when its text equals the exact native path-prefix relocation of that source; the original reviewed path/hash remain in `relocation_provenance`. Proposal hashes, line coordinates, and acceptance remain unchanged.
- The same accepted-midline transaction publishes a native proposal manifest after verifying unchanged reviewed proposal bytes. Its inputs/outputs point to current native files, its original frozen manifest hash remains explicit, and rollback restores both proposal and materialization manifests.
- With the native-layout manifest present, scoring comparisons use `derived/score-activity-bpi/registration/`, where the identity-neutral scorer owns its products. A missing score-owned control remains a missing-control warning; downstream identity-enriched canonical masters are never score controls. Historical staged comparison paths are unchanged.

- Fail fast if required response columns are missing in downstream HCR-centric consumers.
- Do not mix old cached outputs with newly regenerated authoritative tables.
- Distrust cached outputs whenever upstream writer-stage semantics changed, even if filenames and schemas stayed stable.
- Single-fish `[50l]` now auto-invalidates cached `[56i]` motion AUC tables when `functional_roi_activity_identity.csv`, `hcr_activity_status.csv`, or `midline_params_func_ref.json` is newer than `motion_auc_plot_points.csv` or `motion_auc_plot_counts.csv`.
- Staged `make-figures` fails before rendering when its `make-motion-auc-tables` points or counts predate the current canonical ROI/HCR tables, scorer windows, or accepted anatomy midline. Rerun `make-motion-auc-tables` after those inputs change, then render figures.

## Smoke-tier policy (efficient failure localization)

Use staged smoke checks before full reruns:

1. **Tier A**: verify ROI-centric base table exists and has geometry/identity/provenance columns (`functional_roi_activity_identity.csv` from `[50i]`).
2. **Tier B**: verify response/BPI annotations are present (`[50ia]` response columns + `functional_roi_activity_bpi_cells.csv`).
3. **Tier C**: verify HCR-centric response-aware exports (`hcr_activity_status.csv`, `conf_to_func_pairs.csv`, `hcr_func_candidates.csv`) and fish-id consistency.
4. **Tier D**: verify full stale-output regeneration surface exists after an end-to-end refresh.

CLI entrypoint:

- `python tools/notebook_smoke.py --fish-id <FISH_ID> --tier A|B|C|D`

Bugfix loop:

1. Reproduce on the lowest failing tier.
2. Fix the nearest upstream producer stage (avoid downstream plotting-only workarounds).
3. Re-run the same tier, then the next tier.
4. Mark fixed only after adding/adjusting a regression test for the broken contract.

## Default validation workflow after edits

- Run the smallest relevant smoke or contract test first.
- Validate the first downstream consumer of the edited writer stage.
- If a canonical table changed, verify required columns and filename stability explicitly.
- Do not declare success from static reasoning alone.

## Manual identity viewer display cache

`cellMatcher` caches the scaled XZ/YZ image sections and cell outlines for the current XY cut. Z scrolling paints fresh position lines onto copies of those images; the image crosshair is rebuilt only when the XY cut changes. Changing marker stack, selected/pending/recorded cells, positive visibility, image contrast/gamma/opacity/visibility/data, mapped-region visibility, or side-panel width invalidates the relevant image cache. No scientific outputs or on-disk caches change.

Use `tools/benchmark_cellmatcher_scroll.py --manifest <dataset_manifest.json> --baseline <pre-change-commit>` in the manualIdentity GUI environment with `PYTHONPATH=src` to compare the isolated side-view callback and real Qt Z-scroll path. It uses temporary annotation sessions, alternates trial order, includes the existing 30 ms debounce in full-scroll timing, and keeps dataset loading/window initialization outside timing. Optional `--screenshots /tmp/cellmatcher-scroll-visuals` saves untimed rendered evidence. Keep performance work isolated until measured end-to-end timing demonstrates an improvement.


Manual identity functional-region volumes are cached separately under the fish’s
`manual_identity/functional_review_scopes/` directory. The metadata binds region
schema, anatomy/label/reference and forward/inverse-transform hashes, plus the
volume hash, ZYX axes and ex-vivo grid. Old metadata or missing/corrupted volume
files triggers recomputation on prepare/open; no canonical tables need rerunning
for a display-only region overlay.
