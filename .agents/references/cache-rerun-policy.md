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

- Fail fast if required response columns are missing in downstream HCR-centric consumers.
- Do not mix old cached outputs with newly regenerated authoritative tables.

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
