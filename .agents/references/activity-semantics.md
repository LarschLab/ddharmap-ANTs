# Activity and BPI Semantics

**Purpose:** define response-state and BPI meaning without altering geometry policy.

**Use this file when:** editing `[50ia]`, activity labels, BPI classes, or activity-based figures.

## Separation of concerns

- Geometry matching is resolved before activity semantics.
- Activity/BPI annotate already matched ROI rows.
- Activity/BPI must not change geometry or identity assignment.

## `suite2p_is_cell` interpretation (current conservative policy)

- `suite2p_is_cell=True`: high-quality trace provenance (`Active neurons` in provenance columns).
- `suite2p_is_cell=False`: low-quality trace provenance (`Low-quality traces`), retained in master ROI table.
- In `[50ia]`, `suite2p_is_cell=False` rows are currently marked `response unavailable`.
- This is a conservative trace-quality gate: these rows are treated as having no extractable cellular response information for the current analysis, not as a biological inactivity or absence statement.

## Two-stage classification in `[50ia]`

### Stage 1: response-state classification

- **Responsive**: passes AUC null-threshold test in bout and/or continuous condition.
- **Low activity**: extractable/high-quality trace with sufficient trials but fails both response thresholds.
- **Response unavailable**: insufficient trials or low-quality trace policy gate, including the current `suite2p_is_cell=False` policy.

Primary fields:

- `response_is_active`
- `response_class`
- `response_summary_class`
- `bout_response_pass`, `cont_response_pass`
- trial-count and null-threshold fields

### Stage 2: BPI categorization

BPI formula: `(bout_auc - cont_auc) / (bout_auc + cont_auc)`.

- Both conditions pass:
  - `|BPI| > 0.50` -> bout-responsive or continuous-responsive
  - `|BPI| <= 0.50` -> both-responsive
- Only one condition passes:
  - `|BPI| <= 0.50` -> weak-response
  - `|BPI| > 0.50` -> directional class (bout/continuous)

Primary fields:

- `bpi`
- `bpi_z`
- `activity_mag`
- `bpi_data_available`
- `bpi_category`
- `bpi_zero_band`
- `bpi_activity_threshold`

## Threshold ownership

- `[50ia]` owns the exported BPI/activity thresholds used to interpret downstream plots.
- `[23c]` may compute pre-identity Suite2p response/BPI calls with the same threshold knobs so early diagnostics can filter trace panels before HCR geometry exists; those outputs are diagnostic until merged by `[50ia]`.
- `functional_roi_activity_bpi_cells.csv` is a response/BPI diagnostic helper table. It may carry an `anat_label` column for schema compatibility, but identity should remain blank there; use the ROI master table for authoritative anatomy identity.
- Downstream figures should reuse `bpi_zero_band` for near-zero BPI guide lines and `bpi_activity_threshold` for low-activity guide lines when those columns are present.
- Do not hard-code duplicate threshold constants in downstream cohort or figure stages when the authoritative `[50ia]` columns are available.

## Color scheme (synchronized)

- bout-responsive: `#2c7fb8`
- continuous-responsive: `#d95f0e`
- both-responsive: `#d946ef`
- weak-response: `#000000`
- low-activity: `#9e9e9e`
- response-unavailable: `#ececec`

This palette is expected to stay synchronized across `[50ia]`, `[50j]`, and `[50l]` outputs.

## Ambiguity ladder

- ROI-centric vs HCR-centric scope: defer to `scientific-policy.md`, then `current-state.md`.
- Activity wording and response-state meaning: this file is authoritative.
- Output ownership and canonical table choice: defer to `canonical-tables.md`.
- If code conflicts with policy docs, follow policy docs unless they are clearly outdated.
