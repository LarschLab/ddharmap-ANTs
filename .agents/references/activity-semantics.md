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

Before either classification stage, every session-specific stimulus context is
mapped onto the complete concatenated Suite2p trace. Acquisition block `B0`
therefore remains present as baseline context, while only `B1` and `B2` event
windows are scored. The shared mapping preserves within-block log offsets and
uses the planned schedule to locate complete acquisition blocks when available.
`scored_stimulus_windows.csv` records the resulting full-trace coordinates and
is the timing authority for response/BPI figures.

The persisted stimulus-window provenance includes the complete presented
vocabulary, with an explicit `used_for_bpi` flag. The BPI calculation itself is
balanced and restricted to `LB_trajectory` plus `RB_trajectory` for the bout
pool and `LC_trajectory` plus `RC_trajectory` for the continuous pool. `LAB`,
`RAB`, `Ll_RB`, `Rl_RB`, `WFCl`, and `WFCo` remain visible in the heatmap and
trace panels but do not enter response/BPI pooling.

The scored response interval is a fixed 12.5 s from movement onset (stimulus
appearance + 10 s) for every condition. Each trial subtracts its own 12.5 s
true prestimulus pause, ending at stimulus appearance. Pseudo-baseline and
pseudo-response windows for the bootstrap are distinct duration-matched draws
from prestimulus pauses in the same imaging session; neither B0 nor poststimulus
decay enters the null. Planned acquisition-grid onsets and logged pause
durations are combined on one trace grid because the mapped raw-log and planned
block offsets can differ. The Q1 timing audit retains both clocks.

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

### Stage 2: BPI preference, separate from response class

BPI formula: `(max(0, bout_auc) - max(0, cont_auc)) / (max(0, bout_auc) + max(0, cont_auc))`,
using session-local, baseline-subtracted mean condition AUCs. The ratio is
unavailable when its denominator is negligible.

- `response_class` and compatibility `bpi_category` encode which response
  tests passed: bout, continuous, both, low activity, or unavailable.
- `bpi_preference` is bout-preferential for BPI > 0.5, continuous-preferential
  for BPI < -0.5, non-preferential otherwise, and not-responsive when neither
  response test passes. Boundaries are strict; `weak-response` is retired
  for newly scored rows.

Primary fields:

- `response_magnitude_auc` (mean nonnegative bout/continuous AUC; dF/F·s)
- `bpi`
- `bpi_z`
- `denom` (sum of nonnegative condition AUCs; dF/F·s)
- `activity_mag`
- `bpi_data_available`
- `bpi_preference`
- `bpi_category`
- `bpi_zero_band`
- `bpi_activity_threshold`

## Threshold ownership

- `[50ia]` owns the exported BPI/activity thresholds used to interpret downstream plots.
- `[23c]` may compute pre-identity Suite2p response/BPI calls with the same threshold knobs so early diagnostics can filter trace panels before HCR geometry exists; those outputs are diagnostic until merged by `[50ia]`. Imaging sessions are scored separately; L765_f04 planes 0–4 belong to r1 and 5–9 to r2.
- `[23c]` also writes a separate, pre-identity response screen for every presented stimulus. Each type uses its own delayed motion-window mean AUC and prestimulus bootstrap null; the null quantile is Bonferroni-adjusted over the presented vocabulary for the fixed `responsive to any presented stimulus` cohort. A cell is `nonresponsive` only when every presented type is assessable and none passes; otherwise a non-passing cell is `unavailable`. These diagnostic fields do not redefine `[50ia]` response state or expand BPI eligibility.
- `[23c]` and `[50ia]` must use the same full-trace block mapper and persisted all-presented window provenance. The `[23c]` full-session heatmap overlays every exact presented interval; BPI eligibility comes only from its `used_for_bpi` flag. A separately reconstructed B1-relative overlay is forbidden.
- `functional_roi_activity_bpi_cells.csv` is a response/BPI diagnostic helper table. It may carry an `anat_label` column for schema compatibility, but identity should remain blank there; use the ROI master table for authoritative anatomy identity.
- Downstream figures should reuse `bpi_zero_band` for unitless BPI guide lines. Do not draw AUC-valued `bpi_activity_threshold` on a z-response axis.
- Do not hard-code duplicate threshold constants in downstream cohort or figure stages when the authoritative `[50ia]` columns are available.

## Color scheme (synchronized)

- bout-responsive: `#2c7fb8`
- continuous-responsive: `#d95f0e`
- both-responsive: `#d946ef`
- weak-response (legacy rows only): `#000000`
- low-activity: `#9e9e9e`
- response-unavailable: `#ececec`

This palette is expected to stay synchronized across `[50ia]`, `[50j]`, and `[50l]` outputs.

## Ambiguity ladder

- ROI-centric vs HCR-centric scope: defer to `scientific-policy.md`, then `current-state.md`.
- Activity wording and response-state meaning: this file is authoritative.
- Output ownership and canonical table choice: defer to `canonical-tables.md`.
- If code conflicts with policy docs, follow policy docs unless they are clearly outdated.
