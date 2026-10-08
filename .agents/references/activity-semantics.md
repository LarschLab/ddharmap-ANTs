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

### Stage 1: per-stimulus signed response tests (approved 2026-10-08)

LB, RB, LC and RC `used_for_bpi` trajectories are tested separately. Each mean
uses valid trials (minimum `min_trials_per_class`, default 3); its per-cell null
uses the same number of valid duration-matched trial draws. Total family alpha
`1-response_null_q` is split over four stimuli and two tails: at 0.99 the applied
lower/upper quantiles are **0.00125 / 0.99875**. The persisted
`response_null_lower_quantile` / `response_null_upper_quantile` are exclusively
these **per-stimulus family-wise** quantiles and describe `s_null_lower_auc` /
`s_null_upper_auc` for `s` in LB/RB/LC/RC. They do not describe the pooled
condition thresholds. Only exact `LB_trajectory`, `RB_trajectory`,
`LC_trajectory`, `RC_trajectory` names enter these per-stimulus pools; legacy
LLB/RLB/LLC/RLC aliases and all non-BPI types are excluded.
Activation requires mean AUC
≥ max(0.05, upper); suppression requires mean AUC ≤ min(-0.05, lower).
The 12.5 s response/prestimulus windows and dF/F definition are unchanged.
`ActivityConfig` promotes old smaller bootstrap settings to at least 20 expected
samples per tail (16,000 at 0.99). ROI chunks of 128 share float32 cumsum/AUC caches.

For each `s` in `lb`, `rb`, `lc`, `rc`, persist `s_mean_auc_dff`,
`s_null_upper_auc`, `s_null_lower_auc`, `s_n_trials`, `s_response_valence`
(`activated`, `suppressed`, `not-significant`, `unavailable`),
`s_n_consistent_suppressed_trials`, and `s_n_prestim_elevated_trials`.

`response_is_active` means any stimulus passes either tail. Condition passes
mean any side passes. Existing `response_class` and `bpi_category` vocabulary
remains bout-/continuous-/both-responsive, low activity, response unavailable.
A non-passing cell is low activity only if all four stimulus tests are
assessable; otherwise unavailable. A passing cell may have incomplete evidence.
`response_valence_complete` means all four tests are assessable.
Condition valence gives activation precedence, then suppression, then
not-significant (unavailable if non-passing incomplete evidence).
`bout_mixed_side_valence` / `cont_mixed_side_valence` flag simultaneous activation
and suppression on opposite sides. Positive/negative pass fields both remain true
in that case. `response_has_suppression` retains any suppressed stimulus.

`response_valence_class` values: bout-activated, continuous-activated,
both-activated, bout-suppressed, continuous-suppressed, both-suppressed,
bout-activated/continuous-suppressed, continuous-activated/bout-suppressed,
low activity, response unavailable. These use condition-valence precedence;
read mixed-side flags as well for same-condition mixed evidence.

### Stage 2: activation-only side tuning

`bpi_left = (LB⁺-LC⁺)/(LB⁺+LC⁺)`; `bpi_right` uses RB/RC, with
X⁺ = max(0, mean signed trial AUC). A side BPI is defined only with finite paired
means and denominator > `denom_eps`. The side containing the strongest
significantly activated stimulus supplies authoritative `bpi`, `denom`, and
`bpi_side` (left/right, otherwise NaN). Ties follow LB/RB/LC/RC order, choosing
left for equal opposite-side activation strengths. Missing paired means leave
that selected BPI unavailable; do not switch sides to fill missing evidence.

Only cells with significant activation receive bout-preferential (BPI > 0.5),
continuous-preferential (BPI < -0.5), or non-preferential tuning. Suppression-only
cells emit **suppressed-only**, with `bpi` NaN and `bpi_data_available=False`.
Nonresponders emit not-responsive; incomplete nonresponders emit unavailable.
`bpi_z` remains a legacy pooled z-response diagnostic, not activation tuning.

Signed `mean_bout_auc_dff` / `mean_cont_auc_dff`, response magnitudes and
`signed_condition_contrast = (bout_auc-cont_auc)/(abs(bout_auc)+abs(cont_auc))`
remain unchanged. They are signed pooled condition summaries, not the new gates.
`bout_null_lower_auc` / `cont_null_lower_auc` and `bout_null_upper_auc` /
`cont_null_upper_auc` retain the **pooled comparison** thresholds at 0.005 /
0.995 (default total confidence 0.99), respectively. Pair these condition
thresholds only with `pooled_response_null_lower_quantile` /
`pooled_response_null_upper_quantile`, never the unprefixed per-stimulus
quantile metadata. `bout_null_q99_auc` / `cont_null_q99_auc` are older pooled
0.99 upper-tail comparison quantiles, not either classification gate.
The `s_null_*` columns alone own the per-stimulus decisions. The old AUCs, nulls,
positive/negative/condition passes, valence, response class, rectified BPI,
preference, availability, category, denominator and applied quantiles are copied
to `pooled_` names (exact list in the scientific artifact contract). All
`pooled_` fields are **comparison-only**, including legacy quantiles 0.005/0.995.

### Suppression confidence: annotation only, never a gate

`quiet_level_dff` is the median over the union of all presented-trial prestimulus
pause frames on that plane (including non-BPI trials). B0 and poststimulus decay
are excluded. `quiet_spread_dff` = 1.4826 × median absolute deviation.
For each stimulus, a consistent suppression trial has signed trial AUC < 0
and response-window mean dF/F < quiet. An elevated prestimulus trial has its
12.5 s prestimulus mean > quiet + 2 × spread. `suppression_confidence` is high
if every significantly suppressed stimulus has ≥ 3 consistent trials (the usual
four repetitions); low otherwise; NaN without suppression. The elevated-trial
count is contextual and never vetoes a call or confidence label. The inspector
renders high as “highly probable suppression”. Danin will review this definition
with Johannes; it is not a significance test.

## Threshold ownership

- `[50ia]` owns the exported BPI/activity thresholds used to interpret downstream plots.
- `[23c]` may compute pre-identity Suite2p response/BPI calls with the same threshold knobs so early diagnostics can filter trace panels before HCR geometry exists; those outputs are diagnostic until merged by `[50ia]`. Imaging sessions are scored separately; L765_f04 planes 0–4 belong to r1 and 5–9 to r2.
- `[23c]` also writes a separate, pre-identity response screen for every presented stimulus. Each type uses its own delayed motion-window mean AUC and prestimulus bootstrap null. If `m` stimulus types are presented, total alpha is split across `2m` stimulus-tail tests: at the default 0.99 family confidence and ten types, the applied lower and upper null quantiles are 0.0005 and 0.9995. Positive and negative passes are persisted separately, and either pass qualifies the fixed `responsive to any presented stimulus` cohort. A cell is `nonresponsive` only when every presented type is assessable and neither tail passes; otherwise a non-passing cell is `unavailable`. `null_auc_threshold` and `null_quantile_familywise` remain upper-tail compatibility aliases; explicit lower/upper fields own interpretation. These diagnostic fields do not redefine `[50ia]` response state or expand BPI eligibility.
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
