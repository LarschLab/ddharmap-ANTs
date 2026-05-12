# Figure Rules

**Purpose:** keep figures consistent with authoritative tables and response semantics.

**Use this file when:** adding/refactoring plots or trace-summary panels.

## Data source rules

1. Start from the authoritative ROI-centric table unless the figure is explicitly HCR-centric.
2. Define subset filters first; join trace-level data after subset definition.
3. Do not rebuild identity in plotting cells.
4. Do not let figure-local logic silently switch identity sources.

## Activity semantics in figures

- Use response-aware fields from `[50ia]`: `response_is_active`, `response_class`, `response_summary_class`, threshold pass columns.
- Do not use legacy `activity_class`/`is_active` as sole functional-activity definitions.
- If using Suite2p provenance, label explicitly with `suite2p_is_cell` / `suite2p_activity_class`.
- Trace-focused figures should usually filter `suite2p_is_cell == True` unless explicitly analyzing low-quality traces.
- Do not derive `low activity` from ad hoc quantiles/medians when response calls are available.
- `[23c]` trace panels filter to pre-identity `response_is_active == True` calls when available; `[23c]` full-experiment heatmaps keep all Suite2p-cell traces, including low-activity neurons.
- `[23c]` trace stimulus panels use two columns: left-column stimuli (`L*` and `WFCl`) on the left, right-column stimuli (`R*` and `WFCo`) on the right, with rows keyed by stimulus type after the side prefix.
- `[23c]` full-experiment heatmaps use frame on the X axis and a white-to-black scale where black is high activity, with transparent stimulus spans over the corresponding session/plane row block; `planned_schedule.csv` rest blocks must remain span-free.

## HCR-centric figure exception

For explicitly identified-cell activity figures, starting from `hcr_activity_status.csv` or `conf_to_func_pairs.csv` is valid, but scope must be clearly labeled HCR-centric.

- `[57b-anatomy-coexpression-summary]` is HCR-centric and anatomy-label scoped:
  - source from in-plane accepted `hcr_activity_status.csv` rows represented on functional planes
  - dedupe unit remains accepted `(gene, anat_label)` labels before collapsing
  - possible co-expression means one `anat_label` carries more than one distinct gene in that in-plane subset
  - this stage summarizes anatomy-label evidence only; it does not imply ROI-level convergence or confirmed single-cell co-expression

## Stage-specific hybrid exception

- `[34c]` is an anatomy-space regional QA overlay:
  - source the saved `[22d]` regional crop in anatomy-space coordinates
  - warp functional ROI labels forward into anatomy space with the selected in-plane transform
  - overlay ROI boundaries on the fixed anatomy/anatomy-label slice; do not inverse-warp anatomy labels into functional display space
- `[50l]` is a package-rendered single-fish composite:
  - whole-population response/BPI panels source ROI-centric `[50ia]` outputs
  - gene-specific AUC panels source HCR-centric identified-cell rows from package-owned `[56i]` motion-AUC plot tables
  - plotting code lives in `plots.analysis.render_single_fish_50l_composite`; the notebook cell should only provide knobs, paths, display, and legacy bindings
- `[57a-responsive-identity-donut]` is explicitly hybrid-scoped:
  - denominator is ROI-centric and responsive-only from `functional_roi_activity_identity.csv` (`response_is_active == True` and responsive `bpi_category` classes)
  - identity assignment for the identified fraction comes from selected `conf_to_func_pairs.csv` rows collapsed to exact per-ROI gene combinations
  - responsive ROIs without a selected HCR join are `unidentified`
  - selected HCR rows that do not join back to responsive ROIs are excluded from this figure
- `[cohort-50l-responsive-identity-donut-row]` inherits the same hybrid semantics from `[57a-responsive-identity-donut]`.

## Cohort analogue inheritance rule

- Cohort analogue figures must inherit the single-fish figure’s subset semantics, dedupe unit, and QC intent unless explicitly documented otherwise in the owning stage/reference docs.

## Figure defaults

- Notebook onboarding markdown near the top of a notebook should stay plain-English and reader-facing; reserve stage tags, implementation jargon, and interpretation details for the later stage markdown cells.
- Displayed figure titles should read like journal-ready takeaways in neutral voice.
- Prefer concept-first wording over stage tags, raw method names, or implementation jargon.
- Put figure-level takeaway in the `suptitle` and panel-level evidence statements in panel titles.
- Keep metrics, thresholds, and processing details in axis labels, legends, annotations, or notebook markdown unless they are essential to interpretation.
- For repeated small multiples, stable identifiers such as fish IDs or gene names may remain as panel titles when the suptitle already states the conceptual message.
- Font: prefer `Aptos (Body)` with sans-serif fallback.
- Panel title size: `11`.
- Default width: `A4_width * 0.8` unless panel constraints require otherwise.
- Share axes when directly comparable; disable only with explicit reason.
- Keep paired-condition colors clearly distinct and semantically stable.
- Parameterize spacing/geometry constants at top of cell.
- Auto y-limit with clearance (e.g., `max + 0.1`) for panel-level scaling.
- Use fixed RNG seeds for jittered points.
- Save publication PNG at `dpi=300`; add vector output when needed.
- In-panel numeric annotations (e.g., medians, n-labels) must use collision-aware placement (minimum vertical separation and x-neighbor checks) and should use a subtle text background (`bbox`) when plotted over dense marks.
- Prefer the 53a summary style for dense panels: place annotation text above the plotted data band, stack upward on collisions, and expand y-limits as needed so labels never overlap data or each other.
- Prefer reusing existing anti-overlap helpers/patterns (e.g., sample-size label collision avoidance in `plots.qa`) instead of ad hoc fixed-y text placement.
