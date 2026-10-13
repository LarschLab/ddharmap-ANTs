# Figure Rules

**Purpose:** keep figures consistent with authoritative tables and response semantics.

**Use this file when:** adding/refactoring plots or trace-summary panels.

Machine-readable contracts for accepted high-risk tables and figures live in `.agents/contracts/scientific-artifacts.json`. A renderer change must preserve its question, source, population, filters, denominator, units, grouping, thresholds, and forbidden reinterpretations or record an explicit scientific-change decision.

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
- Show response class and BPI preference separately. A large z-response or BPI magnitude is not, by itself, a significant AUC/null response call.
- Notebook 06 left/right-index QC maps and histograms use the same mode-responsive, side-assigned, nonnegative-AUC ROI subset. The histogram shows separate bout and continuous ROI counts on a fixed LRI axis displayed +1 to −1 (left preference on the left), with the notebook's editable ±threshold guide; negative-AUC and unstable-denominator rows remain separately flagged, not silently plotted as preferences. Neither view changes response or BPI scoring.
- In the Notebook 06 BPI gate, condition-AUC, magnitude-bin, and response-class panels use every response-assessable ROI and state that denominator; response-unavailable ROIs appear only in the all-ROI population partition. BPI-versus-magnitude panels include response-positive ROIs only, because BPI is an effect-size preference rule rather than a contrast-significance test. Encode BPI preference by color and the independently tested response class by marker shape.
- Notebook 01 places a per-session stimulus/counts/valid-counts table before the trace figures so missing trials are visible.
- `[23c]` trace panels use one fixed cohort responsive to any presented stimulus, not the narrower LB/RB/LC/RC BPI response flag. They show individual stimulus-averaged cell traces only: cross-cell/session population means cancel distinct responses and are omitted. Assessable nonresponsive cells remain separately counted and persisted, but are not plotted as a mean; unavailable cells are not silently called nonresponsive. Trace time zero is stimulus appearance, with the delayed motion-scoring onset marked separately. `[23c]` full-experiment heatmaps keep all Suite2p-cell traces, including nonresponsive neurons.
- Notebook 01 renders the Q1 trace and heatmap figures from same-root persisted numeric payloads, not cached PNGs. The trace payload is hash-bound to its summary CSV so mixed-generation inputs fail closed.
- `[23c]` trace stimulus panels use two columns: left-column stimuli (`L*` and `WFCl`) on the left, right-column stimuli (`R*` and `WFCo`) on the right, with rows keyed by stimulus type after the side prefix.
- `[23c]` full-experiment heatmaps use frame on the X axis and a white-to-black scale where black is high activity, with transparent spans for every presented stimulus over the corresponding session/plane row block; `planned_schedule.csv` rest blocks remain span-free. The provenance table marks which intervals feed the balanced LB/RB-versus-LC/RC BPI calculation.
- `multiFish.ipynb` inherits `[23c]`: average traces use the fixed any-presented-stimulus responder cohort when screening is available, while full-session heatmaps retain all valid Suite2p-cell traces. The cohort cache records this selection policy so older BPI-filtered caches cannot be reused silently.

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
- `[56h]` staged `make-figures` trace/status artifact (F049 restoration approved by Danin, 2026-10-12):
  - primary content is individual identified-cell trial-averaged dF/F plus an equal-cell gene mean, in contra/ipsi rows × bout-like/continuous columns per gene; bilateral conditions are excluded
  - cell sets and gene membership come from `[56i]` `motion_auc_plot_points.csv`, with `(gene, plane_idx, func_label)` deduplication; persisted `motion_auc_roi_panels.csv` supplies `roi_side` (the stage's midline-side annotation) and Suite2p quality
  - current `functional_roi_activity_bpi_cells.csv` supplies response state and `bpi_preference`; low activity is graded grey, suppressed-only is gene-colored and dashed, with no BPI availability gate
  - load fluorescence via the same `suite2p.load_suite2p_dff_map` as response scoring (10th-percentile F0, epsilon 1e-6), subtract each persisted trial's true prestimulus mean, then average trials per cell; no z-score or new normalization
  - `score-activity-bpi/registration/scored_stimulus_windows.csv` is required: time zero is stimulus appearance (`prestim_baseline_end_frame`), and the shaded movement interval uses persisted scored frame boundaries; display includes 12.5 s before appearance and 7.5 s after the 12.5 s movement interval
  - HCR-status donuts retain the HCR-label denominator from `hcr_activity_status.csv`, separately from trace ROI counts
  - keep `per_gene_stimulus_trace_with_hcr_status_56h.png/.pdf`; the summary CSV records plotted cell/trial counts, not AUC bars
  - no notebook-local `results` or `results_per_cell` payloads; no identity reconstruction in plotting
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

## Curated single-fish gallery (Danin, 2026-10-13)

- `qc_figure_export.CURATED_CARDS` owns ordered role selection; never select by extraction F-number. Preserve complete inventory, source hashes, exclusions and missing evidence.
- P003/P005/P010/P020 are package-rendered native-resolution composites, orchestrated in QC notebooks 02/03/06. The exporter only selects PNGs.
- P008 uses saved accepted/rejected HCR masks on every geometry-owned functional anatomy plane; no new matching.
- P019 retains Q7 marker-stratified traces and adds the legacy two-ring HCR scope/status partition. Inner: within/outside functional planes/unmatched. Outer: responsive, low activity, response unavailable, no functional match, out-of-plane, unmatched. Count only persisted HCR status rows; do not add unmatched masks from the historical cohort summary. Trace ROI membership counts and HCR-label denominators remain distinct.
- P013 retains the fixed any-presented-stimulus cohort. P014 is approved as signed [23c] AUC violins (round 2). P026 and the standalone 56h gallery card are omitted; writer products remain.
- Q7 response evidence shows LB/RB/LC/RC signed margins against each stimulus's own saved null and persisted floor, and saved selected tuning side. Pooled AUC panels are comparison-only with no floor decision lines.
- F046 global and population AUC panels share one data-derived limit, with median labels in two fixed rows above data (independent of gene count). Identity count summaries label every non-zero count and zoom small categories.

### Gallery review round 1 (Claude, 2026-10-13)

P019 adds explicit figure-level inner-location and outer-status colour legends and a west-anchored adjacent donut column. P015 colours margins by saved per-stimulus valence (non-significant first, activation and suppression passes above); legends count finite plotted calls. Tuning-side bars include only rows with at least one saved activated call, with excluded/no-side counts in the title. The degenerate magnitude-bin curve is removed, retaining its annotation-count legend. P008 uses a two-column all-plane grid with thicker display-only outlines. P003 places both complete row sets side by side at equal heights; P010/P020 upscale narrower parts to equal widths. No source PNG is downsampled or cropped; originals remain in inventory. P005/P016/P020b remain unchanged.

### Signed early-screen plots (Danin, 2026-10-13, round 2)

P014 is now approved: two violin rows (all represented Suite2p cells; fixed any-presented-stimulus responders) × session columns, all ten presented stimuli, using saved [23c] signed `mean_auc_dff`. Show zero, median, n and activated/suppressed/not-significant/unavailable counts. Row 2 is selected by the same responses and labelled descriptive. P018 pivots that same independent AUC table; normalized colourbar is unitless with input dF/F·s, raw companion is dF/F·s. The four contaminated [23b] 50 s scalar columns are retired; no response tests or score windows change. Gallery provenance has one row per gate, inline saved values and collapsible original evidence. P014 is no longer held.

### Violin display range (2026-10-13, review round 3)

P014 uses row-wise symmetric bounds from the 0.5–99.5% finite AUC quantiles, shared across sessions within each row. Violin densities omit values outside those display bounds and label above/below counts per stimulus. Medians and saved call counts retain all observations. If the robust bound exceeds 15 times the row median absolute AUC, use a labelled symlog axis with that median as linthresh (a numerical safeguard applies for zero medians). P018 reserves an explicit adjacent colourbar column so no auto-shrink gap separates the mean-AUC panel. These are presentation changes only.
