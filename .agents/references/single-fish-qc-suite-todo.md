# Single-Fish QC Suite Work Register

**Purpose:** track the scientific review questions that the dedicated QC
notebooks must inherit from `notebooks/singleFish.ipynb`.

**Scope:** the active register spans notebooks `01` through `06` under
`notebooks/qc/`; the work below is not limited to `04`–`06`. Reusable loading
and plotting belongs in `src/codeants_2pf_hcr/`; notebook cells remain thin
read-only orchestration.

**Execution location:** perform code changes, tests, notebook execution, and
real-data rendering in `/Users/ddharmap/gitRepo/codeANTs` on `linnaeus`,
accessed through SSH in VSCode. Treat that remote checkout as the working copy;
sync any local assistant workspace changes to it only after inspecting diffs
and preserving pre-existing remote work. Read data from
`/Volumes/dataDrive/dataProcessing/2p_processing` there.

## Non-negotiable review principles

- Reuse the scientific question, population, thresholds, categories, colours,
  and visual grammar of the relevant legacy cell unless a documented defect is
  being corrected.
- QC reads persisted stage outputs. It does not silently recompute, promote, or
  change geometry, identity, activity, or BPI assignments. An explicitly named
  review action may write a hash-bound decision/provenance sidecar—for example,
  accepted or corrected midline coordinates—but it must not overwrite a
  scientific stage table. Laterality analysis may consume the accepted midline
  sidecar only after that review gate is passed.
- Show sample sizes and authoritative cut-offs on distribution plots.
- Keep ROI-centric whole-population outputs separate from HCR-centric
  identified-cell outputs. Hybrid views must say exactly which denominator and
  join they use.
- Validate every visual change with a real L765_f04 render on Linnaeus.
- Follow the 2026-09-02 Johannes review order: establish direct,
  stimulus-resolved response structure before interpreting BPI/AUC ratios;
  inspect molecular-functional correspondence in reviewable tiles before any
  manual-exclusion workflow; and keep laterality-dependent interpretation
  blocked until a midline decision is accepted.

## Priority 0 — resolve before extending the suite

### Q0.1 Verify Notebook 03 XY offsets against legacy `[53a]`

**Question:** Is Notebook 03 measuring the same lateral functional-ROI to
anatomy-centroid displacement as legacy `[53a]`, in the same coordinate frame
and physical units?

**Why:** Legacy L395_f11 shows a median/typical XY offset below 1 µm and agrees
with visual inspection. Notebook 03 Cell 08 appears larger. This could reflect
a different population, a coordinate-frame error, duplicated transformation,
or incorrect pixel-to-micron conversion.

**Required comparison:**

1. Run the legacy `[53a]` calculation read-only for L765_f04 on Linnaeus.
2. Run the Notebook 03 calculation from the same frozen geometry artifacts.
3. Intersect exact `(plane_idx, func_label)` keys and use the same unique-match
   filter in both paths.
4. Compare row-level XY distances, per-plane medians, overall median, sample
   size, coordinate columns, voxel spacing, and transform direction.
5. Render side-by-side distributions and a row-level legacy-versus-QC scatter.
6. Repeat the metric audit on L395_f11 as a known visual sanity reference; its
   sub-1-µm result is evidence, not a universal threshold.

**Stop condition:** do not redesign later QC geometry panels until the two
calculations agree or the intended semantic difference is documented and
accepted.

## Required QC work

### Q1. Early functional responsiveness and stimulus timing — Notebook 01

**Legacy source:** `[23a]`, `[23c]`.

**Question:** Is the fish functionally active, do responses occur around the
presented stimuli, and are the code's stimulus windows aligned to the recorded
data?

**Required views:**

- full-session Suite2p-cell heatmap, separated by session/plane as appropriate;
- independently reconstructed recorded stimulus windows drawn over the heatmap,
  plus the exact analysis windows consumed by `score-activity-bpi` as a second,
  visually distinguishable overlay;
- representative/average stimulus-aligned traces and early response evidence;
- explicit source table for session logs, frame rate, frame boundaries, and
  planned no-stimulus blocks;
- an event-level timing audit containing recorded onset/offset, scored
  onset/offset, frame and second deltas, session/plane keys, and the documented
  rounding tolerance;
- visible warnings for frame-count, session mapping, or timing disagreement.

The recorded windows and scoring windows must come through independent loading
paths. Plotting the scoring table twice would reproduce a timing bug rather than
detect it. If Step 06 does not persist the exact event windows it used, add a
read-only provenance output to `score-activity-bpi` before implementing this QC
view.

**Placement:** extend Notebook 01 because it already owns functional-reference,
drift, and early-response evidence. This review must precede molecular geometry.

**Meeting-driven extension (2026-09-02):** make the direct evidence legible
before presenting preference indices: include per-stimulus response distributions
or mean traces for the complete Suite2p-cell population, and an optional
response-similarity clustering view whose input rows/normalization are explicit.
Do not use molecular identity or BPI to select clusters. Crossing-versus-
non-crossing trajectory contrasts are exploratory only after condition-selective
responses are visible; annotate receptive-field/stimulus-coverage confounding.

### Q2. Midline placement and laterality prerequisite — Notebooks 01 and 06

**Legacy source:** `[22c]`, `[56f-qc]`, `[56f-qc-activity]`.

**Question:** Is the anatomical/functional midline correctly placed, and do
laterality assignments remain credible for all ROIs and for the activity
subset?

**Required views:**

- manual interactive midline placement/confirmation in registered anatomy
  space, persisted with coordinate-space provenance;
- `[56f-qc]`-style plane-grid overlay for all Suite2p ROIs, with midline,
  hemisphere colours, and per-plane side counts;
- `[56f-qc-activity]`-style spatial activity map, with activity magnitude,
  midline, plane context, and the exact analyzed subset.

The two Notebook 06 views are required, not optional. Laterality-dependent
analysis must remain blocked until midline placement is accepted.

**Automation backlog:** develop and validate automatic midline estimation from
registered anatomy/functional structure. Initially show the automatic proposal
against the manual result and retain manual correction/acceptance. Do not make
automatic placement authoritative until cross-fish validation demonstrates
stable coordinate-space and laterality assignments.

**Implemented, awaiting manual review (2026-09-03; moved to Notebook 01 on
2026-09-08):** Notebook 01 now exposes
a per-functional-plane, click-two-endpoint annotation GUI over registered
anatomy with functional ROI outlines. It can copy a prior plane's line and,
only after explicit complete-set acceptance, writes a hash-bound review sidecar.
The GUI does not alter a canonical midline, laterality assignment, or stage
table. Its saved annotations are intended as training labels for the pilot fish;
the Notebook 06 spatial views and any authoritative laterality consumer remain
pending manual review and acceptance.

**Proposal validation update (2026-09-03):** A 35-plane native-functional
training set and 50 held-out L758-plane validation set support the constrained
dark-corridor proposal with reflection symmetry as the preferred manual-review
candidate. Symmetry reduced held-out p95 axial-angle error from 11.37° to
2.49° and p95 perpendicular offset from 9.01 to 7.55 px. Visual review found
11 dark-only proposals unsuitable for laterality; the known exception is
L395_f10 plane 2, where symmetry is worse. Keep both overlays in QC, default
to symmetry for proposal, and retain manual correction/acceptance as the sole
laterality gate.

### Q3. Functional/anatomy overlay colours — Notebook 03

**Legacy source:** `[22e]`.

Change the current overlay to the legacy green/magenta pairing, preserving the
same data and geometry. Verify boundaries remain distinguishable on both dark
and bright anatomy regions.

### Q4. Cross-modality mask-size distributions — Notebook 04

**Legacy source:** `[53]`, `[53a]`.

**Question:** Do functional, anatomy, and retained HCR segmentations have
plausible and comparable mask sizes laterally and axially, or does a modality
show fragmentation, merging, or Z elongation?

**Required figures:** reproduce the legacy `[53]` bounding-box metrics exactly.
For each 3D label, compute `x_um = (xmax - xmin) * dx`, `y_um = (ymax - ymin) *
dy`, `z_um = (zmax - zmin) * dz`, and `xy_um = (x_um + y_um) / 2`. The primary
paired panels must show `xy_um` and `z_um` for Anatomy, Functional, and HCR,
followed by the same two distributions split by HCR gene/round. Retain X and Y
as auditable source columns and allow an X/Y diagnostic breakdown, but do not
substitute separate X and Y panels for the requested XY summary. Show individual
observations, median, standard deviation, sample size, and physical units.

Apply the legacy filter separately within each modality using `xy_um`: hard-drop
values below that modality's q05 and retain-but-flag values above its q95 as
low-confidence. Display the q05/q95 values and before/after counts. Do not apply
those cut-offs along Z, and do not replace the bounding-box dimensions with
area-equivalent or volume-equivalent diameters. Keep lateral and axial panels
separate because point-spread and sampling differ strongly along Z.

### Q5. HCR matching flow and placement — Notebook 04

**Legacy source:** `[44]`, `[47b]`.

**Question:** For each retained gene/round, what happened between segmented HCR
labels and accepted one-to-one anatomy assignments, and do the transformed
labels sit plausibly in anatomy space?

**Required views:**

- current fast anatomy-space intensity/label viewer;
- per-gene/round counts through candidate, gate, ambiguity, quality, rejection,
  and final accepted one-to-one stages;
- optional accepted/rejected overlay toggle for investigating a problematic
  category without changing pair membership.

**Implemented (2026-09-02; extended 2026-09-08):** Notebook 04 reads the persisted label, review,
and final-pair artifacts into a per-gene/round flow. It reports segmented,
candidate, gate, ambiguity, review-quality, rejected, and accepted-one-to-one
counts without regenerating pairs. Its optional anatomy-space overlay now
loads accepted labels from saved final pairs and rejected labels from saved
review rows absent from those pairs; the green/orange visibility toggles are
read-only and cannot alter pair membership. Manual placement review remains
required.

**Two-ring review update (2026-09-08):** the Cell 10 flow surface now uses the
legacy two-ring donut grammar for each gene/round: inner ring final one-to-one
acceptance versus non-acceptance, outer ring a mutually exclusive persisted
terminal outcome partition. The full stage-count table remains visible for
auditing, but the bar-chart substitute is no longer the primary review figure.

### Q6. Molecular identity fate — Notebook 05

**Legacy source:** `[50]`, `[50e]`, `[53a]` HCR/anatomy offsets.

**Question:** Are accepted HCR identities attached without changing frozen
geometry, and why is each molecular label present or absent from functional
analysis?

**Required views:**

- current HCR/anatomy XY and Z centroid-offset distributions by gene, with
  sample sizes and median anatomy XY-radius reference;
- `[50e]`-style per-gene fate view with the complete ordered category domain,
  including categories with a zero count: `unmatched` (no accepted HCR-to-
  anatomy assignment), `out-of-plane anatomy label`, `in-plane no functional
  ROI candidate`, `in-plane response unavailable`, `in-plane low-activity ROI`,
  and `in-plane responsive ROI`;
- counts as well as proportions, so a low active fraction can be traced to no
  geometry match, lack of plane coverage, trace-quality/availability, or a
  genuinely low response;
- explicit HCR-centric scope and denominator.

The first category is an HCR-to-anatomy failure; the third is an
anatomy-to-functional failure after a valid molecular assignment. They must be
computed from their respective authoritative tables and must never be conflated.
Do not merge any of these categories into a single inactive group.

**Meeting-driven extension (2026-09-02):** add a review-only tile grid for
every selected molecular-to-functional correspondence. Each tile must identify
the accepted HCR/anatomy label and functional ROI, show the registered
anatomy/GCaMP context plus HCR and ROI boundaries, and include the available
trace/response status. Group or filter tiles by gene, response status, and
flagged geometry/segmentation outcome. This is an inspection surface for merged
masks and vessel/background artifacts; it must not create exclusions or change
pair membership. A manual-exclusion GUI is deferred until review criteria are
explicitly agreed.

**Implemented (2026-09-02; extended 2026-09-08):** Notebook 05 reads frozen final pairs and the
persisted scored activity-identity table to show the complete ordered HCR-label
fate domain, including zero-count categories, with HCR-centric counts and
proportions. Its correspondence tile grid reads persisted final-pair and
frozen-geometry/activity rows only, renders every selected HCR→anatomy→ROI link
by default, and provides display-only gene, response-status, and flagged-record
filters. It never writes an exclusion or changes pair membership. Manual
inspection of merged masks and vessel/background artifacts remains required.

### Q7. Functional activity and BPI gate — Notebook 06

**Legacy source:** `[50l]`, `[56g]`.

**Question:** Are response calls and BPI classes supported by the underlying
bout/continuous response magnitudes, and do authoritative cut-offs divide the
population as intended?

**Required views:**

- `[56g]` response plane: bout response versus continuous response, preserving
  all response/BPI categories, sample sizes, and cut-off lines;
- BPI versus activity magnitude with zero-band and activity threshold;
- activity-binned absolute BPI;
- activity distributions by response/BPI class;
- `[50l]` top-left whole-population response-strength-versus-BPI panel;
- `[50l]` top-right population response/BPI nested donut;
- `[50l]` bottom-left global ipsilateral/contralateral bout-versus-continuous
  AUC view and its population-count strip.

Use thresholds persisted by `score-activity-bpi`; plotting must not recalculate
or hard-code them. Preserve the established response colours.

**Meeting-driven guardrail (2026-09-02):** present this gate only after Q1's
direct individual-stimulus views. BPI is a response ratio, not the first
functional result. Clearly distinguish a directional response call (response
pass plus BPI direction) from a finite raw BPI in a low-activity/no-response
trace.

### Q8. Deferred until identity and functional response are merged

**Legacy source:** `[51]`, `[56]`, `[56h]`, `[57]`, `[57a]`, `[57b]`.

After both Identity and Activity/BPI gates are accepted, add HCR-identified
stimulus-aligned traces, the `[56h]` combined identity/response summary,
full-session per-gene traces, responsive-identity coverage, and anatomy-label
co-expression summaries. `[56h]` must remain deferred until the identity and
functional response branches are explicitly merged.

Targeted rejected-mask exports/plots from `[50f]` and `[50g]` remain escalation
tools for a suspicious gene or failure category rather than mandatory views for
every fish.

## Proposed implementation order

1. Q1 early `[23c]` responsiveness/timing review, including the new direct
   individual-stimulus and optional response-similarity views.
2. Q0.1 XY-offset equivalence audit before extending any geometry panels.
3. Q2 midline acceptance and both required Notebook 06 spatial views.
4. Q4 mask-size XY/Z distributions and Q5 HCR matching-flow summary.
5. Q6 complete HCR fate view plus the review-only correspondence tile grid.
6. Q7 `[56g]` and selected `[50l]` activity/BPI panels, interpreted only after
   the direct stimulus views; add the global ipsilateral/contralateral AUC view
   only after accepted midline provenance.
7. Q3 colour-only overlay adjustment.
8. Q8 merged identity-response views after both gates are accepted; begin with
   marker-stratified individual-stimulus responses before any gene-level ratio.

Each slice requires focused tests, real L765_f04 execution on Linnaeus, and
visual inspection of the resulting notebook/figure before it is marked done.
