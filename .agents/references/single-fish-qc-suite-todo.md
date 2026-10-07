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

## Current unattended queue — 2026-10-07

Current correction: queue `20261007T085018Z-seeded-native-remaining-fish-33fb`
stopped on f02 reference preparation (obsolete Suite2p absolute root). Other
three fish did not start. User requests repair/validation only, no resubmission.
Audit legacy acquisition frames and moved inputs with the package-owned
`suite2p_provenance` writer; preserve the failed recipes and original JSON.
New pinned recipes are required after repair; do not reuse failed spec/state.

- [x] Fresh isolated L765_f04 control with fixed ANTs seed 123: all 12
  writers and QC 00–05 passed without intervention; artifact audit and no-op
  resume passed. Job `20261007T074626Z-fixed-seed123-fresh-control-cbfb`.
- [x] Danin reviewed all 36 exported figures as good and authorized remaining
  fish production. This is visual control approval, not evidence of two
  byte-identical fresh whole-run seeded repeats. Historical seed-zero failure
  and canonical f04 geometry remain preserved; no f04 promotion is authorized.
- [ ] Submit sequential detached native jobs: L765_f02 (all ten planes),
  L758_f07 (all ten), L765_f05 (functional R2/global planes 5–9 only),
  L765_f06 (all ten). Fresh recipes pin seed 123, interpreter, clean checkout,
  source/drift/NCC hashes and consumer-specific fish-local outputs.
- [ ] On user-requested inspection, read terminal per-fish reports and retrieve
  exported QC galleries. No AI polling, automatic repair or retry; stop queue
  on the first failed job. Retain backups before per-file native promotion.
- [ ] Review each new fish's registration, resolved anatomy-label Z and
  proposed anatomy-grid midline before laterality/final figures/QC 06 release.
  Existing 19 retained HCR masks are provisional approximate inputs; quality
  gates remain active. Missing 17 native HCR masks and manual identity merge
  remain deferred. Do not infer a new biological exclusion from a job failure.

Execution checkout: `/Users/ddharmap/gitRepo/codeANTs-remaining-fish-20261006`.
Original dirty manual-identity checkout must remain untouched. Client evidence:
`wikiLogbook/daily/2026/2026-10-07.md` and dataset
`20-fresh-unattended-native-batch.md` (user decision supersedes the proposed
two-fresh-control launch hold; full-chain repeatability is still unproven).

## Historical working root — superseded by native production

- The sole active L765_f04 pipeline/QC root is
  `/Volumes/dataDrive/dataProcessing/2p_processing/_staged_pipeline_runs/20260915T093051Z_WORKING_PIPELINE_AND_QC/L765_f04`.
- Keep this dated root unchanged as the notebook fixture until the complete
  pipeline writer chain and all seven QC notebooks have been verified
  operational. Do not rotate individual notebooks to newer partial roots.
- Earlier dated roots are comparison/provenance sources only. Necessary
  artifacts must be freshly written into the active root through their owning
  stages; do not merge downstream folders from different runs or copy a
  convenient late-stage file without its dependency chain.
- All QC notebooks `00`–`06` point at this root. Stored notebook outputs are
  cleared when the root changes so an old rendered path cannot masquerade as
  current evidence.
- The fresh writer chain reaches canonical export and QA report. The
  2026-09-17 anatomy-midline proposal, logbook-gated materialization, and
  motion-AUC table stages now pass in this same root. Notebook 06 consumes
  their same-root outputs. Keep the root active until the final figure writer
  and all seven QC notebooks are revalidated after these changes.
- The 2026-09-15 consolidation audit found that the fish-local spatial manifest
  had backfilled legacy May individual-plane and motion-corrected products as
  canonical XY even though all ten fresh references were exact left-right
  mirrors of the reviewed references. Those entries now declare acquisition XY; the original
  manifest is preserved under the active root's `source-provenance/` directory.
  Functional references must record acquisition input -> canonical output and
  reproduce the reviewed orientation before registration proceeds.

## Deferred numbered-stage outline deviation

**Decision (2026-09-22):** do not rename, renumber, or reorder the working
pipeline to match the proposed 010–150 outline yet. The current pipeline is
operational, and preserving its live contracts is more important than
conforming paths to a prospective presentation order. Revisit this only as an
explicit migration with fresh stage-status and downstream-consumer validation.

The current implementation differs from a simple numbered linear outline in
these material ways:

- It exposes 17 semantic stages. Preprocessing is split into five granular
  writers before registration and geometry matching.
- `make-anatomy-midline-proposal`,
  `materialize-anatomy-midline`, and `make-motion-auc-tables` are explicit
  gates/writers and must not disappear inside broader numbered steps.
- The dependency graph is not purely linear: identity attachment and
  activity/BPI scoring both consume frozen geometry, canonical export joins
  their outputs, and QA/motion-AUC/figure branches have distinct dependencies.
- Run-level `_history`, `source-provenance`, and `review-artifacts`
  directories are lifecycle/provenance surfaces rather than analysis stages.
- Existing stage names, manifests, output paths, notebook loaders, and
  freshness checks are active compatibility contracts.

**To do:** when the numbered outline is revisited, first map each proposed
number to the live stage DAG and list every path/manifest/notebook consumer
that would migrate. No implementation change is authorized by this note.

## Non-negotiable review principles

- Reuse the scientific question, population, thresholds, categories, colours,
  and visual grammar of the relevant legacy cell unless a documented defect is
  being corrected.
- QC notebooks read persisted stage outputs and write nothing. Gate decisions
  and progress belong in wikiLogbook after agent/user evidence review. Explicit
  pipeline stages, never notebook cells, materialize hash-bound accepted
  computational inputs and derived tables. Laterality analysis consumes the
  anatomy-grid midline only after the logbook gate is passed.
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

**Approved resolution (2026-09-09):** The retained artifact does not preserve
an independently runnable legacy `[53a]` table/transform, so this is not an
ANTs-versus-NCC performance comparison. On the same frozen 3,419 unique
ROI/anatomy pairs, the stored field `selected_dist_um` is exactly the raw
centroid displacement in pixels; the physical remeasurement is its linear
conversion using anatomy XY spacing (about `0.477123 µm/px`). The non-unit
slope in the audit is therefore the missing pixel-to-micrometre conversion,
not a disagreement in pair geometry. Treat the stored field as pixel-valued
until its schema/name is repaired; Q0.1 is accepted as a units-semantics
resolution. L395_f11 historical equivalence remains unavailable and is not
needed to reopen this resolved L765_f04 display-metric question.

## Required QC work

### Q1. Early functional responsiveness and stimulus timing — Notebook 01

**Legacy source:** `[23a]`, `[23c]`.

**Question:** Is the fish functionally active, do responses occur around the
presented stimuli, and are the code's stimulus windows aligned to the recorded
data?

**Required views:**

- one persisted full-session Suite2p-cell heatmap, separated by session/plane
  as appropriate, with colored spans equal to the exact analysis windows used
  by its response scorer;
- representative/average stimulus-aligned traces and early response evidence;
- explicit source table for session logs, frame rate, frame boundaries, and
  planned no-stimulus blocks;
- an event-level timing audit containing recorded onset/offset, scored
  onset/offset, frame and second deltas, session/plane keys, and the documented
  rounding tolerance;
- visible warnings for frame-count, session mapping, or timing disagreement.

The recorded windows and scoring windows must remain separately auditable in
the timing table. The figure itself uses the exact scorer windows so it is a
literal view of the computation; do not add a second reconstructed full-session
overlay whose coordinate transform could diverge. `score-activity-bpi` must
persist the exact event windows it used.

**Accepted timing review (2026-09-09):** Q1 independently matched all 80
score-provenance input windows to the raw logs within the declared 0.5-frame
tolerance. The approximately 10-second analysis onset shift is the documented
scoring delay. The heatmap now marks B0 as planned no-stimulus baseline context
and draws only B1/B2 spans that feed the paired complete-Suite2p averages;
every stimulus/session average has four valid B1/B2 trials. Timing alignment is
accepted for this selected scoring universe. This does not promote the
response/BPI tables or accept downstream BPI/laterality interpretation.

**Correction and renewed acceptance (2026-09-15):** the 9 September timing
acceptance compared raw and scored events in the same B1-relative coordinate
system and therefore missed their placement on full 5,502-frame Suite2p traces
that still contained the 1,834-frame B0 block. All 80 prior windows were one
block early. A central mapper now places complete B0/B1/B2 acquisition blocks
on the Suite2p grid before both `[23c]` and `score-activity-bpi` scoring. In the
fixed working root, B1 scored onsets are frames 2,247–3,441 and B2 scored onsets
are frames 4,373–5,342 across sessions; the 80 `[23c]` heatmap windows exactly
equal the persisted scorer windows. The single retained heatmap shows B0 as
uncolored baseline context and the expected response structure after the exact
scored spans. Notebook 01 and Notebook 06 execute end-to-end, and response-active
ROIs change from the superseded 83 to 225. Q1 timing is accepted on this
corrected full-trace basis; downstream biological BPI interpretation remains a
separate review gate.

**Complete-stimulus correction (2026-09-16):** the corrected trace-grid
placement still scored only conditions whose legacy code ended in `LB` or
`LC`, so the 80-window review covered only `LB_trajectory` and
`LC_trajectory`. The central provenance now retains every presented condition,
but BPI is deliberately restricted to the balanced comparison requested by the
experiment: `LB+RB` bouts versus `LC+RC` continuous stimuli. `LAB`, `RAB`,
`Ll_RB`, `Rl_RB`, `WFCl`, and `WFCo` remain visible in the heatmap and trace
panels and are explicitly marked `used_for_bpi=False`. The fixed working root
contains 400 presented windows (40 per condition; 200 each in B1 and B2),
with 160 BPI-eligible windows (8 bout and 8 continuous trials per plane). B0
remains unscored. The balanced rerun yields 161 response-active ROIs and 2,450
finite BPI values. The heatmap and all ten trace panels passed visual review,
and all seven QC notebooks execute end-to-end. Timing and condition coverage
are accepted; biological BPI interpretation remains a separate review gate.

**Recovered in the single working root (2026-09-15):** the 8–9 September Q1
implementation had been displaced when Notebook 02 was correctly restored to
its functional-registration ownership. The package renderers and timing loader
were still present, but no QC notebook called them. Notebook 01 now owns the
persisted Q1 review as intended: `prepare-functional-reference-stacks
--run-23c` writes the full-session matrix/row keys, B0/B1/B2-aware heatmap,
stimulus-locked traces, per-cell stimulus summary, source table, and
pre-identity response summary into the fixed active root; Notebook 01 joins
that evidence to independently loaded raw logs and the same-root
`scored_stimulus_windows.csv`. It never invokes the raw in-memory fallback.

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

**L765_f04 visual review (2026-09-15):** the displayed native-functional
fixed-consensus midline proposal was reviewed and accepted as suitable for the
functional-plane QC view. This acceptance does not manufacture the distinct
anatomy-grid, hash-bound sidecar required by Notebook 06; anatomy-space
laterality and laterality-dependent AUC remain blocked until that coordinate-
space provenance exists.

**Implemented and visually reviewed (2026-09-17):** The backend now projects
the saved native-functional fixed-consensus proposal through the selected
scale/NCC/ANTs chain onto the anatomy grid and renders all ten planes over
anatomy and transformed ROI outlines. The user-confirmed midline separates
the bilateral fields in the full-grid visual review. wikiLogbook
`daily/2026/2026-09-17.md` owns the accepted gate and exact proposal CSV/PNG
hashes; `materialize-anatomy-midline` wrote the hash-bound anatomy-grid JSON,
line table, and accepted overlay. Notebook 01 only displays the proposal;
Notebook 06 only reads the materialized line and laterality tables. The old
notebook/GUI sidecar writer was removed. Cross-fish automation remains a
separate validation question.

### Q2.1 Functional/anatomy plane and label linkage — Notebook 02

**Question:** Does Cell 11 show the anatomy intensity page used by functional
registration together with the anatomically corresponding label page, so a
visible mismatch diagnoses a backend mismatch rather than a plotting mistake?

**Accepted resolution (2026-09-15):** the reviewed registration summaries
defaulted label Z to direct indexing, while the independently inferred mapping
for L765_f04 is reverse. The `make-functional-registration-qc` writer now uses
the central anatomy-label Z resolver, persists both `anat_label_z_mode` and the
explicit resolved `anat_label_z` for every plane, validates that provenance,
and fails closed on an invalid mapping. Cell 11 remains a thin reader of this
single persisted plane summary; it must not invert pages locally. The fresh
active-root summary resolves all ten planes as reverse (`label_z = 51-best_z`),
and the `3976x6845` real-data grid was visually reviewed: each fourth-column
boundary overlay follows the same anatomical structures shown in the adjacent
best-Z intensity column. The figure now functions as the intended backend
orientation check.

### Q3. Functional/anatomy overlay colours — Notebook 03

**Legacy source:** `[22e]`.

Change the current overlay to the legacy green/magenta pairing, preserving the
same data and geometry. Verify boundaries remain distinguishable on both dark
and bright anatomy regions.

**Implemented and retained (2026-09-08; re-audited 2026-09-15):** anatomy-label
boundaries are green (`#00A651`) and transformed functional-ROI boundaries are
magenta (`#D81B60`) in the package-owned geometry placement views. The current
Notebook 03 remains the accepted/frozen geometry review surface; this colour
change does not alter labels, matches, or coordinate transforms.

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

**Implemented (2026-09-08; display completed 2026-09-15):** Notebook 04 reads
the persisted anatomy, transformed-functional, and retained-HCR label masks and
computes the legacy bounding-box metrics without changing any mask. It applies
modality-local XY q05 drops, retains q95-flagged observations, shows counts,
medians, SDs, and units, and draws the q95 limits as red horizontal segments in
both XY panels. Individual scatter points remain intentionally omitted from
the violin plots after the 8 September review.

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

**Current Cell 12 two-ring functional-plane audit (2026-09-08):** replaced the
earlier acceptance-flow donut, which was not the requested legacy surface. The
read-only Cell 12 now reproduces the `hcr_activity_status_overview` grammar:
the inner ring is `within plane`, `outside plane`, or `unmatched`; the outer
ring partitions within-plane labels as responsive, low-activity, response
unavailable, or no-functional-match, and represents outside/unmatched labels
explicitly. It reads only persisted HCR labels, their q95 warp metadata, and
`hcr_activity_status.csv`. q95 labels are excluded from this legacy
representation denominator; no HCR/anatomy/ROI matching is recomputed or
changed.

**L395_f11 validation and handoff (2026-09-08):** real Linnaeus rendering
reproduced the legacy five-panel layout and denominator (`n=263` total; pth2
20, sst1.1 153, sst1.2 13, tac3b 77). Focused QC-molecular tests passed 17/17
locally and on Linnaeus. The current persisted tac3b sources split the 77
labels as 19 within-plane, 41 outside-plane, and 17 unmatched, whereas the
historical PNG shows 19/43/15. This is preserved as a provenance discrepancy:
the historical figure and current q95/status artifacts are not jointly
reconstructible for those two labels. Do not fabricate historical assignments
to force visual parity. Next session: visually review the Cell 12 L395 output;
if exact historical tac3b parity is required, locate the historical status/q95
writer inputs before changing the read-only QC classifier.

**L765_f04 working-root update (2026-09-15):** the fresh identity/HCR writer
now persists `hcr_activity_status.csv` (434 rows) plus its summary, raw-pair,
responsive-pair, and candidate tables in the active root. Notebook 04 Cell 12
executed from that writer-owned path without the previous FileNotFoundError or
missing-status block.

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

**Physical-spacing correction (2026-09-17):** Notebook 05 Cell 04 previously
read the anatomy-label TIFF's default `1 px = 1 µm` metadata, inflating XY
offsets and the radius reference while understating Z offsets. The centroid
offset builder now requires the prepared anatomy NRRD as an explicit voxel
source, records that provenance and spacing in its review table, and labels the
reference as the all-label maximum-XY-section equivalent radius. Notebook 03
retains its intentionally different matched-label, resolved-plane-section
reference; both now use the same physical anatomy grid. The complete Notebook
03 and Notebook 05 cell sequences execute without errors on the active L765_f04
root; visual review confirms the expected 2.94 µm matched-plane reference in
Notebook 03 and corrected 2.87 µm all-label max-section reference in Notebook
05, with legible unclipped XY/Z panels.

**Shared-radius resolution (2026-09-17):** The earlier 2.94 µm
matched-plane reference was not the intended plausibility population.
Notebook 03, Notebook 05, and package matching/QA now use one computed
median of each anatomy label's maximal XY-section equivalent radius across
the entire 3-D label volume, with physical anatomy XY spacing. L765_f04
currently computes to 2.874 µm (15,761 labels); no fish-specific constant
is hardcoded. This supersedes the differing-reference statement above.

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

**Implemented review surface (2026-09-08; active-root execution 2026-09-15):**
Notebook 06 renders the ROI-centric response plane, BPI/activity diagnostics,
activity-binned absolute BPI, response-class distributions, and population
response/BPI summary from the persisted 3,553-row activity master (2,450 rows
with finite response evidence). After the Q1 full-trace timing correction, 225
rows pass the response-active gate (the superseded one-block-early outputs had
83), and all 2,450 finite BPI values were recomputed. The global
ipsilateral/contralateral AUC panel
continues to fail closed because the distinct accepted anatomy-grid midline
sidecar and L765_f04 motion-AUC point/count tables do not exist. This preserves
the original Q7 work without falsely declaring the laterality branch complete.

**Laterality/AUC backend completed (2026-09-17):** The first
`make-motion-auc-tables` run correctly exposed a parser gap for this fish's
`LB/RB/LC/RC_trajectory` log names. The central stimulus parser now recognizes
those unilateral circular-motion conditions while excluding approach, whole-
field, and mixed conditions. The rerun passed and wrote same-root point,
count, and ROI-panel CSVs from the accepted anatomy-grid midline. Notebook 06
now reads these tables. A subsequent audit showed that its first generation used 7-s BPI-eligible windows. On 2026-09-17 scoring, identity attachment, canonical export, motion-AUC, QA report, and figures were recomputed in place in the pinned working root; all 160 BPI-eligible windows now use the fixed 12.5-s movement-onset policy, and Notebook 06 cell 09 executes with status `ready`. The earlier separate continuation remains diagnostic provenance only. This is computational validation, not acceptance of biological BPI interpretation.

**2026-09-18 LRI review extension:** Notebook 06 now pairs the persisted ipsi/contra motion-AUC points into left and right stimulus AUC for each side-assigned ROI and mode, then displays `(left-right)/(left+right)` on the accepted anatomy midline and as mode-separated count histograms with a notebook-editable visual `|LRI| > 0.1` guide. Only rows passing the corresponding bout or continuous response test receive left/right preference colors; other scored ROIs remain context. Negative AUC and unstable-denominator rows are flagged, and this diagnostic does not change canonical response/BPI classes. Danin accepted current BPI preference classes provisionally. On 2026-09-22, Danin accepted significant negative AUC as a suppressed response and chose the stricter two-sided null rule: 0.995 for activation and 0.005 for suppression, preserving a combined 1% false-positive allowance. The 6-of-8 trial-sign check remains diagnostic only until it can be compared across several fish. Canonical promotion waits for an isolated L765_f04 rerun and laterality review.

### Q8. Identity-functional merged review — Notebook 06

**Legacy source:** `[51]`, `[56]`, `[56h]`, `[57]`, `[57a]`, `[57b]`.

Notebook 06 now adds five read-only views after the independently persisted
identity and activity branches: an all-trace-available Suite2p-cell response-similarity raster with
identity/response/side annotation strips, HCR-centric responsive-marker traces,
persisted marker AUC by accepted laterality, separate HCR-fate and ROI-centric
responsive-identity coverage panels, and an accepted-midline anatomy map of
exact identity combinations and response classes. The loader rejects mixed Q1
trace generations and marker AUC whose midline hash does not match the accepted
sidecar. Coexpression stays as an exact multi-marker ROI combination; it is not
duplicated into the ROI-centric coverage denominator.

This implementation provides the review surface but does not itself accept the
biology or promote the run. Full-session per-gene traces and a selectivity summary remain deferred until
the accepted signed-response and stricter two-sided classifier have been
rerun and reviewed on L765_f04. The 6-of-8 expected-sign check remains
diagnostic only pending comparison across several fish.

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
8. Q8 merged identity-response views in Notebook 06; keep selectivity metrics
   deferred until the unresolved response-sign policy is decided.

Each slice requires focused tests, real L765_f04 execution on Linnaeus, and
visual inspection of the resulting notebook/figure before it is marked done.
