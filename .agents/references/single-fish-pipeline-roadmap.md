# Single-Fish Pipeline Roadmap

**Purpose:** define the migration path from the current notebook-led single-fish workflow to a staged, reproducible pipeline while preserving `notebooks/singleFish.ipynb` as an internal control.

**Use this file when:** planning or implementing the notebook-to-pipeline migration, defining stage contracts, adding manifests, comparing old and new outputs, or deciding whether new logic belongs in a package stage, CLI wrapper, QA report, or notebook.

## Guiding Goal

Build a new single-fish pipeline that can recreate the current notebook outputs without modifying the original notebook. The current notebook remains a reference implementation and internal control during the migration.

The scientific ordering stays unchanged:

1. Decide geometry first.
2. Assign molecular identity second.
3. Add activity and BPI third.
4. Build figures and reports last.

The engineering change is to move stateful processing out of notebooks and into explicit package-owned stages with declared inputs, outputs, QA checks, and manifests.

## Target Shape

The future workflow should be executable without opening a notebook.

Package and CLI stages should own:

- raw input audits
- preprocessing
- registration
- ROI/anatomy/HCR matching
- molecular identity assignment
- response and BPI scoring
- canonical table writing
- state/cache validation
- QA artifact generation

Notebooks should become read-only review surfaces for biologists:

- inspect inputs
- review registration quality
- review ambiguous matches
- inspect activity and identity summaries
- assemble or review figures

Notebooks should not secretly redefine matching, identity, response semantics, or canonical table schemas.

## Migration Strategy

Use a parallel "strangler" migration.

The original `notebooks/singleFish.ipynb` stays untouched. The new pipeline grows beside it, stage by stage, and is compared against frozen notebook outputs for representative fish.

During migration, both paths may exist:

- old notebook path: internal control and behavior reference
- new staged path: future canonical processing route

Promotion happens only after the new path consistently recreates canonical outputs or produces documented, intentional differences.

## Phase 0: Freeze Current Notebook Outputs

Select a small panel of representative fish and run the current notebook end to end.

Save a baseline bundle for each fish:

- canonical CSVs
- ROI-centric outputs
- HCR-centric outputs
- registration overlays
- key QA images
- final figures
- run metadata
- package/notebook revision information when available

Define comparison classes:

- exact match required: filenames, required columns, table keys, row counts
- numerically close: distances, overlaps, registration scores, AUC, BPI
- visually equivalent: overlays, QA images, figures
- allowed to differ with explanation: known stale-state artifacts or corrected notebook ambiguity

## Phase 1: Define Stage Contracts

Represent the pipeline as explicit stages:

1. `audit-inputs`
2. `preprocess-functional`
3. `preprocess-anatomy`
4. `preprocess-hcr`
5. `register-functional-to-anatomy`
6. `register-hcr-to-anatomy`
7. `match-roi-to-anatomy`
8. `assign-hcr-identity`
9. `score-activity-bpi`
10. `export-canonical-tables`
11. `make-qa-report`
12. `make-figures`

For each stage, define:

- input files and upstream stage products
- output files
- required metadata
- parameters
- QA checks
- pass/warn/fail conditions
- canonical versus diagnostic status
- stale-output detection rules

Keep table semantics owned by the stage that writes the table.

## Phase 2: Add Manifests and State Tracking

Every stage should write a manifest, preferably JSON or YAML.

Each manifest should include:

- fish ID
- stage name
- stage version
- code/package revision when available
- input paths
- input hashes or timestamps
- parameter values
- output paths
- upstream manifests consumed
- QA status
- warnings and errors

The pipeline should be able to report:

- this output is current
- this output is stale because an upstream input changed
- this stage used older parameters than a downstream stage
- this figure was built from the current canonical table
- this output cannot be trusted until a named upstream stage is rerun

This is the main guard against notebook-style hidden state.

## Phase 3: Move Logic Into Package-Owned Stages

New processing logic should live in `src/codeants_2pf_hcr/`.

CLI wrappers in `tools/` may orchestrate stages, parse arguments, and expose commands, but they should not own business logic.

Notebook cells should not be used as the source of truth for new behavior. Consult the old notebook only when package context is insufficient to preserve current behavior for comparison.

Prefer extracting or reusing existing package-owned wrappers before reimplementing behavior.

## Phase 4: Recreate Outputs Stage By Stage

Do not attempt a full rewrite in one pass.

Suggested migration order:

1. input audit
2. preprocessing manifests
3. Suite2p ROI inventory
4. functional reference generation
5. anatomy preprocessing
6. HCR mask discovery
7. registration output inventory
8. ROI-to-anatomy matching
9. identity assignment
10. activity and BPI scoring
11. canonical table export
12. QA reports and figures

For each stage:

1. identify the old notebook output for the control fish
2. run the new stage
3. compare outputs
4. fix differences or document intentional differences
5. add focused contract tests before moving downstream

Migrate table-producing stages before figure-producing stages. Figures should consume tables rather than rebuild identity, activity, or matching.

## Phase 5: Add Contract Tests

Contract tests should catch semantic drift without requiring full interactive notebook runs.

Minimum checks:

- expected files exist
- required columns exist
- canonical table keys are unique
- `functional_roi_activity_identity.csv` has one row per functional ROI
- ROI-centric outputs are not silently replaced by HCR-centric outputs
- geometry fields exist before identity/activity annotation
- identity fields are assigned after geometry matching
- response and BPI fields are added after matching
- low-quality Suite2p traces are retained but marked response-unavailable under current policy
- figure builders use canonical tables and subset filters rather than recomputing semantics

Control-fish comparisons should include:

- row counts
- unmatched ROI counts
- identity counts per gene
- response class counts
- BPI category counts
- key numeric deltas
- missing or extra files
- visual QA thumbnails

## Phase 6: Create Biologist-Facing QA Reports

Create read-only review notebooks or generated reports that load staged outputs.

Possible review surfaces:

1. `01_single_fish_input_QA.ipynb`
2. `02_single_fish_registration_QA.ipynb`
3. `03_single_fish_matching_QA.ipynb`
4. `04_single_fish_activity_identity_summary.ipynb`

These review notebooks should help answer:

- Did the expected data load?
- Are the functional traces usable?
- Are stimulus logs aligned to imaging frames and sessions?
- Does the anatomy preparation look correct?
- Did registration work?
- Which matches are ambiguous?
- Which ROIs are molecularly identified?
- Which identified cells are responsive?

They should not write canonical tables.

## Phase 7: Run Old and New Paths In Parallel

For a validation period, process selected fish both ways:

1. old notebook output
2. new pipeline output
3. comparison report

Comparison reports should distinguish:

- exact matches
- tolerated numeric differences
- visual equivalence
- documented intentional changes
- regressions
- missing validation coverage

The new path should not be promoted because it "looks plausible"; it should be promoted because it reproduces or explicitly improves the old path with evidence.

## Phase 8: Promote The New Pipeline

After enough control fish pass comparison:

- mark the staged pipeline as canonical
- keep the old notebook archived as an internal control
- stop adding new logic to the old notebook
- use notebooks only for review, exploration, and figure inspection
- require staged pipeline outputs for cohort analyses

## Proposed CLI Shape

The CLI should be thin and stage-oriented, for example:

```text
codeants single-fish audit-inputs --fish FISH_ID
codeants single-fish preprocess-functional --fish FISH_ID
codeants single-fish preprocess-anatomy --fish FISH_ID
codeants single-fish preprocess-hcr --fish FISH_ID
codeants single-fish register --fish FISH_ID
codeants single-fish match-geometry --fish FISH_ID
codeants single-fish assign-identity --fish FISH_ID
codeants single-fish score-activity --fish FISH_ID
codeants single-fish export-tables --fish FISH_ID
codeants single-fish report --fish FISH_ID
codeants single-fish run-all --fish FISH_ID
```

Each command should declare what it reads and writes, then emit a manifest.

## Data Introduction Order

Preferred order for future pipeline and QA:

1. metadata and file inventory
2. functional data and Suite2p traces
3. stimulus/frame/session alignment
4. anatomy preparation
5. HCR/confocal object discovery
6. functional-to-anatomy registration
7. HCR-to-anatomy registration
8. ROI-to-anatomy geometry matching
9. HCR identity assignment
10. response and BPI annotation
11. table export
12. reports and figures

This order catches metadata and trace-quality failures early, before expensive or interpretive downstream work.

## QA Gates To Add Earlier

Raw data audit:

- expected files exist
- dimensions are plausible
- metadata fields are present
- fish/session IDs are consistent

Stimulus and frame audit:

- logs match imaging frame counts
- sessions map to the correct global planes
- rest blocks are represented without stimulus spans

Suite2p audit:

- ROI counts per plane
- Suite2p cell counts per plane
- non-empty traces
- trace/frame dimensions align

Anatomy preparation audit:

- orientation is explicit
- scaling/resizing happens once
- old derived files are detected as stale

Registration audit:

- per-plane overlay images
- numeric registration scores
- pass/warn/fail status
- fallback provenance when a backend changes

Matching ambiguity report:

- unmatched ROIs
- multiple plausible matches
- suspicious distance or overlap values
- one-to-one assignment conflicts

Canonical table audit:

- expected columns
- unique biological keys
- correct row unit
- no silent ROI-centric/HCR-centric path mixing

## Non-Goals During Migration

- Do not rewrite `notebooks/singleFish.ipynb`.
- Do not change canonical output filenames unless a migration phase explicitly approves it.
- Do not use activity, BPI, or gene identity to decide geometry.
- Do not make figures authoritative for identity or response semantics.
- Do not substitute HCR-centric exports for ROI-centric whole-population tables.
- Do not move business logic into `tools/` wrappers.

## Success Criteria

The migration is successful when:

- the new pipeline can run without opening a notebook
- each stage writes a manifest and QA status
- stale upstream state is detectable before downstream interpretation
- the new pipeline recreates current control-fish outputs or reports intentional differences
- canonical tables preserve their expected row units and semantics
- review notebooks/reports are useful to a biologist but not required to process data
- cohort analyses can consume the staged single-fish outputs confidently
