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

## Stage Naming Rule

Do not use generic executable names such as `preprocess-anatomy` or `preprocess-hcr` for writer commands when the command performs only one concrete operation. In this workflow, preprocessing includes raw audits, stack conversion, NRRD formatting, orientation, functional extraction, Suite2p/stimulus handling, anatomy preparation, HCR intensity discovery, segmentation, and registration preparation. Generic names hide the actual side effect and make manifests ambiguous.

Use broad names only as roadmap groupings. Executable writer stages should name the operation and modality, for example:

- `prepare-ex-vivo-anatomy-stack`: convert/orient the raw ex vivo 2P anatomy stack into a registration-ready analysis artifact.
- `segment-ex-vivo-anatomy-cellpose`: run Cellpose on the prepared ex vivo anatomy stack.
- `segment-hcr-cellpose`: run Cellpose on HCR/confocal intensity stacks from an explicit source such as `rbest`.

Ex vivo analysis artifacts belong under `03_analysis/structural/ex_vivo/` and must not be mixed with canonical in vivo anatomy outputs. The earlier `02_reg/00_preprocessing/2p_anatomy/ex_vivo/` location is legacy/bridge context for registration preparation; new ex vivo Cellpose masks and manifests should live under the structural analysis ex vivo subtree.

For ex vivo segmentation, distinguish pre-rotation/pre-manual stack preparation from segmentation-ready anatomy. If a manual-oriented NRRD exists, `segment-ex-vivo-anatomy-cellpose` should consume that file explicitly via `--anatomy-stack-path`; do not silently regenerate and segment the raw or pre-rotation stack. For `L765_f02`, the intended source is:

```text
02_reg/00_preprocessing/2p_anatomy/ex_vivo/L765_f02_exvivo_anatomy_2P_GCaMP_uint8_manual_oriented.nrrd
```

## Helga NAS Credential Workflow

Helga is the preferred workstation for CUDA Cellpose segmentation. The NAS share is:

```text
\\nasdcsr.unil.ch\RECHERCHE\FAC\FBM\CIG\jlarsch
```

When a job needs personal NAS credentials, use a headful SSH terminal so the user can enter the password directly. The username to use is `danin.dharmaperwira@unil.ch`. Do not extract stored credentials from macOS Keychain, do not store the password on Helga, and do not put credentials in scripts, repo files, logs, or shell history.

Use a session-local mapping (`/persistent:no`) and run the NAS-dependent job in the same SSH session:

```text
net use Y: \\nasdcsr.unil.ch\RECHERCHE\FAC\FBM\CIG\jlarsch /user:danin.dharmaperwira@unil.ch * /persistent:no
```

Then delete the temporary mapping before exit:

```text
net use Y: /delete /yes
```

`tools/helga_nas_session_test.bat` is the minimal test script for this pattern. Future Helga Cellpose job scripts should follow the same shape: prompt, verify `Y:\default\D2c\07_Data`, run all NAS-dependent commands before the session ends, then unmap `Y:`.

## Migration Strategy

Use a parallel "strangler" migration.

The original `notebooks/singleFish.ipynb` stays untouched. The new pipeline grows beside it, stage by stage, and is compared against frozen notebook outputs for representative fish.

During migration, both paths may exist:

- old notebook path: internal control and behavior reference
- new staged path: future canonical processing route

Promotion happens only after the new path consistently recreates canonical outputs or produces documented, intentional differences.

## Phase 0: Use The First Control Outputs

For the current migration slice, use the existing `L395_f11` staged outputs as the first control/baseline. Do not freeze a separate baseline bundle yet.

Preprocessing is intentionally out of scope for this slice. Assume the fish folder already has completed preprocessing, Suite2p, registration, identity/activity tables, staged output folders, and plots before running the staged audit/status surface. Preprocessing should be tested later on a separate dataset.

The currently runnable CLI surface is:

```text
PYTHONPATH=src python tools/single_fish_pipeline.py contracts
PYTHONPATH=src python tools/single_fish_pipeline.py audit-inputs --fish-id FISH_ID --local-root DATA_ROOT --strict
PYTHONPATH=src python tools/single_fish_pipeline.py audit-inputs --fish-id FISH_ID --local-root DATA_ROOT --strict --write-manifest
PYTHONPATH=src python tools/single_fish_pipeline.py status --fish-id FISH_ID --local-root DATA_ROOT --strict
PYTHONPATH=src python tools/single_fish_pipeline.py stage-status --fish-id FISH_ID --local-root DATA_ROOT --strict --stage-name STAGE_NAME
PYTHONPATH=src python tools/single_fish_pipeline.py compare-staged --fish-id FISH_ID --local-root DATA_ROOT --strict --stage-name STAGE_NAME
```

Future stages should still compare against frozen/control outputs for representative fish before promotion. A later baseline bundle may include:

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

Target scaffold, not all currently runnable:

```text
PYTHONPATH=src python tools/single_fish_pipeline.py audit-inputs --fish-id FISH_ID --local-root DATA_ROOT --strict
PYTHONPATH=src python tools/single_fish_pipeline.py prepare-functional-reference-stacks --fish-id FISH_ID --local-root DATA_ROOT --strict --hash-files --run-23c
PYTHONPATH=src python tools/single_fish_pipeline.py prepare-in-vivo-anatomy-stack --fish-id FISH_ID --local-root DATA_ROOT --strict --hash-files
PYTHONPATH=src python tools/single_fish_pipeline.py prepare-ex-vivo-anatomy-stack --fish-id FISH_ID --local-root DATA_ROOT --strict --hash-files
PYTHONPATH=src python tools/single_fish_pipeline.py segment-ex-vivo-anatomy-cellpose --fish-id FISH_ID --local-root DATA_ROOT --strict --hash-files
PYTHONPATH=src python tools/single_fish_pipeline.py segment-hcr-cellpose --fish-id FISH_ID --local-root DATA_ROOT --strict --hcr-source rbest --hash-files
PYTHONPATH=src python tools/single_fish_pipeline.py register-functional-to-anatomy --fish-id FISH_ID --local-root DATA_ROOT --strict --hash-files
PYTHONPATH=src python tools/single_fish_pipeline.py register-hcr-to-anatomy --fish-id FISH_ID --local-root DATA_ROOT --strict --hash-files
PYTHONPATH=src python tools/single_fish_pipeline.py match-roi-to-anatomy --fish-id FISH_ID --local-root DATA_ROOT --strict --hash-files
PYTHONPATH=src python tools/single_fish_pipeline.py assign-hcr-identity --fish-id FISH_ID --local-root DATA_ROOT --strict --hash-files
PYTHONPATH=src python tools/single_fish_pipeline.py score-activity-bpi --fish-id FISH_ID --local-root DATA_ROOT --strict --hash-files
PYTHONPATH=src python tools/single_fish_pipeline.py export-canonical-tables --fish-id FISH_ID --local-root DATA_ROOT --strict --hash-files
PYTHONPATH=src python tools/single_fish_pipeline.py make-qa-report --fish-id FISH_ID --local-root DATA_ROOT --strict --hash-files
PYTHONPATH=src python tools/single_fish_pipeline.py make-figures --fish-id FISH_ID --local-root DATA_ROOT --strict --hash-files
PYTHONPATH=src python tools/single_fish_pipeline.py freeze-legacy-baseline --fish-id FISH_ID --local-root DATA_ROOT --strict --overwrite
PYTHONPATH=src python tools/single_fish_pipeline.py compare-legacy-baseline --fish-id FISH_ID --local-root DATA_ROOT --strict
PYTHONPATH=src python tools/single_fish_pipeline.py compare-staged --fish-id FISH_ID --local-root DATA_ROOT --strict
```

Later writer stages, preprocessing comparisons, legacy-baseline commands, and non-declared comparison groups remain roadmap targets. The current `compare-staged` command is read-only and post-preprocessing only: it compares declared staged CSVs by header/row-count shape, warns on CSV byte differences, and checks declared figures for control presence plus non-empty staged files.

By default, frozen legacy bundles are written under:

```text
DATA_ROOT/pipeline_baselines/FISH_ID/legacy_singleFish/
```

The baseline bundle manifest pair is:

```text
baseline_manifest.json
comparison_manifest.json
```

Future stage-owned legacy comparison reports may be written under the relevant staged output folder, for example:

```text
FISH_ID/03_analysis/functional/pipeline_outputs/preprocess-functional/preprocess-functional_legacy_comparison.csv
FISH_ID/03_analysis/functional/pipeline_manifests/compare-staged-preprocess-functional_manifest.json
```

Current downstream staged-output status:

- `stage-status` is read-only. It inventories existing post-preprocessing staged output folders for `assign-hcr-identity`, `score-activity-bpi`, `export-canonical-tables`, and `make-figures`.
- These stage-status manifests declare required outputs and compare accepted staged CSV/file shapes against the existing `L395_f11` control outputs when present.
- `compare-staged` is read-only and uses the same declared post-preprocessing stage output specs; it does not create/freeze baseline bundles.
- This support does not execute, recompute, promote, or render downstream stages. Writer stages and broader comparison behavior remain roadmap targets.
- Existing diagnostic folders such as `assign-hcr-identity/recompute-audit/` remain baseline evidence only until a package-owned writer stage is implemented and validated.

Path contract for these CLI examples: `--local-root` is the directory that directly contains fish folders. For Danin/Microscopy NAS data, use `Y:\default\D2c\07_Data\Danin\Microscopy` on Helga, not the broader `Y:\default\D2c\07_Data` root.

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

Current `make-figures` status:

- `single_fish_50l_responsive_identity_donut.*` is regenerated from `functional_roi_activity_identity.csv` and `conf_to_func_pairs.csv` into `03_analysis/functional/pipeline_outputs/make-figures/04_plots/`.
- `single_fish_hcr_anatomy_coexpression_summary.*` is regenerated from `hcr_activity_status.csv` into the same staged figure folder.
- `compound_50j_56i_unified.*`, `bpi_all_pairs.png`, and `per_gene_stimulus_trace_with_hcr_status_56h.png` are still copied from legacy `04_plots` until their full package-owned render inputs are staged.

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
codeants single-fish prepare-functional-reference-stacks --fish FISH_ID
codeants single-fish prepare-in-vivo-anatomy-stack --fish FISH_ID
codeants single-fish prepare-ex-vivo-anatomy-stack --fish FISH_ID
codeants single-fish segment-ex-vivo-anatomy-cellpose --fish FISH_ID
codeants single-fish segment-hcr-cellpose --fish FISH_ID --hcr-source rbest
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
