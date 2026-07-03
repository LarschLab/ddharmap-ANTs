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
PYTHONPATH=src python tools/single_fish_pipeline.py audit-hcr-activity-replay --fish-id FISH_ID --local-root DATA_ROOT --strict
PYTHONPATH=src python tools/single_fish_pipeline.py audit-score-activity-bpi --fish-id FISH_ID --local-root DATA_ROOT --strict
PYTHONPATH=src python tools/single_fish_pipeline.py score-activity-bpi --fish-id FISH_ID --local-root DATA_ROOT --strict --force-recompute
PYTHONPATH=src python tools/single_fish_pipeline.py export-canonical-tables --fish-id FISH_ID --local-root DATA_ROOT --strict --hash-files
PYTHONPATH=src python tools/single_fish_pipeline.py make-qa-report --fish-id FISH_ID --local-root DATA_ROOT --strict --hash-files
PYTHONPATH=src python tools/single_fish_pipeline.py make-figures --fish-id FISH_ID --local-root DATA_ROOT --strict --hash-files
PYTHONPATH=src python tools/single_fish_pipeline.py freeze-legacy-baseline --fish-id FISH_ID --local-root DATA_ROOT --strict --overwrite
PYTHONPATH=src python tools/single_fish_pipeline.py compare-legacy-baseline --fish-id FISH_ID --local-root DATA_ROOT --strict
PYTHONPATH=src python tools/single_fish_pipeline.py compare-staged --fish-id FISH_ID --local-root DATA_ROOT --strict
```

Later writer stages, preprocessing comparisons, and non-declared comparison groups remain roadmap targets. The current `compare-staged` command is read-only and post-preprocessing only: it compares declared staged CSVs by header/row-count shape, declared row keys, exact semantic cells, declared numeric cells, and figure dimensions/thumbnails where available. `freeze-legacy-baseline` and `compare-legacy-baseline` now provide a first frozen-bundle surface for the same declared post-processing output specs.

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
- `compare-staged` is read-only and uses the same declared post-preprocessing stage output specs; it checks CSV shape, declared row keys, exact semantic cells, and declared numeric cells where table semantics are known, while preserving byte-identical CSV comparison as warn-only. Declared numeric checks currently use `1e-5` absolute tolerance, calibrated against accepted `L395_f11` BPI control-vs-staged differences. Figure checks include dependency-free PNG dimensions plus optional Pillow thumbnail MAE/RMS warnings. It does not create/freeze baseline bundles.
- `audit-score-activity-bpi` is a separate read-only recompute audit. It calls `activity.build_response_bpi_tables` with `precomputed_scored_bpi_df=None`, requires unscored ROI trace-quality inputs (`activity_class`, `is_active`), records Suite2p/stimulus/session provenance inputs, and compares recomputed in-memory activity/BPI tables to control CSVs with the same declared key/exact/numeric semantics. It writes no staged CSV outputs.
- `audit-hcr-activity-replay` is a separate read-only replay audit for HCR-centric `[50]` identified-cell activity tables. It calls `matching.build_hcr_activity_tables` from staged `plane_refs_summary.json`, Suite2p, anatomy labels, and staged HCR final pairs, applies the notebook-equivalent functional orientation while rebuilding Suite2p labels, uses the ROI master only as response lookup through `matching.hcr_response_lookup_from_roi_master_df`, overlays selected accepted `ants_rigid_affine` transformlists when present, checks AntsPyx (`ants`) availability and transform-file coverage, scores selected-ANTs and persisted affine-CSV transform variants in memory, finalizes via `matching.finalize_hcr_activity_export_tables`, compares row/key counts to accepted HCR outputs when present, writes no staged HCR CSV outputs, and keeps promotion disabled.
- `assign-hcr-identity` is now a recompute writer stage for the identity/HCR bundle. It requires staged ROI/anatomy geometry under `match-roi-to-anatomy/registration/`, staged functional/anatomy `plane_refs_summary.json`, Suite2p, anatomy labels, and staged HCR/anatomy artifacts under `register-hcr-to-anatomy/confocal/aligned/` before writing into `--pipeline-root/assign-hcr-identity/registration`. It recomputes `anatomy_identity_lookup.csv` from staged HCR final-pair CSVs using the accepted gene order, recomputes `functional_roi_activity_identity.csv` by attaching the lookup to staged ROI/anatomy geometry while preserving pass-through trace-quality/response fields, recomputes `hcr_activity_status.csv`, `conf_to_func_pairs_raw.csv`, `conf_to_func_pairs.csv`, and `hcr_func_candidates.csv` through the label-first HCR replay, regenerates `hcr_activity_status_summary.csv` from recomputed status plus HCR warp metadata, refuses overwrite without `--force-recompute`, checks that ROI/anatomy inputs remain geometry-only, and compares the recomputed CSVs against accepted controls.
- `score-activity-bpi` is a writer stage. It calls `activity.build_response_bpi_tables` with `precomputed_scored_bpi_df=None`, writes `functional_roi_activity_identity.csv`, `functional_roi_activity_bpi_cells.csv`, and `functional_roi_activity_bpi_summary.csv` under `--pipeline-root/score-activity-bpi/registration` or the fish default pipeline output root, and refuses to overwrite existing staged score CSVs unless `--force-recompute` is set.
- By default, `score-activity-bpi` consumes the staged `assign-hcr-identity` ROI master; use `--identity-input-path` only for controlled validation/bootstrap when the upstream staged identity writer is not yet available in the chosen output root.
- `export-canonical-tables` is a writer stage. It copies the three ROI/BPI canonical CSVs from staged `score-activity-bpi/registration/` and the five HCR/identity/activity CSVs from staged `assign-hcr-identity/registration/`, or from explicit `--score-input-root` / `--hcr-input-root` bootstrap roots. It writes the 8 CSV canonical export bundle under `--pipeline-root/export-canonical-tables/registration` or the fish default pipeline output root, and refuses to overwrite existing staged canonical CSVs unless `--force-recompute` is set.
- `make-qa-report` is a generated report writer stage. It requires staged canonical export CSV inputs, writes a compact Markdown report and JSON summary under `--pipeline-root/make-qa-report/`, and refuses to overwrite existing staged report artifacts unless `--force-recompute` is set. This first report writer summarizes staged artifacts for review; richer biologist-facing HTML/PDF/notebook reports remain later work.
- `make-figures` is a mixed render/baseline writer stage. It requires staged canonical export CSV inputs plus declared `[56i]` AUC CSV inputs, renders the 50l composite, responsive identity donut, and HCR anatomy coexpression summary, copies the remaining declared legacy figure PNG artifacts from the fish `04_plots` folder or an explicit `--figure-input-root`, writes them under `--pipeline-root/make-figures/04_plots`, and refuses to overwrite existing staged figure outputs unless `--force-recompute` is set. Replacing the remaining copied figures remains later figure-specific promotion work.
- On `L395_f11`, dependency-aware `assign-hcr-identity --strict` now runs after recomputed staged `match-roi-to-anatomy`, staged `register-functional-to-anatomy`, and staged `register-hcr-to-anatomy` in one temporary `--pipeline-root`: it uses the default staged dependency roots, sees 4,530 ROI/anatomy geometry rows, verifies the ROI/anatomy table is geometry-only, sees 4 staged HCR/anatomy label TIFFs and 162 accepted final-pair rows, recomputes a 137-row `anatomy_identity_lookup.csv` from staged final-pair CSVs with accepted control parity, recomputes a 4,530-row ROI identity master with accepted keyed/exact/numeric parity, and recomputes HCR activity outputs with `hcr_activity_status.csv=162`, `conf_to_func_pairs_raw.csv=254`, `conf_to_func_pairs.csv=40`, `hcr_func_candidates.csv=148`, and `hcr_activity_status_summary.csv=19`. Strict row/key/exact/numeric checks pass; byte parity warnings remain expected for recomputed CSVs.
- On `L395_f11`, `audit-hcr-activity-replay --strict` now runs from a current-worktree copy on `linnaeus` after staged five-plane `prepare-functional-reference-stacks`, `register-functional-to-anatomy`, and `register-hcr-to-anatomy` into `/tmp/codeants-hcr-replay-abzbAl/staged-L395`. The audit completed with status `pass`, zero errors, zero warnings, and promotion disabled. The selected-ANTs replay reports selected `ants_rigid_affine` transformlists for all five replay planes, `ants_available=True`, zero missing transform files, and anatomy XY spacing `0.5964024861653645`; after applying notebook-equivalent functional orientation during Suite2p label reconstruction, it matches accepted HCR outputs exactly: `hcr_activity_status.csv=162/162`, `conf_to_func_pairs_raw.csv=254/254`, `conf_to_func_pairs.csv=40/40`, `hcr_func_candidates.csv=148/148`, and candidate-key parity missing `0`, extra `0`. The in-manifest variant scoreboard still evaluates accepted `registration/tforms_by_plane.csv`, accepted `ncc/tforms_by_plane.csv`, selected-best-Z variants, and p4 `dx=-1,dy=+1` offset variants for diagnostic context.
- On `L395_f11`, `audit-score-activity-bpi --strict` now passes from a current-worktree copy on `linnaeus`: the real recompute path uses Suite2p/stimulus inputs, row/key coverage matches, BPI values match within `1e-5`, and exact/numeric checks pass for the scored ROI master, BPI cells, and BPI summary. The pass depends on the `[50ia]`/control `zero_band=0.50` and identity-neutral BPI helper rows.
- On `L395_f11`, `score-activity-bpi --strict` now runs from a current-worktree copy on `linnaeus` with a temporary `--pipeline-root` and explicit control identity input: it writes all three staged score CSVs, uses the real experiment-log recompute path, has zero failed checks, and reports only two CSV byte-parity warnings for ROI master/BPI cells.
- On `L395_f11`, `export-canonical-tables --strict` now runs from a current-worktree copy on `linnaeus` with a temporary `--pipeline-root`, a freshly generated staged score root, and an explicit control HCR input root: it writes all 8 staged canonical CSVs, has zero failed checks, and reports only two CSV byte-parity warnings for the scored ROI master and BPI cells.
- On `L395_f11`, generated `make-qa-report --strict` now runs from a current-worktree copy on `linnaeus` with a temporary `--pipeline-root` seeded by staged canonical exports: it writes `qa_report.md` and `qa_report_summary.json`, and `compare-staged --stage-name make-qa-report` passes with zero failed or warning checks.
- On `L395_f11`, baseline `make-figures --strict` now runs from a current-worktree copy on `linnaeus` with a temporary `--pipeline-root` seeded by staged canonical exports: it writes all 5 staged figure PNGs and `compare-staged --stage-name make-figures` passes with zero failed or warning checks.
- On `L395_f11`, `/tmp/codeants-fig-composite-xjyda6` validated the promoted `compound_50j_56i_unified.png` render path from accepted canonical CSVs plus accepted `[56i]` AUC CSV inputs. `make-figures --strict` wrote all 5 PNGs with zero errors and only the pre-existing responsive-identity donut thumbnail warning; `compare-staged --stage-name make-figures` reported zero failed checks and zero warning entries. Visual verification confirmed the rendered 50l composite was nonblank, populated, and had separated donut callouts after label-stacking repair.
- On `L395_f11`, the full staged post-geometry chain now runs from one current-worktree copy on `linnaeus` with a temporary `--pipeline-root`: recomputed `match-roi-to-anatomy`, `register-hcr-to-anatomy`, dependency-aware `assign-hcr-identity`, `audit-score-activity-bpi`, `score-activity-bpi`, `export-canonical-tables`, `make-qa-report`, and `make-figures`. The chain wrote staged outputs through figures with zero failed checks; `compare-staged` passed for assign and report, warned only for score/export CSV byte parity, and warned only for the rendered responsive-identity donut thumbnail in figures.
- `prepare-functional-reference-stacks` has focused local contract and CLI coverage plus `linnaeus` real-data validation on `L395_f11`. It wraps `[12]`, discovers `02_reg/00_preprocessing/2p_functional/02_motionCorrected/*mcorrected*.tif`, resolves polarity, writes legacy-compatible raw/norm functional reference TIFF pairs under `--pipeline-root/prepare-functional-reference-stacks/functional/raw/` by default, supports repeatable `--functional-stack-path` and explicit `--output-dir`, and emits valid JSON on stdout. The validated run wrote `L395_f11_plane0_mcorrected_flipX_ref_raw.tif` and `L395_f11_plane0_mcorrected_flipX_ref_norm.tif` from one real `L395_f11` motion-corrected input under `/tmp/codeants-func-refs-n5H1wp/staged-L395/...`.
- `prepare-in-vivo-anatomy-stack` has focused local contract and CLI coverage plus `linnaeus` real-data validation on `L765_f02`. It wraps `[14a]`, defaults to in vivo source discovery that excludes ex vivo-looking files, applies metadata-driven polarity, writes the canonical registration-ready in vivo anatomy NRRD/JSON, verifies uint8 plus `750x750` Y/X output, refuses existing outputs without `--force-recompute`, and emits valid JSON on stdout. The validated run wrote to `/tmp/codeants-stream-check-sUzWQC/staged-L765/...`, selected `/Volumes/dataDrive/dataProcessing/2p_processing/L765_f02/01_raw/2p/anatomy/L765_f02_anatomy_00001.tif`, produced shape `[76, 750, 750]`, and kept the `tifffile` reshape warning on stderr.
- `register-functional-to-anatomy` has focused local contract and CLI coverage plus `linnaeus` real-data validation on `L395_f11`. It reconstructs in-memory `plane_refs` from staged functional reference TIFFs, consumes the prepared in vivo anatomy NRRD, runs NCC best-z/scale search and NCC-only in-plane comparison, writes NCC caches, comparison/recommendation CSVs, warped reference TIFFs, `plane_refs_summary.json`, and a staged `registration/tforms_by_plane.csv`. When anatomy labels and anatomy-space functional labels are available, it now also emits a 200 px center-crop functional/anatomy outline overlay QA PNG/CSV under the stage `qa/` folder. The validated run wrote to `/tmp/codeants-register-func-lki3lJ/staged-L395/register-functional-to-anatomy/` from one real `L395_f11` reference input and reported best_z `124` with `216` NCC scores for `L395_f11_plane0_mcorrected_flipX`. Full `register-functional-to-anatomy` ANTs parity remains later work, while downstream ROI/anatomy matching now consumes selected accepted ANTs transformlists when present.
- `register-hcr-to-anatomy` has focused local contract and CLI coverage plus `linnaeus` real-data validation on `L395_f11`. The default validated mode stages accepted small aligned HCR artifacts from `03_analysis/confocal/aligned/` into `--pipeline-root/register-hcr-to-anatomy/confocal/aligned/`: aligned label/QC TIFFs, match/review/final-pair CSVs, and warp metadata JSONs. It records but does not copy multi-GB aligned intensity NRRDs. Its manifest also reports whether the current fish has the direct-ANTs recompute prerequisites: matching metadata route, raw HCR Cellpose masks, rbest/rn HCR NRRDs, rbest-to-2p transforms, and rn-to-rbest transforms. The opt-in `--recompute-direct-ants` path now applies the notebook `label_voxel_floor_v3` prewarp HCR label filter, recomputes HCR label TIFFs, warp metadata, and HCR/anatomy match/review/final-pair CSVs from raw HCR masks plus current direct ANTs transforms, and gates the result with strict final-pair key parity against accepted controls. On real `L395_f11`, `/tmp/codeants-hcr-filter-nooverlay-w1yksx` recomputed 4 filtered HCR label TIFFs plus 4 metadata JSONs and 12 match/review/final-pair CSVs with zero register-stage errors or warnings, recomputed filter stats `4/4`, accepted filter-stat overlay count `0`, and final-pair key parity passed for all four masks (`73`, `18`, `9`, and `62` accepted pairs). The earlier `/tmp/codeants-hcr-filter-t86kC0` root fed staged `register-functional-to-anatomy`, `match-roi-to-anatomy`, `assign-hcr-identity`, `score-activity-bpi`, and `export-canonical-tables`; functional registration and ROI/anatomy matching passed, and downstream identity/score/export stages had zero errors with expected byte-parity warnings only.
- `match-roi-to-anatomy` has focused local contract and CLI coverage plus `linnaeus` real-data validation on `L395_f11`. The promoted recompute mode consumes staged `register-functional-to-anatomy` plane refs, Suite2p, anatomy labels, selected accepted `ants_rigid_affine` in-plane transformlists, anatomy XY spacing, and notebook-equivalent functional orientation before writing `functional_roi_anatomy_matches.csv`, `functional_roi_anatomy_match_by_plane.csv`, and `functional_roi_anatomy_match_plane_meta.csv` under `--pipeline-root/match-roi-to-anatomy/registration`. The validated recompute run wrote to `/tmp/codeants-match-recompute-promote-RrFHvB/staged-L395/...`, reported `mode=recompute_from_staged_registration`, overlaid 5 selected ANTs transformlists with 0 missing transform files, wrote 4,530 geometry rows across 5 planes, found 3,724 unique anatomy matches, and passed accepted-control key, label, and unique-match parity (`4530/4530`). Control-geometry `--source-root` staging remains available for bootstrap/diagnostic use.
- `prepare-ex-vivo-anatomy-stack` has focused local contract coverage with a real tiny TIFF input: it writes isolated structural ex vivo NRRD/JSON outputs from explicit source/output paths, reuses cached outputs without `--force-recompute`, and emits CLI JSON. Real-data validation should use a fish with ex vivo anatomy input, not `L395_f11`.
- `segment-hcr-cellpose` and `segment-ex-vivo-anatomy-cellpose` have focused local manifest-contract coverage for missing model/input failure paths. `segment-hcr-cellpose` also covers the cached-mask reuse path where existing masks let the stage pass without requiring a model.
- On the mounted `linnaeus` data root, `L765_f02` currently fails structurally for ex vivo/Cellpose validation: no ex vivo source is discoverable under `01_raw/2p/anatomy`, the manual-oriented ex vivo NRRD is absent under `02_reg/00_preprocessing/2p_anatomy/ex_vivo/`, and `02_reg/00_preprocessing/rbest` is absent. These failures are now reported as JSON manifests. Positive `L765_f02` Cellpose validation still points to the Helga NAS workflow unless those inputs are staged onto `linnaeus`.
- Existing diagnostic folders such as `assign-hcr-identity/recompute-audit/` remain historical baseline evidence only; `anatomy_identity_lookup.csv`, ROI identity master recomputation, and HCR activity table replay are now stage-owned for the validated `L395_f11` path.

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
- `compound_50j_56i_unified.*` is regenerated from staged canonical CSVs plus `motion_auc_plot_points.csv` and `motion_auc_plot_counts.csv`.
- `bpi_all_pairs.png` and `per_gene_stimulus_trace_with_hcr_status_56h.png` are still copied from legacy `04_plots` until their full package-owned render inputs are staged.

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
