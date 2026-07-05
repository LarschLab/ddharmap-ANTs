# Agentic Workflow Roadmap

**Purpose:** living status board for the mixed agentic workflow migration.

**Use this file when:** starting or resuming work on the staged single-fish pipeline, agent-facing commands, manifests, provenance, validation, or notebook-to-pipeline migration.

## Update Rule

Update this file after every meaningful workflow change, including design-only changes, implementation slices, validation runs, discovered breakage, or decisions that affect the next slice.

Keep entries compact and current. Move deep details into the owning reference doc, code, tests, manifests, or recent-changes log, then link them here.

## Current Direction

Build a conservative hybrid workflow where agents and users invoke explicit package-owned stages through thin CLI wrappers, each stage records provenance and validation state, and notebooks remain interpretation/QC surfaces.

The immediate priority is to harden the durable workflow surface before broad behavior changes. First-pass commands must stay read-only/dry-run so they can inspect real fish folders without writing or overwriting staged outputs. Manifest persistence is allowed only through explicit opt-in flags.

1. Define the stage and manifest contract.
2. Implement a minimal read-only `audit-inputs` stage.
3. Expose it through `tools/single_fish_pipeline.py`.
4. Add contract tests.
5. Use the existing `L395_f11` staged outputs as the first baseline/control for parity checks before enabling any staged output writer.

## Working

- Current workflow branch is `codex/agentic-workflow-hybrid`; this branch may be intentionally dirty while staged pipeline migration work is in progress.
- Existing package modules already own many notebook stage behaviors under `src/codeants_2pf_hcr/`.
- `single-fish-pipeline-roadmap.md` defines the target stage order and migration strategy.
- `notebooks/singleFish.ipynb` remains the reference/control path during migration.
- Scientific ordering is stable: geometry first, identity second, activity/BPI third, reports and figures last.
- `src/codeants_2pf_hcr/pipeline.py` defines first-pass stage contracts, read-only path resolution, labeled manifest records, semantic check records, and a dry-run `audit-inputs` stage.
- `src/codeants_2pf_hcr/pipeline.py` also defines explicit audit-manifest persistence helpers and persisted-vs-current manifest comparison.
- `tools/single_fish_pipeline.py` exposes `contracts`, dry-run `audit-inputs`, read-only `status`, read-only `stage-status`, read-only post-preprocessing `compare-staged`, read-only `audit-score-activity-bpi`, read-only `audit-hcr-activity-replay`, and writers `assign-hcr-identity`, `score-activity-bpi`, `export-canonical-tables`, `make-qa-report`, and baseline `make-figures`; `--write-manifest` writes manifests, while writer commands write staged outputs under `--pipeline-root` or the fish default pipeline output root.
- Read-only `audit-inputs` and `status` passed on `L395_f11` from `linnaeus` with strict mode after deeper processed-control, semantic, value-domain, and staged-parity checks.
- `audit-inputs --write-manifest` and subsequent `status` passed on `L395_f11` from `linnaeus`; the persisted audit manifest reports `current`.
- `stage-status --write-manifest` and subsequent `status` passed on `L395_f11` from `linnaeus` for `assign-hcr-identity`, `score-activity-bpi`, `export-canonical-tables`, and `make-figures`; downstream persisted manifests report `current`.
- `compare-staged` is runnable for the same four existing post-preprocessing staged output folders and remains read-only; CSV comparisons now include declared keyed-row, exact-cell, and numeric-cell checks where table semantics are known, while byte-identical CSV parity remains warn-only. Figure comparison now checks PNG dimensions without optional image libraries and adds thumbnail MAE/RMS checks when Pillow is available.
- `audit-score-activity-bpi` is a read-only recompute audit for `score-activity-bpi`; it requires unscored ROI trace-quality inputs (`activity_class`, `is_active`), calls `activity.build_response_bpi_tables` with `precomputed_scored_bpi_df=None`, and compares recomputed in-memory tables to control CSVs without writing staged outputs.
- `audit-hcr-activity-replay` is a read-only replay audit for HCR-centric `[50]` identified-cell activity tables with local and `L395_f11` real-data validation. It loads staged `plane_refs_summary.json`, Suite2p, anatomy labels, staged HCR final pairs, and a response-aware ROI master used only as lookup, overlays selected accepted `ants_rigid_affine` transformlists when available, checks AntsPyx (`ants`) availability/transform coverage, runs a read-only transform-variant scoreboard for selected ANTs plus persisted affine CSV variants, then runs `matching.build_hcr_activity_tables` plus `matching.finalize_hcr_activity_export_tables` in memory and compares row/key counts to accepted HCR outputs without writing staged HCR CSVs or enabling promotion.
- `assign-hcr-identity` is now a package-owned hybrid writer stage with focused local and `L395_f11` real-data validation. It requires staged ROI/anatomy geometry, staged functional/anatomy plane refs, Suite2p, anatomy labels, and staged HCR/anatomy artifacts, recomputes `anatomy_identity_lookup.csv` from staged HCR final-pair CSVs using the accepted gene order, recomputes `functional_roi_activity_identity.csv` by attaching that lookup to staged ROI/anatomy geometry while carrying through trace-quality/response fields, recomputes the HCR-centric activity/status/candidate CSV family through the label-first replay, regenerates `hcr_activity_status_summary.csv`, refuses overwrite without `--force-recompute`, and checks geometry-before-identity plus accepted HCR final-pair and accepted-control parity.
- The response-aware HCR activity export finalizer from notebook `[50]` is package-owned as `matching.finalize_hcr_activity_export_tables` and is wired into both `audit-hcr-activity-replay` and `assign-hcr-identity`. On `L395_f11`, the selected-ANTs replay has exact accepted-control row and candidate-key parity for `hcr_activity_status.csv`, `conf_to_func_pairs_raw.csv`, `conf_to_func_pairs.csv`, and `hcr_func_candidates.csv`; byte differences remain warning-only for recomputed staged CSVs.
- `score-activity-bpi` is now a package-owned writer stage. It defaults to the staged `assign-hcr-identity` ROI master, accepts an explicit `--identity-input-path` for controlled validation/bootstrap, refuses to overwrite existing staged score CSVs without `--force-recompute`, and hard-codes `precomputed_scored_bpi_df=None`.
- `export-canonical-tables` is now a package-owned writer stage with focused local and `L395_f11` real-data validation. It assembles the 8 canonical registration CSVs from staged score outputs for ROI/BPI tables and staged or explicit HCR/identity roots for HCR-centric tables, and refuses to overwrite existing staged canonical CSVs without `--force-recompute`.
- `make-qa-report` is now a package-owned generated report writer stage with focused local and `L395_f11` real-data validation. It requires staged canonical export CSV inputs, writes `qa_report.md` and `qa_report_summary.json` under `make-qa-report/`, includes a manual review checklist plus inline previews for existing QA/figure PNG artifacts, and refuses overwrite without `--force-recompute`.
- `make-figures` is now a package-owned mixed render/baseline writer stage. It requires staged canonical export CSV inputs plus declared `[56i]` AUC CSV inputs, renders `compound_50j_56i_unified.*`, `bpi_all_pairs.*`, `single_fish_50l_responsive_identity_donut.*`, and `single_fish_hcr_anatomy_coexpression_summary.*`, copies the remaining declared legacy trace figure PNG under `make-figures/04_plots`, and refuses overwrite without `--force-recompute`.
- On `L395_f11`, `/tmp/codeants-fig-composite-xjyda6` validated the promoted 50l composite render path: `make-figures --strict` wrote all five PNG outputs with zero errors, `compare-staged --stage-name make-figures` had zero failed checks and zero warning entries, and visual verification confirmed the rendered `compound_50j_56i_unified.png` was nonblank, populated, and no longer had overlapping donut callouts.
- On `L395_f11`, `/tmp/codeants-bpi-render-uwqf59` validated the promoted `bpi_all_pairs.png` render path from accepted canonical BPI/identity CSV inputs. `make-figures --strict` wrote all five PNG outputs with zero errors; `compare-staged --stage-name make-figures` had zero failed checks and warning-only BPI visual/dimension drift plus the pre-existing responsive-identity donut thumbnail warning. Visual verification confirmed the rendered BPI diagnostic was nonblank, populated, and had readable four-panel layout.
- On `L395_f11`, `/tmp/codeants-qa-review-F3A65E` validated the richer Markdown QA report: `make-qa-report --strict` and `compare-staged --stage-name make-qa-report` passed, and after staged figures were present the report contained 9 review artifacts, 5 inline image previews, a manual review checklist, and a visual artifact preview section.
- The full staged post-geometry chain has now been validated on `L395_f11` from one temporary `linnaeus` pipeline root: `match-roi-to-anatomy`, `register-hcr-to-anatomy`, dependency-aware `assign-hcr-identity`, `audit-score-activity-bpi`, `score-activity-bpi`, `export-canonical-tables`, `make-qa-report`, `make-figures`, and `compare-staged` for assign/score/export/report/figures all completed with zero failed checks. Remaining warnings were expected score/export CSV byte-parity warnings and one rendered responsive-identity donut thumbnail warning.
- `prepare-ex-vivo-anatomy-stack` has focused local contract coverage as the first upstream preprocessing writer slice. It writes isolated ex vivo structural NRRD/JSON outputs, supports explicit source/output paths, reuses cache without force, and emits CLI JSON.
- `prepare-functional-reference-stacks` has focused local contract coverage plus `linnaeus` real-data validation on `L395_f11` as the first functional preprocessing writer. It wraps the package-owned `[12]` functional reference builder, discovers motion-corrected stacks, writes legacy-compatible raw/norm reference TIFF pairs under the staged pipeline root, supports explicit source/output paths, and emits clean CLI JSON on stdout.
- `prepare-in-vivo-anatomy-stack` has focused local contract coverage plus `linnaeus` real-data validation as the first canonical in vivo anatomy preparation writer. It wraps the package-owned `[14a]` signed-anatomy uint8/NRRD preparation, supports explicit source/output paths, defaults to metadata-driven source discovery that excludes ex vivo-looking files, and emits clean CLI JSON on stdout.
- `register-functional-to-anatomy` has focused local contract coverage plus `linnaeus` real-data validation on `L395_f11` as the first functional-to-anatomy registration writer. It reconstructs lightweight `plane_refs` from staged functional reference TIFFs, runs the package-owned NCC best-z/scale search and NCC-only in-plane comparison, writes NCC caches, warped-reference artifacts, `plane_refs_summary.json`, and a staged `tforms_by_plane.csv`.
- `register-hcr-to-anatomy` has focused local contract coverage plus `linnaeus` real-data validation on `L395_f11` as the first HCR-to-anatomy registration writer. It stages accepted small aligned HCR artifacts from `03_analysis/confocal/aligned/` into `register-hcr-to-anatomy/confocal/aligned/`, validates label/match/final-pair/warp-metadata presence plus final-pair schema and accepted-pair semantics, records large aligned intensity NRRDs as inputs, does not copy multi-GB NRRDs into staged roots, and reports direct-ANTs recompute readiness from current raw HCR masks, rbest/rn HCR NRRDs, matching metadata, and transform files. The opt-in `--recompute-direct-ants` path now applies the notebook `label_voxel_floor_v3` prewarp HCR label filter, recomputes HCR label TIFFs, warp metadata, and HCR/anatomy match/review/final-pair CSVs from raw masks plus current ANTs transforms, and passes strict final-pair key parity on `L395_f11`.
- `match-roi-to-anatomy` has focused local contract coverage plus `linnaeus` real-data validation on `L395_f11` as the first ROI/anatomy geometry writer. The promoted recompute mode consumes staged `register-functional-to-anatomy` plane refs, Suite2p, anatomy labels, selected accepted `ants_rigid_affine` in-plane transformlists, anatomy XY spacing, and notebook-equivalent functional orientation before writing geometry-only ROI/anatomy match tables under `match-roi-to-anatomy/registration/`; accepted-control key, anatomy-label, selected-label, and unique-match parity pass exactly.
- `segment-hcr-cellpose` and `segment-ex-vivo-anatomy-cellpose` have focused local manifest-contract coverage for dependency/input failure paths; HCR segmentation also covers cached-mask reuse without requiring a model.
- The granular Cellpose CLI now supports `--no-gpu` for CPU validation fallback while preserving GPU as the default.
- `L395_f11` existing staged outputs are the first baseline/control for staged parity checks.
- Generic preprocessing is no longer entirely out of scope: concrete granular writers now exist for in vivo anatomy preparation, ex vivo anatomy preparation, and Cellpose segmentation surfaces. The generic roadmap contract still assumes post-preprocessing inputs for `audit-inputs`/`status`/downstream comparison until each upstream writer is promoted and validated.

## Broken Or Missing

- The default `audit-inputs` command does not write manifests to disk by design; `--write-manifest` is the explicit opt-in for persisting the audit manifest.
- The current post-preprocessing staged folders all have writer surfaces. `assign-hcr-identity` now recomputes the anatomy identity lookup, ROI identity master, HCR activity/status/candidate CSV family, and HCR activity status summary from staged dependencies; `score-activity-bpi` recomputes activity/BPI; `export-canonical-tables` assembles staged canonical CSVs; `make-qa-report` generates a Markdown/JSON review report with a manual review checklist and existing image previews; and `make-figures` renders four package-owned final figures while still copying the declared legacy per-gene trace figure PNG. Upstream preprocessing/registration/matching writers now include focused functional-reference, in vivo/ex vivo anatomy, Cellpose, NCC functional-to-anatomy, direct-HCR-to-anatomy recompute, and ROI/anatomy recompute slices. Remaining gaps include richer HTML/PDF/notebook-style biologist-facing reports, broader preprocessing/upstream freshness reporting, full functional-to-anatomy ANTs parity beyond the current NCC/accepted-transform hybrid, broader positive Cellpose/ex vivo validation, and replacement of the remaining copied trace figure artifact with a package-rendered output.
- `compare-staged` compares declared existing post-preprocessing staged outputs with CSV shape checks, declared keyed-row/exact-cell/numeric-cell checks, and PNG dimension checks. `freeze-legacy-baseline` and `compare-legacy-baseline` now cover frozen bundles for those declared post-processing outputs; preprocessing comparisons, full biologist-facing visual review reports, and most writer stages remain missing.
- `audit-score-activity-bpi` remains read-only; `score-activity-bpi` promotes recomputed activity/BPI tables into staged CSV outputs. `export-canonical-tables` promotes staged score/HCR registration CSVs into the canonical export bundle but still depends on explicit HCR/control roots until the upstream identity writer is promoted.
- `status` summarizes the current dry-run trust state and can report missing/current/stale/invalid persisted `audit-inputs` manifests.
- Stale-state reporting covers persisted `audit-inputs` inputs, persisted read-only downstream stage manifests, warning-level freshness checks for existing downstream staged outputs, and dependency freshness checks for existing staged roots that were previously optional/bootstrap inputs. `stage-status` and top-level `status` now warn when required stage outputs are older than required declared inputs or existing upstream staged dependencies; broader preprocessing/upstream freshness coverage remains a later expansion.
- The current input audit confirms key processed-control artifacts across Suite2p, anatomy preprocessing, HCR masks, registration tables, BPI/activity tables, final plots, and optional staged output folders.
- `L395_f11` lacks non-sidecar raw functional files under `01_raw/2p/functional` and lacks real files under `03_analysis/functional/registration/reference_planes`; both are optional in the dry-run audit.
- `L765_f02` on the mounted `linnaeus` data root lacks the ex vivo input stack/manual-oriented ex vivo NRRD and the `02_reg/00_preprocessing/rbest` directory needed for positive ex vivo/HCR Cellpose validation; the writer commands now report these as structured manifest failures instead of tracebacks.
- Stage-specific semantic validation covers Suite2p plane completeness, transform row count, core CSV row presence, required schemas, key/plane consistency, value domains, response/selection consistency, geometry-before-identity nullability, and optional staged-vs-control row/header parity for existing staged outputs.

## Next Slice

Continue after the direct-HCR recompute and HCR activity promotion coverage:

1. Keep existing `L395_f11` staged outputs as the first comparison control.
2. For ex vivo/Cellpose validation, use Helga/NAS for `L765_f02` unless the missing ex vivo/rbest inputs are copied onto the mounted `linnaeus` data root.
3. With ROI identity recompute, HCR activity replay/promotion, direct-HCR label warp recompute, ROI/anatomy recompute, the 50l composite and BPI all-pairs figure renders, and downstream/dependency freshness warnings promoted, the next `linnaeus`-validated slice should either replace the remaining copied per-gene trace figure artifact with a package renderer, broaden preprocessing/upstream freshness coverage, or broaden biologist-facing QA around the validated registration/matching surfaces.
4. Add local contract tests first, then validate real data with a temporary `--pipeline-root` and compare/status commands where applicable.

## Decisions

- Do not use notebooks as the source of truth for new workflow state.
- Do not use `tools/` as business-logic authority.
- Do not claim documented scaffold commands are runnable until the missing package and CLI files exist.
- First-pass real-data commands are read-only/dry-run and should print JSON to stdout instead of writing under fish folders.
- `audit-inputs --write-manifest` may write `03_analysis/functional/pipeline_manifests/audit-inputs_manifest.json`; this remains a dry-run audit of stage outputs, not a staged output writer.
- Existing `L395_f11` staged outputs are the first baseline/control; do not freeze a separate baseline bundle for this slice.
- Current staged workflow work assumes preprocessing has already been done.
- Future writer-stage work should avoid generic executable names like `preprocess-anatomy` or `preprocess-hcr`; use concrete operation names such as `prepare-ex-vivo-anatomy-stack`, `segment-ex-vivo-anatomy-cellpose`, and `segment-hcr-cellpose`, with ex vivo structural artifacts isolated under `03_analysis/structural/ex_vivo/`.
- Keep the roadmap operational and short; use `single-fish-pipeline-roadmap.md` for the longer migration design.
- Prefer additive pipeline files over editing notebook cells for the first slice.

## Validation Plan

Minimum validation for the first slice:

```text
PYTHONPATH=src pytest -q tests/test_agent_docs.py
PYTHONPATH=src pytest -q tests/test_pipeline.py
PYTHONPATH=src python tools/single_fish_pipeline.py contracts
PYTHONPATH=src python tools/single_fish_pipeline.py audit-inputs --fish-id FISH_ID --local-root DATA_ROOT --strict
PYTHONPATH=src python tools/single_fish_pipeline.py audit-inputs --fish-id FISH_ID --local-root DATA_ROOT --strict --write-manifest
PYTHONPATH=src python tools/single_fish_pipeline.py status --fish-id FISH_ID --local-root DATA_ROOT --strict
PYTHONPATH=src python tools/single_fish_pipeline.py stage-status --fish-id FISH_ID --local-root DATA_ROOT --strict --stage-name STAGE_NAME
PYTHONPATH=src python tools/single_fish_pipeline.py compare-staged --fish-id FISH_ID --local-root DATA_ROOT --strict --stage-name STAGE_NAME
PYTHONPATH=src python tools/single_fish_pipeline.py audit-score-activity-bpi --fish-id FISH_ID --local-root DATA_ROOT --strict
```

Use `L395_f11` only after the command surface and manifest tests pass on synthetic paths, and keep the real-data run read-only.

Latest real-data dry-run evidence:

```text
ssh linnaeus 'cd ~/gitRepo/codeANTs-agentic-workflow && PYTHONPATH=src python3 tools/single_fish_pipeline.py audit-inputs --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict'
status: pass
manifest input records: 64
semantic checks: 71 passing
metadata CSV count: 1 metadata + 1 experiment log
Suite2p core files: 10 each for ops/F/Fneu/iscell/stat
tforms rows vs Suite2p planes: 5 vs 5
ROI identity rows: 4530
HCR activity status rows: 162
responsive HCR/function pairs: 40
schema checks: pass for tforms, ROI identity, BPI cells/summary, HCR status, responsive pairs, and HCR candidates
cross-table checks: pass for Suite2p/tforms/ROI/BPI plane consistency, ROI/BPI key equality, HCR candidate key subset, and responsive-pair key subset
value checks: pass for geometry-before-identity flags, response domains, BPI numeric values, HCR trace-export selection, and responsive pair selection
staged parity: pass for 8 staged canonical CSV row/header comparisons and 5 staged figure presence checks
HCR Cellpose masks: 4
staged canonical export CSVs: 8
staged figure outputs: 12
known optional absences: raw functional files, functional reference-plane files
outputs written: none
```

Latest persisted-manifest evidence:

```text
ssh linnaeus 'cd ~/gitRepo/codeANTs-agentic-workflow && PYTHONPATH=src python3 tools/single_fish_pipeline.py audit-inputs --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict --write-manifest && PYTHONPATH=src python3 tools/single_fish_pipeline.py status --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict'
status: pass
persisted audit manifest: current
input records: 64
semantic checks: 71 passing
stale records: 0
warnings/errors: none
```

Latest downstream stage-status evidence:

```text
ssh linnaeus 'cd ~/gitRepo/codeANTs-agentic-workflow && for stage in assign-hcr-identity score-activity-bpi export-canonical-tables make-figures; do PYTHONPATH=src python3 tools/single_fish_pipeline.py stage-status --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict --stage-name $stage --write-manifest; done && PYTHONPATH=src python3 tools/single_fish_pipeline.py status --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict'
assign-hcr-identity: pass, 7 outputs, 14 checks
score-activity-bpi: pass, 3 outputs, 6 checks
export-canonical-tables: pass, 8 outputs, 16 checks
make-figures: pass, 5 outputs, 10 checks
top-level status: pass
downstream persisted manifests: current
```

Latest local comparison-contract evidence:

```text
PYTHONPATH=src pytest -q tests/test_activity.py tests/test_pipeline.py tests/test_agent_docs.py tests/test_package_exports.py
87 passed in 5.12s
```

Coverage includes keyed row reordering, missing/extra keyed rows, duplicate keys, exact-cell mismatches, strict vs non-strict handling, numeric tolerance failures, non-numeric declared numeric values, preserved byte-parity warnings, readable PNG dimension checks, Pillow-backed thumbnail checks, read-only `audit-score-activity-bpi` guardrails against scored-only/fake recompute inputs, baseline writer `assign-hcr-identity` guardrails for identity/HCR artifact staging, overwrite refusal, and CLI JSON output, writer `score-activity-bpi` guardrails for custom output roots, explicit identity inputs, no precomputed shortcut, overwrite refusal, and CLI exposure, writer `export-canonical-tables` guardrails for missing upstream roots, staged assign default source use, score/HCR source precedence, explicit HCR roots, overwrite refusal, and CLI JSON output, writer `make-qa-report` guardrails for missing canonical exports, Markdown/JSON report generation, overwrite refusal, compare-staged support, and CLI JSON output, baseline writer `make-figures` guardrails for missing canonical exports, figure artifact staging, overwrite refusal, and CLI JSON output, `prepare-ex-vivo-anatomy-stack` guardrails for explicit source/output paths, structural ex vivo output placement, cached-output reuse, and CLI JSON output, plus segmentation writer guardrails for manifest failure handling and cached HCR mask reuse.

Latest real-data read-only compare-staged evidence:

```text
rsync current worktree to /tmp/codeants-validate-* on linnaeus, then:
PYTHONPATH=src python3 tools/single_fish_pipeline.py compare-staged --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict
aggregate status: warn
failed checks: 0
assign-hcr-identity: warn, 27 checks, 0 failed, 4 CSV byte-parity warnings
score-activity-bpi: warn, 17 checks, 0 failed, 2 CSV byte-parity warnings
export-canonical-tables: warn, 37 checks, 0 failed, 6 CSV byte-parity warnings
make-figures: warn, 10 checks, 0 failed, 1 dimension warning
```

Numeric checks use an absolute tolerance of `1e-5`; `L395_f11` BPI control-vs-staged differences reached `4.32679e-06`, consistent with formatting/rounding-level drift rather than semantic divergence.
The real-data make-figures warning is `bpi_all_pairs.png` dimension drift (`control` 3000x1800, `staged` 4209x1454), with all other declared figure dimensions matching.

Latest real-data read-only `audit-score-activity-bpi` evidence:

```text
rsync current worktree to /tmp/codeants-validate-audit-score-* on linnaeus, then:
PYTHONPATH=src /Users/ddharmap/gitRepo/LLM/.venv/bin/python tools/single_fish_pipeline.py audit-score-activity-bpi --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict
status: pass
dependency check: pass for pandas/numpy after narrowing activity/Suite2p imports away from notebook/image dependencies
raw ROI input check: pass; control ROI master contains activity_class and is_active
true recompute path: pass; stim_source is the L395_f11 experiment log, not precomputed
row/key checks: pass for scored ROI master, ROI activity/BPI cells, and BPI summary
BPI numeric checks: pass for ROI master and BPI cells; max_abs_diff=4.32679e-06
exact/numeric checks: pass for scored ROI master, ROI activity/BPI cells, and BPI summary
status: pass
```

Latest real-data writer `score-activity-bpi` evidence:

```text
rsync current worktree to /tmp/codeants-score-writer-SbYYLr on linnaeus, then:
PYTHONPATH=src /Users/ddharmap/gitRepo/LLM/.venv/bin/python tools/single_fish_pipeline.py score-activity-bpi --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict --pipeline-root /tmp/codeants-score-writer-SbYYLr/staged-L395 --identity-input-path /Volumes/dataDrive/dataProcessing/2p_processing/L395_f11/03_analysis/functional/registration/functional_roi_activity_identity.csv
status: warn
errors: []
outputs: 3 staged score CSVs under /tmp/codeants-score-writer-SbYYLr/staged-L395/score-activity-bpi/registration
true recompute path: pass; stim_source is the L395_f11 experiment log, not precomputed
row/key/exact/numeric checks: pass for scored ROI master, ROI activity/BPI cells, and BPI summary
warnings: CSV byte parity differs for ROI master and BPI cells only

PYTHONPATH=src /Users/ddharmap/gitRepo/LLM/.venv/bin/python tools/single_fish_pipeline.py compare-staged --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict --pipeline-root /tmp/codeants-score-writer-SbYYLr/staged-L395 --stage-name score-activity-bpi
status: warn
failed checks: 0
warning checks: 2 CSV byte-parity warnings
```

Latest real-data writer `export-canonical-tables` evidence:

```text
rsync current worktree to /tmp/codeants-export-Dy53Yf on linnaeus, then:
PYTHONPATH=src /Users/ddharmap/gitRepo/LLM/.venv/bin/python tools/single_fish_pipeline.py score-activity-bpi --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict --pipeline-root /tmp/codeants-export-Dy53Yf/staged-L395 --identity-input-path /Volumes/dataDrive/dataProcessing/2p_processing/L395_f11/03_analysis/functional/registration/functional_roi_activity_identity.csv
status: warn
errors: []
outputs: 3 staged score CSVs
warnings: 2 CSV byte-parity warnings

PYTHONPATH=src /Users/ddharmap/gitRepo/LLM/.venv/bin/python tools/single_fish_pipeline.py export-canonical-tables --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict --pipeline-root /tmp/codeants-export-Dy53Yf/staged-L395 --hcr-input-root /Volumes/dataDrive/dataProcessing/2p_processing/L395_f11/03_analysis/functional/registration
status: warn
errors: []
outputs: 8 staged canonical CSVs
warnings: 2 CSV byte-parity warnings for staged scored ROI master and BPI cells

PYTHONPATH=src /Users/ddharmap/gitRepo/LLM/.venv/bin/python tools/single_fish_pipeline.py compare-staged --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict --pipeline-root /tmp/codeants-export-Dy53Yf/staged-L395 --stage-name export-canonical-tables
status: warn
failed checks: 0
warning checks: 2 CSV byte-parity warnings
```

Latest real-data mixed-render writer `make-figures` evidence:

```text
rsync current worktree to /tmp/codeants-makefig-render-fh3fjg/codeANTs on linnaeus, then run assign/score/export/make chain into /tmp/codeants-makefig-render-fh3fjg/staged-L395:

PYTHONPATH=src /Users/ddharmap/gitRepo/LLM/.venv/bin/python tools/single_fish_pipeline.py make-figures --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict --pipeline-root /tmp/codeants-makefig-render-fh3fjg/staged-L395
status: warn
errors: []
warnings: 1 visual thumbnail warning from rendered responsive identity donut
outputs: 5 staged figure PNGs
rendered: single_fish_50l_responsive_identity_donut.png, single_fish_hcr_anatomy_coexpression_summary.png
legacy-copied: compound_50j_56i_unified.png, bpi_all_pairs.png, per_gene_stimulus_trace_with_hcr_status_56h.png

PYTHONPATH=src /Users/ddharmap/gitRepo/LLM/.venv/bin/python tools/single_fish_pipeline.py compare-staged --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict --pipeline-root /tmp/codeants-makefig-render-fh3fjg/staged-L395 --stage-name make-figures
status: warn
failed checks: 0
warning checks: 1 visual thumbnail warning from rendered responsive identity donut
```

Latest real-data declared legacy-baseline evidence:

```text
rsync current worktree to /tmp/codeants-legacy-baseline-B8ASDO/codeANTs on linnaeus, symlink /Volumes/dataDrive/dataProcessing/2p_processing/L395_f11 into /tmp/codeants-legacy-baseline-B8ASDO/local-root/L395_f11, then run assign/score/export/make/freeze/compare into temp roots:

PYTHONPATH=src /Users/ddharmap/gitRepo/LLM/.venv/bin/python tools/single_fish_pipeline.py freeze-legacy-baseline --fish-id L395_f11 --local-root /tmp/codeants-legacy-baseline-B8ASDO/local-root --strict --pipeline-root /tmp/codeants-legacy-baseline-B8ASDO/staged-L395 --stage-name make-figures
status: pass
errors: []
warnings: []
copied outputs: 5
baseline_root: /tmp/codeants-legacy-baseline-B8ASDO/local-root/pipeline_baselines/L395_f11/legacy_singleFish

PYTHONPATH=src /Users/ddharmap/gitRepo/LLM/.venv/bin/python tools/single_fish_pipeline.py compare-legacy-baseline --fish-id L395_f11 --local-root /tmp/codeants-legacy-baseline-B8ASDO/local-root --strict --pipeline-root /tmp/codeants-legacy-baseline-B8ASDO/staged-L395 --stage-name make-figures
status: warn
failed checks: 0
warning checks: 1 visual thumbnail warning from rendered responsive identity donut
```

Historical real-data baseline writer `make-figures` evidence:

```text
rsync current worktree to /tmp/codeants-figures-hV4SBO on linnaeus, then run score/export/make chain into /tmp/codeants-figures-hV4SBO/staged-L395:

PYTHONPATH=src /Users/ddharmap/gitRepo/LLM/.venv/bin/python tools/single_fish_pipeline.py make-figures --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict --pipeline-root /tmp/codeants-figures-hV4SBO/staged-L395
status: pass
errors: []
warnings: []
outputs: 5 staged figure PNGs

PYTHONPATH=src /Users/ddharmap/gitRepo/LLM/.venv/bin/python tools/single_fish_pipeline.py compare-staged --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict --pipeline-root /tmp/codeants-figures-hV4SBO/staged-L395 --stage-name make-figures
status: pass
failed checks: 0
warning checks: 0
```

Latest real-data baseline writer `assign-hcr-identity` and downstream chain evidence:

```text
rsync current worktree to /tmp/codeants-assign-5G6XJ1 on linnaeus, then run assign/score/export/make chain into /tmp/codeants-assign-5G6XJ1/staged-L395 without explicit HCR bootstrap roots:

PYTHONPATH=src /Users/ddharmap/gitRepo/LLM/.venv/bin/python tools/single_fish_pipeline.py assign-hcr-identity --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict --pipeline-root /tmp/codeants-assign-5G6XJ1/staged-L395
status: pass
errors: []
warnings: []
outputs: 7 staged identity/HCR CSVs

PYTHONPATH=src /Users/ddharmap/gitRepo/LLM/.venv/bin/python tools/single_fish_pipeline.py score-activity-bpi --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict --pipeline-root /tmp/codeants-assign-5G6XJ1/staged-L395
status: warn
errors: []
warnings: 2 CSV byte-parity warnings
outputs: 3 staged score CSVs

PYTHONPATH=src /Users/ddharmap/gitRepo/LLM/.venv/bin/python tools/single_fish_pipeline.py export-canonical-tables --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict --pipeline-root /tmp/codeants-assign-5G6XJ1/staged-L395
status: warn
errors: []
warnings: 2 CSV byte-parity warnings
outputs: 8 staged canonical CSVs

PYTHONPATH=src /Users/ddharmap/gitRepo/LLM/.venv/bin/python tools/single_fish_pipeline.py make-figures --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict --pipeline-root /tmp/codeants-assign-5G6XJ1/staged-L395
status: pass
errors: []
warnings: []
outputs: 5 staged figure PNGs

Strict compare-staged results:
assign-hcr-identity: pass, 0 failed checks, 0 warnings
export-canonical-tables: warn, 0 failed checks, 2 CSV byte-parity warnings
make-figures: pass, 0 failed checks, 0 warnings
```

Latest real-data generated writer `make-qa-report` evidence:

```text
rsync current worktree to /tmp/codeants-qa-report-8lRb2t on linnaeus, then run assign/score/export/report/make chain into /tmp/codeants-qa-report-8lRb2t/staged-L395:

PYTHONPATH=src /Users/ddharmap/gitRepo/LLM/.venv/bin/python tools/single_fish_pipeline.py make-qa-report --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict --pipeline-root /tmp/codeants-qa-report-8lRb2t/staged-L395
status: pass
errors: []
warnings: []
outputs: 2 staged QA report artifacts

PYTHONPATH=src /Users/ddharmap/gitRepo/LLM/.venv/bin/python tools/single_fish_pipeline.py compare-staged --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict --pipeline-root /tmp/codeants-qa-report-8lRb2t/staged-L395 --stage-name make-qa-report
status: pass
failed checks: 0
warning checks: 0

qa_report_summary.json records 8 canonical tables and 4 post-preprocessing stage summaries.
```

This pass required aligning `ActivityConfig.zero_band` to the notebook/control `[50ia]` default (`0.50`) and keeping `functional_roi_activity_bpi_cells.csv` identity-neutral. The authoritative ROI master still carries anatomy identity; the BPI cells table remains a response/BPI diagnostic helper.

Latest upstream writer `prepare-in-vivo-anatomy-stack` evidence:

```text
rsync current worktree to /tmp/codeants-stream-check-sUzWQC/codeANTs on linnaeus, then run into a temporary pipeline root:

PYTHONPATH=src /Users/ddharmap/miniforge3/bin/python tools/single_fish_pipeline.py prepare-in-vivo-anatomy-stack --fish-id L765_f02 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict --pipeline-root /tmp/codeants-stream-check-sUzWQC/staged-L765 --output-path /tmp/codeants-stream-check-sUzWQC/staged-L765/prepare-in-vivo-anatomy-stack/2p_anatomy/L765_f02_anatomy_2P_GCaMP.nrrd --force-recompute
status: pass
stdout: valid JSON only
stderr: tifffile reshape warning only
selected input: /Volumes/dataDrive/dataProcessing/2p_processing/L765_f02/01_raw/2p/anatomy/L765_f02_anatomy_00001.tif
output shape: [76, 750, 750]
source shape: [76, 512, 512]
polarity: north from 2026-05-12-1329_fL765_f02_metadata.csv:fish_orientation
```

Latest upstream writer `prepare-functional-reference-stacks` evidence:

```text
rsync current worktree to /tmp/codeants-func-refs-n5H1wp/codeANTs on linnaeus, then run one real L395_f11 motion-corrected stack into a temporary pipeline root:

PYTHONPATH=src /Users/ddharmap/miniforge3/bin/python tools/single_fish_pipeline.py prepare-functional-reference-stacks --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict --pipeline-root /tmp/codeants-func-refs-n5H1wp/staged-L395 --functional-stack-path /Volumes/dataDrive/dataProcessing/2p_processing/L395_f11/02_reg/00_preprocessing/2p_functional/02_motionCorrected/L395_f11_plane0_mcorrected.tif --force-recompute
status: pass
stdout: valid JSON only
stderr: empty
outputs: L395_f11_plane0_mcorrected_flipX_ref_raw.tif and L395_f11_plane0_mcorrected_flipX_ref_norm.tif under /tmp/codeants-func-refs-n5H1wp/staged-L395/prepare-functional-reference-stacks/functional/raw/
checks: 1 reference plane, 1 raw TIFF, 1 normalized TIFF
polarity: south from matchingMetadata.csv:polarity
```

Latest registration writer `register-functional-to-anatomy` evidence:

```text
rsync current worktree to /tmp/codeants-register-func-lki3lJ/codeANTs on linnaeus, then run one real L395_f11 functional reference prep followed by NCC-only registration into a temporary pipeline root:

PYTHONPATH=src /Users/ddharmap/miniforge3/bin/python tools/single_fish_pipeline.py register-functional-to-anatomy --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict --pipeline-root /tmp/codeants-register-func-lki3lJ/staged-L395 --anatomy-stack-path /Volumes/dataDrive/dataProcessing/2p_processing/L395_f11/02_reg/00_preprocessing/2p_anatomy/L395_f11_anatomy_2P_GCaMP.nrrd --no-cv2 --force-recompute
status: pass
stdout: valid JSON only
stderr: empty
outputs: ncc_scale_by_fish.json, ncc_bestz_by_plane.json, inplane_registration_comparison.csv, inplane_registration_recommendation.csv, one NCC warped reference TIFF, plane_refs_summary.json, and registration/tforms_by_plane.csv under /tmp/codeants-register-func-lki3lJ/staged-L395/register-functional-to-anatomy/
checks: 1 functional reference input, NCC best-z cache, in-plane comparison CSV, one-row tforms table, plane refs summary
summary first plane: label L395_f11_plane0_mcorrected_flipX, index 0, scale 1.0, best_z 124, ncc_scores_count 216, tform_src ncc_xy, active method ncc_xy
```

## Open Questions

- Should the first manifest schema be strict JSON only, or allow a later YAML/report layer for human review?
- How much downstream stale-output detection belongs in `audit-inputs` versus later stage-specific `status` checks?
- Which additional fish should verify these value-level invariants before writer stages are enabled?
