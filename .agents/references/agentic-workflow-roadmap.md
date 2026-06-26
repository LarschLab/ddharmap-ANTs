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
- `tools/single_fish_pipeline.py` exposes `contracts`, dry-run `audit-inputs`, read-only `status`, read-only `stage-status`, and read-only post-preprocessing `compare-staged`; `--write-manifest` writes only manifests, not staged outputs.
- Read-only `audit-inputs` and `status` passed on `L395_f11` from `linnaeus` with strict mode after deeper processed-control, semantic, value-domain, and staged-parity checks.
- `audit-inputs --write-manifest` and subsequent `status` passed on `L395_f11` from `linnaeus`; the persisted audit manifest reports `current`.
- `stage-status --write-manifest` and subsequent `status` passed on `L395_f11` from `linnaeus` for `assign-hcr-identity`, `score-activity-bpi`, `export-canonical-tables`, and `make-figures`; downstream persisted manifests report `current`.
- `compare-staged` is runnable for the same four existing post-preprocessing staged output folders and remains read-only.
- `L395_f11` existing staged outputs are the first baseline/control for staged parity checks.
- Preprocessing is out of scope for the current roadmap slice; assume preprocessing has already completed before this staged audit/status surface runs.

## Broken Or Missing

- The default `audit-inputs` command does not write manifests to disk by design; `--write-manifest` is the explicit opt-in for persisting the audit manifest.
- Downstream writer stages after `audit-inputs` are declarative contracts only; existing post-preprocessing staged output folders now have read-only `stage-status` inventory/manifests.
- `compare-staged` compares only declared existing post-preprocessing staged outputs; preprocessing comparisons, legacy-baseline commands, numeric tolerance reports, and writer stages remain missing.
- `status` summarizes the current dry-run trust state and can report missing/current/stale/invalid persisted `audit-inputs` manifests.
- Stale-state reporting covers persisted `audit-inputs` inputs and persisted read-only downstream stage manifests. Full dependency/output freshness for future writer stages is not implemented yet.
- The current input audit confirms key processed-control artifacts across Suite2p, anatomy preprocessing, HCR masks, registration tables, BPI/activity tables, final plots, and optional staged output folders.
- `L395_f11` lacks non-sidecar raw functional files under `01_raw/2p/functional` and lacks real files under `03_analysis/functional/registration/reference_planes`; both are optional in the dry-run audit.
- Stage-specific semantic validation covers Suite2p plane completeness, transform row count, core CSV row presence, required schemas, key/plane consistency, value domains, response/selection consistency, geometry-before-identity nullability, and optional staged-vs-control row/header parity for existing staged outputs.

## Next Slice

Validate and harden the read-only workflow surface:

1. Keep existing `L395_f11` staged outputs as the first comparison control.
2. Extend writer-stage behavior only after each downstream stage has declared inputs/outputs in tests and read-only status passes on control data.
3. Start the next read-only stage audit only after its expected inputs/outputs are declared in tests.
4. Do not start preprocessing migration in this slice; test preprocessing later on a separate dataset.

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

Latest read-only compare-staged evidence:

```text
ssh linnaeus 'cd ~/gitRepo/codeANTs-agentic-workflow && PYTHONPATH=src python3 tools/single_fish_pipeline.py compare-staged --fish-id L395_f11 --local-root /Volumes/dataDrive/dataProcessing/2p_processing --strict --write-manifest'
aggregate status: warn
failed checks: 0
assign-hcr-identity: warn, 4 CSV byte-parity warnings
score-activity-bpi: warn, 2 CSV byte-parity warnings
export-canonical-tables: warn, 6 CSV byte-parity warnings
make-figures: pass
```

## Open Questions

- Should the first manifest schema be strict JSON only, or allow a later YAML/report layer for human review?
- How much downstream stale-output detection belongs in `audit-inputs` versus later stage-specific `status` checks?
- Which additional fish should verify these value-level invariants before writer stages are enabled?
