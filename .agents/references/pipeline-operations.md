# Pipeline Operations

**Purpose:** route tasks that run, replay, verify or export a fish, as opposed to editing code.

**Use this file when:** preparing or launching a single-fish run, rerunning after a code change, reading a run result, building review HTML, or finding where run outputs live.

## Choose the run type

| Situation | Run type | Read |
| --- | --- | --- |
| New fish, or registration/geometry code changed | fresh (`native_production.mode = "fresh"`): no frozen-writer reuse; declared writers recompute on a new run, and resume skips current stages | `docs/single-fish-unattended-job.md` |
| Only scoring or later code changed (response rules, identity, exports, figures) | frozen replay (`mode = "frozen_replay"`): reuse unchanged writers from a read-only freeze, recompute the rest | this page, then `docs/freeze-single-fish-run.md` |
| Proving the current code reproduces the accepted baseline | isolated control (L765_f04 copy) with byte-compared baselines | `docs/single-fish-unattended-job.md` |
| Read-only checks before a fish is ready | readiness preflight | `docs/remaining-fish-readiness.md` |

Prefer a frozen replay for changes that leave registration and geometry unchanged; the 2026-10-08 native replays took 2–4 minutes per fish. Fresh native registration needs an explicit positive ANTs seed (the builder default is 123); seed 0 is not deterministic. Default fresh recipes stop after QC 00–05 at the midline review gate. The accepted-midline continuation adds the downstream writers and QC 06.

## Frozen replay rules

- `reuse_stages` selects frozen writer products to import, after verification, without recomputing them. Paths and manifests are rebound to the native fish layout. Reuse only writers whose code and inputs are scientifically unchanged.
- Shared configuration leaks upstream. With `--run-23c` (always set by fresh recipes), `prepare-functional-reference-stacks` writes the early-screen `[23b]/[23c]` tables, using the same `ActivityConfig` defaults as `score-activity-bpi`. A response-rule change can therefore change that writer too. Check the shared callers before declaring a change downstream-only.
- For approved changes to control-gated tables, pass `geometry_control_supersede_reason` to the recipe builder. It is a builder argument, not a spec key. It adds `--supersede-accepted-control-reason` to the matching, scoring, identity and export writers. It does not override artifact byte baselines or frozen geometry/ANTs audits.
- Freezes on exFAT must use `--protection read-only-image`. Mount the image at `/Volumes/<image name>` before launching, and again after a reboot.
- The recipe builder used so far is an out-of-repo script (`~/codeants_validation_scripts/build_replay.py`). A package-owned downstream-only builder option is open work.

## Launch and read results

- Run pipeline CLIs from the pinned checkout with `PYTHONPATH=src` and the scientific interpreter.
- `tools/single_fish_run.py plan --spec SPEC`: inspect which stages will run before the first launch. Other subcommands are `run`, `resume`, `status`, `verify` and `audit-artifacts`.
- `tools/single_fish_job.py --spec SPEC --validate-noop`: run, verify, audit artifacts, prove a no-op resume, and export the gallery. It exits 1 on any non-pass.
  - In launcher scripts, capture `rc=$?` on the line immediately after the job. `echo "$(date) $?"` reports the status of `date`.
  - The authoritative result is `status` in the job report JSON. Reports default to `<spec folder>/job_reports/`, i.e. inside `run_specs/`.
- Launch detached (`nohup … & disown`) with stdin closed. One finishing job per fish at a time.
- **Never edit or commit the pinned checkout while a job runs.** Specs pin an exact clean commit.
- `review_required` marks a pending scientific gate declared in the spec. It is not scientific acceptance. Mechanical success is the report's `status: pass`.
- Native runs promote into the existing fish folders. Isolated controls must run on copied writable inputs.

## Review outputs

- QC gallery: exported by the finishing job to `<fish>/03_analysis/functional/review_gallery/<state root name>/<source hash>/index.html`. The standalone `tools/export_qc_figures.py` requires `--output-dir` (`docs/qc-figure-gallery.md`).
- All-neuron response-trace inspector: `tools/render_response_trace_inspector.py` (`src/codeants_2pf_hcr/response_trace_inspector.py`). HCR match inspector: `tools/render_hcr_match_inspector.py`. Both take an explicit output path.
- Manual HCR identity (cellMatcher app): `docs/manual-hcr-identity-gui.md`. Install it from the manual-identity checkout (`codex/manual-hcr-identity-gui`; the installer is not on the pipeline branches yet) with `tools/install_cellmatcher_app.sh <python>`. The app runs whatever is checked out in that checkout (`~/gitRepo/codeANTs`), so keep it separate from the pinned pipeline checkout.

## Locations

- Data root: `/Volumes/dataDrive/dataProcessing/2p_processing/<fish>` (an exFAT drive; it moves between machines). `data_root` / `--local-root` must directly contain the fish folders. Staged runs and freezes are under `_staged_pipeline_runs/`.
- Fish layout: `03_analysis/functional/{run_specs,run_state,pipeline_manifests,review_gallery}` plus the recipe-declared QC notebook folder; job reports are in `run_specs/job_reports/`.
- Scientific interpreter: `~/miniforge3/envs/cellMatching/bin/python` (writers, notebooks, exporters).
- Test interpreter: `~/miniforge3/bin/python -m pytest` (pytest is not in `cellMatching`). Manual-identity GUI: the `manualIdentity` venv layered on `cellMatching`.
- Fish set: L765_f04 (validation baseline), L765_f02, L758_f07, L765_f05 (R2 only, planes 5–9), L765_f06.

## Run history

Recent run outcomes and their reasons are in `.agents/references/recent-changes-single-fish.md`. Query it rather than reading it whole:

```bash
python3 .agents/scripts/query_recent_changes.py --repo codeANTs --query "frozen replay" --limit 5
```
