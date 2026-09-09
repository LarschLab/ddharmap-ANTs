# 2PF-HCR Single-Fish Router

**Purpose:** compact routing surface for single-fish workflow tasks.

**Use this file when:** working on `notebooks/singleFish.ipynb` or its owning package modules.

## Read order (single-fish)

1. Read `.agents/workflows/2pf-hcr-router.md`.
2. Read this single-fish router.
3. Read `.agents/references/coding.md` (required baseline guardrails).
4. Read the smallest additional relevant file in `.agents/references/`.
5. Open `.agents/references/symbol-index.md` only if symbol lookup is needed.
6. Open `.agents/references/notebook-stage-map.md` only if stage ownership or cell mapping is still unclear.
7. Open the owning package module in `src/codeants_2pf_hcr/`.
8. Open notebook cells only if package code is insufficient for the task.
9. Open `tools/` wrappers only for CLI behavior or wrapper behavior.

## Task routing table (single-fish)

| If task is about... | Read this first |
| --- | --- |
| current status or next staged-pipeline step | `.agents/references/current-state.md` + `.agents/references/agentic-workflow-roadmap.md` |
| notebook path/setup/state issue | `.agents/references/notebook-stage-map.md` + `.agents/references/refactor-rules.md` |
| QC notebook review or refactor | `notebooks/qc/README.md` + `.agents/references/notebook-stage-map.md` + the semantic policy for that review gate |
| HCR activity replay QA | `.agents/references/current-state.md` + `.agents/references/canonical-tables.md` |
| spatial/orientation/registration issue | `.agents/references/notebook-stage-map.md` + `.agents/references/scientific-policy.md` |
| maintained manual registration utility | `.agents/references/notebook-stage-map.md` + `.agents/references/current-state.md`, then the file under `registrations/` |
| Suite2p or ROI inventory issue | `.agents/references/notebook-stage-map.md` + `.agents/references/canonical-tables.md` |
| HCR/anatomy warp or identity lookup issue | `.agents/references/scientific-policy.md` + `.agents/references/notebook-stage-map.md` |
| ROI-centric matching issue | `.agents/references/scientific-policy.md` + `.agents/references/canonical-tables.md` |
| HCR-centric identified-cell activity issue | `.agents/references/scientific-policy.md` + `.agents/references/current-state.md` |
| response classification / BPI issue | `.agents/references/activity-semantics.md` |
| canonical table / output confusion | `.agents/references/canonical-tables.md` |
| figure or plot issue | `.agents/references/figure-rules.md` + `.agents/references/activity-semantics.md` + `.agents/references/canonical-tables.md` |
| cache/rerun issue | `.agents/references/cache-rerun-policy.md` |
| staged pipeline / agentic workflow migration | `.agents/references/agentic-workflow-roadmap.md` + `.agents/references/single-fish-pipeline-roadmap.md` + `.agents/references/refactor-rules.md` |
| smoke-test/bugfix workflow issue | `.agents/references/cache-rerun-policy.md` + `.agents/references/notebook-stage-map.md` |
| continue refactor / multi-pass migration / resume previous slice | `.agents/references/refactor-loop-policy.md` + `.agents/references/refactor-rules.md` |
| refactor or package ownership question | `.agents/references/refactor-rules.md` + `.agents/references/symbol-index.md` |
| CLI wrapper behavior question | `.agents/references/symbol-index.md`, then the relevant wrapper file |
| explicit legacy comparison or reproduction | `legacy/README.md` + `.agents/references/scientific-policy.md` + `.agents/references/notebook-stage-map.md`; legacy behavior is evidence, not current authority |

## Single-fish ownership guidance

- Single-fish stage semantics are authored in `.agents/references/notebook-stage-map.md`.
- Reusable implementation ownership remains in `src/codeants_2pf_hcr/`.
- Notebook cells should stay orchestration-thin: explicit knobs, package calls, optional display/save.
- Do **not** use `tools/` as business-logic authority.
- Handoff logging for single-fish work goes to `.agents/references/recent-changes-single-fish.md`.
