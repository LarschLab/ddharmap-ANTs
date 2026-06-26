# 2PF-HCR Single-Fish Router

**Purpose:** compact routing surface for single-fish workflow tasks.

**Use this file when:** working on `notebooks/singleFish.ipynb` or its owning package modules.

## Read order (single-fish)

1. Read `.agents/workflows/2pf-hcr-router.md`.
2. Read this single-fish router.
3. Read `.agents/references/coding.md` (required baseline guardrails).
4. Read the smallest additional relevant file in `.agents/references/`.
5. Open `symbol-index.md` only if symbol lookup is needed.
6. Open `notebook-stage-map.md` only if stage ownership or cell mapping is still unclear.
7. Open the owning package module in `src/codeants_2pf_hcr/`.
8. Open notebook cells only if package code is insufficient for the task.
9. Open `tools/` wrappers only for CLI behavior or wrapper behavior.

## Task routing table (single-fish)

| If task is about... | Read this first |
| --- | --- |
| notebook path/setup/state issue | `references/notebook-stage-map.md` + `references/refactor-rules.md` |
| spatial/orientation/registration issue | `references/notebook-stage-map.md` + `references/scientific-policy.md` |
| Suite2p or ROI inventory issue | `references/notebook-stage-map.md` + `references/canonical-tables.md` |
| HCR/anatomy warp or identity lookup issue | `references/scientific-policy.md` + `references/notebook-stage-map.md` |
| ROI-centric matching issue | `references/scientific-policy.md` + `references/canonical-tables.md` |
| HCR-centric identified-cell activity issue | `references/scientific-policy.md` + `references/current-state.md` |
| response classification / BPI issue | `references/activity-semantics.md` |
| canonical table / output confusion | `references/canonical-tables.md` |
| figure or plot issue | `references/figure-rules.md` + `references/activity-semantics.md` |
| cache/rerun issue | `references/cache-rerun-policy.md` |
| staged pipeline / agentic workflow migration | `references/agentic-workflow-roadmap.md` + `references/single-fish-pipeline-roadmap.md` + `references/refactor-rules.md` |
| smoke-test/bugfix workflow issue | `references/cache-rerun-policy.md` + `references/notebook-stage-map.md` |
| continue refactor / multi-pass migration / resume previous slice | `references/refactor-loop-policy.md` + `references/refactor-rules.md` |
| refactor or package ownership question | `references/refactor-rules.md` + `references/symbol-index.md` |
| CLI wrapper behavior question | `references/symbol-index.md`, then `tools/` wrapper file |

## Single-fish ownership guidance

- Single-fish stage semantics are authored in `references/notebook-stage-map.md`.
- Reusable implementation ownership remains in `src/codeants_2pf_hcr/`.
- Notebook cells should stay orchestration-thin: explicit knobs, package calls, optional display/save.
- Do **not** use `tools/` as business-logic authority.
- Handoff logging for single-fish work goes to `references/recent-changes-single-fish.md`.
