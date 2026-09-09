# 2PF-HCR Cohort Router

**Purpose:** compact routing surface for cohort workflow tasks.

**Use this file when:** working on `notebooks/multi_fish_56h_56g.ipynb`, `notebooks/multiFish.ipynb`, or cohort/multi-fish ownership in package modules.

## Read order (cohort)

1. Read `.agents/workflows/2pf-hcr-router.md`.
2. Read this cohort router.
3. Read `.agents/references/coding.md` (required baseline guardrails).
4. Read `.agents/references/cohort-stage-map.md`.
5. Read the smallest additional semantic reference needed (`.agents/references/canonical-tables.md`, `.agents/references/activity-semantics.md`, `.agents/references/figure-rules.md`, `.agents/references/current-state.md`, `.agents/references/cache-rerun-policy.md`).
6. Open `.agents/references/symbol-index.md` only if symbol lookup is needed.
7. Open the owning package module in `src/codeants_2pf_hcr/`.
8. Open notebook cells only if package code is insufficient for the task.
9. Open `tools/` wrappers only for CLI behavior or wrapper behavior.

## Task routing table (cohort)

| If cohort task is about... | Read this first |
| --- | --- |
| stage ownership, tags, or output locations | `.agents/references/cohort-stage-map.md` |
| build/cache orchestration or cache invalidation | `.agents/references/cohort-stage-map.md` + `.agents/references/cache-rerun-policy.md` |
| cohort semantic boundary vs single-fish authority | `.agents/references/cohort-stage-map.md` + `.agents/references/current-state.md` + `.agents/references/canonical-tables.md` |
| cohort notebook refactor ownership | `.agents/references/cohort-stage-map.md` + `.agents/references/refactor-rules.md` + `.agents/references/symbol-index.md` |
| cohort figure sourcing constraints | `.agents/references/cohort-stage-map.md` + `.agents/references/figure-rules.md` + `.agents/references/canonical-tables.md` |
| explicit legacy comparison or reproduction | `legacy/README.md` + `.agents/references/cohort-stage-map.md`; legacy behavior is evidence, not current authority |

## Cohort ownership guidance

- Cohort stage semantics are authored in `.agents/references/cohort-stage-map.md`.
- Shared semantic authority docs remain shared unless explicitly split.
- Cohort notebook cells should stay orchestration-thin and consume package-owned builders/renderers.
- Do **not** use `tools/` as business-logic authority.
- Handoff logging for cohort work goes to `.agents/references/recent-changes-cohort.md`.
