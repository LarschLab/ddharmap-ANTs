# 2PF-HCR Cohort Router

**Purpose:** compact routing surface for cohort workflow tasks.

**Use this file when:** working on `notebooks/multi_fish_56h_56g.ipynb` or cohort ownership in package modules.

## Read order (cohort)

1. Read `.agents/workflows/2pf-hcr-router.md`.
2. Read this cohort router.
3. Read `.agents/references/coding.md` (required baseline guardrails).
4. Read `references/cohort-stage-map.md`.
5. Read the smallest additional semantic reference needed (`canonical-tables.md`, `activity-semantics.md`, `figure-rules.md`, `current-state.md`, `cache-rerun-policy.md`).
6. Open `symbol-index.md` only if symbol lookup is needed.
7. Open the owning package module in `src/codeants_2pf_hcr/`.
8. Open notebook cells only if package code is insufficient for the task.
9. Open `tools/` wrappers only for CLI behavior or wrapper behavior.

## Task routing table (cohort)

| If cohort task is about... | Read this first |
| --- | --- |
| stage ownership, tags, or output locations | `references/cohort-stage-map.md` |
| build/cache orchestration or cache invalidation | `references/cohort-stage-map.md` + `references/cache-rerun-policy.md` |
| cohort semantic boundary vs single-fish authority | `references/cohort-stage-map.md` + `references/current-state.md` + `references/canonical-tables.md` |
| cohort notebook refactor ownership | `references/cohort-stage-map.md` + `references/refactor-rules.md` + `references/symbol-index.md` |
| cohort figure sourcing constraints | `references/cohort-stage-map.md` + `references/figure-rules.md` |

## Cohort ownership guidance

- Cohort stage semantics are authored in `references/cohort-stage-map.md`.
- Shared semantic authority docs remain shared unless explicitly split.
- Cohort notebook cells should stay orchestration-thin and consume package-owned builders/renderers.
- Do **not** use `tools/` as business-logic authority.
- Handoff logging for cohort work goes to `references/recent-changes-cohort.md`.
