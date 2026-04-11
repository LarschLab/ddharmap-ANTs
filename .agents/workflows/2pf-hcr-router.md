# 2PF-HCR Router

**Purpose:** fast routing for low-token agent sessions.

**Use this file when:** you are starting any task on `notebooks/2PF_to_HCR.ipynb` or related package/tools code.

## Read order (always)

1. Read this router first.
2. Read the smallest relevant file in `.agents/references/`.
3. Open `symbol-index.md` only if symbol lookup is needed.
4. Open `notebook-stage-map.md` only if stage ownership or cell mapping is still unclear.
5. Open the owning package module in `src/codeants_2pf_hcr/`.
6. Open notebook cells only if package code is insufficient for the task.
7. Open `tools/` wrappers only for CLI behavior, plotting wrapper behavior, or reference patterns.

## Never these first

- Do **not** open large notebook regions first.
- Do **not** use `tools/` as business-logic authority.
- Do **not** infer semantics from downstream figures before reading the writer stage.
- Stop searching once the owning module and authoritative reference doc answer the question.

## Task routing table

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
| smoke-test/bugfix workflow issue | `references/cache-rerun-policy.md` + `references/notebook-stage-map.md` |
| continue refactor / multi-pass migration / resume previous slice | `references/refactor-loop-policy.md` + `references/refactor-rules.md` |
| refactor or package ownership question | `references/refactor-rules.md` + `references/symbol-index.md` |
| CLI wrapper behavior question | `references/symbol-index.md`, then `tools/` wrapper file |

## Invariants to preserve while editing

- Prefer package edits over notebook edits.
- Table semantics belong to the stage that writes the table, not downstream consumers.
- Preserve canonical outputs, filenames, variable names, and stage semantics.
- Keep geometry matching independent of activity/BPI/gene identity.
- Do not let HCR-centric exports silently replace ROI-centric authoritative outputs.

## Error triage order

1. Silent output corruption.
2. Canonical table misuse.
3. Upstream semantic bugs.
4. Loud crashes.
5. Downstream plotting or cosmetic symptoms.
