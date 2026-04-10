# 2PF-HCR Router

**Purpose:** fast routing for low-token agent sessions.

**Use this file when:** you are starting any task on `notebooks/2PF_to_HCR.ipynb` or related package/tools code.

## Read order (always)

1. Read this router first.
2. Read the smallest relevant file in `.agents/references/`.
3. Open `notebook-stage-map.md` or `symbol-index.md` only if ownership/cell mapping is unclear.
4. Open the owning package module in `src/codeants_2pf_hcr/`.
5. Open notebook cells only if package code is insufficient for the task.
6. Open `tools/` wrappers only for CLI behavior, plotting wrapper behavior, or reference patterns.

Do **not** open large notebook regions first.

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
| refactor or package ownership question | `references/refactor-rules.md` + `references/symbol-index.md` |
| CLI wrapper behavior question | `references/symbol-index.md`, then `tools/` wrapper file |

## Invariants to preserve while editing

- Prefer package edits over notebook edits.
- Preserve canonical outputs, filenames, variable names, and stage semantics.
- Keep geometry matching independent of activity/BPI/gene identity.
- Do not let HCR-centric exports silently replace ROI-centric authoritative outputs.
