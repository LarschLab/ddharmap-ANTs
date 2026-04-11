# 2PF-HCR Router

**Purpose:** thin top-level dispatcher for workflow-profile routing.

**Use this file when:** starting tasks on `notebooks/2PF_to_HCR.ipynb`, `notebooks/multi_fish_56h_56g.ipynb`, or related package/tools code.

## Read this first

1. Read this router first.
2. Select the workflow profile.
3. Jump to the profile router and follow its read order and task-routing table.

## Workflow profile dispatch

| If your task primarily targets... | Open this router |
| --- | --- |
| `notebooks/2PF_to_HCR.ipynb` and single-fish stage ownership | `.agents/workflows/2pf-hcr-single-fish-router.md` |
| `notebooks/multi_fish_56h_56g.ipynb` and cohort aggregation/rendering ownership | `.agents/workflows/2pf-hcr-cohort-router.md` |

## Cross-workflow invariants (always)

- Prefer package edits over notebook edits.
- Table semantics belong to the stage that writes the table, not downstream consumers.
- Preserve canonical outputs, filenames, variable names, and stage semantics.
- Keep geometry matching independent of activity/BPI/gene identity.
- Do not let HCR-centric exports silently replace ROI-centric authoritative outputs.
- Do **not** use `tools/` as business-logic authority.

## Compact scaling rule

- Add a notebook to an existing workflow profile when it shares stage semantics, ownership modules, and validation surface.
- Create a new profile only when it needs its own stage map, handoff cadence, and routing table.
