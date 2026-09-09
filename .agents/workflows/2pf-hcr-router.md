# 2PF-HCR Router

**Purpose:** thin top-level dispatcher for workflow-profile routing.

**Use this file when:** starting any current or legacy 2PF-HCR task in this repository.

## Read this first

1. Read this router first.
2. Read `.agents/references/coding.md` for universal coding guardrails.
3. Select the workflow profile.
4. Jump to the profile router and follow its read order and task-routing table.
5. When a task matches more than one row in a profile router, combine the referenced policies; do not silently choose only one row.

## Workflow profile dispatch

| If your task primarily targets... | Open this router |
| --- | --- |
| `notebooks/singleFish.ipynb` and single-fish stage ownership | `.agents/workflows/2pf-hcr-single-fish-router.md` |
| `notebooks/qc/`, `notebooks/hcr_activity_replay_qa.ipynb`, or their package-owned review surfaces | `.agents/workflows/2pf-hcr-single-fish-router.md` |
| `registrations/` or the root compatibility paths for maintained registration utilities | `.agents/workflows/2pf-hcr-single-fish-router.md` |
| `notebooks/multi_fish_56h_56g.ipynb`, `notebooks/multiFish.ipynb`, and cohort/multi-fish aggregation or orchestration ownership | `.agents/workflows/2pf-hcr-cohort-router.md` |
| `legacy/` comparison or reproduction | Read `legacy/README.md`, then use the single-fish or cohort router matching the historical artifact. Legacy material is never default authority. |

## Cross-workflow invariants (always)

- Prefer package edits over notebook edits.
- Table semantics belong to the stage that writes the table, not downstream consumers.
- Preserve canonical outputs, filenames, variable names, and stage semantics.
- Keep geometry matching independent of activity/BPI/gene identity.
- Do not let HCR-centric exports silently replace ROI-centric authoritative outputs.
- Do **not** use `tools/` or `registrations/` as business-logic authority.
- Do **not** use `legacy/` as current authority. Open it only for an explicitly requested comparison or reproduction.

## Compact scaling rule

- Add a notebook to an existing workflow profile when it shares stage semantics, ownership modules, and validation surface.
- Create a new profile only when it needs its own stage map, handoff cadence, and routing table.
