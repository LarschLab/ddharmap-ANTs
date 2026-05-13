# Recent Changes Index

**Purpose:** index and compatibility pointer for workflow-specific rolling logs.

**Use this file when:** deciding which workflow handoff log to open, or recording a short cross-workflow pointer entry.

## Workflow log routing

- Single-fish work (`notebooks/singleFish.ipynb`, single-fish ownership modules):
  - use `.agents/references/recent-changes-single-fish.md`
- Cohort work (`notebooks/multi_fish_56h_56g.ipynb`, cohort ownership modules):
  - use `.agents/references/recent-changes-cohort.md`

## Cross-workflow compatibility entries (optional)

- Add brief entries here only when a change intentionally spans both workflows or changes shared contracts/policy.
- Keep full per-workflow handoff detail in the workflow-specific log files.

### YYYY-MM-DD - short cross-workflow label

- Scope:
  - shared behavior or policy touched across single-fish and cohort workflows
- Pointers:
  - relevant entry in `recent-changes-single-fish.md` (if any)
  - relevant entry in `recent-changes-cohort.md` (if any)
