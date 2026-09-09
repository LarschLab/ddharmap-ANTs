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

## Compact Query Route

Before opening a workflow-specific append-only log, query it with:

```bash
python3 .agents/scripts/query_recent_changes.py --repo <repo-name> --query <term> --limit 5
```

Open the full log only when the compact result points to an entry that needs detailed reading.

### 2026-09-09 - repository authority and layout cleanup

- Scope:
  - normalized single-fish/cohort routing, classified reference authority, separated maintained `registrations/` from `legacy/`, removed tracked generated caches, and added executable location and scientific-artifact contracts
- Pointers:
  - `recent-changes-single-fish.md` entry for the restored QC notebook 02 registration gate and contract validation
  - commits on `codex/repository-cleanup` following `e4683d7` (`pre-cleanup`)
