# Reference Classification

**Purpose:** identify the authority and default routing status of every reference document.

Read normative policy before operational state, and operational state before roadmaps or history. Historical and compatibility documents are never current scientific authority.

| Document | Class | Default route | Role |
| --- | --- | --- | --- |
| `coding.md` | normative policy | always | Universal implementation guardrails. |
| `scientific-policy.md` | normative policy | semantic tasks | Scientific ordering and matching intent. |
| `canonical-tables.md` | normative policy | table and analysis tasks | Table ownership and allowed use. |
| `activity-semantics.md` | normative policy | response and BPI tasks | Response and BPI meaning. |
| `figure-rules.md` | normative policy | figure tasks | Figure sources, populations, and interpretation. |
| `refactor-rules.md` | normative policy | refactor tasks | Code and artifact ownership. |
| `refactor-loop-policy.md` | normative policy | continued refactors | Refactor persistence and stop conditions. |
| `cache-rerun-policy.md` | normative policy | rerun and validation tasks | Cache invalidation and validation order. |
| `notebook-stage-map.md` | current ownership map | single-fish stage tasks | Current single-fish stages and outputs. |
| `cohort-stage-map.md` | current ownership map | cohort tasks | Current cohort stages and outputs. |
| `symbol-index.md` | current lookup map | only when symbol lookup is needed | Package ownership and public symbols. |
| `current-state.md` | operational state | status and migration-boundary tasks | Current limitations, accepted evidence, and warnings. |
| `agentic-workflow-roadmap.md` | roadmap | staged-pipeline migration only | Living implementation status and next slice. |
| `single-fish-pipeline-roadmap.md` | roadmap | staged-pipeline migration only | Target pipeline and migration plan. |
| `single-fish-qc-suite-todo.md` | backlog | QC implementation planning only | Outstanding and accepted QC review work. |
| `stage-map.md` | compatibility pointer | never directly | Redirects old references to `notebook-stage-map.md`. |
| `recent-changes.md` | compatibility pointer | log selection only | Routes to workflow-specific logs. |
| `recent-changes-single-fish.md` | history log | explicit queried lookup only | Append-only single-fish handoff evidence. |
| `recent-changes-cohort.md` | history log | explicit queried lookup only | Append-only cohort handoff evidence. |

## Authority order

1. Normative policy.
2. Current ownership maps.
3. Operational state.
4. Roadmaps and backlogs.
5. Compatibility pointers and queried history.

If two documents in the same class disagree, stop and reconcile the conflict in the owning policy or map instead of choosing silently.
