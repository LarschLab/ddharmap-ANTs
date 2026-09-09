# Agent Entrypoint

This repository uses a routed instruction system under `.agents/`.

Start here, then read only what your task needs.

## Large Log Routing

Do not open workflow-specific `recent-changes-*.md` logs wholesale just to find context. First query them with:

```bash
python3 .agents/scripts/query_recent_changes.py --repo <repo-name> --query <term> --limit 5
```

Open the full log only when the compact query shows that exact entries are needed.

## Answer-Direct Route

If the user asks a general question, asks for a short explanation, or provides enough context in the prompt to answer without repo/vault state, answer directly and do not load routers, references, indexes, logs, or templates.

Use repo/vault routing only when the request requires local files, durable updates, validation, provenance, or project-specific workflow rules.

## Required startup order

1. Open `.agents/workflows/2pf-hcr-router.md`.
2. Follow its task routing table.
3. Read the smallest relevant file in `.agents/references/`.
4. Open owning package module(s) before large notebook regions.
5. Open notebook cells only when package/module context is insufficient.

## Non-negotiable repo rules

- Ownership:
  - notebook orchestration lives in notebook cells only
  - reusable logic lives in `src/codeants_2pf_hcr/`
  - general CLI behavior lives in `tools/` wrappers
  - maintained manual registration entrypoints live in `registrations/`
  - figure construction lives in `plots.*`
  - table semantics are owned by the stage that writes them, not downstream consumers
- Prefer package edits over notebook edits.
- Do not introduce new notebook-local helper definitions when reusable package code is appropriate.
- Preserve canonical outputs, filenames, variable names, and notebook stage semantics.
- Keep geometry matching independent of activity/BPI/gene identity.
- Assign identity after geometry; add response/BPI after geometry is fixed.
- Figures should filter authoritative tables, not rebuild identity in plotting cells.
- ROI-centric master table is authoritative for whole-population analyses.
- HCR-centric exports are for identified-cell activity analyses only and must not silently replace ROI-centric authoritative outputs.
- Consult `.agents/workflows/2pf-hcr-router.md` before opening large notebook regions.
- Search order:
  - router first, then the smallest relevant reference doc, then `symbol-index.md` or `notebook-stage-map.md` only if needed, then the owning module, then notebook cells
  - do not open large notebook regions first
  - do not use `tools/`, `registrations/`, or `legacy/` as business-logic authority
- Edit scope:
  - fix at the narrowest owning layer
  - do not patch downstream figures to compensate for upstream semantic bugs
  - do not change schemas to dodge one local failure
  - continue refactor work through repeated passes within the same ownership slice; do not stop after one cleaned-up cell unless a real boundary or blocker is reached
  - detailed multi-pass refactor stop/continue policy lives in `.agents/references/refactor-loop-policy.md`
- Validation:
  - run the smallest relevant smoke or contract check after edits
  - verify the first downstream consumer when canonical table semantics changed
  - do not declare success from static reasoning alone
- Change logging:
  - public behavior change -> update the relevant reference doc
  - public function change -> update `symbol-index.md`
  - stage/output ownership change -> update `notebook-stage-map.md` and `current-state.md`
  - meaningful work with remaining breakage -> append the workflow-specific recent-changes file (`.agents/references/recent-changes-single-fish.md` or `.agents/references/recent-changes-cohort.md`)
- Prompt minimization:
  - future prompts should specify task, target, and any special constraint only
  - repo docs are the default source of policy and workflow; do not restate repo-wide rules in every prompt

## Reference files

- `.agents/references/scientific-policy.md` - scientific ground truth and matching intent.
- `.agents/references/notebook-stage-map.md` - staged notebook flow, cell tags, and key stage outputs.
- `.agents/references/canonical-tables.md` - authoritative tables, keys, and allowed use.
- `.agents/references/activity-semantics.md` - response/BPI semantics and `suite2p_is_cell` policy.
- `.agents/references/figure-rules.md` - figure sourcing and plotting constraints.
- `.agents/references/cache-rerun-policy.md` - stale outputs and minimum rerun sequence.
- `.agents/references/current-state.md` - current mixed migration status and practical warnings.
- `.agents/references/agentic-workflow-roadmap.md` - living status board for staged pipeline/agentic workflow migration.
- `.agents/references/recent-changes.md` - index/compat pointer for workflow-specific rolling logs.
- `.agents/references/recent-changes-single-fish.md` - single-fish rolling handoff log.
- `.agents/references/recent-changes-cohort.md` - cohort rolling handoff log.
- `.agents/references/refactor-loop-policy.md` - multi-pass refactor persistence and stop/continue rules.
- `.agents/references/refactor-rules.md` - package ownership and implementation constraints.
- `.agents/references/symbol-index.md` - package symbol lookup for fast navigation.

## Scope note

`AGENTS.md` is intentionally short. Detailed policy belongs in `.agents/references/` and routing belongs in `.agents/workflows/`.
