# Recent Changes

**Purpose:** rolling manual handoff log for meaningful work, remaining breakpoints, and rerun implications.

**Use this file when:** a task changes public behavior, leaves known breakage, or would otherwise force the next session to rediscover context.

## Update template

### YYYY-MM-DD - short task label

- What changed:
  - concise description of the behavior, ownership, or workflow change
- What remains broken:
  - known failures, gaps, or deferred follow-up
- Next likely breakpoint:
  - first stage, file, test, or command most likely to fail next
- Rerun implications:
  - minimum rerun or validation sequence needed after this change

## Notes

- Append new entries; do not rewrite unrelated history.
- Keep migration state in `current-state.md`; use this file for per-change handoff detail.
