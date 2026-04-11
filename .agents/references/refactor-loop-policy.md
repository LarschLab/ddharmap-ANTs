# Refactor Loop Policy

**Purpose:** keep notebook-to-package refactor sessions moving through repeated passes until an ownership slice is complete or blocked at a real boundary.

**Use this file when:** continuing a notebook refactor, resuming a prior migration slice, or deciding whether a refactor session should keep going or hand off.

## Default working unit

- The default refactor session unit is an **ownership slice**.
- An ownership slice is the smallest owner-complete migration unit, usually:
  - one owning package module or stage family
  - the notebook cell wrappers that should thin down to call that owner
  - the first downstream consumer that must still work after the extraction
- Prefer owner-complete slice reduction over isolated one-cell cleanup.

## Required loop inside one refactor session

1. State the current slice goal before editing.
2. Pick the next sub-pass inside that same slice.
3. Extract or reduce the owning logic at the narrowest valid layer.
4. Run the smallest relevant validation for that sub-pass.
5. Reassess what remains in the same slice.
6. Continue immediately if the remaining work still belongs to the same owner and uses the same local validation surface.

## Keep-going rule

- Do not treat a single converted notebook cell as a natural completion point.
- Keep going while adjacent cells, helper flows, or first-consumer checks still belong to the same extracted owner.
- Stop only when one of the valid stop conditions below is met.

## Valid stop conditions

- **Slice complete and validated**
  - The owner-complete slice has been reduced and the smallest relevant validation has passed, including the first downstream consumer when needed.
- **Concrete blocker recorded**
  - A real blocker prevents continuation, and the handoff records the exact failing stage, file, test, or command plus the next resume point.
- **Ownership boundary reached**
  - The next work belongs to a different owning module or stage family and should start as a new slice.

## Invalid stop condition

- "One cell was cleaned up" is not, by itself, a valid reason to stop a refactor session.

## Handoff requirements when stopping mid-migration

- Update `.agents/references/recent-changes.md` if the session changed behavior, leaves known breakage, or would force the next session to rediscover context.
- Record:
  - the slice goal
  - the passes completed in this session
  - the remaining cells, helpers, or functions still inside the same slice
  - the exact next breakpoint or resume point
  - the minimum rerun or validation still needed

## Validation expectations

- After each sub-pass, run the smallest relevant smoke, contract, or regression check.
- When the slice changes a writer stage or canonical output, verify the first downstream consumer before treating the slice as complete.
- Do not declare a refactor slice complete from static reasoning alone.
