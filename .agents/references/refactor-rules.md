# 2PF-HCR Refactor Rules

## Ownership rules

1. No new notebook-local `def` blocks in `notebooks/2PF_to_HCR.ipynb`.
2. Notebook orchestration lives in notebook cells only.
3. Reusable logic lives in `src/codeants_2pf_hcr/`.
4. CLI behavior lives in `tools/` wrappers only; `tools/` is not business-logic authority.
5. Figure construction lives in `plots.*`.
6. Table semantics are owned by the stage that writes the table, not downstream consumers.

## Edit-scope rules

7. Fix at the narrowest owning layer.
8. Avoid multi-file refactors unless required by ownership or contract boundaries.
9. If package ownership is clear, fix the package first.
10. Do not patch downstream figures to compensate for upstream semantic bugs.
11. Do not change schemas to dodge one local failure.
12. Preserve canonical outputs and filenames unless a compatibility shim is added deliberately.
13. Preserve stage order and cell tags even when implementation moves into the package.
14. Notebook cells should contain explicit knobs, imports, one or two package calls, and optional display/save code.
15. Fail fast on missing prerequisites; do not add new `globals()`-based fallback state.
16. Whole-population matching remains ROI-centric in `[50i]`; identified-cell export remains HCR-centric in `[50]`; response/BPI stays downstream of geometry in `[50ia]`.
17. Prefer adding or updating package-level smoke/contract checks over ad hoc notebook assertions.

## Legacy and ambiguity rules

18. Old notebook-local behavior is not authoritative if a package implementation exists.
19. Legacy columns may persist, but semantics come from the current canonical stage docs.
20. Do not revive deprecated notebook-local patterns because they still appear in old cells.
21. If code and docs disagree, follow policy docs unless they are clearly outdated.

## Public API rules

22. Notebook-callable functions must be public and listed in `__all__`.
23. Notebook-facing module docstrings should name the relevant notebook cell tags and canonical outputs.
24. Stage functions should accept explicit inputs and return legacy-shaped outputs without hidden notebook-state dependencies.

## Refactor persistence rules

25. Treat notebook migration work as an ownership-slice refactor, not a one-cell cleanup task.
26. Prefer owner-complete slice reduction over isolated cell edits when adjacent cells still belong to the same owning module or stage family.
27. Continue repeated passes while the remaining work stays within the same owner and uses the same validation surface.
28. Stop only at a real boundary: completed-and-validated slice, concrete recorded blocker, or ownership handoff to a different module/stage family.

## Trusted patterns

- `context.py` = path/config normalization pattern.
- `suite2p.py` = Suite2p file/discovery and dF/F loading pattern.
- `stimulus.py` = explicit stage input/output pattern.
- `activity.py` = response/BPI stage-owned semantics pattern.
- `traces.py` = trace export and trace-ready mapping pattern.
- `plots.*` = deterministic figure-builder pattern.
- `tools/` = wrapper pattern only.
