# 2PF-HCR Refactor Rules

1. No new notebook-local `def` blocks in `notebooks/2PF_to_HCR.ipynb`.
2. Package first, wrapper second. Reusable logic belongs in `src/codeants_2pf_hcr/`; `tools/` stays thin.
3. Preserve canonical outputs and filenames unless a compatibility shim is added deliberately.
4. Preserve stage order and cell tags even when implementation moves into the package.
5. Notebook cells should contain explicit knobs, imports, one or two package calls, and optional display/save code.
6. Fail fast on missing prerequisites; do not add new `globals()`-based fallback state.
7. Whole-population matching remains ROI-centric in `[50i]`; identified-cell export remains HCR-centric in `[50]`; response/BPI stays downstream of geometry in `[50ia]`.
