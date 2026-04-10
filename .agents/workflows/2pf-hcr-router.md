# 2PF-HCR Router

Use this file before opening large notebook regions.

## Routing

- Notebook path/setup/state issue: open `codeants_2pf_hcr.context`.
- Spatial prep, voxel/orientation, best-z or in-plane registration helper: open `codeants_2pf_hcr.spatial`.
- Stimulus log parsing, frame-rate resolution, motion-window timing: open `codeants_2pf_hcr.stimulus`.
- Anatomy identity lookup or ROI/anatomy matching helper: open `codeants_2pf_hcr.matching`.
- CLI figure behavior for the existing tools: open the wrapper in `tools/` only after opening `codeants_2pf_hcr.plots.*`.

## Rules

- Prefer package edits over notebook edits.
- If a notebook cell still owns logic, move the logic first, then collapse the cell to imports and package calls.
- Preserve canonical variable names assigned in the notebook so downstream cells remain runnable during migration.
- Preserve canonical CSV names and do not fork alternate exports.
