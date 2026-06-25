# Canonical Tables

**Purpose:** define authoritative data sources and correct table usage.

**Use this file when:** deciding which CSV/DF to use, joining tables, or validating output ownership.

## Authoritative table for whole-population analyses

### `functional_roi_activity_identity.csv` (`FUNC_ACTIVITY_IDENTITY_DF`)

- Authority:
  - whole-population identity
  - whole-population activity/BPI views after `[50ia]`
  - default figure filtering semantics unless a figure is explicitly HCR-centric
- One row per functional ROI (`plane_idx`, `func_label`).
- Includes all segmented ROIs (including `suite2p_is_cell=False`).
- Carries geometry match status, identity annotation, and post-`[50ia]` response/BPI fields.
- This is the default source for whole-population analyses and figure filtering.

Representative column groups:

- ROI/geometry: `plane_idx`, `func_label`, `selected_anat_label`, overlap/distance and match outcome fields.
- Identity: `anat_label`, `identity_label`, `has_identity_assigned`, `identity_display_label`.
- Suite2p provenance: `suite2p_is_cell`, `suite2p_activity_class`.
- Response/BPI: `response_is_active`, `response_class`, `response_summary_class`, `bpi`, `bpi_category`, trial/AUC/null-threshold fields.

## Derived helper tables

### `functional_roi_activity_bpi_cells.csv`

- Authority:
  - response/BPI scoring diagnostics written by `[50ia]`
- Per-ROI response/BPI scoring export from `[50ia]`.
- Useful for response/BPI diagnostics.
- Not an independent identity source.

### `anatomy_identity_lookup.csv`

- Authority:
  - convenience anatomy-label to identity join only
- Anatomy-label to identity lookup helper.
- Convenience join table only.

### `suite2p_traces/suite2p_dff_traces_meta.csv`

- Authority:
  - trace export metadata only
- Trace metadata/index for exported ROI traces.
- Should remain aligned to ROI-centric authoritative subset definitions.

## HCR-centric identified-cell tables (non-authoritative for whole-population identity)

### `hcr_activity_status.csv`

- Authority:
  - identified-cell activity status written by `[50]`
- Identified-label activity/status view for HCR-centric analyses.

### `conf_to_func_pairs.csv`

- Authority:
  - identified-cell responsive trace-ready export written by `[50]`
- Response-positive trace-ready mapping keyed by accepted HCR/anatomy labels.
- Valid for identified-cell activity analyses only.

## Concept authorities

- Whole-population identity -> `functional_roi_activity_identity.csv`.
- Identified-cell activity export -> HCR-centric tables from `[50]`.
- Response semantics -> `[50ia]` / activity stage outputs.
- Geometry matching -> matching stage only.
- Figure semantics -> canonical tables plus subset filters; never infer identity or response state in plotting code.

## Usage guardrails

- Whole-population identity/activity/BPI: start from `functional_roi_activity_identity.csv`.
- Downstream stages may filter or join canonical tables, but they do not redefine the writer stage semantics.
- Do not substitute `conf_to_func_pairs.csv` for ROI-centric authoritative identity summaries.
- If starting from HCR-centric status for identified-cell reporting, keep scope explicit and avoid silent path mixing.

## Ambiguity ladder

- ROI-centric vs HCR-centric meaning -> `scientific-policy.md`, then `current-state.md`.
- Activity wording -> `activity-semantics.md`.
- Output ownership -> this file.
- If code conflicts with docs, follow policy docs unless they are clearly outdated.
