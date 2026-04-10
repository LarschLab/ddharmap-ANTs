# Canonical Tables

**Purpose:** define authoritative data sources and correct table usage.

**Use this file when:** deciding which CSV/DF to use, joining tables, or validating output ownership.

## Authoritative table for whole-population analyses

### `functional_roi_activity_identity.csv` (`FUNC_ACTIVITY_IDENTITY_DF`)

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

- Per-ROI response/BPI scoring export from `[50ia]`.
- Useful for response/BPI diagnostics.
- Not an independent identity source.

### `anatomy_identity_lookup.csv`

- Anatomy-label to identity lookup helper.
- Convenience join table only.

### `suite2p_traces/suite2p_dff_traces_meta.csv`

- Trace metadata/index for exported ROI traces.
- Should remain aligned to ROI-centric authoritative subset definitions.

## HCR-centric identified-cell tables (non-authoritative for whole-population identity)

### `hcr_activity_status.csv`

- Identified-label activity/status view for HCR-centric analyses.

### `conf_to_func_pairs.csv`

- Response-positive trace-ready mapping keyed by accepted HCR/anatomy labels.
- Valid for identified-cell activity analyses only.

## Usage guardrails

- Whole-population identity/activity/BPI: start from `functional_roi_activity_identity.csv`.
- Do not substitute `conf_to_func_pairs.csv` for ROI-centric authoritative identity summaries.
- If starting from HCR-centric status for identified-cell reporting, keep scope explicit and avoid silent path mixing.
