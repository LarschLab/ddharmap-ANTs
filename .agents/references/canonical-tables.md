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
- `plane_idx` is the original acquisition-plane index, not the position in a
  selected reference list. R2-only planes 5–9 retain those keys. Array/list
  positions remain internal; scoring verifies the matching Suite2p source path.
- Includes all segmented ROIs (including `suite2p_is_cell=False`).
- Carries geometry match status, identity annotation, and post-`[50ia]` response/BPI fields.
- This is the default source for whole-population analyses and figure filtering.
- It is not the candidate universe for HCR-centric identified-cell recovery; label-first HCR exports may intentionally evaluate local candidates outside the ROI-centric one-to-one competition outcome.

Representative column groups:

- ROI/geometry: `plane_idx`, `func_label`, `selected_anat_label`, overlap/distance and match outcome fields.
- Identity: `anat_label`, `identity_label`, `has_identity_assigned`, `identity_display_label`.
- Suite2p provenance: `suite2p_is_cell`, `suite2p_activity_class`.
- Response/BPI: `response_is_active`, `response_class`,
  `response_summary_class`, per-condition response valence, signed condition
  AUCs and contrast, legacy rectified `bpi`, `bpi_category`, and
  trial/upper-and-lower-null-threshold fields.

## Derived helper tables

### `match-roi-to-anatomy/registration/plane_refs_summary_geometry.json`

- Authority:
  - direct-versus-reversed anatomy-label Z convention used by ROI/anatomy geometry matching
- Records the anatomy intensity `best_z`, resolved anatomy-label page, and label-Z mode for every functional plane.
- Geometry QC must use this geometry-owned sidecar rather than independently reinterpreting registration plane references.
- A missing, ambiguous, or table-inconsistent label-Z mapping blocks geometry review and all downstream identity/activity stages.

### `functional_roi_activity_bpi_cells.csv`

- Authority:
  - response/BPI scoring diagnostics written by `[50ia]`
- Per-ROI response/BPI scoring export from `[50ia]`.
- Persists the applied two-sided bootstrap-null gates explicitly: 0.995 upper
  and 0.005 lower at the default 0.99 total confidence. Legacy q99 AUC fields
  remain comparison-only and do not drive classification.
- Useful for response/BPI diagnostics.
- Not an independent identity source.

### `suite2p_presented_stimulus_response_23c.csv` and `suite2p_any_presented_response_23c.csv`

- Authority: early, pre-identity all-presented-stimulus response diagnostics and the fixed trace-panel cohort only.
- Per-stimulus and per-ROI screens use motion-window AUCs with a two-sided prestimulus null and one Bonferroni family across every presented stimulus and both tails.
- Explicit lower/upper thresholds, quantiles, signed pass flags, and valence own interpretation. `null_auc_threshold` and `null_quantile_familywise` are upper-tail compatibility aliases.
- These diagnostics do not replace `[50ia]` response/BPI fields or the ROI-centric master table.

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

## HCR geometry and quality authorities

- `register-hcr-to-anatomy/confocal/aligned/hcr_label_metrics.csv` owns per-label HCR size, measurement-space provenance, small-speck and low-confidence-segmentation flags, and table-driven analysis eligibility. QC notebooks only display it.
- Per-mask `*_matches.csv` and `*_review.csv` retain candidate pairs and explicit quality/radius gate failures. Per-mask `*_final_pairs.csv` contains only good one-to-one pairs passing both gates; the identity writer consumes these final pairs.
- `assign-hcr-identity/registration/functional_roi_activity_identity.csv` owns the identity-enriched ROI master. `score-activity-bpi` owns the identity-neutral response/BPI detail and scored cells. `export-canonical-tables` takes its ROI identity master from `assign-hcr-identity` and BPI cells/summary from `score-activity-bpi`; it must not replace identity with the score-stage detail table.

## HCR-centric identified-cell tables (non-authoritative for whole-population identity)

### `hcr_activity_status.csv`

- Authority:
  - identified-cell activity status written by `[50]`
- Identified-label activity/status view for HCR-centric analyses.
- Starts from accepted HCR/anatomy labels and then evaluates local functional ROI candidates; it is not a whole-population ROI inventory.

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

## Manual ex-vivo identity source (2026-10-05)

`tools/manual_hcr_identity.py` writes independent per-marker calls keyed by dataset, stack, and ex-vivo label. Reviewed geometry maps positives to in-vivo `anat_label`; old HCR-mask identity is not unioned. Canonical ROI schemas and keys remain unchanged. Complete calls (including negatives and unreviewed/unassessable states) and source hashes remain in sidecars. Legacy no-identity fields do not establish marker negativity.

`export-canonical-tables --manual-identity-root` creates an isolated ROI-only bundle with BPI cells/summary from the validated score root. It refuses HCR root mixing and existing output directories, and returns `warn` for its declared scope. HCR-centric tables/figures are unsupported until an anatomy-centric route is explicitly implemented; consumers retain their missing-input gates. Manual roots supplied as legacy HCR roots are rejected. See `docs/manual-hcr-identity-gui.md`.

## Single-fish early display summary and signed responses (2026-10-13)

`suite2p_stimulus_locked_summary_23b.csv` owns trace row keys, sessions, trial counts and fixed any-presented-stimulus classification; its numeric trace payload remains hash-bound to those rows. It no longer persists `mean_z_pre`, `mean_z_post`, `mean_dff_post`, or `peak_z_post`. Do not recover scalar measurements from its 50 s display traces.

`suite2p_presented_stimulus_response_23c.csv` owns signed `mean_auc_dff` and per-stimulus calls for P014 and the P018 population matrix. `suite2p_any_presented_response_23c.csv` owns P014's fixed responder membership. These remain early response diagnostics and do not replace the authoritative ROI identity master or [50ia] calls. The identity-functional loader takes the presented-response path explicitly and validates ROI/session agreement with the trace payload summary.
