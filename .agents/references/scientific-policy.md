# Scientific Policy

**Purpose:** preserve scientific intent and matching semantics.

**Use this file when:** changing matching, identity assignment, analysis scope, or policy wording.

## Scientific goal

Map functional responses to molecular identity by placing three segmented modalities in shared 2P anatomy space:

- functional Suite2p ROIs
- 2P anatomy labels
- HCR/confocal masks with gene identity

## Core principle

The primary analysis unit is the **functional ROI**. The authoritative whole-population view is one row per functional ROI with geometry, identity, response, and BPI annotations layered in order.

Current conservative policy:

- `suite2p_is_cell=True` is treated as high-quality trace provenance.
- `suite2p_is_cell=False` remains in the master ROI inventory but is currently labeled low-quality trace and marked response-unavailable in `[50ia]`.
- This is a conservative trace-quality gate: these rows are treated as having no extractable cellular response information for the current trace analysis, not as a biological inactivity or absence claim.

## Authoritative matching policy

1. Match functional ROIs to anatomy labels in shared anatomy space.
2. Decide geometry using geometric evidence only (overlap, distance, plausibility, assignment constraints).
3. Do not use activity, BPI, or identity to decide geometry.
4. Allow unmatched ROIs.
5. Assign molecular identity after geometry.
6. Add activity/BPI after geometry.
7. Downstream figures filter authoritative tables; they do not rebuild identity.

Short form: **geometry first, identity second, activity third, figures last**.

## Dual analysis paths

### ROI-centric (authoritative for whole-population)

- Canonical path for whole-population QA/activity/BPI summaries.
- Authoritative table: `functional_roi_activity_identity.csv`.
- Starts from the global functional ROI inventory, including Suite2p rows with `iscell=0`.
- Response classification then separates response-unavailable rows, low-activity/non-threshold-passing rows, and responsive/high-quality signal rows after geometry is fixed.

### HCR-centric (identified-cell activity only)

- Starts from accepted HCR↔anatomy labels, then finds local ROI candidates.
- Used for identified-cell response status and trace-ready exports.
- Prioritizes recovering responses for identified labels.
- Its candidate universe is intentionally label-first and local to identified HCR/anatomy labels; it should not be forced to reuse the ROI-centric one-to-one competition table when that would suppress identified cells of interest.
- Must not silently replace the ROI-centric authoritative path.

## HCR label quality and anatomy plausibility

- `register-hcr-to-anatomy` writes one `hcr_label_metrics.csv` row per HCR label, keyed by fish, mask identity, and label ID. It retains one aligned label stack per mask; no filtered duplicate label stack is needed.
- The 200-native-voxel floor is expressed as a physical-volume threshold using the native HCR intensity spacing. When the native mask is unavailable, the backend measures the retained aligned stack using anatomy-grid spacing and records `measurement_space=aligned_2p`; this is a provenance-limited approximation, not a recovered native segmentation.
- Labels below the small-speck floor are excluded. Among non-small labels in each mask, equivalent-volume diameters above q95 are retained as `is_low_confidence_segmentation` but excluded from accepted identity and downstream functional claims. The q95 is not a bounding-box XY threshold.
- HCR/anatomy candidates must also have lateral centroid XY distance no greater than the fish-specific median equivalent XY radius of every anatomy label's maximal 2-D section across the complete 3-D label stack. The radius is computed with physical XY spacing, never hardcoded, and never applied to axial or 3-D distance.
- All failed candidates remain in the match/review audit tables with explicit gate flags and exclusion reasons. Only good one-to-one pairs that pass both quality and anatomy-radius gates enter `*_final_pairs.csv`. Response/BPI never changes these geometric or segmentation gates.

## Ambiguity ladder

- ROI-centric vs HCR-centric meaning: start here, then confirm migration caveats in `current-state.md`.
- Activity wording: defer to `activity-semantics.md`.
- Output ownership: defer to `canonical-tables.md`.
- If code conflicts with policy docs, follow policy docs unless they are clearly outdated.

## Why this split exists

Historical ROI-first and label-first flows can disagree under competition. ROI-centric remains authoritative for whole-population inference; HCR-centric remains valid for identified-cell activity questions.

## Modalities and spaces

- Functional space: functional images + Suite2p ROIs.
- Anatomy space: segmented 2P anatomy; common matching space.
- HCR/confocal space: gene masks before warping.

Final matching decisions are expressed in anatomy space.

## Functional imaging sessions and planes

Some fish are acquired in more than one functional imaging session. After the first session, the Z focus can be shifted so that a new set of functional planes is visible. These later-session planes are scientifically separate planes, not repeated views of the earlier-session planes.

Policy:

- Plane indices are global within a fish after preprocessing.
- Session 1 (`r1`) owns its output planes, typically `0-4` for five-plane acquisitions.
- Session 2 (`r2`) owns its output planes, typically `5-9` when a second five-plane acquisition is appended after a Z-focus shift.
- The preprocessing metadata is the authority for the exact session-to-plane mapping; do not infer it from activity, identity, or downstream tables.
- Each plane must use the experiment log and metadata from the imaging session that produced that plane.
- Response/BPI and stimulus-window analyses are plane-session aware: r2 planes use r2 timing/metadata, r1 planes use r1 timing/metadata.
- Geometry matching remains per plane in shared anatomy space. Sessions do not change the rule that geometry is decided before identity and activity.
- Do not merge planes across sessions by local plane number. Use the global `plane_idx`/`plane` assigned after preprocessing.
- Figures and exports must preserve the global plane identity or explicitly document any session grouping.

## Matching guidance for refactors

Preferred structure:

1. Build plausible ROI↔anatomy candidates.
2. Gate by overlap and distance.
3. Score overlap-first with distance secondary.
4. Solve one-to-one assignment where required.
5. Allow unmatched ROIs.
6. Annotate matched anatomy with identity afterward.

## Decision rule when code and docs disagree

Refactor toward this policy while preserving reproducibility; do not introduce new analyses that depend on historical ambiguity.
