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
- This is a conservative trace-quality gate, not a biological inactivity claim.

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

### HCR-centric (identified-cell activity only)

- Starts from accepted HCR↔anatomy labels, then finds local ROI candidates.
- Used for identified-cell response status and trace-ready exports.
- Prioritizes recovering responses for identified labels.
- Must not silently replace the ROI-centric authoritative path.

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
