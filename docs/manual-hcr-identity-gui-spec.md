# Manual HCR identity annotation GUI — specification v1

Date: 2026-10-05
Status: design baseline for implementation; no GUI or new identity outputs implemented yet.
Repository: Linnaeus `/Users/ddharmap/gitRepo/codeANTs`.
Design branch: `codex/manual-hcr-identity-gui`, based on newest local work branch `codex/qc-notebooks`, commit `c2a0716` (after refreshing origin refs).

## Purpose and agreed scope

Assign molecular identity from HCR fluorescence to existing, pipeline-filtered ex-vivo 2P GCaMP anatomy masks. Annotation starts from scratch and is performed independently for each marker stack. A cell may be positive for multiple markers. Confocal HCR segmentation is not required for annotation or assignment.

The scientific motivation is the 2026-09-18 Johannes meeting: HCR-mask segmentation limits identity recovery; ex-vivo anatomy offers more reliable cell boundaries, and independent manual marker review can preserve blinding. The current accepted L765_f04 assignments remain the baseline for later comparison.

Use napari as the viewer with a small custom annotation panel. Retain Cellpose as the existing segmentation producer. V1 does not edit, split, merge, or rerun anatomy masks and does not introduce new mask-size/noise thresholds. Existing pipeline eligibility is authoritative. A reviewer can flag a residual problem without changing the label image or silently redefining eligibility.

## Review unit and decisions

The stable annotation key is `(dataset_id, marker_stack_id, exvivo_label_id)`. The dataset manifest binds the key to fish, anatomy image, label image, eligibility source, and registration provenance. Numeric label IDs are meaningful only within that exact label image.

Each marker stack has an explicit gene, imaging round, source channel, and stable stack ID. Repeated acquisitions of one gene remain separate review sessions; gene names alone are not stack identifiers.

| Call | Meaning | Eligible for positive identity export |
| --- | --- | --- |
| unreviewed | No decision recorded | No |
| positive | Reviewer assigns this marker's signal to this anatomy cell | Yes, subject to approved geometry |
| negative | Assessable evidence supports absence of marker signal | No |
| uncertain | Signal is assessable but cell attribution or positivity is ambiguous | No |
| unassessable | Coverage, staining, registration, or image quality prevents a decision | No |

Negative is not equivalent to unreviewed, uncertain, or unassessable. All states survive in annotation exports even though legacy identity fields represent positive membership only.

A separate optional issue flag records concerns such as merged anatomy label, registration mismatch, poor coverage, or bleed-through. A blocking issue prevents identity export until resolved. Such flags do not modify upstream eligibility. Notes are optional; no mandatory confidence score in V1.

## Inputs and coordinate contract

The GUI opens an explicit prepared-dataset manifest rather than discovering arbitrary files. It contains:

- Fish ID, dataset ID, schema version, units, axes, and named coordinate frame.
- Prepared/manual-oriented ex-vivo 2P GCaMP intensity and its matching Cellpose label image, with paths and content hashes.
- An authoritative eligible-label inventory and the existing filtering source/policy provenance. Reuse the pipeline's eligibility decision; if it is not persisted, preparation must materialize that existing decision without changing its semantics.
- One or more registered HCR intensity stacks, each identified by gene, round, source channel, source hash, and transform-chain provenance.
- Explicit per-stack coverage or valid sampling region, so out-of-field/padded pixels cannot imply marker negativity.
- Optional frozen ex-vivo-to-in-vivo label correspondence artifact for downstream export. Its absence permits annotation but blocks canonical identity export.

Anatomy intensity, anatomy labels, and the displayed marker must share the exact ex-vivo grid: size, spacing, origin, direction, and explicit array axes. Verify physical geometry, not just shape. Existing orientation metadata must resolve any label/intensity Z convention. No implicit flips, guessed axes, or display-only alignments.

Prepare HCR intensity using the existing per-round registration chain into ex-vivo space (direct round-to-ex-vivo or documented round-to-rbest-to-ex-vivo). Preserve fluorescence dynamic range; display normalization must not modify saved evidence. Intensity resampling uses a documented intensity interpolator; any label transformation uses nearest-neighbor interpolation.

Missing/ambiguous transforms or grid metadata block loading the affected stack with a specific diagnostic. Registration preparation is separate from the viewer and does not silently rerun registration.

## Reviewer interaction

1. Open a dataset and select one marker stack.
2. Show grayscale GCaMP anatomy, the current HCR intensity, and fixed anatomy outlines. Independently control intensity contrast, opacity, and layer visibility.
3. Click an eligible mask or choose a cell from the queue. Highlight the selected label across Z and center the view on it; retain surrounding tissue and neighboring outlines.
4. Scroll through XY slices and inspect linked XZ/YZ views. Preserve physical aspect ratios. A full 3D rendering is optional, not the primary decision view.
5. Assign a call using panel controls or documented shortcuts. Advance to the next unreviewed cell while retaining convenient navigation back to previous cells.
6. Autosave each call and support undo/redo. Resume with the same marker, selected cell, and review progress.

Queue filters: unreviewed, positive, negative, uncertain, unassessable, and issue-flagged. Show total eligible cells, reviewed cells, and counts by call; no response-based or functional-presence ordering.

Keep functional ROIs, activity/BPI, accepted historical identities, other marker intensities, and other marker calls hidden during independent review. A separate post-review summary can show coexpression and baseline comparisons.

HCR may lie around a cell rather than inside its GCaMP mask. Always show surrounding signal and adjacent cells. Optional intensity summaries can be added later; V1 does not prescribe a dilation shell, threshold, automatic positivity classifier, or automatic identity suggestion.

## Annotation persistence

The prepared input manifest is immutable for a review dataset. Store annotation files in an isolated analysis/run directory, proposed as `03_analysis/structural/ex_vivo/manual_identity/<dataset_id>/`, consistent with repository ex-vivo ownership.

Required artifacts:

- `dataset_manifest.json`: input identities, hashes, physical geometry, coverage, eligibility, and transforms.
- `annotations.csv`: one current row per eligible cell per loaded marker stack, including unreviewed rows.
- `annotation_events.jsonl`: append-only record of decisions and undo/redo changes, with event IDs and old/new values.
- `session.json`: application/schema version, reviewer, timestamps, current marker/cell, view settings, and latest persisted revision.

Current annotation columns: `schema_version`, `dataset_id`, `fish_id`, `marker_stack_id`, `gene`, `round_id`, `source_channel`, `exvivo_label_id`, `call`, `issue_code`, `issue_blocks_export`, `note`, `reviewer`, `updated_at_utc`, `revision`.

Use atomic snapshot writes and a recoverable event log. Detect another writer and refuse concurrent editing of one session. Explicit export produces a versioned frozen annotation snapshot. Input hash/eligibility mismatches block resuming calls onto a changed dataset; migration must be explicit.

## Downstream compatibility

### Existing contracts verified in the repository

`matching.build_anat_identity_lookup_df` currently produces `anat_label`, `identity_label`, `identity_gene_count`, and `identity_genes` from accepted HCR-mask final pairs. Multiple genes are supported; the identity string is slash-separated in configured gene order.

`matching.attach_identity_to_functional_roi_geometry_df` joins that lookup onto existing anatomy/functional geometry. Whole-population authority remains `functional_roi_activity_identity.csv`, one row per `(plane_idx, func_label)`. `assign-hcr-identity` owns identity, and `export-canonical-tables` combines identity with independently scored activity/BPI.

The current staged identity writer also builds HCR-centric status/candidate/trace tables from confocal mask final pairs. A manual anatomy annotation is not a confocal mask and cannot transparently substitute for those inputs.

### Required new adapter boundary

1. Read a frozen annotation snapshot. Only positive calls without blocking issues contribute identities.
2. Resolve repeated stacks of one gene explicitly. V1 uses a manifest-declared primary stack for each gene; contradictory reviewed replicates block that cell/gene's export pending resolution. Never silently use the last row or OR conflicting calls.
3. Join through a frozen, independently reviewed geometry correspondence from ex-vivo labels to existing in-vivo anatomy IDs. The correspondence must identify both label-image hashes, transform chain, matched label IDs, geometry metrics, acceptance status, and reasons for unmatched/ambiguous cases. Do not assume numerical IDs match across spaces.
4. Use geometry-only criteria and existing applicable policy. Ambiguous, conflicting, or unmatched correspondences remain in a mapping audit and contribute no canonical positive identity. Different ex-vivo cells competing for one in-vivo cell require explicit geometry resolution before export; never union their markers automatically.
5. Aggregate accepted genes by in-vivo `anat_label` using the existing configured gene ordering and lookup schema. Manual calls form a distinct identity source, not an implicit merge with historical HCR-mask calls.
6. Invoke the identity attachment boundary against frozen ROI/anatomy geometry. Preserve ROI inventory, keys, geometric columns, and response/BPI evidence. Do not use marker calls or functional responses to choose correspondence.
7. Persist manual-source provenance, full call states, mapping outcomes, and source snapshot hash in sidecars. Legacy no-identity fields must not be interpreted as confirmed marker negativity.

Add explicit identity-source selection to the owning stage (proposed values `hcr_masks` and `manual_exvivo`). Existing default behavior remains intact. Manual exports are produced in a separate pipeline run root for comparison before any accepted-output replacement.

Preserve downstream filenames, columns, coexpression formatting, and row keys for the ROI-centric route. The adapter must be validated through canonical export and its first consuming figure/cohort stage, not merely by emitting a look-alike CSV.

HCR-centric compatibility requires an explicit consumer audit: retain legacy exports only with visible legacy-source provenance, or provide a separate anatomy-centric manual export and update consumers to select it. Do not fabricate confocal label IDs, final-pair files, or claim complete HCR-centric support from ROI-table compatibility. Mixed identity sources must not pass as one coherent run. This audit is a required implementation milestone before end-to-end release.

## Architecture and implementation order

Keep the program inside codeANTs with extractable components:

1. Headless package-owned manifest, annotations, validation, and persistence logic.
2. Thin napari app under `src/codeants_2pf_hcr/apps/` and a launcher under `tools/`.
3. Separate package-owned dataset preparation and identity export adapters.

Napari/Qt remain optional imports so pipeline and headless utilities work without a GUI environment. Reuse registration and eligibility authorities rather than duplicating scientific logic.

Build milestones:

1. Audit one pilot fish's ex-vivo masks, authoritative eligibility, marker intensities, coverage, and registration assets; select the pilot based on verified availability. L765_f04 is the preferred baseline-comparison candidate, not an assumed ready input.
2. Implement the read-only stack viewer and fixed-mask cell selection.
3. Implement independent per-stack calls, queue, autosave, recovery, and frozen export.
4. Validate the ex-vivo/in-vivo correspondence and manual identity adapter; audit HCR-centric consumers.
5. Compare manual and historical identity in an isolated run and complete downstream checks before seeking scientific acceptance.

## Acceptance criteria

- Native and registered input geometry/coverage are verified; known landmarks and selected masks align in XY/XZ/YZ on rendered inspection.
- Existing eligible anatomy labels are preserved; annotation changes neither source masks nor eligibility policy.
- A real cell can receive independent positives for two markers; negative, uncertain, unassessable, and unreviewed remain distinguishable after restart/export.
- Review is independent of functional information, historical calls, and other marker decisions.
- Save/resume, undo/redo, interrupted-write recovery, duplicate-session prevention, and input-change rejection work on a representative dataset.
- Identity export rejects ambiguous mappings and conflicting evidence, preserves all functional ROI keys and frozen geometry, and reproduces configured coexpression formatting.
- Canonical export plus first downstream consumers accept the resulting manual-source ROI tables; unsupported or mixed-source HCR-centric outputs are explicitly blocked or correctly routed.
- A comparison report records annotation completion, positive counts, mapping losses, and agreement/disagreement with the accepted baseline without automatically promoting new calls.

## Defaults and remaining implementation checks

No further user decision is required to begin building from this specification. Defaults: napari, fixed eligible anatomy masks, independent marker review, five call states, no automated suggestions, separate manual-source exports.

Asset availability, persisted eligibility, geometry-correspondence readiness, runtime dependencies, and HCR-centric consumer routing are technical checks to resolve during implementation. They are not presumed solved by this document. If those checks expose a new scientific threshold or identity-resolution decision, document and discuss it before changing scientific semantics.
