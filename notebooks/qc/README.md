# Single-fish QC notebooks

These notebooks are read-only scientific review surfaces for persisted
single-fish pipeline outputs. Reusable loading, validation, filtering, and
plotting logic belongs in `src/codeants_2pf_hcr/`; notebook cells contain only
explicit configuration, package calls, and display calls.
Notebook 01 renders Q1 heatmap and trace figures from persisted numeric data,
not saved PNGs. Its trace loader verifies the numeric payload and summary came
from the same writer run; missing or mixed-generation inputs fail visibly.
Notebooks 03–05 reload the shared `spatial` dependency before their QC plot
modules when rerun in an existing kernel, avoiding stale package imports.

| Notebook | Review ownership | Decision gate |
| --- | --- | --- |
| `00_single_fish_pipeline_overview.ipynb` | fish/pipeline provenance and stage inventory | choose the next applicable review |
| `01_functional_reference_and_drift_qc.ipynb` | functional references, temporal Z-drift, early response evidence availability | session stable, corrected, material drift, or blocked |
| `02_functional_registration_qc.ipynb` | in-vivo anatomy preparation, functional-to-anatomy registration, transformed ROI labels | per-plane registration accepted, rejected, or review required |
| `03_roi_anatomy_geometry_qc.ipynb` | geometry-only ROI-to-anatomy assignments | freeze geometry or return it for review |
| `04_molecular_geometry_qc.ipynb` | ex-vivo/HCR segmentation and distinct bridge-registration tracks | molecular geometry accepted or review required |
| `05_molecular_identity_qc.ipynb` | molecular identity attached after frozen geometry | identities accepted, ambiguous, coexpressing, or unmatched |
| `06_activity_and_export_qc.ipynb` | response/BPI annotation, canonical tables, QA reports, and figures | final scientific interpretation ready or blocked |

## Safety contract

- Set `FISH_ID`, `LOCAL_ROOT`, and an explicit `PIPELINE_ROOT` for the exact
  persisted run under review. Do not use the default fish-local
  `pipeline_outputs/` root to review historical isolated runs.
- Fail visibly on missing or mismatched fish/manifests rather than falling back
  to another fish or pipeline root.
- Never run Cellpose, ANTs, matching, identity assignment, response scoring,
  canonical export, or promotion from a QC notebook.
- Notebook cells write nothing: no sidecars, tables, figures, manifests, or
  gate records. The agent discusses evidence with the user and records accepted
  decisions in wikiLogbook. Explicit pipeline CLI stages then materialize
  hash-bound computational inputs and derived tables outside the notebooks.
- Review geometry without gene, response, or BPI information. Review identity
  after geometry. Review response/BPI after geometry is frozen.
- ROI-centric tables remain authoritative for whole-population analysis;
  HCR-centric tables remain limited to identified-cell activity analysis.

The logbook owns gate decisions; the pipeline owns computational artifacts.
An executed notebook is evidence of inspection, never an acceptance signal.
For midline review, the pipeline projects persisted native-functional lines
to the anatomy grid, the logbook binds acceptance to the proposal CSV/PNG
hashes, and `materialize-anatomy-midline` creates the accepted input.
`make-motion-auc-tables` consumes that input and the same-run fixed 12.5-s
scored windows before notebook 06 displays laterality-dependent evidence.
Notebook 06 checks the pinned scoring-window policy and blocks the older
L765_f04 7-s AUC tables. The corrected response-window continuation lives at
`/Volumes/dataDrive/dataProcessing/2p_processing/_staged_pipeline_runs/20260917T1150Z_RESPONSE_WINDOW_RECOMPUTE/L765_f04`; it is a separate staged analysis, not an implicit replacement for the pinned registration-QC root.

For historical functional-registration runs that predate
`prepare-in-vivo-anatomy-stack`, notebook 02 records the explicitly selected
in-vivo anatomy NRRD as `legacy_unmanifested`; it does not fabricate a
preparation manifest or treat the anatomy as newly accepted.
