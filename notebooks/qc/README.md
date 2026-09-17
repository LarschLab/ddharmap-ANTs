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

Notebook 04 reads the backend `hcr_label_metrics.csv` to display retained large masks as low-confidence segmentation; its XY bounding-box quantiles are display diagnostics, not the eligibility rule. Notebook 05 reads candidate match-table gate flags for the radius audit and the identity writer's ROI master for actual assignments. Canonical export takes that identity-enriched master from the identity stage, with BPI cells/summary from the score stage.

For individual HCR-label inspection, the backend wrapper `tools/render_hcr_match_inspector.py` writes standalone HTML from a specified fish directory and pinned pipeline root. It shows every HCR label, saved anatomy candidate/final-pair status, size and XY-radius gates, same-grid mask overlays, and anatomy-linked functional ROI context. This is an optional review artifact, not a notebook output or gate decision. The grayscale backdrop is 2P anatomy intensity, not HCR fluorescence; missing native HCR masks remain marked as aligned-grid size measurements.

The logbook owns gate decisions; the pipeline owns computational artifacts.
An executed notebook is evidence of inspection, never an acceptance signal.
For midline review, the pipeline projects persisted native-functional lines
to the anatomy grid, the logbook binds acceptance to the proposal CSV/PNG
hashes, and `materialize-anatomy-midline` creates the accepted input.
`make-motion-auc-tables` consumes that input and the same-run fixed 12.5-s
scored windows before notebook 06 displays laterality-dependent evidence.
Notebook 06 checks the pinned scoring-window policy and blocks historical
7-s AUC tables. For the L765_f04 pipeline test, corrected scoring and every
dependent output are recomputed in the single pinned working root rather than
reviewed from a second pipeline root.

For historical functional-registration runs that predate
`prepare-in-vivo-anatomy-stack`, notebook 02 records the explicitly selected
in-vivo anatomy NRRD as `legacy_unmanifested`; it does not fabricate a
preparation manifest or treat the anatomy as newly accepted.
