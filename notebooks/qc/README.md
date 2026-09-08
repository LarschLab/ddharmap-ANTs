# Single-fish QC notebooks

These notebooks are read-only scientific review surfaces for persisted
single-fish pipeline outputs. Reusable loading, validation, filtering, and
plotting logic belongs in `src/codeants_2pf_hcr/`; notebook cells contain only
explicit configuration, package calls, and display/save controls.

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
- The only permitted write is an explicit review JSON sidecar beneath the
  reviewed stage's `reviews/` directory. It is hash-bound to reviewed
  artifacts and does not promote them.
- Review geometry without gene, response, or BPI information. Review identity
  after geometry. Review response/BPI after geometry is frozen.
- ROI-centric tables remain authoritative for whole-population analysis;
  HCR-centric tables remain limited to identified-cell activity analysis.

Review records are operational evidence. Reconcile accepted decisions through
the lab logbook and an explicit promotion step; never infer acceptance from a
notebook having executed successfully.

For historical functional-registration runs that predate
`prepare-in-vivo-anatomy-stack`, notebook 02 records the explicitly selected
in-vivo anatomy NRRD as `legacy_unmanifested`; it does not fabricate a
preparation manifest or treat the anatomy as newly accepted.
