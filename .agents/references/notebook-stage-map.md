# Notebook Stage Map

**Purpose:** navigation map for `notebooks/singleFish.ipynb`.

**Use this file when:** locating stage ownership, key cell tags, or expected stage outputs.

## End-to-end stages

1. **Setup and fish-scoped paths** (`[1]-[5]`)
   - Imports, fish context, run configuration.
   - Key vars: `FISH_ID`, `RUN_CONFIG`, `NAS_ROOT`, `FISH_DIR`, `OUTDIR`, `OUT_REG`, `OUT_QA`, `OUT_DERIVED`.
2. **Early functional response readout** (`[23a] [23c]`)
   - Suite2p trace/ROI load and full-experiment fish response quality diagnostics before anatomy or functional preprocessing.
   - `[23a]` can run without `plane_refs`; when functional references are not available yet, Suite2p planes are keyed by discovered Suite2p plane index.
   - `[23c]` places metadata/log blocks on equal Suite2p frame-count boundaries while preserving stimulus offsets within each block; `planned_schedule.csv` is authoritative for no-stimulus rest blocks when present.
   - `[23c]` uses preprocessing metadata for plane-to-session mapping when present; if it is absent but explicit `r1`/`r2` stimulus logs exist and Suite2p planes split evenly, it infers a contiguous equal split by session order.
   - `[23c]` computes pre-identity Suite2p response/BPI calls for trace-panel filtering; these outputs are non-canonical and later merge into `[50ia]`.
   - Key object: `suite2p_by_ref_idx`.
3. **Spatial preparation** (`[13] [14] [14a] [7] [9] [11] [15] [19] [19a] [21] [22d] [24a] [22e]`)
   - Orientation, voxel alignment, best-z/scale search, in-plane placement comparison, regional crop selection, QA overlays.
   - `[8]` resolves anatomy Z from fish metadata `step_size_um_anatomy`; TIFF page-count Z is not authoritative for the anatomy stack.
   - `[10]` is legacy compatibility only when the calcium preprocessing spatial manifest is absent. Canonical functional movies are already oriented before Suite2p and must not be copied or flipped again.
   - Raw-anatomy polarity inference is available as a separate review-only package/CLI audit. It compares both existing north/south flip conventions against manually labeled anatomy references, uses acquisition-series-held-out calibration, can abstain, and writes CSV/PNG evidence without editing metadata or changing canonical preprocessing inputs. Interleaved `L427` anatomy is excluded until channel deinterleaving is explicitly supported.
   - `[12]` builds/reuses 2D references from canonical motion-corrected stacks without another XY transform. Explicit legacy acquisition products retain the direct north=`flipY`/south=`flipX` compatibility path. First-block exclusion semantics remain unchanged.
   - `[14]`/`[14a]` consume the upstream registration-ready anatomy NRRD when the spatial manifest is present and perform no image transform or rewrite. The historical signed correction, XY orientation, registration-Z flip, and 750x750 resize remain only for explicitly legacy acquisition inputs.
   - Experimental ex vivo 2P anatomy bridge preparation is package-owned in `context.py`: raw ex vivo stacks from `01_raw/2p/anatomy` can be converted to isolated NRRD/JSON outputs with mirrored-2P X flip, registration-convention Z flip, and default `750x750` Y/X resizing; `tools/ex_vivo_manual_orientation_gui.py` provides the interactive review step and a separate manual-orientation helper applies brainAtlas-style preview-angle rotation/crop plus explicit flips before rbest->ex vivo and ex vivo->in vivo registration trials. New writer stages should use concrete names such as `prepare-ex-vivo-anatomy-stack` and place ex vivo analysis artifacts under `03_analysis/structural/ex_vivo/`; these outputs are not canonical `[14a]` in vivo anatomy, must not silently replace `ANAT_STACK_PATH`, and intentionally avoid duplicate TIFF image outputs.
   - `[19a]` writes NCC-guided per-plane fixed anatomy-space squares for masked ANTs in-plane registration; `[20]` uses NCC placement as the deterministic initializer for ANTs rigid+affine refinement. ANTs failure is terminal for this stage; NCC is not accepted as a final fallback transform.
   - Experimental within-session Z-drift diagnosis is package-owned in `z_drift.py` and exposed by `tools/diagnose_functional_z_drift.py`. It requires the accepted functional-reference manifest, fails closed unless fish identity/status/north-or-south polarity and per-plane first-block-exclusion provenance are valid, builds correctly oriented time-windowed references from the same post-block-0 frames as the canonical reference, labels plots by retained acquisition block and within-block window, reuses the persisted per-plane scale, and scores anatomy Z only; its isolated outputs are review evidence and do not replace canonical functional references or transforms.
   - The staged CLI separates functional registration into three ordered writers: `register-functional-to-anatomy` consumes the canonical version-4 preprocessing NCC handoff when present (saved pooled reference, scale, complete best-Z profile, and XY placement), skips the duplicated NCC search, and writes the ANTs-refined composed transform; its legacy reference/cache route remains available for older fish. `transform-functional-rois-to-anatomy` applies that exact transform to Suite2p labels, and `make-functional-registration-qc` renders intensity, label-overlay, plane-row, NCC-profile, and native-functional fixed-consensus-midline PNGs only after transformed labels exist. The midline proposal uses the registered plane summary's normalized functional references and preprocessing session-to-plane mapping; it is review-only and never supplies anatomy-space laterality.
   - `[24a]` runs before `[22e]` so the regional review can include anatomy-label boundaries; `[22e]` also runs after Suite2p loading so the same review can include ROI boundaries.
   - `[22e]` infers whether the anatomy-label stack uses direct or reversed Z-page indexing against the anatomy intensity stack, then records `anat_label_z_mode` in `plane_refs` for downstream anatomy-label consumers.
   - Key object: `plane_refs`.
4. **Functional ROI geometry QA** (`[23b] [25] [29] [33] [34a]`)
   - Suite2p orientation QC, functional labels on references, and geometry QC helpers.
   - QC helpers only; not identity source.
   - Diameter and regional-review support cells `[30]` and `[34c]` are package-owned migration wrappers.
   - Native segmentation stages `[24]` and `[24a]` are package-owned and should remain orchestration-thin in the notebook; `[24a]` is ordered earlier because `[22e]` consumes `ANAT_LABELS_PATH`.
5. **HCR discovery/warp/HCR↔anatomy QC** (`[37] [39] [41] [42] [43] [44]`)
   - HCR mask discovery, warping, QC summaries.
   - Key object: `hcr_match_results`.
   - HCR discovery/config/metadata/matching support cells `[38]`, `[40]`, `[41]`, `[44]`, `[46]`, `[47]`, and `[47b]` are now package-owned migration wrappers; notebook cells should stay def-free.
   - External-BigWarp prep stages `[43]` and `[43b]` are package-owned wrappers with stage-local ANTs imports.
6. **HCR-centric identified-cell activity mapping** (`[50]`)
   - Builds `hcr_activity_status.csv`, `conf_to_func_pairs_raw.csv`, response-positive `conf_to_func_pairs.csv`.
   - Owns identified-cell activity export semantics.
7. **ROI-centric whole-population matching** (`[50h] [50i]`)
   - Authoritative ROI↔anatomy matching and identity attachment.
   - Owns geometry matching semantics.
8. **Activity/BPI annotation** (`[50ia]`)
   - Response/BPI annotations merged onto frozen ROI/anatomy geometry, reusing pre-identity `[23c]` response calls when available; molecular identity is not an input dependency.
   - Owns response semantics after geometry is fixed.
9. **Population figures** (`[50e] [50j] [50k]`)
   - Table-driven summaries.
   - Must filter canonical tables; they do not own semantic definitions.
10. **Trace export and stimulus-aligned analyses** (`[50l] [51] [55] [56] [56h] [56g] [57a-responsive-identity-donut] [57b-anatomy-coexpression-summary] [57]`)
    - Trace export, stimulus alignment, full-session and diagnostics figures.
    - `[56h]` also writes `sst12_contra_continuous_event_traces_56h.png/.pdf`, an auto-selected `sst1.2` contra-continuous all-events sanity trace.
    - `[56h]` also writes `poster_single_row_average_traces_56h.png/.pdf`, a one-row poster trace figure with condition blocks adjacent and fixed gene colors.
    - Late trace/figure migration wrappers include `[51]`, `[54]`, and `[56d]`; their notebook cells should contain only the public package call and display/binding code.

## Core outputs by stage

- ROI-centric authoritative table: `functional_roi_activity_identity.csv` (`[50i]` + `[50ia]`).
- Response/BPI exports: `functional_roi_activity_bpi_cells.csv`, `functional_roi_activity_bpi_summary.csv` (`[50ia]`).
- Response-scoring timing provenance: `scored_stimulus_windows.csv` (`score-activity-bpi`); it records the exact frame windows consumed by scoring for independent Notebook 01 audit.
- Pre-identity response/BPI diagnostics: `suite2p_response_bpi_cells_23c.csv`, `suite2p_response_bpi_summary_23c.csv` (`[23c]`, non-canonical).
- Full-session QC payload: `suite2p_full_session_heatmap_matrix_23c.npy` plus `suite2p_full_session_heatmap_rows_23c.csv` (`[23c]`); Notebook 01 uses these persisted traces with separately loaded raw-log and score-window overlays.
- HCR-centric identified-cell outputs: `hcr_activity_status.csv`, `conf_to_func_pairs.csv`, `hcr_func_candidates.csv` (`[50]`).
- Single-fish donut summary output: `hcr_activity_status_summary.csv` (`[50e]`), including per-gene inner/outer status counts and unmatched rows.
- Single-fish `[50l]` composite output: `compound_50j_56i_unified.png/.pdf`, package-rendered by `plots.analysis.render_single_fish_50l_composite` from `[50ia]` response/BPI outputs plus `[56i]` motion-AUC point/count tables; poster-scale standalone population donut output: `poster_50l_population_response_donut.png/.pdf` plus counts CSV, package-rendered by `plots.analysis.render_single_fish_50l_population_response_donut_poster` from `[50ia]`.
- Single-fish responsive hybrid donut outputs: `single_fish_50l_responsive_identity_donut.png/.pdf`, plus long/wide counts CSVs (`[57a-responsive-identity-donut]`).
- Single-fish anatomy-label co-expression outputs: `single_fish_hcr_anatomy_coexpression_summary.png/.pdf` plus summary/combo-count CSVs (`[57b-anatomy-coexpression-summary]`).
- Staged pipeline status inventories selected late-output folders under `03_analysis/functional/pipeline_outputs/`: `assign-hcr-identity/registration/`, `score-activity-bpi/registration/`, `export-canonical-tables/registration/`, `make-qa-report/`, and `make-figures/04_plots/`. These stages now have package-owned writer commands plus read-only status/comparison surfaces; remaining migration work is stage-specific promotion of upstream writers, richer QA, freshness checks, and figure render replacements.

## Concept ownership

- Whole-population identity -> `functional_roi_activity_identity.csv`.
- Identified-cell activity export -> HCR-centric outputs from `[50]`.
- Response semantics -> `[50ia]` / activity stage.
- Geometry matching -> matching stage only.
- Figure semantics -> downstream table filtering only; figures do not infer identity or response state.

## Read-only QC notebook suite

Dedicated review notebooks under `notebooks/qc/` consume persisted stage
outputs without recomputing or promoting them. They are grouped by human
scientific gate rather than one notebook per executable stage:

1. `00_single_fish_pipeline_overview.ipynb`: provenance and stage inventory.
2. `01_functional_reference_and_drift_qc.ipynb`: functional-reference,
   temporal Z-drift, and early response-evidence availability review.
3. `02_functional_registration_qc.ipynb`: anatomy preparation,
   functional-to-anatomy registration, and transformed-label review.
4. `03_roi_anatomy_geometry_qc.ipynb`: geometry-only ROI/anatomy matching and
   ambiguity/unmatched review before geometry is frozen. Spatial views consume
   `match-roi-to-anatomy/registration/plane_refs_summary_geometry.json`, whose
   explicit per-plane anatomy-label Z pages must agree with the selected labels
   in the geometry table.
5. `04_molecular_geometry_qc.ipynb`: direct in-vivo HCR-label inventory,
   dynamic anatomy-plane label-overlay review, and distinct molecular
   bridge-registration tracks. It must distinguish available transformed masks
   from molecular/anatomy candidate matching that has not yet been run.
6. `05_molecular_identity_qc.ipynb`: identity review after frozen geometry, including read-only lateral-XY-distance and signed-Z-offset violins by gene for accepted pairs.
7. `06_activity_and_export_qc.ipynb`: independent response/BPI review from the
   `score-activity-bpi` stage before canonical export, with separate optional
   HCR identity context and preserved ROI-centric/HCR-centric scopes. It also
   renders the required read-only all-ROI and persisted-response-subset
   anatomy-space midline consequence grids only from a complete manually
   accepted hash-bound sidecar; laterality-dependent AUC stays out of scope.

Reusable QC loading, provenance, filtering, and plotting logic is package
owned. Notebook cells contain explicit knobs and package calls only. An
explicit save may write a hash-bound review JSON beneath the reviewed stage's
`reviews/` directory; it never writes or promotes scientific outputs.

## Navigation notes

- For module ownership and functions: see `symbol-index.md`.
- For table meaning and allowed usage: see `canonical-tables.md`.
- For migration caveats in downstream cells: see `current-state.md`.
- `notebooks/hcr_activity_replay_qa.ipynb` is a recall/QA notebook for read-only HCR activity replay manifests, candidate tables, and historical warped-label artifacts. It is not a stage owner; reusable replay logic remains package-owned.
