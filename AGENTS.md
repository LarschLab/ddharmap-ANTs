# Notebook Pipeline Policy

This file is the authoritative guidance for LLMs and humans working on [`notebooks/2PF_to_HCR.ipynb`](/Users/ddharmap/gitRepo/codeANTs/notebooks/2PF_to_HCR.ipynb).

Use it to interpret the notebook, add new analyses, and refactor old figures without reintroducing ambiguity about which matching pipeline is correct.

## Scientific Goal

The goal of this notebook is to map functional responses to molecular identities by bringing three segmented modalities into the same 2P anatomy space:

- functional ROIs from Suite2p
- 2P anatomy labels
- HCR/confocal masks with gene identity

The intended output is a consistent per-functional-ROI view of:

- where each ROI sits in anatomy space
- whether it matches an anatomy label
- whether that anatomy label has a molecular identity
- whether the ROI is active or inactive
- how the ROI behaves in stimulus-aligned and full-session analyses

## Core Principle

The primary analysis unit is the functional ROI.

Everything downstream should be derived from one authoritative table with one row per functional ROI. Molecular identity, activity state, BPI, and figure membership are all annotations on that same ROI table.

There is one important exception for identified-cell activity analyses:

- whole-population summaries stay ROI-centric
- identified-cell activity and trace analyses may use an HCR-centric table keyed by accepted HCR/anatomy labels

That HCR-centric path exists to answer a different question:

- ROI-centric: which anatomy/identity belongs to each functional ROI?
- HCR-centric: for each identified anatomical cell, is there a functional ROI and is it active?

## Authoritative Matching Policy

This is the policy that should govern the entire notebook, even if some older cells still reflect an earlier approach.

1. Match all functional ROIs to all anatomy labels on the relevant anatomy plane.
2. Perform this matching in shared anatomy space only.
3. Use geometric evidence only for the match itself:
   - overlap
   - centroid distance
   - mask size / plausibility checks
   - one-to-one assignment
4. Do not use activity, BPI, or gene identity to decide the geometry match.
5. Allow ROIs to remain unmatched when no plausible anatomy partner exists.
6. Assign molecular identity only after a functional ROI has been matched to an anatomy label.
7. Add activity and BPI only after the geometry match is fixed.
8. Downstream figures must filter the master ROI table. They must not rebuild identity with a second competing matching pass.

In plain language:

- geometry first
- identity second
- activity third
- figures last

## Dual Analysis Paths

The notebook now intentionally supports two analysis paths.

### 1. ROI-Centric Whole-Population Path

This remains the authoritative geometric matching path.

Use it for:

- whole-population QA
- whole-population activity summaries
- BPI analyses
- any figure where one-to-one ROI↔anatomy competition matters

Canonical table:

- `functional_roi_activity_identity.csv`

### 2. HCR-Centric Identified-Cell Activity Path

This path starts from accepted HCR↔anatomy matches, then asks whether the matched anatomy label is represented on functional planes and whether an active or inactive ROI candidate exists.

Use it for:

- identified-cell active vs inactive summaries
- HCR-focused within-plane vs out-of-plane summaries
- trace export and stimulus/full-session activity analyses for identified cells

Important rules for this path:

- HCR↔anatomy matching still happens first
- functional ROI candidates are evaluated using geometry only
- active ROI candidates are preferred over inactive ROI candidates for identified-cell activity export
- this path does not use global ROI competition to block an active ROI when an inactive ROI also overlaps the same anatomy label
- reused ROIs must be flagged explicitly, and downstream averaging should deduplicate by `(gene, plane, func_label)` to avoid double-counting fragmented labels

Canonical tables for this path:

- `hcr_activity_status.csv`
- `conf_to_func_pairs.csv` for active trace-ready mappings

## Why This Policy Exists

The notebook currently contains two historical patterns:

- a label-first path centered on `conf_to_func_pairs.csv`
- an ROI-first path centered on `functional_roi_activity_identity.csv`

These can disagree for the same ROI, because they do not resolve competition in the same direction.

This policy chooses the ROI-centric whole-population path as the authoritative design, because the scientific question is ultimately about which functional cells correspond to which molecular identities.

## Modalities And Spaces

Keep these spaces distinct:

- Functional space: raw and oriented 2P functional images and Suite2p ROIs.
- Anatomy space: segmented 2P anatomy stack. This is the shared target space for matching.
- HCR/confocal space: gene-labeled confocal masks before warping.

The notebook warps or maps everything into anatomy space. All final matching decisions should be expressed there.

## End-To-End Data Flow

The notebook is organized as a staged pipeline.

### 1. Setup And Fish-Scoped Paths

Cells:

- `[1]` to `[5]`

Purpose:

- import dependencies
- resolve fish-specific paths
- load run configuration
- define utility helpers

Important variables:

- `FISH_ID`
- `RUN_CONFIG`
- `NAS_ROOT`
- fish-specific path variables under `FISH_DIR`, `OUTDIR`, `OUT_REG`, `OUT_QA`, `OUT_DERIVED`

### 2. Spatial Preparation

Cells:

- `[7]`
- `[9]`
- `[11]`
- `[13]`
- `[15]`
- `[19]`
- `[21]`

Purpose:

- infer voxel sizes for functional, anatomy, and HCR data
- orient functional stacks to match anatomy orientation
- build per-plane functional reference images
- convert anatomy NRRD to TIFF when needed
- estimate the best anatomy Z for each functional plane
- estimate in-plane functional-to-anatomy placement and transform
- provide visual QA overlays

Key object:

- `plane_refs`

`plane_refs` holds per-plane reference information and is the backbone for later functional-to-anatomy matching.

### 3. Functional ROI Extraction

Cells:

- `[23a]`
- `[25]`

Purpose:

- load Suite2p outputs
- build label masks for all segmented ROIs
- mark active vs inactive cells using Suite2p `iscell`
- compute dF/F traces for functional ROIs

Key object:

- `suite2p_by_ref_idx`

This is the source of the functional ROI population. Both active and inactive ROIs matter for whole-population analyses.

### 4. Segmentation QC And Geometry QA

Cells:

- `[29]`
- `[33]`
- `[34a]`

Purpose:

- size-based segmentation QC
- centroid and overlap sanity checks
- debug summaries for functional-to-anatomy geometry

These cells are QA helpers, not the source of truth for identity.

### 5. HCR/Confocal Discovery, Warp, And HCR-to-Anatomy QC

Cells:

- `[37]`
- `[39]`
- `[41]`
- `[42]`
- `[43]`
- `[44]`

Purpose:

- discover HCR mask files and transform metadata
- save compact run metadata
- find ANTs transforms and warp confocal masks into anatomy space
- compare warped HCR masks against anatomy labels
- build QC tables describing HCR-to-anatomy relationships

Key object:

- `hcr_match_results`

This stage determines which anatomy labels have HCR support and gene identity.

### 6. HCR-Centric Identified-Cell Activity Mapping

Cells:

- `[50]`

Purpose:

- build `hcr_activity_status.csv`
- export `conf_to_func_pairs_raw.csv`
- export a trace-ready `conf_to_func_pairs.csv`
- support identified-cell activity analyses keyed by accepted HCR/anatomy labels

Important warning:

- this path is not the authoritative whole-population identity source
- it is the intended source for HCR-centric identified-cell activity analyses
- local competition should be restricted to ROI candidates for the accepted anatomy label
- activity should be reported after local geometric ranking, not used to silently override it

### 7. Whole-Population Functional-To-Anatomy Matching

Cells:

- `[50h]`
- `[50i]`

Purpose:

- match the full functional ROI population against anatomy labels
- save one row per functional ROI
- annotate match status, anatomy partner, and identity information

This is the part of the notebook that should become and remain authoritative.

### 8. Activity And BPI Annotation

Cells:

- `[50ia]`

Purpose:

- compute per-ROI activity summaries
- compute BPI and related activity metrics
- merge those fields back into the master ROI table

Important rule:

- BPI is an annotation on already-matched ROIs
- BPI must not change geometry matching or identity assignment

### 9. Population Summary Figures

Cells:

- `[50j]`
- `[50k]`

Purpose:

- summarize all ROIs by activity and BPI state
- summarize callable active identified ROIs by gene identity and BPI class

These figures should be pure views of the master ROI table.

### 10. Trace Export And Stimulus-Aligned Analyses

Cells:

- `[51]`
- `[55]`
- `[56]`
- `[56h]`
- `[56g]`
- `[57]`

Purpose:

- export trace files
- load stimulus timeline
- compute stimulus-aligned gene summaries
- render condensed and unified figures
- render full-session traces
- render BPI/activity diagnostics

These analyses should ultimately derive their cell sets from the same authoritative ROI table, even when they also require trace-level data.

Important note:

- `[51]`, `[56]`, `[56h]`, and `[57]` currently use the HCR-centric identified-cell activity export from `[50]`
- that export should use local geometry-first competition within each accepted HCR/anatomy label
- only labels whose local best functional candidate is active should enter the active trace export

## Notebook State Discipline

The notebook should be written to behave deterministically when cells are rerun.

Avoid `globals()`-style configuration and fallback logic:

- do not read config with `globals().get(...)`
- do not write outputs with `globals()[...] = ...`
- do not let old in-memory values silently override the current cell's explicit settings

Preferred pattern:

- define an explicit config block at the top of the cell
- pass values into helpers as function arguments
- read durable state from saved JSON/CSV files when cross-cell persistence is needed
- use named variables created by earlier cells directly when they are canonical pipeline outputs

In plain language:

- explicit config is good
- disk-backed cache is good
- hidden notebook-state fallback is bad

This matters especially for:

- fish-specific paths
- matching parameters
- visualization settings
- manual QA selections

If a cell needs to expose a result for downstream use, prefer one of these:

- a normal top-level variable assignment in that cell
- a saved file under the fish output directory
- a small returned object or dataframe passed forward explicitly

Do not introduce new `globals()`-based override patterns during refactors.

## Canonical Tables

### 1. Master ROI Table

Canonical object:

- `FUNC_ACTIVITY_IDENTITY_DF`
- CSV: `functional_roi_activity_identity.csv`

Key:

- `plane_idx`
- `func_label`

Role:

- one row per Suite2p ROI
- includes active and inactive ROIs
- includes geometry match status
- includes anatomy identity
- after `[50ia]`, also includes BPI and activity annotations
- legacy provenance columns may still exist, but downstream analyses should treat this table as the canonical ROI inventory

Important columns already present in the saved CSV include:

- ROI identity and geometry:
  - `plane`
  - `plane_idx`
  - `func_label`
  - `roi_idx`
  - `func_source`
  - `selected_anat_label`
  - `selected_dist_um`
  - `selected_overlap_px`
  - `n_overlap_candidates_any`
  - `n_overlap_candidates_valid`
  - `plane_match_outcome`
  - `matched_anat_plane`
  - `claim_outcome`
  - `has_unique_anat_match`
- molecular identity:
  - `anat_label`
  - `identity_label`
  - `identity_gene_count`
  - `has_identity_assigned`
  - `identity_display_label`
- activity and BPI:
  - `activity_class`
  - `is_active`
  - `n_bout_trials`
  - `n_cont_trials`
  - `mean_bout_dff`
  - `mean_cont_dff`
  - `bpi`
  - `bpi_z`
  - `activity_mag`
  - `bpi_data_available`
  - `bpi_category`

Downstream analyses should start from this table unless there is a very explicit reason not to.

### 2. Active BPI Subset

Derived object:

- CSV: `functional_roi_activity_bpi_cells.csv`

Role:

- active ROI subset with BPI-related fields
- useful for scatter plots and active-only summaries

This is derived from the master ROI table and must not be treated as an independent identity source.

### 3. Anatomy Identity Lookup

Derived object:

- CSV: `anatomy_identity_lookup.csv`

Role:

- convenience lookup from anatomy label to molecular identity

Use as a helper only. It is not the primary analysis table.

### 4. HCR-Centric Pair Table

Derived object:

- `CONF_FUNC_CSV`
- CSV: `conf_to_func_pairs.csv`

Role:

- active trace-ready export keyed by accepted HCR/anatomy labels
- one row per identified label when the local best functional ROI is active
- used by the identified-cell trace and stimulus-aligned cells

Important warning:

- this table is not the authoritative whole-population identity source
- use it for identified-cell activity analyses only
- if whole-population ROI identity is needed, start from `functional_roi_activity_identity.csv` instead

### 5. Trace Export Metadata

Derived object:

- CSV: `suite2p_traces/suite2p_dff_traces_meta.csv`

Role:

- file index for exported ROI traces

This should eventually be keyed and filtered from the master ROI table, not from a separate identity pathway.

## Rules For Future Figures And Analyses

If you are adding a figure or analysis:

1. Start from the master ROI table.
2. Filter rows for the needed subset:
   - active only
   - identified only
   - callable BPI only
   - specific genes
   - specific sides or stimulus classes
3. Join trace-level data only after the ROI subset is defined.
4. Never rebuild identity from scratch in the plotting cell.
5. Never let a figure silently use a different identity source than the rest of the notebook.

Exception:

- if the figure is explicitly about identified HCR-matched cells being active or inactive, start from `hcr_activity_status.csv` or the active trace-ready `conf_to_func_pairs.csv`
- in that case, be explicit in the code and figure title that the analysis is HCR-centric rather than ROI-centric

When you need a gene-labeled subset, the logic should be:

- match ROI to anatomy
- annotate anatomy with gene identity
- filter for `has_identity_assigned == True`

not:

- start from gene-labeled confocal pairs and treat that as the primary analysis population

## Current Legacy State

The notebook is not fully migrated yet.

At the time this file was written:

- `[50i]`, `[50ia]`, `[50j]`, and `[50k]` operate on the ROI-level table.
- `[50]`, `[50e]`, `[51]`, `[56]`, `[56h]`, and `[57]` use the HCR-centric identified-cell activity path.

This means the notebook intentionally mixes two paths, but each one has a distinct purpose.

When editing those downstream cells:

- preserve the ROI-centric master path for whole-population analyses
- preserve the HCR-centric path only for identified-cell activity analyses
- do not blur the two into an implicit hybrid inside a plotting cell

## Matching Guidance For Refactors

When improving the actual matching implementation, prefer this structure:

1. build plausible ROI↔anatomy candidate pairs first
2. gate candidates by overlap and distance
3. score plausible pairs using overlap-first logic with distance as a secondary term
4. solve a one-to-one assignment on that candidate graph
5. allow unmatched ROIs
6. annotate matched anatomy labels with gene identity afterward

This is more robust than:

- forcing every ROI to match something
- matching only to already-labeled anatomy cells
- or running a second identity-resolution pass that can override the first

## Cache And Rerun Guidance

Many notebook cells reuse cached CSVs and derived files.

If the matching logic changes, treat the following as stale and regenerate them for the current fish:

- `hcr_activity_status.csv`
- `conf_to_func_pairs_raw.csv`
- `conf_to_func_pairs.csv`
- `hcr_func_candidates.csv`
- `functional_roi_activity_identity.csv`
- `functional_roi_activity_identity_summary.csv`
- `functional_roi_activity_identity_by_plane.csv`
- `functional_roi_activity_bpi_cells.csv`
- `functional_roi_activity_bpi_summary.csv`
- `suite2p_traces/suite2p_dff_traces_meta.csv`
- downstream figure exports produced from those tables

At minimum, rerun:

- spatial prep and ROI loading cells if upstream references changed
- HCR warp and HCR-to-anatomy matching cells if confocal geometry changed
- `[50h]`, `[50i]`, `[50ia]`
- all downstream figure and trace-analysis cells that consume those outputs

## Decision Rule When Code And This File Disagree

If an old cell conflicts with this document:

- treat this document as the intended policy
- preserve reproducibility, but refactor toward the policy rather than away from it
- do not introduce new analyses that depend on the old ambiguity
