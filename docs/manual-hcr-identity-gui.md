# cellMatcher: manual HCR identity on ex-vivo masks

cellMatcher records one marker at a time on fixed ex-vivo Cellpose masks in ex-vivo space. Multiple markers can be positive for one cell. Historical identity and functional data are not shown while reviewing.

Design history: [specification](manual-hcr-identity-gui-spec.md). The correspondence-based ROI-only export described there was replaced by the pipeline-integrated flow below.

## Launch

On Linnaeus: Cmd+Space → **cellMatcher**. Run it in the native desktop session (Screen Sharing is fine); napari needs a real display.

Install or reinstall the app (after moving the repository or environment):

```bash
tools/install_cellmatcher_app.sh ~/miniforge3/envs/manualIdentity/bin/python
```

The app runs whatever is checked out in this repository. Logs: `~/Library/Logs/cellMatcher/`. Without the app: `python tools/manual_hcr_identity.py app`.

## Workflow

1. **Pick the fish** in the launcher (data root defaults to `/Volumes/dataDrive/dataProcessing/2p_processing`) and enter your reviewer name once.
2. **Prepare dataset** (once per mask set). Discovers, by pipeline convention:
   - ex-vivo anatomy `02_reg/00_preprocessing/2p_anatomy/ex_vivo/<fish>_exvivo_anatomy_2P_GCaMP_uint8_manual_oriented.nrrd`;
   - the single mask TIFF in `03_analysis/structural/ex_vivo/cp_masks/`, bound to that grid by `structural/ex_vivo/manifests/segment-ex-vivo-anatomy-cellpose_manifest.json` (segmentation may run on another machine; bring the manifest along);
   - every registered HCR stack `02_reg/03_rbest-exvivo/aligned/*_in_exvivo.nrrd` and `02_reg/05_rn-exvivo/aligned/*_in_exvivo.nrrd`, its source intensity in `00_preprocessing/{rbest,rn}`, and its transform chain (direct or via rbest).

   For each stack it writes a coverage mask (source acquisition domain warped with the same chain) and refuses the stack unless re-warping the source reproduces the registered image (Pearson r ≥ 0.99). All masks enter the review queue. Output: `03_analysis/structural/ex_vivo/manual_identity/<fish>_exvivo_<mask-hash>/`.
3. **Open review.** Pick a marker stack, then scroll through Z and **click every cell that contains HCR signal**; it turns orange (pending). Click again to unmark; Option/Alt-click only selects. The view never moves on its own (use *Go to selected cell* or *Follow selection*). The selected cell has a yellow outline and is named in the header; recorded positives are green. **Approve marked…** records all marks of the stack as positive in one journaled, undoable step and asks whether the remaining unreviewed, fully imaged cells should become negative (leave them unreviewed unless you checked the whole stack). Marks survive closing the app. The left *HCR contrast* panel shows the intensity histogram inside the imaged area: drag the red line to hide background, the blue line to saturate signal, and use the brightness slider. *Single-cell call* (collapsed) records uncertain/unassessable calls, issues, and notes (keys 1–5; N/P step through the list; M toggles the mark). Every control has a tooltip. Positive/negative require complete coverage of the mask. Work is saved continuously (every approval, call and undo is journaled; pending marks and view settings go to `session.json`), so just close the window and reopen the fish to resume; *Snapshot* is an optional extra frozen revision. The layout favours the image: napari's layer controls and layer list are tabs behind *HCR contrast* on the left, and the XZ/YZ side views (modelled on the brainAtlas orthogonal viewer) show the full stack depth over a fixed 200 µm around the cut, at physical aspect ratio, stacked one above the other. They do not rescale with zoom; panning redraws them only when the cut has to move back into the visible area. Red lines in the side views mark the current Z slice and where the other view cuts; in the image a faint crosshair with an open centre (about one cell diameter) marks the same point without covering the cell (toggle below the views). Clicking or dragging in a side view jumps to that Z slice and moves the cut; **right-clicking a cell** in the image cuts both views through its centre without marking it or moving the image (marking a cell does the same). The side views outline the selected cell (yellow), marked cells (orange) and recorded positives (green), so cells missed in the current slice stand out. If a panel disappears, use **cellMatcher → Restore all panels** (Ctrl+Shift+R).
4. **Finish & export identity** (review window). Freezes a snapshot, shows what will be written or replaced, and asks before touching existing outputs. Then:
   - warps every ex-vivo mask into in-vivo 2P space with `02_reg/04_exvivo-2p` (nearest neighbour, IDs kept) and matches all of them to the in-vivo anatomy masks with the HCR matching code (identity-independent geometry);
   - writes a per-gene aligned bundle with the HCR file contract to `exports/<rev>/aligned/` (`conf_label` = ex-vivo mask ID);
   - writes **only** `functional_roi_activity_identity.csv` and `anatomy_identity_lookup.csv` to the assign-hcr-identity location (`03_analysis/functional/derived/assign-hcr-identity/registration/` for native fish), built with the pipeline's own functions and gene order;
   - moves the five HCR-centric assign tables aside (backed up) and writes `pipeline_manifests/assign-hcr-identity_manifest.json` with status `warn`, `identity_source=manual_exvivo`, `identity_gate=awaiting_continuation`.

   **The pipeline stops here.** `manual_identity_audit.csv` in the export folder records every label × gene outcome (exported, negative, blocking_issue, replicate_conflict, positive_unmatched_geometry, …).
5. **Continue pipeline** (launcher). Requires the gate tables to be unchanged, then runs, with the fish run-spec commands and interpreter: assign-hcr-identity (`--identity-source manual_exvivo`, must reproduce the gate tables byte for byte, also writes the HCR-centric tables) → export-canonical-tables → make-qa-report → make-motion-auc-tables → make-figures. For native fish each stage runs in `03_analysis/functional/run_state/<stamp>_manual_identity/transaction_graph` (seeded from the last committed graph, binaries hash-verified) and is promoted with the production journal/backup mechanism.

Replacements are journaled under `03_analysis/functional/run_state/manual_identity/` (gate) and `run_state/<stamp>_manual_identity/history/` (continuation); `native_run.rollback_promotion` restores a journal.

## Scientific notes

- Geometry is matched once for all ex-vivo masks; identity only selects which matched pairs carry a gene.
- HCR label-quality gates (small-speck floor of 200 native voxels, per-gene q95 large-mask flag) currently apply to ex-vivo masks; a correctly matched positive that fails them is reported as `positive_unmatched_geometry` in the audit.
- Repeated stacks of one gene need `primary_stack_by_gene` in the dataset manifest; conflicting positive/negative replicates are withheld.

## CLI

```bash
python tools/manual_hcr_identity.py prepare-fish --fish-id L765_f04 --local-root /Volumes/dataDrive/dataProcessing/2p_processing
python tools/manual_hcr_identity.py gate --manifest <dataset_manifest.json> --snapshot <session/annotations-rNNNNNN.json> --local-root <root> [--confirm-replace]
python tools/manual_hcr_identity.py continue --fish-id L765_f04 --local-root <root>
python tools/manual_hcr_identity.py recover-lock --session <session> --expected-pid <pid>
```

## Verification

```bash
PYTHONPATH=src:tests ~/miniforge3/envs/manualIdentity/bin/python -m pytest \
  tests/test_manual_identity.py tests/test_manual_identity_pipeline.py tests/test_manual_identity_gui.py -q
```

Staging validation (2026-10-06, copy of L765_f04 under `/Volumes/dataDrive/dataProcessing/_cellmatcher_staging`, stand-in ex-vivo masks = in-vivo masks inverse-warped to ex vivo, test calls replaying production identity): all six transform chains reproduced their registered stacks (r = 1.000); ROI master columns identical to production with 99.8 % identity agreement over 3553 ROIs; all five continuation stages passed; all eight canonical tables column-identical to production; `conf_label == anat_label` for every HCR-status row (exact round trip).
