# Manual HCR identity annotation

The program assigns one marker at a time to fixed, already eligible ex-vivo 2P GCaMP cells. It does not require segmented HCR masks. Multiple markers can be positive for one cell. Historical calls and functional information are not loaded into the reviewer.

Design: [specification](manual-hcr-identity-gui-spec.md).

## Launch on Linnaeus

A dedicated `manualIdentity` environment was created from the existing `cellMatching` environment without modifying that environment. It contains napari 0.6.6, PyQt5, and pytest. Run the GUI in a native desktop terminal on Linnaeus; macOS Qt offscreen rendering has no usable OpenGL context for napari on this machine.

```bash
cd /Users/ddharmap/gitRepo/codeANTs
~/miniforge3/envs/manualIdentity/bin/python tools/manual_hcr_identity.py view \
  --manifest /absolute/path/dataset_manifest.json \
  --session /absolute/path/manual_identity/session \
  --reviewer Danin
```

For another environment, install the package's optional dependency with `python -m pip install -e '.[manual-identity]'`.

## Prepare a dataset

The viewer consumes an explicit, hash-bound manifest. `prepare` validates registered input assets and records their hashes. It does not run registration, guess axis flips, or invent filtering thresholds.

```bash
python tools/manual_hcr_identity.py prepare --config input_config.json --output dataset_manifest.json
```

Use schema version 1, axes `ZYX`, units `um`, a named coordinate frame, and a `grid` with `shape_zyx`, `spacing_xyz`, `origin_xyz`, and flattened `direction_xyz`. Supply `anatomy`, `labels`, and `eligibility` objects with `path`. `prepare` adds `sha256`; the viewer requires it. NRRD physical metadata are checked against the manifest. TIFF and NPY assets require an explicit `grid` in each volume object; label/array geometry is never inferred from equal shape.

`eligibility` points to JSON containing `policy_source` and `eligible_label_ids`. Materialize the existing upstream eligibility decision. Do not recompute a different filter for this program.

Each entry in `markers` requires:

```json
{
  "stack_id": "pth2-r1-ch2",
  "gene": "pth2",
  "round_id": "r1",
  "source_channel": "ch2",
  "source_sha256": "SHA256_OF_ORIGINAL_INTENSITY",
  "transform_provenance": ["exact round-to-exvivo transform paths or immutable IDs"],
  "image": {"path": "/absolute/path/registered_marker.nrrd"},
  "coverage": {"path": "/absolute/path/registered_valid_coverage.nrrd"}
}
```

Coverage is a binary volume on the exact ex-vivo grid. Derive it from the valid source acquisition domain under the same registration chain; padded zeros are not marker-negative evidence. Positive/negative calls require complete coverage of the selected cell mask. Uncertain and unassessable calls remain possible for incomplete coverage.

For repeated marker acquisitions, set `primary_stack_by_gene`, for example `{"pth2": "pth2-r1-ch2"}`. Contradictory positive/negative reviewed replicates are withheld and recorded as conflicts, even if a primary stack was declared.

The inspected mount contains L765_f02 ex-vivo segmentation TIFFs, but no complete verified pilot bundle was established. The implementation was validated on synthetic volumes; preparing a real fish requires verified registered intensities, grid/coverage metadata, and authoritative eligibility. No accepted dataset was changed.

## Review

Select one marker stack. Click an eligible anatomy mask or choose a cell from the queue. The selected cell is highlighted in the XY view; XZ/YZ context through its center appears beside the controls. Scroll Z in napari; use its image-layer controls for independent contrast, opacity, and visibility. Orthogonal panels are contextual image crops, not editable mask views.

Calls: unreviewed, positive, negative, uncertain, unassessable. Shortcuts 1–5 correspond to those calls, N/P navigate the current queue. Issue and note fields are committed with the call button. A blocking issue prevents positive export without modifying anatomy eligibility. The unreviewed queue advances after a decision; an empty queue disables call buttons. Annotation history undo/redo is available within the current process; committed events survive restart.

Every call is durably journaled before CSV/session checkpoints are updated. A failed checkpoint is reported as saved to the journal, retains undo/redo, and can be recovered on restart. The current marker, cell, filter, and display settings are saved. Freeze creates `annotations-rNNNNNN.json`, a versioned immutable snapshot. The complete annotation CSV preserves all states; missing canonical identity does not imply a reviewed negative.

A session permits one writer. After a crash, inspect `writer.lock` and recover only the recorded dead process on the same host:

```bash
python tools/manual_hcr_identity.py recover-lock --session /path/session --expected-pid 12345
```

The command refuses a live process or another host. A partial final journal event is backed up and discarded on resume; complete events remain authoritative.

## Export identities

Supply an independently reviewed ex-vivo-to-in-vivo correspondence JSON. It requires `schema_version: 1`, `fish_id`, `exvivo_labels_sha256`, `invivo_labels_sha256`, `invivo_labels` (`path`, `sha256`), `geometry_sha256`, `transform_provenance`, `reviewer`, `reviewed_at_utc`, and `rows`. Geometry is the frozen ROI/anatomy CSV; the in-vivo asset is the exact integer label image used for that geometry.

Each correspondence row has `exvivo_label_id`, `anat_label`, `status`, and geometry-only `metrics`. Only `status: "accepted"` is exported. Other statuses retain unresolved cells without identity. Accepted mappings must be one-to-one and their target IDs must exist in the bound in-vivo label image. Numerical label IDs are not assumed identical across spaces. This program consumes reviewed correspondence; it does not create scientific geometry approvals.

```bash
python tools/manual_hcr_identity.py export \
  --manifest dataset_manifest.json --snapshot session/annotations-r000123.json \
  --correspondence reviewed_correspondence.json \
  --geometry frozen_functional_roi_anatomy_matches.csv \
  --roi-master scored_functional_roi_activity_identity.csv \
  --gene-order sst1.2,cort,sst1.1,pth2 \
  --output /path/new_manual_export
```

Use the current pipeline's configured gene order. The output directory must be new. Positive calls form an independent manual identity source, replacing old identity fields in the new table; historical identity is not silently unioned. `anatomy_identity_lookup.csv` and `functional_roi_activity_identity.csv` keep the existing schemas and coexpression strings. The full calls and mapping-loss/conflict audit are separate files. A source manifest binds all output hashes. Frozen dataset metadata, the annotation snapshot, and the reviewed correspondence are included, with original source paths and mapping target/status/metrics in the audit, so moving the bundle preserves its provenance. Large source image volumes remain external.

## Canonical export and consumers

```bash
python tools/single_fish_pipeline.py export-canonical-tables \
  --fish-id FISH --local-root /path/data \
  --pipeline-root /path/new_pipeline_run \
  --manual-identity-root /path/new_manual_export \
  --score-input-root /path/score-activity-bpi/registration \
  --write-manifest
```

Alternatively use `tools/manual_hcr_identity.py canonical --manual-root ... --score-root ... --output ...`.

Manual mode writes an explicit **ROI-only** canonical bundle: ROI identity, anatomy lookup, BPI cells/summary, annotations, mapping audit, and provenance. It rejects missing geometry/response columns and verifies unchanged ROI keys and geometry/response fields against the score-stage master, validates source output hashes, and refuses existing output directories or an additional HCR input root. `--force-recompute` does not override the new-directory requirement in manual mode.

ROI-centric consumers can read the unchanged ROI table from this isolated run. The first cohort geometry deduplication consumer is exercised by the integration test. Legacy cohort discovery still defaults to accepted fish-local paths; do not copy manual outputs into those paths until scientific review and promotion are explicitly approved.

Manual-mode stage status is `warn` to make the limited bundle scope explicit. Existing HCR-centric trace/status tables and HCR-dependent compound figures require confocal-label IDs and are deliberately not generated. Those stages fail their existing missing-input gates. No fake confocal masks or legacy HCR tables are added to make the bundle look complete. The legacy full canonical exporter rejects a manual root supplied as `--hcr-input-root`.

## Verification

```bash
PYTHONPATH=src ~/miniforge3/envs/manualIdentity/bin/python -m pytest \
  tests/test_manual_identity.py tests/test_manual_identity_gui.py \
  tests/test_matching.py tests/test_lazy_imports.py -q
```

GUI checks use the native macOS Qt platform. `MANUAL_IDENTITY_SCREENSHOT=/tmp/manual-identity-gui.png` captures a complete fixture window during the GUI test. No real scientific annotations or existing pipeline outputs are written by tests.
