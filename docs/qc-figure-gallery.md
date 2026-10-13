# Standalone QC figure gallery

`tools/export_qc_figures.py` extracts every rendered PNG from the seven executed
read-only QC notebook copies and all PNG outputs recorded by current writer
manifests. It also scans directories explicitly declared as writer outputs.
Current writer PNGs must live in the actual fish folder; stale staged-root paths
fail visibly. Notebook 00 is currently table-only. Nonempty unexecuted cells,
notebook errors, missing figures in notebooks 01–06, non-PNG image outputs, and
blank or undersized images fail visibly.

```bash
PYTHONPATH=src /Users/ddharmap/miniforge3/envs/cellMatching/bin/python \
  tools/export_qc_figures.py \
  --notebook-dir /path/to/seven/executed/notebooks \
  --manifest-dir /path/to/L765_f04/03_analysis/functional/pipeline_manifests \
  --fish-dir /path/to/L765_f04 \
  --output-dir /path/to/L765_f04/03_analysis/functional/review_gallery/RUN_ID
```

The exporter writes full-resolution PNGs, an HTML gallery, numbered contact
sheets, `figure_index.csv`, and `gallery_manifest.json`. Exact duplicate PNG
bytes share one figure entry; every notebook SHA-256, cell/output index, writer
path, and manifest SHA-256 association is retained. All seven notebook coverage
records are included, even when a notebook has no image. Printed blocked or
unavailable review messages are recorded for interpretation, not silently
converted to scientific acceptance.
Cell-level renderer-call/PNG coverage is also recorded so a plot cell that
executes without serializing its image cannot be overlooked. Execute QC copies
with inline PNG capture; `MPLBACKEND=Agg` alone does not serialize Matplotlib
figures into notebook outputs. Scientific writer stages can continue using Agg.

Keep the output directory outside all runner-owned input/output paths so this
presentation export cannot invalidate scientific stage state. Inputs and notebook
code remain untouched. Existing gallery destinations are refused. Gallery
publication is atomic: failed coverage checks leave no completed gallery.
Rendered artifacts still require visual inspection; passing a notebook or extracting its
figures does not establish biological acceptance.

## Curated review (Danin, 2026-10-13)

The exporter keeps the complete extraction inventory above, then selects an
ordered review by **source role**, never extraction F-number. Notebook roles use
the notebook number, the owning code-cell's numbered label, and PNG position
within that cell (text/table outputs do not shift it). Writer roles use exact
output basenames. Changes to those notebook labels must update the selection
contract in `qc_figure_export.CURATED_CARDS`.

The six gates are Z-drift, registration/geometry, midline, response/BPI,
identity-functional, and laterality. Cards are P001–P025 with P020b after P020;
P019b and P026 are omitted. P014 is the approved signed [23c] AUC violin figure, with all-cell and fixed
any-stimulus responder rows and separate session columns. P012 uses `suite2p_stimulus_locked_heatmaps_23b`; P013 retains
the notebook's fixed any-presented-stimulus responder traces once. The replay8
F057/F058 numbering differs from the proposal; output names own selection.

Package `plots.gallery` constructs P003 (writer plane rows + intensity rows),
P005 (geometry + offsets), P010 (accepted midline + ROI side/count grid), and
P020 (accepted-HCR fate / responsive-ROI coverage + exact responsive identity).
Explicit owning QC notebook cells display these composites. P003 places the two complete row sets side by side, enlarging the shorter set
only to align plane rows. P005 retains native PNG stacking. P010/P020 enlarge the
narrower part to equal width, so neither part becomes a small thumbnail.
Matplotlib panels are rasterized at publication DPI. No source panel is shrunk,
cropped, or plane-limited; original PNGs remain in the extraction inventory. The exporter only selects these
rendered PNGs; it computes no cohorts, identities, traces or composites.

P008 displays thicker saved accepted/rejected HCR outlines in a two-column grid
at every persisted functional anatomy plane, using the geometry-owned plane-reference sidecar. P019 retains
the Q7 marker-stratified traces and adds aligned two-ring HCR-label donuts:
inner = within/outside functional planes/unmatched; outer = responsive,
low activity, response unavailable, no functional match, out-of-plane, unmatched.
The denominator is **only the supplied `hcr_activity_status.csv` rows**. The
historical cohort implementation added unmatched masks from a separate summary;
this curated view deliberately does not expand the current denominator. The
standalone 56h trace/status writer output remains unchanged on disk.

The HTML opens with saved session/trial evidence, drift, geometry parity,
bridge-track states, acceptance hashes and export scopes from the executed
notebook table/text outputs and current manifests. It explicitly retains
provisional release status. Missing cards are listed visibly and recorded;
pre-midline galleries can remain incomplete without inventing downstream data.
`gallery_manifest.json` adds `cards`, `exclusion_map`, `missing_cards`,
`curation_version`, and `provenance`, while preserving `figures` and all hashes
and source associations. The contact sheets and CSV remain full-inventory
retrieval surfaces.

An optional `--response-trace-inspector /path/to/inspector.html` adds a link.
Presentation-only `curated_artifacts` overrides in the Python exporter allow
read-only previews of newly rendered roles against older executed notebooks.
They remain separate hashed inventory entries; original inputs remain recorded.
The finishing job includes the exporter source hash in its gallery fingerprint,
so changed curation creates a new immutable destination.

Review round 1 adds figure-level inner- and outer-ring legends to P019 and moves
the donut column next to the trace grid. P015 draws light-grey non-significant
points first, then coloured saved activation and suppression passes, with count
legends per stimulus. Its tuning-side bars include only ROIs with a saved
activated call; excluded ROIs are counted in the title. The degenerate
magnitude-bin curve is removed, retaining its response/tuning count legend.

## Signed early-screen measurements (Danin, 2026-10-13, round 2)

The single-fish [23b] display-trace builder no longer emits `mean_z_pre`,
`mean_z_post`, `mean_dff_post`, or `peak_z_post`. The 50 s display window can
include neighboring trials and must not define scalar stimulus responses.
Display traces and their hash-bound numeric payload remain available; response
calls, scoring windows, and the 0.05 AUC floor are unchanged.

P014 reads `suite2p_presented_stimulus_response_23c.csv` and
`suite2p_any_presented_response_23c.csv`. It shows signed 12.5 s motion-window
AUC minus the matched prestimulus pause for all represented Suite2p cells and
for the fixed any-presented-stimulus responder cohort, split by session. The
second row is explicitly descriptive because the same responses selected it.
Counts include unavailable rows; violins and medians use finite AUCs.

P018 also reads the independent presented-response table. Its matrix input is
signed `mean_auc_dff`; existing within-ROI normalization and clustering remain.
The normalized colourbar is unitless with input units dF/F·s; the raw companion
uses dF/F·s. Trace summaries no longer supply scalar response measurements.

Provenance is seven rows, with saved key values visible, small source links and
collapsible complete evidence. Geometry parity reports the persisted stored /
physical median comparison rather than inventing acceptance. Cohort 50 s
summary fields remain unchanged and require separate cohort work.

Review round 3: P014 shares a robust symmetric AUC range across sessions within
each population row. Its violin densities omit out-of-range values, with above
and below counts shown for each stimulus; medians and call counts use the full
population. Rows with large dynamic range use a labelled symlog axis with the
row median absolute AUC as linthresh. P018 places its colourbar in a dedicated
column beside the mean-AUC panel. These display changes do not alter tables.
