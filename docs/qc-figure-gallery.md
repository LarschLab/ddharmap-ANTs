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
code remain untouched. Existing gallery destinations are refused. Rendered
gallery publication is atomic: failed coverage checks leave no completed gallery.
Rendered
artifacts still require visual inspection; passing a notebook or extracting its
figures does not establish biological acceptance.
