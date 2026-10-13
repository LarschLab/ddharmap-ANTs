# Remaining-fish unattended readiness jobs

Run `tools/single_fish_readiness.py --spec /absolute/fish/readiness.json` through
the persistent SSH launcher. This is a read-only preflight, **not** a full
production pipeline job. Reports and checksum-identical existing QC copies are
fish-local under `03_analysis/functional/run_state/remaining_readiness/UUID/`.
No ANTs, segmentation, scientific writer, canonical promotion, identity or gate
acceptance occurs. An exit-0 `review_required` report means its mechanical
checks passed; it does not mean analysis completed. Failed checks return exit 1.

The spec pins clean code, interpreter, baseline finishing proof, source metadata,
eligible sessions/global planes, explicit per-plane registration sources, each
retained HCR round's ordered transform chain and review exclusions. The report
checks current label grid/dtype/counts and native label IDs only where a native
mask is available. Archive paths are explicitly rebound within the same fish;
the original sidecar is never rewritten. Current transform hashes cannot prove
identity to August transforms because the old sidecars did not record hashes.

L758_f07 currently has accepted NCC placement, not the L765 ANTs bundle.
L765_f02 must combine accepted plane-4 retry-v2 with nine original planes;
L765_f05 must use metadata-defined R2 planes only. All four fish still require
marker acceptance and anatomy-label Z provenance/midline review before the full
DAG can be specified safely. Missing native masks must be retrieved from the
archive or retained as explicitly provenance-limited inputs after review.
