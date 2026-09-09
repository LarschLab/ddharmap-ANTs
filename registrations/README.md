# Registration utilities

This directory owns maintained manual registration and transform entrypoints that run outside the package-driven notebook pipeline.

The scripts may orchestrate ANTs, FIJI/BigWarp, cluster jobs, and synchronization, but reusable scientific logic remains package-owned. Root-level names are temporary compatibility paths for external callers and should be removed only after an external-consumer review.
