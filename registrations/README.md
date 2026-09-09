# Registration utilities

This directory owns maintained manual registration and transform entrypoints that run outside the package-driven notebook pipeline.

The scripts may orchestrate ANTs, FIJI/BigWarp, cluster jobs, and synchronization, but reusable scientific logic remains package-owned. Invoke these entrypoints from this directory; the former root compatibility names have been removed.

## Maintained entrypoints

- `ants_toRef.sh`: submit manifest-driven ANTs registration jobs.
- `applyTransform.py`: apply explicit or discovered ANTs transform chains.
- `bigwarp_loop.groovy`: apply reviewed FIJI/BigWarp landmarks.
- `sync.sh`: synchronize and canonicalize registration products.
