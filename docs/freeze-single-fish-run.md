# Freeze a single-fish pipeline run

`tools/freeze_single_fish_run.py` delegates to the package-owned
`codeants_2pf_hcr.freeze_run.freeze_single_fish_run`. It inventories both the
complete pipeline root and the external writer-manifest directory, copies them,
checks source stability and copied SHA-256 content, and protects only the
verified snapshot. Existing destinations are refused and sources are untouched.

Use the scientific environment that produced the run to capture its interpreter
and package versions. `--scientific-commit` identifies the historical run's code;
the utility's actual HEAD, branch and dirty state are recorded separately. A Git
archive of the scientific commit is included. A historical commit is declared
provenance, not a reconstruction of the original execution's dirty state.

```bash
PYTHONPATH=src /Users/ddharmap/miniforge3/envs/cellMatching/bin/python \
  tools/freeze_single_fish_run.py \
  --fish-id L765_f04 \
  --pipeline-root /path/to/working/L765_f04 \
  --manifest-dir /path/to/actual/L765_f04/03_analysis/functional/pipeline_manifests \
  --destination /path/to/frozen/L765_f04_TIMESTAMP \
  --scientific-commit dfbc3b8 \
  --protection read-only-image
```

On a POSIX filesystem, the default `--protection permissions` removes write bits
and verifies the resulting modes (macOS immutable flags are attempted when
needed). On macOS exFAT, use `read-only-image`: chmod and flags are unreliable on
that filesystem. The copied tree becomes a UDRO disk image containing a browsable
`snapshot/`, SHA-256 inventory, scientific Git archive, utility Git state,
interpreter/package list, stage parameters/output paths, compact stage statuses,
and freeze metadata. The image is checked with `hdiutil verify`, mounted at
`/Volumes/DESTINATION_BASENAME`, rehashed, and tested for read-only filesystem status and an
actual rejected write (`EROFS`). `image_verification.json` records the image hash.
Image assembly copies each file literally into a writable HFS+ image before
converting it to UDRO; this preserves inventoried exFAT AppleDouble sidecar files
that `hdiutil -srcfolder` would otherwise absorb into extended attributes.

The mounted payload is read-only. The surrounding exFAT container and image file
can still be deleted or replaced by someone with access to the drive; this is not
a claim of immutable storage. The image hash detects replacement or modification.
The outer README includes the exact read-only remount command for after reboot.
External manifest paths are preserved as historical provenance; they are not
rewritten to pretend the frozen run remains executable at its new location.

Tests: `PYTHONPATH=src python -m pytest tests/test_freeze_run.py -q`. To include the
real macOS disk-image integration test, set `FREEZE_IMAGE_TEST_ROOT` to an existing
writable directory on the target volume. It exercises a nested payload and checks
that both file creation and existing-file overwrite are rejected on the mount.
