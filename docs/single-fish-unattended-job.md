# Unattended single-fish finishing job

`tools/single_fish_job.py --spec /absolute/fish/run_specs/FISH.json --validate-noop`
executes the existing DAG with hash-aware resume, verifies all recorded stages,
audits real canonical artifacts, proves a second resume performs no stage writes,
and exports all seven QC notebooks and current writer PNGs. It does not accept
scientific review gates. Use a clean pinned checkout; do not use an ongoing
manual-annotation checkout or silently migrate old state into a new code pin.

Submit this command once through the established persistent SSH job launcher.
The process continues without polling, an AI session, or stdin. Scientific
writers and validators have stdin closed and stdout/stderr in stage log files.
Success or failure produces a unique atomic compact JSON report, detailed phase
reports, and a traceback log. The CLI prints only status and report/log paths.
No automatic retry or speculative expensive rerun occurs in the no-op proof.
The initial resume is a normal authorized production resume and may execute
stale stages; inspect `single_fish_run.py plan` before first submission.

QC notebook defaults derive from the spec's declared notebook outputs. Other
defaults are fish-local `pipeline_manifests`, `review_gallery/STATE_ROOT_NAME/SOURCE_HASH`,
and a `job_reports` directory beside the spec. Override with `--notebook-dir`,
`--manifest-dir`, `--gallery-dir`, and `--report-dir`. Existing galleries are
reused only when their full saved inventory and all source hashes/coverage
remain exact. Otherwise the job fails and preserves the old evidence; use a
new gallery directory when an explicit output override was used. Default galleries
are versioned by the post-run notebook and manifest content hashes: changed QC
automatically receives a new directory; unchanged evidence is verified and reused.
Notebook-directory inference fails before any writer runs if it is ambiguous.
A fish-level nonblocking advisory lock prevents two
finishing jobs for the same fish even with different state roots. Direct old
runner commands do not take that finishing lock: do not launch them concurrently.

Blank/relative artifact declarations are invalid, not optional missing paths.
An empty optional-input string previously meant `.` and hashed the checkout,
spuriously invalidating registration/geometry when unrelated code changed.
Correct an existing spec only via explicit hash-validated state migration,
preserving its original records; this job never performs state adoption.
