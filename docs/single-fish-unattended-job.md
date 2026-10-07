# Unattended single-fish finishing job

## Fresh native recipes (2026-10-07)

**Current deployment gate:** the first real fresh control completed all 12
writers and QC 00–05 unattended, but historical byte parity failed and the
batch stopped before production. All 20 reference TIFFs and anatomy bytes
matched; ten ANTs matrices differed and 16/3553 anatomy-label assignments
changed. Response counts stayed 267 (266 activated, one suppressed).
The legacy seed 0 is ANTs default/system-time seeding. A bounded plane-4 probe
produced different matrices twice with seed 0, but identical matrices twice
with seed 123. Fresh recipes now pin 123 and native fresh mode rejects zero
before any writes. Historical frozen replay is unchanged. The current batch
gate still requires historical byte parity and therefore must not be bypassed
or silently rebased. Approval is needed to replace it with two fresh seeded
runs matching each other, plus explicit historical geometry-delta review.
No new production batch has been submitted under that revised gate.

`tools/fresh_single_fish_batch.py prepare` creates an isolated L765_f04 control
and native recipes for L765_f02, L758_f07, L765_f05, L765_f06. The control copies
writable anatomy/aligned inputs and links read-only raw/Suite2p sources. Its
scientific CSVs must match current outputs byte-for-byte after only fish-root
path normalization; all ten ANTs matrices must match exactly. `run --batch`
executes the control first and launches no production fish if it fails. It
stops at the first subsequent failure, preserving diagnostics, with no polling,
interactive recovery or automatic retry. Explicit review gates still apply.

`tools/build_fresh_single_fish_spec.py` creates a common fish-local recipe from
the established folder/naming conventions, a clean exact code pin, interpreter
packages, selected functional plane indices, and retained aligned HCR masks.
It takes `--fish-id`, `--data-root`, `--repo-root`, `--run-id`, `--spec`,
`--ncc-cache-source`, and repeated `--plane-index`. It refuses existing specs.
L765_f05 is explicitly constrained to global planes 5–9 (functional R2).
The saved NCC scale/depth initializer files are copied and checksum-pinned;
all selected XY/ANTs registrations and subsequent scientific writers run anew.
Existing aligned masks are inputs; missing original HCR segmentation masks
are not synthesized and direct HCR rewarping is not claimed.

Native `mode=fresh` has no frozen-root dependency or writer reuse. Every stage
writes to a transaction graph before verified promotion into existing consumer
directories. Explicit anatomy output paths are redirected into that graph.
All promoted products are hashed in state and checked on resume, not only
the spec's principal outputs. Old graph products are archived before a fresh
retry. Promotion retains the existing per-file atomicity/manifest commit limits.

This recipe runs 12 scientific writers plus QC notebooks 00–05, ending at an
explicit `review_required` midline gate. It does not manufacture accepted
midlines, laterality tables, final figures, or notebook 06. The finishing job
still verifies hashes, canonical schemas/keys, signed responses and [23c]
tails, and proves a no-op resume. It exports a clearly labeled preliminary
six-notebook gallery using only this recipe's writer manifests; a pass denotes
successful execution, not scientific release. Full production specs retain
the strict seven-notebook gallery requirement.

BPI/geometry baseline comparisons run only when explicit baseline hashes are
present. Frozen table/matrix byte comparisons remain mandatory for frozen
replay specs, but do not apply to a new fish's fresh registration. Universal
schema/key/window/two-sided checks are unchanged.

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
