"""Unattended finishing orchestration; never owns scientific writer semantics."""
from __future__ import annotations
from codeants_2pf_hcr.paths import visible_glob, visible_rglob, is_visible_path

from contextlib import redirect_stdout, redirect_stderr
import fcntl
import json
from pathlib import Path
import traceback
import uuid

from .single_fish_run import (
    _file_hash, _json_hash, _utc_now, _write_json_atomic, build_run_plan,
    execute_run, load_run_spec, verify_run, audit_run_artifacts,
)


def _output_metadata(spec) -> dict:
    paths = {Path(output) for stage in spec.stages for output in stage.outputs}
    for state_path in visible_glob(spec.state_root / "stages", "*.json"):
        paths.update(Path(record["path"]) for record in json.loads(state_path.read_text()).get("promoted_products", ()))
    paths.add(spec.data_root / spec.fish_id / "03_analysis/functional/pipeline_manifests/native_layout.json")
    files = set()
    for path in paths:
        if path.is_dir():
            files.update(child for child in visible_rglob(path, "*") if child.is_file())
        elif path.exists():
            files.add(path)
    return {str(path): (path.stat().st_size, path.stat().st_mtime_ns) for path in files}


def _gallery(*, notebook_dir: Path, manifest_dir: Path, output_dir: Path,
             fish_dir: Path, review_gate: dict | None = None,
             manifest_paths: tuple[Path, ...] | None = None, notebook_count: int | None = None) -> dict:
    from .qc_figure_export import export_qc_figure_gallery
    if not output_dir.exists():
        export_qc_figure_gallery(notebook_dir=notebook_dir, manifest_dir=manifest_dir,
                                 output_dir=output_dir, fish_dir=fish_dir, review_gate=review_gate, manifest_paths=manifest_paths,
                                 notebook_count=notebook_count)
        inventory = {str(path.relative_to(output_dir)): _file_hash(path)
                     for path in visible_rglob(output_dir, "*") if path.is_file()}
        _write_json_atomic(output_dir / "job_inventory.json", {"sha256": inventory})
    report = json.loads((output_dir / "gallery_manifest.json").read_text())
    inventory = json.loads((output_dir / "job_inventory.json").read_text())["sha256"]
    files = {str(path.relative_to(output_dir)) for path in visible_rglob(output_dir, "*")
             if path.is_file() and path.name != "job_inventory.json"}
    if files != set(inventory) or any(_file_hash(output_dir / name) != digest
                                    for name, digest in inventory.items()):
        raise RuntimeError("Existing gallery inventory changed; preserve it and use a new output directory")
    for key in ("notebook_coverage", "writer_manifest_coverage"):
        for source in report[key]:
            if _file_hash(Path(source["path"])) != source["sha256"]:
                raise RuntimeError(f"Gallery source changed: {source['path']}; use a new output directory")
    if {entry["path"] for entry in report["notebook_coverage"]} != {str(p) for p in visible_glob(notebook_dir, "*.ipynb")}:
        raise RuntimeError("Gallery notebook coverage changed")
    manifests = {str(p) for p in (manifest_paths if manifest_paths is not None else visible_glob(manifest_dir, "*_manifest.json"))
                 if is_visible_path(p) and ".pre_" not in p.name}
    if {entry["path"] for entry in report["writer_manifest_coverage"]} != manifests:
        raise RuntimeError("Gallery manifest coverage changed")
    for figure in report["figures"]:
        for source in figure["sources"]:
            if source["kind"] == "writer_png" and _file_hash(Path(source["path"])) != figure["sha256"]:
                raise RuntimeError(f"Writer PNG changed: {source['path']}")
    return {"output_dir": str(output_dir), "figure_count": report["figure_count"],
            "notebook_png_outputs": report["notebook_png_output_count"],
            "writer_png_references": report["writer_png_reference_count"]}


def run_finishing_job(*, spec_path: Path, notebook_dir: Path | None = None,
                      manifest_dir: Path | None = None, gallery_dir: Path | None = None,
                      report_dir: Path | None = None,
                      validate_noop: bool = False) -> dict:
    """Resume, verify, audit, export all QC, and optionally prove a no-op resume.

    Failures are terminal saved reports, never prompts or automatic retries.
    A fish-level lock spans the entire sequence, independent of run state root.
    """
    report_dir = (report_dir or spec_path.resolve().parent / "job_reports").expanduser().resolve()
    report_dir.mkdir(parents=True, exist_ok=True)
    job_id = uuid.uuid4().hex
    report_path = report_dir / f"{job_id}.json"
    log_path = report_dir / f"{job_id}.log"
    report = {"job_id": job_id, "spec": str(spec_path), "started_at": _utc_now(),
              "status": "running", "phase": "load-spec", "log": str(log_path),
              "report": str(report_path), "steps": {}}
    _write_json_atomic(report_path, report)
    try:
        with log_path.open("w") as log, redirect_stdout(log), redirect_stderr(log):
            spec = load_run_spec(spec_path)
            review_gate = spec.raw.get("review_gate")
            # Frozen replay recomputes downstream stages from verified frozen upstream products, so it may await review too.
            if review_gate and (spec.raw.get("native_production", {}).get("mode") not in {"fresh", "frozen_replay"} or review_gate.get("status") != "review_required"):
                raise ValueError("Review-gate gallery requires a native fresh or frozen-replay run awaiting scientific review")
            fish_dir = spec.data_root / spec.fish_id
            functional = fish_dir / "03_analysis/functional"
            if notebook_dir is None:
                parents = {Path(output).parent for stage in spec.stages for output in stage.outputs
                           if output.endswith(".ipynb")}
                if len(parents) != 1:
                    raise ValueError("Specify notebook_dir: no unique QC notebook output directory")
                notebook_dir = parents.pop()
            notebook_dir = notebook_dir.expanduser().resolve()
            manifest_dir = (manifest_dir or functional / "pipeline_manifests").expanduser().resolve()
            lock_path = fish_dir / "03_analysis/functional/single_fish_job.lock"
            lock_path.parent.mkdir(parents=True, exist_ok=True)
            with lock_path.open("a") as lock:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                for phase, action in (
                    ("resume", lambda: execute_run(spec, resume=True)),
                    ("verify", lambda: verify_run(spec)),
                    ("audit-artifacts", lambda: audit_run_artifacts(spec)),
                ):
                    report["phase"] = phase
                    _write_json_atomic(report_path, report)
                    result = action()
                    _write_json_atomic(report_dir / f"{job_id}.{phase}.json", result)
                    ok = result.get("status") == "pass" if phase == "resume" else result.get("ok") is True
                    report["steps"][phase] = {"ok": ok}
                    if not ok:
                        raise RuntimeError(f"{phase} failed; see saved detailed report")
                if validate_noop:
                    report["phase"] = "no-op-resume"
                    plan = build_run_plan(spec, resume=True)
                    if not plan["environment"]["ok"] or any(s["action"] != "skip" for s in plan["stages"]):
                        raise RuntimeError("No-op proof refused: some stages would run")
                    before = {str(p): _file_hash(p) for p in visible_glob(spec.state_root / "stages", "*.json")}
                    products_before = _output_metadata(spec)
                    result = execute_run(spec, resume=True)
                    after = {str(p): _file_hash(p) for p in visible_glob(spec.state_root / "stages", "*.json")}
                    products_unchanged = products_before == _output_metadata(spec)
                    ok = (result["status"] == "pass" and before == after
                          and products_unchanged
                          and all(s["action"] == "skip" for s in result["results"]))
                    _write_json_atomic(report_dir / f"{job_id}.no-op-resume.json", result)
                    report["steps"]["no-op-resume"] = {"ok": ok, "stage_records_unchanged": before == after,
                                                        "product_mtimes_unchanged": products_unchanged}
                    if not ok or not verify_run(spec)["ok"]:
                        raise RuntimeError("No-op resume proof failed")
                report["phase"] = "export-qc"
                if gallery_dir is None:
                    # Choose after resume: changed source bytes get a new
                    # immutable evidence gallery without user intervention.
                    sources = sorted(visible_glob(notebook_dir, "*.ipynb")) + sorted(
                        path for path in visible_glob(manifest_dir, "*_manifest.json")
                        if is_visible_path(path) and ".pre_" not in path.name)
                    # Presentation changes must also create a new immutable gallery.
                    from . import qc_figure_export
                    sources.append(Path(qc_figure_export.__file__))
                    fingerprint = _json_hash([[str(path), _file_hash(path)] for path in sources])
                    gallery_dir = functional / "review_gallery" / spec.state_root.name / fingerprint
                report["steps"]["export-qc"] = _gallery(
                    notebook_dir=notebook_dir, manifest_dir=manifest_dir,
                    output_dir=gallery_dir.resolve(), fish_dir=fish_dir,
                    **({"review_gate": review_gate, "manifest_paths": tuple(Path(s.manifest) for s in spec.stages if s.manifest),
                        "notebook_count": sum("tools/notebook_run.py" in s.command for s in spec.stages)} if review_gate else {}))
                report["status"] = "pass"
                if review_gate:
                    report["scientific_release"] = review_gate
    except Exception as exc:
        report.update(status="fail", error=f"{type(exc).__name__}: {exc}")
        with log_path.open("a") as log:
            traceback.print_exc(file=log)
    finally:
        report["finished_at"] = _utc_now()
        _write_json_atomic(report_path, report)
    return report


__all__ = ["run_finishing_job"]
