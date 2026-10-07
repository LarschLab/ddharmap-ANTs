"""Fresh control validation followed by a conditional unattended fish batch."""
import hashlib
import json
from pathlib import Path
import shutil

from .fresh_run_spec import build_fresh_run_spec
from .single_fish_run import _file_hash, _utc_now, _write_json_atomic
from .single_fish_job import run_finishing_job


def prepare_fresh_batch(*, data_root: Path, repo_root: Path, validation_root: Path,
                        run_id: str, batch_path: Path) -> dict:
    """Copy writable control inputs; link only read-only raw/reference sources."""
    if validation_root.exists() or batch_path.exists():
        raise FileExistsError("Refusing an existing validation root or batch spec")
    source = data_root / "L765_f04"
    fish = validation_root / source.name
    fish.mkdir(parents=True)
    (validation_root / "matchingMetadata.csv").symlink_to(data_root / "matchingMetadata.csv")
    (fish / "01_raw").symlink_to(source / "01_raw")
    preproc = fish / "02_reg/00_preprocessing"
    preproc.mkdir(parents=True)
    for child in (source / "02_reg").iterdir():
        if child.name != "00_preprocessing" and not child.name.startswith("."):
            (fish / "02_reg" / child.name).symlink_to(child)
    for child in (source / "02_reg/00_preprocessing").iterdir():
        if child.name != "2p_anatomy" and not child.name.startswith("."):
            (preproc / child.name).symlink_to(child)
    anatomy_dir = preproc / "2p_anatomy"
    anatomy_dir.mkdir()
    anatomy_name = "L765_f04_anatomy_2P_GCaMP.nrrd"
    for name in (anatomy_name, anatomy_name + ".json"):
        origin = source / "02_reg/00_preprocessing/2p_anatomy" / name
        if origin.exists():
            shutil.copy2(origin, anatomy_dir / name)
    analysis = fish / "03_analysis"
    (analysis / "functional").mkdir(parents=True)
    (analysis / "structural").symlink_to(source / "03_analysis/structural")
    (analysis / "functional/suite2P").symlink_to(source / "03_analysis/functional/suite2P")
    if (source / "03_analysis/voxel_sizes.json").exists():
        shutil.copy2(source / "03_analysis/voxel_sizes.json", analysis / "voxel_sizes.json")
    (analysis / "confocal").mkdir()
    for child in (source / "03_analysis/confocal").iterdir():
        if child.name == "aligned":
            shutil.copytree(child, analysis / "confocal/aligned", ignore=shutil.ignore_patterns("._*"))
        elif not child.name.startswith("."):
            (analysis / "confocal" / child.name).symlink_to(child)
    control_path = fish / "03_analysis/functional/run_specs" / f"{run_id}.json"
    control = build_fresh_run_spec(fish_id=source.name, data_root=validation_root, repo_root=repo_root,
                                   run_id=run_id, spec_path=control_path, plane_indices=tuple(range(10)),
                                   ncc_cache_source=source / "03_analysis/functional/ncc")
    # Compare every CSV produced by these existing scientific stages, and all
    # ten registration matrices. Normalize only the relocated fish root.
    baseline_paths = set()
    for stage in control["stages"]:
        if not stage.get("manifest"):
            continue
        writer = stage["command"][2]
        old_manifest = source / "03_analysis/functional/pipeline_manifests" / f"{writer}_manifest.json"
        if old_manifest.exists():
            for record in json.loads(old_manifest.read_text()).get("outputs", ()):
                path = Path(record["path"])
                if path.is_file() and path.suffix in {".csv", ".mat"} and path.is_relative_to(source):
                    baseline_paths.add(path)
    if not baseline_paths or sum(p.suffix == ".mat" for p in baseline_paths) != 10:
        raise ValueError("Control needs scientific table baselines and all ten ANTs matrices")
    control["artifact_byte_baselines"] = []
    for origin in sorted(baseline_paths):
        text = origin.suffix == ".csv"
        data = origin.read_bytes()
        if text: data = data.replace(str(source).encode(), b"{fish_dir}")
        control["artifact_byte_baselines"].append({"relative_path": origin.relative_to(source).as_posix(),
                                                   "normalize_fish_root": text, "source_fish_root": str(source), "sha256": hashlib.sha256(data).hexdigest()})
    _write_json_atomic(control_path, control)
    specs = [control_path]
    for fish_id in ("L765_f02", "L758_f07", "L765_f05", "L765_f06"):
        target = data_root / fish_id / "03_analysis/functional/run_specs" / f"{run_id}.json"
        build_fresh_run_spec(fish_id=fish_id, data_root=data_root, repo_root=repo_root,
                            run_id=run_id, spec_path=target, plane_indices=tuple(range(5, 10)) if fish_id == "L765_f05" else tuple(range(10)),
                            ncc_cache_source=data_root / fish_id / "03_analysis/functional/ncc")
        specs.append(target)
    payload = {"schema_version": 1, "created_at": _utc_now(), "policy": "Stop at first failure; no automatic repair/retry. Remaining fish run only after fresh isolated control byte parity and no-op verification pass.",
               "specs": [{"path": str(p), "sha256": _file_hash(p)} for p in specs], "control_baseline_count": len(baseline_paths)}
    _write_json_atomic(batch_path, payload)
    return payload


def run_fresh_batch(batch_path: Path) -> dict:
    """Run sequentially without polling or dependence on a chat session."""
    batch = json.loads(batch_path.read_text())
    report_path = batch_path.with_suffix(".report.json")
    report = {"status": "running", "started_at": _utc_now(), "batch": str(batch_path), "jobs": []}
    _write_json_atomic(report_path, report)
    try:
        for index, record in enumerate(batch["specs"]):
            spec = Path(record["path"])
            if _file_hash(spec) != record["sha256"]:
                raise ValueError(f"Pinned batch recipe changed: {spec}")
            result = run_finishing_job(spec_path=spec, validate_noop=True)
            report["jobs"].append({"spec": str(spec), "role": "isolated_fresh_control" if index == 0 else "native_production", "status": result["status"], "report": result["report"], "error": result.get("error")})
            _write_json_atomic(report_path, report)
            if result["status"] != "pass":
                raise RuntimeError("Fresh batch stopped; preserve diagnostic output and inspect the failed job report")
        report["status"] = "pass"
        report["scientific_release"] = "review_required; proposed midlines and provisional geometry remain unaccepted"
    except Exception as exc:
        report.update(status="fail", error=f"{type(exc).__name__}: {exc}")
    report["finished_at"] = _utc_now()
    _write_json_atomic(report_path, report)
    return report
