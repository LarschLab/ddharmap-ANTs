import fcntl
import json
from pathlib import Path

from codeants_2pf_hcr import single_fish_job as job
from test_single_fish_run import _build_spec


def _args(tmp_path):
    spec, _, _ = _build_spec(tmp_path)
    return dict(spec_path=spec, notebook_dir=tmp_path / "qc", manifest_dir=tmp_path / "manifests",
                gallery_dir=tmp_path / "gallery", report_dir=tmp_path / "reports", validate_noop=True)


def test_finishing_job_noop_and_compact_saved_report(tmp_path, monkeypatch):
    args = _args(tmp_path)
    monkeypatch.setattr(job, "audit_run_artifacts", lambda spec: {"ok": True})
    monkeypatch.setattr(job, "_gallery", lambda **kwargs: {"figure_count": 57})
    result = job.run_finishing_job(**args)
    assert result["status"] == "pass"
    assert result["steps"]["no-op-resume"]["stage_records_unchanged"] is True
    assert json.loads(Path(result["report"]).read_text()) == result
    assert Path(result["log"]).exists()


def test_audit_failure_stops_before_gallery_and_persists_error(tmp_path, monkeypatch):
    args = _args(tmp_path)
    monkeypatch.setattr(job, "audit_run_artifacts", lambda spec: {"ok": False})
    monkeypatch.setattr(job, "_gallery", lambda **kwargs: (_ for _ in ()).throw(AssertionError("must not export")))
    result = job.run_finishing_job(**args)
    assert result["status"] == "fail"
    assert result["phase"] == "audit-artifacts"
    assert "audit-artifacts failed" in result["error"]


def test_invalid_spec_has_terminal_report(tmp_path):
    args = _args(tmp_path)
    args["spec_path"].write_text("{}")
    result = job.run_finishing_job(**args)
    assert result["status"] == "fail"
    assert result["phase"] == "load-spec"
    assert "RunSpecError" in result["error"]


def test_notebook_default_ambiguity_fails_before_execution(tmp_path, monkeypatch):
    args = _args(tmp_path)
    args["notebook_dir"] = None
    monkeypatch.setattr(job, "execute_run", lambda *a, **kw: (_ for _ in ()).throw(AssertionError("must not execute")))
    result = job.run_finishing_job(**args)
    assert result["status"] == "fail"
    assert "no unique QC" in result["error"]


def test_default_gallery_versions_changed_qc_and_reuses_same_version(tmp_path, monkeypatch):
    args = _args(tmp_path)
    args.update(gallery_dir=None, validate_noop=False)
    args["notebook_dir"].mkdir()
    nb = args["notebook_dir"] / "00_qc.ipynb"
    nb.write_text("first QC")
    destinations = []
    monkeypatch.setattr(job, "verify_run", lambda spec: {"ok": True})
    monkeypatch.setattr(job, "audit_run_artifacts", lambda spec: {"ok": True})
    monkeypatch.setattr(job, "_gallery", lambda **kw: destinations.append(kw["output_dir"]) or {"figure_count": 57})
    assert job.run_finishing_job(**args)["status"] == "pass"
    assert job.run_finishing_job(**args)["status"] == "pass"
    assert destinations[0] == destinations[1]
    nb.write_text("corrected QC")
    assert job.run_finishing_job(**args)["status"] == "pass"
    assert destinations[2] != destinations[0]


def test_same_fish_lock_blocks_before_execution(tmp_path, monkeypatch):
    args = _args(tmp_path)
    spec = job.load_run_spec(args["spec_path"])
    lock_path = spec.data_root / spec.fish_id / "03_analysis/functional/single_fish_job.lock"
    lock_path.parent.mkdir(parents=True)
    monkeypatch.setattr(job, "execute_run", lambda *a, **kw: (_ for _ in ()).throw(AssertionError("must not execute")))
    with lock_path.open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = job.run_finishing_job(**args)
    assert result["status"] == "fail"
    assert "BlockingIOError" in result["error"]


def test_existing_gallery_requires_exact_inventory_and_source_hashes(tmp_path):
    gallery = tmp_path / "gallery"
    gallery.mkdir()
    notebooks = tmp_path / "qc"
    notebooks.mkdir()
    nb = notebooks / "00_qc.ipynb"
    nb.write_text("source")
    manifests = tmp_path / "manifests"
    manifests.mkdir()
    report = {"notebook_coverage": [{"path": str(nb), "sha256": job._file_hash(nb)}],
              "writer_manifest_coverage": [], "figures": [], "figure_count": 0,
              "notebook_png_output_count": 0, "writer_png_reference_count": 0}
    (gallery / "gallery_manifest.json").write_text(json.dumps(report))
    (gallery / "index.html").write_text("gallery")
    inventory = {p.name: job._file_hash(p) for p in gallery.iterdir()}
    (gallery / "job_inventory.json").write_text(json.dumps({"sha256": inventory}))
    kwargs = dict(notebook_dir=notebooks, manifest_dir=manifests, output_dir=gallery, fish_dir=tmp_path)
    assert job._gallery(**kwargs)["figure_count"] == 0
    nb.write_text("changed")
    import pytest
    with pytest.raises(RuntimeError, match="source changed"):
        job._gallery(**kwargs)
