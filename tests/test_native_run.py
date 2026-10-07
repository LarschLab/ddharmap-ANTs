import json
from pathlib import Path
import pytest
from codeants_2pf_hcr.native_paths import native_product_path
from codeants_2pf_hcr.native_run import promote_files, rollback_promotion, _rebind
from codeants_2pf_hcr.native_run import validate_native_inputs


def test_routes_preserve_native_consumers(tmp_path):
    fish = tmp_path / "L000_f00"
    assert native_product_path(fish, "register-functional-to-anatomy", "plane_refs_summary.json") == fish / "03_analysis/functional/registration/plane_refs_summary.json"
    assert native_product_path(fish, "make-motion-auc-tables", "registration/motion_auc_plot_points.csv") == fish / "03_analysis/functional/derived/motion_auc_plot_points.csv"
    assert native_product_path(fish, "make-figures", "04_plots/example.png") == fish / "04_plots/example.png"


def test_fresh_native_run_has_no_frozen_bundle_dependency(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from codeants_2pf_hcr import single_fish_run
    from codeants_2pf_hcr.native_run import execute_native_run
    monkeypatch.setattr(single_fish_run, "validate_spec_environment", lambda spec: {"ok": True})
    spec = SimpleNamespace(data_root=tmp_path, fish_id="L000_f00", pipeline_root=tmp_path / "L000_f00", state_root=tmp_path / "state",
                           raw={"native_production": {"mode": "fresh"}}, stages=[])
    result = execute_native_run(spec, resume=False)
    assert result["status"] == "pass"
    assert not (spec.state_root / "transaction_graph/make-anatomy-midline-proposal").exists()
    spec.raw["native_production"]["reuse_stages"] = ["prepare-functional-reference-stacks"]
    with pytest.raises(ValueError, match="cannot reuse"):
        execute_native_run(spec, resume=False)


def test_unknown_native_mode_fails_before_writes(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from codeants_2pf_hcr import single_fish_run
    from codeants_2pf_hcr.native_run import execute_native_run
    monkeypatch.setattr(single_fish_run, "validate_spec_environment", lambda spec: {"ok": True})
    spec = SimpleNamespace(data_root=tmp_path, fish_id="L000_f00", state_root=tmp_path / "state",
                           raw={"native_production": {"mode": "typo"}}, stages=[])
    with pytest.raises(ValueError, match="Unknown native"):
        execute_native_run(spec, resume=False)
    assert not spec.state_root.exists()


def test_fresh_zero_seed_is_rejected_before_any_write(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from codeants_2pf_hcr import single_fish_run
    from codeants_2pf_hcr.native_run import execute_native_run
    monkeypatch.setattr(single_fish_run, "validate_spec_environment", lambda spec: {"ok": True})
    spec = SimpleNamespace(data_root=tmp_path, fish_id="L000_f00", pipeline_root=tmp_path / "L000_f00", state_root=tmp_path / "state",
                           raw={"native_production": {"mode": "fresh"}}, stages=[SimpleNamespace(command=("python", "writer", "register-functional-to-anatomy", "--ants-deterministic-seed", "0"))])
    with pytest.raises(ValueError, match="positive fixed seed"):
        execute_native_run(spec, resume=False)
    assert not spec.state_root.exists()


def test_fresh_writer_stages_explicit_output_and_tracks_every_product(tmp_path, monkeypatch):
    from dataclasses import replace
    from test_single_fish_run import _build_spec
    from codeants_2pf_hcr import single_fish_run as runner
    from codeants_2pf_hcr.native_run import execute_native_run
    path, _, _ = _build_spec(tmp_path)
    spec = runner.load_run_spec(path)
    fish = spec.data_root / spec.fish_id
    destination = fish / "02_reg/00_preprocessing/2p_anatomy/test.nrrd"
    manifest = fish / "03_analysis/functional/pipeline_manifests/prepare-in-vivo-anatomy-stack_manifest.json"
    command = (str(spec.python), "tools/single_fish_pipeline.py", "prepare-in-vivo-anatomy-stack",
               "--pipeline-root", str(fish), "--output-path", str(destination))
    stage = runner.StageSpec(name="anatomy", command=command, inputs=(), outputs=(str(manifest),), manifest=str(manifest), parameter_pins={})
    spec = replace(spec, pipeline_root=fish, raw={**spec.raw, "native_production": {"mode": "fresh"}}, stages=(stage,))
    runtime = {"ok": True, "git": {"actual_commit": spec.git_commit}, "interpreter": {"actual_executable": str(spec.python)}}
    monkeypatch.setattr(runner, "validate_spec_environment", lambda spec: runtime)
    def execute(command, *, cwd, env, log_path):
        out = Path(command[command.index("--output-path") + 1])
        assert out != destination and not destination.exists()
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(b"new anatomy")
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(json.dumps({"stage_name": command[2], "fish_id": spec.fish_id, "status": "pass", "errors": [], "dry_run": False, "parameters": {}, "outputs": [{"path": str(out)}]}))
        return 0
    monkeypatch.setattr(runner, "_run_command", execute)
    assert execute_native_run(spec, resume=False)["status"] == "pass"
    assert destination.read_bytes() == b"new anatomy"
    assert runner._stage_current(spec, stage, runtime)[0]
    destination.write_bytes(b"tampered")
    current, reasons = runner._stage_current(spec, stage, runtime)
    assert not current and "promoted writer products changed" in reasons


def test_noop_native_resume_preserves_layout_mtime_and_blocks_mismatch(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from codeants_2pf_hcr import single_fish_run
    from codeants_2pf_hcr.native_run import execute_native_run
    fish = tmp_path / "L000_f00"
    state = fish / "state"
    frozen = tmp_path / "frozen"
    (frozen / "make-anatomy-midline-proposal").mkdir(parents=True)
    spec = SimpleNamespace(data_root=tmp_path, fish_id="L000_f00", state_root=state,
                           raw={"native_production": {"frozen_root": str(frozen), "original_root": "/old"}}, stages=[])
    monkeypatch.setattr(single_fish_run, "validate_spec_environment", lambda spec: {"ok": True})
    execute_native_run(spec, resume=True)
    layout = fish / "03_analysis/functional/pipeline_manifests/native_layout.json"
    mtime = layout.stat().st_mtime_ns
    execute_native_run(spec, resume=True)
    assert layout.stat().st_mtime_ns == mtime
    layout.write_text("{}")
    with pytest.raises(RuntimeError, match="layout disagrees"):
        execute_native_run(spec, resume=True)


def test_native_score_parity_uses_own_stage_control_even_when_missing(tmp_path):
    from types import SimpleNamespace
    from codeants_2pf_hcr.pipeline import _stage_output_specs
    functional = tmp_path / "fish/03_analysis/functional"
    paths = SimpleNamespace(functional_registration_dir=functional / "registration",
                            pipeline_root=tmp_path / "graph")
    old = _stage_output_specs(paths, "score-activity-bpi")
    assert old[0].control_path == str(functional / "registration/functional_roi_activity_identity.csv")
    marker = functional / "pipeline_manifests/native_layout.json"
    marker.parent.mkdir(parents=True);marker.write_text("{}")
    new = _stage_output_specs(paths, "score-activity-bpi")
    assert new[0].control_path == str(functional / "derived/score-activity-bpi/registration/functional_roi_activity_identity.csv")
    assert not Path(new[0].control_path).exists()


def test_native_accepted_proposal_manifest_tracks_routed_reviewed_bytes(tmp_path):
    from codeants_2pf_hcr.native_run import _native_proposal_manifest
    fish = tmp_path / "fish";frozen = tmp_path / "frozen";root = "/reviewed/root"
    relative = "make-anatomy-midline-proposal/qa/anatomy_midline_proposals.csv"
    source = frozen / relative;source.parent.mkdir(parents=True);source.write_text("reviewed")
    target = fish / "03_analysis/functional/qa/anatomy_midline_proposals.csv"
    target.parent.mkdir(parents=True);target.write_text("reviewed")
    manifest = tmp_path / "proposal_manifest.json"
    manifest.write_text(json.dumps({"status": "pass", "errors": [], "generated_at": "reviewed_time", "parameters": {"pipeline_root": root}, "inputs": [], "outputs": [{"path": root + "/" + relative, "kind": "file", "required": True}]}))
    result = _native_proposal_manifest(manifest,frozen,[str(tmp_path / "graph"),root,str(frozen)],fish)
    assert result["outputs"][0]["path"] == str(target)
    assert result["outputs"][0]["exists"] is True
    assert result["pipeline_root"] == str(fish)
    assert result["parameters"]["pipeline_root"] == str(fish)
    assert result["reused_from"]["original_pipeline_root"] == root
    assert result["reused_from"]["writer_generated_at"] == "reviewed_time"
    target.write_text("corrupted")
    with pytest.raises(RuntimeError,match="reviewed bytes"):
        _native_proposal_manifest(manifest,frozen,[str(tmp_path / "graph"),root,str(frozen)],fish)


def test_split_native_inputs_reject_synthetic_aggregate_before_promotion(tmp_path):
    fish = tmp_path / "L000_f00"
    actual = native_product_path(fish, "transform-functional-rois-to-anatomy", "functional/anatomy/a.tif")
    actual.parent.mkdir(parents=True);actual.write_text("existing")
    validate_native_inputs([str(actual)], [])
    with pytest.raises(RuntimeError, match="before promotion"):
        validate_native_inputs([str(fish / "functional")], [])


def test_rebind_exact_products_and_keep_history(tmp_path):
    fish = tmp_path / "L000_f00"; root = "/source/run"
    value = root + "/register-functional-to-anatomy/ncc/transforms/a.mat"
    assert _rebind(value, [root], fish) == str(fish / "03_analysis/functional/ncc/transforms/a.mat")
    value = root + "/match-roi-to-anatomy/registration/functional_roi_anatomy_matches.csv"
    assert _rebind(value, [root], fish) == str(fish / "03_analysis/functional/registration/functional_roi_anatomy_matches.csv")


@pytest.mark.parametrize("corrupt", [False, True])
def test_midline_relocation_preserves_review_and_rejects_semantic_corruption(tmp_path, corrupt):
    from codeants_2pf_hcr.native_run import _relocate_accepted_midline
    from codeants_2pf_hcr.single_fish_run import _file_hash
    fish = tmp_path / "fish"; frozen = tmp_path / "frozen"; root = "/reviewed/root"
    relative = "register-functional-to-anatomy/plane_refs_summary.json"
    source = frozen / relative; source.parent.mkdir(parents=True)
    source.write_text(json.dumps({"transform": root + "/register-functional-to-anatomy/ncc/a.mat", "x": 1}))
    target = fish / "03_analysis/functional/registration/plane_refs_summary.json"
    target.parent.mkdir(parents=True); roots = [str(tmp_path / "graph"), root, str(frozen)]
    text = _rebind(source.read_text(), roots, fish)
    target.write_text(text.replace('"x": 1', '"x": 2') if corrupt else text)
    payload = {"accepted": True, "lines_by_plane": {"0": {"x0": 3}}, "source_artifacts": [{"path": root + "/" + relative, "sha256": _file_hash(source)}]}
    reviewed = frozen / "materialize-anatomy-midline/anatomy_midline.json"
    reviewed.parent.mkdir(); reviewed.write_text(json.dumps(payload))
    sidecar = tmp_path / "sidecar.json"; sidecar.write_text(_rebind(json.dumps(payload), roots, fish))
    if corrupt:
        with pytest.raises(RuntimeError, match="exact approved path-only"):
            _relocate_accepted_midline(sidecar, frozen, roots, fish)
    else:
        _relocate_accepted_midline(sidecar, frozen, roots, fish)
        bound = json.loads(sidecar.read_text())
        assert bound["lines_by_plane"] == payload["lines_by_plane"]
        assert bound["source_artifacts"][0]["sha256"] == _file_hash(target)
        assert bound["relocation_provenance"][0]["reviewed_sha256"] == _file_hash(source)


def test_exact_backup_and_crash_rollback(tmp_path):
    fish = tmp_path / "L000_f00"; target = fish / "03_analysis/functional/registration/a.csv"
    target.parent.mkdir(parents=True); target.write_text("old")
    source = tmp_path / "new.csv";source.write_text("new")
    journal = tmp_path / "journal"
    promote_files(fish, {source: target}, journal)
    assert target.read_text() == "new"
    assert (journal / "original-0").read_text() == "old"
    rollback_promotion(journal)
    assert target.read_text() == "old"
    rollback_promotion(journal)
    assert target.read_text() == "old"


def test_late_copy_failure_rolls_back_first_product(tmp_path, monkeypatch):
    import codeants_2pf_hcr.native_run as module
    fish = tmp_path / "L000_f00"; folder = fish / "03_analysis/functional/registration";folder.mkdir(parents=True)
    a = folder / "a.csv";a.write_text("old-a");b = folder / "b.csv";b.write_text("old-b")
    source = tmp_path / "source.csv";source.write_text("new")
    original = module.shutil.copy2
    def fail(source_path, target_path):
        if str(target_path).endswith("original-1"): raise OSError("injected backup failure")
        return original(source_path,target_path)
    monkeypatch.setattr(module.shutil,"copy2",fail)
    with pytest.raises(OSError,match="injected"):
        promote_files(fish,{source:a, source.with_name("missing.csv"):b},tmp_path / "journal")
    assert a.read_text() == "old-a" and b.read_text() == "old-b"


def test_explicit_frozen_aligned_source_checks_existing_masks(tmp_path):
    from codeants_2pf_hcr.native_run import _verify_reuse_inputs
    fish = tmp_path / "fish"; aligned = fish / "03_analysis/confocal/aligned"
    aligned.mkdir(parents=True); override = tmp_path / "frozen";override.mkdir()
    name = "x_cp_masks_in_2p_labels_uint16.tif"
    (aligned / name).write_bytes(b"accepted");(override / name).write_bytes(b"accepted")
    manifest = {"inputs": [{"path": str(aligned), "kind": "glob", "pattern": "*_matches.csv", "required": True, "exists": False}]}
    with pytest.raises(RuntimeError, match="no longer exists"):
        _verify_reuse_inputs(manifest, fish, "unused")
    _verify_reuse_inputs(manifest, fish, "unused", aligned_override=str(override))
    (aligned / name).write_bytes(b"changed")
    with pytest.raises(RuntimeError, match="differs"):
        _verify_reuse_inputs(manifest, fish, "unused", aligned_override=str(override))


def test_unknown_product_fails_without_native_overwrite(tmp_path):
    fish = tmp_path / "L000_f00";source = tmp_path / "new.csv";source.write_text("new")
    with pytest.raises(ValueError,match="ownership"):
        promote_files(fish,{source:fish / "01_raw/unsafe.csv"},tmp_path / "journal")


@pytest.mark.parametrize("prior_manifest", [True, False])
def test_crash_after_publication_restores_manifest_and_products(tmp_path, prior_manifest):
    fish = tmp_path / "L000_f00"; target = fish / "03_analysis/functional/registration/a.csv"
    target.parent.mkdir(parents=True);target.write_text("old")
    manifest = tmp_path / "manifest.json"
    if prior_manifest: manifest.write_text(json.dumps({"old": True}))
    source = tmp_path / "new.csv";source.write_text("new");journal = tmp_path / "journal"
    promote_files(fish,{source:target},journal,manifest_path=manifest)
    saved = json.loads((journal / "previous_manifest.json").read_text())
    assert saved["payload"] == ({"old": True} if prior_manifest else None)
    manifest.write_text(json.dumps({"new": True}))
    proposal = tmp_path / "proposal_manifest.json"
    proposal.write_text(json.dumps({"new_proposal": True}))
    (journal / "previous_proposal_manifest.json").write_text(json.dumps({"path": str(proposal), "payload": {"reviewed": True} if prior_manifest else None}))
    rollback_promotion(journal)
    if prior_manifest:
        assert json.loads(proposal.read_text()) == {"reviewed": True}
    else:
        assert not proposal.exists()
        assert json.loads((journal / "previous_proposal_manifest-uncommitted.json").read_text()) == {"new_proposal": True}
        assert json.loads((journal / "previous_manifest-uncommitted.json").read_text()) == {"new": True}
    assert target.read_text() == "old"
    if prior_manifest: assert json.loads(manifest.read_text()) == {"old": True}
    else: assert not manifest.exists()
