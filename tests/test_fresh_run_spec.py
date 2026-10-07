from pathlib import Path
import pytest
from codeants_2pf_hcr import fresh_run_spec as recipe
from codeants_2pf_hcr.single_fish_run import load_run_spec
from test_single_fish_run import _build_spec


def test_shared_recipe_pins_r2_and_has_no_frozen_or_midline_acceptance(tmp_path, monkeypatch):
    spec_path, _, _ = _build_spec(tmp_path)
    old = load_run_spec(spec_path)
    fish = old.data_root / "L765_f05"
    relative = ["02_reg/00_preprocessing/2p_anatomy/L765_f05_anatomy_2P_GCaMP.nrrd",
                "03_analysis/functional/ncc/z_drift/z_drift_manifest.json",
                "03_analysis/structural/cp_masks/L765_f05_anatomy_00001_uint8_8bit_cp_masks.tif",
                "02_reg/00_preprocessing/2p_functional/01_individualPlanes/L765_f05_preprocessing_metadata.json",
                "01_raw/2p/metadata/session_metadata.csv",
                "03_analysis/confocal/aligned/L765_f05_r2_channel2_sst1_2_cp_masks_in_2p_labels_uint16.tif",
                "03_analysis/confocal/aligned/L765_f05_r2_channel2_sst1_2_cp_masks_in_2p_warp_meta.json"]
    relative += [f"02_reg/00_preprocessing/2p_functional/02_motionCorrected/L765_f05_plane{i}_mcorrected.tif" for i in range(5, 10)]
    relative += [f"03_analysis/functional/suite2P/plane{i}/L765_f05_plane{i}_{suffix}.npy" for i in range(5, 10) for suffix in ("F", "Fneu", "stat", "ops", "iscell", "spks")]
    for name in relative:
        path = fish / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"fixture")
    (old.data_root / "matchingMetadata.csv").write_text("fixture")
    from codeants_2pf_hcr import suite2p_provenance
    monkeypatch.setattr(suite2p_provenance, "check_suite2p_input_provenance", lambda **kwargs: {"status": "pass"})
    manifest = fish / "02_reg/00_preprocessing/spatial_preprocessing_manifest.json"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text("{}")
    cache = tmp_path / "ncc"
    cache.mkdir()
    for name in ("ncc_bestz_by_plane.json", "ncc_scale_by_fish.json"):
        (cache / name).write_text("{}")
    monkeypatch.setattr(recipe, "resolve_func_polarity", lambda *a, **kw: ("south", "fixture"))
    monkeypatch.setattr(recipe.importlib.metadata, "version", lambda n: "fixture")
    notebook_dir = old.repo_root / "notebooks/qc"
    notebook_dir.mkdir(parents=True)
    for i in range(6):
        (notebook_dir / f"{i:02}_qc.ipynb").write_text("{}")
    import subprocess
    subprocess.run(["git", "add", "notebooks"], cwd=old.repo_root, check=True)
    subprocess.run(["git", "commit", "-qm", "QC fixtures"], cwd=old.repo_root, check=True)
    target = tmp_path / "fresh.json"
    payload = recipe.build_fresh_run_spec(fish_id="L765_f05", data_root=old.data_root, repo_root=old.repo_root,
                                        run_id="test", spec_path=target, plane_indices=tuple(range(5, 10)), ncc_cache_source=cache)
    parsed = load_run_spec(target)
    assert len(parsed.stages) == 18
    assert payload["native_production"] == {"mode": "fresh", "reuse_stages": []}
    assert "materialize-anatomy-midline" not in {s.command[2] for s in parsed.stages}
    assert payload["accepted_choices"]["midline_gate"] == "review_required"
    assert payload["accepted_choices"]["eligible_planes"] == list(range(5, 10))
    assert not any("L765_f04" in value for s in parsed.stages for value in s.command + s.inputs)
    register = parsed.stages[2].command
    assert [register[i+1] for i, value in enumerate(register) if value == "--reference-plane-index"] == list(map(str, range(5, 10)))
    assert "--reuse-ncc-cache-dir" in register
    assert register[register.index("--ants-deterministic-seed") + 1] == "123"
    assert parsed.stages[2].parameter_pins["ants_deterministic_seed"] == 123
    fresh = recipe.build_fresh_run_spec(fish_id="L765_f05", data_root=old.data_root, repo_root=old.repo_root,
                                        run_id="search", spec_path=tmp_path / "search.json", plane_indices=tuple(range(5, 10)),
                                        geometry_control_supersede_reason="corrected registration")
    fresh_register = fresh["stages"][2]["command"]
    assert "--reuse-ncc-cache-dir" not in fresh_register
    assert not any("input_bundles" in value for value in fresh_register + fresh["stages"][2]["inputs"])
    assert not (target.parent / "input_bundles" / "search").exists()
    match = fresh["stages"][6]["command"]
    assert match[2] == "match-roi-to-anatomy"
    assert match[match.index("--supersede-accepted-control-reason") + 1] == "corrected registration"
    assert "--supersede-accepted-control-reason" not in parsed.stages[6].command
    with pytest.raises(ValueError, match="restricted to R2"):
        recipe.build_fresh_run_spec(fish_id="L765_f05", data_root=old.data_root, repo_root=old.repo_root,
                                    run_id="invalid", spec_path=tmp_path / "invalid.json", plane_indices=tuple(range(10)), ncc_cache_source=cache)
