import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import tifffile

from codeants_2pf_hcr.ncc_contract import (
    NCCContractError,
    load_preprocessing_ncc_handoff,
    plane_refs_from_preprocessing_ncc_handoff,
)
from codeants_2pf_hcr import spatial
from codeants_2pf_hcr.pipeline import (
    SingleFishPipelineConfig,
    functional_to_anatomy_registration_root,
    resolve_pipeline_paths,
    run_register_functional_to_anatomy_stage,
)


def _handoff(tmp_path: Path, *, status: str = "pass_candidate") -> tuple[Path, Path]:
    fish_id = "L000_f00"
    fish = tmp_path / fish_id
    ncc = fish / "03_analysis" / "functional" / "ncc"
    references = ncc / "functional_references"
    references.mkdir(parents=True)
    anatomy = fish / "02_reg" / "00_preprocessing" / "2p_anatomy" / f"{fish_id}_anatomy_2P_GCaMP.nrrd"
    anatomy.parent.mkdir(parents=True)
    anatomy.touch()
    reference = references / f"{fish_id}_plane0_pooled_post_block0_ref.tif"
    tifffile.imwrite(reference, np.arange(12, dtype=np.float32).reshape(3, 4))
    placement = ncc / "ncc_scale_bestz_by_plane.csv"
    pd.DataFrame([
        {
            "fish_id": fish_id,
            "session": "R1",
            "plane_index": 0,
            "plane_label": f"{fish_id}_plane0_mcorrected",
            "reference_selection": "pooled_post_block0",
            "scale": 1.25,
            "best_z": 1,
            "best_z_subslice": 1.2,
            "max_ncc": 0.8,
            "placement_x": 7,
            "placement_y": 9,
            "reference_path": str(reference),
        }
    ]).to_csv(placement, index=False)
    profiles = ncc / "ncc_anchor_profiles.csv"
    pd.DataFrame(
        {
            "fish_id": [fish_id] * 3,
            "plane_index": [0, 0, 0],
            "anatomy_z": [0, 1, 2],
            "ncc": [0.4, 0.8, 0.5],
        }
    ).to_csv(profiles, index=False)
    manifest = ncc / "functional_anatomy_qc_manifest.json"
    manifest.write_text(json.dumps({
        "stage": "functional_anatomy_ncc_qc",
        "version": 4,
        "fish_id": fish_id,
        "status": status,
        "inputs": {"canonical_anatomy": str(anatomy)},
        "downstream_handoff": {
            "schema": "functional_anatomy_ncc_handoff_v1",
            "authoritative_placement_table": str(placement),
            "authoritative_anchor_profiles": str(profiles),
            "functional_reference_directory": str(references),
            "xy_frame": "codeants_2p_canonical_xy_v1",
            "z_frame": "codeants_confocal_registration_z_v1",
        },
    }))
    return manifest, anatomy


def test_handoff_reconstructs_reference_scale_z_profile_and_xy(tmp_path: Path) -> None:
    manifest, anatomy = _handoff(tmp_path)
    handoff = load_preprocessing_ncc_handoff(
        manifest,
        fish_id="L000_f00",
        anatomy_stack_path=anatomy,
    )
    refs = plane_refs_from_preprocessing_ncc_handoff(handoff, anatomy_z_count=3)
    assert len(refs) == 1
    ref = refs[0]
    assert (ref["index"], ref["scale"], ref["best_z"]) == (0, 1.25, 1)
    assert ref["best_z_subslice"] == 1.2
    assert ref["ncc_xy"] == {"x0": 7, "y0": 9, "score": 0.8, "source": "preprocessing_ncc_handoff"}
    np.testing.assert_allclose(ref["ncc_scores"], [0.4, 0.8, 0.5])
    assert ref["ref_scaled_shape"] == (4, 5)


def test_handoff_rejects_nonpassing_drift_gate(tmp_path: Path) -> None:
    manifest, anatomy = _handoff(tmp_path, status="fail_candidate")
    with pytest.raises(NCCContractError, match="does not permit static registration"):
        load_preprocessing_ncc_handoff(manifest, fish_id="L000_f00", anatomy_stack_path=anatomy)


def test_inplane_placement_reuses_xy_without_running_ncc_search(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(spatial, "ncc_xy", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("recomputed")))
    result = spatial._ncc_in_plane_result(
        ref_scaled=np.ones((2, 2), dtype=np.float32),
        fixed_slice=np.ones((6, 6), dtype=np.float32),
        use_cv2=False,
        display_normalize_placed=False,
        initial_ncc_xy={"x0": 2, "y0": 3, "score": 0.9},
    )
    assert result["ncc_xy"] == {"x0": 2, "y0": 3, "score": 0.9}


def test_registration_stage_uses_handoff_without_writing_legacy_ncc_caches(tmp_path: Path) -> None:
    import SimpleITK as sitk

    manifest_path, declared_anatomy = _handoff(tmp_path)
    anatomy = np.zeros((3, 12, 12), dtype=np.uint8)
    sitk.WriteImage(sitk.GetImageFromArray(anatomy), str(declared_anatomy), useCompression=False)
    config = SingleFishPipelineConfig(
        fish_id="L000_f00",
        local_root=tmp_path,
        strict=True,
        pipeline_root=tmp_path / "staged",
    )
    result = run_register_functional_to_anatomy_stage(
        config,
        anatomy_stack_path=declared_anatomy,
        preprocessing_ncc_manifest_path=manifest_path,
        run_inplane_comparison=False,
        emit_visual_qa=False,
        force_recompute=True,
    )
    stage_root = functional_to_anatomy_registration_root(resolve_pipeline_paths(config))
    assert result.status == "pass"
    assert result.parameters["ncc_search_reused"] is True
    assert not (stage_root / "ncc" / "ncc_scale_by_fish.json").exists()
    assert not (stage_root / "ncc" / "ncc_bestz_by_plane.json").exists()
    summary = json.loads((stage_root / "plane_refs_summary.json").read_text())
    assert summary[0]["best_z"] == 1
    assert summary[0]["best_z_subslice"] == 1.2
    assert summary[0]["ncc_xy"]["x0"] == 7
