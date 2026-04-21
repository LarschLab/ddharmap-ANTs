from pathlib import Path

import numpy as np
import tifffile

from codeants_2pf_hcr import (
    AnatomyNormalizationStageConfig,
    FunctionalPlacementConfig,
    FunctionalReferenceConfig,
    RegistrationSearchConfig,
    build_functional_references_stage,
    normalize_anatomy_stack_stage,
    run_ncc_placement_stage,
    run_registration_search_stage,
)


def test_normalize_anatomy_stack_stage_keeps_tiff_path_when_no_conversion_needed(tmp_path: Path) -> None:
    anat_path = tmp_path / "anat.tif"
    tifffile.imwrite(anat_path, np.arange(16, dtype=np.uint16).reshape(4, 4))

    result = normalize_anatomy_stack_stage(
        anat_stack_path=anat_path,
        tmp_convert_dir=tmp_path / "converted",
        preproc_dir=tmp_path / "preproc",
        config=AnatomyNormalizationStageConfig(force_recompute_anat_convert=False),
    )

    assert result["bindings"]["ANAT_STACK_PATH"] == anat_path
    assert result["bindings"]["ANAT_STACK_PATH_ORIG"] == anat_path
    assert "[INFO] Anatomy already TIFF; no conversion" in result["log_lines"]


def test_functional_reference_registration_and_placement_stage_chain(tmp_path: Path) -> None:
    out_raw = tmp_path / "raw"
    out_ncc = tmp_path / "ncc"
    out_raw.mkdir()
    out_ncc.mkdir()

    func_path = tmp_path / "fish_plane_stack_flipX.tif"
    func = np.zeros((2, 2, 4, 4), dtype=np.float32)
    func[:, 0, :, :] = np.array(
        [
            [1, 2, 1, 0],
            [2, 4, 2, 0],
            [1, 2, 1, 0],
            [0, 0, 0, 0],
        ],
        dtype=np.float32,
    )
    func[:, 1, :, :] = np.array(
        [
            [0, 0, 0, 0],
            [0, 3, 3, 0],
            [0, 3, 6, 0],
            [0, 0, 0, 0],
        ],
        dtype=np.float32,
    )
    tifffile.imwrite(func_path, func)

    refs_result = build_functional_references_stage(
        flipped_list=[func_path],
        out_raw=out_raw,
        outdir=tmp_path / "legacy",
        vox_func_by_path={str(func_path): {"X": 1.0, "Y": 1.0, "Z": 1.0}},
        config=FunctionalReferenceConfig(
            use_top_corr_refs=False,
            reuse_saved_refs=False,
            force_recompute_refs=False,
        ),
    )

    plane_refs = refs_result["plane_refs"]
    assert len(plane_refs) == 2
    assert refs_result["ref2d_raw"].shape == (4, 4)
    assert plane_refs[0]["label"].endswith("plane0")
    assert plane_refs[1]["label"].endswith("plane1")

    anat_path = tmp_path / "anat_stack.tif"
    anat = np.zeros((5, 4, 4), dtype=np.float32)
    anat[1] = plane_refs[0]["ref2d_raw"]
    anat[3] = plane_refs[1]["ref2d_raw"]
    tifffile.imwrite(anat_path, anat)

    registration_result = run_registration_search_stage(
        anat_stack_path=anat_path,
        plane_refs=plane_refs,
        fish_id="TEST_FISH",
        out_ncc=out_ncc,
        vox_anat={"X": 1.0, "Y": 1.0, "Z": 1.0},
        vox_func={"X": 1.0, "Y": 1.0, "Z": 1.0},
        config=RegistrationSearchConfig(
            manual_scale=1.0,
            scale_metric="max_score",
            scale_coarse=(1.0, 1.0, 1.0),
            scale_fine=(0.0, 1.0),
            scale_xfine=(0.0, 1.0),
            scale_ufine=(0.0, 1.0),
            scale_refine_only=False,
            scale_per_plane=False,
            use_cv2=False,
        ),
    )

    assert set(registration_result["df"]["best_z"]) == {1, 3}
    assert all("ref_match" in pr for pr in registration_result["plane_refs"])

    placement_result = run_ncc_placement_stage(
        plane_refs=registration_result["plane_refs"],
        anat_f=registration_result["anat_f"],
        best_z=registration_result["best_z"],
        config=FunctionalPlacementConfig(
            use_ncc_placement=True,
            display_normalize_placed=True,
            use_cv2=False,
        ),
    )

    assert placement_result["bindings"]["plane_refs"] is registration_result["plane_refs"]
    assert placement_result["ref_warped_raw"] is not None
    assert placement_result["ref_warped"] is not None
    for plane_ref in registration_result["plane_refs"]:
        assert "ncc_xy" in plane_ref
        assert plane_ref.get("tform_src") == "ncc_xy"
        assert plane_ref.get("ref_warped") is not None
