from pathlib import Path

import json
import numpy as np
import tifffile

from codeants_2pf_hcr import (
    AnatomyNormalizationStageConfig,
    AnatomyUint8PreprocessingConfig,
    FunctionalPlacementConfig,
    FunctionalReferenceConfig,
    RegistrationSearchConfig,
    build_functional_references_stage,
    normalize_anatomy_stack_stage,
    preprocess_anatomy_uint8_stage,
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


def test_preprocess_anatomy_uint8_stage_offsets_signed_stack_and_rebinds_path(tmp_path: Path) -> None:
    preproc_dir = tmp_path / "preproc"
    raw_path = preproc_dir / "2p_anatomy" / "fish_anatomy_2P_GCaMP.tif"
    raw_path.parent.mkdir(parents=True)
    stack = np.array([[[-10, 0], [10, 30]]], dtype=np.int16)
    tifffile.imwrite(raw_path, stack)

    result = preprocess_anatomy_uint8_stage(
        anat_stack_path=raw_path,
        anat_stack_path_orig=raw_path,
        preproc_dir=preproc_dir,
        config=AnatomyUint8PreprocessingConfig(
            force_recompute_anat_uint8=True,
            apply_func_orientation=False,
            target_xy_shape=None,
        ),
    )

    out_path = result["bindings"]["ANAT_8BIT_STACK_PATH"]
    out = tifffile.imread(out_path)
    assert out.dtype == np.uint8
    assert out.tolist() == [[[0, 64], [128, 255]]]
    assert out_path == preproc_dir / "2p_anatomy" / "fish_anatomy_2P_GCaMP_uint8.tif"
    assert result["bindings"]["ANAT_STACK_PATH"] == out_path
    assert result["bindings"]["ANAT_STACK_PATH_ORIG"] == raw_path
    assert result["bindings"]["ANAT_STACK_PATH_16BIT"] == raw_path
    assert result["artifacts"]["negative_offset"] == 10


def test_preprocess_anatomy_uint8_stage_handles_constant_signed_stack(tmp_path: Path) -> None:
    preproc_dir = tmp_path / "preproc"
    raw_path = preproc_dir / "2p_anatomy" / "fish_anatomy_2P_GCaMP.tif"
    raw_path.parent.mkdir(parents=True)
    tifffile.imwrite(raw_path, np.full((2, 2), -4, dtype=np.int16))

    result = preprocess_anatomy_uint8_stage(
        anat_stack_path=raw_path,
        preproc_dir=preproc_dir,
        config=AnatomyUint8PreprocessingConfig(
            force_recompute_anat_uint8=True,
            apply_func_orientation=False,
            target_xy_shape=None,
        ),
    )

    out = tifffile.imread(result["bindings"]["ANAT_8BIT_STACK_PATH"])
    assert out.dtype == np.uint8
    assert int(out.min()) == 0
    assert int(out.max()) == 0
    assert result["artifacts"]["negative_offset"] == 4


def test_preprocess_anatomy_uint8_stage_applies_functional_orientation(tmp_path: Path) -> None:
    preproc_dir = tmp_path / "preproc"
    raw_path = preproc_dir / "2p_anatomy" / "fish_anatomy_2P_GCaMP.tif"
    raw_path.parent.mkdir(parents=True)
    stack = np.array([[[1, 2], [3, 4]]], dtype=np.int16)
    tifffile.imwrite(raw_path, stack)

    result = preprocess_anatomy_uint8_stage(
        anat_stack_path=raw_path,
        preproc_dir=preproc_dir,
        polarity="north",
        polarity_source="test",
        config=AnatomyUint8PreprocessingConfig(
            force_recompute_anat_uint8=True,
            target_xy_shape=None,
        ),
    )

    out = tifffile.imread(result["bindings"]["ANAT_8BIT_STACK_PATH"])
    assert out.tolist() == [[[170, 255], [0, 85]]]
    assert result["artifacts"]["orientation_mode"] == "rot180+flipX"
    assert result["artifacts"]["polarity"] == "north"


def test_preprocess_anatomy_uint8_stage_defaults_to_750_xy(tmp_path: Path) -> None:
    preproc_dir = tmp_path / "preproc"
    raw_path = preproc_dir / "2p_anatomy" / "fish_anatomy_2P_GCaMP.tif"
    raw_path.parent.mkdir(parents=True)
    stack = np.array([[[1, 2], [3, 4]]], dtype=np.int16)
    tifffile.imwrite(raw_path, stack)

    result = preprocess_anatomy_uint8_stage(
        anat_stack_path=raw_path,
        preproc_dir=preproc_dir,
        config=AnatomyUint8PreprocessingConfig(force_recompute_anat_uint8=True),
    )

    out = tifffile.imread(result["bindings"]["ANAT_8BIT_STACK_PATH"])
    assert out.dtype == np.uint8
    assert out.shape == (1, 750, 750)
    assert result["artifacts"]["target_xy_shape"] == (750, 750)


def test_preprocess_anatomy_uint8_stage_rebuilds_unversioned_cache(tmp_path: Path) -> None:
    preproc_dir = tmp_path / "preproc"
    raw_path = preproc_dir / "2p_anatomy" / "fish_anatomy_2P_GCaMP.tif"
    raw_path.parent.mkdir(parents=True)
    stack = np.array([[[1, 2], [3, 4]]], dtype=np.int16)
    tifffile.imwrite(raw_path, stack)
    cached_path = preproc_dir / "2p_anatomy" / "fish_anatomy_2P_GCaMP_uint8.tif"
    tifffile.imwrite(cached_path, np.zeros((1, 2, 2), dtype=np.uint8))

    result = preprocess_anatomy_uint8_stage(
        anat_stack_path=raw_path,
        preproc_dir=preproc_dir,
        config=AnatomyUint8PreprocessingConfig(
            apply_func_orientation=False,
            target_xy_shape=None,
        ),
    )

    out = tifffile.imread(result["bindings"]["ANAT_8BIT_STACK_PATH"])
    assert out.tolist() == [[[0, 85], [170, 255]]]
    assert result["artifacts"]["used_cached_uint8"] is False


def test_preprocess_anatomy_uint8_stage_reuses_uint8_input_without_chaining_or_reorienting(tmp_path: Path) -> None:
    preproc_dir = tmp_path / "preproc"
    raw_path = preproc_dir / "2p_anatomy" / "fish_anatomy_2P_GCaMP.tif"
    raw_path.parent.mkdir(parents=True)
    stack = np.array([[[1, 2], [3, 4]]], dtype=np.int16)
    tifffile.imwrite(raw_path, stack)

    first_result = preprocess_anatomy_uint8_stage(
        anat_stack_path=raw_path,
        anat_stack_path_orig=raw_path,
        preproc_dir=preproc_dir,
        polarity="north",
        polarity_source="test",
        config=AnatomyUint8PreprocessingConfig(
            force_recompute_anat_uint8=True,
            target_xy_shape=None,
        ),
    )
    out_path = first_result["bindings"]["ANAT_8BIT_STACK_PATH"]
    first_out = tifffile.imread(out_path)

    second_result = preprocess_anatomy_uint8_stage(
        anat_stack_path=out_path,
        anat_stack_path_orig=out_path,
        preproc_dir=preproc_dir,
        polarity="north",
        polarity_source="test",
        config=AnatomyUint8PreprocessingConfig(target_xy_shape=None),
    )

    assert second_result["bindings"]["ANAT_8BIT_STACK_PATH"] == out_path
    assert second_result["bindings"]["ANAT_STACK_PATH"] == out_path
    assert not (out_path.parent / "fish_anatomy_2P_GCaMP_uint8_uint8.tif").exists()
    np.testing.assert_array_equal(tifffile.imread(out_path), first_out)
    assert second_result["artifacts"]["used_cached_uint8"] is True


def test_preprocess_anatomy_uint8_stage_reuses_uint8_input_without_metadata(tmp_path: Path) -> None:
    preproc_dir = tmp_path / "preproc"
    out_path = preproc_dir / "2p_anatomy" / "fish_anatomy_2P_GCaMP_uint8.tif"
    out_path.parent.mkdir(parents=True)
    anat_u8 = np.array([[[10, 20], [30, 40]]], dtype=np.uint8)
    tifffile.imwrite(out_path, anat_u8)

    result = preprocess_anatomy_uint8_stage(
        anat_stack_path=out_path,
        anat_stack_path_orig=out_path,
        preproc_dir=preproc_dir,
        polarity="north",
        polarity_source="test",
        config=AnatomyUint8PreprocessingConfig(target_xy_shape=None),
    )

    assert result["bindings"]["ANAT_8BIT_STACK_PATH"] == out_path
    assert result["artifacts"]["used_cached_uint8"] is True
    assert not (out_path.parent / "fish_anatomy_2P_GCaMP_uint8_uint8.tif").exists()
    np.testing.assert_array_equal(tifffile.imread(out_path), anat_u8)


def test_preprocess_anatomy_uint8_stage_force_recomputes_uint8_input_from_metadata_source(tmp_path: Path) -> None:
    preproc_dir = tmp_path / "preproc"
    raw_path = preproc_dir / "2p_anatomy" / "fish_anatomy_2P_GCaMP.tif"
    raw_path.parent.mkdir(parents=True)
    tifffile.imwrite(raw_path, np.array([[[1, 2], [3, 4]]], dtype=np.int16))

    first_result = preprocess_anatomy_uint8_stage(
        anat_stack_path=raw_path,
        anat_stack_path_orig=raw_path,
        preproc_dir=preproc_dir,
        config=AnatomyUint8PreprocessingConfig(
            force_recompute_anat_uint8=True,
            apply_func_orientation=False,
            target_xy_shape=None,
        ),
    )
    out_path = first_result["bindings"]["ANAT_8BIT_STACK_PATH"]
    meta = json.loads((out_path.parent / f"{out_path.name}.json").read_text())
    assert Path(meta["source_path"]) == raw_path

    tifffile.imwrite(raw_path, np.array([[[0, 10], [20, 30]]], dtype=np.int16))
    second_result = preprocess_anatomy_uint8_stage(
        anat_stack_path=out_path,
        anat_stack_path_orig=out_path,
        preproc_dir=preproc_dir,
        config=AnatomyUint8PreprocessingConfig(
            force_recompute_anat_uint8=True,
            apply_func_orientation=False,
            target_xy_shape=None,
        ),
    )

    assert second_result["bindings"]["ANAT_8BIT_STACK_PATH"] == out_path
    assert second_result["artifacts"]["anat_uint8_source_path"] == raw_path
    assert second_result["artifacts"]["used_cached_uint8"] is False
    assert tifffile.imread(out_path).tolist() == [[[0, 85], [170, 255]]]


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


def test_functional_references_from_nonflipped_stack_preserve_legacy_flipx_names(tmp_path: Path) -> None:
    out_raw = tmp_path / "raw"
    out_raw.mkdir()
    source_path = tmp_path / "fish_plane_stack.tif"
    legacy_flip_path = out_raw / "fish_plane_stack_flipX.tif"
    func = np.array(
        [
            [
                [[1, 2], [3, 4]],
                [[5, 6], [7, 8]],
            ],
            [
                [[2, 4], [6, 8]],
                [[10, 12], [14, 16]],
            ],
        ],
        dtype=np.float32,
    )
    tifffile.imwrite(source_path, func)

    refs_result = build_functional_references_stage(
        flipped_list=[legacy_flip_path],
        func_nonflipped_list=[source_path],
        out_raw=out_raw,
        vox_func_by_path={str(source_path): {"X": 1.0, "Y": 1.0, "Z": 1.0}},
        polarity="south",
        config=FunctionalReferenceConfig(
            use_top_corr_refs=False,
            reuse_saved_refs=False,
        ),
    )

    plane_refs = refs_result["plane_refs"]
    assert len(plane_refs) == 2
    assert plane_refs[0]["label"] == "fish_plane_stack_flipX_plane0"
    assert (out_raw / "fish_plane_stack_flipX_plane0_raw.tif").exists()
    np.testing.assert_array_equal(plane_refs[0]["ref2d_raw"], func[:, 0, :, :].mean(axis=0)[:, ::-1])
    np.testing.assert_array_equal(plane_refs[1]["ref2d_raw"], func[:, 1, :, :].mean(axis=0)[:, ::-1])
    assert not legacy_flip_path.exists()
