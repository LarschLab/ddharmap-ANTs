from pathlib import Path

import json
import numpy as np
import pytest
import tifffile

from codeants_2pf_hcr import (
    AnatomyNormalizationStageConfig,
    AnatomyUint8PreprocessingConfig,
    ExVivoAnatomyPreprocessingConfig,
    FunctionalPlacementConfig,
    FunctionalReferenceConfig,
    ManualAnatomyOrientationConfig,
    RegistrationSearchConfig,
    apply_manual_anatomy_orientation_stage,
    build_functional_references_stage,
    normalize_anatomy_stack_stage,
    preprocess_ex_vivo_anatomy_stage,
    preprocess_anatomy_uint8_stage,
    run_ncc_placement_stage,
    run_registration_search_stage,
)


def _read_nrrd_zyx(path: Path) -> np.ndarray:
    nrrd = pytest.importorskip("nrrd")
    data, _header = nrrd.read(str(path))
    arr = np.asarray(data)
    if arr.ndim == 3:
        return np.transpose(arr, (2, 1, 0))
    if arr.ndim == 2:
        return np.transpose(arr, (1, 0))
    return arr


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
    out = _read_nrrd_zyx(out_path)
    assert out.dtype == np.uint8
    assert out.tolist() == [[[0, 64], [128, 255]]]
    assert out_path == preproc_dir / "2p_anatomy" / "fish_anatomy_2P_GCaMP.nrrd"
    assert not (preproc_dir / "2p_anatomy" / "fish_anatomy_2P_GCaMP_uint8.tif").exists()
    assert result["bindings"]["ANAT_STACK_PATH"] == out_path
    assert result["bindings"]["ANAT_STACK_PATH_ORIG"] == raw_path
    assert result["bindings"]["ANAT_STACK_PATH_16BIT"] == raw_path
    assert result["artifacts"]["negative_offset"] == 10


def test_preprocess_anatomy_uint8_stage_writes_canonical_registration_nrrd(tmp_path: Path) -> None:
    nrrd = pytest.importorskip("nrrd")
    preproc_dir = tmp_path / "preproc"
    raw_path = preproc_dir / "2p_anatomy" / "L765_f02_anatomy_00001.tif"
    raw_path.parent.mkdir(parents=True)
    stack = np.array([[[0, 10], [20, 30]]], dtype=np.int16)
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

    nrrd_path = preproc_dir / "2p_anatomy" / "L765_f02_anatomy_2P_GCaMP.nrrd"
    assert result["bindings"]["ANAT_REG_NRRD_PATH"] == nrrd_path
    assert result["artifacts"]["registration_nrrd_path"] == nrrd_path
    assert nrrd_path.exists()
    data, header = nrrd.read(str(nrrd_path))
    assert data.shape == (2, 2, 1)
    assert header["encoding"] == "raw"
    np.testing.assert_array_equal(np.transpose(data, (2, 1, 0)), _read_nrrd_zyx(result["bindings"]["ANAT_8BIT_STACK_PATH"]))


def test_preprocess_anatomy_uint8_stage_flips_z_for_registration(tmp_path: Path) -> None:
    preproc_dir = tmp_path / "preproc"
    raw_path = preproc_dir / "2p_anatomy" / "fish_anatomy_2P_GCaMP.tif"
    raw_path.parent.mkdir(parents=True)
    stack = np.arange(8, dtype=np.int16).reshape(2, 2, 2)
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

    out = _read_nrrd_zyx(result["bindings"]["ANAT_8BIT_STACK_PATH"])
    expected_scaled = np.rint(stack.astype(np.float64) * (255.0 / 7.0)).astype(np.uint8)
    np.testing.assert_array_equal(out, expected_scaled[::-1])
    assert result["artifacts"]["flip_z_for_registration"] is True


def test_preprocess_anatomy_uint8_stage_writes_spatial_nrrd_header(tmp_path: Path) -> None:
    nrrd = pytest.importorskip("nrrd")
    fish_dir = tmp_path / "L765_f02"
    preproc_dir = fish_dir / "02_reg" / "00_preprocessing"
    raw_path = fish_dir / "01_raw" / "2p" / "anatomy" / "L765_f02_anatomy_00001.tif"
    metadata_dir = fish_dir / "01_raw" / "2p" / "metadata"
    raw_path.parent.mkdir(parents=True)
    metadata_dir.mkdir(parents=True)
    metadata_dir.joinpath("fish_metadata.csv").write_text("parameter,value\nstep_size_um_anatomy,3\n")
    stack = np.array([[[0, 10, 20, 30], [40, 50, 60, 70]]], dtype=np.int16)
    tifffile.imwrite(raw_path, stack, resolution=(1000.0, 2000.0), resolutionunit="CENTIMETER")

    result = preprocess_anatomy_uint8_stage(
        anat_stack_path=raw_path,
        anat_stack_path_orig=raw_path,
        preproc_dir=preproc_dir,
        config=AnatomyUint8PreprocessingConfig(
            force_recompute_anat_uint8=True,
            apply_func_orientation=False,
            target_xy_shape=(4, 8),
        ),
    )

    data, header = nrrd.read(str(result["bindings"]["ANAT_REG_NRRD_PATH"]))
    assert data.shape == (8, 4, 1)
    np.testing.assert_allclose(
        header["space directions"],
        np.array([[5.0, 0.0, 0.0], [0.0, 2.5, 0.0], [0.0, 0.0, 3.0]]),
    )
    assert list(header["space units"]) == ["microns", "microns", "microns"]
    assert header["source_path"] == str(raw_path)
    assert header["source_shape"] == "1x2x4"


def test_preprocess_anatomy_uint8_stage_uses_imagej_info_resolution_fallback(tmp_path: Path) -> None:
    nrrd = pytest.importorskip("nrrd")
    fish_dir = tmp_path / "L758_f04"
    preproc_dir = fish_dir / "02_reg" / "00_preprocessing"
    raw_path = fish_dir / "01_raw" / "2p" / "anatomy" / "L758_f04_anatomy_00001.tif"
    metadata_dir = fish_dir / "01_raw" / "2p" / "metadata"
    raw_path.parent.mkdir(parents=True)
    metadata_dir.mkdir(parents=True)
    metadata_dir.joinpath("fish_metadata.csv").write_text("parameter,value\nstep_size_um_anatomy,2\n")
    imagej_info = "ResolutionUnit = Centimeter\nXResolution = 1000\nYResolution = 2000\n"
    tifffile.imwrite(
        raw_path,
        np.arange(16, dtype=np.uint16).reshape(2, 2, 4),
        imagej=True,
        metadata={"axes": "ZYX", "Info": imagej_info},
    )

    result = preprocess_anatomy_uint8_stage(
        anat_stack_path=raw_path,
        anat_stack_path_orig=raw_path,
        preproc_dir=preproc_dir,
        config=AnatomyUint8PreprocessingConfig(
            force_recompute_anat_uint8=True,
            apply_func_orientation=False,
            target_xy_shape=(4, 8),
        ),
    )

    _data, header = nrrd.read(str(result["bindings"]["ANAT_REG_NRRD_PATH"]))
    np.testing.assert_allclose(
        header["space directions"],
        np.array([[5.0, 0.0, 0.0], [0.0, 2.5, 0.0], [0.0, 0.0, 2.0]]),
    )


def test_preprocess_anatomy_uint8_stage_preserves_source_nrrd_geometry(tmp_path: Path) -> None:
    nrrd = pytest.importorskip("nrrd")
    preproc_dir = tmp_path / "preproc"
    source_path = preproc_dir / "2p_anatomy" / "L765_f02_anatomy_source.nrrd"
    source_path.parent.mkdir(parents=True)
    source_xyz = np.arange(8, dtype=np.int16).reshape(4, 2, 1)
    nrrd.write(
        str(source_path),
        source_xyz,
        header={
            "space dimension": 3,
            "space directions": np.array([[10.0, 0.0, 0.0], [0.0, 5.0, 0.0], [0.0, 0.0, 3.0]]),
            "space units": ["um", "um", "um"],
            "space origin": np.array([1.0, 2.0, 3.0]),
        },
    )

    result = preprocess_anatomy_uint8_stage(
        anat_stack_path=source_path,
        anat_stack_path_orig=source_path,
        preproc_dir=preproc_dir,
        config=AnatomyUint8PreprocessingConfig(
            force_recompute_anat_uint8=True,
            apply_func_orientation=False,
            target_xy_shape=(4, 8),
        ),
    )

    _data, header = nrrd.read(str(result["bindings"]["ANAT_REG_NRRD_PATH"]))
    np.testing.assert_allclose(
        header["space directions"],
        np.array([[5.0, 0.0, 0.0], [0.0, 2.5, 0.0], [0.0, 0.0, 3.0]]),
    )
    np.testing.assert_allclose(header["space origin"], np.array([1.0, 2.0, 3.0]))
    assert header["source_path"] == str(source_path)


def test_preprocess_anatomy_uint8_stage_backfills_registration_nrrd_for_cached_uint8(tmp_path: Path) -> None:
    preproc_dir = tmp_path / "preproc"
    out_path = preproc_dir / "2p_anatomy" / "L765_f02_anatomy_00001_uint8.tif"
    out_path.parent.mkdir(parents=True)
    tifffile.imwrite(out_path, np.array([[[10, 20], [30, 40]]], dtype=np.uint8))

    result = preprocess_anatomy_uint8_stage(
        anat_stack_path=out_path,
        anat_stack_path_orig=out_path,
        preproc_dir=preproc_dir,
        config=AnatomyUint8PreprocessingConfig(target_xy_shape=None),
    )

    nrrd_path = preproc_dir / "2p_anatomy" / "L765_f02_anatomy_2P_GCaMP.nrrd"
    assert result["bindings"]["ANAT_8BIT_STACK_PATH"] == nrrd_path
    assert result["bindings"]["ANAT_REG_NRRD_PATH"] == nrrd_path
    assert nrrd_path.exists()


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

    out = _read_nrrd_zyx(result["bindings"]["ANAT_8BIT_STACK_PATH"])
    assert out.dtype == np.uint8
    assert int(out.min()) == 0
    assert int(out.max()) == 0
    assert result["artifacts"]["negative_offset"] == 4


def test_preprocess_anatomy_uint8_stage_applies_functional_orientation_by_default(tmp_path: Path) -> None:
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

    out = _read_nrrd_zyx(result["bindings"]["ANAT_8BIT_STACK_PATH"])
    assert out.tolist() == [[[170, 255], [0, 85]]]
    assert result["artifacts"]["orientation_mode"] == "flipY"
    assert result["artifacts"]["apply_func_orientation"] is True
    assert result["artifacts"]["polarity"] == "north"


def test_preprocess_anatomy_uint8_stage_can_preserve_anatomy_xy_when_explicitly_requested(tmp_path: Path) -> None:
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
            apply_func_orientation=False,
            target_xy_shape=None,
        ),
    )

    out = _read_nrrd_zyx(result["bindings"]["ANAT_8BIT_STACK_PATH"])
    assert out.tolist() == [[[0, 85], [170, 255]]]
    assert result["artifacts"]["orientation_mode"] == "none"
    assert result["artifacts"]["apply_func_orientation"] is False
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
        polarity="south",
        config=AnatomyUint8PreprocessingConfig(force_recompute_anat_uint8=True),
    )

    out = _read_nrrd_zyx(result["bindings"]["ANAT_8BIT_STACK_PATH"])
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

    out = _read_nrrd_zyx(result["bindings"]["ANAT_8BIT_STACK_PATH"])
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
    first_out = _read_nrrd_zyx(out_path)

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
    np.testing.assert_array_equal(_read_nrrd_zyx(out_path), first_out)
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

    nrrd_path = preproc_dir / "2p_anatomy" / "fish_anatomy_2P_GCaMP.nrrd"
    assert result["bindings"]["ANAT_8BIT_STACK_PATH"] == nrrd_path
    assert result["artifacts"]["used_cached_uint8"] is False
    assert not (out_path.parent / "fish_anatomy_2P_GCaMP_uint8_uint8.tif").exists()
    np.testing.assert_array_equal(_read_nrrd_zyx(nrrd_path), anat_u8)


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
    assert _read_nrrd_zyx(out_path).tolist() == [[[0, 85], [170, 255]]]


def test_preprocess_ex_vivo_anatomy_stage_writes_isolated_x_and_z_flipped_outputs(tmp_path: Path) -> None:
    nrrd = pytest.importorskip("nrrd")
    fish_id = "L758_f02"
    fish_dir = tmp_path / fish_id
    preproc_dir = fish_dir / "02_reg" / "00_preprocessing"
    raw_path = fish_dir / "01_raw" / "2p" / "anatomy" / f"{fish_id}_exvivo_920nm.tif"
    raw_path.parent.mkdir(parents=True)
    stack = np.arange(8, dtype=np.int16).reshape(2, 2, 2)
    tifffile.imwrite(raw_path, stack)

    result = preprocess_ex_vivo_anatomy_stage(
        fish_id=fish_id,
        ex_vivo_stack_path=raw_path,
        preproc_dir=preproc_dir,
        config=ExVivoAnatomyPreprocessingConfig(
            force_recompute=True,
            target_xy_shape=None,
        ),
    )

    out_path = result["bindings"]["EX_VIVO_ANAT_PRE_ROTATION_NRRD"]
    assert out_path == preproc_dir / "2p_anatomy" / "ex_vivo" / f"{fish_id}_exvivo_anatomy_2P_GCaMP_uint8.nrrd"
    out, _header = nrrd.read(str(out_path), index_order="C")
    expected_scaled = np.rint(stack.astype(np.float64) * (255.0 / 7.0)).astype(np.uint8)
    np.testing.assert_array_equal(out, expected_scaled[::-1, :, ::-1])
    meta = json.loads(result["bindings"]["EX_VIVO_ANAT_PRE_ROTATION_METADATA"].read_text())
    assert meta["stage"] == "preprocess_ex_vivo_anatomy_stage"
    assert meta["flip_x"] is True
    assert meta["flip_z_for_registration"] is True
    assert meta["source_path"] == str(raw_path)
    assert not out_path.with_suffix(".tif").exists()


def test_apply_manual_anatomy_orientation_stage_rotates_and_records_operation(tmp_path: Path) -> None:
    nrrd = pytest.importorskip("nrrd")
    input_path = tmp_path / "L765_f04_exvivo_anatomy_2P_GCaMP_uint8.nrrd"
    arr = np.arange(6, dtype=np.uint8).reshape(1, 2, 3)
    nrrd.write(str(input_path), np.transpose(arr, (2, 1, 0)), header={"encoding": "raw"})

    result = apply_manual_anatomy_orientation_stage(
        input_path=input_path,
        config=ManualAnatomyOrientationConfig(
            force_recompute=True,
            rot90_k=1,
            flip_x=True,
        ),
    )

    out_path = result["bindings"]["MANUAL_ORIENTED_ANAT_NRRD"]
    assert out_path == input_path.with_name("L765_f04_exvivo_anatomy_2P_GCaMP_uint8_manual_oriented.nrrd")
    out, _header = nrrd.read(str(out_path), index_order="C")
    expected = np.flip(np.rot90(arr, k=1, axes=(-2, -1)), axis=-1)
    np.testing.assert_array_equal(out, expected)
    assert not out_path.with_suffix(".tif").exists()
    meta = json.loads(result["bindings"]["MANUAL_ORIENTED_ANAT_METADATA"].read_text())
    assert meta["stage"] == "apply_manual_anatomy_orientation_stage"
    assert meta["manual_orientation"] == {
        "rotation_degrees": 0.0,
        "applied_rotation_degrees": -0.0,
        "interpolation": "linear",
        "expand_canvas": True,
        "crop_center_yx": None,
        "crop_size_px": None,
        "rot90_k": 1,
        "flip_x": True,
        "flip_y": False,
        "flip_z": False,
    }
    assert meta["source_path"] == str(input_path)


def test_apply_manual_anatomy_orientation_stage_uses_brainatlas_rotation_crop_convention(tmp_path: Path) -> None:
    nrrd = pytest.importorskip("nrrd")
    input_path = tmp_path / "L758_f02_exvivo_anatomy_2P_GCaMP_uint8.nrrd"
    arr = np.arange(25, dtype=np.uint8).reshape(1, 5, 5)
    nrrd.write(str(input_path), np.transpose(arr, (2, 1, 0)), header={"encoding": "raw"})

    result = apply_manual_anatomy_orientation_stage(
        input_path=input_path,
        config=ManualAnatomyOrientationConfig(
            force_recompute=True,
            rotation_degrees=15.0,
            crop_center_yx=(2, 2),
            crop_size_px=3,
        ),
    )

    out, _header = nrrd.read(str(result["bindings"]["MANUAL_ORIENTED_ANAT_NRRD"]), index_order="C")
    assert out.shape == (1, 3, 3)
    meta = json.loads(result["bindings"]["MANUAL_ORIENTED_ANAT_METADATA"].read_text())
    assert meta["manual_orientation"]["rotation_degrees"] == 15.0
    assert meta["manual_orientation"]["applied_rotation_degrees"] == -15.0
    assert meta["manual_orientation"]["crop_center_yx"] == [2, 2]
    assert meta["manual_orientation"]["crop_size_px"] == 3


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
