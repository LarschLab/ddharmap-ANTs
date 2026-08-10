import json
from pathlib import Path

import numpy as np
import tifffile

from codeants_2pf_hcr.context import AnatomyUint8PreprocessingConfig, preprocess_anatomy_uint8_stage
from codeants_2pf_hcr.spatial import FunctionalReferenceConfig, build_functional_references_stage
from codeants_2pf_hcr.spatial_contract import SpatialFrameError, load_spatial_manifest


def _manifest(fish: Path, functional_path: Path, anatomy_path: Path) -> Path:
    path = fish / "02_reg" / "00_preprocessing" / "spatial_preprocessing_manifest.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "stage": "canonical_spatial_preprocessing",
        "status": "complete",
        "polarity": {"value": "south"},
        "coordinate_frames": {
            "canonical_functional_xy": "codeants_2p_canonical_xy_v1",
            "canonical_anatomy_xy": "codeants_2p_canonical_xy_v1",
            "canonical_anatomy_z": "codeants_confocal_registration_z_v1",
        },
        "functional_planes": [{"output_path": str(functional_path)}],
        "anatomy": {"output_path": str(anatomy_path)},
    }))
    return path


def test_canonical_functional_reference_is_not_flipped_again(tmp_path: Path) -> None:
    fish = tmp_path / "L000_f00"
    source = fish / "02_reg" / "00_preprocessing" / "2p_functional" / "01_individualPlanes" / "L000_f00_plane0.tif"
    source.parent.mkdir(parents=True)
    movie = np.asarray([[[1, 2], [3, 4]], [[5, 6], [7, 8]]], dtype=np.uint16)
    tifffile.imwrite(source, movie, photometric="minisblack")
    anatomy = fish / "02_reg" / "00_preprocessing" / "2p_anatomy" / "L000_f00_anatomy_2P_GCaMP.nrrd"
    _manifest(fish, source, anatomy)
    result = build_functional_references_stage(
        flipped_list=[],
        func_nonflipped_list=[source],
        out_raw=tmp_path / "refs",
        polarity="south",
        config=FunctionalReferenceConfig(use_top_corr_refs=False, reuse_saved_refs=False),
    )
    np.testing.assert_array_equal(result["ref2d_raw"], movie.mean(axis=0))
    assert result["bindings"]["FUNCTIONAL_INPUT_XY_FRAME"] == "codeants_2p_canonical_xy_v1"


def test_canonical_anatomy_is_passed_through_without_rewrite(tmp_path: Path) -> None:
    import SimpleITK as sitk

    fish = tmp_path / "L000_f00"
    functional = fish / "02_reg" / "00_preprocessing" / "2p_functional" / "01_individualPlanes" / "L000_f00_plane0.tif"
    anatomy = fish / "02_reg" / "00_preprocessing" / "2p_anatomy" / "L000_f00_anatomy_2P_GCaMP.nrrd"
    anatomy.parent.mkdir(parents=True)
    array = np.arange(2 * 3 * 4, dtype=np.uint8).reshape(2, 3, 4)
    sitk.WriteImage(sitk.GetImageFromArray(array), str(anatomy), useCompression=False)
    _manifest(fish, functional, anatomy)
    before = anatomy.read_bytes()
    result = preprocess_anatomy_uint8_stage(
        anat_stack_path=anatomy,
        preproc_dir=fish / "02_reg" / "00_preprocessing",
        polarity="south",
        config=AnatomyUint8PreprocessingConfig(force_recompute_anat_uint8=True),
    )
    assert result["bindings"]["ANAT_STACK_PATH"] == anatomy
    assert anatomy.read_bytes() == before
    assert result["artifacts"]["orientation_mode"] == "none"
    assert result["artifacts"]["flip_z_for_registration"] is False


def test_unknown_or_contradictory_manifest_frame_fails_closed(tmp_path: Path) -> None:
    fish = tmp_path / "L000_f00"
    path = _manifest(fish, tmp_path / "functional.tif", tmp_path / "anatomy.nrrd")
    payload = json.loads(path.read_text())
    payload["coordinate_frames"]["canonical_anatomy_xy"] = "unknown"
    path.write_text(json.dumps(payload))
    try:
        load_spatial_manifest(fish, required=True)
    except SpatialFrameError:
        pass
    else:
        raise AssertionError("Contradictory coordinate frames must fail closed")
