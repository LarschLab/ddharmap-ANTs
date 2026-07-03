from __future__ import annotations

import sys
from types import SimpleNamespace
from pathlib import Path

import numpy as np
import tifffile

from codeants_2pf_hcr.hcr_warp import build_direct_ants_hcr_transform_chain, run_direct_ants_hcr_label_warp


def test_build_direct_ants_hcr_transform_chain_appends_rn_to_rbest(tmp_path: Path) -> None:
    rbest_to_2p = tmp_path / "01_rbest-2p" / "transMatrices"
    rn_to_rbest = tmp_path / "02_rn-rbest" / "transMatrices"
    rbest_to_2p.mkdir(parents=True)
    rn_to_rbest.mkdir(parents=True)
    best_warp = rbest_to_2p / "L000_f00_round2_GCaMP_to_2p_1Warp.nii.gz"
    best_affine = rbest_to_2p / "L000_f00_round2_GCaMP_to_2p_0GenericAffine.mat"
    rn_warp = rn_to_rbest / "L000_f00_round1_GCaMP_to_r2_1Warp.nii.gz"
    rn_affine = rn_to_rbest / "L000_f00_round1_GCaMP_to_r2_0GenericAffine.mat"
    for path in (best_warp, best_affine, rn_warp, rn_affine):
        path.write_text("transform\n")

    chain = build_direct_ants_hcr_transform_chain(
        round_idx=1,
        best_round_idx=2,
        rbest_to_2p_transform_dir=rbest_to_2p,
        rn_to_rbest_transform_dir=rn_to_rbest,
    )

    assert chain == (best_warp, best_affine, rn_warp, rn_affine)


def test_run_direct_ants_hcr_label_warp_transposes_zyx_and_writes_metadata(
    tmp_path: Path,
    monkeypatch,
) -> None:
    calls: dict[str, object] = {}

    class FakeImage:
        def __init__(self, arr):
            self._arr = np.asarray(arr)
            self.shape = self._arr.shape
            self.spacing = (1.0, 2.0, 3.0)
            self.origin = (0.0, 0.0, 0.0)
            self.direction = np.eye(3)

        def set_spacing(self, value):
            self.spacing = tuple(value)

        def set_origin(self, value):
            self.origin = tuple(value)

        def set_direction(self, value):
            self.direction = np.asarray(value)

        def numpy(self):
            return self._arr

    def fake_from_numpy(arr):
        calls["from_numpy_shape"] = tuple(np.asarray(arr).shape)
        return FakeImage(arr)

    def fake_image_read(path):
        calls.setdefault("image_read", []).append(str(path))
        return FakeImage(np.zeros((4, 3, 2), dtype=np.float32))

    def fake_apply_transforms(**kwargs):
        calls["transformlist"] = tuple(kwargs["transformlist"])
        calls["whichtoinvert"] = tuple(kwargs["whichtoinvert"])
        calls["interpolator"] = kwargs["interpolator"]
        return kwargs["moving"]

    fake_ants = SimpleNamespace(
        from_numpy=fake_from_numpy,
        image_read=fake_image_read,
        apply_transforms=fake_apply_transforms,
    )
    monkeypatch.setitem(sys.modules, "ants", fake_ants)

    fish_id = "L000_f00"
    preproc = tmp_path / fish_id / "02_reg" / "00_preprocessing"
    raw_masks = tmp_path / fish_id / "03_analysis" / "confocal" / "raw" / "cp_masks"
    aligned = tmp_path / "aligned"
    rbest_to_2p = tmp_path / fish_id / "02_reg" / "01_rbest-2p" / "transMatrices"
    rn_to_rbest = tmp_path / fish_id / "02_reg" / "02_rn-rbest" / "transMatrices"
    for directory in (preproc / "rbest", raw_masks, rbest_to_2p, rn_to_rbest):
        directory.mkdir(parents=True, exist_ok=True)
    mask_path = raw_masks / f"{fish_id}_round2_channel2_sst1_2_cp_masks.tif"
    labels = np.zeros((2, 3, 4), dtype=np.uint16)
    labels[1, 1, 2] = 7
    tifffile.imwrite(mask_path, labels)
    (preproc / "rbest" / f"{fish_id}_round2_channel2_sst1_2.nrrd").write_text("nrrd\n")
    anatomy = preproc / "2p_anatomy" / f"{fish_id}_anatomy_2P_GCaMP.nrrd"
    anatomy.parent.mkdir(parents=True)
    anatomy.write_text("nrrd\n")
    best_warp = rbest_to_2p / f"{fish_id}_round2_GCaMP_to_2p_1Warp.nii.gz"
    best_affine = rbest_to_2p / f"{fish_id}_round2_GCaMP_to_2p_0GenericAffine.mat"
    best_warp.write_text("warp\n")
    best_affine.write_text("affine\n")

    results = run_direct_ants_hcr_label_warp(
        fish_id=fish_id,
        mask_paths=(mask_path,),
        preproc_dir=preproc,
        anatomy_intensity_path=anatomy,
        output_dir=aligned,
        best_round_idx=2,
        rbest_to_2p_transform_dir=rbest_to_2p,
        rn_to_rbest_transform_dir=rn_to_rbest,
    )

    assert len(results) == 1
    assert calls["from_numpy_shape"] == (4, 3, 2)
    assert calls["transformlist"] == (str(best_warp), str(best_affine))
    assert calls["whichtoinvert"] == (False, False)
    assert calls["interpolator"] == "nearestNeighbor"
    assert Path(results[0].output_label_path).exists()
    assert Path(results[0].output_metadata_path).exists()
    assert np.array_equal(tifffile.imread(results[0].output_label_path), labels)
