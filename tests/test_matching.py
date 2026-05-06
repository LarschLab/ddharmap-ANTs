from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd
from skimage.transform import SimilarityTransform
import tifffile

from codeants_2pf_hcr.matching import (
    FunctionalAnatomyDebugConfig,
    build_hcr_mask_fate_df,
    build_functional_anatomy_debug_stage,
    resample_image,
    summarize_functional_anatomy_geometry_metrics,
    transform_points_between_spaces,
)


def test_build_functional_anatomy_debug_stage_returns_debug_df_bindings() -> None:
    with TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        anat_labels_path = tmp_path / "anat_labels.tif"
        tifffile.imwrite(anat_labels_path, np.asarray([[[0, 1], [0, 1]]], dtype=np.uint16))

        plane_refs = [{"best_z": 0, "label": "plane0"}]

        def _load_func_labels_for_plane(_plane_idx: int):
            return np.asarray([[0, 1], [0, 1]], dtype=np.uint16), "func.tif", None

        result = build_functional_anatomy_debug_stage(
            plane_refs=plane_refs,
            anat_labels_path=anat_labels_path,
            vox_anat={"X": 1.0, "Y": 1.0},
            load_func_labels_for_plane_func=_load_func_labels_for_plane,
            config=FunctionalAnatomyDebugConfig(debug_enabled=True),
        )

        assert result["status"] == "ok"
        assert "df_f2a_debug" in result["bindings"]
        assert isinstance(result["debug_df"], pd.DataFrame)
        assert not result["debug_df"].empty


def test_resample_image_uses_plane_transform_for_intensity_exports() -> None:
    image = np.zeros((3, 3), dtype=np.float32)
    image[1, 1] = 5.0

    out = resample_image(image, SimilarityTransform(translation=(2, 1)), output_shape=(6, 6), order=1)

    assert out.shape == (6, 6)
    assert np.isclose(float(out[2, 3]), 5.0)


def test_transform_points_between_spaces_uses_skimage_transform_directions() -> None:
    tform = SimilarityTransform(translation=(2, 1))

    xf, yf = transform_points_between_spaces([1.0], [2.0], tform, direction="moving_to_fixed")
    xm, ym = transform_points_between_spaces(xf, yf, tform, direction="fixed_to_moving")

    assert np.allclose(xf, [3.0])
    assert np.allclose(yf, [3.0])
    assert np.allclose(xm, [1.0])
    assert np.allclose(ym, [2.0])


def test_transform_points_between_spaces_uses_ants_point_inverse_flags(monkeypatch) -> None:
    calls = []

    class FakeAnts:
        @staticmethod
        def apply_transforms_to_points(dim, points, transformlist, whichtoinvert=None):
            calls.append(
                {
                    "dim": dim,
                    "points": points.copy(),
                    "transformlist": list(transformlist),
                    "whichtoinvert": list(whichtoinvert),
                }
            )
            return pd.DataFrame({"x": points["x"].astype(float) + 10.0, "y": points["y"].astype(float) + 20.0})

    import sys

    monkeypatch.setitem(sys.modules, "ants", FakeAnts)
    tform = {"type": "ants_transformlist", "transformlist": ["/tmp/example.mat"]}

    x, y = transform_points_between_spaces([1.0], [2.0], tform, direction="fixed_to_moving")

    assert np.allclose(x, [11.0])
    assert np.allclose(y, [22.0])
    assert len(calls) == 1
    assert calls[0]["dim"] == 2
    assert calls[0]["transformlist"] == ["/tmp/example.mat"]
    assert calls[0]["whichtoinvert"] == [True]
    pd.testing.assert_frame_equal(calls[0]["points"], pd.DataFrame({"x": [1.0], "y": [2.0]}))


def test_summarize_functional_anatomy_geometry_metrics_reports_match_fractions() -> None:
    master_df = pd.DataFrame(
        {
            "fish_id": ["F1", "F1", "F1"],
            "plane_idx": [0, 0, 0],
            "plane": ["plane0", "plane0", "plane0"],
            "has_unique_anat_match": [True, False, True],
            "selected_dist_um": [3.0, np.nan, 5.0],
            "selected_overlap_px": [10, np.nan, 20],
            "n_overlap_candidates_any": [1, 0, 2],
            "n_overlap_candidates_valid": [1, 0, 1],
            "has_identity_assigned": [True, False, False],
        }
    )

    out = summarize_functional_anatomy_geometry_metrics(master_df, method="ncc_xy")

    assert len(out) == 1
    row = out.iloc[0]
    assert row["method"] == "ncc_xy"
    assert int(row["n_rois"]) == 3
    assert int(row["n_unique_anat_match"]) == 2
    assert np.isclose(float(row["unique_match_frac"]), 2 / 3)
    assert float(row["median_selected_dist_um"]) == 4.0
    assert float(row["median_selected_overlap_px"]) == 15.0
    assert int(row["n_identity_assigned"]) == 1


def test_build_hcr_mask_fate_df_classifies_far_iou_and_good_rows() -> None:
    hcr_match_results = [
        {
            "mask_path": "channel0_sst1_1_cp_masks.tif",
            "df_conf": pd.DataFrame({"label": [1, 2, 3]}),
            "matches": pd.DataFrame(
                {
                    "conf_label": [1, 2, 3],
                    "twoP_label": [11, 22, 33],
                    "distance_um": [12.0, 3.0, 2.0],
                    "within_gate": [False, True, True],
                    "pair_type": ["rejected", "1-1", "1-1"],
                    "quality": ["rejected", "iffy", "good"],
                    "iou": [0.0, 0.02, 0.30],
                    "overlap_voxels": [0, 5, 9],
                }
            ),
        }
    ]

    out = build_hcr_mask_fate_df(hcr_match_results)

    assert list(out["conf_label"].astype(int)) == [1, 2, 3]
    assert list(out["gene"].astype(str)) == ["sst1.1", "sst1.1", "sst1.1"]
    assert list(out["conf_mask_name"].astype(str)) == ["channel0_sst1_1_cp_masks.tif"] * 3

    row_far = out.loc[out["conf_label"].astype(int) == 1].iloc[0]
    assert str(row_far["anat_unmatched_reason"]) == "too far / no overlap anatomy"
    assert bool(row_far["within_gate"]) is False

    row_iou = out.loc[out["conf_label"].astype(int) == 2].iloc[0]
    assert str(row_iou["anat_unmatched_reason"]) == "1-to-1 anatomy relation, IoU below threshold"
    assert bool(row_iou["within_gate"]) is True
    assert str(row_iou["pair_type"]) == "1-1"
    assert str(row_iou["quality"]) == "iffy"
    assert int(row_iou["twoP_label"]) == 22

    row_good = out.loc[out["conf_label"].astype(int) == 3].iloc[0]
    assert pd.isna(row_good["anat_unmatched_reason"])
    assert bool(row_good["within_gate"]) is True
    assert str(row_good["quality"]) == "good"
    assert int(row_good["twoP_label"]) == 33
