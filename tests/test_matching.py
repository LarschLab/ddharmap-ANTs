from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd
import tifffile

from codeants_2pf_hcr.matching import (
    FunctionalAnatomyDebugConfig,
    build_hcr_mask_fate_df,
    build_functional_anatomy_debug_stage,
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
