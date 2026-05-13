from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
import tifffile

from codeants_2pf_hcr import MultiFishAnatomySegmentationConfig, run_multifish_anatomy_segmentation_stage


def _write_raw_orientation_metadata(fish_dir: Path, orientation: str) -> None:
    metadata_dir = fish_dir / "01_raw" / "2p" / "metadata"
    metadata_dir.mkdir(parents=True)
    (metadata_dir / f"{fish_dir.name}_metadata.csv").write_text(
        f"parameter,value\nfish_orientation,{orientation}\n",
        encoding="utf-8",
    )


def test_run_multifish_anatomy_segmentation_reuses_existing_masks(tmp_path: Path) -> None:
    fish_dir = tmp_path / "L758_f02"
    anat_dir = fish_dir / "02_reg" / "00_preprocessing" / "2p_anatomy"
    mask_dir = fish_dir / "03_analysis" / "structural" / "cp_masks"
    model_path = tmp_path / "anat_model"
    anat_dir.mkdir(parents=True)
    mask_dir.mkdir(parents=True)
    model_path.touch()
    _write_raw_orientation_metadata(fish_dir, "bottom-left")

    anat_path = anat_dir / "L758_f02_anatomy_2P_GCaMP_uint8.tif"
    mask_path = mask_dir / "L758_f02_anatomy_2P_GCaMP_uint8_8bit_cp_masks.tif"
    tifffile.imwrite(str(anat_path), np.zeros((2, 5, 5), dtype=np.uint8))
    mask = np.zeros((2, 5, 5), dtype=np.uint16)
    mask[0, 1:3, 1:3] = 1
    mask[1, 2:4, 2:4] = 2
    tifffile.imwrite(str(mask_path), mask)

    result = run_multifish_anatomy_segmentation_stage(
        MultiFishAnatomySegmentationConfig(
            fish_ids_csv="L758_f02",
            data_mode="local",
            local_root_override=tmp_path,
            anat_cp_model_path_override=model_path,
            cohort_outdir_override=tmp_path / "cohort_outputs" / "multiFish",
        )
    )

    summary = result["bindings"]["MULTIFISH_ANATOMY_SEGMENTATION_DF"]
    assert summary[["fish_id", "status"]].apply(tuple, axis=1).tolist() == [("L758_f02", "cached")]
    assert int(summary.iloc[0]["n_anatomy_labels"]) == 2
    saved = pd.read_csv(tmp_path / "cohort_outputs" / "multiFish" / "multifish_anatomy_segmentation_summary.csv")
    assert saved.iloc[0]["anat_labels_path"] == str(mask_path)


def test_run_multifish_anatomy_segmentation_records_missing_source(tmp_path: Path) -> None:
    model_path = tmp_path / "anat_model"
    model_path.touch()
    fish_dir = tmp_path / "missing_fish"
    (fish_dir / "03_analysis").mkdir(parents=True)
    _write_raw_orientation_metadata(fish_dir, "top-right")

    result = run_multifish_anatomy_segmentation_stage(
        MultiFishAnatomySegmentationConfig(
            fish_ids_csv="missing_fish",
            data_mode="local",
            local_root_override=tmp_path,
            anat_cp_model_path_override=model_path,
            cohort_outdir_override=tmp_path / "out",
        )
    )

    summary = result["bindings"]["MULTIFISH_ANATOMY_SEGMENTATION_DF"]
    assert summary[["fish_id", "status", "notes"]].apply(tuple, axis=1).tolist() == [
        ("missing_fish", "skip", "anatomy source missing")
    ]


def test_run_multifish_anatomy_segmentation_preprocesses_raw_anatomy_to_750_xy(tmp_path: Path) -> None:
    fish_id = "L758_f03"
    fish_dir = tmp_path / fish_id
    raw_anat_dir = fish_dir / "01_raw" / "2p" / "anatomy"
    analysis_dir = fish_dir / "03_analysis"
    model_path = tmp_path / "anat_model"
    raw_anat_dir.mkdir(parents=True)
    analysis_dir.mkdir(parents=True)
    model_path.touch()
    _write_raw_orientation_metadata(fish_dir, "top-right")
    raw_anat_path = raw_anat_dir / f"{fish_id}_anatomy_00001.tif"
    tifffile.imwrite(str(raw_anat_path), np.arange(2 * 4 * 6, dtype=np.int16).reshape(2, 4, 6))

    captured: dict[str, object] = {}

    def _fake_run_anatomy_cellpose_stage(**kwargs):
        src = Path(kwargs["anat_seg_source_path"])
        mask_path = Path(kwargs["analysis_dir"]) / "structural" / "cp_masks" / "fake_cp_masks.tif"
        mask_path.parent.mkdir(parents=True, exist_ok=True)
        tifffile.imwrite(str(mask_path), np.zeros((2, 750, 750), dtype=np.uint16))
        captured["anat_seg_source_path"] = src
        captured["shape"] = tuple(int(v) for v in tifffile.imread(str(src)).shape)
        return {
            "status": "cached",
            "bindings": {
                "ANAT_LABELS_PATH": mask_path,
            },
            "log_lines": [],
        }

    with patch("codeants_2pf_hcr.multifish.run_anatomy_cellpose_stage", side_effect=_fake_run_anatomy_cellpose_stage):
        result = run_multifish_anatomy_segmentation_stage(
            MultiFishAnatomySegmentationConfig(
                fish_ids_csv=fish_id,
                data_mode="local",
                local_root_override=tmp_path,
                anat_cp_model_path_override=model_path,
                cohort_outdir_override=tmp_path / "out",
            )
        )

    summary = result["bindings"]["MULTIFISH_ANATOMY_SEGMENTATION_DF"]
    assert summary.iloc[0]["status"] == "cached"
    assert captured["shape"] == (2, 750, 750)
    assert str(captured["anat_seg_source_path"]).endswith("_uint8.tif")


def test_run_multifish_anatomy_segmentation_raises_when_orientation_missing(tmp_path: Path) -> None:
    fish_id = "L765_f01"
    fish_dir = tmp_path / fish_id
    anat_dir = fish_dir / "02_reg" / "00_preprocessing" / "2p_anatomy"
    model_path = tmp_path / "anat_model"
    anat_dir.mkdir(parents=True)
    (fish_dir / "03_analysis").mkdir(parents=True)
    model_path.touch()
    tifffile.imwrite(str(anat_dir / f"{fish_id}_anatomy_2P_GCaMP.tif"), np.zeros((2, 5, 5), dtype=np.uint8))

    from codeants_2pf_hcr.context import OrientationResolutionError

    with pytest.raises(OrientationResolutionError):
        run_multifish_anatomy_segmentation_stage(
            MultiFishAnatomySegmentationConfig(
                fish_ids_csv=fish_id,
                data_mode="local",
                local_root_override=tmp_path,
                anat_cp_model_path_override=model_path,
                cohort_outdir_override=tmp_path / "out",
            )
        )
