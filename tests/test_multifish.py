from pathlib import Path

import numpy as np
import pandas as pd
import tifffile

from codeants_2pf_hcr import MultiFishAnatomySegmentationConfig, run_multifish_anatomy_segmentation_stage


def test_run_multifish_anatomy_segmentation_reuses_existing_masks(tmp_path: Path) -> None:
    fish_dir = tmp_path / "L758_f02"
    anat_dir = fish_dir / "02_reg" / "00_preprocessing" / "2p_anatomy"
    mask_dir = fish_dir / "03_analysis" / "structural" / "cp_masks"
    model_path = tmp_path / "anat_model"
    anat_dir.mkdir(parents=True)
    mask_dir.mkdir(parents=True)
    model_path.touch()

    anat_path = anat_dir / "L758_f02_anatomy_2P_GCaMP.tif"
    mask_path = mask_dir / "L758_f02_anatomy_2P_GCaMP_8bit_cp_masks.tif"
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
