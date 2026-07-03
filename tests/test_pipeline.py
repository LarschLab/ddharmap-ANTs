import json
import importlib.util
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from codeants_2pf_hcr.pipeline import (
    GRANULAR_PREPROCESSING_STAGE_NAMES,
    PIPELINE_STAGE_ORDER,
    REQUIRED_CSV_COLUMNS,
    SingleFishPipelineConfig,
    build_single_fish_compare_staged_manifest,
    build_single_fish_compare_legacy_baseline_manifest,
    build_single_fish_downstream_stage_manifest,
    build_single_fish_downstream_stage_manifests,
    build_single_fish_hcr_activity_replay_manifest,
    build_single_fish_score_activity_bpi_recompute_manifest,
    build_single_fish_stage_status,
    build_single_fish_status,
    cellpose_stage_manifest_path,
    compare_single_fish_legacy_baseline,
    compare_single_fish_staged_outputs,
    discover_functional_reference_pairs,
    discover_functional_motion_corrected_stacks,
    discover_ex_vivo_anatomy_stack,
    downstream_stage_names,
    ex_vivo_structural_root,
    functional_to_anatomy_registration_root,
    functional_reference_output_dir,
    hcr_to_anatomy_registration_root,
    load_plane_refs_summary,
    prepared_in_vivo_anatomy_path,
    prepared_ex_vivo_anatomy_path,
    pipeline_contracts,
    resolve_pipeline_paths,
    run_prepare_functional_reference_stacks_stage,
    run_prepare_in_vivo_anatomy_stack_stage,
    run_prepare_ex_vivo_anatomy_stack_stage,
    run_register_functional_to_anatomy_stage,
    run_register_hcr_to_anatomy_stage,
    run_match_roi_to_anatomy_stage,
    roi_to_anatomy_match_root,
    run_segment_ex_vivo_anatomy_cellpose_stage,
    run_segment_hcr_cellpose_stage,
    run_single_fish_assign_hcr_identity_stage,
    run_single_fish_audit_inputs_stage,
    run_single_fish_export_canonical_tables_stage,
    run_single_fish_freeze_legacy_baseline_stage,
    run_single_fish_make_figures_stage,
    run_single_fish_make_qa_report_stage,
    run_single_fish_score_activity_bpi_stage,
    stage_manifest_path,
    write_stage_manifest,
    _overlay_selected_ants_transformlists,
    _plane_refs_from_tforms_csv,
)
from codeants_2pf_hcr.activity import ActivityConfig
from codeants_2pf_hcr.matching import (
    HcrActivityExportConfig,
    attach_identity_to_functional_roi_geometry_df,
    build_hcr_activity_tables,
    finalize_hcr_activity_export_tables,
    gene_from_mask,
    hcr_response_lookup_from_roi_master_df,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def _make_minimal_fish(root: Path, fish_id: str = "L000_f00") -> Path:
    fish_dir = root / fish_id
    metadata_dir = fish_dir / "01_raw" / "2p" / "metadata"
    functional_raw_dir = fish_dir / "01_raw" / "2p" / "functional"
    anatomy_raw_dir = fish_dir / "01_raw" / "2p" / "anatomy"
    anatomy_preproc_dir = fish_dir / "02_reg" / "00_preprocessing" / "2p_anatomy"
    rbest_dir = fish_dir / "02_reg" / "00_preprocessing" / "rbest"
    suite2p_plane_dir = fish_dir / "03_analysis" / "functional" / "suite2P" / "plane0"
    registration_dir = fish_dir / "03_analysis" / "functional" / "registration"
    ncc_compare_dir = fish_dir / "03_analysis" / "functional" / "ncc" / "inplane_registration_comparison"
    cp_masks_dir = fish_dir / "03_analysis" / "confocal" / "raw" / "cp_masks"
    confocal_aligned_dir = fish_dir / "03_analysis" / "confocal" / "aligned"
    plots_dir = fish_dir / "04_plots"
    for directory in (
        metadata_dir,
        functional_raw_dir,
        anatomy_raw_dir,
        anatomy_preproc_dir,
        rbest_dir,
        suite2p_plane_dir,
        registration_dir / "reference_planes",
        ncc_compare_dir,
        cp_masks_dir,
        confocal_aligned_dir,
        plots_dir,
    ):
        directory.mkdir(parents=True)
    (metadata_dir / f"{fish_id}_metadata.csv").write_text("key,value\n")
    (metadata_dir / f"{fish_id}_experiment_log.csv").write_text("time,event\n")
    (metadata_dir / ".DS_Store").write_text("ignored\n")
    (functional_raw_dir / ".DS_Store").write_text("ignored\n")
    (anatomy_preproc_dir / f"{fish_id}_anatomy_2P_GCaMP.nrrd").write_text("NRRD0005\n")
    (rbest_dir / f"{fish_id}_round1_channel2_gene.nrrd").write_text("NRRD0005\n")
    for name in ("F.npy", "Fneu.npy", "iscell.npy", "ops.npy", "stat.npy"):
        (suite2p_plane_dir / f"{fish_id}_plane0_{name}").write_bytes(b"npy")
    required_registration_csvs = (
        "tforms_by_plane.csv",
        "plane_links.csv",
        "anatomy_identity_lookup.csv",
        "functional_roi_activity_identity.csv",
        "functional_roi_activity_identity_summary.csv",
        "functional_roi_activity_identity_by_plane.csv",
        "functional_roi_activity_bpi_cells.csv",
        "functional_roi_activity_bpi_summary.csv",
        "hcr_activity_status.csv",
        "hcr_activity_status_summary.csv",
        "conf_to_func_pairs_raw.csv",
        "conf_to_func_pairs.csv",
        "hcr_func_candidates.csv",
    )
    for name in required_registration_csvs:
        header = REQUIRED_CSV_COLUMNS.get(name, ("col",))
        if name == "anatomy_identity_lookup.csv":
            header = ("anat_label", "identity_label", "identity_gene_count", "identity_genes")
        if name == "conf_to_func_pairs.csv":
            header = tuple(dict.fromkeys((*header, "conf_mask", "conf_label")))
        if name == "hcr_activity_status_summary.csv":
            header = ("gene", "inner_status", "outer_status", "n_labels")
        row_values = {
            "plane_index": "0",
            "plane_idx": "0",
            "plane": "0",
            "func_label": "1",
            "conf_mask": "mask_a",
            "conf_label": "1",
            "gene": "sst1.1",
            "anat_label": "1",
            "has_unique_anat_match": "True",
            "has_identity_assigned": "True",
            "suite2p_is_cell": "True",
            "response_is_active": "True",
            "selected_for_trace_export": "True",
            "is_selected_for_analysis": "True",
            "response_class": "bout-responsive",
            "response_summary_class": "Responsive neurons",
            "bpi_category": "bout-responsive",
            "bpi": "1.0",
            "identity_gene_count": "1",
            "identity_genes": "['sst1.1']",
            "inner_status": "within functional planes",
            "outer_status": "in-plane responsive ROI",
            "n_labels": "1",
        }
        if name == "anatomy_identity_lookup.csv":
            row_values["anat_label"] = "7"
            row_values["identity_label"] = "sst1.1"
        if name == "functional_roi_activity_identity.csv":
            row_values["selected_anat_label"] = "7"
            row_values["anat_label"] = "7"
            row_values["identity_label"] = "sst1.1"
        row = tuple(row_values.get(column, "1") for column in header)
        (registration_dir / name).write_text(",".join(header) + "\n" + ",".join(row) + "\n")
    (registration_dir / "reference_planes" / f"{fish_id}_plane0_ref.tif").write_bytes(b"tif")
    (fish_dir / "03_analysis" / "functional" / "ncc" / "ncc_bestz_by_plane.json").write_text("{}\n")
    (ncc_compare_dir / "inplane_registration_comparison.csv").write_text("col\n")
    (ncc_compare_dir / "inplane_registration_recommendation.csv").write_text("col\n")
    (cp_masks_dir / f"{fish_id}_round1_channel2_gene_cp_masks.tif").write_bytes(b"tif")
    (confocal_aligned_dir / "aligned.csv").write_text("col\n")
    for name in (
        "compound_50j_56i_unified.png",
        "bpi_all_pairs.png",
        "per_gene_stimulus_trace_with_hcr_status_56h.png",
        "single_fish_50l_responsive_identity_donut.png",
        "single_fish_hcr_anatomy_coexpression_summary.png",
    ):
        (plots_dir / name).write_bytes(b"png")
    return fish_dir


def _copy_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_bytes(src.read_bytes())


def _copy_csv_with_crlf(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_bytes(src.read_text().replace("\n", "\r\n").encode())


def _make_minimal_staged_outputs(fish_dir: Path) -> None:
    registration_dir = fish_dir / "03_analysis" / "functional" / "registration"
    plots_dir = fish_dir / "04_plots"
    pipeline_outputs = fish_dir / "03_analysis" / "functional" / "pipeline_outputs"
    assign_dir = pipeline_outputs / "assign-hcr-identity" / "registration"
    for filename in (
        "functional_roi_activity_identity.csv",
        "hcr_activity_status.csv",
        "hcr_activity_status_summary.csv",
        "conf_to_func_pairs_raw.csv",
        "conf_to_func_pairs.csv",
        "hcr_func_candidates.csv",
    ):
        _copy_file(registration_dir / filename, assign_dir / filename)
    (assign_dir / "anatomy_identity_lookup.csv").write_text("gene,anat_label\nmarker,1\n")

    score_dir = pipeline_outputs / "score-activity-bpi" / "registration"
    for filename in (
        "functional_roi_activity_identity.csv",
        "functional_roi_activity_bpi_cells.csv",
        "functional_roi_activity_bpi_summary.csv",
    ):
        _copy_file(registration_dir / filename, score_dir / filename)

    canonical_dir = pipeline_outputs / "export-canonical-tables" / "registration"
    for filename in (
        "functional_roi_activity_identity.csv",
        "functional_roi_activity_bpi_cells.csv",
        "functional_roi_activity_bpi_summary.csv",
        "hcr_activity_status.csv",
        "hcr_activity_status_summary.csv",
        "conf_to_func_pairs.csv",
        "conf_to_func_pairs_raw.csv",
        "hcr_func_candidates.csv",
    ):
        _copy_file(registration_dir / filename, canonical_dir / filename)

    qa_report_dir = pipeline_outputs / "make-qa-report"
    qa_report_dir.mkdir(parents=True)
    (qa_report_dir / "qa_report.md").write_text("# Single-Fish QA Report\n")
    (qa_report_dir / "qa_report_summary.json").write_text("{}\n")

    figure_dir = pipeline_outputs / "make-figures" / "04_plots"
    for filename in (
        "compound_50j_56i_unified.png",
        "bpi_all_pairs.png",
        "per_gene_stimulus_trace_with_hcr_status_56h.png",
        "single_fish_50l_responsive_identity_donut.png",
        "single_fish_hcr_anatomy_coexpression_summary.png",
    ):
        _copy_file(plots_dir / filename, figure_dir / filename)


def _write_roi_identity_csv(path: Path, rows: list[dict[str, str]]) -> None:
    header = REQUIRED_CSV_COLUMNS["functional_roi_activity_identity.csv"]
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [",".join(header)]
    for row in rows:
        lines.append(",".join(str(row.get(column, "")) for column in header))
    path.write_text("\n".join(lines) + "\n")


def _write_recompute_identity_csv(path: Path, rows: list[dict[str, str]]) -> None:
    header = REQUIRED_CSV_COLUMNS["functional_roi_activity_identity.csv"] + ("activity_class", "is_active")
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [",".join(header)]
    for row in rows:
        lines.append(",".join(str(row.get(column, "")) for column in header))
    path.write_text("\n".join(lines) + "\n")


def _roi_identity_row(func_label: str, *, bpi: str = "1.0", identity_label: str = "sst1.1") -> dict[str, str]:
    return {
        "plane_idx": "0",
        "func_label": func_label,
        "selected_anat_label": func_label,
        "has_unique_anat_match": "True",
        "anat_label": func_label,
        "identity_label": identity_label,
        "has_identity_assigned": "True",
        "suite2p_is_cell": "True",
        "response_is_active": "True",
        "response_class": "bout-responsive",
        "response_summary_class": "Responsive neurons",
        "bpi": bpi,
        "bpi_category": "bout-responsive",
        "activity_class": "Active neurons",
        "is_active": "True",
    }


def _write_rgb_png(path: Path, *, size: tuple[int, int] = (12, 10), color: tuple[int, int, int] = (10, 120, 200)) -> None:
    from PIL import Image

    path.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", size, color=color).save(path)


def _write_tiny_ex_vivo_tiff(path: Path) -> None:
    np_spec = importlib.util.find_spec("numpy")
    tif_spec = importlib.util.find_spec("tifffile")
    nrrd_spec = importlib.util.find_spec("nrrd")
    sitk_spec = importlib.util.find_spec("SimpleITK")
    if np_spec is None or tif_spec is None or (nrrd_spec is None and sitk_spec is None):
        pytest.skip("ex vivo prep test requires numpy, tifffile, and pynrrd or SimpleITK")
    import numpy as np
    import tifffile

    path.parent.mkdir(parents=True, exist_ok=True)
    stack = np.arange(2 * 6 * 5, dtype=np.uint16).reshape(2, 6, 5)
    tifffile.imwrite(path, stack)


def _write_tiny_functional_tiff(path: Path) -> None:
    np_spec = importlib.util.find_spec("numpy")
    tif_spec = importlib.util.find_spec("tifffile")
    if np_spec is None or tif_spec is None:
        pytest.skip("functional reference prep test requires numpy and tifffile")
    import numpy as np
    import tifffile

    path.parent.mkdir(parents=True, exist_ok=True)
    stack = np.arange(3 * 6 * 5, dtype=np.uint16).reshape(3, 6, 5)
    tifffile.imwrite(path, stack)


def _write_tiny_registration_anatomy_tiff(path: Path) -> None:
    np_spec = importlib.util.find_spec("numpy")
    tif_spec = importlib.util.find_spec("tifffile")
    if np_spec is None or tif_spec is None:
        pytest.skip("registration prep test requires numpy and tifffile")
    import numpy as np
    import tifffile

    path.parent.mkdir(parents=True, exist_ok=True)
    stack = np.zeros((4, 12, 12), dtype=np.float32)
    for zi in range(stack.shape[0]):
        stack[zi, 2:8, 3:8] = np.arange(6 * 5, dtype=np.float32).reshape(6, 5) + zi
    tifffile.imwrite(path, stack)


def _write_minimal_suite2p_plane(plane_dir: Path) -> None:
    np_spec = importlib.util.find_spec("numpy")
    if np_spec is None:
        pytest.skip("ROI/anatomy matching test requires numpy")
    import numpy as np

    plane_dir.mkdir(parents=True, exist_ok=True)
    np.save(plane_dir / "F.npy", np.asarray([[1.0, 3.0, 5.0]], dtype=np.float32))
    np.save(plane_dir / "Fneu.npy", np.asarray([[0.1, 0.2, 0.3]], dtype=np.float32))
    np.save(plane_dir / "spks.npy", np.asarray([[0.0, 1.0, 0.0]], dtype=np.float32))
    np.save(
        plane_dir / "stat.npy",
        np.asarray(
            [{"ypix": np.asarray([1, 1, 2]), "xpix": np.asarray([1, 2, 1]), "overlap": np.asarray([False, False, False])}],
            dtype=object,
        ),
        allow_pickle=True,
    )
    np.save(plane_dir / "ops.npy", {"Ly": 4, "Lx": 5, "fs": 2.0}, allow_pickle=True)
    np.save(plane_dir / "iscell.npy", np.asarray([[1.0, 0.0]], dtype=np.float32), allow_pickle=True)


def _write_tiny_anatomy_labels_tiff(path: Path) -> None:
    np_spec = importlib.util.find_spec("numpy")
    tif_spec = importlib.util.find_spec("tifffile")
    if np_spec is None or tif_spec is None:
        pytest.skip("ROI/anatomy matching test requires numpy and tifffile")
    import numpy as np
    import tifffile

    path.parent.mkdir(parents=True, exist_ok=True)
    labels = np.zeros((1, 4, 5), dtype=np.uint16)
    labels[0, 1, 2] = 7
    labels[0, 1, 3] = 7
    labels[0, 2, 3] = 7
    tifffile.imwrite(path, labels)


def _write_plane_refs_summary(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            [
                {
                    "label": "L000_f00_plane0_mcorrected_flipX",
                    "index": 0,
                    "best_z": 0,
                    "scale": 1.0,
                    "tform_src": "ncc_xy",
                    "ncc_xy": {"x0": 0, "y0": 0, "score": 1.0},
                    "ref_shape": [4, 5],
                    "ref_scaled_shape": [4, 5],
                    "reference_raw_path": "ref_raw.tif",
                    "reference_norm_path": "ref_norm.tif",
                }
            ],
            indent=2,
        )
    )


def _write_selected_ants_comparison(path: Path, transform_path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    transform_path.parent.mkdir(parents=True, exist_ok=True)
    transform_path.write_text("mock ants transform placeholder\n")
    path.write_text(
        "fish_id,plane_idx,plane,method,selected,best_z,scale,ref_shape,ref_scaled_shape,transformlist\n"
        f"L000_f00,0,L000_f00_plane0_mcorrected_flipX,ncc_xy,False,0,1.0,\"(4, 5)\",\"(4, 5)\",\n"
        f"L000_f00,0,L000_f00_plane0_mcorrected_flipX,ants_rigid_affine,True,2,1.25,\"(4, 5)\",\"(5, 6)\",{transform_path}\n"
    )


def _write_tforms_by_plane(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "plane_index,label,best_z,m00,m01,m02,m10,m11,m12\n"
        "0,L000_f00_plane0_mcorrected_flipX,3,1,0,10,0,1,20\n"
    )


def _write_hcr_aligned_artifacts(source_root: Path, fish_id: str = "L000_f00") -> None:
    source_root.mkdir(parents=True, exist_ok=True)
    prefix = f"{fish_id}_round1_channel2_sst1_1_cp_masks_in_2p"
    (source_root / f"{prefix}_labels_uint16.tif").write_bytes(b"label-tif")
    (source_root / f"{prefix}_conf_within_labels.tif").write_bytes(b"qc-label-tif")
    (source_root / f"{prefix}_twoP_within_mask.tif").write_bytes(b"qc-mask-tif")
    pair_header = "conf_label,twoP_label,distance_um,overlap_voxels,within_gate,conf_vol,twoP_vol,iou,overlap_frac_conf,overlap_frac_twoP,pair_type,quality"
    pair_row = "3,7,1.2,25,True,40,50,0.4,0.6,0.5,1-1,good"
    (source_root / f"{prefix}_matches.csv").write_text(pair_header + "\n" + pair_row + "\n")
    (source_root / f"{prefix}_review.csv").write_text(pair_header + "\n" + pair_row + "\n")
    (source_root / f"{prefix}_final_pairs.csv").write_text(pair_header + "\n" + pair_row + "\n")
    (source_root / f"{prefix}_warp_meta.json").write_text(
        '{"space":"2p","filter_stats":{"n_labels_after":2,"low_conf_labels":[]}}\n'
    )
    (source_root / f"{fish_id}_round1_channel2_sst1_1_in_2p.nrrd").write_bytes(b"large-volume-placeholder")


def _write_hcr_direct_recompute_inputs(fish_dir: Path) -> None:
    fish_id = fish_dir.name
    data_root = fish_dir.parent
    (data_root / "matchingMetadata.csv").write_text(
        "\ufefffish_id,best_round,num_rounds,polarity,bigwarp\n"
        f"{fish_id},r2,2,south,False\n"
    )
    raw_cp_masks = fish_dir / "03_analysis" / "confocal" / "raw" / "cp_masks"
    raw_cp_masks.mkdir(parents=True, exist_ok=True)
    (raw_cp_masks / f"{fish_id}_round1_channel2_sst1_1_cp_masks.tif").write_bytes(b"raw-mask")
    rbest = fish_dir / "02_reg" / "00_preprocessing" / "rbest"
    rn = fish_dir / "02_reg" / "00_preprocessing" / "rn"
    rbest.mkdir(parents=True, exist_ok=True)
    rn.mkdir(parents=True, exist_ok=True)
    (rbest / f"{fish_id}_round2_channel2_sst1_2.nrrd").write_bytes(b"rbest-hcr")
    (rn / f"{fish_id}_round1_channel2_sst1_1.nrrd").write_bytes(b"rn-hcr")
    rbest_tforms = fish_dir / "02_reg" / "01_rbest-2p" / "transMatrices"
    rn_tforms = fish_dir / "02_reg" / "02_rn-rbest" / "transMatrices"
    rbest_tforms.mkdir(parents=True, exist_ok=True)
    rn_tforms.mkdir(parents=True, exist_ok=True)
    (rbest_tforms / f"{fish_id}_round2_GCaMP_to_2p_0GenericAffine.mat").write_text("affine\n")
    (rbest_tforms / f"{fish_id}_round2_GCaMP_to_2p_1Warp.nii.gz").write_bytes(b"warp")
    (rbest_tforms / f"{fish_id}_round2_GCaMP_to_2p_1InverseWarp.nii.gz").write_bytes(b"inverse")
    (rn_tforms / f"{fish_id}_round1_GCaMP_to_r2_0GenericAffine.mat").write_text("affine\n")
    (rn_tforms / f"{fish_id}_round1_GCaMP_to_r2_1Warp.nii.gz").write_bytes(b"warp")
    (rn_tforms / f"{fish_id}_round1_GCaMP_to_r2_1InverseWarp.nii.gz").write_bytes(b"inverse")


def _write_control_roi_identity_csv(source_root: Path) -> Path:
    source_root.mkdir(parents=True, exist_ok=True)
    path = source_root / "functional_roi_activity_identity.csv"
    path.write_text(
        "\n".join(
            [
                "fish_id,plane_idx,func_label,roi_idx,selected_anat_label,selected_dist_um,selected_overlap_px,n_overlap_candidates_any,n_overlap_candidates_valid,matched_anat_plane,plane_match_outcome,claim_outcome,has_unique_anat_match,anat_label,identity_label,has_identity_assigned,response_class,response_summary_class,bpi,bpi_category,gene",
                "L000_f00,0,roi_0001,1,7,1.25,3,1,1,0,matched,matched,True,7,slc17a6,True,responsive,responsive,0.5,positive,slc17a6",
                "L000_f00,0,roi_0002,2,,nan,0,0,0,,no_overlap,unmatched,False,,,,inactive,inactive,0.0,neutral,",
            ]
        )
        + "\n"
    )
    return path


def _write_staged_identity_geometry_dependencies(pipeline_root: Path, fish_id: str = "L000_f00") -> None:
    roi_root = pipeline_root / "match-roi-to-anatomy" / "registration"
    roi_root.mkdir(parents=True, exist_ok=True)
    (roi_root / "functional_roi_anatomy_matches.csv").write_text(
        "\n".join(
            [
                "fish_id,plane_idx,func_label,roi_idx,selected_anat_label,has_unique_anat_match,anat_label",
                f"{fish_id},0,1,1,7,True,7",
            ]
        )
        + "\n"
    )
    (roi_root / "functional_roi_anatomy_match_by_plane.csv").write_text(
        "fish_id,plane_idx,n_rois,n_unique_matches\n" f"{fish_id},0,1,1\n"
    )
    hcr_root = pipeline_root / "register-hcr-to-anatomy" / "confocal" / "aligned"
    _write_hcr_aligned_artifacts(hcr_root, fish_id)


def _write_assign_hcr_replay_inputs_and_controls(fish_dir: Path, pipeline_root: Path) -> None:
    import pandas as pd

    from codeants_2pf_hcr.context import resolve_func_polarity
    from codeants_2pf_hcr.pipeline import _build_hcr_activity_status_summary_df
    from codeants_2pf_hcr.spatial import apply_func_orientation, imread_any
    from codeants_2pf_hcr.suite2p import Suite2pStageConfig, load_suite2p_stage

    fish_id = fish_dir.name
    suite2p_plane = fish_dir / "03_analysis" / "functional" / "suite2P" / "plane0"
    _write_minimal_suite2p_plane(suite2p_plane)
    anatomy_labels = fish_dir / "03_analysis" / "structural" / "cp_masks" / f"{fish_id}_anatomy_00001_8bit_cp_masks.tif"
    _write_tiny_anatomy_labels_tiff(anatomy_labels)
    plane_summary = pipeline_root / "register-functional-to-anatomy" / "plane_refs_summary.json"
    _write_plane_refs_summary(plane_summary)

    plane_refs = load_plane_refs_summary(plane_summary)
    polarity, polarity_source = resolve_func_polarity(
        fish_id,
        fish_dir / "01_raw" / "2p" / "metadata" / f"{fish_id}_metadata.csv",
        fish_dir=fish_dir,
    )
    suite2p_result = load_suite2p_stage(
        plane_refs=plane_refs,
        suite2p_root=fish_dir / "03_analysis" / "functional" / "suite2P",
        fish_id=fish_id,
        polarity=polarity,
        polarity_source=polarity_source,
        config=Suite2pStageConfig(verbose=False),
    )
    response_lookup = hcr_response_lookup_from_roi_master_df(
        pd.read_csv(fish_dir / "03_analysis" / "functional" / "registration" / "functional_roi_activity_identity.csv"),
        fish_id=fish_id,
    )
    hcr_root = pipeline_root / "register-hcr-to-anatomy" / "confocal" / "aligned"
    cfg = HcrActivityExportConfig()

    def orient(arr):
        return apply_func_orientation(arr, polarity=polarity, flip_x=True)

    status_df, raw_df, _analysis_df, candidate_df, _plane_meta_df = build_hcr_activity_tables(
        suite2p_result["suite2p_by_ref_idx"],
        plane_refs,
        imread_any(anatomy_labels),
        [
            {
                "mask_path": str(next(hcr_root.glob("*_cp_masks_in_2p_labels_uint16.tif"))),
                "final_pairs": pd.read_csv(next(hcr_root.glob("*_cp_masks_in_2p_final_pairs.csv"))),
            }
        ],
        fish_id=fish_id,
        active_class=cfg.active_class,
        inactive_class=cfg.inactive_class,
        require_overlap=cfg.require_overlap_func_anat,
        min_overlap=cfg.min_overlap_func_anat,
        max_dist_um=cfg.max_dist_func_anat,
        out_of_plane=cfg.hcr_out_of_plane,
        in_plane_active=cfg.hcr_in_plane_responsive,
        in_plane_inactive=cfg.hcr_in_plane_unavailable,
        in_plane_no_func=cfg.hcr_in_plane_no_func,
        match_policy_version=cfg.hcr_activity_match_policy,
        selection_rule=cfg.selection_rule,
        gene_from_mask_func=gene_from_mask,
        response_lookup_df=response_lookup,
        apply_func_orientation_func=orient,
    )
    final_status_df, final_raw_df, final_analysis_df, final_candidate_df = finalize_hcr_activity_export_tables(
        status_df,
        raw_df,
        candidate_df,
        response_lookup,
        config=cfg,
        fish_id=fish_id,
    )
    final_summary_df = _build_hcr_activity_status_summary_df(final_status_df, hcr_root)
    registration_dir = fish_dir / "03_analysis" / "functional" / "registration"
    final_status_df.to_csv(registration_dir / "hcr_activity_status.csv", index=False)
    final_summary_df.to_csv(registration_dir / "hcr_activity_status_summary.csv", index=False)
    final_raw_df.to_csv(registration_dir / "conf_to_func_pairs_raw.csv", index=False)
    final_analysis_df.to_csv(registration_dir / "conf_to_func_pairs.csv", index=False)
    final_candidate_df.to_csv(registration_dir / "hcr_func_candidates.csv", index=False)


def test_attach_identity_to_functional_roi_geometry_uses_staged_lookup_over_source_identity() -> None:
    import pandas as pd

    geometry_df = pd.DataFrame(
        [
            {
                "fish_id": "L000_f00",
                "plane_idx": 0,
                "func_label": 1,
                "selected_anat_label": 7,
                "has_unique_anat_match": True,
                "anat_label": 7,
            },
            {
                "fish_id": "L000_f00",
                "plane_idx": 0,
                "func_label": 2,
                "selected_anat_label": "",
                "has_unique_anat_match": False,
                "anat_label": "",
            },
        ]
    )
    lookup_df = pd.DataFrame(
        [{"anat_label": 7, "identity_label": "sst1.1", "identity_gene_count": 1}]
    )
    passthrough_df = pd.DataFrame(
        [
            {
                "plane_idx": 0,
                "func_label": 1,
                "selected_anat_label": 99,
                "has_unique_anat_match": False,
                "anat_label": 99,
                "identity_label": "stale",
                "identity_gene_count": 1,
                "has_identity_assigned": True,
                "identity_display_label": "stale",
                "activity_class": "Active neurons",
                "is_active": True,
            },
            {
                "plane_idx": 0,
                "func_label": 2,
                "selected_anat_label": "",
                "has_unique_anat_match": False,
                "anat_label": "",
                "identity_label": "stale",
                "identity_gene_count": 1,
                "has_identity_assigned": True,
                "identity_display_label": "stale",
                "activity_class": "Low-quality traces",
                "is_active": False,
            },
        ]
    )

    out = attach_identity_to_functional_roi_geometry_df(
        geometry_df,
        lookup_df,
        passthrough_df=passthrough_df,
    )

    assert list(out.columns) == list(passthrough_df.columns)
    first = out[out["func_label"].astype(str).eq("1")].iloc[0]
    assert str(first["selected_anat_label"]) == "7"
    assert str(first["anat_label"]) == "7"
    assert first["identity_label"] == "sst1.1"
    assert bool(first["has_identity_assigned"]) is True
    assert first["activity_class"] == "Active neurons"
    second = out[out["func_label"].astype(str).eq("2")].iloc[0]
    assert str(second["identity_label"]) in {"", "<NA>", "nan", "NaN"}
    assert bool(second["has_identity_assigned"]) is False
    assert second["identity_display_label"] == "no identity assigned"


def test_finalize_hcr_activity_export_tables_adds_response_selection_contract_columns() -> None:
    import pandas as pd

    status_legacy = pd.DataFrame(
        [
            {
                "fish_id": "L000_f00",
                "gene": "sst1.1",
                "conf_mask": "channel2_sst1_1_cp_masks.tif",
                "anat_label": 7,
                "conf_labels": "[3]",
                "conf_label_count": 1,
                "primary_conf_label": 3,
                "conf_label": 3,
                "dist_conf_anat_um": 1.0,
                "represented_on_func_plane": True,
                "represented_plane_count": 1,
                "represented_plane_indices": "[0]",
                "functional_status": "in-plane active ROI",
            }
        ]
    )
    raw_legacy = pd.DataFrame(
        [
            {
                "fish_id": "L000_f00",
                "gene": "sst1.1",
                "conf_mask": "channel2_sst1_1_cp_masks.tif",
                "anat_label": 7,
                "conf_labels": "[3]",
                "conf_label_count": 1,
                "primary_conf_label": 3,
                "conf_label": 3,
                "dist_conf_anat_um": 1.0,
                "plane": "plane0",
                "plane_idx": 0,
                "best_z": 2,
                "func_label": 10,
                "roi_idx": 9,
                "overlap_px_func_anat": 5,
                "dist_func_anat_um": 2.5,
                "is_active": False,
                "activity_class": "Low-quality traces",
            },
            {
                "fish_id": "L000_f00",
                "gene": "sst1.1",
                "conf_mask": "channel2_sst1_1_cp_masks.tif",
                "anat_label": 7,
                "conf_labels": "[3]",
                "conf_label_count": 1,
                "primary_conf_label": 3,
                "conf_label": 3,
                "dist_conf_anat_um": 1.0,
                "plane": "plane0",
                "plane_idx": 0,
                "best_z": 2,
                "func_label": 11,
                "roi_idx": 10,
                "overlap_px_func_anat": 4,
                "dist_func_anat_um": 1.5,
                "is_active": False,
                "activity_class": "Low-quality traces",
            },
        ]
    )
    roi_master = pd.DataFrame(
        [
            {
                "fish_id": "L000_f00",
                "plane_idx": 0,
                "func_label": 10,
                "response_is_active": False,
                "response_class": "low activity",
                "response_summary_class": "Low activity",
                "suite2p_is_cell": True,
                "suite2p_activity_class": "Active neurons",
            },
            {
                "fish_id": "L000_f00",
                "plane_idx": 0,
                "func_label": 11,
                "response_is_active": True,
                "response_class": "bout-responsive",
                "response_summary_class": "Responsive neurons",
                "suite2p_is_cell": True,
                "suite2p_activity_class": "Active neurons",
            },
        ]
    )

    response_lookup = hcr_response_lookup_from_roi_master_df(roi_master, fish_id="L000_f00")
    status_df, raw_df, analysis_df, candidate_df = finalize_hcr_activity_export_tables(
        status_legacy,
        raw_legacy,
        raw_legacy.copy(),
        response_lookup,
        fish_id="L000_f00",
    )

    assert status_df.loc[0, "functional_status"] == "in-plane responsive ROI"
    assert bool(status_df.loc[0, "selected_for_trace_export"]) is True
    assert int(status_df.loc[0, "selected_func_label"]) == 11
    assert int(status_df.loc[0, "n_responsive_candidates"]) == 1
    assert set(raw_df["candidate_response_bucket"]) == {"low activity", "responsive"}
    assert bool(raw_df.loc[raw_df["func_label"].astype(int).eq(11), "is_selected_for_analysis"].iloc[0]) is True
    assert len(analysis_df) == 1
    assert int(analysis_df.loc[0, "func_label"]) == 11
    assert "candidate_response_bucket" in candidate_df.columns


def test_pipeline_contracts_follow_declared_stage_order() -> None:
    contracts = pipeline_contracts()
    assert tuple(contract.name for contract in contracts) == PIPELINE_STAGE_ORDER
    assert contracts[0].name == "audit-inputs"
    assert contracts[0].depends_on == ()
    assert contracts[-1].name == "make-figures"
    assert "prepare-functional-reference-stacks" in GRANULAR_PREPROCESSING_STAGE_NAMES
    assert "prepare-in-vivo-anatomy-stack" in GRANULAR_PREPROCESSING_STAGE_NAMES
    assert "prepare-ex-vivo-anatomy-stack" in GRANULAR_PREPROCESSING_STAGE_NAMES
    assert "segment-hcr-cellpose" in GRANULAR_PREPROCESSING_STAGE_NAMES
    assert "segment-ex-vivo-anatomy-cellpose" in GRANULAR_PREPROCESSING_STAGE_NAMES


def test_activity_config_default_zero_band_matches_notebook_control_default() -> None:
    assert ActivityConfig().zero_band == 0.50


def test_prepare_functional_reference_stacks_stage_writes_manifest_outputs(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    (fish_dir / "01_raw" / "2p" / "metadata" / f"{fish_dir.name}_metadata.csv").write_text(
        "parameter,value\nfish_orientation,bottom-left\n"
    )
    source = fish_dir / "02_reg" / "00_preprocessing" / "2p_functional" / "02_motionCorrected" / f"{fish_dir.name}_plane0_mcorrected.tif"
    _write_tiny_functional_tiff(source)
    output_dir = tmp_path / "functional-refs"

    manifest = run_prepare_functional_reference_stacks_stage(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True),
        functional_stack_paths=[source],
        output_dir=output_dir,
        force_recompute=True,
    )

    raw_ref = output_dir / f"{source.stem}_flipX_ref_raw.tif"
    norm_ref = output_dir / f"{source.stem}_flipX_ref_norm.tif"
    assert manifest.status == "pass"
    assert raw_ref.exists()
    assert norm_ref.exists()
    assert manifest.inputs[0].path == str(source)
    assert {record.path for record in manifest.outputs} == {str(raw_ref), str(norm_ref)}
    assert any(check.label == "functional reference raw TIFFs" and check.status == "pass" for check in manifest.checks)


def test_prepare_functional_reference_stacks_stage_discovers_motion_corrected_inputs(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    (fish_dir / "01_raw" / "2p" / "metadata" / f"{fish_dir.name}_metadata.csv").write_text(
        "parameter,value\nfish_orientation,top-right\n"
    )
    motion_dir = fish_dir / "02_reg" / "00_preprocessing" / "2p_functional" / "02_motionCorrected"
    ignored = motion_dir / f"{fish_dir.name}_plane0_other.tif"
    source = motion_dir / f"{fish_dir.name}_plane0_mcorrected.tif"
    _write_tiny_functional_tiff(ignored)
    _write_tiny_functional_tiff(source)
    paths = resolve_pipeline_paths(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, pipeline_root=tmp_path / "staged")
    )

    assert discover_functional_motion_corrected_stacks(paths) == (source,)
    manifest = run_prepare_functional_reference_stacks_stage(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True, pipeline_root=tmp_path / "staged"),
        force_recompute=True,
    )

    assert manifest.status == "pass"
    assert manifest.inputs[0].path == str(source)
    assert all(record.path.startswith(str(functional_reference_output_dir(paths))) for record in manifest.outputs)


def test_single_fish_pipeline_cli_prepare_functional_reference_outputs_manifest(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    (fish_dir / "01_raw" / "2p" / "metadata" / f"{fish_dir.name}_metadata.csv").write_text(
        "parameter,value\nfish_orientation,bottom-left\n"
    )
    source = fish_dir / "02_reg" / "00_preprocessing" / "2p_functional" / "02_motionCorrected" / f"{fish_dir.name}_plane0_mcorrected.tif"
    _write_tiny_functional_tiff(source)
    output_dir = tmp_path / "functional-refs"

    result = subprocess.run(
        [
            sys.executable,
            "tools/single_fish_pipeline.py",
            "prepare-functional-reference-stacks",
            "--fish-id",
            fish_dir.name,
            "--local-root",
            str(tmp_path),
            "--strict",
            "--functional-stack-path",
            str(source),
            "--output-dir",
            str(output_dir),
            "--force-recompute",
        ],
        cwd=REPO_ROOT,
        env={"PYTHONPATH": "src"},
        text=True,
        capture_output=True,
        check=False,
    )
    payload = json.loads(result.stdout)
    assert result.returncode == 0
    assert payload["stage_name"] == "prepare-functional-reference-stacks"
    assert payload["status"] == "pass"
    assert len(payload["outputs"]) == 2


def test_register_functional_to_anatomy_stage_writes_ncc_outputs(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    (fish_dir / "01_raw" / "2p" / "metadata" / f"{fish_dir.name}_metadata.csv").write_text(
        "parameter,value\nfish_orientation,bottom-left\n"
    )
    source = fish_dir / "02_reg" / "00_preprocessing" / "2p_functional" / "02_motionCorrected" / f"{fish_dir.name}_plane0_mcorrected.tif"
    _write_tiny_functional_tiff(source)
    anatomy = tmp_path / "registration-anatomy.tif"
    _write_tiny_registration_anatomy_tiff(anatomy)
    import numpy as np
    import tifffile

    anatomy_labels = tmp_path / "registration-anatomy-labels.tif"
    labels = np.zeros((4, 12, 12), dtype=np.uint16)
    labels[:, 4:8, 5:9] = 3
    tifffile.imwrite(anatomy_labels, labels)
    func_labels_dir = fish_dir / "03_analysis" / "functional" / "derived"
    func_labels_dir.mkdir(parents=True, exist_ok=True)
    func_labels = np.zeros((12, 12), dtype=np.uint16)
    func_labels[4:8, 5:9] = 11
    tifffile.imwrite(func_labels_dir / f"{fish_dir.name}_plane0_mcorrected_flipX_func_mask_in_2p.tif", func_labels)
    config = SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True, pipeline_root=tmp_path / "staged")
    refs_manifest = run_prepare_functional_reference_stacks_stage(
        config,
        functional_stack_paths=[source],
        force_recompute=True,
    )
    assert refs_manifest.status == "pass"

    manifest = run_register_functional_to_anatomy_stage(
        config,
        anatomy_stack_path=anatomy,
        anatomy_labels_path=anatomy_labels,
        force_recompute=True,
    )

    stage_root = functional_to_anatomy_registration_root(resolve_pipeline_paths(config))
    tforms_path = stage_root / "registration" / "tforms_by_plane.csv"
    summary_path = stage_root / "plane_refs_summary.json"
    qa_png = stage_root / "qa" / "functional_anatomy_center_overlay_200px.png"
    qa_csv = stage_root / "qa" / "functional_anatomy_center_overlay_200px.csv"
    assert manifest.status == "pass"
    assert tforms_path.exists()
    assert summary_path.exists()
    assert qa_png.exists()
    assert qa_csv.exists()
    summary = json.loads(summary_path.read_text())
    assert summary[0]["label"] == f"{fish_dir.name}_plane0_mcorrected_flipX"
    assert summary[0]["ncc_scores_count"] > 0
    assert summary[0]["anat_label_z_mode"] == "direct"
    assert (stage_root / "ncc" / "ncc_bestz_by_plane.json").exists()
    assert (stage_root / "ncc" / "inplane_registration_comparison" / "inplane_registration_comparison.csv").exists()
    assert discover_functional_reference_pairs(functional_reference_output_dir(resolve_pipeline_paths(config)))
    assert any(check.label == "functional transform table" and check.status == "pass" for check in manifest.checks)
    assert any(check.label == "functional/anatomy center overlay QA" and check.status == "pass" for check in manifest.checks)


def test_single_fish_pipeline_cli_register_functional_to_anatomy_outputs_manifest(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    (fish_dir / "01_raw" / "2p" / "metadata" / f"{fish_dir.name}_metadata.csv").write_text(
        "parameter,value\nfish_orientation,bottom-left\n"
    )
    source = fish_dir / "02_reg" / "00_preprocessing" / "2p_functional" / "02_motionCorrected" / f"{fish_dir.name}_plane0_mcorrected.tif"
    _write_tiny_functional_tiff(source)
    anatomy = tmp_path / "registration-anatomy.tif"
    _write_tiny_registration_anatomy_tiff(anatomy)
    pipeline_root = tmp_path / "staged"
    config = SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True, pipeline_root=pipeline_root)
    assert (
        run_prepare_functional_reference_stacks_stage(
            config,
            functional_stack_paths=[source],
            force_recompute=True,
        ).status
        == "pass"
    )

    result = subprocess.run(
        [
            sys.executable,
            "tools/single_fish_pipeline.py",
            "register-functional-to-anatomy",
            "--fish-id",
            fish_dir.name,
            "--local-root",
            str(tmp_path),
            "--strict",
            "--pipeline-root",
            str(pipeline_root),
            "--anatomy-stack-path",
            str(anatomy),
            "--no-cv2",
            "--force-recompute",
        ],
        cwd=REPO_ROOT,
        env={"PYTHONPATH": "src"},
        text=True,
        capture_output=True,
        check=False,
    )
    payload = json.loads(result.stdout)
    assert result.returncode == 0
    assert payload["stage_name"] == "register-functional-to-anatomy"
    assert payload["status"] == "pass"
    assert any(output["label"] == "staged plane refs summary" for output in payload["outputs"])
    assert any(output["label"] == "staged functional transform table" for output in payload["outputs"])


def test_register_hcr_to_anatomy_stage_stages_small_aligned_artifacts(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    source_root = fish_dir / "03_analysis" / "confocal" / "aligned"
    _write_hcr_aligned_artifacts(source_root, fish_dir.name)
    config = SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True, pipeline_root=tmp_path / "staged")

    manifest = run_register_hcr_to_anatomy_stage(config, source_root=source_root, force_recompute=True)

    out_root = hcr_to_anatomy_registration_root(resolve_pipeline_paths(config)) / "confocal" / "aligned"
    assert manifest.status == "pass"
    assert (out_root / f"{fish_dir.name}_round1_channel2_sst1_1_cp_masks_in_2p_labels_uint16.tif").exists()
    assert (out_root / f"{fish_dir.name}_round1_channel2_sst1_1_cp_masks_in_2p_matches.csv").exists()
    assert (out_root / f"{fish_dir.name}_round1_channel2_sst1_1_cp_masks_in_2p_final_pairs.csv").exists()
    assert not (out_root / f"{fish_dir.name}_round1_channel2_sst1_1_in_2p.nrrd").exists()
    assert manifest.parameters["copy_policy"] == "csv_json_tif_only"
    assert manifest.parameters["input_aligned_nrrd_count"] == 1
    assert manifest.parameters["hcr_recompute_mode"] == "accepted_artifact_staging_with_direct_ants_readiness"
    assert any(check.label == "large aligned intensity volumes not copied" and check.status == "pass" for check in manifest.checks)
    assert any(check.label == "HCR direct recompute matching metadata" and check.status == "warn" for check in manifest.checks)


def test_register_hcr_to_anatomy_stage_reports_direct_ants_recompute_prerequisites(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    source_root = fish_dir / "03_analysis" / "confocal" / "aligned"
    _write_hcr_aligned_artifacts(source_root, fish_dir.name)
    _write_hcr_direct_recompute_inputs(fish_dir)
    config = SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True, pipeline_root=tmp_path / "staged")

    manifest = run_register_hcr_to_anatomy_stage(config, source_root=source_root, force_recompute=True)

    checks_by_label = {check.label: check for check in manifest.checks}
    assert manifest.status == "pass"
    assert manifest.parameters["matching_metadata_best_round"] == "r2"
    assert manifest.parameters["matching_metadata_num_rounds"] == "2"
    assert manifest.parameters["matching_metadata_bigwarp"] is False
    assert manifest.parameters["hcr_direct_ants_expected"] is True
    assert manifest.parameters["hcr_raw_cp_mask_count"] == 2
    assert manifest.parameters["hcr_rbest_nrrd_count"] == 2
    assert manifest.parameters["hcr_rn_nrrd_count"] == 1
    assert manifest.parameters["hcr_rbest_to_2p_affine_count"] == 1
    assert manifest.parameters["hcr_rbest_to_2p_warp_count"] == 1
    assert manifest.parameters["hcr_rn_to_rbest_required"] is True
    assert checks_by_label["HCR direct recompute matching metadata"].status == "pass"
    assert checks_by_label["HCR direct recompute route"].status == "pass"
    assert checks_by_label["HCR direct recompute raw masks"].status == "pass"
    assert checks_by_label["HCR direct recompute rbest NRRDs"].status == "pass"
    assert checks_by_label["HCR direct recompute rbest-to-2p transforms"].status == "pass"
    assert checks_by_label["HCR direct recompute rn-to-rbest transforms"].status == "pass"


def test_register_hcr_to_anatomy_stage_can_recompute_direct_ants_labels(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    source_root = fish_dir / "03_analysis" / "confocal" / "aligned"
    _write_hcr_aligned_artifacts(source_root, fish_dir.name)
    _write_hcr_direct_recompute_inputs(fish_dir)
    anatomy_labels = fish_dir / "03_analysis" / "structural" / "cp_masks" / f"{fish_dir.name}_anatomy_00001_8bit_cp_masks.tif"
    _write_tiny_anatomy_labels_tiff(anatomy_labels)
    config = SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True, pipeline_root=tmp_path / "staged")

    def fake_direct_warp(**kwargs):
        import numpy as np
        import tifffile

        output_dir = Path(kwargs["output_dir"])
        output_dir.mkdir(parents=True, exist_ok=True)
        label_path = output_dir / f"{fish_dir.name}_round1_channel2_sst1_1_cp_masks_in_2p_labels_uint16.tif"
        meta_path = output_dir / f"{fish_dir.name}_round1_channel2_sst1_1_cp_masks_in_2p_warp_meta.json"
        labels = np.zeros((1, 4, 5), dtype=np.uint16)
        labels[0, 1, 2] = 3
        labels[0, 1, 3] = 3
        labels[0, 2, 3] = 3
        tifffile.imwrite(label_path, labels)
        meta_path.write_text('{"method":"ants_direct_to_2p"}\n')
        return (
            SimpleNamespace(
                output_label_path=str(label_path),
                output_metadata_path=str(meta_path),
            ),
        )

    import codeants_2pf_hcr.hcr_warp as hcr_warp

    monkeypatch.setattr(hcr_warp, "run_direct_ants_hcr_label_warp", fake_direct_warp)

    manifest = run_register_hcr_to_anatomy_stage(
        config,
        source_root=source_root,
        force_recompute=True,
        recompute_direct_ants=True,
    )

    out_root = hcr_to_anatomy_registration_root(resolve_pipeline_paths(config)) / "confocal" / "aligned"
    checks_by_label = {check.label: check for check in manifest.checks}
    assert manifest.status == "pass"
    assert (out_root / f"{fish_dir.name}_round1_channel2_sst1_1_cp_masks_in_2p_matches.csv").exists()
    assert (out_root / f"{fish_dir.name}_round1_channel2_sst1_1_cp_masks_in_2p_review.csv").exists()
    assert (out_root / f"{fish_dir.name}_round1_channel2_sst1_1_cp_masks_in_2p_final_pairs.csv").exists()
    assert manifest.parameters["hcr_recompute_mode"] == "direct_ants_label_warp_and_match_tables"
    assert manifest.parameters["recompute_direct_ants"] is True
    assert manifest.parameters["direct_ants_warp_result_count"] == 1
    assert manifest.parameters["direct_ants_filter_stats_overlay_count"] == 1
    assert manifest.parameters["recomputed_label_artifact_count"] == 2
    assert manifest.parameters["direct_ants_match_table_artifact_count"] == 3
    assert checks_by_label["HCR direct ANTs label warp recompute"].status == "pass"
    assert checks_by_label["HCR direct ANTs accepted filter stats overlay"].status == "pass"
    assert checks_by_label["HCR direct ANTs match table recompute"].status == "pass"
    meta = json.loads((out_root / f"{fish_dir.name}_round1_channel2_sst1_1_cp_masks_in_2p_warp_meta.json").read_text())
    assert meta["filter_stats"]["n_labels_after"] == 2
    assert meta["filter_stats_source"] == "accepted_hcr_warp_metadata"


def test_register_hcr_to_anatomy_stage_refuses_existing_outputs_without_force(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    source_root = fish_dir / "03_analysis" / "confocal" / "aligned"
    _write_hcr_aligned_artifacts(source_root, fish_dir.name)
    config = SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True, pipeline_root=tmp_path / "staged")

    first = run_register_hcr_to_anatomy_stage(config, source_root=source_root, force_recompute=True)
    second = run_register_hcr_to_anatomy_stage(config, source_root=source_root)

    assert first.status == "pass"
    assert second.status == "fail"
    assert "pass --force-recompute" in second.errors[0]


def test_register_hcr_to_anatomy_stage_fails_without_required_aligned_artifacts(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    source_root = tmp_path / "empty-aligned"
    source_root.mkdir()
    config = SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True, pipeline_root=tmp_path / "staged")

    manifest = run_register_hcr_to_anatomy_stage(config, source_root=source_root, force_recompute=True)

    assert manifest.status == "fail"
    assert any("HCR aligned label TIFFs" in error for error in manifest.errors)
    assert any("HCR aligned match CSVs" in error for error in manifest.errors)


def test_single_fish_pipeline_cli_register_hcr_to_anatomy_outputs_manifest(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    source_root = fish_dir / "03_analysis" / "confocal" / "aligned"
    _write_hcr_aligned_artifacts(source_root, fish_dir.name)
    pipeline_root = tmp_path / "staged"

    result = subprocess.run(
        [
            sys.executable,
            "tools/single_fish_pipeline.py",
            "register-hcr-to-anatomy",
            "--fish-id",
            fish_dir.name,
            "--local-root",
            str(tmp_path),
            "--strict",
            "--pipeline-root",
            str(pipeline_root),
            "--source-root",
            str(source_root),
            "--force-recompute",
        ],
        cwd=REPO_ROOT,
        env={"PYTHONPATH": "src"},
        text=True,
        capture_output=True,
        check=False,
    )
    payload = json.loads(result.stdout)
    assert result.returncode == 0
    assert payload["stage_name"] == "register-hcr-to-anatomy"
    assert payload["status"] == "pass"
    assert payload["parameters"]["copy_policy"] == "csv_json_tif_only"
    assert any(output["label"] == "staged HCR aligned label TIFFs" for output in payload["outputs"])


def _write_hcr_replay_response_master(path: Path, fish_id: str = "L000_f00") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                "fish_id,plane_idx,func_label,response_is_active,response_class,response_summary_class,suite2p_is_cell,suite2p_activity_class",
                f"{fish_id},0,1,True,responsive,Responsive neurons,True,Active neurons",
            ]
        )
        + "\n"
    )


def test_hcr_activity_replay_audit_runs_label_first_without_outputs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    (fish_dir / "01_raw" / "2p" / "metadata" / f"{fish_dir.name}_metadata.csv").write_text(
        "parameter,value\nfish_orientation,top-right\n"
    )
    _write_minimal_suite2p_plane(fish_dir / "03_analysis" / "functional" / "suite2P" / "plane0")
    anatomy_labels = tmp_path / "anat_labels.tif"
    _write_tiny_anatomy_labels_tiff(anatomy_labels)
    plane_summary = tmp_path / "plane_refs_summary.json"
    _write_plane_refs_summary(plane_summary)
    hcr_root = tmp_path / "hcr-aligned"
    _write_hcr_aligned_artifacts(hcr_root, fish_dir.name)
    response_master = tmp_path / "functional_roi_activity_identity.csv"
    _write_hcr_replay_response_master(response_master, fish_dir.name)
    config = SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True, pipeline_root=tmp_path / "staged")
    import codeants_2pf_hcr.matching as matching_module

    observed_orientation_callbacks = []
    original_build_hcr_activity_tables = matching_module.build_hcr_activity_tables

    def spy_build_hcr_activity_tables(*args, **kwargs):
        observed_orientation_callbacks.append(kwargs.get("apply_func_orientation_func"))
        return original_build_hcr_activity_tables(*args, **kwargs)

    monkeypatch.setattr(matching_module, "build_hcr_activity_tables", spy_build_hcr_activity_tables)

    manifest = build_single_fish_hcr_activity_replay_manifest(
        config,
        plane_refs_summary_path=plane_summary,
        hcr_anatomy_root=hcr_root,
        identity_input_path=response_master,
        anatomy_labels_path=anatomy_labels,
    )

    assert manifest.stage_name == "audit-hcr-activity-replay"
    assert manifest.status in {"pass", "warn"}
    assert manifest.outputs == ()
    assert manifest.parameters["promotion_enabled"] is False
    assert manifest.parameters["source_policy"] == "label-first HCR replay; ROI master is response lookup only"
    assert any(check.label == "HCR replay source policy" and check.status == "pass" for check in manifest.checks)
    assert any(check.label == "HCR replay transform backend" and check.status == "warn" for check in manifest.checks)
    assert any(check.label == "HCR activity replay recompute" and check.status == "pass" for check in manifest.checks)
    assert observed_orientation_callbacks
    assert all(callable(callback) for callback in observed_orientation_callbacks)


def test_hcr_activity_replay_overlays_selected_ants_transformlists(tmp_path: Path) -> None:
    plane_summary = tmp_path / "plane_refs_summary.json"
    _write_plane_refs_summary(plane_summary)
    comparison_path = tmp_path / "ncc" / "inplane_registration_comparison" / "inplane_registration_comparison.csv"
    transform_path = tmp_path / "ncc" / "inplane_registration_comparison" / "transforms" / "plane0_ants_0.mat"
    _write_selected_ants_comparison(comparison_path, transform_path)

    plane_refs = load_plane_refs_summary(plane_summary)
    overlaid, report = _overlay_selected_ants_transformlists(plane_refs, comparison_path)

    assert report["selected_ants_rows"] == 1
    assert report["overlay_applied_planes"] == 1
    assert report["backend_counts"] == {"ants_rigid_affine": 1}
    assert report["missing_transform_files"] == 0
    assert report["ants_xy_spacing"] == [1.0, 1.0]
    assert overlaid[0]["tform_src"] == "ants_rigid_affine"
    assert overlaid[0]["ants_transformlist"] == [str(transform_path)]
    assert overlaid[0]["ants_transform"]["type"] == "ants_transformlist"
    assert overlaid[0]["best_z"] == 2


def test_hcr_activity_replay_reconstructs_affine_tforms_with_best_z_and_offsets(tmp_path: Path) -> None:
    plane_summary = tmp_path / "plane_refs_summary.json"
    _write_plane_refs_summary(plane_summary)
    tforms_path = tmp_path / "registration" / "tforms_by_plane.csv"
    _write_tforms_by_plane(tforms_path)

    plane_refs = load_plane_refs_summary(plane_summary)
    rebuilt, report = _plane_refs_from_tforms_csv(
        tforms_path,
        plane_refs,
        best_z_by_plane={0: 7},
        selected_metadata_by_plane={0: {"scale": 1.25, "ref_shape": (4, 5), "ref_scaled_shape": (5, 6)}},
        offsets_by_plane={0: (-1.0, 1.0)},
    )

    assert report["rows_loaded"] == 1
    assert report["backend_counts"] == {"affine_tform": 1}
    assert report["best_z_overrides"] == 1
    assert report["offset_planes"] == {"0": [-1.0, 1.0]}
    assert rebuilt[0]["tform_src"] == "affine_tform"
    assert rebuilt[0]["best_z"] == 7
    assert rebuilt[0]["scale"] == 1.25
    assert rebuilt[0]["ref_scaled_shape"] == (5, 6)
    assert rebuilt[0]["tform"]["type"] == "skimage_affine"
    assert rebuilt[0]["tform"]["moving_shape"] == (5, 6)
    assert rebuilt[0]["affine_matrix"][0][2] == 9.0
    assert rebuilt[0]["affine_matrix"][1][2] == 21.0


def test_single_fish_pipeline_cli_audit_hcr_activity_replay_outputs_manifest(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    (fish_dir / "01_raw" / "2p" / "metadata" / f"{fish_dir.name}_metadata.csv").write_text(
        "parameter,value\nfish_orientation,top-right\n"
    )
    _write_minimal_suite2p_plane(fish_dir / "03_analysis" / "functional" / "suite2P" / "plane0")
    anatomy_labels = tmp_path / "anat_labels.tif"
    _write_tiny_anatomy_labels_tiff(anatomy_labels)
    plane_summary = tmp_path / "plane_refs_summary.json"
    _write_plane_refs_summary(plane_summary)
    hcr_root = tmp_path / "hcr-aligned"
    _write_hcr_aligned_artifacts(hcr_root, fish_dir.name)
    response_master = tmp_path / "functional_roi_activity_identity.csv"
    _write_hcr_replay_response_master(response_master, fish_dir.name)

    result = subprocess.run(
        [
            sys.executable,
            "tools/single_fish_pipeline.py",
            "audit-hcr-activity-replay",
            "--fish-id",
            fish_dir.name,
            "--local-root",
            str(tmp_path),
            "--strict",
            "--pipeline-root",
            str(tmp_path / "staged"),
            "--plane-refs-summary-path",
            str(plane_summary),
            "--hcr-anatomy-root",
            str(hcr_root),
            "--identity-input-path",
            str(response_master),
            "--anatomy-labels-path",
            str(anatomy_labels),
        ],
        cwd=REPO_ROOT,
        env={"PYTHONPATH": "src"},
        text=True,
        capture_output=True,
        check=False,
    )
    payload = json.loads(result.stdout)
    assert result.returncode == 0
    assert payload["stage_name"] == "audit-hcr-activity-replay"
    assert payload["parameters"]["promotion_enabled"] is False
    assert any(check["label"] == "HCR replay source policy" for check in payload["checks"])


def test_match_roi_to_anatomy_stage_writes_geometry_outputs(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    (fish_dir / "01_raw" / "2p" / "metadata" / f"{fish_dir.name}_metadata.csv").write_text(
        "parameter,value\nfish_orientation,top-right\n"
    )
    _write_minimal_suite2p_plane(fish_dir / "03_analysis" / "functional" / "suite2P" / "plane0")
    anatomy_labels = tmp_path / "anat_labels.tif"
    _write_tiny_anatomy_labels_tiff(anatomy_labels)
    plane_summary = tmp_path / "plane_refs_summary.json"
    _write_plane_refs_summary(plane_summary)
    config = SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True, pipeline_root=tmp_path / "staged")

    manifest = run_match_roi_to_anatomy_stage(
        config,
        plane_refs_summary_path=plane_summary,
        anatomy_labels_path=anatomy_labels,
        force_recompute=True,
    )

    out_root = tmp_path / "staged" / "match-roi-to-anatomy" / "registration"
    detail_path = out_root / "functional_roi_anatomy_matches.csv"
    summary_path = out_root / "functional_roi_anatomy_match_by_plane.csv"
    assert manifest.status == "pass"
    assert detail_path.exists()
    assert summary_path.exists()
    rows = detail_path.read_text()
    assert "selected_anat_label" in rows
    assert ",7," in rows
    assert load_plane_refs_summary(plane_summary)[0]["ncc_xy"]["x0"] == 0
    assert any(check.label == "ROI/anatomy match rows" and check.status == "pass" for check in manifest.checks)


def test_match_roi_to_anatomy_stage_threads_selected_ants_and_orientation(tmp_path: Path) -> None:
    from unittest.mock import patch

    import numpy as np
    import pandas as pd

    fish_dir = _make_minimal_fish(tmp_path)
    (fish_dir / "01_raw" / "2p" / "metadata" / f"{fish_dir.name}_metadata.csv").write_text(
        "parameter,value\nfish_orientation,top-right\n"
    )
    _write_minimal_suite2p_plane(fish_dir / "03_analysis" / "functional" / "suite2P" / "plane0")
    anatomy_labels = tmp_path / "anat_labels.tif"
    _write_tiny_anatomy_labels_tiff(anatomy_labels)
    plane_summary = tmp_path / "plane_refs_summary.json"
    _write_plane_refs_summary(plane_summary)
    analysis_dir = fish_dir / "03_analysis"
    (analysis_dir / "voxel_sizes.json").write_text(
        json.dumps({"by_path": {"2p_anatomy/anatomy.nrrd": {"X": 0.5, "Y": 0.75, "Z": 2.0}}})
    )
    comparison_dir = analysis_dir / "functional" / "ncc" / "inplane_registration_comparison"
    transform_dir = comparison_dir / "transforms"
    transform_dir.mkdir(parents=True)
    transform_path = transform_dir / "plane0_0GenericAffine.mat"
    transform_path.write_text("#Insight Transform File V1.0\n")
    (comparison_dir / "inplane_registration_comparison.csv").write_text(
        "\n".join(
            [
                "plane_idx,plane,method,selected,best_z,scale,ref_shape,ref_scaled_shape,transformlist",
                f"0,{fish_dir.name}_plane0_mcorrected_flipX,ants_rigid_affine,True,0,1.0,\"(4, 5)\",\"(4, 5)\",{transform_path}",
            ]
        )
        + "\n"
    )
    captured: dict[str, object] = {}

    def fake_build_functional_roi_master_df(suite2p_by_ref_idx, plane_refs, anat_labels, **kwargs):
        captured["plane_refs"] = plane_refs
        captured["kwargs"] = kwargs
        oriented = kwargs["apply_func_orientation_func"](np.asarray([[1, 2], [3, 4]], dtype=np.uint16))
        captured["oriented"] = oriented
        return (
            pd.DataFrame(
                [
                    {
                        "fish_id": fish_dir.name,
                        "plane": f"{fish_dir.name}_plane0_mcorrected_flipX",
                        "plane_idx": 0,
                        "best_z": 0,
                        "func_label": 1,
                        "roi_idx": 0,
                        "selected_anat_label": 7,
                        "has_unique_anat_match": True,
                        "anat_label": 7,
                    }
                ]
            ),
            pd.DataFrame([{"plane_idx": 0, "status": "ok"}]),
        )

    config = SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True, pipeline_root=tmp_path / "staged")
    with patch("codeants_2pf_hcr.matching.build_functional_roi_master_df", side_effect=fake_build_functional_roi_master_df):
        manifest = run_match_roi_to_anatomy_stage(
            config,
            plane_refs_summary_path=plane_summary,
            anatomy_labels_path=anatomy_labels,
            force_recompute=True,
        )

    assert manifest.status == "pass"
    passed_refs = captured["plane_refs"]
    passed_kwargs = captured["kwargs"]
    assert passed_refs[0]["tform_src"] == "ants_rigid_affine"
    assert passed_refs[0]["ants_transform"]["transformlist"] == [str(transform_path)]
    assert passed_kwargs["dx_um"] == 0.5
    assert passed_kwargs["dy_um"] == 0.75
    assert callable(passed_kwargs["apply_func_orientation_func"])
    assert captured["oriented"].tolist() == [[2, 1], [4, 3]]
    assert manifest.parameters["selected_ants_overlay_count"] == 1
    assert manifest.parameters["selected_ants_missing_transform_files"] == 0
    assert manifest.parameters["anatomy_xy_spacing_um"] == (0.5, 0.75)


def test_match_roi_to_anatomy_stage_stages_control_geometry_only(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    source_root = tmp_path / "control-registration"
    _write_control_roi_identity_csv(source_root)
    config = SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True, pipeline_root=tmp_path / "staged")

    manifest = run_match_roi_to_anatomy_stage(config, source_root=source_root, force_recompute=True)

    out_root = roi_to_anatomy_match_root(resolve_pipeline_paths(config)) / "registration"
    detail_path = out_root / "functional_roi_anatomy_matches.csv"
    summary_path = out_root / "functional_roi_anatomy_match_by_plane.csv"
    meta_path = out_root / "functional_roi_anatomy_match_plane_meta.csv"
    assert manifest.status == "pass"
    assert detail_path.exists()
    assert summary_path.exists()
    assert meta_path.exists()
    header = detail_path.read_text().splitlines()[0].split(",")
    assert "selected_anat_label" in header
    assert "identity_label" not in header
    assert "has_identity_assigned" not in header
    assert "response_class" not in header
    assert "bpi" not in header
    assert "gene" not in header
    assert manifest.parameters["mode"] == "control_geometry"
    assert any(check.label == "geometry-only columns" and check.status == "pass" for check in manifest.checks)


def test_match_roi_to_anatomy_stage_refuses_existing_outputs_without_force(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    source_root = tmp_path / "control-registration"
    _write_control_roi_identity_csv(source_root)
    config = SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True, pipeline_root=tmp_path / "staged")

    first = run_match_roi_to_anatomy_stage(config, source_root=source_root, force_recompute=True)
    second = run_match_roi_to_anatomy_stage(config, source_root=source_root)

    assert first.status == "pass"
    assert second.status == "fail"
    assert "pass --force-recompute" in second.errors[0]


def test_match_roi_to_anatomy_stage_requires_control_geometry_columns(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    source_root = tmp_path / "control-registration"
    source_root.mkdir()
    (source_root / "functional_roi_activity_identity.csv").write_text("plane_idx,func_label\n0,roi_0001\n")
    config = SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True, pipeline_root=tmp_path / "staged")

    manifest = run_match_roi_to_anatomy_stage(config, source_root=source_root, force_recompute=True)

    assert manifest.status == "fail"
    assert "missing geometry columns" in manifest.errors[0]


def test_single_fish_pipeline_cli_match_roi_to_anatomy_outputs_manifest(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    source_root = tmp_path / "control-registration"
    _write_control_roi_identity_csv(source_root)
    pipeline_root = tmp_path / "staged"

    result = subprocess.run(
        [
            sys.executable,
            "tools/single_fish_pipeline.py",
            "match-roi-to-anatomy",
            "--fish-id",
            fish_dir.name,
            "--local-root",
            str(tmp_path),
            "--strict",
            "--pipeline-root",
            str(pipeline_root),
            "--source-root",
            str(source_root),
            "--force-recompute",
        ],
        cwd=REPO_ROOT,
        env={"PYTHONPATH": "src"},
        text=True,
        capture_output=True,
        check=False,
    )
    payload = json.loads(result.stdout)
    assert result.returncode == 0
    assert payload["stage_name"] == "match-roi-to-anatomy"
    assert payload["status"] == "pass"
    assert any(output["label"] == "staged ROI/anatomy geometry matches" for output in payload["outputs"])


def test_prepare_in_vivo_anatomy_stack_stage_writes_manifest_outputs(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    (fish_dir / "01_raw" / "2p" / "metadata" / f"{fish_dir.name}_metadata.csv").write_text(
        "parameter,value\nfish_orientation,bottom-left\n"
    )
    source = fish_dir / "01_raw" / "2p" / "anatomy" / f"{fish_dir.name}_anatomy_00001.tif"
    _write_tiny_ex_vivo_tiff(source)
    output = tmp_path / "custom-prepared" / f"{fish_dir.name}_anatomy_2P_GCaMP.nrrd"

    manifest = run_prepare_in_vivo_anatomy_stack_stage(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True),
        anatomy_stack_path=source,
        output_path=output,
        force_recompute=True,
    )

    assert manifest.status == "pass"
    assert output.exists()
    assert Path(str(output) + ".json").exists()
    assert manifest.inputs[0].path == str(source)
    assert manifest.outputs[0].path == str(output)
    assert any(check.label == "in vivo anatomy output Y/X" and check.status == "pass" for check in manifest.checks)


def test_prepare_in_vivo_anatomy_stack_stage_default_discovery_ignores_ex_vivo_sources(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    (fish_dir / "01_raw" / "2p" / "metadata" / f"{fish_dir.name}_metadata.csv").write_text(
        "parameter,value\nfish_orientation,bottom-left\n"
    )
    anatomy_dir = fish_dir / "01_raw" / "2p" / "anatomy"
    ex_vivo_source = anatomy_dir / f"{fish_dir.name}_anatomy_ex_vivo_00001.tif"
    in_vivo_source = anatomy_dir / f"{fish_dir.name}_anatomy_00001.tif"
    _write_tiny_ex_vivo_tiff(ex_vivo_source)
    _write_tiny_ex_vivo_tiff(in_vivo_source)
    output = tmp_path / "custom-prepared" / f"{fish_dir.name}_anatomy_2P_GCaMP.nrrd"

    manifest = run_prepare_in_vivo_anatomy_stack_stage(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True),
        output_path=output,
        force_recompute=True,
    )

    assert manifest.status == "pass"
    assert manifest.inputs[0].path == str(in_vivo_source)


def test_single_fish_pipeline_cli_prepare_in_vivo_outputs_manifest(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    (fish_dir / "01_raw" / "2p" / "metadata" / f"{fish_dir.name}_metadata.csv").write_text(
        "parameter,value\nfish_orientation,top-right\n"
    )
    source = fish_dir / "01_raw" / "2p" / "anatomy" / f"{fish_dir.name}_anatomy_00001.tif"
    _write_tiny_ex_vivo_tiff(source)
    output = tmp_path / "custom-prepared" / f"{fish_dir.name}_anatomy_2P_GCaMP.nrrd"

    result = subprocess.run(
        [
            sys.executable,
            "tools/single_fish_pipeline.py",
            "prepare-in-vivo-anatomy-stack",
            "--fish-id",
            fish_dir.name,
            "--local-root",
            str(tmp_path),
            "--strict",
            "--anatomy-stack-path",
            str(source),
            "--output-path",
            str(output),
            "--force-recompute",
        ],
        cwd=REPO_ROOT,
        env={"PYTHONPATH": "src"},
        text=True,
        capture_output=True,
        check=False,
    )
    payload = json.loads(result.stdout)
    assert result.returncode == 0
    assert payload["stage_name"] == "prepare-in-vivo-anatomy-stack"
    assert payload["status"] == "pass"
    assert payload["outputs"][0]["path"] == str(output)


def test_ex_vivo_stage_paths_are_structural_ex_vivo_scoped(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    paths = resolve_pipeline_paths(SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path))
    assert prepared_in_vivo_anatomy_path(paths) == (
        fish_dir / "02_reg" / "00_preprocessing" / "2p_anatomy" / f"{fish_dir.name}_anatomy_2P_GCaMP.nrrd"
    )
    assert ex_vivo_structural_root(paths) == fish_dir / "03_analysis" / "structural" / "ex_vivo"
    assert prepared_ex_vivo_anatomy_path(paths) == (
        fish_dir / "03_analysis" / "structural" / "ex_vivo" / "prepared" / f"{fish_dir.name}_exvivo_anatomy_2P_GCaMP_uint8.nrrd"
    )
    assert cellpose_stage_manifest_path(paths, "segment-hcr-cellpose") == (
        fish_dir / "03_analysis" / "confocal" / "raw" / "manifests" / "segment-hcr-cellpose_manifest.json"
    )
    assert cellpose_stage_manifest_path(paths, "segment-ex-vivo-anatomy-cellpose") == (
        fish_dir / "03_analysis" / "structural" / "ex_vivo" / "manifests" / "segment-ex-vivo-anatomy-cellpose_manifest.json"
    )


def test_discover_ex_vivo_anatomy_stack_requires_explicit_choice_when_ambiguous(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    anatomy_dir = fish_dir / "01_raw" / "2p" / "anatomy"
    ex_vivo = anatomy_dir / f"{fish_dir.name}_anatomy_ex_vivo_00001.tif"
    ex_vivo.write_bytes(b"tif")
    paths = resolve_pipeline_paths(SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path))
    assert discover_ex_vivo_anatomy_stack(paths) == ex_vivo
    (anatomy_dir / f"{fish_dir.name}_second_ex_vivo_00001.tif").write_bytes(b"tif")
    try:
        discover_ex_vivo_anatomy_stack(paths)
    except RuntimeError as exc:
        assert "--ex-vivo-stack-path" in str(exc)
    else:
        raise AssertionError("ambiguous ex vivo stacks should require an explicit path")


def test_prepare_ex_vivo_anatomy_stack_stage_writes_structural_outputs(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    source = fish_dir / "01_raw" / "2p" / "anatomy" / f"{fish_dir.name}_anatomy_ex_vivo_00001.tif"
    _write_tiny_ex_vivo_tiff(source)
    output = tmp_path / "custom-prepared" / f"{fish_dir.name}_manual_prepared.nrrd"

    manifest = run_prepare_ex_vivo_anatomy_stack_stage(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True),
        ex_vivo_stack_path=source,
        output_path=output,
    )

    assert manifest.status == "pass"
    assert output.exists()
    assert Path(str(output) + ".json").exists()
    assert manifest.outputs[0].path == str(output)
    assert any(check.label == "prepared ex vivo anatomy stack" and check.status == "pass" for check in manifest.checks)


def test_prepare_ex_vivo_anatomy_stack_stage_uses_cached_output_without_force(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    source = fish_dir / "01_raw" / "2p" / "anatomy" / f"{fish_dir.name}_anatomy_ex_vivo_00001.tif"
    _write_tiny_ex_vivo_tiff(source)
    output = tmp_path / "custom-prepared" / f"{fish_dir.name}_manual_prepared.nrrd"
    first = run_prepare_ex_vivo_anatomy_stack_stage(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True),
        ex_vivo_stack_path=source,
        output_path=output,
    )
    assert first.status == "pass"
    before_mtime = output.stat().st_mtime_ns

    second = run_prepare_ex_vivo_anatomy_stack_stage(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True),
        ex_vivo_stack_path=source,
        output_path=output,
    )

    assert second.status == "pass"
    assert output.stat().st_mtime_ns == before_mtime
    assert second.parameters is not None
    assert any("Using existing preprocessed" in line for line in second.parameters["log_lines"])


def test_single_fish_pipeline_cli_prepare_ex_vivo_outputs_manifest(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    source = fish_dir / "01_raw" / "2p" / "anatomy" / f"{fish_dir.name}_anatomy_ex_vivo_00001.tif"
    _write_tiny_ex_vivo_tiff(source)
    output = tmp_path / "custom-prepared" / f"{fish_dir.name}_manual_prepared.nrrd"

    result = subprocess.run(
        [
            sys.executable,
            "tools/single_fish_pipeline.py",
            "prepare-ex-vivo-anatomy-stack",
            "--fish-id",
            fish_dir.name,
            "--local-root",
            str(tmp_path),
            "--strict",
            "--ex-vivo-stack-path",
            str(source),
            "--output-path",
            str(output),
        ],
        cwd=REPO_ROOT,
        env={"PYTHONPATH": "src"},
        text=True,
        capture_output=True,
        check=False,
    )
    payload = json.loads(result.stdout)
    assert result.returncode == 0
    assert payload["stage_name"] == "prepare-ex-vivo-anatomy-stack"
    assert payload["status"] == "pass"
    assert payload["outputs"][0]["path"] == str(output)


def test_single_fish_pipeline_cli_prepare_ex_vivo_reports_missing_source_as_manifest(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)

    result = subprocess.run(
        [
            sys.executable,
            "tools/single_fish_pipeline.py",
            "prepare-ex-vivo-anatomy-stack",
            "--fish-id",
            fish_dir.name,
            "--local-root",
            str(tmp_path),
            "--strict",
        ],
        cwd=REPO_ROOT,
        env={"PYTHONPATH": "src"},
        text=True,
        capture_output=True,
        check=False,
    )

    payload = json.loads(result.stdout)
    assert result.returncode == 1
    assert payload["stage_name"] == "prepare-ex-vivo-anatomy-stack"
    assert payload["status"] == "fail"
    assert "No ex vivo anatomy stack found" in payload["errors"][0]
    assert "Traceback" not in result.stderr


def test_segment_ex_vivo_anatomy_cellpose_stage_reports_missing_inputs_as_manifest_failure(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    missing_stack = tmp_path / "missing-prepared.nrrd"
    missing_model = tmp_path / "missing-model"

    manifest = run_segment_ex_vivo_anatomy_cellpose_stage(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True),
        anatomy_stack_path=missing_stack,
        anat_cp_model_path=missing_model,
        use_gpu=False,
    )

    assert manifest.status == "fail"
    assert manifest.errors
    assert manifest.inputs[0].path == str(missing_stack)
    assert manifest.inputs[0].exists is False
    assert manifest.inputs[1].path == str(missing_model)
    assert manifest.outputs[0].label == "ex vivo anatomy Cellpose mask directory"


def test_segment_hcr_cellpose_stage_can_reuse_cached_masks_without_model(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    missing_model = tmp_path / "missing-hcr-model"

    manifest = run_segment_hcr_cellpose_stage(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True),
        hcr_source="rbest",
        cp_hcr_model_path=missing_model,
        use_gpu=False,
    )

    assert manifest.status == "pass"
    assert manifest.errors == ()
    assert manifest.inputs[0].label == "HCR rbest intensity directory"
    assert manifest.inputs[1].path == str(missing_model)
    assert manifest.inputs[1].exists is False
    assert any(output.exists for output in manifest.outputs)


def test_segment_hcr_cellpose_stage_reports_missing_model_without_cached_masks(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    missing_model = tmp_path / "missing-hcr-model"
    for mask_path in (fish_dir / "03_analysis" / "confocal" / "raw" / "cp_masks").glob("*"):
        mask_path.unlink()

    manifest = run_segment_hcr_cellpose_stage(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True),
        hcr_source="rbest",
        cp_hcr_model_path=missing_model,
        use_gpu=False,
    )

    assert manifest.status == "fail"
    assert manifest.errors
    assert manifest.inputs[1].path == str(missing_model)
    assert manifest.inputs[1].exists is False


def test_single_fish_pipeline_cli_segment_hcr_cellpose_outputs_cached_manifest(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    missing_model = tmp_path / "missing-hcr-model"

    result = subprocess.run(
        [
            sys.executable,
            "tools/single_fish_pipeline.py",
            "segment-hcr-cellpose",
            "--fish-id",
            fish_dir.name,
            "--local-root",
            str(tmp_path),
            "--strict",
            "--cp-hcr-model-path",
            str(missing_model),
            "--no-gpu",
        ],
        cwd=REPO_ROOT,
        env={"PYTHONPATH": "src"},
        text=True,
        capture_output=True,
        check=False,
    )

    payload = json.loads(result.stdout)
    assert result.returncode == 0
    assert payload["stage_name"] == "segment-hcr-cellpose"
    assert payload["status"] == "pass"
    assert payload["parameters"]["use_gpu"] is False


def test_audit_inputs_is_read_only_and_passes_on_minimal_fish(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    pipeline_root = tmp_path / "dry-run-output-root"
    before = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))
    manifest = run_single_fish_audit_inputs_stage(
        SingleFishPipelineConfig(
            fish_id=fish_dir.name,
            local_root=tmp_path,
            strict=True,
            pipeline_root=pipeline_root,
        )
    )
    after = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))
    assert before == after
    assert manifest.stage_name == "audit-inputs"
    assert manifest.status == "pass"
    assert manifest.dry_run is True
    assert manifest.errors == ()
    assert any(check.label == "Suite2p plane directories" and check.status == "pass" for check in manifest.checks)
    assert any(check.label == "tforms rows vs Suite2p planes" and check.observed == "1" for check in manifest.checks)
    assert any(
        check.label == "CSV schema: functional_roi_activity_identity.csv" and check.status == "pass"
        for check in manifest.checks
    )
    assert any(check.label == "BPI cell keys match ROI master keys" and check.status == "pass" for check in manifest.checks)
    assert any(
        check.label == "responsive pair keys subset ROI master keys" and check.status == "pass"
        for check in manifest.checks
    )
    assert any(
        check.label == "HCR trace export selections match responsive pairs" and check.status == "pass"
        for check in manifest.checks
    )
    assert any(check.label == "responsive pairs are active selected rows" and check.status == "pass" for check in manifest.checks)
    assert manifest.parameters is not None
    assert manifest.parameters["pipeline_root"] == str(pipeline_root)
    assert not pipeline_root.exists()
    metadata_record = next(record for record in manifest.inputs if record.label == "raw 2p metadata CSVs")
    assert metadata_record.count == 1
    assert metadata_record.mtime is not None
    raw_functional_record = next(record for record in manifest.inputs if record.label == "raw 2p functional files")
    assert raw_functional_record.count == 0


def test_audit_inputs_strict_fails_missing_required_inputs(tmp_path: Path) -> None:
    manifest = run_single_fish_audit_inputs_stage(
        SingleFishPipelineConfig(fish_id="missing_fish", local_root=tmp_path, strict=True)
    )
    assert manifest.status == "fail"
    assert any("missing_fish" in path for path in manifest.errors)


def test_status_summarizes_dry_run_audit(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    status = build_single_fish_status(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True)
    )
    assert status["status"] == "pass"
    assert status["dry_run"] is True
    assert status["input_records"] > 0
    assert status["check_records"] > 0
    assert status["missing_required_inputs"] == []
    assert status["failed_checks"] == []
    assert status["persisted_manifests"][0]["status"] == "missing"
    assert status["stale_records"] == []


def test_manifest_write_is_explicit_and_status_reports_current(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    config = SingleFishPipelineConfig(
        fish_id=fish_dir.name,
        local_root=tmp_path,
        strict=True,
        dry_run=True,
        write_manifest=True,
    )
    manifest = run_single_fish_audit_inputs_stage(config)
    manifest_path = write_stage_manifest(manifest, resolve_pipeline_paths(config))
    assert manifest_path == stage_manifest_path(resolve_pipeline_paths(config), "audit-inputs")
    assert manifest_path.exists()
    assert manifest.dry_run is True
    assert manifest.parameters is not None
    assert manifest.parameters["write_manifest"] is True
    status = build_single_fish_status(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True)
    )
    assert status["status"] == "pass"
    assert status["persisted_manifests"][0]["status"] == "current"
    assert status["stale_records"] == []


def test_status_reports_stale_manifest_when_inputs_change(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    write_config = SingleFishPipelineConfig(
        fish_id=fish_dir.name,
        local_root=tmp_path,
        strict=True,
        dry_run=True,
        write_manifest=True,
    )
    write_stage_manifest(run_single_fish_audit_inputs_stage(write_config), resolve_pipeline_paths(write_config))
    metadata_path = fish_dir / "01_raw" / "2p" / "metadata" / f"{fish_dir.name}_metadata.csv"
    metadata_path.write_text("key,value\nnew,row\n")
    status = build_single_fish_status(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True)
    )
    assert status["status"] == "warn"
    assert status["persisted_manifests"][0]["status"] == "stale"
    assert "inputs.raw 2p metadata CSVs" in status["stale_records"]


def test_status_fails_on_invalid_persisted_manifest_json(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    config = SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True)
    manifest_path = stage_manifest_path(resolve_pipeline_paths(config), "audit-inputs")
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text("{not json\n")
    status = build_single_fish_status(config)
    assert status["status"] == "fail"
    assert status["persisted_manifests"][0]["status"] == "fail"
    assert "invalid JSON" in status["stale_records"][0]


def test_audit_inputs_flags_staged_canonical_csv_parity_mismatch_when_baseline_present(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    staged_dir = (
        fish_dir
        / "03_analysis"
        / "functional"
        / "pipeline_outputs"
        / "export-canonical-tables"
        / "registration"
    )
    staged_dir.mkdir(parents=True)
    (staged_dir / "functional_roi_activity_identity.csv").write_text("unexpected\n1\n")
    manifest = run_single_fish_audit_inputs_stage(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True)
    )
    check = next(
        item
        for item in manifest.checks
        if item.label == "staged canonical CSV parity: functional_roi_activity_identity.csv"
    )
    assert check.status == "fail"
    assert manifest.status == "fail"


def test_downstream_stage_manifests_are_read_only_and_pass_when_outputs_exist(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    _make_minimal_staged_outputs(fish_dir)
    before = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))
    manifests = build_single_fish_downstream_stage_manifests(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True)
    )
    after = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))
    assert before == after
    assert tuple(manifest.stage_name for manifest in manifests) == downstream_stage_names()
    assert all(manifest.status == "pass" for manifest in manifests)
    assert all(manifest.outputs for manifest in manifests)
    assert all(any(check.label.startswith("required output exists") for check in manifest.checks) for manifest in manifests)


def test_downstream_stage_manifest_fails_missing_required_stage_outputs(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    _make_minimal_staged_outputs(fish_dir)
    missing_path = (
        fish_dir
        / "03_analysis"
        / "functional"
        / "pipeline_outputs"
        / "export-canonical-tables"
        / "registration"
        / "hcr_func_candidates.csv"
    )
    missing_path.unlink()
    manifest = build_single_fish_downstream_stage_manifest(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True),
        "export-canonical-tables",
    )
    assert manifest.status == "fail"
    assert any(record.label == "staged canonical export: hcr_func_candidates.csv" for record in manifest.outputs)
    assert any("hcr_func_candidates.csv" in error for error in manifest.errors)


def test_status_includes_downstream_stage_summaries(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    _make_minimal_staged_outputs(fish_dir)
    status = build_single_fish_status(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True)
    )
    assert status["status"] == "pass"
    stage_summaries = {stage["stage_name"]: stage for stage in status["downstream_stages"]}
    assert set(stage_summaries) == set(downstream_stage_names())
    assert stage_summaries["export-canonical-tables"]["status"] == "pass"
    assert stage_summaries["export-canonical-tables"]["output_records"] == 8


def test_status_fails_when_existing_downstream_stage_is_incomplete(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    incomplete_stage_dir = (
        fish_dir
        / "03_analysis"
        / "functional"
        / "pipeline_outputs"
        / "export-canonical-tables"
        / "registration"
    )
    incomplete_stage_dir.mkdir(parents=True)
    (incomplete_stage_dir / "functional_roi_activity_identity.csv").write_text("unexpected\n1\n")
    status = build_single_fish_status(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True)
    )
    assert status["status"] == "fail"
    stage_summary = next(stage for stage in status["downstream_stages"] if stage["stage_name"] == "export-canonical-tables")
    assert stage_summary["status"] == "fail"
    assert "staged canonical export: hcr_func_candidates.csv" in stage_summary["missing_required_outputs"]


def test_stage_status_reports_not_started_without_affecting_global_status(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    status = build_single_fish_status(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True)
    )
    assert status["status"] == "pass"
    assert {stage["status"] for stage in status["downstream_stages"]} == {"not_started"}


def test_single_fish_stage_status_reports_current_after_manifest_write(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    _make_minimal_staged_outputs(fish_dir)
    config = SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True)
    manifest = build_single_fish_downstream_stage_manifest(config, "make-figures")
    write_stage_manifest(manifest, resolve_pipeline_paths(config))
    stage_status = build_single_fish_stage_status(config, "make-figures")
    assert stage_status["status"] == "pass"
    assert stage_status["persisted_manifest"]["status"] == "current"


def test_compare_staged_is_read_only_and_passes_on_minimal_staged_outputs(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    _make_minimal_staged_outputs(fish_dir)
    before = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))
    payload = compare_single_fish_staged_outputs(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True)
    )
    after = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))
    assert before == after
    assert payload["status"] == "pass"
    assert [item["stage_name"] for item in payload["comparisons"]] == list(downstream_stage_names())
    assert all(item["failed_checks"] == [] for item in payload["comparisons"])


def test_compare_staged_reports_csv_shape_mismatch(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    _make_minimal_staged_outputs(fish_dir)
    staged_path = (
        fish_dir
        / "03_analysis"
        / "functional"
        / "pipeline_outputs"
        / "export-canonical-tables"
        / "registration"
        / "functional_roi_activity_identity.csv"
    )
    staged_path.write_text("unexpected\n1\n")
    payload = compare_single_fish_staged_outputs(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True),
        "export-canonical-tables",
    )
    assert payload["status"] == "fail"
    comparison = payload["comparisons"][0]
    assert comparison["stage_name"] == "export-canonical-tables"
    assert any("comparison CSV shape" in label for label in comparison["failed_checks"])


def test_freeze_legacy_baseline_copies_declared_control_outputs(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    config = SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True)
    manifest = run_single_fish_freeze_legacy_baseline_stage(
        config,
        stage_name="make-figures",
    )
    baseline_root = tmp_path / "pipeline_baselines" / fish_dir.name / "legacy_singleFish"
    frozen_figure = (
        baseline_root
        / "stages"
        / "make-figures"
        / "04_plots"
        / "compound_50j_56i_unified.png"
    )
    assert manifest.status == "pass"
    assert manifest.parameters is not None
    assert manifest.parameters["copied_outputs"] == 5
    assert manifest.parameters["baseline_root"] == str(baseline_root)
    assert frozen_figure.read_bytes() == (fish_dir / "04_plots" / "compound_50j_56i_unified.png").read_bytes()


def test_freeze_legacy_baseline_refuses_overwrite_without_flag(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    config = SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True)
    first = run_single_fish_freeze_legacy_baseline_stage(config, stage_name="make-figures")
    assert first.status == "pass"
    frozen_figure = (
        tmp_path
        / "pipeline_baselines"
        / fish_dir.name
        / "legacy_singleFish"
        / "stages"
        / "make-figures"
        / "04_plots"
        / "compound_50j_56i_unified.png"
    )
    frozen_figure.write_text("sentinel\n")

    second = run_single_fish_freeze_legacy_baseline_stage(config, stage_name="make-figures")

    assert second.status == "fail"
    assert frozen_figure.read_text() == "sentinel\n"
    assert any(check.label.startswith("freeze legacy output gate") and check.status == "fail" for check in second.checks)


def test_compare_legacy_baseline_reports_staged_drift(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    _make_minimal_staged_outputs(fish_dir)
    config = SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True)
    freeze = run_single_fish_freeze_legacy_baseline_stage(config, stage_name="export-canonical-tables")
    assert freeze.status == "pass"
    staged_csv = (
        fish_dir
        / "03_analysis"
        / "functional"
        / "pipeline_outputs"
        / "export-canonical-tables"
        / "registration"
        / "functional_roi_activity_identity.csv"
    )
    staged_csv.write_bytes(staged_csv.read_text().replace("\n", "\r\n").encode())

    manifest = build_single_fish_compare_legacy_baseline_manifest(config, "export-canonical-tables")
    payload = compare_single_fish_legacy_baseline(config, "export-canonical-tables")

    assert manifest.status == "warn"
    assert any(
        check.label == "legacy baseline CSV byte parity: staged canonical export: functional_roi_activity_identity.csv"
        and check.status == "warn"
        for check in manifest.checks
    )
    assert payload["status"] == "warn"
    assert payload["baseline_root"].endswith(f"pipeline_baselines/{fish_dir.name}/legacy_singleFish")


def test_compare_staged_allows_keyed_row_reorder_with_byte_warning(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    _make_minimal_staged_outputs(fish_dir)
    control_path = fish_dir / "03_analysis" / "functional" / "registration" / "functional_roi_activity_identity.csv"
    staged_path = (
        fish_dir
        / "03_analysis"
        / "functional"
        / "pipeline_outputs"
        / "export-canonical-tables"
        / "registration"
        / "functional_roi_activity_identity.csv"
    )
    rows = [_roi_identity_row("1"), _roi_identity_row("2")]
    _write_roi_identity_csv(control_path, rows)
    _write_roi_identity_csv(staged_path, list(reversed(rows)))
    manifest = build_single_fish_compare_staged_manifest(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True),
        "export-canonical-tables",
    )
    assert not any(check.status == "fail" for check in manifest.checks)
    assert any(check.label == "comparison CSV byte parity: staged canonical export: functional_roi_activity_identity.csv" and check.status == "warn" for check in manifest.checks)
    assert any(check.label == "comparison CSV keyed rows: staged canonical export: functional_roi_activity_identity.csv" and check.status == "pass" for check in manifest.checks)


def test_compare_staged_fails_missing_and_extra_keyed_rows(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    _make_minimal_staged_outputs(fish_dir)
    control_path = fish_dir / "03_analysis" / "functional" / "registration" / "functional_roi_activity_identity.csv"
    staged_path = (
        fish_dir
        / "03_analysis"
        / "functional"
        / "pipeline_outputs"
        / "export-canonical-tables"
        / "registration"
        / "functional_roi_activity_identity.csv"
    )
    _write_roi_identity_csv(control_path, [_roi_identity_row("1"), _roi_identity_row("2")])
    _write_roi_identity_csv(staged_path, [_roi_identity_row("1"), _roi_identity_row("3")])
    manifest = build_single_fish_compare_staged_manifest(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True),
        "export-canonical-tables",
    )
    check = next(
        item
        for item in manifest.checks
        if item.label == "comparison CSV keyed rows: staged canonical export: functional_roi_activity_identity.csv"
    )
    assert check.status == "fail"
    assert check.observed == "missing=1; extra=1; keys=2"
    assert manifest.status == "fail"


def test_compare_staged_non_strict_reports_keyed_row_failure_as_warning(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    _make_minimal_staged_outputs(fish_dir)
    control_path = fish_dir / "03_analysis" / "functional" / "registration" / "functional_roi_activity_identity.csv"
    staged_path = (
        fish_dir
        / "03_analysis"
        / "functional"
        / "pipeline_outputs"
        / "export-canonical-tables"
        / "registration"
        / "functional_roi_activity_identity.csv"
    )
    _write_roi_identity_csv(control_path, [_roi_identity_row("1"), _roi_identity_row("2")])
    _write_roi_identity_csv(staged_path, [_roi_identity_row("1"), _roi_identity_row("3")])
    manifest = build_single_fish_compare_staged_manifest(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=False),
        "export-canonical-tables",
    )
    assert manifest.status == "warn"
    assert manifest.errors == ()
    assert any("comparison CSV keyed rows" in warning for warning in manifest.warnings)


def test_compare_staged_fails_duplicate_keyed_rows(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    _make_minimal_staged_outputs(fish_dir)
    control_path = fish_dir / "03_analysis" / "functional" / "registration" / "functional_roi_activity_identity.csv"
    staged_path = (
        fish_dir
        / "03_analysis"
        / "functional"
        / "pipeline_outputs"
        / "export-canonical-tables"
        / "registration"
        / "functional_roi_activity_identity.csv"
    )
    _write_roi_identity_csv(control_path, [_roi_identity_row("1"), _roi_identity_row("2")])
    _write_roi_identity_csv(staged_path, [_roi_identity_row("1"), _roi_identity_row("1")])
    manifest = build_single_fish_compare_staged_manifest(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True),
        "export-canonical-tables",
    )
    check = next(
        item
        for item in manifest.checks
        if item.label == "comparison CSV unique keys: staged canonical export: functional_roi_activity_identity.csv"
    )
    assert check.status == "fail"
    assert check.observed == "control=0; staged=1"


def test_compare_staged_fails_exact_cell_mismatch(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    _make_minimal_staged_outputs(fish_dir)
    control_path = fish_dir / "03_analysis" / "functional" / "registration" / "functional_roi_activity_identity.csv"
    staged_path = (
        fish_dir
        / "03_analysis"
        / "functional"
        / "pipeline_outputs"
        / "export-canonical-tables"
        / "registration"
        / "functional_roi_activity_identity.csv"
    )
    _write_roi_identity_csv(control_path, [_roi_identity_row("1")])
    _write_roi_identity_csv(staged_path, [_roi_identity_row("1", identity_label="pth2")])
    manifest = build_single_fish_compare_staged_manifest(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True),
        "export-canonical-tables",
    )
    check = next(
        item
        for item in manifest.checks
        if item.label == "comparison CSV exact cells: staged canonical export: functional_roi_activity_identity.csv"
    )
    assert check.status == "fail"
    assert check.observed == "1"


def test_compare_staged_numeric_tolerance_and_failures(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    _make_minimal_staged_outputs(fish_dir)
    control_path = fish_dir / "03_analysis" / "functional" / "registration" / "functional_roi_activity_identity.csv"
    staged_path = (
        fish_dir
        / "03_analysis"
        / "functional"
        / "pipeline_outputs"
        / "export-canonical-tables"
        / "registration"
        / "functional_roi_activity_identity.csv"
    )
    _write_roi_identity_csv(control_path, [_roi_identity_row("1", bpi="1.0")])
    _write_roi_identity_csv(staged_path, [_roi_identity_row("1", bpi="1.0000004")])
    passing = build_single_fish_compare_staged_manifest(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True),
        "export-canonical-tables",
    )
    passing_check = next(
        item
        for item in passing.checks
        if item.label == "comparison CSV numeric cells: staged canonical export: functional_roi_activity_identity.csv"
    )
    assert passing_check.status == "pass"

    _write_roi_identity_csv(staged_path, [_roi_identity_row("1", bpi="1.01")])
    failing = build_single_fish_compare_staged_manifest(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True),
        "export-canonical-tables",
    )
    failing_check = next(
        item
        for item in failing.checks
        if item.label == "comparison CSV numeric cells: staged canonical export: functional_roi_activity_identity.csv"
    )
    assert failing_check.status == "fail"
    assert "mismatches=1" in str(failing_check.observed)


def test_compare_staged_fails_non_numeric_declared_numeric_cell(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    _make_minimal_staged_outputs(fish_dir)
    control_path = fish_dir / "03_analysis" / "functional" / "registration" / "functional_roi_activity_identity.csv"
    staged_path = (
        fish_dir
        / "03_analysis"
        / "functional"
        / "pipeline_outputs"
        / "export-canonical-tables"
        / "registration"
        / "functional_roi_activity_identity.csv"
    )
    _write_roi_identity_csv(control_path, [_roi_identity_row("1", bpi="")])
    _write_roi_identity_csv(staged_path, [_roi_identity_row("1", bpi="not-a-number")])
    manifest = build_single_fish_compare_staged_manifest(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True),
        "export-canonical-tables",
    )
    check = next(
        item
        for item in manifest.checks
        if item.label == "comparison CSV numeric cells: staged canonical export: functional_roi_activity_identity.csv"
    )
    assert check.status == "fail"
    assert "invalid=1" in str(check.observed)


def test_compare_staged_specific_stage_can_fail_strict(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    _make_minimal_staged_outputs(fish_dir)
    staged_path = (
        fish_dir
        / "03_analysis"
        / "functional"
        / "pipeline_outputs"
        / "make-figures"
        / "04_plots"
        / "bpi_all_pairs.png"
    )
    staged_path.write_bytes(b"")
    manifest = build_single_fish_compare_staged_manifest(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True),
        "make-figures",
    )
    assert manifest.status == "fail"
    assert any("bpi_all_pairs.png" in error for error in manifest.errors)


def test_compare_staged_reports_visual_thumbnail_checks_for_readable_figures(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    _make_minimal_staged_outputs(fish_dir)
    control_path = fish_dir / "04_plots" / "bpi_all_pairs.png"
    staged_path = (
        fish_dir
        / "03_analysis"
        / "functional"
        / "pipeline_outputs"
        / "make-figures"
        / "04_plots"
        / "bpi_all_pairs.png"
    )
    _write_rgb_png(control_path)
    _write_rgb_png(staged_path)
    manifest = build_single_fish_compare_staged_manifest(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True),
        "make-figures",
    )
    dimension_check = next(
        item
        for item in manifest.checks
        if item.label == "comparison figure dimensions: staged figure: bpi_all_pairs.png"
    )
    thumbnail_check = next(
        item
        for item in manifest.checks
        if item.label == "comparison figure thumbnail: staged figure: bpi_all_pairs.png"
    )
    assert dimension_check.status == "pass"
    assert thumbnail_check.status == "pass"
    assert "rms=0" in str(thumbnail_check.observed)


def test_compare_staged_reports_visual_dimension_drift_as_warning(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    _make_minimal_staged_outputs(fish_dir)
    control_path = fish_dir / "04_plots" / "bpi_all_pairs.png"
    staged_path = (
        fish_dir
        / "03_analysis"
        / "functional"
        / "pipeline_outputs"
        / "make-figures"
        / "04_plots"
        / "bpi_all_pairs.png"
    )
    _write_rgb_png(control_path, size=(12, 10), color=(10, 120, 200))
    _write_rgb_png(staged_path, size=(14, 10), color=(10, 120, 200))
    manifest = build_single_fish_compare_staged_manifest(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True),
        "make-figures",
    )
    dimension_check = next(
        item
        for item in manifest.checks
        if item.label == "comparison figure dimensions: staged figure: bpi_all_pairs.png"
    )
    assert dimension_check.status == "warn"
    assert manifest.status == "warn"
    assert manifest.errors == ()


def test_score_activity_bpi_recompute_audit_rejects_scored_only_identity_input(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    before = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))
    manifest = build_single_fish_score_activity_bpi_recompute_manifest(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True)
    )
    after = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))
    assert before == after
    assert manifest.stage_name == "audit-score-activity-bpi"
    assert manifest.status == "fail"
    assert any(
        check.label == "score-activity-bpi raw ROI inputs" and check.status == "fail"
        for check in manifest.checks
    )
    assert any("activity_class" in error and "is_active" in error for error in manifest.errors)
    assert manifest.parameters is not None
    assert manifest.parameters["uses_precomputed_scored_bpi_df"] is False


def test_score_activity_bpi_recompute_audit_compares_mocked_real_recompute(tmp_path: Path, monkeypatch) -> None:
    import pandas as pd

    fish_dir = _make_minimal_fish(tmp_path)
    registration_dir = fish_dir / "03_analysis" / "functional" / "registration"
    _write_recompute_identity_csv(registration_dir / "functional_roi_activity_identity.csv", [_roi_identity_row("1")])
    calls: list[dict[str, object]] = []

    def fake_build_response_bpi_tables(detail_df, **kwargs):
        calls.append(kwargs)
        scored_bpi_df = pd.DataFrame(
            [
                {
                    "plane_idx": 0,
                    "func_label": 1,
                    "anat_label": 1,
                    "response_is_active": True,
                    "response_class": "bout-responsive",
                    "response_summary_class": "Responsive neurons",
                    "bpi": 1.0,
                    "bpi_category": "bout-responsive",
                }
            ]
        )
        summary_df = pd.DataFrame(
            [
                {
                    "response_summary_class": "Responsive neurons",
                    "bpi_category": "bout-responsive",
                    "n_rois": 1,
                }
            ]
        )
        return {
            "detail_df": detail_df.copy(),
            "scored_bpi_df": scored_bpi_df,
            "summary_df": summary_df,
            "stim_source": str(fish_dir / "01_raw" / "2p" / "metadata" / f"{fish_dir.name}_experiment_log.csv"),
        }

    monkeypatch.setattr("codeants_2pf_hcr.activity.build_response_bpi_tables", fake_build_response_bpi_tables)
    before = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))
    manifest = build_single_fish_score_activity_bpi_recompute_manifest(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True)
    )
    after = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))
    assert before == after
    assert manifest.status == "pass"
    assert calls
    assert calls[0]["precomputed_scored_bpi_df"] is None
    assert calls[0]["suite2p_root"] == fish_dir / "03_analysis" / "functional" / "suite2P"
    assert any(
        check.label == "score-activity-bpi true recompute path" and check.status == "pass"
        for check in manifest.checks
    )
    assert any(
        check.label == "recompute numeric cells: recomputed ROI activity/BPI cells" and check.status == "pass"
        for check in manifest.checks
    )


def test_score_activity_bpi_writer_requires_staged_identity_by_default(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    output_root = tmp_path / "staged-output-root"
    manifest = run_single_fish_score_activity_bpi_stage(
        SingleFishPipelineConfig(
            fish_id=fish_dir.name,
            local_root=tmp_path,
            pipeline_root=output_root,
            strict=True,
        )
    )
    assert manifest.status == "fail"
    assert manifest.dry_run is False
    assert manifest.parameters is not None
    assert manifest.parameters["identity_input_is_explicit"] is False
    assert any("assign-hcr-identity" in error for error in manifest.errors)
    assert not output_root.exists()
    assert not (fish_dir / "03_analysis" / "functional" / "pipeline_outputs").exists()


def test_score_activity_bpi_writer_writes_recomputed_tables_to_custom_pipeline_root(
    tmp_path: Path,
    monkeypatch,
) -> None:
    import pandas as pd

    fish_dir = _make_minimal_fish(tmp_path)
    registration_dir = fish_dir / "03_analysis" / "functional" / "registration"
    identity_path = registration_dir / "functional_roi_activity_identity.csv"
    _write_recompute_identity_csv(identity_path, [_roi_identity_row("1")])
    output_root = tmp_path / "staged-output-root"
    calls: list[dict[str, object]] = []

    def fake_build_response_bpi_tables(detail_df, **kwargs):
        calls.append(kwargs)
        detail_out = detail_df.copy()
        detail_out["bpi"] = 1.0
        detail_out["bpi_category"] = "bout-responsive"
        scored_bpi_df = pd.DataFrame(
            [
                {
                    "plane_idx": 0,
                    "func_label": 1,
                    "anat_label": 1,
                    "response_is_active": True,
                    "response_class": "bout-responsive",
                    "response_summary_class": "Responsive neurons",
                    "bpi": 1.0,
                    "bpi_category": "bout-responsive",
                }
            ]
        )
        summary_df = pd.DataFrame(
            [
                {
                    "response_summary_class": "Responsive neurons",
                    "bpi_category": "bout-responsive",
                    "n_rois": 1,
                }
            ]
        )
        return {
            "detail_df": detail_out,
            "scored_bpi_df": scored_bpi_df,
            "summary_df": summary_df,
            "stim_source": str(fish_dir / "01_raw" / "2p" / "metadata" / f"{fish_dir.name}_experiment_log.csv"),
        }

    monkeypatch.setattr("codeants_2pf_hcr.activity.build_response_bpi_tables", fake_build_response_bpi_tables)
    manifest = run_single_fish_score_activity_bpi_stage(
        SingleFishPipelineConfig(
            fish_id=fish_dir.name,
            local_root=tmp_path,
            pipeline_root=output_root,
            strict=True,
        ),
        identity_input_path=identity_path,
    )
    score_dir = output_root / "score-activity-bpi" / "registration"
    assert manifest.status == "pass"
    assert calls
    assert calls[0]["precomputed_scored_bpi_df"] is None
    assert calls[0]["suite2p_root"] == fish_dir / "03_analysis" / "functional" / "suite2P"
    assert (score_dir / "functional_roi_activity_identity.csv").exists()
    assert (score_dir / "functional_roi_activity_bpi_cells.csv").exists()
    assert (score_dir / "functional_roi_activity_bpi_summary.csv").exists()
    assert not (fish_dir / "03_analysis" / "functional" / "pipeline_outputs").exists()
    payload = (score_dir / "functional_roi_activity_bpi_cells.csv").read_text()
    assert "bout-responsive" in payload
    assert any(
        check.label == "score-activity-bpi true recompute path" and check.status == "pass"
        for check in manifest.checks
    )


def test_score_activity_bpi_writer_refuses_existing_outputs_without_force(tmp_path: Path, monkeypatch) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    identity_path = fish_dir / "03_analysis" / "functional" / "registration" / "functional_roi_activity_identity.csv"
    _write_recompute_identity_csv(identity_path, [_roi_identity_row("1")])
    output_root = tmp_path / "staged-output-root"
    existing = output_root / "score-activity-bpi" / "registration" / "functional_roi_activity_identity.csv"
    existing.parent.mkdir(parents=True)
    existing.write_text("sentinel\n")

    def fail_if_called(*args, **kwargs):
        raise AssertionError("writer should not recompute when overwrite gate fails")

    monkeypatch.setattr("codeants_2pf_hcr.activity.build_response_bpi_tables", fail_if_called)
    manifest = run_single_fish_score_activity_bpi_stage(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, pipeline_root=output_root),
        identity_input_path=identity_path,
    )
    assert manifest.status == "fail"
    assert existing.read_text() == "sentinel\n"
    assert any(
        check.label == "score-activity-bpi overwrite gate" and check.status == "fail"
        for check in manifest.checks
    )


def test_single_fish_pipeline_cli_contracts_outputs_json() -> None:
    result = subprocess.run(
        [sys.executable, "tools/single_fish_pipeline.py", "contracts"],
        cwd=REPO_ROOT,
        env={"PYTHONPATH": "src"},
        text=True,
        capture_output=True,
        check=True,
    )
    payload = json.loads(result.stdout)
    assert [stage["name"] for stage in payload] == list(PIPELINE_STAGE_ORDER)


def test_single_fish_pipeline_cli_help_exposes_current_read_only_and_granular_writer_commands() -> None:
    result = subprocess.run(
        [sys.executable, "tools/single_fish_pipeline.py", "--help"],
        cwd=REPO_ROOT,
        env={"PYTHONPATH": "src"},
        text=True,
        capture_output=True,
        check=True,
    )
    assert "contracts" in result.stdout
    assert "compare-staged" in result.stdout
    assert "freeze-legacy-baseline" in result.stdout
    assert "compare-legacy-baseline" in result.stdout
    assert "audit-score-activity-bpi" in result.stdout
    assert "assign-hcr-identity" in result.stdout
    assert "score-activity-bpi" in result.stdout
    assert "export-canonical-tables" in result.stdout
    assert "make-qa-report" in result.stdout
    assert "make-figures" in result.stdout
    assert "prepare-functional-reference-stacks" in result.stdout
    assert "prepare-in-vivo-anatomy-stack" in result.stdout
    assert "register-functional-to-anatomy" in result.stdout
    assert "prepare-ex-vivo-anatomy-stack" in result.stdout
    assert "segment-ex-vivo-anatomy-cellpose" in result.stdout
    assert "segment-hcr-cellpose" in result.stdout
    for command in (
        "preprocess-functional",
    ):
        assert command not in result.stdout


def test_target_scaffold_commands_are_not_runnable_yet() -> None:
    for command in ("preprocess-functional",):
        result = subprocess.run(
            [sys.executable, "tools/single_fish_pipeline.py", command],
            cwd=REPO_ROOT,
            env={"PYTHONPATH": "src"},
            text=True,
            capture_output=True,
            check=False,
        )
        assert result.returncode == 2
        assert "invalid choice" in result.stderr


def test_assign_hcr_identity_writer_stages_baseline_identity_and_hcr_outputs(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    output_root = tmp_path / "staged-output-root"
    _write_staged_identity_geometry_dependencies(output_root, fish_dir.name)
    _write_assign_hcr_replay_inputs_and_controls(fish_dir, output_root)
    manifest = run_single_fish_assign_hcr_identity_stage(
        SingleFishPipelineConfig(
            fish_id=fish_dir.name,
            local_root=tmp_path,
            pipeline_root=output_root,
            strict=True,
        )
    )
    assign_dir = output_root / "assign-hcr-identity" / "registration"
    registration_dir = fish_dir / "03_analysis" / "functional" / "registration"
    assert manifest.status == "pass"
    assert manifest.dry_run is False
    assert len(tuple(assign_dir.glob("*.csv"))) == 7
    for filename in (
        "anatomy_identity_lookup.csv",
    ):
        assert (assign_dir / filename).read_bytes() == (registration_dir / filename).read_bytes()
    for filename in (
        "hcr_activity_status.csv",
        "hcr_activity_status_summary.csv",
        "conf_to_func_pairs_raw.csv",
        "conf_to_func_pairs.csv",
        "hcr_func_candidates.csv",
    ):
        assert (assign_dir / filename).exists()
        assert (assign_dir / filename).read_text()
    roi_identity_text = (assign_dir / "functional_roi_activity_identity.csv").read_text()
    assert "sst1.1" in roi_identity_text
    assert ",7," in roi_identity_text
    assert manifest.parameters is not None
    assert manifest.parameters["source_root_is_explicit"] is False
    assert manifest.parameters["roi_anatomy_root_is_explicit"] is False
    assert manifest.parameters["hcr_anatomy_root_is_explicit"] is False
    assert any(
        check.label == "assign-hcr-identity staged ROI/anatomy geometry" and check.status == "pass"
        for check in manifest.checks
    )
    assert any(
        check.label == "assign-hcr-identity staged HCR/anatomy final-pair acceptance" and check.status == "pass"
        for check in manifest.checks
    )
    assert any(
        check.label == "assign-hcr-identity recomputed anatomy identity lookup" and check.status == "pass"
        for check in manifest.checks
    )
    assert any(
        check.label == "assign-hcr-identity anatomy identity lookup parity" and check.status == "pass"
        for check in manifest.checks
    )
    assert any(
        check.label == "assign-hcr-identity recomputed ROI identity master" and check.status == "pass"
        for check in manifest.checks
    )
    assert any(
        check.label == "assign-hcr-identity ROI identity master parity" and check.status == "pass"
        for check in manifest.checks
    )
    assert any(
        check.label == "assign-hcr-identity recomputed HCR activity replay" and check.status == "pass"
        for check in manifest.checks
    )
    assert manifest.parameters["source_policy"].startswith(
        "functional_roi_activity_identity.csv and anatomy_identity_lookup.csv are recomputed"
    )
    assert not (fish_dir / "03_analysis" / "functional" / "pipeline_outputs").exists()


def test_assign_hcr_identity_writer_requires_staged_geometry_dependencies(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    output_root = tmp_path / "staged-output-root"

    manifest = run_single_fish_assign_hcr_identity_stage(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, pipeline_root=output_root, strict=True)
    )

    assert manifest.status == "fail"
    assert any("match-roi-to-anatomy" in error for error in manifest.errors)
    assert any("register-hcr-to-anatomy" in error for error in manifest.errors)
    assert any(
        check.label == "assign-hcr-identity staged ROI/anatomy geometry" and check.status == "fail"
        for check in manifest.checks
    )
    assert any(
        check.label == "assign-hcr-identity staged HCR/anatomy final pairs" and check.status == "fail"
        for check in manifest.checks
    )
    assert not (output_root / "assign-hcr-identity").exists()


def test_assign_hcr_identity_writer_refuses_existing_outputs_without_force(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    output_root = tmp_path / "staged-output-root"
    _write_staged_identity_geometry_dependencies(output_root, fish_dir.name)
    existing = output_root / "assign-hcr-identity" / "registration" / "functional_roi_activity_identity.csv"
    existing.parent.mkdir(parents=True)
    existing.write_text("sentinel\n")

    manifest = run_single_fish_assign_hcr_identity_stage(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, pipeline_root=output_root)
    )

    assert manifest.status == "fail"
    assert existing.read_text() == "sentinel\n"
    assert any(
        check.label == "assign-hcr-identity overwrite gate" and check.status == "fail"
        for check in manifest.checks
    )


def test_single_fish_pipeline_cli_assign_hcr_identity_outputs_manifest(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    output_root = tmp_path / "staged-output-root"
    _write_staged_identity_geometry_dependencies(output_root, fish_dir.name)
    _write_assign_hcr_replay_inputs_and_controls(fish_dir, output_root)
    result = subprocess.run(
        [
            sys.executable,
            "tools/single_fish_pipeline.py",
            "assign-hcr-identity",
            "--fish-id",
            fish_dir.name,
            "--local-root",
            str(tmp_path),
            "--strict",
            "--pipeline-root",
            str(output_root),
        ],
        cwd=REPO_ROOT,
        env={"PYTHONPATH": "src"},
        text=True,
        capture_output=True,
        check=False,
    )
    payload = json.loads(result.stdout)
    assert result.returncode == 0
    assert payload["stage_name"] == "assign-hcr-identity"
    assert payload["dry_run"] is False
    assert payload["parameters"]["pipeline_root"] == str(output_root)
    assert payload["parameters"]["roi_anatomy_root"] == str(output_root / "match-roi-to-anatomy" / "registration")
    assert payload["parameters"]["hcr_anatomy_root"] == str(output_root / "register-hcr-to-anatomy" / "confocal" / "aligned")
    assert len(payload["outputs"]) == 7
    assert not (fish_dir / "03_analysis" / "functional" / "pipeline_manifests").exists()


def test_export_canonical_tables_writer_requires_staged_upstream_roots_by_default(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    output_root = tmp_path / "staged-output-root"
    manifest = run_single_fish_export_canonical_tables_stage(
        SingleFishPipelineConfig(
            fish_id=fish_dir.name,
            local_root=tmp_path,
            pipeline_root=output_root,
            strict=True,
        )
    )
    assert manifest.status == "fail"
    assert manifest.dry_run is False
    assert manifest.parameters is not None
    assert manifest.parameters["score_input_root_is_explicit"] is False
    assert manifest.parameters["hcr_input_root_is_explicit"] is False
    assert any("score-activity-bpi" in error for error in manifest.errors)
    assert any("assign-hcr-identity" in error for error in manifest.errors)
    assert not output_root.exists()
    assert not (fish_dir / "03_analysis" / "functional" / "pipeline_outputs").exists()


def test_export_canonical_tables_writer_uses_staged_assign_root_by_default(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    output_root = tmp_path / "staged-output-root"
    _write_staged_identity_geometry_dependencies(output_root, fish_dir.name)
    _write_assign_hcr_replay_inputs_and_controls(fish_dir, output_root)
    assign_manifest = run_single_fish_assign_hcr_identity_stage(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, pipeline_root=output_root, strict=True)
    )
    assert assign_manifest.status == "pass"
    score_root = output_root / "score-activity-bpi" / "registration"
    registration_dir = fish_dir / "03_analysis" / "functional" / "registration"
    for filename in (
        "functional_roi_activity_identity.csv",
        "functional_roi_activity_bpi_cells.csv",
        "functional_roi_activity_bpi_summary.csv",
    ):
        _copy_file(registration_dir / filename, score_root / filename)

    manifest = run_single_fish_export_canonical_tables_stage(
        SingleFishPipelineConfig(
            fish_id=fish_dir.name,
            local_root=tmp_path,
            pipeline_root=output_root,
            strict=True,
        )
    )

    canonical_dir = output_root / "export-canonical-tables" / "registration"
    assert manifest.status == "pass"
    assert (canonical_dir / "hcr_activity_status.csv").read_bytes() == (
        output_root / "assign-hcr-identity" / "registration" / "hcr_activity_status.csv"
    ).read_bytes()
    assert manifest.parameters is not None
    assert manifest.parameters["hcr_input_root_is_explicit"] is False


def test_export_canonical_tables_writer_assembles_staged_score_and_hcr_sources(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    output_root = tmp_path / "staged-output-root"
    score_root = output_root / "score-activity-bpi" / "registration"
    hcr_root = output_root / "assign-hcr-identity" / "registration"
    score_root.mkdir(parents=True)
    hcr_root.mkdir(parents=True)
    registration_dir = fish_dir / "03_analysis" / "functional" / "registration"
    score_filenames = (
        "functional_roi_activity_identity.csv",
        "functional_roi_activity_bpi_cells.csv",
        "functional_roi_activity_bpi_summary.csv",
    )
    hcr_filenames = (
        "hcr_activity_status.csv",
        "hcr_activity_status_summary.csv",
        "conf_to_func_pairs.csv",
        "conf_to_func_pairs_raw.csv",
        "hcr_func_candidates.csv",
    )
    for filename in score_filenames:
        _copy_csv_with_crlf(registration_dir / filename, score_root / filename)
    for filename in hcr_filenames:
        _copy_csv_with_crlf(registration_dir / filename, hcr_root / filename)

    manifest = run_single_fish_export_canonical_tables_stage(
        SingleFishPipelineConfig(
            fish_id=fish_dir.name,
            local_root=tmp_path,
            pipeline_root=output_root,
            strict=True,
        )
    )

    canonical_dir = output_root / "export-canonical-tables" / "registration"
    assert manifest.status == "warn"
    assert len(tuple(canonical_dir.glob("*.csv"))) == 8
    for filename in score_filenames:
        assert (canonical_dir / filename).read_text() == (score_root / filename).read_text()
    for filename in hcr_filenames:
        assert (canonical_dir / filename).read_text() == (hcr_root / filename).read_text()
    assert manifest.parameters is not None
    assert manifest.parameters["source_policy"].startswith("score CSVs from staged score root")


def test_export_canonical_tables_writer_uses_explicit_hcr_input_root(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    output_root = tmp_path / "staged-output-root"
    score_root = output_root / "score-activity-bpi" / "registration"
    hcr_root = tmp_path / "bootstrap-hcr-root"
    score_root.mkdir(parents=True)
    hcr_root.mkdir(parents=True)
    registration_dir = fish_dir / "03_analysis" / "functional" / "registration"
    for filename in (
        "functional_roi_activity_identity.csv",
        "functional_roi_activity_bpi_cells.csv",
        "functional_roi_activity_bpi_summary.csv",
    ):
        _copy_file(registration_dir / filename, score_root / filename)
    for filename in (
        "hcr_activity_status.csv",
        "hcr_activity_status_summary.csv",
        "conf_to_func_pairs.csv",
        "conf_to_func_pairs_raw.csv",
        "hcr_func_candidates.csv",
    ):
        _copy_csv_with_crlf(registration_dir / filename, hcr_root / filename)

    manifest = run_single_fish_export_canonical_tables_stage(
        SingleFishPipelineConfig(
            fish_id=fish_dir.name,
            local_root=tmp_path,
            pipeline_root=output_root,
            strict=True,
        ),
        hcr_input_root=hcr_root,
    )

    canonical_dir = output_root / "export-canonical-tables" / "registration"
    assert manifest.status == "warn"
    assert (canonical_dir / "hcr_activity_status.csv").read_text() == (hcr_root / "hcr_activity_status.csv").read_text()
    assert manifest.parameters is not None
    assert manifest.parameters["hcr_input_root"] == str(hcr_root)
    assert manifest.parameters["hcr_input_root_is_explicit"] is True


def test_export_canonical_tables_writer_refuses_existing_outputs_without_force(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    output_root = tmp_path / "staged-output-root"
    _make_minimal_staged_outputs(fish_dir)
    source_score = fish_dir / "03_analysis" / "functional" / "pipeline_outputs" / "score-activity-bpi" / "registration"
    source_hcr = fish_dir / "03_analysis" / "functional" / "pipeline_outputs" / "assign-hcr-identity" / "registration"
    existing = output_root / "export-canonical-tables" / "registration" / "functional_roi_activity_identity.csv"
    existing.parent.mkdir(parents=True)
    existing.write_text("sentinel\n")

    manifest = run_single_fish_export_canonical_tables_stage(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, pipeline_root=output_root),
        score_input_root=source_score,
        hcr_input_root=source_hcr,
    )

    assert manifest.status == "fail"
    assert existing.read_text() == "sentinel\n"
    assert any(
        check.label == "export-canonical-tables overwrite gate" and check.status == "fail"
        for check in manifest.checks
    )


def test_single_fish_pipeline_cli_export_canonical_tables_outputs_manifest(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    output_root = tmp_path / "staged-output-root"
    _make_minimal_staged_outputs(fish_dir)
    source_score = fish_dir / "03_analysis" / "functional" / "pipeline_outputs" / "score-activity-bpi" / "registration"
    source_hcr = fish_dir / "03_analysis" / "functional" / "pipeline_outputs" / "assign-hcr-identity" / "registration"
    result = subprocess.run(
        [
            sys.executable,
            "tools/single_fish_pipeline.py",
            "export-canonical-tables",
            "--fish-id",
            fish_dir.name,
            "--local-root",
            str(tmp_path),
            "--strict",
            "--pipeline-root",
            str(output_root),
            "--score-input-root",
            str(source_score),
            "--hcr-input-root",
            str(source_hcr),
        ],
        cwd=REPO_ROOT,
        env={"PYTHONPATH": "src"},
        text=True,
        capture_output=True,
        check=False,
    )
    payload = json.loads(result.stdout)
    assert result.returncode == 0
    assert payload["stage_name"] == "export-canonical-tables"
    assert payload["dry_run"] is False
    assert payload["parameters"]["pipeline_root"] == str(output_root)
    assert len(payload["outputs"]) == 8
    assert not (fish_dir / "03_analysis" / "functional" / "pipeline_manifests").exists()


def test_make_qa_report_writer_requires_staged_canonical_exports_by_default(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    output_root = tmp_path / "staged-output-root"
    manifest = run_single_fish_make_qa_report_stage(
        SingleFishPipelineConfig(
            fish_id=fish_dir.name,
            local_root=tmp_path,
            pipeline_root=output_root,
            strict=True,
        )
    )
    assert manifest.status == "fail"
    assert manifest.dry_run is False
    assert manifest.parameters is not None
    assert manifest.parameters["canonical_input_root_is_explicit"] is False
    assert any("export-canonical-tables" in error for error in manifest.errors)
    assert not output_root.exists()


def test_make_qa_report_writer_generates_markdown_and_json_summary(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    output_root = tmp_path / "staged-output-root"
    canonical_root = output_root / "export-canonical-tables" / "registration"
    registration_dir = fish_dir / "03_analysis" / "functional" / "registration"
    for filename in (
        "functional_roi_activity_identity.csv",
        "functional_roi_activity_bpi_cells.csv",
        "functional_roi_activity_bpi_summary.csv",
        "hcr_activity_status.csv",
        "hcr_activity_status_summary.csv",
        "conf_to_func_pairs.csv",
        "conf_to_func_pairs_raw.csv",
        "hcr_func_candidates.csv",
    ):
        _copy_file(registration_dir / filename, canonical_root / filename)

    manifest = run_single_fish_make_qa_report_stage(
        SingleFishPipelineConfig(
            fish_id=fish_dir.name,
            local_root=tmp_path,
            pipeline_root=output_root,
            strict=True,
        )
    )

    report_dir = output_root / "make-qa-report"
    summary = json.loads((report_dir / "qa_report_summary.json").read_text())
    markdown = (report_dir / "qa_report.md").read_text()
    assert manifest.status == "pass"
    assert summary["fish_id"] == fish_dir.name
    assert len(summary["canonical_tables"]) == 8
    assert all(table["exists"] for table in summary["canonical_tables"])
    assert "# Single-Fish QA Report" in markdown
    assert "functional_roi_activity_identity.csv" in markdown


def test_make_qa_report_writer_refuses_existing_outputs_without_force(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    output_root = tmp_path / "staged-output-root"
    canonical_root = output_root / "export-canonical-tables" / "registration"
    registration_dir = fish_dir / "03_analysis" / "functional" / "registration"
    for filename in (
        "functional_roi_activity_identity.csv",
        "functional_roi_activity_bpi_cells.csv",
        "functional_roi_activity_bpi_summary.csv",
        "hcr_activity_status.csv",
        "hcr_activity_status_summary.csv",
        "conf_to_func_pairs.csv",
        "conf_to_func_pairs_raw.csv",
        "hcr_func_candidates.csv",
    ):
        _copy_file(registration_dir / filename, canonical_root / filename)
    existing = output_root / "make-qa-report" / "qa_report.md"
    existing.parent.mkdir(parents=True)
    existing.write_text("sentinel\n")

    manifest = run_single_fish_make_qa_report_stage(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, pipeline_root=output_root)
    )

    assert manifest.status == "fail"
    assert existing.read_text() == "sentinel\n"
    assert any(check.label == "make-qa-report overwrite gate" and check.status == "fail" for check in manifest.checks)


def test_single_fish_pipeline_cli_make_qa_report_outputs_manifest(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    output_root = tmp_path / "staged-output-root"
    canonical_root = output_root / "export-canonical-tables" / "registration"
    registration_dir = fish_dir / "03_analysis" / "functional" / "registration"
    for filename in (
        "functional_roi_activity_identity.csv",
        "functional_roi_activity_bpi_cells.csv",
        "functional_roi_activity_bpi_summary.csv",
        "hcr_activity_status.csv",
        "hcr_activity_status_summary.csv",
        "conf_to_func_pairs.csv",
        "conf_to_func_pairs_raw.csv",
        "hcr_func_candidates.csv",
    ):
        _copy_file(registration_dir / filename, canonical_root / filename)
    result = subprocess.run(
        [
            sys.executable,
            "tools/single_fish_pipeline.py",
            "make-qa-report",
            "--fish-id",
            fish_dir.name,
            "--local-root",
            str(tmp_path),
            "--strict",
            "--pipeline-root",
            str(output_root),
        ],
        cwd=REPO_ROOT,
        env={"PYTHONPATH": "src"},
        text=True,
        capture_output=True,
        check=False,
    )
    payload = json.loads(result.stdout)
    assert result.returncode == 0
    assert payload["stage_name"] == "make-qa-report"
    assert payload["dry_run"] is False
    assert payload["parameters"]["pipeline_root"] == str(output_root)
    assert len(payload["outputs"]) == 2
    assert not (fish_dir / "03_analysis" / "functional" / "pipeline_manifests").exists()


def test_compare_staged_make_qa_report_outputs_json(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    output_root = tmp_path / "staged-output-root"
    canonical_root = output_root / "export-canonical-tables" / "registration"
    registration_dir = fish_dir / "03_analysis" / "functional" / "registration"
    for filename in (
        "functional_roi_activity_identity.csv",
        "functional_roi_activity_bpi_cells.csv",
        "functional_roi_activity_bpi_summary.csv",
        "hcr_activity_status.csv",
        "hcr_activity_status_summary.csv",
        "conf_to_func_pairs.csv",
        "conf_to_func_pairs_raw.csv",
        "hcr_func_candidates.csv",
    ):
        _copy_file(registration_dir / filename, canonical_root / filename)
    run_single_fish_make_qa_report_stage(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, pipeline_root=output_root, strict=True)
    )

    comparison = compare_single_fish_staged_outputs(
        SingleFishPipelineConfig(
            fish_id=fish_dir.name,
            local_root=tmp_path,
            pipeline_root=output_root,
            strict=True,
        ),
        "make-qa-report",
    )

    assert comparison["status"] == "pass"
    assert comparison["comparisons"][0]["stage_name"] == "make-qa-report"
    assert comparison["comparisons"][0]["failed_checks"] == []


def test_make_figures_writer_requires_staged_canonical_exports_by_default(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    output_root = tmp_path / "staged-output-root"
    manifest = run_single_fish_make_figures_stage(
        SingleFishPipelineConfig(
            fish_id=fish_dir.name,
            local_root=tmp_path,
            pipeline_root=output_root,
            strict=True,
        )
    )
    assert manifest.status == "fail"
    assert manifest.dry_run is False
    assert manifest.parameters is not None
    assert manifest.parameters["canonical_input_root_is_explicit"] is False
    assert any("export-canonical-tables" in error for error in manifest.errors)
    assert not output_root.exists()
    assert not (fish_dir / "03_analysis" / "functional" / "pipeline_outputs").exists()


def test_make_figures_writer_stages_declared_figure_artifacts(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    output_root = tmp_path / "staged-output-root"
    canonical_root = output_root / "export-canonical-tables" / "registration"
    registration_dir = fish_dir / "03_analysis" / "functional" / "registration"
    for filename in (
        "functional_roi_activity_identity.csv",
        "functional_roi_activity_bpi_cells.csv",
        "functional_roi_activity_bpi_summary.csv",
        "hcr_activity_status.csv",
        "hcr_activity_status_summary.csv",
        "conf_to_func_pairs.csv",
        "conf_to_func_pairs_raw.csv",
        "hcr_func_candidates.csv",
    ):
        _copy_file(registration_dir / filename, canonical_root / filename)

    manifest = run_single_fish_make_figures_stage(
        SingleFishPipelineConfig(
            fish_id=fish_dir.name,
            local_root=tmp_path,
            pipeline_root=output_root,
            strict=True,
        )
    )

    figure_dir = output_root / "make-figures" / "04_plots"
    assert manifest.status == "pass"
    assert len(tuple(figure_dir.glob("*.png"))) == 5
    assert (figure_dir / "compound_50j_56i_unified.png").read_bytes() == (
        fish_dir / "04_plots" / "compound_50j_56i_unified.png"
    ).read_bytes()
    assert (figure_dir / "single_fish_50l_responsive_identity_donut.png").read_bytes() != (
        fish_dir / "04_plots" / "single_fish_50l_responsive_identity_donut.png"
    ).read_bytes()
    assert (figure_dir / "single_fish_hcr_anatomy_coexpression_summary.png").read_bytes() != (
        fish_dir / "04_plots" / "single_fish_hcr_anatomy_coexpression_summary.png"
    ).read_bytes()
    assert (figure_dir / "single_fish_50l_responsive_identity_donut_counts.csv").exists()
    assert (figure_dir / "single_fish_hcr_anatomy_coexpression_summary.csv").exists()
    assert manifest.parameters is not None
    assert "package-owned renderers" in manifest.parameters["source_policy"]
    assert tuple(manifest.parameters["rendered_figures"]) == (
        "single_fish_50l_responsive_identity_donut.png",
        "single_fish_hcr_anatomy_coexpression_summary.png",
    )
    assert tuple(manifest.parameters["legacy_copied_figures"]) == (
        "compound_50j_56i_unified.png",
        "bpi_all_pairs.png",
        "per_gene_stimulus_trace_with_hcr_status_56h.png",
    )


def test_make_figures_writer_refuses_existing_outputs_without_force(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    output_root = tmp_path / "staged-output-root"
    canonical_root = output_root / "export-canonical-tables" / "registration"
    registration_dir = fish_dir / "03_analysis" / "functional" / "registration"
    for filename in (
        "functional_roi_activity_identity.csv",
        "functional_roi_activity_bpi_cells.csv",
        "functional_roi_activity_bpi_summary.csv",
        "hcr_activity_status.csv",
        "hcr_activity_status_summary.csv",
        "conf_to_func_pairs.csv",
        "conf_to_func_pairs_raw.csv",
        "hcr_func_candidates.csv",
    ):
        _copy_file(registration_dir / filename, canonical_root / filename)
    existing = output_root / "make-figures" / "04_plots" / "compound_50j_56i_unified.png"
    existing.parent.mkdir(parents=True)
    existing.write_text("sentinel\n")

    manifest = run_single_fish_make_figures_stage(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, pipeline_root=output_root)
    )

    assert manifest.status == "fail"
    assert existing.read_text() == "sentinel\n"
    assert any(check.label == "make-figures overwrite gate" and check.status == "fail" for check in manifest.checks)


def test_single_fish_pipeline_cli_make_figures_outputs_manifest(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    output_root = tmp_path / "staged-output-root"
    canonical_root = output_root / "export-canonical-tables" / "registration"
    registration_dir = fish_dir / "03_analysis" / "functional" / "registration"
    for filename in (
        "functional_roi_activity_identity.csv",
        "functional_roi_activity_bpi_cells.csv",
        "functional_roi_activity_bpi_summary.csv",
        "hcr_activity_status.csv",
        "hcr_activity_status_summary.csv",
        "conf_to_func_pairs.csv",
        "conf_to_func_pairs_raw.csv",
        "hcr_func_candidates.csv",
    ):
        _copy_file(registration_dir / filename, canonical_root / filename)
    result = subprocess.run(
        [
            sys.executable,
            "tools/single_fish_pipeline.py",
            "make-figures",
            "--fish-id",
            fish_dir.name,
            "--local-root",
            str(tmp_path),
            "--strict",
            "--pipeline-root",
            str(output_root),
        ],
        cwd=REPO_ROOT,
        env={"PYTHONPATH": "src"},
        text=True,
        capture_output=True,
        check=False,
    )
    payload = json.loads(result.stdout)
    assert result.returncode == 0
    assert payload["stage_name"] == "make-figures"
    assert payload["dry_run"] is False
    assert payload["parameters"]["pipeline_root"] == str(output_root)
    assert len(payload["outputs"]) == 5
    assert not (fish_dir / "03_analysis" / "functional" / "pipeline_manifests").exists()


def test_single_fish_pipeline_cli_score_activity_bpi_outputs_manifest(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    result = subprocess.run(
        [
            sys.executable,
            "tools/single_fish_pipeline.py",
            "score-activity-bpi",
            "--fish-id",
            fish_dir.name,
            "--local-root",
            str(tmp_path),
            "--strict",
            "--pipeline-root",
            str(tmp_path / "staged-output-root"),
        ],
        cwd=REPO_ROOT,
        env={"PYTHONPATH": "src"},
        text=True,
        capture_output=True,
        check=False,
    )
    payload = json.loads(result.stdout)
    assert result.returncode == 1
    assert payload["stage_name"] == "score-activity-bpi"
    assert payload["dry_run"] is False
    assert payload["parameters"]["pipeline_root"] == str(tmp_path / "staged-output-root")
    assert "invalid choice" not in result.stderr


def test_single_fish_pipeline_cli_audit_inputs_dry_run(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    result = subprocess.run(
        [
            sys.executable,
            "tools/single_fish_pipeline.py",
            "audit-inputs",
            "--fish-id",
            fish_dir.name,
            "--local-root",
            str(tmp_path),
            "--strict",
            "--pipeline-root",
            str(tmp_path / "reported-output-root"),
        ],
        cwd=REPO_ROOT,
        env={"PYTHONPATH": "src"},
        text=True,
        capture_output=True,
        check=True,
    )
    payload = json.loads(result.stdout)
    assert payload["stage_name"] == "audit-inputs"
    assert payload["status"] == "pass"
    assert payload["dry_run"] is True
    assert payload["parameters"]["pipeline_root"] == str(tmp_path / "reported-output-root")
    assert not (fish_dir / "03_analysis" / "functional" / "pipeline_manifests").exists()


def test_single_fish_pipeline_cli_audit_inputs_writes_manifest_with_opt_in(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    result = subprocess.run(
        [
            sys.executable,
            "tools/single_fish_pipeline.py",
            "audit-inputs",
            "--fish-id",
            fish_dir.name,
            "--local-root",
            str(tmp_path),
            "--strict",
            "--write-manifest",
        ],
        cwd=REPO_ROOT,
        env={"PYTHONPATH": "src"},
        text=True,
        capture_output=True,
        check=True,
    )
    payload = json.loads(result.stdout)
    assert payload["stage_name"] == "audit-inputs"
    assert payload["dry_run"] is True
    assert payload["parameters"]["write_manifest"] is True
    manifest_path = fish_dir / "03_analysis" / "functional" / "pipeline_manifests" / "audit-inputs_manifest.json"
    assert manifest_path.exists()
    assert json.loads(manifest_path.read_text())["stage_name"] == "audit-inputs"


def test_single_fish_pipeline_cli_status_outputs_summary(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    result = subprocess.run(
        [
            sys.executable,
            "tools/single_fish_pipeline.py",
            "status",
            "--fish-id",
            fish_dir.name,
            "--local-root",
            str(tmp_path),
            "--strict",
        ],
        cwd=REPO_ROOT,
        env={"PYTHONPATH": "src"},
        text=True,
        capture_output=True,
        check=True,
    )
    payload = json.loads(result.stdout)
    assert payload["status"] == "pass"
    assert payload["dry_run"] is True
    assert payload["failed_checks"] == []


def test_single_fish_pipeline_cli_stage_status_outputs_manifest(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    _make_minimal_staged_outputs(fish_dir)
    result = subprocess.run(
        [
            sys.executable,
            "tools/single_fish_pipeline.py",
            "stage-status",
            "--fish-id",
            fish_dir.name,
            "--local-root",
            str(tmp_path),
            "--strict",
            "--stage-name",
            "make-figures",
        ],
        cwd=REPO_ROOT,
        env={"PYTHONPATH": "src"},
        text=True,
        capture_output=True,
        check=True,
    )
    payload = json.loads(result.stdout)
    assert payload["stage_name"] == "make-figures"
    assert payload["status"] == "pass"
    assert len(payload["outputs"]) == 5


def test_single_fish_pipeline_cli_compare_staged_outputs_json(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    _make_minimal_staged_outputs(fish_dir)
    result = subprocess.run(
        [
            sys.executable,
            "tools/single_fish_pipeline.py",
            "compare-staged",
            "--fish-id",
            fish_dir.name,
            "--local-root",
            str(tmp_path),
            "--strict",
            "--stage-name",
            "export-canonical-tables",
        ],
        cwd=REPO_ROOT,
        env={"PYTHONPATH": "src"},
        text=True,
        capture_output=True,
        check=True,
    )
    payload = json.loads(result.stdout)
    assert payload["status"] == "pass"
    assert payload["stage_name"] == "export-canonical-tables"
    assert len(payload["comparisons"]) == 1
    assert payload["comparisons"][0]["stage_name"] == "export-canonical-tables"


def test_single_fish_pipeline_cli_freeze_and_compare_legacy_baseline_outputs_json(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    _make_minimal_staged_outputs(fish_dir)
    freeze_result = subprocess.run(
        [
            sys.executable,
            "tools/single_fish_pipeline.py",
            "freeze-legacy-baseline",
            "--fish-id",
            fish_dir.name,
            "--local-root",
            str(tmp_path),
            "--strict",
            "--stage-name",
            "make-figures",
        ],
        cwd=REPO_ROOT,
        env={"PYTHONPATH": "src"},
        text=True,
        capture_output=True,
        check=True,
    )
    freeze_payload = json.loads(freeze_result.stdout)
    assert freeze_payload["stage_name"] == "freeze-legacy-baseline"
    assert freeze_payload["status"] == "pass"
    assert freeze_payload["parameters"]["copied_outputs"] == 5

    compare_result = subprocess.run(
        [
            sys.executable,
            "tools/single_fish_pipeline.py",
            "compare-legacy-baseline",
            "--fish-id",
            fish_dir.name,
            "--local-root",
            str(tmp_path),
            "--strict",
            "--stage-name",
            "make-figures",
        ],
        cwd=REPO_ROOT,
        env={"PYTHONPATH": "src"},
        text=True,
        capture_output=True,
        check=True,
    )
    compare_payload = json.loads(compare_result.stdout)
    assert compare_payload["status"] == "pass"
    assert compare_payload["comparisons"][0]["stage_name"] == "make-figures"
    assert compare_payload["comparisons"][0]["failed_checks"] == []


def test_single_fish_pipeline_cli_audit_score_activity_bpi_outputs_manifest_json(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    result = subprocess.run(
        [
            sys.executable,
            "tools/single_fish_pipeline.py",
            "audit-score-activity-bpi",
            "--fish-id",
            fish_dir.name,
            "--local-root",
            str(tmp_path),
            "--strict",
        ],
        cwd=REPO_ROOT,
        env={"PYTHONPATH": "src"},
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 1
    payload = json.loads(result.stdout)
    assert payload["stage_name"] == "audit-score-activity-bpi"
    assert payload["status"] == "fail"
    assert payload["dry_run"] is True
    assert payload["parameters"]["control_outputs_are_comparison_only"] is True
    assert not (fish_dir / "03_analysis" / "functional" / "pipeline_manifests").exists()
