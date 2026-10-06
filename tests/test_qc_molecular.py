from pathlib import Path
import json

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pytest
import SimpleITK as sitk
import tifffile

from codeants_2pf_hcr.plots.qc_molecular import (
    MOLECULAR_GEOMETRY_TRACKS,
    build_hcr_anatomy_centroid_offset_table,
    build_molecular_identity_fate_table,
    build_hcr_anatomy_match_outcome_table,
    build_hcr_functional_plane_status_donut_table,
    build_hcr_matching_flow_donut_table,
    build_activity_bpi_gate_table,
    inspect_activity_export_qc,
    inspect_molecular_geometry_qc,
    inspect_molecular_identity_qc,
    load_molecular_label_viewer,
    load_hcr_match_overlay_viewer,
    plot_activity_export_qc,
    plot_activity_bpi_gate,
    plot_hcr_anatomy_centroid_offsets,
    plot_hcr_anatomy_match_outcome_donuts,
    plot_hcr_functional_plane_status_donuts,
    plot_hcr_matching_flow_donuts,
    plot_cross_modality_mask_sizes,
    plot_molecular_geometry_qc,
    plot_molecular_identity_qc,
    plot_molecular_correspondence_tiles,
    _bbox_size_rows,
)


FISH_ID = "L765_f04"


def _csv(path: Path, rows: list[dict]) -> Path:
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def _molecular_fate_inputs(tmp_path: Path, mode: str) -> dict:
    anatomy = np.zeros((3, 5, 5), dtype=np.uint16)
    anatomy[0, 1:3, 1:3] = 10
    anatomy[2, 1:3, 1:3] = 20
    anatomy_path = tmp_path / "anatomy.tif"
    tifffile.imwrite(anatomy_path, anatomy, photometric="minisblack")
    mask_path = tmp_path / "L765_f04_rbest_channel1_sst1.2_cp_masks_in_2p_labels_uint16.tif"
    tifffile.imwrite(mask_path, anatomy, photometric="minisblack")
    pair_path = mask_path.with_name(mask_path.name.replace("_labels_uint16.tif", "_final_pairs.csv"))
    _csv(pair_path, [{"conf_label": 10, "twoP_label": 10}, {"conf_label": 20, "twoP_label": 20}])
    geometry_path = _csv(tmp_path / "geometry.csv", [{"plane_idx": 0, "best_z": 0}])
    identity_path = _csv(tmp_path / "identity.csv", [{"anat_label": 20, "has_unique_anat_match": True,
                                                    "bpi_data_available": True, "response_is_active": True}])
    refs_path = tmp_path / "plane_refs_summary_geometry.json"
    refs_path.write_text(json.dumps([{"index": 0, "best_z": 0, "anat_label_z_mode": mode,
                                     "anat_label_z": 2 if mode == "reverse" else 0}]))
    return dict(label_paths=[mask_path], final_pair_paths=[pair_path], identity_table_path=identity_path,
                geometry_table_path=geometry_path, anatomy_labels_path=anatomy_path, plane_refs_path=refs_path)


@pytest.mark.parametrize("mode", ["direct", "reverse"])
def test_molecular_fate_uses_geometry_owned_label_pages(tmp_path: Path, mode: str) -> None:
    inputs = _molecular_fate_inputs(tmp_path, mode)
    before = {path: path.read_bytes() for path in tmp_path.iterdir()}
    counts = build_molecular_identity_fate_table(**inputs).set_index("fate")["n_labels"]
    assert counts["out-of-plane anatomy label"] == 1
    assert counts["in-plane responsive ROI"] == (1 if mode == "reverse" else 0)
    assert counts["in-plane no functional ROI candidate"] == (0 if mode == "reverse" else 1)
    assert counts.sum() == 2
    assert len(counts) == 6
    assert {path: path.read_bytes() for path in tmp_path.iterdir()} == before


@pytest.mark.parametrize("fault", ["missing_file", "missing_plane", "duplicate", "missing_mode", "missing_page",
                                    "wrong_page", "wrong_best_z", "out_of_bounds", "ambiguous_best_z", "invalid_json_shape"])
def test_molecular_fate_blocks_invalid_label_z_provenance(tmp_path: Path, fault: str) -> None:
    inputs = _molecular_fate_inputs(tmp_path, "reverse")
    refs_path = inputs["plane_refs_path"]
    refs = json.loads(refs_path.read_text())
    if fault == "missing_file":
        inputs["plane_refs_path"] = tmp_path / "absent.json"
    elif fault == "missing_plane": refs = []
    elif fault == "duplicate": refs.append(dict(refs[0]))
    elif fault == "missing_mode": refs[0].pop("anat_label_z_mode")
    elif fault == "missing_page": refs[0].pop("anat_label_z")
    elif fault == "wrong_page": refs[0]["anat_label_z"] = 0
    elif fault == "wrong_best_z": refs[0]["best_z"] = 1
    elif fault == "out_of_bounds":
        refs[0].update(best_z=4, anat_label_z=-2)
        _csv(inputs["geometry_table_path"], [{"plane_idx": 0, "best_z": 4}])
    elif fault == "ambiguous_best_z":
        _csv(inputs["geometry_table_path"], [{"plane_idx": 0, "best_z": 0}, {"plane_idx": 0, "best_z": 1}])
    elif fault == "invalid_json_shape": refs = {}
    refs_path.write_text(json.dumps(refs))
    with pytest.raises((ValueError, FileNotFoundError)):
        build_molecular_identity_fate_table(**inputs)


def test_molecular_geometry_keeps_four_tracks_distinct_and_reports_ambiguity(tmp_path: Path) -> None:
    tracks = {}
    for track in MOLECULAR_GEOMETRY_TRACKS:
        artifact = tmp_path / f"{track}.json"
        artifact.write_text("{}")
        tracks[track] = [artifact]
    segmentation = _csv(
        tmp_path / "segments.csv",
        [
            {"fish_id": FISH_ID, "round_id": "rbest", "gene": "sst1.2", "hcr_label": 1},
            {"fish_id": FISH_ID, "round_id": "later", "gene": "gad2", "hcr_label": 2},
        ],
    )
    pairs = _csv(
        tmp_path / "pairs.csv",
        [
            {"fish_id": FISH_ID, "hcr_label": 1, "anat_label": 10},
            {"fish_id": FISH_ID, "hcr_label": 1, "anat_label": 11},
            {"fish_id": FISH_ID, "hcr_label": 2, "anat_label": 12},
        ],
    )

    report = inspect_molecular_geometry_qc(
        fish_id=FISH_ID,
        pipeline_root=tmp_path,
        track_artifacts=tracks,
        segmentation_table_path=segmentation,
        pair_candidates_path=pairs,
    )

    assert report["status"] == "ready_for_review"
    assert report["tracks"]["track"].tolist() == list(MOLECULAR_GEOMETRY_TRACKS)
    assert int(report["pair_ambiguity"]["is_ambiguous"].sum()) == 1
    figure = plot_molecular_geometry_qc(report)
    assert len(figure.axes) == 2
    plt.close(figure)


def test_molecular_geometry_missing_artifact_is_clear_and_read_only(tmp_path: Path) -> None:
    report = inspect_molecular_geometry_qc(
        fish_id=FISH_ID,
        pipeline_root=tmp_path,
        track_artifacts={track: [] for track in MOLECULAR_GEOMETRY_TRACKS},
        segmentation_table_path=tmp_path / "not-there.csv",
        pair_candidates_path=None,
    )
    assert report["status"] == "incomplete"
    assert report["read_only"] is True
    assert "no computation was attempted" in " ".join(report["issues"]["detail"]).lower()
    assert list(tmp_path.iterdir()) == []


def test_molecular_geometry_combines_per_mask_tables_without_label_collisions(tmp_path: Path) -> None:
    tracks = {}
    for track in MOLECULAR_GEOMETRY_TRACKS:
        artifact = tmp_path / track
        artifact.mkdir()
        tracks[track] = [artifact]
    reviews = [
        _csv(tmp_path / "gene_a_review.csv", [{"hcr_label": 1, "anat_label": 10}]),
        _csv(tmp_path / "gene_b_review.csv", [{"hcr_label": 1, "anat_label": 11}]),
    ]
    report = inspect_molecular_geometry_qc(
        fish_id=FISH_ID,
        pipeline_root=tmp_path,
        track_artifacts=tracks,
        segmentation_table_path=reviews,
        pair_candidates_path=reviews,
    )
    assert report["status"] == "ready_for_review"
    assert len(report["pair_ambiguity"]) == 2
    assert not report["pair_ambiguity"]["is_ambiguous"].any()
    assert set(report["pair_ambiguity"]["source_file"]) == {"gene_a_review.csv", "gene_b_review.csv"}


def test_molecular_geometry_distinguishes_available_masks_from_unstarted_candidates(tmp_path: Path) -> None:
    mask = tmp_path / "L765_f04_r2_channel2_sst1_2_cp_masks_in_2p_labels_uint16.tif"
    tifffile.imwrite(mask, [[1]])
    report = inspect_molecular_geometry_qc(
        fish_id=FISH_ID,
        pipeline_root=tmp_path,
        track_artifacts={track: [] for track in MOLECULAR_GEOMETRY_TRACKS},
        segmentation_table_path=None,
        segmentation_label_paths=[mask],
        pair_candidates_path=None,
        pair_candidates_state="not_started",
    )
    assert report["label_masks"]["status"].tolist() == ["present"]
    assert report["issues"].query("area == 'molecular/anatomy pair candidates'")["status"].tolist() == ["not_started"]
    assert not report["issues"]["detail"].str.contains("No artifact paths were configured", regex=False).any()


def test_molecular_label_viewer_loader_accepts_same_grid_masks_and_names_dynamic_gene(tmp_path: Path) -> None:
    anatomy = tmp_path / "anatomy.tif"
    label = tmp_path / "L765_f04_r2_channel2_sst1_2_cp_masks_in_2p_labels_uint16.tif"
    tifffile.imwrite(anatomy, [[[1, 2], [3, 4]], [[5, 6], [7, 8]]])
    tifffile.imwrite(label, [[[0, 1], [0, 0]], [[2, 0], [0, 0]]])
    viewer = load_molecular_label_viewer(anatomy, [label])
    assert viewer.anatomy_zyx.shape == (2, 2, 2)
    assert tuple(viewer.label_masks_zyx) == ("r2 · sst1.2",)

    mismatch = tmp_path / "L765_f04_rbest_channel3_pth2_cp_masks_in_2p_labels_uint16.tif"
    tifffile.imwrite(mismatch, [[[1]]])
    with pytest.raises(ValueError, match="does not share the in-vivo anatomy grid"):
        load_molecular_label_viewer(anatomy, [mismatch])


def test_hcr_match_overlay_viewer_uses_saved_pair_membership_without_mutating_masks(tmp_path: Path) -> None:
    anatomy = tmp_path / "anatomy.tif"
    label = tmp_path / "L765_f04_r2_channel2_sst1_2_cp_masks_in_2p_labels_uint16.tif"
    review = tmp_path / "L765_f04_r2_channel2_sst1_2_cp_masks_in_2p_review.csv"
    final = tmp_path / "L765_f04_r2_channel2_sst1_2_cp_masks_in_2p_final_pairs.csv"
    tifffile.imwrite(anatomy, np.zeros((1, 2, 3), dtype="uint16"))
    tifffile.imwrite(label, np.array([[[1, 2, 3], [0, 0, 0]]], dtype="uint16"))
    _csv(review, [{"conf_label": 1}, {"conf_label": 2}])
    _csv(final, [{"conf_label": 1, "twoP_label": 7}])

    viewer = load_hcr_match_overlay_viewer(anatomy, [label], [review], [final])

    assert set(viewer.label_categories.values()) == {"accepted", "rejected"}
    accepted = next(mask for key, mask in viewer.label_masks_zyx.items() if key.startswith("accepted"))
    rejected = next(mask for key, mask in viewer.label_masks_zyx.items() if key.startswith("rejected"))
    assert np.array_equal(accepted, np.array([[[1, 0, 0], [0, 0, 0]]], dtype="uint16"))
    assert np.array_equal(rejected, np.array([[[0, 2, 0], [0, 0, 0]]], dtype="uint16"))
    assert np.array_equal(tifffile.imread(label), np.array([[[1, 2, 3], [0, 0, 0]]], dtype="uint16"))


def test_hcr_matching_flow_donuts_partition_segmented_labels(tmp_path: Path) -> None:
    label = tmp_path / "L765_f04_r2_channel2_sst1_2_cp_masks_in_2p_labels_uint16.tif"
    review = tmp_path / "L765_f04_r2_channel2_sst1_2_cp_masks_in_2p_review.csv"
    final = tmp_path / "L765_f04_r2_channel2_sst1_2_cp_masks_in_2p_final_pairs.csv"
    tifffile.imwrite(label, np.array([[[1, 2, 3, 4, 5]]], dtype="uint16"))
    _csv(review, [
        {"conf_label": 2, "within_gate": False, "pair_type": "1-1"},
        {"conf_label": 3, "within_gate": True, "pair_type": "1-many"},
        {"conf_label": 4, "within_gate": True, "pair_type": "1-1", "quality": "iffy"},
        {"conf_label": 5, "within_gate": True, "pair_type": "1-1", "quality": "good"},
    ])
    _csv(final, [{"conf_label": 5, "pair_type": "1-1", "within_gate": True, "quality": "good"}])

    donuts = build_hcr_matching_flow_donut_table(label_paths=[label], review_paths=[review], final_pair_paths=[final])

    outer = donuts.query("ring == 'outer'")
    assert outer["n_labels"].sum() == 5
    assert outer.set_index("category").loc["accepted 1:1", "n_labels"] == 1
    figure = plot_hcr_matching_flow_donuts(donuts, fish_id=FISH_ID)
    assert len(figure.axes) == 1
    plt.close(figure)


def test_hcr_anatomy_match_outcomes_match_legacy_category_precedence(tmp_path: Path) -> None:
    label = tmp_path / "L765_f04_round2_channel2_sst1_2_cp_masks_in_2p_labels_uint16.tif"
    matches = tmp_path / "L765_f04_round2_channel2_sst1_2_cp_masks_in_2p_matches.csv"
    meta = tmp_path / "L765_f04_round2_channel2_sst1_2_cp_masks_in_2p_warp_meta.json"
    status = tmp_path / "hcr_activity_status.csv"
    tifffile.imwrite(label, np.array([[[1, 2, 3, 4, 5, 6]]], dtype="uint16"))
    _csv(matches, [
        {"conf_label": 1, "within_gate": True, "pair_type": "1-1", "quality": "good"},
        {"conf_label": 2, "within_gate": True, "pair_type": "1-1", "quality": "good"},
        {"conf_label": 3, "within_gate": True, "pair_type": "1-1", "quality": "iffy"},
        {"conf_label": 4, "within_gate": False, "pair_type": "rejected", "quality": "rejected"},
        {"conf_label": 5, "within_gate": True, "pair_type": "1-many", "quality": "iffy"},
    ])
    meta.write_text('{"filter_stats": {"low_conf_labels": [2, 6]}}')
    _csv(status, [
        {"gene": "sst1.2", "conf_label": 1, "represented_on_func_plane": True},
        {"gene": "sst1.2", "conf_label": 2, "represented_on_func_plane": False},
    ])

    outcomes = build_hcr_anatomy_match_outcome_table(
        label_paths=[label], match_paths=[matches], activity_status_path=status,
    )
    counts = outcomes.set_index("category")["n_labels"]
    assert counts["in-plane anatomy match"] == 1
    assert counts["out-of-plane anatomy match"] == 1  # good match precedes its size flag
    assert counts[">q95 large mask"] == 1
    assert counts["1-to-1 anatomy relation, IoU below threshold"] == 1
    assert counts["too far / no overlap anatomy"] == 1
    assert counts["split anatomy relation"] == 1
    assert int(counts.sum()) == 6
    figure = plot_hcr_anatomy_match_outcome_donuts(outcomes, fish_id=FISH_ID)
    assert len(figure.axes) == 1
    plt.close(figure)


def test_hcr_functional_plane_status_donuts_use_legacy_two_ring_partition(tmp_path: Path) -> None:
    label = tmp_path / "L765_f04_round2_channel2_sst1_2_cp_masks_in_2p_labels_uint16.tif"
    meta = tmp_path / "L765_f04_round2_channel2_sst1_2_cp_masks_in_2p_warp_meta.json"
    status = tmp_path / "hcr_activity_status.csv"
    tifffile.imwrite(label, np.array([[[1, 2, 3, 4, 5, 6]]], dtype="uint16"))
    meta.write_text('{"filter_stats": {"low_conf_labels": [6]}}')
    _csv(status, [
        {"conf_mask": "L765_f04_round2_channel2_sst1_2_cp_masks.tif", "conf_label": 1, "gene": "sst1.2", "represented_on_func_plane": True, "functional_status": "in-plane responsive ROI"},
        {"conf_mask": "L765_f04_round2_channel2_sst1_2_cp_masks.tif", "conf_label": 2, "gene": "sst1.2", "represented_on_func_plane": True, "functional_status": "in-plane low-activity ROI"},
        {"conf_mask": "L765_f04_round2_channel2_sst1_2_cp_masks.tif", "conf_label": 3, "gene": "sst1.2", "represented_on_func_plane": True, "functional_status": "in-plane no functional ROI candidate"},
        {"conf_mask": "L765_f04_round2_channel2_sst1_2_cp_masks.tif", "conf_label": 4, "gene": "sst1.2", "represented_on_func_plane": False, "functional_status": "out-of-plane anatomy label"},
    ])
    donuts = build_hcr_functional_plane_status_donut_table(label_paths=[label], activity_status_path=status)
    panel = donuts.query("panel == 'sst1.2'")
    inner = panel.query("ring == 'inner'").set_index("category")["n_labels"]
    outer = panel.query("ring == 'outer'").set_index("category")["n_labels"]
    assert inner.to_dict() == {"within plane": 3, "outside plane": 1, "unmatched": 1}
    assert outer.to_dict() == {"responsive ROI": 1, "low-activity ROI": 1, "response unavailable": 0, "no functional match": 1, "out of plane": 1, "unmatched": 1}
    figure = plot_hcr_functional_plane_status_donuts(donuts, fish_id=FISH_ID)
    assert len(figure.axes) == 3  # one data panel plus two hidden slots in the legacy 3-column grid
    plt.close(figure)


def test_hcr_plane_status_prefers_exact_aligned_mask_over_legacy_name(tmp_path: Path) -> None:
    label = tmp_path / "L765_f04_r2_channel2_sst1_2_cp_masks_in_2p_labels_uint16.tif"
    tifffile.imwrite(label, np.array([[[1, 2]]], dtype="uint16"))
    status = _csv(tmp_path / "hcr_activity_status.csv", [
        {"conf_mask": label.name, "conf_label": 1, "gene": "sst1.2", "represented_on_func_plane": True, "functional_status": "in-plane responsive ROI"},
        {"conf_mask": "L765_f04_r2_channel2_sst1_2_cp_masks.tif", "conf_label": 1, "gene": "sst1.2", "represented_on_func_plane": False, "functional_status": "out-of-plane anatomy label"},
        {"conf_mask": label.name, "conf_label": 2, "gene": "sst1.2", "represented_on_func_plane": False, "functional_status": "out-of-plane anatomy label"},
    ])
    counts = build_hcr_functional_plane_status_donut_table(label_paths=[label], activity_status_path=status)
    inner = counts.query("panel == 'All genes' and ring == 'inner'").set_index("category").n_labels
    assert inner.to_dict() == {"within plane": 1, "outside plane": 1, "unmatched": 0}


def test_cross_modality_sizes_use_legacy_max_minus_min_bbox_dimensions(tmp_path: Path) -> None:
    anatomy, functional, hcr = (tmp_path / name for name in ("anatomy.tif", "functional.tif", "L765_f04_r2_channel2_sst1_2_cp_masks_in_2p_labels_uint16.tif"))
    # A label spanning z=0..2, y=0..1, x=1..3 has legacy dimensions 2, 1, 2 pixels.
    mask = np.zeros((3, 3, 4), dtype="uint16")
    mask[:, :2, 1:] = 1
    for path in (anatomy, functional, hcr):
        tifffile.imwrite(path, mask, imagej=True, metadata={"axes": "ZYX", "spacing": 2.0, "unit": "um"}, resolution=(0.5, 1.0))
    row = pd.DataFrame(_bbox_size_rows(anatomy, modality="anatomy", voxel_zyx_um={"Z": 2.0, "Y": 1.0, "X": 2.0})).iloc[0]
    assert (row.x_px, row.y_px, row.z_px) == (2, 1, 2)
    assert (row.x_um, row.y_um, row.z_um, row.xy_um) == (4.0, 1.0, 4.0, 2.5)


def test_cross_modality_size_plot_draws_red_xy_q95_limits() -> None:
    rows = []
    for modality in ("anatomy", "functional", "hcr"):
        for index, value in enumerate((1.0, 2.0, 3.0)):
            rows.append({
                "modality": modality,
                "xy_um": value,
                "z_um": value + 1.0,
                "included_in_distribution": True,
                "is_xy_q95_flag": index == 2,
                "is_low_confidence_segmentation": modality == "hcr" and index == 2,
                "xy_q05_um": 1.1,
                "xy_q95_um": 2.9,
                "round": "r2" if modality == "hcr" else None,
                "gene": "sst1.2" if modality == "hcr" else None,
            })
    figure = plot_cross_modality_mask_sizes(pd.DataFrame(rows), fish_id=FISH_ID)
    red = np.asarray([0xD6, 0x27, 0x28, 0xFF], dtype=float) / 255.0
    for axis in figure.axes:
        colors = [
            color for collection in axis.collections
            for color in (collection.get_facecolors() if hasattr(collection, "get_facecolors") else [])
        ]
        assert any(np.allclose(color, red) for color in colors)
    plt.close(figure)


def test_hcr_anatomy_centroid_offset_plot_uses_accepted_pairs_with_lateral_xy_distance(tmp_path: Path) -> None:
    anatomy = tmp_path / "anatomy_labels.tif"
    anatomy_voxel_source = tmp_path / "anatomy.nrrd"
    hcr = tmp_path / "L765_f04_r2_channel2_sst1_2_cp_masks_in_2p_labels_uint16.tif"
    pairs = tmp_path / "L765_f04_r2_channel2_sst1_2_cp_masks_in_2p_final_pairs.csv"
    anatomy_arr = np.zeros((3, 5, 5), dtype="uint16")
    anatomy_arr[1, 2, 2] = 7
    hcr_arr = np.zeros_like(anatomy_arr)
    hcr_arr[2, 3, 1] = 4
    tifffile.imwrite(anatomy, anatomy_arr, imagej=True, metadata={"axes": "ZYX", "spacing": 2.0, "unit": "um"})
    tifffile.imwrite(hcr, hcr_arr, imagej=True, metadata={"axes": "ZYX", "spacing": 2.0, "unit": "um"})
    anatomy_image = sitk.GetImageFromArray(anatomy_arr)
    anatomy_image.SetSpacing((0.5, 0.75, 2.0))
    sitk.WriteImage(anatomy_image, str(anatomy_voxel_source), useCompression=False)
    _csv(pairs, [{"conf_label": 4, "twoP_label": 7}])

    offsets = build_hcr_anatomy_centroid_offset_table(
        anatomy_labels_path=anatomy,
        anatomy_voxel_source_path=anatomy_voxel_source,
        hcr_label_paths=[hcr],
        final_pair_paths=[pairs],
    )

    assert offsets["gene"].unique().tolist() == ["sst1.2"]
    assert offsets.set_index("axis").loc["x", "offset_um"] == -0.5
    assert offsets.set_index("axis").loc["y", "offset_um"] == 0.75
    assert offsets.set_index("axis").loc["z", "offset_um"] == 2.0
    assert offsets["median_anatomy_xy_radius_um"].iloc[0] == pytest.approx(np.sqrt((0.5 * 0.75) / np.pi))
    assert offsets["anatomy_dx_um"].unique().tolist() == [0.5]
    assert offsets["anatomy_dy_um"].unique().tolist() == [0.75]
    assert offsets["anatomy_dz_um"].unique().tolist() == [2.0]
    assert offsets["anatomy_voxel_source_path"].unique().tolist() == [str(anatomy_voxel_source)]
    figure = plot_hcr_anatomy_centroid_offsets(offsets)
    assert len(figure.axes) == 2
    assert figure.axes[0].get_title() == "XY offset"
    assert figure.axes[1].get_title() == "Z offset"
    assert "Median all-label max-section anatomy radius" in figure.axes[0].get_legend().get_texts()[0].get_text()
    plt.close(figure)


def test_hcr_anatomy_centroid_offsets_require_explicit_voxel_source(tmp_path: Path) -> None:
    anatomy = tmp_path / "anatomy_labels.tif"
    hcr = tmp_path / "L765_f04_r2_channel2_sst1_2_cp_masks_in_2p_labels_uint16.tif"
    pairs = tmp_path / "pairs.csv"
    mask = np.zeros((2, 3, 3), dtype="uint16")
    mask[0, 1, 1] = 1
    tifffile.imwrite(anatomy, mask)
    tifffile.imwrite(hcr, mask)
    _csv(pairs, [{"conf_label": 1, "twoP_label": 1}])

    with pytest.raises(FileNotFoundError, match="Anatomy voxel source"):
        build_hcr_anatomy_centroid_offset_table(
            anatomy_labels_path=anatomy,
            anatomy_voxel_source_path=tmp_path / "missing.nrrd",
            hcr_label_paths=[hcr],
            final_pair_paths=[pairs],
        )


def test_identity_qc_accepts_attachment_when_geometry_is_frozen_and_unchanged(tmp_path: Path) -> None:
    rows = [
        {"fish_id": FISH_ID, "plane_idx": 0, "func_label": 1, "selected_anat_label": 8, "overlap_px": 14},
        {"fish_id": FISH_ID, "plane_idx": 0, "func_label": 2, "selected_anat_label": 9, "overlap_px": 11},
    ]
    geometry = _csv(tmp_path / "geometry.csv", rows)
    identity_rows = [dict(row, identity_label=gene) for row, gene in zip(rows, ("sst1.2", "cort"))]
    identity = _csv(tmp_path / "identity.csv", identity_rows)

    report = inspect_molecular_identity_qc(
        fish_id=FISH_ID,
        pipeline_root=tmp_path,
        geometry_state="frozen",
        frozen_geometry_table_path=geometry,
        identity_table_path=identity,
    )

    assert report["status"] == "ready_for_review"
    assert report["geometry_changes"].empty
    assert set(report["identity_summary"]["identity"]) == {"sst1.2", "cort"}
    figure = plot_molecular_identity_qc(report)
    assert len(figure.axes) == 1
    plt.close(figure)


def test_identity_qc_blocks_changed_geometry_and_unfrozen_input(tmp_path: Path) -> None:
    geometry = _csv(
        tmp_path / "geometry.csv",
        [{"plane_idx": 0, "func_label": 1, "selected_anat_label": 8, "overlap_px": 14}],
    )
    identity = _csv(
        tmp_path / "identity.csv",
        [{"plane_idx": 0, "func_label": 1, "selected_anat_label": 99, "overlap_px": 14, "identity_label": "gad2"}],
    )
    report = inspect_molecular_identity_qc(
        fish_id=FISH_ID,
        pipeline_root=tmp_path,
        geometry_state="computed_unreviewed",
        frozen_geometry_table_path=geometry,
        identity_table_path=identity,
    )
    assert report["status"] == "blocked"
    assert len(report["geometry_changes"]) == 1


@pytest.mark.parametrize("mode", ["direct", "reverse"])
def test_molecular_correspondence_tiles_use_all_persisted_links_and_only_filter_display(tmp_path: Path, mode: str) -> None:
    anatomy = tmp_path / "anatomy.tif"
    anatomy_labels = tmp_path / "anatomy_labels.tif"
    hcr = tmp_path / "L765_f04_r2_channel2_sst1_2_cp_masks_in_2p_labels_uint16.tif"
    final = tmp_path / "L765_f04_r2_channel2_sst1_2_cp_masks_in_2p_final_pairs.csv"
    functional_dir = tmp_path / "functional" / "anatomy"
    functional_dir.mkdir(parents=True)
    shape = (2, 12, 12)
    intensity = np.zeros(shape, dtype=np.uint16)
    intensity[0] = np.arange(12)[None, :] * 10
    intensity[1] = np.arange(12)[:, None] * 10
    anat = np.zeros(shape, dtype=np.uint16)
    anat[0, 3:6, 3:6] = 7
    anat[1, 6:9, 6:9] = 8
    hcr_mask = np.zeros(shape, dtype=np.uint16)
    hcr_mask[0, 3:6, 3:6] = 4
    hcr_mask[1, 6:9, 6:9] = 5
    func0 = np.zeros(shape[1:], dtype=np.uint16); func0[3:6, 3:6] = 11
    func1 = np.zeros(shape[1:], dtype=np.uint16); func1[3:6, 3:6] = 13; func1[6:9, 6:9] = 12
    tifffile.imwrite(anatomy, intensity)
    tifffile.imwrite(anatomy_labels, anat)
    tifffile.imwrite(hcr, hcr_mask)
    tifffile.imwrite(functional_dir / "L765_f04_plane0_func_mask_in_2p.tif", func0)
    tifffile.imwrite(functional_dir / "L765_f04_plane1_func_mask_in_2p.tif", func1)
    (functional_dir / "._L765_f04_plane0_func_mask_in_2p.tif").write_bytes(b"not a TIFF")
    _csv(final, [{"conf_label": 4, "twoP_label": 7}, {"conf_label": 5, "twoP_label": 8, "is_low_confidence_segmentation": True}])
    identity = _csv(
        tmp_path / "identity.csv",
        [
            {"anat_label": 7, "plane_idx": 0, "func_label": 11, "best_z": 0, "has_unique_anat_match": True, "response_class": "responsive", "bpi_data_available": True},
            {"anat_label": 7, "plane_idx": 2, "func_label": 13, "best_z": 0, "has_unique_anat_match": True, "response_class": "low activity", "bpi_data_available": True},
            {"anat_label": 8, "plane_idx": 1, "func_label": 12, "best_z": 1, "has_unique_anat_match": True, "response_class": "low activity", "bpi_data_available": False},
        ],
    )
    geometry = _csv(tmp_path / "geometry.csv", [{"plane_idx": 0, "best_z": 0}, {"plane_idx": 1, "best_z": 1}, {"plane_idx": 2, "best_z": 0}])
    refs = tmp_path / "refs.json"
    refs.write_text(json.dumps([{"index": i, "best_z": z, "anat_label_z_mode": mode,
                                "anat_label_z": z if mode == "direct" else 1-z} for i, z in [(0, 0), (1, 1), (2, 0)]]))
    tifffile.imwrite(functional_dir / "L765_f04_plane2_func_mask_in_2p.tif", func1)
    if mode == "reverse":
        tifffile.imwrite(anatomy_labels, anat[::-1])
        tifffile.imwrite(hcr, hcr_mask[::-1])

    figure = plot_molecular_correspondence_tiles(
        anatomy_path=anatomy, anatomy_labels_path=anatomy_labels,
        hcr_label_paths=[hcr], final_pair_paths=[final], identity_table_path=identity,
        functional_label_dir=functional_dir, fish_id=FISH_ID, plane_refs_path=refs, geometry_table_path=geometry,
    )
    assert len(figure.axes) == 3
    # Both label orientations must retain anatomy intensity at frozen best_z,
    # not silently choose the label-page index or a volume-maximum slice.
    from codeants_2pf_hcr.spatial import norm01
    for ax in figure.axes:
        expected_z = 1 if "→R12" in ax.get_title() else 0
        np.testing.assert_allclose(ax.images[0].get_array()[..., 0], norm01(intensity[expected_z]))
    assert any("flags: is_low_confidence_segmentation" in ax.get_title() for ax in figure.axes)
    assert any("trace/BPI available" in ax.get_title() for ax in figure.axes)
    assert any("trace/BPI unavailable" in ax.get_title() for ax in figure.axes)
    assert sum("H4→A7→R" in ax.get_title() for ax in figure.axes) == 2
    assert "HCR-centric review: showing 3 of 3 persisted selected links" in figure._suptitle.get_text()
    assert {"green", "magenta", "white"}  # colors are rendered as boundary artists, not selection logic.
    plt.close(figure)

    flagged = plot_molecular_correspondence_tiles(
        anatomy_path=anatomy, anatomy_labels_path=anatomy_labels,
        hcr_label_paths=[hcr], final_pair_paths=[final], identity_table_path=identity,
        functional_label_dir=functional_dir, fish_id=FISH_ID, flagged_only=True, plane_refs_path=refs, geometry_table_path=geometry,
    )
    assert len(flagged.axes) == 1
    assert "H5→A8→R12" in flagged.axes[0].get_title()
    assert "HCR-centric review: showing 1 of 3 persisted selected links" in flagged._suptitle.get_text()
    assert len(pd.read_csv(final)) == 2  # plotting has not changed final-pair membership.
    plt.close(flagged)
    refs.unlink()
    with pytest.raises(FileNotFoundError):
        plot_molecular_correspondence_tiles(
            anatomy_path=anatomy, anatomy_labels_path=anatomy_labels,
            hcr_label_paths=[hcr], final_pair_paths=[final], identity_table_path=identity,
            functional_label_dir=functional_dir, fish_id=FISH_ID, plane_refs_path=refs, geometry_table_path=geometry,
        )


def test_activity_export_qc_preserves_scopes_and_session_provenance(tmp_path: Path) -> None:
    master = _csv(
        tmp_path / "functional_roi_activity_identity.csv",
        [
            {"fish_id": FISH_ID, "plane_idx": 0, "func_label": 1, "session_id": "r1", "response_is_active": True, "response_class": "responsive", "bpi": 0.6, "bpi_category": "bout-responsive"},
            {"fish_id": FISH_ID, "plane_idx": 5, "func_label": 2, "session_id": "r2", "response_is_active": False, "response_class": "low activity", "bpi": 0.0, "bpi_category": "low-activity"},
        ],
    )
    bpi = _csv(
        tmp_path / "functional_roi_activity_bpi_cells.csv",
        [
            {"fish_id": FISH_ID, "plane_idx": 0, "func_label": 1, "session_id": "r1", "response_is_active": True, "bpi": 0.6, "bpi_category": "bout-responsive"},
            {"fish_id": FISH_ID, "plane_idx": 5, "func_label": 2, "session_id": "r2", "response_is_active": False, "bpi": 0.0, "bpi_category": "low-activity"},
        ],
    )
    hcr_status = _csv(tmp_path / "hcr_activity_status.csv", [{"fish_id": FISH_ID, "gene": "sst1.2", "anat_label": 8, "functional_status": "responsive", "represented_on_func_plane": True, "selection_rule": "accepted", "match_policy_version": "v1"}])
    hcr_pairs = _csv(tmp_path / "conf_to_func_pairs.csv", [{"fish_id": FISH_ID, "gene": "sst1.2", "conf_label": 3, "anat_label": 8, "plane_idx": 0, "plane": 0, "func_label": 1, "response_is_active": True, "response_class": "responsive", "is_selected_for_analysis": True, "match_policy_version": "v1"}])
    traces = _csv(tmp_path / "suite2p_dff_traces_meta.csv", [{"fish_id": FISH_ID, "plane_idx": 0, "func_label": 1, "session_id": "r1"}])
    figure_path = tmp_path / "summary.png"
    figure_path.write_bytes(b"png")

    report = inspect_activity_export_qc(
        fish_id=FISH_ID,
        pipeline_root=tmp_path,
        roi_master_path=master,
        bpi_cells_path=bpi,
        hcr_status_path=hcr_status,
        hcr_pairs_path=hcr_pairs,
        trace_meta_path=traces,
        expected_figure_paths=[figure_path],
    )

    assert report["status"] == "ready_for_review"
    assert report["scope_summary"]["scope"].str.startswith(("ROI-centric", "HCR-centric")).all()
    assert report["scope_summary"]["rows"].tolist() == [2, 2, 1, 1]
    figure = plot_activity_export_qc(report)
    assert len(figure.axes) == 2
    plt.close(figure)


def test_activity_export_qc_blocks_missing_session_provenance(tmp_path: Path) -> None:
    row = {"plane_idx": 0, "func_label": 1, "response_is_active": True, "response_class": "responsive", "bpi": 0.2, "bpi_category": "both-responsive"}
    master = _csv(tmp_path / "master.csv", [row])
    bpi = _csv(tmp_path / "bpi.csv", [{k: v for k, v in row.items() if k != "response_class"}])
    report = inspect_activity_export_qc(
        fish_id=FISH_ID,
        pipeline_root=tmp_path,
        roi_master_path=master,
        bpi_cells_path=bpi,
        hcr_status_path=None,
        hcr_pairs_path=None,
    )
    assert report["status"] == "blocked"
    assert report["issues"]["detail"].str.contains("Session-aware").any()


def test_activity_bpi_gate_uses_persisted_thresholds_and_all_categories(tmp_path: Path) -> None:
    table = _csv(
        tmp_path / "functional_roi_activity_bpi_cells.csv",
        [
            {"mean_bout_auc_dff": 0.08, "mean_cont_auc_dff": 0.02, "response_magnitude_auc": 0.05, "activity_mag": 0.08, "bpi": 0.6, "response_is_active": True, "response_class": "bout-responsive", "bpi_category": "bout-responsive", "bpi_preference": "bout-preferential", "bpi_zero_band": 0.5, "bpi_activity_threshold": 0.05},
            {"mean_bout_auc_dff": 0.02, "mean_cont_auc_dff": 0.09, "response_magnitude_auc": 0.055, "activity_mag": 0.09, "bpi": -0.63, "response_is_active": True, "response_class": "continuous-responsive", "bpi_category": "continuous-responsive", "bpi_preference": "continuous-preferential", "bpi_zero_band": 0.5, "bpi_activity_threshold": 0.05},
            {"mean_bout_auc_dff": 0.01, "mean_cont_auc_dff": 0.01, "response_magnitude_auc": 0.01, "activity_mag": 1.2, "bpi": 0.8, "response_is_active": False, "response_class": "low activity", "bpi_category": "low activity", "bpi_preference": "not-responsive", "bpi_zero_band": 0.5, "bpi_activity_threshold": 0.05},
            {"mean_bout_auc_dff": 0.0, "mean_cont_auc_dff": 0.0, "response_magnitude_auc": 0.0, "activity_mag": 1.4, "bpi": np.nan, "response_is_active": False, "response_class": "low activity", "bpi_category": "low activity", "bpi_preference": "not-responsive", "bpi_zero_band": 0.5, "bpi_activity_threshold": 0.05},
            {"mean_bout_auc_dff": 0.04, "mean_cont_auc_dff": 0.04, "response_magnitude_auc": 0.04, "activity_mag": 0.04, "bpi": 0.0, "response_is_active": False, "response_class": "response unavailable", "bpi_category": "response unavailable", "bpi_preference": "unavailable", "bpi_zero_band": 0.5, "bpi_activity_threshold": 0.05},
        ],
    )
    work, thresholds = build_activity_bpi_gate_table(table)
    assert len(work) == 5
    assert thresholds == {"bpi_zero_band": 0.5, "bpi_activity_threshold": 0.05}
    figure, plotted = plot_activity_bpi_gate(table, fish_id=FISH_ID)
    assert len(figure.axes) == 6
    assert set(plotted["bpi_category"]) == {"bout-responsive", "continuous-responsive", "low activity", "response unavailable"}
    assert len(figure.axes[1].lines) == 3  # zero and the two unitless BPI preference guides
    assert "effect-size rule, not a contrast test" in figure.axes[1].get_title()
    assert "n=4 assessable" in figure.axes[0].get_title()
    assert sum(len(collection.get_offsets()) for collection in figure.axes[1].collections) == 2
    assert figure.axes[3].get_ylim()[1] >= 1.4
    assert figure.axes[2].get_ylabel() == "Fraction response-positive"
    plt.close(figure)


@pytest.mark.parametrize("failure", [None, "ambiguous", "missing_plane", "reference_mismatch", "source_mismatch"])
def test_activity_sessions_resolve_persisted_windows_without_schema_changes(tmp_path: Path, failure) -> None:
    import json
    master_rows = [
        {"fish_id": FISH_ID, "plane_idx": plane, "func_label": 1, "func_source": str(tmp_path / "suite2P" / f"plane{plane}"), "response_is_active": True, "response_class": "bout-responsive", "bpi": 0.8, "bpi_category": "bout-responsive"}
        for plane in (0, 5)
    ]
    if failure == "source_mismatch":
        master_rows[0]["func_source"] = str(tmp_path / "suite2P" / "plane5")
    master = _csv(tmp_path / "master.csv", master_rows)
    bpi = _csv(tmp_path / "bpi.csv", [{k: v for k, v in row.items() if k != "func_source"} for row in master_rows])
    window_rows = [{"fish_id": FISH_ID, "plane_idx": 0, "session_label": "r1"}, {"fish_id": FISH_ID, "plane_idx": 5, "session_label": "r2"}]
    if failure == "ambiguous":
        window_rows.append({"fish_id": FISH_ID, "plane_idx": 0, "session_label": "r2"})
    if failure == "missing_plane":
        window_rows.pop()
    windows = _csv(tmp_path / "scored_stimulus_windows.csv", window_rows)
    reference = tmp_path / "reference_manifest.json"
    reference.write_text(json.dumps({"fish_id": FISH_ID, "parameters": {"frame_selection_by_plane": {
        "L765_f04_plane0_mcorrected_flipX": {"session_label": "r2" if failure == "reference_mismatch" else "r1"},
        "L765_f04_plane5_mcorrected_flipX": {"session_label": "r2"},
    }}}))
    report = inspect_activity_export_qc(fish_id=FISH_ID, pipeline_root=tmp_path, roi_master_path=master,
        bpi_cells_path=bpi, hcr_status_path=None, hcr_pairs_path=None, require_hcr_exports=False,
        require_trace_meta=False, scored_windows_path=windows, reference_manifest_path=reference)
    assert report["status"] == ("ready_for_review" if failure is None else "blocked")
    assert "session_id" not in pd.read_csv(master).columns


def test_molecular_geometry_reads_geometry_writers_twop_label_column(tmp_path: Path) -> None:
    pairs = _csv(tmp_path / "mask_matches.csv", [
        {"conf_label": 1, "twoP_label": 11}, {"conf_label": 1, "twoP_label": 12},
        {"conf_label": 2, "twoP_label": 13},
    ])
    report = inspect_molecular_geometry_qc(fish_id=FISH_ID, pipeline_root=tmp_path,
        track_artifacts={name: [tmp_path] for name in MOLECULAR_GEOMETRY_TRACKS},
        segmentation_table_path=None, pair_candidates_path=[pairs], pair_candidates_state="expected")
    assert report["status"] == "ready_for_review"
    assert report["pair_ambiguity"].set_index("conf_label").candidate_anatomy_labels.to_dict() == {1: 2, 2: 1}


@pytest.mark.parametrize("payload", [[], {"parameters": []}, {"parameters": {"frame_selection_by_plane": []}}, {"parameters": {"frame_selection_by_plane": {"fish_plane0": []}}}])
def test_activity_sessions_block_malformed_reference_without_crashing(tmp_path: Path, payload) -> None:
    import json
    row = {"plane_idx": 0, "func_label": 1, "response_is_active": True, "response_class": "bout-responsive", "bpi": 0.8, "bpi_category": "bout-responsive"}
    master = _csv(tmp_path / "master.csv", [row])
    bpi = _csv(tmp_path / "bpi.csv", [row])
    windows = _csv(tmp_path / "windows.csv", [{"plane_idx": 0, "session_label": "r1"}])
    if isinstance(payload, dict):
        payload["fish_id"] = FISH_ID
    reference = tmp_path / "manifest.json"
    reference.write_text(json.dumps(payload))
    report = inspect_activity_export_qc(fish_id=FISH_ID, pipeline_root=tmp_path,
        roi_master_path=master, bpi_cells_path=bpi, hcr_status_path=None, hcr_pairs_path=None,
        require_hcr_exports=False, require_trace_meta=False, scored_windows_path=windows,
        reference_manifest_path=reference)
    assert report["status"] == "blocked"
    assert report["issues"].query("area == 'reference session provenance' and status == 'blocked'").shape[0] == 1
