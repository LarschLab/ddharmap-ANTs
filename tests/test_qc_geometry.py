import json
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pytest
import tifffile

from codeants_2pf_hcr.plots.qc_geometry import (
    build_legacy_qc_xy_offset_audit,
    geometry_review_queue,
    load_geometry_review_bundle,
    render_centroid_offset_review,
    render_centroid_offset_detail,
    render_legacy_qc_xy_offset_audit,
    render_geometry_placement_overview,
    render_geometry_review_summary,
    summarize_geometry_review,
    summarize_legacy_qc_xy_offset_audit,
)


matplotlib.use("Agg")


def _write_geometry(root: Path) -> None:
    root.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "fish_id": "L765_f04",
                "plane": "plane0",
                "plane_idx": 0,
                "func_label": 1,
                "selected_anat_label": 10,
                "centroid_x_func": 20.0,
                "centroid_y_func": 30.0,
                "centroid_x_func_anat": 20.0,
                "centroid_y_func_anat": 30.0,
                "centroid_x_anat": 21.0,
                "centroid_y_anat": 31.0,
                "selected_dist_um": 2.0,
                "selected_overlap_px": 8,
                "n_overlap_candidates_any": 1,
                "n_overlap_candidates_valid": 1,
                "plane_match_outcome": "anatomy match",
                "claim_outcome": "matched unique anatomy",
                "has_unique_anat_match": True,
            },
            {
                "fish_id": "L765_f04",
                "plane": "plane0",
                "plane_idx": 0,
                "func_label": 2,
                "selected_anat_label": "",
                "centroid_x_func": 40.0,
                "centroid_y_func": 50.0,
                "centroid_x_func_anat": 40.0,
                "centroid_y_func_anat": 50.0,
                "centroid_x_anat": "",
                "centroid_y_anat": "",
                "selected_dist_um": "",
                "selected_overlap_px": "",
                "n_overlap_candidates_any": 0,
                "n_overlap_candidates_valid": 0,
                "plane_match_outcome": "no anatomy overlap candidate",
                "claim_outcome": "no anatomy claim",
                "has_unique_anat_match": False,
            },
            {
                "fish_id": "L765_f04",
                "plane": "plane1",
                "plane_idx": 1,
                "func_label": 1,
                "selected_anat_label": "",
                "centroid_x_func": 60.0,
                "centroid_y_func": 70.0,
                "centroid_x_func_anat": 60.0,
                "centroid_y_func_anat": 70.0,
                "centroid_x_anat": "",
                "centroid_y_anat": "",
                "selected_dist_um": "",
                "selected_overlap_px": "",
                "n_overlap_candidates_any": 2,
                "n_overlap_candidates_valid": 2,
                "plane_match_outcome": "overlap candidate lost in 1-to-1 assignment",
                "claim_outcome": "duplicate anatomy claim lost",
                "has_unique_anat_match": False,
            },
        ]
    ).to_csv(root / "functional_roi_anatomy_matches.csv", index=False)
    pd.DataFrame([{"plane_idx": 0, "n_rois": 2}, {"plane_idx": 1, "n_rois": 1}]).to_csv(
        root / "functional_roi_anatomy_match_by_plane.csv", index=False
    )
    pd.DataFrame([{"plane_idx": 0, "status": "ok"}, {"plane_idx": 1, "status": "ok"}]).to_csv(
        root / "functional_roi_anatomy_match_plane_meta.csv", index=False
    )


def test_load_and_summarize_geometry_review(tmp_path: Path) -> None:
    root = tmp_path / "registration"
    _write_geometry(root)
    bundle = load_geometry_review_bundle(root, expected_fish_id="L765_f04")
    summary = summarize_geometry_review(bundle.matches)

    assert bundle.fish_id == "L765_f04"
    assert summary["n_rois"].sum() == 3
    assert summary["n_unique_matches"].sum() == 1
    assert summary["n_no_overlap"].sum() == 1
    assert summary["n_competition_lost"].sum() == 1
    assert render_geometry_review_summary(summary) is not None
    offsets = render_centroid_offset_review(bundle.matches)
    assert len(offsets.axes) == 2


def test_geometry_review_queue_filters_decision_categories(tmp_path: Path) -> None:
    root = tmp_path / "registration"
    _write_geometry(root)
    bundle = load_geometry_review_bundle(root, expected_fish_id="L765_f04")

    queue = geometry_review_queue(bundle.matches, outcomes=("competition-lost",))

    assert len(queue) == 1
    assert queue.iloc[0]["review_category"] == "competition-lost"
    assert int(queue.iloc[0]["plane_idx"]) == 1


def test_geometry_spatial_reviews_use_saved_anatomy_and_labels(tmp_path: Path) -> None:
    root = tmp_path / "registration"
    _write_geometry(root)
    anatomy = np.arange(320, dtype=np.uint16).reshape(4, 8, 10)
    anatomy_path = tmp_path / "anatomy.tif"
    tifffile.imwrite(anatomy_path, anatomy, photometric="minisblack")
    anatomy_labels = np.zeros((4, 8, 10), dtype=np.uint16)
    anatomy_labels[1, 2:5, 3:7] = 10
    anatomy_labels_path = tmp_path / "anatomy_labels.tif"
    tifffile.imwrite(anatomy_labels_path, anatomy_labels, photometric="minisblack")
    refs_path = tmp_path / "plane_refs_summary.json"
    refs_path.write_text(
        json.dumps(
            [
                {"index": 0, "best_z": 2, "anat_label_z_mode": "reverse", "anat_label_z": 1},
                {"index": 1, "best_z": 3, "anat_label_z_mode": "reverse", "anat_label_z": 0},
            ]
        )
    )
    transformed_root = tmp_path / "transformed"
    labels_dir = transformed_root / "functional" / "anatomy"
    labels_dir.mkdir(parents=True)
    functional_labels = np.zeros((8, 10), dtype=np.uint16)
    functional_labels[2:5, 2:6] = 1
    tifffile.imwrite(labels_dir / "L765_f04_plane0_func_mask_in_2p.tif", functional_labels)
    tifffile.imwrite(labels_dir / "L765_f04_plane1_func_mask_in_2p.tif", functional_labels)
    # macOS sidecars can be present on the mounted acquisition drive; they
    # are not image data and must never shadow the real label TIFF.
    (labels_dir / "._L765_f04_plane0_func_mask_in_2p.tif").write_bytes(b"not a TIFF")

    bundle = load_geometry_review_bundle(
        root,
        expected_fish_id="L765_f04",
        anatomy_stack_path=anatomy_path,
        anatomy_labels_path=anatomy_labels_path,
        plane_refs_path=refs_path,
        transformed_roi_root=transformed_root,
    )

    overview = render_geometry_placement_overview(bundle)
    assert len(overview.axes) == 2
    assert "anatomy Z=2, label Z=1" in overview.axes[0].get_title()
    detail = render_centroid_offset_detail(bundle, plane_idx=0)
    assert len(detail.axes) == 2
    plt.close(overview)
    plt.close(detail)


def test_geometry_loader_rejects_post_geometry_semantics(tmp_path: Path) -> None:
    root = tmp_path / "registration"
    _write_geometry(root)
    path = root / "functional_roi_anatomy_matches.csv"
    df = pd.read_csv(path)
    df["identity_label"] = "sst1.1"
    df.to_csv(path, index=False)

    with pytest.raises(ValueError, match="geometry-only"):
        load_geometry_review_bundle(root, expected_fish_id="L765_f04")


def test_geometry_loader_requires_transformed_functional_centroids(tmp_path: Path) -> None:
    root = tmp_path / "registration"
    _write_geometry(root)
    path = root / "functional_roi_anatomy_matches.csv"
    df = pd.read_csv(path).drop(columns=["centroid_x_func_anat", "centroid_y_func_anat"])
    df.to_csv(path, index=False)

    with pytest.raises(ValueError, match="centroid_x_func_anat"):
        load_geometry_review_bundle(root, expected_fish_id="L765_f04")


def test_geometry_loader_rejects_fish_mismatch(tmp_path: Path) -> None:
    root = tmp_path / "registration"
    _write_geometry(root)
    with pytest.raises(ValueError, match="fish mismatch"):
        load_geometry_review_bundle(root, expected_fish_id="L765_f03")


def test_legacy_qc_xy_offset_audit_intersects_keys_and_uses_physical_centroids(tmp_path: Path) -> None:
    root = tmp_path / "registration"
    _write_geometry(root)
    qc = pd.read_csv(root / "functional_roi_anatomy_matches.csv")
    legacy = qc.copy()
    legacy.loc[legacy["func_label"].eq(1), "selected_dist_um"] = 1.5
    legacy = pd.concat(
        [
            legacy,
            pd.DataFrame(
                [{
                    **legacy.iloc[0].to_dict(),
                    "plane_idx": 9,
                    "func_label": 99,
                    "selected_dist_um": 99.0,
                }]
            ),
        ],
        ignore_index=True,
    )
    audit = build_legacy_qc_xy_offset_audit(legacy, qc, dx_um=0.5, dy_um=0.25)

    assert audit[["plane_idx", "func_label"]].to_dict("records") == [{"plane_idx": 0, "func_label": 1}]
    assert audit.loc[0, "legacy_53a_xy_offset_um"] == pytest.approx(1.5)
    assert audit.loc[0, "qc_xy_offset_um"] == pytest.approx(np.hypot(0.5, 0.25))
    summary = summarize_legacy_qc_xy_offset_audit(audit)
    assert summary.loc[summary["scope"].eq("overall"), "n"].item() == 1
    figure = render_legacy_qc_xy_offset_audit(audit, fish_id="L765_f04")
    assert len(figure.axes) == 2
    plt.close(figure)


def test_legacy_qc_xy_offset_audit_rejects_duplicate_unique_keys(tmp_path: Path) -> None:
    root = tmp_path / "registration"
    _write_geometry(root)
    qc = pd.read_csv(root / "functional_roi_anatomy_matches.csv")
    legacy = pd.concat([qc, qc.iloc[[0]]], ignore_index=True)
    with pytest.raises(ValueError, match="duplicate unique-match keys"):
        build_legacy_qc_xy_offset_audit(legacy, qc, dx_um=1.0, dy_um=1.0)


def test_legacy_qc_xy_offset_audit_fails_closed_without_plane_match_outcome(tmp_path: Path) -> None:
    root = tmp_path / "registration"
    _write_geometry(root)
    qc = pd.read_csv(root / "functional_roi_anatomy_matches.csv")
    with pytest.raises(ValueError, match="plane_match_outcome"):
        build_legacy_qc_xy_offset_audit(qc.drop(columns=["plane_match_outcome"]), qc, dx_um=1.0, dy_um=1.0)


def test_legacy_qc_xy_offset_audit_requires_anatomy_match_outcome(tmp_path: Path) -> None:
    root = tmp_path / "registration"
    _write_geometry(root)
    qc = pd.read_csv(root / "functional_roi_anatomy_matches.csv")
    legacy = qc.copy()
    legacy.loc[legacy["func_label"].eq(1), "plane_match_outcome"] = "no anatomy overlap candidate"
    with pytest.raises(ValueError, match="no intersected unique-match"):
        build_legacy_qc_xy_offset_audit(legacy, qc, dx_um=1.0, dy_um=1.0)


def test_notebook_03_review_writer_is_opt_in() -> None:
    notebook = Path("notebooks/qc/03_roi_anatomy_geometry_qc.ipynb")
    source = notebook.read_text()
    assert "SAVE_REVIEW = False" in source
    assert "DECISION = 'review_required'" in source
