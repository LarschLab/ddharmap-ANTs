from __future__ import annotations

import hashlib
import json
from pathlib import Path
from unittest.mock import patch

import matplotlib
import numpy as np
import pandas as pd
import pytest

matplotlib.use("Agg")

from codeants_2pf_hcr.midline_review import (
    load_accepted_anatomy_midline_context,
    project_native_midline_to_anatomy,
)
from codeants_2pf_hcr.plots.qc_midline import (
    build_midline_laterality_qc_tables,
    build_left_right_index_qc_table,
    render_left_right_index_histogram,
    render_left_right_index_plane_grid,
    render_midline_activity_plane_grid,
    render_midline_laterality_plane_grid,
)


FISH = "L765_f04"


def _csv(path: Path, rows: list[dict]) -> Path:
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def _sidecar(path: Path, *, accepted: bool = True, space: str = "anatomy pixel grid") -> Path:
    source = path.with_name("review-source.csv")
    source.write_text("source\nmidline\n")
    payload = {
        "schema": "codeants_midline_annotation_review_v1", "fish_id": FISH,
        "accepted": accepted, "coordinate_space": space,
        "source_artifacts": [{"path": str(source), "sha256": hashlib.sha256(source.read_bytes()).hexdigest()}],
        "lines_by_plane": {"0": {"x0": 5, "y0": 5, "theta_deg": -90}},
    }
    path.write_text(json.dumps(payload))
    return path


def test_midline_qc_uses_all_roi_inventory_and_exact_persisted_activity_subset(tmp_path: Path) -> None:
    master = _csv(tmp_path / "master.csv", [
        {"fish_id": FISH, "plane_idx": 0, "func_label": 1, "centroid_x_anat": 7, "centroid_y_anat": 4, "gene": "x"},
        {"fish_id": FISH, "plane_idx": 0, "func_label": 2, "centroid_x_anat": 3, "centroid_y_anat": 4, "gene": "y"},
        {"fish_id": FISH, "plane_idx": 0, "func_label": 3, "centroid_x_anat": 5, "centroid_y_anat": 4, "gene": "z"},
    ])
    bpi = _csv(tmp_path / "bpi.csv", [
        {"fish_id": FISH, "plane_idx": 0, "func_label": 1, "activity_mag": 3.0},
        {"fish_id": FISH, "plane_idx": 0, "func_label": 2, "activity_mag": float("nan")},
    ])
    all_rois, activity, meta = build_midline_laterality_qc_tables(master, bpi, _sidecar(tmp_path / "accepted.json"), fish_id=FISH)
    assert len(all_rois) == 3
    assert all_rois["midline_side"].tolist() == ["left", "right", "midline"]
    assert activity["func_label"].tolist() == [1]
    assert set(activity.columns).isdisjoint({"gene"})  # identity never controls this subset
    anatomy = np.zeros((1, 10, 10))
    fig1 = render_midline_laterality_plane_grid(all_rois, meta["midline_context"], anatomy, {0: 0})
    fig2 = render_midline_activity_plane_grid(activity, meta["midline_context"], anatomy, {0: 0})
    assert len(fig1.axes) >= 1 and len(fig2.axes) >= 1


def test_midline_qc_rejects_unaccepted_or_native_functional_sidecars(tmp_path: Path) -> None:
    master = _csv(tmp_path / "master.csv", [{"fish_id": FISH, "plane_idx": 0, "func_label": 1, "centroid_x_anat": 7, "centroid_y_anat": 4}])
    bpi = _csv(tmp_path / "bpi.csv", [{"fish_id": FISH, "plane_idx": 0, "func_label": 1, "activity_mag": 3.0}])
    with pytest.raises(ValueError, match="explicitly accepted"):
        build_midline_laterality_qc_tables(master, bpi, _sidecar(tmp_path / "draft.json", accepted=False), fish_id=FISH)
    with pytest.raises(ValueError, match="anatomy pixel grid"):
        build_midline_laterality_qc_tables(master, bpi, _sidecar(tmp_path / "native.json", space="native functional pixel grid"), fish_id=FISH)


def test_midline_qc_rejects_sidecar_when_review_artifact_changed(tmp_path: Path) -> None:
    master = _csv(tmp_path / "master.csv", [{"fish_id": FISH, "plane_idx": 0, "func_label": 1, "centroid_x_anat": 7, "centroid_y_anat": 4}])
    bpi = _csv(tmp_path / "bpi.csv", [{"fish_id": FISH, "plane_idx": 0, "func_label": 1, "activity_mag": 3.0}])
    sidecar = _sidecar(tmp_path / "accepted.json")
    sidecar.with_name("review-source.csv").write_text("changed after review\n")
    with pytest.raises(ValueError, match="artifact hash changed"):
        build_midline_laterality_qc_tables(master, bpi, sidecar, fish_id=FISH)


def test_materialized_midline_requires_fish_specific_proposal_hash_gate(tmp_path: Path) -> None:
    csv_path = tmp_path / "anatomy_midline_proposals.csv"
    png_path = tmp_path / "anatomy_midline_proposals.png"
    csv_path.write_text("""plane_idx,x0,y0
0,5,5
""")
    png_path.write_bytes(b"reviewed overlay")
    csv_hash = hashlib.sha256(csv_path.read_bytes()).hexdigest()
    png_hash = hashlib.sha256(png_path.read_bytes()).hexdigest()
    logbook = tmp_path / "decision.md"
    logbook.write_text(f"""---
fish_id: {FISH}
midline_gate: accepted
midline_proposal_csv_sha256: {csv_hash}
midline_proposal_png_sha256: {png_hash}
---
""")
    artifact = tmp_path / "anatomy_midline.json"
    artifact.write_text(json.dumps({
        "schema": "codeants_anatomy_midline_materialization_v1",
        "fish_id": FISH, "accepted": True,
        "coordinate_space": "anatomy pixel grid",
        "logbook_decision_path": str(logbook),
        "lines_by_plane": {"0": {"x0": 5, "y0": 5, "theta_deg": -90}},
        "source_artifacts": [
            {"path": str(csv_path), "sha256": csv_hash},
            {"path": str(png_path), "sha256": png_hash},
        ],
    }))
    context = load_accepted_anatomy_midline_context(artifact, fish_id=FISH, required_planes=[0])
    assert context["review"]["accepted"] is True
    logbook.write_text(logbook.read_text().replace("midline_gate: accepted", "midline_gate: review_required"))
    with pytest.raises(ValueError, match="midline_gate"):
        load_accepted_anatomy_midline_context(artifact, fish_id=FISH, required_planes=[0])


def test_native_midline_projection_uses_saved_scale_and_ncc_placement() -> None:
    ref = {"index": 0, "best_z": 2, "ref_shape": [10, 10],
           "ref_scaled_shape": [20, 20], "ncc_xy": {"x0": 3, "y0": 4}}
    with patch("codeants_2pf_hcr.matching.resolve_plane_transform", return_value={"type": "identity"}), patch(
        "codeants_2pf_hcr.matching.transform_points_between_spaces",
        side_effect=lambda x, y, transform: (x, y),
    ):
        row = project_native_midline_to_anatomy(
            ref, x0=5, y0=5, theta_deg=0, anatomy_shape=(30, 30), n_samples=5,
        )
    assert row["plane_idx"] == 0
    assert row["best_z"] == 2
    assert row["x0"] == pytest.approx(13)
    assert row["y0"] == pytest.approx(14)
    assert row["fit_rmse_px"] == pytest.approx(0)


def test_ncc_xy_plane_maps_native_rois_and_midline_with_scale_once() -> None:
    from codeants_2pf_hcr.matching import harmonize_functional_labels_to_anatomy

    ref = {"index": 0, "best_z": 2, "tform_src": "ncc_xy", "ref_shape": [10, 10],
           "ref_scaled_shape": [20, 20], "ncc_xy": {"x0": 3, "y0": 4}}
    row = project_native_midline_to_anatomy(ref, x0=5, y0=5, theta_deg=0, anatomy_shape=(30, 30), n_samples=5)
    assert (row["x0"], row["y0"]) == pytest.approx((2 * 5 + 0.5 + 3, 2 * 5 + 0.5 + 4))
    labels = np.zeros((10, 10), dtype=np.uint16)
    labels[4:6, 4:6] = 1
    warped = harmonize_functional_labels_to_anatomy(labels, ref, (30, 30))["labels"]
    ys, xs = np.nonzero(warped == 1)
    assert len(xs) == 16
    # resize-centred scaling: native label centre 4.5 maps to 2 * 4.5 + 0.5 before NCC placement.
    assert (xs.mean(), ys.mean()) == pytest.approx((2 * 4.5 + 0.5 + 3, 2 * 4.5 + 0.5 + 4), abs=0.5)


def test_native_midline_proposals_round_trip_exactly_through_csv(tmp_path: Path) -> None:
    from codeants_2pf_hcr.pipeline import _read_native_midline_proposals

    values = np.random.default_rng(0).uniform(-500, 500, size=(200, 3))
    frame = pd.DataFrame(values, columns=["fixed_x0", "fixed_y0", "fixed_theta_deg"])
    path = tmp_path / "functional_automatic_midline_proposals.csv"
    frame.to_csv(path, index=False)
    assert _read_native_midline_proposals(path).to_numpy().tobytes() == frame.to_numpy().tobytes()


def test_midline_qc_retains_scored_rois_without_anatomy_centroids_as_unknown(tmp_path: Path) -> None:
    fish_root = tmp_path / FISH
    fish_root.mkdir()
    master = _csv(fish_root / "master.csv", [
        {"fish_id": FISH, "plane_idx": 0, "func_label": 1, "centroid_x_anat": 7, "centroid_y_anat": 4},
        {"fish_id": FISH, "plane_idx": 0, "func_label": 2, "centroid_x_anat": np.nan, "centroid_y_anat": np.nan},
    ])
    bpi = _csv(fish_root / "bpi.csv", [
        {"plane_idx": 0, "func_label": 1, "activity_mag": 1.0},
        {"plane_idx": 0, "func_label": 2, "activity_mag": 2.0},
    ])
    all_rois, activity, _ = build_midline_laterality_qc_tables(
        master, bpi, _sidecar(fish_root / "accepted.json"), fish_id=FISH,
    )
    assert len(all_rois) == 2
    assert len(activity) == 2
    assert activity.set_index("func_label").loc[2, "midline_side"] == "unknown"


def test_left_right_index_review_pairs_by_roi_side_and_flags_signed_auc(tmp_path: Path) -> None:
    rois = pd.DataFrame([
        {"plane_idx": 0, "func_label": 1, "centroid_x_anat": 7, "centroid_y_anat": 4, "midline_side": "left", "bout_response_pass": True, "cont_response_pass": False},
        {"plane_idx": 0, "func_label": 2, "centroid_x_anat": 3, "centroid_y_anat": 4, "midline_side": "right", "bout_response_pass": True, "cont_response_pass": False},
        {"plane_idx": 0, "func_label": 3, "centroid_x_anat": 8, "centroid_y_anat": 6, "midline_side": "left", "bout_response_pass": True, "cont_response_pass": False},
        {"plane_idx": 0, "func_label": 4, "centroid_x_anat": 2, "centroid_y_anat": 6, "midline_side": "right", "bout_response_pass": True, "cont_response_pass": False},
    ])
    points = []
    for label, ipsi, contra in ((1, 0.6, 0.2), (2, 0.2, 0.6), (3, -0.2, 0.2), (4, 0.3, -0.1)):
        for laterality, auc in (("ipsi", ipsi), ("contra", contra)):
            points.append({
                "group": "All neurons", "plane_idx": 0, "func_label": label,
                "stim_mode": "bout", "laterality": laterality, "auc_dff": auc,
                "response_is_active": True, "accepted_midline_sidecar_sha256": "review-hash",
            })
    source = _csv(tmp_path / "points.csv", points)
    context = {"sidecar_sha256": "review-hash", "review": {"lines_by_plane": {"0": {"x0": 5, "y0": 5, "theta_deg": -90}}}}
    rows = build_left_right_index_qc_table(rois, source, context)
    keyed = rows.set_index("func_label")
    assert keyed.loc[1, "left_auc"] == pytest.approx(0.6)
    assert keyed.loc[2, "left_auc"] == pytest.approx(0.6)
    assert keyed.loc[1, "lri"] == pytest.approx(0.5)
    assert keyed.loc[2, "lri"] == pytest.approx(0.5)
    assert keyed.loc[3, "lri_qc_class"] == "unavailable"
    assert keyed.loc[4, "lri_qc_class"] == "negative AUC review"
    assert (keyed.loc[[1, 2], "lri_qc_class"] == "left-preferring").all()
    wider_guide = build_left_right_index_qc_table(rois, source, context, threshold=0.6)
    assert (wider_guide.set_index("func_label").loc[[1, 2], "lri_qc_class"] == "within threshold").all()
    fig = render_left_right_index_plane_grid(rows, context, np.zeros((1, 10, 10)), {0: 0})
    assert len(fig.axes) == 2
    histogram = render_left_right_index_histogram(rows)
    assert len(histogram.axes) == 2
    assert all(ax.get_xlim()[0] > ax.get_xlim()[1] for ax in histogram.axes)
    assert sum(bar.get_height() for bar in histogram.axes[0].patches if hasattr(bar, "get_height") and bar.get_height() > 0) == 2
    assert sum(bar.get_height() for bar in histogram.axes[1].patches if hasattr(bar, "get_height") and bar.get_height() > 0) == 0
    with pytest.raises(ValueError, match="accepted anatomy midline"):
        build_left_right_index_qc_table(rois, source, {**context, "sidecar_sha256": "other"})
