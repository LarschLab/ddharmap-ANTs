from __future__ import annotations

import hashlib
import json
from pathlib import Path

import matplotlib
import numpy as np
import pandas as pd
import pytest

matplotlib.use("Agg")

from codeants_2pf_hcr.plots.qc_midline import (
    build_midline_laterality_qc_tables,
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
