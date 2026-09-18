from __future__ import annotations

import hashlib
import json
from pathlib import Path

import matplotlib
import numpy as np
import pandas as pd
import pytest

matplotlib.use("Agg")

from codeants_2pf_hcr.plots.qc_identity_functional import (
    build_marker_trace_rows,
    build_population_response_matrix,
    build_status_coverage_tables,
    load_identity_functional_qc_bundle,
    render_marker_stimulus_traces,
    render_marker_auc_review,
    render_population_response_raster,
    render_status_coverage,
)

FISH = "L765_f04"
STIMULI = [
    "LB_trajectory", "RB_trajectory", "LC_trajectory", "RC_trajectory",
    "LAB_trajectory", "RAB_trajectory", "Ll_RB_trajectory",
    "Rl_RB_trajectory", "WFCl", "WFCo",
]


def _write_bundle(tmp_path: Path) -> dict:
    master = pd.DataFrame([
        {"fish_id": FISH, "plane_idx": 0, "func_label": 1, "identity_label": "pth2",
         "response_is_active": True, "response_class": "bout-responsive",
         "bpi_category": "bout-responsive",
         "centroid_x_anat": 2.0, "centroid_y_anat": 3.0},
        {"fish_id": FISH, "plane_idx": 0, "func_label": 2, "identity_label": np.nan,
         "response_is_active": True, "response_class": "continuous-responsive",
         "bpi_category": "continuous-responsive",
         "centroid_x_anat": 7.0, "centroid_y_anat": 4.0},
        {"fish_id": FISH, "plane_idx": 0, "func_label": 3, "identity_label": np.nan,
         "response_is_active": True, "response_class": "low activity",
         "bpi_category": "low activity",
         "centroid_x_anat": 5.0, "centroid_y_anat": 6.0},
    ])
    status = pd.DataFrame([
        {"fish_id": FISH, "gene": "pth2", "anat_label": i + 1, "functional_status": value}
        for i, value in enumerate([
            "in-plane responsive ROI", "in-plane low-activity ROI",
            "in-plane response unavailable", "in-plane no functional ROI candidate",
            "out-of-plane anatomy label",
        ])
    ])
    pairs = pd.DataFrame([
        {"fish_id": FISH, "gene": gene, "plane": 0, "func_label": 1,
         "response_is_active": True, "response_class": "bout-responsive",
         "is_selected_for_analysis": True}
        for gene in ("pth2", "sst1.2")
    ])
    summary_rows = []
    traces = []
    for func_label in (1, 2, 3):
        for stim_index, stimulus in enumerate(reversed(STIMULI)):
            summary_rows.append({
                "fish_id": FISH, "plane_idx": 0, "func_label": func_label,
                "session_label": "r1", "stim_type": stimulus,
                "mean_dff_post": float(func_label + stim_index / 10),
                "any_presented_response_state": "responsive" if func_label < 3 else "nonresponsive",
            })
            traces.append(np.linspace(0, func_label + stim_index, 5, dtype=np.float32))
    summary = pd.DataFrame(summary_rows)
    paths = {}
    for name, frame in (
        ("master", master), ("status", status), ("pairs", pairs), ("summary", summary),
    ):
        paths[name] = tmp_path / f"{name}.csv"
        frame.to_csv(paths[name], index=False)
    points = pd.DataFrame([
        {"fish_id": FISH, "group": "pth2", "laterality": "ipsi", "stim_mode": "bout",
         "auc_dff": 0.2, "accepted_midline_sidecar_sha256": "hash"}
    ])
    counts = pd.DataFrame([
        {"fish_id": FISH, "group": "pth2", "laterality": "ipsi", "stim_mode": "bout",
         "n_total": 1, "accepted_midline_sidecar_sha256": "hash"}
    ])
    paths["points"], paths["counts"] = tmp_path / "points.csv", tmp_path / "counts.csv"
    points.to_csv(paths["points"], index=False)
    counts.to_csv(paths["counts"], index=False)
    paths["payload"] = tmp_path / "payload.npz"
    metadata = {
        "stim_order": list(reversed(STIMULI)), "session_colors": {"r1": "blue"},
        "duration_by_stim": {stimulus: 20.0 for stimulus in STIMULI},
        "motion_onset_delay_sec": 10.0,
    }
    np.savez_compressed(
        paths["payload"], traces=np.asarray(traces), tvec=np.arange(5, dtype=np.float32),
        summary_sha256=hashlib.sha256(paths["summary"].read_bytes()).hexdigest(),
        plot_metadata_json=json.dumps(metadata),
    )
    return load_identity_functional_qc_bundle(
        fish_id=FISH, roi_master_path=paths["master"], hcr_status_path=paths["status"],
        hcr_pairs_path=paths["pairs"], trace_summary_path=paths["summary"],
        trace_payload_path=paths["payload"], motion_auc_points_path=paths["points"],
        motion_auc_counts_path=paths["counts"],
    )


def test_population_matrix_uses_all_complete_cells_and_fixed_pair_order(tmp_path: Path) -> None:
    bundle = _write_bundle(tmp_path)
    side = pd.DataFrame([
        {"plane_idx": 0, "func_label": 1, "midline_side": "left"},
        {"plane_idx": 0, "func_label": 2, "midline_side": "right"},
        {"plane_idx": 0, "func_label": 3, "midline_side": "midline"},
    ])
    matrix = build_population_response_matrix(bundle, midline_rows=side)
    assert matrix["normalized"].shape == (3, 10)
    assert matrix["stim_order"][:4] == ["LB_trajectory", "RB_trajectory", "LC_trajectory", "RC_trajectory"]
    assert matrix["n_dropped_incomplete"] == 0
    assert set(matrix["annotations"]["identity_bucket"]) == {"pth2", "unidentified"}
    assert len(render_population_response_raster(matrix, fish_id=FISH).axes) >= 5


def test_hcr_trace_rows_allow_coexpression_without_duplicate_gene_rows(tmp_path: Path) -> None:
    bundle = _write_bundle(tmp_path)
    rows = build_marker_trace_rows(bundle)
    assert len(rows) == 20
    assert rows[["gene", "plane_idx", "func_label", "stim_type"]].duplicated().sum() == 0
    assert rows[["plane_idx", "func_label"]].drop_duplicates().shape[0] == 1
    figure, plotted = render_marker_stimulus_traces(bundle)
    assert len(figure.axes) == 10
    assert set(plotted["gene"]) == {"pth2", "sst1.2"}


def test_status_and_coverage_keep_hcr_and_roi_denominators_separate(tmp_path: Path) -> None:
    bundle = _write_bundle(tmp_path)
    status, coverage, coverage_rows = build_status_coverage_tables(bundle)
    assert status["count"].sum() == 5
    assert dict(zip(coverage["coverage"], coverage["count"])) == {
        "accepted HCR identity": 1, "unidentified": 1,
    }
    assert len(coverage_rows) == 2
    figure, tables = render_status_coverage(bundle)
    assert len(figure.axes) == 2
    assert tables["coverage"]["count"].sum() == 2


def test_loader_rejects_mixed_generation_trace_payload(tmp_path: Path) -> None:
    bundle = _write_bundle(tmp_path)
    summary_path = bundle["paths"]["trace_summary"]
    summary_path.write_text(summary_path.read_text() + "\n")
    with pytest.raises(RuntimeError, match="different runs"):
        load_identity_functional_qc_bundle(
            fish_id=FISH, roi_master_path=bundle["paths"]["master"],
            hcr_status_path=bundle["paths"]["hcr_status"],
            hcr_pairs_path=bundle["paths"]["hcr_pairs"],
            trace_summary_path=summary_path, trace_payload_path=bundle["paths"]["trace_payload"],
            motion_auc_points_path=bundle["paths"]["motion_auc_points"],
            motion_auc_counts_path=bundle["paths"]["motion_auc_counts"],
        )


def test_marker_auc_requires_ready_global_laterality_gate(tmp_path: Path) -> None:
    bundle = _write_bundle(tmp_path)
    with pytest.raises(RuntimeError, match="ready Q7 global laterality"):
        render_marker_auc_review(
            bundle, accepted_midline_context={"sidecar_sha256": "hash"},
            global_laterality_status="blocked",
        )
