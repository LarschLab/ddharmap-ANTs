from __future__ import annotations

import hashlib
import json
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
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
                "any_presented_response_state": "responsive" if func_label < 3 else "nonresponsive",
            })
            traces.append(np.linspace(0, func_label + stim_index, 5, dtype=np.float32))
    summary = pd.DataFrame(summary_rows)
    presented = summary[["fish_id", "plane_idx", "func_label", "session_label", "stim_type"]].copy()
    presented["mean_auc_dff"] = [float(row["func_label"] + (i % 10)/10) for i, row in enumerate(summary_rows)]
    paths_presented = tmp_path / "presented.csv"
    presented.to_csv(paths_presented, index=False)
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
        motion_auc_counts_path=paths["counts"], presented_response_path=paths_presented,
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
            presented_response_path=bundle["paths"]["presented_response"],
        )


def test_marker_auc_requires_ready_global_laterality_gate(tmp_path: Path) -> None:
    bundle = _write_bundle(tmp_path)
    with pytest.raises(RuntimeError, match="ready Q7 global laterality"):
        render_marker_auc_review(
            bundle, accepted_midline_context={"sidecar_sha256": "hash"},
            global_laterality_status="blocked",
        )


def test_curated_marker_traces_preserve_every_trace_and_add_label_denominator_rings(tmp_path):
    from codeants_2pf_hcr.plots.qc_identity_functional import render_marker_traces_with_hcr_status
    import matplotlib.pyplot as plt
    bundle = _write_bundle(tmp_path)
    original, original_rows = render_marker_stimulus_traces(bundle)
    curated, rows = render_marker_traces_with_hcr_status(bundle)
    pd.testing.assert_frame_equal(rows, original_rows)
    assert len(curated.axes) == len(original.axes) + 2
    for before, after in zip(original.axes, curated.axes):
        assert len(before.lines) == len(after.lines)
        for a, b in zip(before.lines, after.lines):
            np.testing.assert_array_equal(a.get_ydata(), b.get_ydata())
            np.testing.assert_array_equal(a.get_xdata(), b.get_xdata())
    assert any('n=5' in t.get_text() for t in curated.axes[-1].texts)
    legends = {legend.get_title().get_text(): legend for legend in curated.legends}
    assert 'Inner ring: location' in legends
    outer = legends['Outer ring: functional status']
    assert [t.get_text() for t in outer.get_texts()] == [
        'Responsive ROI', 'Low activity', 'Response unavailable',
        'No functional ROI candidate', 'Out-of-plane', 'Unmatched']
    assert (curated.axes[-2].get_position().x0 - curated.axes[4].get_position().x1) * curated.get_size_inches()[0] < .25
    plt.close(original); plt.close(curated)


def test_population_matrix_reads_signed_auc_independently_of_display_traces(tmp_path):
    bundle = _write_bundle(tmp_path)
    # Display traces contain no scalar response measurements. The matrix's raw
    # values must come directly from the separate persisted scorer-window AUCs.
    matrix = build_population_response_matrix(bundle)
    expected = bundle['presented_response'].pivot(index=['plane_idx','func_label'], columns='stim_type', values='mean_auc_dff')
    for i, row in matrix['annotations'].iterrows():
        np.testing.assert_allclose(matrix['raw'][i], expected.loc[(row.plane_idx,row.func_label), matrix['stim_order']])
    figure = render_population_response_raster(matrix, fish_id=FISH)
    assert any('normalized signed AUC' in ax.get_ylabel() for ax in figure.axes)
    figure.canvas.draw()
    colorbar, magnitude = figure.axes[-2:]
    # Reserve only enough space for ticks/label, without colorbar shrinkage
    # stealing a large blank region from the matrix's gridspec cell.
    assert 0 < magnitude.get_position().x0 - colorbar.get_position().x1 < .07
    label_extent = colorbar.yaxis.label.get_window_extent()
    assert label_extent.x1 < magnitude.get_window_extent().x0
    plt.close(figure)
