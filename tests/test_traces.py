from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd

from codeants_2pf_hcr.traces import (
    MotionAucPlotConfig,
    annotate_midline_side,
    build_single_fish_motion_auc_plot_tables,
    extract_window_with_padding,
    filter_high_confidence_pairs,
    load_midline_context,
)


def _write_metadata_tree(root: Path) -> tuple[Path, Path]:
    meta_dir = root / "01_raw" / "2p" / "metadata"
    meta_dir.mkdir(parents=True, exist_ok=True)
    metadata_csv = meta_dir / "fishA_metadata.csv"
    metadata_csv.write_text("parameter,value\nframerate,1.0\n")
    log_csv = meta_dir / "fishA_experiment_log.csv"
    log_df = pd.DataFrame(
        {
            "event": [
                "B1_start",
                "B1_stim1_LLB",
                "B1_poststim1_pause",
                "B1_stim2_RLC",
                "B1_poststim2_pause",
                "B1_end",
            ],
            "time": [0.0, 0.0, 2.0, 2.0, 4.0, 4.0],
        }
    )
    log_df.to_csv(log_csv, index=False)
    return metadata_csv, log_csv


def test_load_and_annotate_midline_context_preserves_public_columns() -> None:
    with TemporaryDirectory() as tmpdir:
        midline_json = Path(tmpdir) / "midline.json"
        midline_json.write_text(
            json.dumps(
                {
                    "fish_id": "fishA",
                    "midline_space": "func",
                    "side_labels": {"positive": "left", "negative": "right"},
                    "manual": {"uncertain_band_px": 0.25},
                    "per_plane": {"0": {"x0": 0.0, "y0": 0.0, "theta_deg": 0.0}},
                }
            )
        )
        ctx = load_midline_context(midline_json, fish_id="fishA")
        df = pd.DataFrame(
            {
                "plane_idx": [0, 0, 0, 1],
                "centroid_x_func": [0.0, 0.0, 0.0, 0.0],
                "centroid_y_func": [1.0, -1.0, 0.1, 1.0],
            },
            index=[10, 11, 12, 13],
        )

        out = annotate_midline_side(df, ctx)

        assert ctx["bundle_path"] == midline_json
        assert ctx["midline_space"] == "func"
        assert out["midline_signed_dist_px"].tolist()[:3] == [1.0, -1.0, 0.1]
        assert out["midline_side"].tolist() == ["left", "right", "midline", "unknown"]
        assert out["midline_uncertain"].tolist() == [False, False, True, False]
        assert out["midline_space"].tolist() == ["func", "func", "func", "func"]


def test_annotate_midline_side_prefers_anatomy_centroids_for_anat_midline() -> None:
    with TemporaryDirectory() as tmpdir:
        midline_json = Path(tmpdir) / "midline.json"
        midline_json.write_text(
            json.dumps(
                {
                    "fish_id": "fishA",
                    "midline_space": "anat",
                    "side_labels": {"positive": "left", "negative": "right"},
                    "manual": {"uncertain_band_px": 0.0},
                    "per_plane": {"0": {"x0": 0.0, "y0": 0.0, "theta_deg": 0.0}},
                }
            )
        )
        ctx = load_midline_context(midline_json, fish_id="fishA")
        df = pd.DataFrame(
            {
                "plane_idx": [0, 0],
                "centroid_x_func": [0.0, 0.0],
                "centroid_y_func": [-10.0, -20.0],
                "centroid_x_anat": [0.0, 0.0],
                "centroid_y_anat": [5.0, -5.0],
            }
        )

        out = annotate_midline_side(df, ctx)

        assert out["midline_signed_dist_px"].tolist() == [5.0, -5.0]
        assert out["midline_side"].tolist() == ["left", "right"]
        assert out["midline_space"].tolist() == ["anat", "anat"]


def test_filter_high_confidence_pairs_uses_existing_low_confidence_columns() -> None:
    pairs = pd.DataFrame(
        {
            "gene": ["a", "b", "c", "d"],
            "_is_low_conf_match_any_globalR50": [False, True, False, False],
            "is_low_confidence_segmentation": ["false", "false", "yes", "0"],
        },
        index=[10, 11, 12, 13],
    )

    out, stats = filter_high_confidence_pairs(pairs, return_stats=True)

    assert out.index.tolist() == [10, 13]
    assert out["gene"].tolist() == ["a", "d"]
    assert stats["n_before"] == 4
    assert stats["n_low_any"] == 2
    assert stats["n_after"] == 2


def test_extract_window_with_padding_returns_requested_length_and_counts_valid() -> None:
    trace = np.asarray([1.0, np.nan, 3.0], dtype=np.float32)

    seg, n_valid, win_len = extract_window_with_padding(trace, -1, 4, min_valid_frac=0.4)

    assert win_len == 5
    assert n_valid == 2
    assert seg is not None
    np.testing.assert_allclose(seg, np.asarray([np.nan, 1.0, np.nan, 3.0, np.nan], dtype=np.float32), equal_nan=True)


def test_extract_window_with_padding_strict_rejects_out_of_bounds() -> None:
    seg, n_valid, win_len = extract_window_with_padding(np.asarray([1.0, 2.0]), -1, 1, mode="strict")

    assert seg is None
    assert n_valid == 0
    assert win_len == 2


def test_build_single_fish_motion_auc_plot_tables_writes_expected_tables() -> None:
    with TemporaryDirectory() as tmpdir:
        fish_dir = Path(tmpdir) / "fishA"
        out_reg = fish_dir / "03_analysis" / "functional" / "registration"
        plane_dir = fish_dir / "03_analysis" / "functional" / "suite2P" / "plane0"
        out_reg.mkdir(parents=True, exist_ok=True)
        plane_dir.mkdir(parents=True, exist_ok=True)
        _write_metadata_tree(fish_dir)

        np.save(
            plane_dir / "F.npy",
            np.asarray(
                [
                    [1.0, 2.0, 3.0, 4.0],
                    [4.0, 3.0, 2.0, 1.0],
                ],
                dtype=np.float32,
            ),
        )

        detail_df = pd.DataFrame(
            {
                "fish_id": ["fishA", "fishA"],
                "plane_idx": [0, 0],
                "func_label": [1, 2],
                "func_source": [str(plane_dir), str(plane_dir)],
                "centroid_x_func": [0.0, 0.0],
                "centroid_y_func": [1.0, -1.0],
                "response_class": ["responsive", "low activity"],
                "response_is_active": [True, False],
                "bpi_category": ["bout-responsive", "low activity"],
                "suite2p_is_cell": [True, True],
                "identity_display_label": ["cell-1", "cell-2"],
                "has_identity_assigned": [True, False],
            }
        )
        detail_csv = out_reg / "functional_roi_activity_identity.csv"
        detail_df.to_csv(detail_csv, index=False)

        status_df = pd.DataFrame(
            {
                "fish_id": ["fishA", "fishA"],
                "functional_status": ["matched", "matched"],
                "selected_plane": [0, 0],
                "selected_func_label": [1, 1],
                "gene": ["sst1.1", "sst1.1"],
                "anat_label": [10, 11],
                "conf_label": [20, 21],
            }
        )
        status_csv = out_reg / "hcr_activity_status.csv"
        status_df.to_csv(status_csv, index=False)

        midline_json = out_reg / "midline_params_func_ref.json"
        midline_json.write_text(
            json.dumps(
                {
                    "fish_id": "fishA",
                    "base": {"source_label": "func"},
                    "side_labels": {"positive": "left", "negative": "right"},
                    "manual": {"uncertain_band_px": 0.0},
                    "per_plane": {"0": {"x0": 0.0, "y0": 0.0, "theta_deg": 0.0}},
                }
            )
        )

        out = build_single_fish_motion_auc_plot_tables(
            fish_dir=fish_dir,
            fish_id="fishA",
            out_reg=out_reg,
            config=MotionAucPlotConfig(onset_delay_sec=0.0),
        )

        points_df = out["points_df"]
        counts_df = out["counts_df"]
        roi_panel_df = out["roi_panel_df"]
        assert (out_reg / "motion_auc_plot_points.csv").exists()
        assert (out_reg / "motion_auc_plot_counts.csv").exists()
        assert (out_reg / "motion_auc_roi_panels.csv").exists()
        assert set(points_df["group"].astype(str)) == {"All neurons", "sst1.1"}
        assert len(points_df[points_df["group"].astype(str) == "sst1.1"]) == 2
        assert len(roi_panel_df) == 4

        all_ipsi_bout = counts_df[
            (counts_df["group"].astype(str) == "All neurons")
            & (counts_df["laterality"].astype(str) == "ipsi")
            & (counts_df["stim_mode"].astype(str) == "bout")
        ].iloc[0]
        assert int(all_ipsi_bout["n_total"]) == 2
        assert int(all_ipsi_bout["n_responsive_used"]) == 1
        assert int(all_ipsi_bout["n_low_used"]) == 0
        assert int(all_ipsi_bout["n_other"]) == 1

        gene_ipsi_bout = counts_df[
            (counts_df["group"].astype(str) == "sst1.1")
            & (counts_df["laterality"].astype(str) == "ipsi")
            & (counts_df["stim_mode"].astype(str) == "bout")
        ].iloc[0]
        assert int(gene_ipsi_bout["n_total"]) == 2
        assert int(gene_ipsi_bout["n_responsive_used"]) == 2
        assert int(gene_ipsi_bout["n_low_used"]) == 0
        assert int(gene_ipsi_bout["n_other"]) == 0
        assert out["ordered_genes"] == ["sst1.1"]


def test_build_single_fish_motion_auc_plot_tables_fails_fast_on_missing_midline() -> None:
    with TemporaryDirectory() as tmpdir:
        fish_dir = Path(tmpdir) / "fishA"
        out_reg = fish_dir / "03_analysis" / "functional" / "registration"
        plane_dir = fish_dir / "03_analysis" / "functional" / "suite2P" / "plane0"
        out_reg.mkdir(parents=True, exist_ok=True)
        plane_dir.mkdir(parents=True, exist_ok=True)
        _write_metadata_tree(fish_dir)
        np.save(plane_dir / "F.npy", np.asarray([[1.0, 2.0], [2.0, 1.0]], dtype=np.float32))
        pd.DataFrame(
            {
                "fish_id": ["fishA"],
                "plane_idx": [0],
                "func_label": [1],
                "func_source": [str(plane_dir)],
                "centroid_x_func": [0.0],
                "centroid_y_func": [1.0],
                "response_class": ["responsive"],
                "response_is_active": [True],
                "bpi_category": ["bout-responsive"],
                "suite2p_is_cell": [True],
            }
        ).to_csv(out_reg / "functional_roi_activity_identity.csv", index=False)
        pd.DataFrame(
            {
                "fish_id": ["fishA"],
                "functional_status": ["matched"],
                "selected_plane": [0],
                "selected_func_label": [1],
                "gene": ["sst1.1"],
                "anat_label": [10],
                "conf_label": [20],
            }
        ).to_csv(out_reg / "hcr_activity_status.csv", index=False)

        try:
            build_single_fish_motion_auc_plot_tables(
                fish_dir=fish_dir,
                fish_id="fishA",
                out_reg=out_reg,
                config=MotionAucPlotConfig(onset_delay_sec=0.0),
            )
        except RuntimeError as exc:
            assert "Missing midline JSON" in str(exc)
        else:
            raise AssertionError("expected missing-midline failure")
