from pathlib import Path

import numpy as np
import pandas as pd

from codeants_2pf_hcr import (
    aggregate_cohort_23c_trace_means,
    parse_fish_ids_csv,
    resolve_cohort_suite2p_23c_fish_dir,
)


def test_parse_fish_ids_csv_preserves_order_and_drops_empty_duplicates() -> None:
    assert parse_fish_ids_csv(" L758_f02, L758_f03,,L758_f02, L758_f04 ") == [
        "L758_f02",
        "L758_f03",
        "L758_f04",
    ]


def test_resolve_cohort_suite2p_23c_fish_dir_prefers_direct_fish_folder(tmp_path: Path) -> None:
    direct = tmp_path / "L758_f02"
    nested = tmp_path / "Matilde" / "L758_f02"
    direct.mkdir()
    nested.mkdir(parents=True)

    assert resolve_cohort_suite2p_23c_fish_dir(tmp_path, "L758_f02") == direct


def test_resolve_cohort_suite2p_23c_fish_dir_finds_owner_nested_folder(tmp_path: Path) -> None:
    nested = tmp_path / "Matilde" / "Microscopy" / "L758_f03"
    nested.mkdir(parents=True)

    assert resolve_cohort_suite2p_23c_fish_dir(tmp_path, "L758_f03") == nested


def test_aggregate_cohort_23c_trace_means_uses_one_row_per_fish_session_stimulus() -> None:
    tvec = np.asarray([-1.0, 0.0, 1.0], dtype=np.float32)
    trace_df = pd.DataFrame(
        {
            "fish_id": ["f1", "f1", "f1", "f2"],
            "session_label": ["r1", "r1", "r2", "r1"],
            "stim_type": ["LB", "LB", "LB", "LB"],
            "plane_idx": [0, 0, 1, 0],
            "func_label": [1, 2, 1, 1],
            "response_is_active": [True, True, True, True],
            "mean_trace": [
                np.asarray([0.0, 1.0, 2.0], dtype=np.float32),
                np.asarray([2.0, 3.0, 4.0], dtype=np.float32),
                np.asarray([10.0, 11.0, 12.0], dtype=np.float32),
                np.asarray([20.0, 21.0, 22.0], dtype=np.float32),
            ],
        }
    )

    out = aggregate_cohort_23c_trace_means(trace_df, tvec)

    assert out[["fish_id", "session_label", "stim_type"]].apply(tuple, axis=1).tolist() == [
        ("f1", "r1", "LB"),
        ("f1", "r2", "LB"),
        ("f2", "r1", "LB"),
    ]
    f1_r1 = out[(out["fish_id"] == "f1") & (out["session_label"] == "r1")].iloc[0]
    np.testing.assert_allclose(f1_r1["mean_trace"], np.asarray([1.0, 2.0, 3.0], dtype=np.float32))
    assert int(f1_r1["n_responsive_rois"]) == 2


def test_heatmap_rows_can_remain_all_cells_when_trace_rows_are_response_filtered() -> None:
    tvec = np.asarray([-1.0, 0.0, 1.0], dtype=np.float32)
    all_trace_df = pd.DataFrame(
        {
            "fish_id": ["f1", "f1", "f1"],
            "session_label": ["r1", "r1", "r1"],
            "stim_type": ["LB", "LB", "LB"],
            "plane_idx": [0, 0, 0],
            "func_label": [1, 2, 3],
            "response_is_active": [True, False, False],
            "mean_trace": [
                np.asarray([0.0, 1.0, 2.0], dtype=np.float32),
                np.asarray([2.0, 3.0, 4.0], dtype=np.float32),
                np.asarray([5.0, 6.0, 7.0], dtype=np.float32),
            ],
        }
    )
    active_trace_df = all_trace_df[all_trace_df["response_is_active"]].copy()
    heatmap_row_df = all_trace_df[["fish_id", "session_label", "plane_idx", "func_label"]].copy()

    out = aggregate_cohort_23c_trace_means(active_trace_df, tvec)

    assert int(out.iloc[0]["n_responsive_rois"]) == 1
    assert len(heatmap_row_df) == 3
