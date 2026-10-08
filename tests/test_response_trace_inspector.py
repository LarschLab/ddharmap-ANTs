from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

from codeants_2pf_hcr.response_trace_inspector import (
    STIM_TYPES,
    floor_excluded_mask,
    render_response_trace_inspector,
)

FPS = 2.0


def _windows(plane_idx: int, session: str) -> pd.DataFrame:
    rows, start = [], 30
    for stim in STIM_TYPES:
        for _ in range(4):
            rows.append(
                dict(plane_idx=plane_idx, session_label=session, stim_type=stim, used_for_bpi=True, fps=FPS,
                     prestim_baseline_start_frame=start, prestim_baseline_end_frame=start + 25,
                     recorded_onset_frame=start + 25, scored_onset_frame=start + 45, scored_offset_frame=start + 70)
            )
            start += 90
    rows.append(dict(rows[0], stim_type="WFCl", used_for_bpi=False))
    return pd.DataFrame(rows)


def _fixture(tmp_path, n_rois=3):
    rng = np.random.default_rng(0)
    windows = pd.concat([_windows(5, "r2"), _windows(6, "r2")], ignore_index=True)
    cell_rows = []
    for plane in (5, 6):
        n_frames = 30 + 16 * 90 + 10
        f = 1000.0 + rng.normal(0, 5, size=(n_rois, n_frames)).astype(np.float32)
        plane_w = windows[(windows.plane_idx == plane) & windows.used_for_bpi]
        for t in plane_w.itertuples():
            if t.stim_type.startswith(("LB", "RB")):
                f[0, t.scored_onset_frame:t.scored_offset_frame] += 200.0
        d = tmp_path / "suite2P" / f"plane{plane}"
        d.mkdir(parents=True)
        np.save(d / f"FISH_plane{plane}_F.npy", f)
        for roi in range(n_rois):
            dff = (f[roi] - np.percentile(f[roi], 10)) / (np.percentile(f[roi], 10) + 1e-6)
            aucs = []
            for t in plane_w.sort_values("prestim_baseline_start_frame").itertuples():
                base = dff[t.prestim_baseline_start_frame:t.prestim_baseline_end_frame].mean()
                aucs.append((t.stim_type, float(np.sum(dff[t.scored_onset_frame:t.scored_offset_frame] - base) / FPS)))
            bout = np.mean([a for s, a in aucs if s.startswith(("LB", "RB"))])
            cont = np.mean([a for s, a in aucs if s.startswith(("LC", "RC"))])
            active = roi == 0
            cell_rows.append(dict(
                plane_idx=plane, func_label=roi + 1, mean_bout_auc_dff=bout, mean_cont_auc_dff=cont,
                bout_null_upper_auc=0.01, bout_null_lower_auc=-0.5, cont_null_upper_auc=0.5, cont_null_lower_auc=-0.5,
                response_auc_threshold=0.05,
                response_is_active=active, response_class="bout-responsive" if active else "low activity",
                response_valence_pattern="bout:activation" if active else "none",
                bpi=1.0 if active else np.nan, bpi_preference="bout-preferential" if active else "not-responsive",
            ))
    cells = pd.DataFrame(cell_rows)
    score = tmp_path / "score"
    score.mkdir()
    cells.to_csv(score / "functional_roi_activity_bpi_cells.csv", index=False)
    windows.to_csv(score / "scored_stimulus_windows.csv", index=False)
    return cells, score


def test_floor_excluded_mask_requires_null_pass_and_sub_floor_auc():
    cells = pd.DataFrame(dict(
        mean_bout_auc_dff=[0.03, 0.03, 0.08, -0.03], mean_cont_auc_dff=[0.0, 0.0, 0.0, 0.0],
        bout_null_upper_auc=[0.02, 0.04, 0.02, 0.02], bout_null_lower_auc=[-0.02] * 4,
        cont_null_upper_auc=[0.02] * 4, cont_null_lower_auc=[-0.02] * 4, response_auc_threshold=[0.05] * 4,
        response_is_active=[False, False, True, False], response_class=["low activity"] * 2 + ["bout-responsive", "low activity"],
    ))
    assert floor_excluded_mask(cells).tolist() == [True, False, False, True]


def test_render_reproduces_auc_and_writes_self_contained_html(tmp_path):
    cells, score = _fixture(tmp_path)
    out = tmp_path / "out"
    meta = render_response_trace_inspector(fish_id="FISH", fish_dir=tmp_path, score_dir=score, output_dir=out,
                                           suite2p_dir=tmp_path / "suite2P")
    assert meta["n_all_rois"] == len(cells)
    assert meta["max_recomputed_mean_auc_difference"] < 1e-5
    assert meta["n_points"] == 70 and meta["movement_onset_sec"] == pytest.approx(10.0)
    html_text = (out / "FISH_response_trace_inspector.html").read_text()
    assert "<script src" not in html_text and "floor-excluded" in html_text
    index = pd.read_csv(out / "row_index.csv")
    assert set(index.plane_idx) == {5, 6}
    assert json.loads((out / "inspector_metadata.json").read_text())["review_group_counts"]["response-positive"] == 2


def test_render_refuses_when_table_auc_does_not_reproduce(tmp_path):
    cells, score = _fixture(tmp_path)
    cells.loc[0, "mean_bout_auc_dff"] += 1.0
    cells.to_csv(score / "functional_roi_activity_bpi_cells.csv", index=False)
    with pytest.raises(RuntimeError, match="AUC reproduction"):
        render_response_trace_inspector(fish_id="FISH", fish_dir=tmp_path, score_dir=score, output_dir=tmp_path / "o",
                                        suite2p_dir=tmp_path / "suite2P")
