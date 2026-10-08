from __future__ import annotations

import json
import re

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
                bpi=1.0 if active else np.nan, bpi_zero_band=0.5, bpi_preference="bout-preferential" if active else "not-responsive",
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
    meta_json = json.loads((out / "inspector_metadata.json").read_text())
    assert meta_json["review_group_counts"]["response-positive"] == 2
    assert meta_json["tuning_counts"]["bout-tuned"] == 2
    assert 'data-tuning="bout-tuned"' in html_text and "tests: bout only" in html_text and "(unused)" not in html_text.split("data-tuning=\"bout-tuned\"")[1].split("</div>")[0]


def test_render_refuses_when_table_auc_does_not_reproduce(tmp_path):
    cells, score = _fixture(tmp_path)
    cells.loc[0, "mean_bout_auc_dff"] += 1.0
    cells.to_csv(score / "functional_roi_activity_bpi_cells.csv", index=False)
    with pytest.raises(RuntimeError, match="AUC reproduction"):
        render_response_trace_inspector(fish_id="FISH", fish_dir=tmp_path, score_dir=score, output_dir=tmp_path / "o",
                                        suite2p_dir=tmp_path / "suite2P")


def test_tuning_uses_bpi_preference_not_response_test(tmp_path):
    cells, score = _fixture(tmp_path)
    cells.loc[0, ["bpi", "bpi_preference"]] = [0.27, "non-preferential"]
    cells.to_csv(score / "functional_roi_activity_bpi_cells.csv", index=False)
    meta = render_response_trace_inspector(fish_id="FISH", fish_dir=tmp_path, score_dir=score, output_dir=tmp_path / "o",
                                           suite2p_dir=tmp_path / "suite2P")
    assert meta["tuning_counts"] == {"bout-tuned": 1, "non-preferential responder": 1, "low activity": 4}
    index = pd.read_csv(tmp_path / "o" / "row_index.csv")
    row = index[(index.plane_idx == 5) & (index.func_label == 1)].iloc[0]
    assert (row.tuning, row.response_class) == ("non-preferential responder", "bout-responsive")


def test_per_stimulus_guard_labels_and_view_geometry(tmp_path):
    cells, score = _fixture(tmp_path)
    windows = pd.read_csv(score / "scored_stimulus_windows.csv")
    for stim in STIM_TYPES:
        key = stim[:2].lower()
        cells[f"{key}_mean_auc_dff"] = np.nan
        cells[f"{key}_null_upper_auc"] = .01
        cells[f"{key}_null_lower_auc"] = -.01
        cells[f"{key}_response_valence"] = "not-significant"
    for idx, row in cells.iterrows():
        f = np.load(tmp_path / "suite2P" / f"plane{row.plane_idx}" / f"FISH_plane{row.plane_idx}_F.npy")[int(row.func_label)-1]
        dff = (f-np.percentile(f,10))/(np.percentile(f,10)+1e-6)
        for stim in STIM_TYPES:
            trial = windows[(windows.plane_idx == row.plane_idx) & windows.stim_type.eq(stim)]
            aucs = [(np.sum(dff[t.scored_onset_frame:t.scored_offset_frame]-np.mean(dff[t.prestim_baseline_start_frame:t.prestim_baseline_end_frame]))/FPS) for t in trial.itertuples()]
            cells.loc[idx, stim[:2].lower()+"_mean_auc_dff"] = np.mean(aucs)
    # Labels consume calls, without rebuilding them from the display traces.
    cells.loc[0, ["bpi_preference", "bpi", "response_valence_class"]] = ["suppressed-only", np.nan, "bout-suppressed"]
    cells.loc[0, "lb_response_valence"] = "suppressed"
    cells.loc[0, "suppression_confidence"] = "high"
    cells.loc[3, "response_valence_class"] = "bout-activated/continuous-suppressed"
    cells.loc[3, "bpi_side"] = "left"
    cells.to_csv(score / "functional_roi_activity_bpi_cells.csv", index=False)
    output = tmp_path / "out"
    render_response_trace_inspector(fish_id="FISH", fish_dir=tmp_path, score_dir=score, output_dir=output, suite2p_dir=tmp_path / "suite2P")
    html = (output / "FISH_response_trace_inspector.html").read_text()
    assert 'data-tuning="bout-suppressed"' in html
    assert 'data-tuning="mixed activation/suppression"' in html
    assert "highly probable suppression" in html and "LB sup · RB n.s." in html
    assert "first.dataset.index" in html and ".trace-row:not(.hidden)" in html
    assert 'sc.scrollTop+=' in html and 'data-index="${index}"' in html
    row_rule = re.search(r"(?m)^\.trace-row\{([^}]+)\}", html).group(1)
    assert "min-height:var(--rowh)" in row_rule
    assert "overflow:hidden" not in row_rule and "max-height" not in row_rule
    assert not re.search(r"(?:^|;)height:", row_rule)
    assert 'contain:size;align-self:stretch;height:auto;min-height:var(--rowh)' in html
    # Identical label content and label widths yield identical intrinsic row heights,
    # even when wrapping at 96px or with large text; SVG aspect ratios cannot set size.
    trial_view = html.split('<section id="view-nonavg"')[1].split('</section>')[0]
    mean_view = html.split('<section id="view-mean"')[1].split('</section>')[0]
    label_pattern = r'<div class="row-label"[^>]*>(.*?)</div>'
    assert re.findall(label_pattern, trial_view) == re.findall(label_pattern, mean_view)
    assert "±trial" in trial_view and "±mean" in trial_view
    assert "H=126" in html and "H=view===" not in html
    # Opposite per-stimulus errors cancel in pooled means but must fail this guard.
    cells.loc[0, "lb_mean_auc_dff"] += .1
    cells.loc[0, "rb_mean_auc_dff"] -= .1
    cells.to_csv(score / "functional_roi_activity_bpi_cells.csv", index=False)
    with pytest.raises(RuntimeError, match="AUC reproduction"):
        render_response_trace_inspector(fish_id="FISH", fish_dir=tmp_path, score_dir=score, output_dir=output, suite2p_dir=tmp_path / "suite2P")


def test_floor_excluded_uses_each_stimulus_not_pooled_mean():
    cells = pd.DataFrame(dict(response_auc_threshold=[.05], response_is_active=[False], response_class=["low activity"]))
    for stim in ("lb", "rb", "lc", "rc"):
        cells[f"{stim}_mean_auc_dff"] = .03 if stim == "lb" else 0
        cells[f"{stim}_null_upper_auc"] = .02
        cells[f"{stim}_null_lower_auc"] = -.02
        cells[f"{stim}_response_valence"] = "not-significant"
    assert floor_excluded_mask(cells).tolist() == [True]


def test_change_filter_marks_rows_against_pooled_calls(tmp_path):
    cells, score = _fixture(tmp_path)
    cells["pooled_response_is_active"] = False
    cells["pooled_bpi_preference"] = "not-responsive"
    cells.to_csv(score / "functional_roi_activity_bpi_cells.csv", index=False)
    render_response_trace_inspector(fish_id="FISH", fish_dir=tmp_path, score_dir=score, output_dir=tmp_path / "o", suite2p_dir=tmp_path / "suite2P")
    html_text = (tmp_path / "o" / "FISH_response_trace_inspector.html").read_text()
    assert 'id="change-filter"' in html_text and 'data-change="gained responder"' in html_text and "pooled: not responsive" in html_text
