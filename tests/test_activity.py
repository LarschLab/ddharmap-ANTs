import json
import unittest
from pathlib import Path
from unittest.mock import patch
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd

from codeants_2pf_hcr.activity import (
    ActivityConfig,
    SingleFishBpiDiagnosticsConfig,
    _bpi_stimulus_key,
    _build_activity_stimulus_context,
    _compute_bootstrap_null_quantiles,
    build_presented_stimulus_response_tables,
    build_response_bpi_tables,
    build_scored_stimulus_windows_table,
    build_suite2p_response_seed_table,
    prepare_single_fish_bpi_diagnostics_stage,
)


class ActivityTests(unittest.TestCase):
    def test_two_sided_null_splits_one_percent_across_both_tails(self) -> None:
        dff = np.arange(20, dtype=np.float32)[None, :]
        events = [{"stim_class": "bout", "duration_frames": 2}]
        starts = {2: np.array([0, 2, 4, 6, 8], dtype=int)}
        with patch("codeants_2pf_hcr.activity.np.quantile", wraps=np.quantile) as quantile:
            result = _compute_bootstrap_null_quantiles(
                dff, events, starts, fps=1.0, n_boot=25, q=0.99, seed=7,
            )

        called_quantiles = [float(call.args[1]) for call in quantile.call_args_list]
        np.testing.assert_allclose(called_quantiles, [0.99, 0.995, 0.005])
        self.assertTrue(np.all(result["bout_upper"] >= result["bout"]))
        self.assertTrue(np.all(result["bout_lower"] <= result["bout"]))

    def test_presented_stimulus_screen_controls_both_tails_across_the_family(self) -> None:
        dff = np.zeros((4, 30), dtype=np.float32)
        dff[0, 20:22] = 2.0
        dff[1, 24:26] = -2.0
        dff[3, :] = np.nan
        events = [
            {
                "stim_type": "A", "session_label": "r1", "duration_frames": 2,
                "idx0": 20, "idx1": 22, "baseline_idx0": 18, "baseline_idx1": 20,
            },
            {
                "stim_type": "B", "session_label": "r1", "duration_frames": 2,
                "idx0": 24, "idx1": 26, "baseline_idx0": 22, "baseline_idx1": 24,
            },
        ]
        response_result = {
            "presented_stim_events_by_plane": {0: events},
            "s2p_map": {0: {"dff": dff}},
            "fps_by_plane": {0: 1.0},
            "prestim_trial_windows_by_plane": {0: []},
        }
        roi_keys = pd.DataFrame({"plane_idx": [0, 0, 0, 0], "func_label": [1, 2, 3, 4]})
        cfg = ActivityConfig(
            min_trials_per_class=1,
            response_min_auc=0.5,
            response_null_q=0.99,
            response_null_bootstrap_n=10,
            response_null_min_windows=1,
        )

        with (
            patch(
                "codeants_2pf_hcr.activity.build_null_window_start_map",
                return_value={2: np.asarray([0, 2, 4], dtype=int)},
            ),
            patch("codeants_2pf_hcr.activity.np.quantile", wraps=np.quantile) as quantile,
        ):
            per_stim, any_response = build_presented_stimulus_response_tables(
                response_result, roi_keys, fish_id="L758_f02", config=cfg,
            )

        called_quantiles = [float(call.args[1]) for call in quantile.call_args_list]
        np.testing.assert_allclose(called_quantiles, [0.9975, 0.0025, 0.9975, 0.0025])
        np.testing.assert_allclose(per_stim["null_upper_quantile_familywise"], 0.9975)
        np.testing.assert_allclose(per_stim["null_lower_quantile_familywise"], 0.0025)
        self.assertTrue((per_stim["null_auc_upper_threshold"].dropna() == 0.0).all())
        self.assertTrue((per_stim["null_auc_lower_threshold"].dropna() == 0.0).all())
        pd.testing.assert_series_equal(per_stim["null_auc_threshold"], per_stim["null_auc_upper_threshold"], check_names=False)
        pd.testing.assert_series_equal(per_stim["null_quantile_familywise"], per_stim["null_upper_quantile_familywise"], check_names=False)
        calls = per_stim.set_index(["func_label", "stim_type"])
        self.assertTrue(calls.loc[(1, "A"), "positive_response_pass"])
        self.assertEqual(calls.loc[(1, "A"), "response_valence"], "activated")
        self.assertTrue(calls.loc[(2, "B"), "negative_response_pass"])
        self.assertEqual(calls.loc[(2, "B"), "response_valence"], "suppressed")
        any_calls = any_response.set_index("func_label")
        self.assertTrue(any_calls.loc[1, "any_presented_response_is_active"])
        self.assertTrue(any_calls.loc[2, "any_presented_response_has_suppression"])
        self.assertEqual(any_calls.loc[3, "any_presented_response_state"], "nonresponsive")

        self.assertEqual(any_calls.loc[4, "any_presented_response_state"], "unavailable")

    def test_presented_stimulus_screen_rejects_old_upper_boundary(self) -> None:
        dff = np.zeros((1, 20), dtype=np.float32)
        dff[0, 10:12] = 0.525
        event = {
            "stim_type": "A", "session_label": "r1", "duration_frames": 2,
            "idx0": 10, "idx1": 12, "baseline_idx0": 8, "baseline_idx1": 10,
        }
        response_result = {
            "presented_stim_events_by_plane": {0: [event]},
            "s2p_map": {0: {"dff": dff}},
            "fps_by_plane": {0: 1.0},
            "prestim_trial_windows_by_plane": {0: []},
        }
        seen_quantiles: list[float] = []

        def fake_quantile(values, q, axis):
            seen_quantiles.append(float(q))
            threshold = 1.1 if float(q) > 0.5 else -1.1
            return np.full(values.shape[0], threshold, dtype=np.float32)

        with (
            patch(
                "codeants_2pf_hcr.activity.build_null_window_start_map",
                return_value={2: np.asarray([0, 2, 4], dtype=int)},
            ),
            patch("codeants_2pf_hcr.activity.np.quantile", side_effect=fake_quantile),
        ):
            per_stim, any_response = build_presented_stimulus_response_tables(
                response_result,
                pd.DataFrame({"plane_idx": [0], "func_label": [1]}),
                fish_id="L758_f02",
                config=ActivityConfig(
                    min_trials_per_class=1, response_min_auc=0.0,
                    response_null_q=0.99, response_null_bootstrap_n=10,
                    response_null_min_windows=1,
                ),
            )

        np.testing.assert_allclose(seen_quantiles, [0.995, 0.005])
        self.assertAlmostEqual(float(per_stim.loc[0, "mean_auc_dff"]), 1.05, places=6)
        self.assertGreater(float(per_stim.loc[0, "mean_auc_dff"]), 1.0)
        self.assertFalse(bool(per_stim.loc[0, "response_pass"]))
        self.assertEqual(any_response.loc[0, "any_presented_response_state"], "nonresponsive")

    def test_scored_window_provenance_retains_plane_keys_for_shared_sessions(self) -> None:
        events = {0: [{"session_label": "r1", "block": "B1", "stim_idx": 0, "stim_type": "LB_trajectory", "stim_class": "bout", "idx0": 4, "idx1": 8, "duration_frames": 4, "duration_s": 2.0}], 1: [{"session_label": "r1", "block": "B1", "stim_idx": 0, "stim_type": "LB_trajectory", "stim_class": "bout", "idx0": 4, "idx1": 8, "duration_frames": 4, "duration_s": 2.0}]}
        source = {plane: pd.DataFrame([{"block": "B1", "stim_idx": 0, "type": "LB_trajectory", "start": 2.0, "end": 4.0}]) for plane in events}
        table = build_scored_stimulus_windows_table(fish_id="L758_f02", df_stim=pd.DataFrame(), stim_events=[], stim_events_by_plane=events, df_stim_by_plane=source, fps_by_plane={0: 2.0, 1: 2.0})
        self.assertEqual(table["plane_idx"].tolist(), [0, 1])
        self.assertEqual(len(table), 2)
    def test_planned_grid_anchors_true_prestim_and_fixed_motion_window(self) -> None:
        context = {
            "df_evt": pd.DataFrame([
                {"event": "B1_prestim0_pause", "time": 103.0},
                {"event": "B1_stim0_LB_trajectory", "time": 115.5},
                {"event": "B1_prestim1_pause", "time": 148.0},
                {"event": "B1_stim1_LC_trajectory", "time": 160.5},
            ]),
            "df_stim": pd.DataFrame([
                {"block": "B1", "stim_idx": 0, "type": "LB_trajectory", "start": 112.5, "end": 129.5, "duration": 17.0},
                {"block": "B1", "stim_idx": 1, "type": "LC_trajectory", "start": 157.5, "end": 174.5, "duration": 17.0},
            ]),
            "session_label": "r1",
        }
        scored = _build_activity_stimulus_context(
            context, fps=2.0, cfg=ActivityConfig(response_null_min_windows=1),
        )
        self.assertEqual(scored["prestim_trial_windows"][0]["idx0"], 200)
        self.assertEqual(scored["prestim_trial_windows"][0]["idx1"], 225)
        self.assertEqual(scored["presented_stim_events"][0]["idx0"], 245)
        self.assertEqual(scored["presented_stim_events"][0]["idx1"], 270)
        self.assertEqual(scored["presented_stim_events"][0]["baseline_idx0"], 200)
        self.assertEqual(scored["presented_stim_events"][0]["baseline_idx1"], 225)


    def test_response_scoring_maps_b1_b2_after_full_trace_b0(self) -> None:
        with TemporaryDirectory() as tmpdir:
            fish_id = "L758_f02"
            fish_dir = Path(tmpdir) / fish_id
            metadata = fish_dir / "01_raw" / "2p" / "metadata"
            metadata.mkdir(parents=True)
            (metadata / f"2026_f{fish_id}_metadata.csv").write_text("parameter,value\nframerate,1.0\n")
            (metadata / f"2026_f{fish_id}_experiment_log.csv").write_text(
                "event,timestamp\n"
                "B0_start,0\nB0_end,10\n"
                "B1_start,10\nB1_prestim0_pause,10\nB1_stim0_LB_trajectory,12\nB1_poststim0_pause,14\nB1_end,20\n"
                "B2_start,20\nB2_prestim1_pause,20\nB2_stim1_RC_trajectory,22\nB2_poststim1_pause,24\nB2_end,30\n"
            )
            (metadata / f"2026_f{fish_id}_planned_schedule.csv").write_text(
                "kind,label,start_sec,duration_sec,end_sec,trial_index,block_num,stimulus_name\n"
                "rest,Baseline rest,0,10,10,,0,\n"
                "trigger,B1_start,10,0,10,,1,\n"
                "prestim_pause,Trial 1 pre-pause,10,2,12,0,1,LB_trajectory\n"
                "stimulus,LB_trajectory,12,2,14,0,1,LB_trajectory\n"
                "trigger,B2_start,20,0,20,,2,\n"
                "prestim_pause,Trial 2 pre-pause,20,2,22,1,2,RC_trajectory\n"
                "stimulus,RC_trajectory,22,2,24,1,2,RC_trajectory\n"
            )
            dff = np.ones((1, 30), dtype=np.float32)
            dff[0, 12:14] = 4.0
            dff[0, 22:24] = 3.0
            detail = pd.DataFrame(
                {
                    "plane_idx": [0],
                    "func_label": [1],
                    "activity_class": ["Active neurons"],
                    "is_active": [True],
                }
            )

            result = build_response_bpi_tables(
                detail,
                fish_dir=fish_dir,
                fish_id=fish_id,
                suite2p_dff_map={0: {"dff": dff}},
                config=ActivityConfig(
                    min_trials_per_class=1,
                    stim_onset_delay_sec=0.0,
                    response_window_sec=2.0,
                    response_null_min_windows=1,
                    response_null_bootstrap_n=5,
                    zscore_min_points=1,
                ),
            )

            events = result["stim_events_by_plane"][0]
            self.assertEqual([event["block"] for event in events], ["B1", "B2"])
            self.assertEqual([event["idx0"] for event in events], [12, 22])
            self.assertGreater(result["scored_bpi_df"].loc[0, "mean_bout_dff"], 2.5)
            self.assertGreater(result["scored_bpi_df"].loc[0, "mean_cont_dff"], 1.5)

            boundary_null = {
                "bout": np.array([5.0], dtype=np.float32),
                "continuous": np.array([3.0], dtype=np.float32),
                "bout_upper": np.array([7.0], dtype=np.float32),
                "continuous_upper": np.array([3.5], dtype=np.float32),
                "bout_lower": np.array([-7.0], dtype=np.float32),
                "continuous_lower": np.array([-3.5], dtype=np.float32),
            }
            boundary_null.update({"lb_upper": boundary_null["bout_upper"], "lb_lower": boundary_null["bout_lower"],
                                  "rc_upper": boundary_null["continuous_upper"], "rc_lower": boundary_null["continuous_lower"]})
            with patch(
                "codeants_2pf_hcr.activity._compute_bootstrap_null_quantiles",
                return_value=boundary_null,
            ):
                strict_result = build_response_bpi_tables(
                    detail,
                    fish_dir=fish_dir,
                    fish_id=fish_id,
                    suite2p_dff_map={0: {"dff": dff}},
                    config=ActivityConfig(
                        min_trials_per_class=1,
                        stim_onset_delay_sec=0.0,
                        response_window_sec=2.0,
                        response_null_min_windows=1,
                        response_null_bootstrap_n=5,
                        zscore_min_points=1,
                    ),
                )

            strict_row = strict_result["scored_bpi_df"].iloc[0]
            self.assertGreater(strict_row["mean_bout_auc_dff"], strict_row["bout_null_q99_auc"])
            self.assertLess(strict_row["mean_bout_auc_dff"], strict_row["bout_null_upper_auc"])
            self.assertFalse(strict_row["bout_response_pass"])
            self.assertTrue(strict_row["cont_response_pass"])

    def test_response_scoring_preserves_suppression_valence_and_signed_contrast(self) -> None:
        with TemporaryDirectory() as tmpdir:
            fish_id = "L765_f04"
            fish_dir = Path(tmpdir) / fish_id
            metadata = fish_dir / "01_raw" / "2p" / "metadata"
            metadata.mkdir(parents=True)
            (metadata / f"2026_f{fish_id}_metadata.csv").write_text("parameter,value\nframerate,1.0\n")
            (metadata / f"2026_f{fish_id}_experiment_log.csv").write_text(
                "event,timestamp\n"
                "B0_start,0\nB0_end,10\n"
                "B1_start,10\nB1_prestim0_pause,10\nB1_stim0_LB_trajectory,12\nB1_poststim0_pause,14\nB1_end,20\n"
                "B2_start,20\nB2_prestim1_pause,20\nB2_stim1_RC_trajectory,22\nB2_poststim1_pause,24\nB2_end,30\n"
            )
            (metadata / f"2026_f{fish_id}_planned_schedule.csv").write_text(
                "kind,label,start_sec,duration_sec,end_sec,trial_index,block_num,stimulus_name\n"
                "rest,Baseline rest,0,10,10,,0,\n"
                "trigger,B1_start,10,0,10,,1,\n"
                "prestim_pause,Trial 1 pre-pause,10,2,12,0,1,LB_trajectory\n"
                "stimulus,LB_trajectory,12,2,14,0,1,LB_trajectory\n"
                "trigger,B2_start,20,0,20,,2,\n"
                "prestim_pause,Trial 2 pre-pause,20,2,22,1,2,RC_trajectory\n"
                "stimulus,RC_trajectory,22,2,24,1,2,RC_trajectory\n"
            )
            dff = np.ones((2, 30), dtype=np.float32)
            dff[0, 12:14] = -1.0
            dff[0, 22:24] = 3.0
            dff[1, 12:14] = 0.0
            dff[1, 22:24] = 0.5
            detail = pd.DataFrame(
                {
                    "plane_idx": [0, 0],
                    "func_label": [1, 2],
                    "activity_class": ["Active neurons", "Active neurons"],
                    "is_active": [True, True],
                }
            )

            result = build_response_bpi_tables(
                detail,
                fish_dir=fish_dir,
                fish_id=fish_id,
                suite2p_dff_map={0: {"dff": dff}},
                config=ActivityConfig(
                    min_trials_per_class=1,
                    stim_onset_delay_sec=0.0,
                    response_window_sec=2.0,
                    response_min_auc=0.05,
                    response_null_min_windows=1,
                    response_null_bootstrap_n=25,
                    zscore_min_points=1,
                ),
            )

            scored_rows = result["scored_bpi_df"].sort_values("func_label").reset_index(drop=True)
            scored = scored_rows.iloc[0]
            self.assertLess(scored["mean_bout_auc_dff"], 0.0)
            self.assertGreater(scored["mean_cont_auc_dff"], 0.0)
            self.assertAlmostEqual(scored["response_null_quantile"], 0.99)
            self.assertAlmostEqual(scored["response_null_upper_quantile"], 0.99875)
            self.assertAlmostEqual(scored["response_null_lower_quantile"], 0.00125)
            self.assertTrue(scored["bout_negative_response_pass"])
            self.assertFalse(scored["bout_positive_response_pass"])
            self.assertTrue(scored["cont_positive_response_pass"])
            self.assertFalse(scored["cont_negative_response_pass"])
            self.assertTrue(scored["bout_response_pass"])
            self.assertTrue(scored["cont_response_pass"])
            self.assertEqual(scored["response_class"], "both-responsive")
            self.assertEqual(scored["bout_response_valence"], "suppressed")
            self.assertEqual(scored["cont_response_valence"], "activated")
            self.assertEqual(scored["response_valence_pattern"], "suppressed|activated")
            self.assertTrue(scored["response_has_suppression"])
            self.assertFalse(scored["response_valence_complete"])
            self.assertAlmostEqual(scored["signed_condition_contrast"], -1.0)
            self.assertAlmostEqual(scored["response_absolute_magnitude_auc"], 4.0)
            self.assertTrue(pd.isna(scored["bpi"]))
            self.assertAlmostEqual(scored["pooled_bpi"], -1.0)
            self.assertEqual(scored["bpi_side"], "right")

            both_suppressed = scored_rows.iloc[1]
            self.assertEqual(both_suppressed["response_valence_pattern"], "suppressed|suppressed")
            self.assertTrue(both_suppressed["bout_negative_response_pass"])
            self.assertTrue(both_suppressed["cont_negative_response_pass"])
            self.assertFalse(both_suppressed["response_valence_complete"])
            self.assertAlmostEqual(both_suppressed["mean_bout_auc_dff"], -2.0)
            self.assertAlmostEqual(both_suppressed["mean_cont_auc_dff"], -1.0)
            self.assertAlmostEqual(both_suppressed["signed_condition_contrast"], -1.0 / 3.0)
            self.assertFalse(both_suppressed["bpi_data_available"])
            self.assertTrue(pd.isna(both_suppressed["bpi"]))

    def test_response_scoring_includes_complete_presented_stimulus_vocabulary(self) -> None:
        with TemporaryDirectory() as tmpdir:
            fish_id = "L758_f02"
            fish_dir = Path(tmpdir) / fish_id
            metadata = fish_dir / "01_raw" / "2p" / "metadata"
            metadata.mkdir(parents=True)
            (metadata / f"2026_f{fish_id}_metadata.csv").write_text("parameter,value\nframerate,1.0\n")
            stim_types = [
                "LAB_trajectory",
                "RAB_trajectory",
                "LB_trajectory",
                "RB_trajectory",
                "Ll_RB_trajectory",
                "Rl_RB_trajectory",
                "LC_trajectory",
                "RC_trajectory",
                "WFCl",
                "WFCo",
            ]
            event_lines = ["event,timestamp", "B0_start,0", "B0_end,100"]
            for block_idx, block in enumerate(("B1", "B2"), start=1):
                block_start = block_idx * 100
                event_lines.append(f"{block}_start,{block_start}")
                for stim_idx, stim_type in enumerate(stim_types):
                    onset = block_start + 2 + 8 * stim_idx
                    event_lines.extend(
                        [
                            f"{block}_prestim{stim_idx}_pause,{onset - 1}",
                            f"{block}_stim{stim_idx}_{stim_type},{onset}",
                            f"{block}_poststim{stim_idx}_pause,{onset + 2}",
                        ]
                    )
                event_lines.append(f"{block}_end,{block_start + 100}")
            (metadata / f"2026_f{fish_id}_experiment_log.csv").write_text("\n".join(event_lines) + "\n")

            dff = np.ones((1, 300), dtype=np.float32)
            detail = pd.DataFrame(
                {
                    "plane_idx": [0],
                    "func_label": [1],
                    "activity_class": ["Active neurons"],
                    "is_active": [True],
                }
            )
            result = build_response_bpi_tables(
                detail,
                fish_dir=fish_dir,
                fish_id=fish_id,
                suite2p_dff_map={0: {"dff": dff}},
                config=ActivityConfig(
                    min_trials_per_class=1,
                    stim_onset_delay_sec=0.0,
                    response_window_sec=1.0,
                    response_null_min_windows=1,
                    response_null_bootstrap_n=5,
                    zscore_min_points=1,
                ),
            )
            scored_windows = build_scored_stimulus_windows_table(
                fish_id=fish_id,
                df_stim=result["df_stim"],
                stim_events=result["presented_stim_events"],
                stim_events_by_plane=result["presented_stim_events_by_plane"],
                df_stim_by_plane=result["df_stim_by_plane"],
                fps_by_plane=result["fps_by_plane"],
            )

            self.assertEqual(set(scored_windows["stim_type"]), set(stim_types))
            self.assertEqual(len(scored_windows), 20)
            self.assertEqual(int(scored_windows["used_for_bpi"].sum()), 8)
            self.assertEqual(
                scored_windows.loc[scored_windows["used_for_bpi"], "stim_type"].value_counts().to_dict(),
                {"LB_trajectory": 2, "RB_trajectory": 2, "LC_trajectory": 2, "RC_trajectory": 2},
            )
            self.assertEqual(
                scored_windows.loc[scored_windows["used_for_bpi"], "bpi_stim_class"].value_counts().to_dict(),
                {"bout": 4, "continuous": 4},
            )
            scored = result["scored_bpi_df"].iloc[0]
            self.assertEqual(int(scored["n_bout_trials"]), 4)
            self.assertEqual(int(scored["n_cont_trials"]), 4)
            self.assertTrue((scored_windows.loc[scored_windows["block"] == "B1", "scored_onset_frame"] >= 100).all())
            self.assertTrue((scored_windows.loc[scored_windows["block"] == "B2", "scored_onset_frame"] >= 200).all())

    def test_build_response_bpi_tables_uses_plane_session_stimulus_metadata(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fish_id = "L758_f02"
            fish_dir = root / fish_id
            meta_dir = fish_dir / "01_raw" / "2p" / "metadata"
            preproc_dir = fish_dir / "02_reg" / "00_preprocessing" / "2p_functional" / "01_individualPlanes"
            suite2p_root = fish_dir / "03_analysis" / "functional" / "suite2P"
            meta_dir.mkdir(parents=True)
            preproc_dir.mkdir(parents=True)
            for plane_idx in (0, 5):
                (suite2p_root / f"plane{plane_idx}").mkdir(parents=True)

            (meta_dir / f"2026_f{fish_id}_metadata.csv").write_text("parameter,value\nframerate,1.0\n")
            (meta_dir / f"2026_f{fish_id}_r2_metadata.csv").write_text("parameter,value\nframerate,1.0\n")
            (meta_dir / f"2026_f{fish_id}_experiment_log.csv").write_text(
                "event,timestamp\n"
                "B1_start,0\n"
                "B1_prestim1_pause,0\n"
                "B1_stim1_LB_trajectory,2\n"
                "B1_poststim1_pause,4\n"
                "B1_prestim2_pause,5\n"
                "B1_stim2_RC_trajectory,7\n"
                "B1_poststim2_pause,9\n"
                "B1_end,10\n"
            )
            (meta_dir / f"2026_f{fish_id}_r2_experiment_log.csv").write_text(
                "event,timestamp\n"
                "B1_start,0\n"
                "B1_prestim1_pause,0\n"
                "B1_stim1_LB_trajectory,4\n"
                "B1_poststim1_pause,6\n"
                "B1_prestim2_pause,6\n"
                "B1_stim2_RC_trajectory,8\n"
                "B1_poststim2_pause,10\n"
                "B1_end,11\n"
            )
            (preproc_dir / f"{fish_id}_preprocessing_metadata.json").write_text(
                json.dumps(
                    {
                        "sessions": [
                            {"session_label": "r1", "output_planes": [0, 1, 2, 3, 4]},
                            {"session_label": "r2", "output_planes": [5, 6, 7, 8, 9]},
                        ]
                    }
                )
            )

            plane0 = np.ones((1, 12), dtype=np.float32)
            plane0[0, 2:4] = 2.0
            plane0[0, 7:9] = 3.0
            plane5 = np.ones((1, 12), dtype=np.float32)
            plane5[0, 4:6] = 30.0
            plane5[0, 8:10] = 40.0
            np.save(suite2p_root / "plane0" / "F.npy", plane0)
            np.save(suite2p_root / "plane5" / "F.npy", plane5)

            detail = pd.DataFrame(
                {
                    "plane_idx": [0, 5],
                    "func_label": [1, 1],
                    "anat_label": [101, 202],
                    "identity_display_label": ["sst1.1", "npy"],
                    "activity_class": ["Active neurons", "Active neurons"],
                    "is_active": [True, True],
                }
            )

            result = build_response_bpi_tables(
                detail,
                fish_dir=fish_dir,
                fish_id=fish_id,
                suite2p_root=suite2p_root,
                config=ActivityConfig(
                    min_trials_per_class=1,
                    stim_onset_delay_sec=0.0,
                    response_window_sec=2.0,
                    response_null_min_windows=1,
                    response_null_bootstrap_n=5,
                    dfof_baseline_pct=0.0,
                    zscore_min_points=1,
                ),
            )

            scored = result["scored_bpi_df"].sort_values("plane_idx").reset_index(drop=True)
            self.assertEqual(scored.loc[0, "n_bout_trials"], 1)
            self.assertEqual(scored.loc[1, "n_bout_trials"], 1)
            self.assertGreater(scored.loc[1, "mean_bout_dff"], 20.0)
            self.assertTrue(scored["anat_label"].isna().all())
            detail_out = result["detail_df"].sort_values("plane_idx").reset_index(drop=True)
            self.assertEqual(list(detail_out["anat_label"]), [101, 202])
            self.assertIn("r1", set(result["df_stim"]["session_label"]))
            self.assertIn("r2", set(result["df_stim"]["session_label"]))

    def test_build_response_bpi_tables_accepts_in_memory_suite2p_map(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fish_id = "L758_f02"
            fish_dir = root / fish_id
            meta_dir = fish_dir / "01_raw" / "2p" / "metadata"
            meta_dir.mkdir(parents=True)
            (meta_dir / f"2026_f{fish_id}_metadata.csv").write_text("parameter,value\nframerate,1.0\n")
            (meta_dir / f"2026_f{fish_id}_experiment_log.csv").write_text(
                "event,timestamp\n"
                "B1_start,0\n"
                "B1_prestim1_pause,0\n"
                "B1_stim1_LB_trajectory,2\n"
                "B1_poststim1_pause,4\n"
                "B1_prestim2_pause,5\n"
                "B1_stim2_RC_trajectory,7\n"
                "B1_poststim2_pause,9\n"
                "B1_end,10\n"
            )
            dff = np.ones((2, 12), dtype=np.float32)
            dff[0, 2:4] = 3.0
            dff[0, 7:9] = 4.0
            suite2p_by_ref_idx = {
                0: {
                    "dff": dff,
                    "iscell": np.asarray([[1, 0.9], [0, 0.1]], dtype=np.float32),
                    "ops": {"fs": 1.0},
                }
            }
            seed_df, dff_map = build_suite2p_response_seed_table(suite2p_by_ref_idx, fish_id=fish_id)

            result = build_response_bpi_tables(
                seed_df,
                fish_dir=fish_dir,
                fish_id=fish_id,
                suite2p_dff_map=dff_map,
                config=ActivityConfig(
                    min_trials_per_class=1,
                    stim_onset_delay_sec=0.0,
                    response_window_sec=2.0,
                    response_null_min_windows=1,
                    response_null_bootstrap_n=5,
                    dfof_baseline_pct=0.0,
                    zscore_min_points=1,
                ),
            )

            scored = result["scored_bpi_df"].sort_values("func_label").reset_index(drop=True)
            self.assertEqual(len(scored), 2)
            self.assertTrue(bool(scored.loc[0, "suite2p_is_cell"]) if "suite2p_is_cell" in scored.columns else True)
            self.assertEqual(scored.loc[1, "bpi_status"], "low_quality_trace")

    def test_build_response_bpi_tables_merges_precomputed_response_calls(self) -> None:
        detail = pd.DataFrame(
            {
                "plane_idx": [0, 0],
                "func_label": [1, 2],
                "activity_class": ["Active neurons", "Active neurons"],
                "is_active": [True, True],
            }
        )
        precomputed = pd.DataFrame(
            {
                "plane_idx": [0, 0],
                "func_label": [1, 2],
                "response_is_active": [True, False],
                "response_class": ["bout-responsive", "low activity"],
                "response_summary_class": ["Responsive neurons", "Low activity"],
                "bpi_category": ["bout-responsive", "low activity"],
            }
        )

        with self.assertRaisesRegex(RuntimeError, "precomputed response table missing columns"):
            build_response_bpi_tables(
                detail,
                fish_dir=Path("."),
                fish_id="fishA",
                precomputed_scored_bpi_df=precomputed,
                config=ActivityConfig(),
            )

        precomputed = precomputed.assign(
            bout_null_upper_auc=[1.0, 1.0],
            cont_null_upper_auc=[1.0, 1.0],
            bout_null_lower_auc=[-1.0, -1.0],
            cont_null_lower_auc=[-1.0, -1.0],
            response_null_upper_quantile=[0.995, 0.995],
            response_null_lower_quantile=[0.005, 0.005],
        )
        result = build_response_bpi_tables(
            detail,
            fish_dir=Path("."),
            fish_id="fishA",
            precomputed_scored_bpi_df=precomputed,
            config=ActivityConfig(),
        )

        out = result["detail_df"].sort_values("func_label").reset_index(drop=True)
        self.assertEqual(list(out["response_is_active"].astype(bool)), [True, False])
        self.assertEqual(list(out["bout_response_valence"]), ["unavailable", "unavailable"])
        self.assertEqual(list(out["cont_response_valence"]), ["unavailable", "unavailable"])
        self.assertEqual(list(out["response_has_suppression"].astype(bool)), [False, False])
        self.assertTrue(out["signed_condition_contrast"].isna().all())
        self.assertEqual(list(out["response_valence_complete"].astype(bool)), [False, False])
        self.assertEqual(list(out["signed_condition_contrast_data_available"].astype(bool)), [False, False])
        self.assertEqual(result["stim_source"], "precomputed")

def test_prepare_single_fish_bpi_diagnostics_stage_backfills_response_columns() -> None:
    with TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        master_csv = tmp_path / "functional_roi_activity_identity.csv"
        pd.DataFrame(
            {
                "fish_id": ["fishA", "fishA"],
                "plane_idx": [0, 0],
                "func_label": [1, 2],
                "response_class": ["responsive", "low activity"],
                "response_summary_class": ["Responsive neurons", "Low activity"],
                "response_is_active": [True, False],
            }
        ).to_csv(master_csv, index=False)

        bpi_cells_df = pd.DataFrame(
            {
                "fish_id": ["fishA", "fishA"],
                "gene": ["sst1.1", "npy"],
                "plane_idx": [0, 0],
                "func_label": [1, 2],
                "mean_bout_zdff": [0.6, 0.1],
                "mean_cont_zdff": [0.2, 0.1],
                "bpi": [0.5, 0.01],
            }
        )

        result = prepare_single_fish_bpi_diagnostics_stage(
            bpi_cells_df,
            fish_id="fishA",
            master_detail_csv=master_csv,
            config=SingleFishBpiDiagnosticsConfig(zero_band=0.05, n_activity_bins=4),
        )

        df = result["df"]
        assert list(df["response_summary_class"].astype(str)) == ["Responsive neurons", "Low activity"]
        assert list(df["response_is_active"].astype(bool)) == [True, False]
        assert result["activity_label"] == "z-scored dF/F"
        assert result["bpi_index_col"] == "bpi"
        assert result["summary_counts"]["n_total"] == 2
        assert result["summary_counts"]["n_near_zero"] == 1
        assert "bpi_activity_df" in result["bindings"]
        assert "BPI_ACTIVITY_FISH_ID" in result["bindings"]


if __name__ == "__main__":
    unittest.main()


def _per_stimulus_scoring_fixture():
    """Eight trace behaviours, four repetitions per side, actual writer and null."""
    profiles = np.array([
        [.099, -.005, 0, 0], [-2, 0, 0, 0], [-2, 0, -2, 0],
        [2, 0, -2, 0], [.1, .3, .09, .01], [2, -2, 0, 0],
        [-2, 0, 0, 0], [-2, 0, 0, 0],
    ])
    dff = np.zeros((len(profiles), 1600), dtype=np.float32)
    events, pauses = [], []
    for j, stim in enumerate(("LB", "RB", "LC", "RC")):
        for rep in range(4):
            start = 30 + (j*4 + rep)*90
            event = dict(stim_type=stim+"_trajectory", stim_class="bout" if j < 2 else "continuous",
                         used_for_bpi=True, idx0=start+45, idx1=start+70,
                         baseline_idx0=start, baseline_idx1=start+25, duration_frames=25)
            events.append(event)
            pauses.append((start, start+25))
            dff[:, start+45:start+70] = profiles[:, j, None] / 12.5
            if j == 0:
                for roi, genuine in ((6, 3), (7, 2)):
                    if rep >= genuine:
                        dff[roi, start:start+25] = .02
                        dff[roi, start+45:start+70] = 0
    starts = np.array([start for start, _ in pauses])
    ctx = dict(fps=2., df_evt=pd.DataFrame(), df_stim=pd.DataFrame(), session_label="r1", stim_source="fixture",
               stim_events=events, presented_stim_events=events, baseline_windows=pauses,
               prestim_trial_windows=[], null_starts_by_duration={25: starts})
    detail = pd.DataFrame(dict(plane_idx=[0]*len(profiles), func_label=np.arange(1, len(profiles)+1),
                               activity_class=["Active neurons"]*len(profiles), is_active=True))
    with patch("codeants_2pf_hcr.activity.resolve_plane_stimulus_contexts", return_value={0: dict(session_label="r1", log_path="fixture", frame_rate=2.)}), \
         patch("codeants_2pf_hcr.activity.map_stimulus_context_to_trace_grid", return_value={}), \
         patch("codeants_2pf_hcr.activity._build_activity_stimulus_context", return_value=ctx):
        return build_response_bpi_tables(detail, fish_dir=Path("."), fish_id="fixture", frame_rate=2.,
                                        suite2p_dff_map={0: {"dff": dff}}, config=ActivityConfig())


def test_per_stimulus_authoritative_calls_and_activation_only_tuning():
    result = _per_stimulus_scoring_fixture()
    rows = result["scored_bpi_df"]
    one = rows.iloc[0]
    assert one.response_is_active and not one.pooled_response_is_active
    np.testing.assert_allclose(one.lb_mean_auc_dff, .099, atol=1e-6)
    assert one.bout_response_pass and one.bpi_preference == "bout-preferential"
    for index, cls in ((1, "bout-suppressed"), (2, "both-suppressed")):
        row = rows.iloc[index]
        assert row.response_valence_class == cls
        assert row.bpi_preference == "suppressed-only" and pd.isna(row.bpi)
        assert row.response_is_active and not row.bpi_data_available
    mixed = rows.iloc[3]
    assert mixed.response_valence_class == "bout-activated/continuous-suppressed"
    assert mixed.bpi_preference == "bout-preferential" and mixed.bpi_side == "left"
    assert mixed.bpi == 1
    side = rows.iloc[4]
    assert side.bpi_side == "right"
    np.testing.assert_allclose(side.bpi, (.3-.01)/(.3+.01), atol=1e-6)
    assert rows.iloc[5].bout_mixed_side_valence
    assert rows.iloc[5].bout_response_valence == "activated"
    assert rows.iloc[6].suppression_confidence == "high"
    assert rows.iloc[6].lb_n_consistent_suppressed_trials == 3
    assert rows.iloc[6].lb_n_prestim_elevated_trials == 1
    assert rows.iloc[7].suppression_confidence == "low"
    assert rows.iloc[7].lb_n_consistent_suppressed_trials == 2
    assert rows.iloc[7].lb_n_prestim_elevated_trials == 2
    assert rows.iloc[7].response_is_active  # confidence never gates suppression
    assert pd.isna(one.suppression_confidence)
    for col in ("lb_response_valence", "response_valence_class", "pooled_bpi", "suppression_confidence"):
        assert col in result["detail_df"]
    # Same annotations must survive the precomputed path and rescoring without suffixes.
    detail = result["detail_df"]
    merged = build_response_bpi_tables(detail, fish_dir=Path("."), fish_id="fixture", frame_rate=2.,
                                      precomputed_scored_bpi_df=rows)["detail_df"]
    assert not any(col.endswith(("_x", "_y")) for col in merged)
    assert merged.lb_response_valence.tolist() == detail.lb_response_valence.tolist()


def test_per_stimulus_null_uses_four_stimulus_two_tail_family():
    dff = np.arange(50, dtype=np.float32)[None, :]
    event = dict(stim_type="LB_trajectory", stim_class="bout", duration_frames=2)
    with patch("codeants_2pf_hcr.activity.np.quantile", wraps=np.quantile) as quantile:
        result = _compute_bootstrap_null_quantiles(dff, [event]*4, {2: np.arange(20)},
                                                   fps=1., n_boot=100, q=.99, seed=1)
    qs = [float(call.args[1]) for call in quantile.call_args_list]
    np.testing.assert_allclose(qs[-3:], [.99, .99875, .00125])
    assert result["lb_upper"][0] >= result["lb_lower"][0]
    assert ActivityConfig(response_null_bootstrap_n=2000).response_null_bootstrap_n == 16000


def test_per_stimulus_null_matches_valid_trial_set_size():
    cfg = ActivityConfig(min_valid_frac=.5)
    rng = np.random.default_rng(9)
    dff = rng.normal(size=(1, 80)).astype(np.float32)
    events = [dict(stim_type="LB_trajectory", stim_class="bout", duration_frames=2,
                   idx0=40+4*i, idx1=42+4*i, baseline_idx0=20+2*i, baseline_idx1=22+2*i) for i in range(4)]
    dff[0, events[-1]["idx0"]:events[-1]["idx1"]] = np.nan
    kwargs = dict(fps=1., n_boot=1000, q=.99, seed=5, config=cfg)
    four = _compute_bootstrap_null_quantiles(dff, events, {2: np.arange(18)}, **kwargs)
    three = _compute_bootstrap_null_quantiles(dff, events[:3], {2: np.arange(18)}, **kwargs)
    np.testing.assert_array_equal(four["lb_upper"], three["lb_upper"])
    np.testing.assert_array_equal(four["lb_lower"], three["lb_lower"])


def test_per_stimulus_pools_admit_only_exact_bpi_trajectories():
    for stimulus, key in (("LB_trajectory", "lb"), ("RB_trajectory", "rb"),
                          ("LC_trajectory", "lc"), ("RC_trajectory", "rc")):
        assert _bpi_stimulus_key({"stim_type": stimulus}) == key
        assert _bpi_stimulus_key({"stim_type": stimulus, "used_for_bpi": False}) == ""
    excluded = ("Ll_RB_trajectory", "Rl_RB_trajectory", "LAB_trajectory", "RAB_trajectory",
                "WFCl", "WFCo", "LLB", "RLB", "LLC", "RLC", "LB", "RB", "lb_trajectory")
    for stimulus in excluded:
        assert _bpi_stimulus_key({"stim_type": stimulus}) == ""
    # Even wrongly tagged non-BPI events cannot change either side's bootstrap pool.
    dff = np.arange(80, dtype=np.float32)[None, :]
    valid = [dict(stim_type=stim, stim_class="bout", duration_frames=2)
             for stim in ("LB_trajectory", "RB_trajectory") for _ in range(4)]
    unrelated = [dict(stim_type=stim, stim_class="bout", duration_frames=7)
                 for stim in ("Ll_RB_trajectory", "LAB_trajectory")]
    kwargs = dict(fps=1., n_boot=1000, q=.99, seed=3)
    clean = _compute_bootstrap_null_quantiles(dff, valid, {2: np.arange(20)}, **kwargs)
    contaminated = _compute_bootstrap_null_quantiles(dff, valid+unrelated, {2: np.arange(20)}, **kwargs)
    for key in ("lb", "rb"):
        for suffix in ("", "_upper", "_lower"):
            np.testing.assert_array_equal(clean[key+suffix], contaminated[key+suffix])
