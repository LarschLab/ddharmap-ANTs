import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd

from codeants_2pf_hcr.activity import (
    ActivityConfig,
    SingleFishBpiDiagnosticsConfig,
    build_response_bpi_tables,
    build_scored_stimulus_windows_table,
    build_suite2p_response_seed_table,
    prepare_single_fish_bpi_diagnostics_stage,
)


class ActivityTests(unittest.TestCase):
    def test_scored_window_provenance_retains_plane_keys_for_shared_sessions(self) -> None:
        events = {0: [{"session_label": "r1", "block": "B1", "stim_idx": 0, "stim_type": "LLB", "stim_class": "bout", "idx0": 4, "idx1": 8, "duration_frames": 4, "duration_s": 2.0}], 1: [{"session_label": "r1", "block": "B1", "stim_idx": 0, "stim_type": "LLB", "stim_class": "bout", "idx0": 4, "idx1": 8, "duration_frames": 4, "duration_s": 2.0}]}
        source = {plane: pd.DataFrame([{"block": "B1", "stim_idx": 0, "type": "LLB", "start": 2.0, "end": 4.0}]) for plane in events}
        table = build_scored_stimulus_windows_table(fish_id="L758_f02", df_stim=pd.DataFrame(), stim_events=[], stim_events_by_plane=events, df_stim_by_plane=source, fps_by_plane={0: 2.0, 1: 2.0})
        self.assertEqual(table["plane_idx"].tolist(), [0, 1])
        self.assertEqual(len(table), 2)

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
                "B1_start,10\nB1_prestim0_pause,10\nB1_stim0_LLB,12\nB1_poststim0_pause,14\nB1_end,20\n"
                "B2_start,20\nB2_prestim1_pause,20\nB2_stim1_RLC,22\nB2_poststim1_pause,24\nB2_end,30\n"
            )
            (metadata / f"2026_f{fish_id}_planned_schedule.csv").write_text(
                "kind,label,start_sec,duration_sec,end_sec,trial_index,block_num,stimulus_name\n"
                "rest,Baseline rest,0,10,10,,0,\n"
                "trigger,B1_start,10,0,10,,1,\n"
                "prestim_pause,Trial 1 pre-pause,10,2,12,0,1,LLB\n"
                "stimulus,LLB,12,2,14,0,1,LLB\n"
                "trigger,B2_start,20,0,20,,2,\n"
                "prestim_pause,Trial 2 pre-pause,20,2,22,1,2,RLC\n"
                "stimulus,RLC,22,2,24,1,2,RLC\n"
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
                    response_null_min_windows=1,
                    response_null_bootstrap_n=5,
                    zscore_min_points=1,
                ),
            )

            events = result["stim_events_by_plane"][0]
            self.assertEqual([event["block"] for event in events], ["B1", "B2"])
            self.assertEqual([event["idx0"] for event in events], [12, 22])
            self.assertGreater(result["scored_bpi_df"].loc[0, "mean_bout_dff"], 3.0)
            self.assertGreater(result["scored_bpi_df"].loc[0, "mean_cont_dff"], 2.0)

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
                    response_null_min_windows=1,
                    response_null_bootstrap_n=5,
                    zscore_min_points=1,
                ),
            )
            scored_windows = build_scored_stimulus_windows_table(
                fish_id=fish_id,
                df_stim=result["df_stim"],
                stim_events=result["stim_events"],
                stim_events_by_plane=result["stim_events_by_plane"],
                df_stim_by_plane=result["df_stim_by_plane"],
                fps_by_plane=result["fps_by_plane"],
            )

            self.assertEqual(set(scored_windows["stim_type"]), set(stim_types))
            self.assertEqual(len(scored_windows), 20)
            self.assertEqual(scored_windows["stim_class"].value_counts().to_dict(), {"bout": 12, "continuous": 8})
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
                "B1_stim1_LLB,2\n"
                "B1_poststim1_pause,4\n"
                "B1_prestim2_pause,5\n"
                "B1_stim2_RLC,7\n"
                "B1_poststim2_pause,9\n"
                "B1_end,10\n"
            )
            (meta_dir / f"2026_f{fish_id}_r2_experiment_log.csv").write_text(
                "event,timestamp\n"
                "B1_start,0\n"
                "B1_prestim1_pause,0\n"
                "B1_stim1_LLB,4\n"
                "B1_poststim1_pause,6\n"
                "B1_prestim2_pause,6\n"
                "B1_stim2_RLC,8\n"
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
                "B1_stim1_LLB,2\n"
                "B1_poststim1_pause,4\n"
                "B1_prestim2_pause,5\n"
                "B1_stim2_RLC,7\n"
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

        result = build_response_bpi_tables(
            detail,
            fish_dir=Path("."),
            fish_id="fishA",
            precomputed_scored_bpi_df=precomputed,
            config=ActivityConfig(),
        )

        out = result["detail_df"].sort_values("func_label").reset_index(drop=True)
        self.assertEqual(list(out["response_is_active"].astype(bool)), [True, False])
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
