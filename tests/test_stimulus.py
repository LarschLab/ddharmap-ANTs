import unittest
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd

from codeants_2pf_hcr.stimulus import (
    build_null_window_start_map,
    build_prestim_baseline_windows,
    build_prestim_trial_windows,
    build_stim_tables,
    classify_stim_type,
    combine_segments,
    compute_zscore_stats,
    discover_functional_sessions,
    effective_motion_window,
    find_experiment_log,
    find_metadata_csv,
    parse_unilateral_stim,
    resolve_presented_stimulus_metadata,
    resolve_plane_stimulus_contexts,
)


class StimulusTests(unittest.TestCase):
    def test_parse_unilateral_stim(self) -> None:
        self.assertEqual(parse_unilateral_stim("LLB"), ("left", "bout"))
        self.assertEqual(parse_unilateral_stim("RLC"), ("right", "continuous"))

    def test_classify_stim_type(self) -> None:
        self.assertEqual(classify_stim_type("LLB"), "bout")
        self.assertEqual(classify_stim_type("RLC"), "continuous")
        self.assertEqual(classify_stim_type("LLB+RLC"), "mixed")

    def test_effective_motion_window_uses_onset_delay(self) -> None:
        start, end, duration = effective_motion_window(5.0, 10.0, None, 2.0)
        self.assertEqual((start, end, duration), (7.0, 15.0, 8.0))

    def test_build_stim_tables_parses_single_stim(self) -> None:
        df_evt = pd.DataFrame(
            {
                "event": ["B1_start", "B1_stim1_LLB", "B1_poststim1_pause", "B1_end"],
                "time": [0.0, 1.0, 6.0, 7.0],
            }
        )
        _, df_stim = build_stim_tables(df_evt, fps=10.0, onset_delay_sec=1.0, remove_interblock_gaps=True)
        self.assertEqual(len(df_stim), 1)
        self.assertEqual(df_stim.loc[0, "trial_id"], "B1_stim1_LLB")

    def test_presented_stimulus_metadata_validates_companion_sequence(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            log_path = root / "2026_fish_experiment_log.csv"
            trial_path = root / "2026_fish_trial_sequence.csv"
            log_path.write_text(
                "event,timestamp\n"
                "B1_start,0\n"
                "B1_stim0_WFCl,1\n"
                "B1_poststim0_pause,2\n"
                "B1_stim1_LAB_trajectory,3\n"
                "B1_poststim1_pause,4\n"
                "B1_end,5\n"
            )
            trial_path.write_text("stimulus\nWFCl\nLAB_trajectory\n")
            df_evt = pd.read_csv(log_path).rename(columns={"timestamp": "time"})
            _, df_stim = build_stim_tables(df_evt, fps=2.0, onset_delay_sec=0.0, remove_interblock_gaps=True)
            meta = resolve_presented_stimulus_metadata(log_path=log_path, df_stim=df_stim)
            self.assertEqual(meta["stimulus_types_source"], "metadata")
            self.assertEqual(meta["stimulus_types"], ["WFCl", "LAB_trajectory"])

            trial_path.write_text("stimulus\nWFCl\nRC_trajectory\n")
            with self.assertRaises(RuntimeError):
                resolve_presented_stimulus_metadata(log_path=log_path, df_stim=df_stim)

    def test_presented_stimulus_metadata_exposes_planned_schedule_blocks(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            log_path = root / "2026_fish_experiment_log.csv"
            schedule_path = root / "2026_fish_planned_schedule.csv"
            log_path.write_text(
                "event,timestamp\n"
                "B0_start,0\n"
                "B0_end,10\n"
                "B1_start,10\n"
                "B1_prestim0_pause,10\n"
                "B1_stim0_WFCl,12\n"
                "B1_poststim0_pause,14\n"
                "B1_end,20\n"
            )
            schedule_path.write_text(
                "kind,label,start_sec,duration_sec,end_sec,trial_index,block_num,stimulus_name\n"
                "trigger,B0_start,0,0,0,,0,\n"
                "rest,Baseline rest,0,10,10,,0,\n"
                "trigger,B1_start,10,0,10,,1,\n"
                "prestim_pause,Trial 1 pre-pause,10,2,12,0,1,WFCl\n"
                "stimulus,WFCl,12,2,14,0,1,WFCl\n"
                "poststim_pause,Trial 1 post-pause,14,2,16,0,1,WFCl\n"
            )
            df_evt = pd.read_csv(log_path).rename(columns={"timestamp": "time"})
            _, df_stim = build_stim_tables(df_evt, fps=1.0, onset_delay_sec=0.0, remove_interblock_gaps=True)

            meta = resolve_presented_stimulus_metadata(log_path=log_path, df_stim=df_stim)

            self.assertEqual(meta["stimulus_sequence"], ["WFCl"])
            self.assertEqual(meta["planned_schedule_blocks"]["block"].tolist(), ["B0", "B1"])
            self.assertEqual(meta["planned_schedule_stimuli"]["block"].tolist(), ["B1"])
            self.assertEqual(meta["planned_schedule_stimuli"]["start"].tolist(), [12])

    def test_prestim_window_builders(self) -> None:
        df_evt = pd.DataFrame(
            {
                "event": [
                    "B1_start",
                    "B1_prestim1_pause",
                    "B1_stim1_LLB",
                    "B1_poststim1_pause",
                    "B1_end",
                ],
                "time": [0.0, 1.0, 3.0, 5.0, 6.0],
            }
        )
        baseline = build_prestim_baseline_windows(df_evt, 2.0, 1.0)
        trials = build_prestim_trial_windows(df_evt, 2.0, 1.0)
        start_map = build_null_window_start_map(trials, [4], step_frames=1, min_windows=1)
        self.assertEqual(baseline, [(2, 8)])
        self.assertEqual(trials[0]["duration_frames"], 6)
        self.assertTrue(isinstance(start_map[4], np.ndarray))

    def test_compute_zscore_stats_flags_low_sigma(self) -> None:
        dff = np.asarray([[1, 1, 1, 1], [0, 1, 2, 3]], dtype=np.float32)
        stats = compute_zscore_stats(dff, [(0, 4)], min_points=2, sigma_eps=1e-6)
        self.assertFalse(bool(stats["valid"][0]))
        self.assertTrue(bool(stats["valid"][1]))

    def test_combine_segments_mean_and_sem(self) -> None:
        arr = np.asarray([[1.0, 3.0], [3.0, np.nan]], dtype=np.float32)
        mean, sem = combine_segments(arr)
        self.assertTrue(np.allclose(mean, np.asarray([2.0, 3.0], dtype=np.float32), equal_nan=True))
        self.assertTrue(np.isfinite(sem[0]))
        self.assertTrue(np.isnan(sem[1]))

    def test_resolve_plane_stimulus_contexts_uses_preprocessing_session_planes(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fish_id = "L758_f02"
            fish_dir = root / fish_id
            meta_dir = fish_dir / "01_raw" / "2p" / "metadata"
            preproc_dir = fish_dir / "02_reg" / "00_preprocessing" / "2p_functional" / "01_individualPlanes"
            meta_dir.mkdir(parents=True)
            preproc_dir.mkdir(parents=True)
            (meta_dir / f"2026_f{fish_id}_metadata.csv").write_text("parameter,value\nframerate,2.0\n")
            (meta_dir / f"2026_f{fish_id}_experiment_log.csv").write_text(
                "event,timestamp\nB1_start,0\nB1_stim1_LLB,1\nB1_poststim1_pause,3\nB1_end,4\n"
            )
            (meta_dir / f"2026_f{fish_id}_r2_metadata.csv").write_text("parameter,value\nframerate,4.0\n")
            (meta_dir / f"2026_f{fish_id}_r2_experiment_log.csv").write_text(
                "event,timestamp\nB1_start,0\nB1_stim1_RLC,2\nB1_poststim1_pause,5\nB1_end,6\n"
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

            sessions = discover_functional_sessions(fish_dir, fish_id)
            self.assertEqual(sessions[0]["output_planes"], [0, 1, 2, 3, 4])
            self.assertEqual(find_experiment_log(fish_dir, fish_id, session_label="r2").name, f"2026_f{fish_id}_r2_experiment_log.csv")
            self.assertEqual(find_metadata_csv(fish_dir, fish_id, session_label="r1").name, f"2026_f{fish_id}_metadata.csv")

            contexts = resolve_plane_stimulus_contexts(fish_dir=fish_dir, fish_id=fish_id, plane_indices=[0, 5])

            self.assertEqual(contexts[0]["session_label"], "r1")
            self.assertEqual(contexts[5]["session_label"], "r2")
            self.assertEqual(contexts[0]["frame_rate"], 2.0)
            self.assertEqual(contexts[5]["frame_rate"], 4.0)
            self.assertEqual(contexts[0]["df_stim"].loc[0, "type"], "LLB")
            self.assertEqual(contexts[5]["df_stim"].loc[0, "type"], "RLC")

    def test_resolve_plane_stimulus_contexts_infers_equal_split_without_preprocessing_metadata(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fish_id = "L758_f03"
            fish_dir = root / fish_id
            meta_dir = fish_dir / "01_raw" / "2p" / "metadata"
            meta_dir.mkdir(parents=True)
            (meta_dir / f"2026_f{fish_id}_r1_metadata.csv").write_text("parameter,value\nframerate,2.0\n")
            (meta_dir / f"2026_f{fish_id}_r1_experiment_log.csv").write_text(
                "event,timestamp\nB1_start,0\nB1_stim1_LLB,1\nB1_poststim1_pause,3\nB1_end,4\n"
            )
            (meta_dir / f"2026_f{fish_id}_r2_metadata.csv").write_text("parameter,value\nframerate,4.0\n")
            (meta_dir / f"2026_f{fish_id}_r2_experiment_log.csv").write_text(
                "event,timestamp\nB1_start,0\nB1_stim1_RLC,2\nB1_poststim1_pause,5\nB1_end,6\n"
            )

            contexts = resolve_plane_stimulus_contexts(fish_dir=fish_dir, fish_id=fish_id, plane_indices=list(range(10)))

            self.assertEqual({plane: contexts[plane]["session_label"] for plane in range(10)}, {**{plane: "r1" for plane in range(5)}, **{plane: "r2" for plane in range(5, 10)}})
            self.assertEqual(contexts[0]["session_mapping_source"], "inferred_equal_split")
            self.assertEqual(contexts[5]["session_mapping_source"], "inferred_equal_split")
            self.assertEqual(contexts[0]["frame_rate"], 2.0)
            self.assertEqual(contexts[5]["frame_rate"], 4.0)
            self.assertEqual(contexts[0]["df_stim"].loc[0, "type"], "LLB")
            self.assertEqual(contexts[5]["df_stim"].loc[0, "type"], "RLC")


if __name__ == "__main__":
    unittest.main()
