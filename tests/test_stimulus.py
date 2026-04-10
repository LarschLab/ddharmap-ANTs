import unittest

import numpy as np
import pandas as pd

from codeants_2pf_hcr.stimulus import (
    build_null_window_start_map,
    build_prestim_baseline_windows,
    build_prestim_trial_windows,
    build_stim_tables,
    classify_stim_type,
    compute_zscore_stats,
    effective_motion_window,
    parse_unilateral_stim,
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


if __name__ == "__main__":
    unittest.main()
