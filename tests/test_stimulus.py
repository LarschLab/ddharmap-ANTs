import unittest

import pandas as pd

from codeants_2pf_hcr.stimulus import build_stim_tables, effective_motion_window, parse_unilateral_stim


class StimulusTests(unittest.TestCase):
    def test_parse_unilateral_stim(self) -> None:
        self.assertEqual(parse_unilateral_stim("LLB"), ("left", "bout"))
        self.assertEqual(parse_unilateral_stim("RLC"), ("right", "continuous"))

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


if __name__ == "__main__":
    unittest.main()
