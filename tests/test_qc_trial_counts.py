import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from codeants_2pf_hcr.plots.qc_functional import build_early_response_trial_count_tables


class TrialCountTableTests(unittest.TestCase):
    def test_counts_are_separate_by_session_and_show_valid_range(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            windows = root / "scored_stimulus_windows.csv"
            responses = root / "suite2p_presented_stimulus_response_23c.csv"
            pd.DataFrame([
                {"fish_id": "L765_f04", "session_label": session, "block": "B1", "stim_idx": trial, "stim_type": "LB_trajectory", "plane_idx": plane}
                for session, plane in (("r1", 0), ("r2", 5))
                for trial in range(4)
            ]).to_csv(windows, index=False)
            pd.DataFrame([
                {"fish_id": "L765_f04", "session_label": "r1", "stim_type": "LB_trajectory", "n_valid_trials": 4},
                {"fish_id": "L765_f04", "session_label": "r1", "stim_type": "LB_trajectory", "n_valid_trials": 3},
                {"fish_id": "L765_f04", "session_label": "r2", "stim_type": "LB_trajectory", "n_valid_trials": 4},
            ]).to_csv(responses, index=False)
            tables = build_early_response_trial_count_tables(
                fish_id="L765_f04", scored_windows_path=windows, presented_response_path=responses,
            )
            self.assertEqual(list(tables), ["r1", "r2"])
            self.assertEqual(tables["r1"].to_dict("records"), [
                {"stimulus": "LB_trajectory", "counts": 4, "valid_counts": "3–4"},
            ])
            self.assertEqual(tables["r2"]["valid_counts"].tolist(), ["4"])


if __name__ == "__main__":
    unittest.main()
