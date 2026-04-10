import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

from codeants_2pf_hcr.activity import ActivityConfig, build_response_bpi_tables, prepare_pairs_for_unique_cells


class ActivityTests(unittest.TestCase):
    def test_prepare_pairs_for_unique_cells_filters_required_surface(self) -> None:
        pairs = pd.DataFrame(
            {
                "gene": ["tac3b", "tac3b"],
                "conf_mask": ["mask.tif", "mask.tif"],
                "conf_label": [1, 2],
                "anat_label": [10, 11],
                "func_label": [5, 6],
                "plane": [0, 0],
                "is_selected_for_analysis": [True, False],
            }
        )
        out = prepare_pairs_for_unique_cells(pairs, strict=False)
        self.assertEqual(len(out), 1)
        self.assertEqual(int(out.iloc[0]["func_label"]), 5)

    def test_build_response_bpi_tables_classifies_response_states(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            metadata_dir = root / "01_raw" / "2p" / "metadata"
            metadata_dir.mkdir(parents=True)
            (metadata_dir / "fish_metadata.csv").write_text("parameter,value\nframerate,2\n")
            (metadata_dir / "fish_experiment_log.csv").write_text(
                "\n".join(
                    [
                        "event,time",
                        "B1_start,0",
                        "B1_prestim1_pause,0",
                        "B1_stim1_LLB,1",
                        "B1_poststim1_pause,2",
                        "B1_prestim2_pause,2",
                        "B1_stim2_LLB,3",
                        "B1_poststim2_pause,4",
                        "B1_prestim3_pause,4",
                        "B1_stim3_LLB,5",
                        "B1_poststim3_pause,6",
                        "B1_prestim4_pause,6",
                        "B1_stim4_RLC,7",
                        "B1_poststim4_pause,8",
                        "B1_prestim5_pause,8",
                        "B1_stim5_RLC,9",
                        "B1_poststim5_pause,10",
                        "B1_prestim6_pause,10",
                        "B1_stim6_RLC,11",
                        "B1_poststim6_pause,12",
                        "B1_end,13",
                    ]
                )
            )
            plane_dir = root / "suite2P" / "plane0"
            plane_dir.mkdir(parents=True)
            f_raw = np.asarray(
                [
                    [10, 10, 10, 15, 15, 15, 10, 10, 10, 15, 15, 15, 10, 10, 10, 15, 15, 15, 10, 10, 10, 11, 11, 11, 10, 10],
                    [10, 10, 10, 10.05, 10.05, 10.05, 10, 10, 10, 10.05, 10.05, 10.05, 10, 10, 10, 10.05, 10.05, 10.05, 10, 10, 10, 10.05, 10.05, 10.05, 10, 10],
                    [10, 10, 10, 10.68, 10.68, 10.68, 10, 10, 10, 10.68, 10.68, 10.68, 10, 10, 10, 10.68, 10.68, 10.68, 10, 10, 10, 10.56, 10.56, 10.56, 10, 10],
                    [10, 10, 10, 15, 15, 15, 10, 10, 10, 15, 15, 15, 10, 10, 10, 15, 15, 15, 10, 10, 10, 15, 15, 15, 10, 10],
                ],
                dtype=np.float32,
            )
            np.save(plane_dir / "F.npy", f_raw)
            np.save(plane_dir / "ops.npy", {"fs": 2.0}, allow_pickle=True)
            detail_df = pd.DataFrame(
                {
                    "plane_idx": [0, 0, 0, 0],
                    "func_label": [1, 2, 3, 4],
                    "plane": ["p0", "p0", "p0", "p0"],
                    "anat_label": [10, 11, 12, 13],
                    "identity_display_label": ["g1", "g2", "g3", "g4"],
                    "activity_class": ["Active neurons"] * 4,
                    "is_active": [True, True, True, False],
                    "func_source": [str(plane_dir)] * 4,
                    "suite2p_is_cell": [True, True, True, False],
                }
            )
            config = ActivityConfig(
                stim_onset_delay_sec=0.0,
                response_min_auc=0.03,
                response_null_q=0.0,
                response_null_bootstrap_n=8,
                response_null_min_windows=1,
                response_null_step_sec=0.5,
                min_trials_per_class=3,
            )
            out = build_response_bpi_tables(
                detail_df,
                fish_dir=root,
                fish_id="fish",
                suite2p_root=root / "suite2P",
                config=config,
            )
            scored = out["scored_bpi_df"].sort_values("func_label").reset_index(drop=True)
            self.assertEqual(scored.loc[0, "response_class"], "bout-responsive")
            self.assertEqual(scored.loc[1, "response_class"], "low activity")
            self.assertEqual(scored.loc[2, "bpi_category"], "weak-response")
            self.assertEqual(scored.loc[3, "response_class"], "response unavailable")


if __name__ == "__main__":
    unittest.main()
