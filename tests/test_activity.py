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
    build_suite2p_response_seed_table,
    prepare_single_fish_bpi_diagnostics_stage,
)


class ActivityTests(unittest.TestCase):
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
