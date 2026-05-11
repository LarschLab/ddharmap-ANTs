import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np

from codeants_2pf_hcr.suite2p import (
    Suite2pStimulusLockedDiagnosticConfig,
    build_suite2p_stimulus_locked_diagnostic,
)


class Suite2pStimulusDiagnosticTests(unittest.TestCase):
    def test_diagnostic_uses_session_specific_metadata_and_raw_stimulus_names(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fish_id = "L758_f02"
            fish_dir = root / fish_id
            meta_dir = fish_dir / "01_raw" / "2p" / "metadata"
            preproc_dir = fish_dir / "02_reg" / "00_preprocessing" / "2p_functional" / "01_individualPlanes"
            meta_dir.mkdir(parents=True)
            preproc_dir.mkdir(parents=True)
            (preproc_dir / f"{fish_id}_preprocessing_metadata.json").write_text(
                json.dumps(
                    {
                        "sessions": [
                            {"session_label": "r1", "output_planes": [0]},
                            {"session_label": "r2", "output_planes": [1]},
                        ]
                    }
                )
            )
            (meta_dir / f"2026_f{fish_id}_metadata.csv").write_text("parameter,value\nframerate,2.0\n")
            (meta_dir / f"2026_f{fish_id}_experiment_log.csv").write_text(
                "event,timestamp\n"
                "B1_start,0\n"
                "B1_prestim0_pause,0\n"
                "B1_stim0_WFCl,2\n"
                "B1_poststim0_pause,5\n"
                "B1_end,6\n"
            )
            (meta_dir / f"2026_f{fish_id}_trial_sequence.csv").write_text("stimulus\nWFCl\n")
            (meta_dir / f"2026_f{fish_id}_r2_metadata.csv").write_text("parameter,value\nframerate,2.0\n")
            (meta_dir / f"2026_f{fish_id}_r2_experiment_log.csv").write_text(
                "event,timestamp\n"
                "B1_start,0\n"
                "B1_prestim0_pause,0\n"
                "B1_stim0_LAB_trajectory,2\n"
                "B1_poststim0_pause,5\n"
                "B1_end,6\n"
            )
            (meta_dir / f"2026_f{fish_id}_r2_trial_sequence.csv").write_text("stimulus\nLAB_trajectory\n")

            dff0 = np.vstack(
                [
                    np.linspace(0.0, 1.0, 20, dtype=np.float32),
                    np.linspace(1.0, 2.0, 20, dtype=np.float32),
                ]
            )
            dff1 = dff0 + 1.0
            suite2p_by_ref_idx = {
                0: {"dff": dff0, "iscell": np.asarray([[1, 0.9], [0, 0.1]], dtype=np.float32), "ops": {"fs": 2.0}},
                1: {"dff": dff1, "iscell": np.asarray([[1, 0.9], [1, 0.8]], dtype=np.float32), "ops": {"fs": 2.0}},
            }

            result = build_suite2p_stimulus_locked_diagnostic(
                fish_dir=fish_dir,
                fish_id=fish_id,
                suite2p_by_ref_idx=suite2p_by_ref_idx,
                config=Suite2pStimulusLockedDiagnosticConfig(
                    pre_sec=1.0,
                    post_sec=2.0,
                    zscore_min_baseline_points=2,
                    min_valid_frac=0.5,
                    suite2p_cells_only=True,
                ),
            )

            self.assertEqual(result["stim_order"], ["WFCl", "LAB_trajectory"])
            trace_df = result["trace_df"]
            self.assertEqual(set(trace_df["session_label"]), {"r1", "r2"})
            self.assertEqual(set(trace_df["stim_type"]), {"WFCl", "LAB_trajectory"})
            self.assertEqual(len(trace_df[trace_df["session_label"] == "r1"]), 1)
            self.assertEqual(len(trace_df[trace_df["session_label"] == "r2"]), 2)
            self.assertEqual(set(result["source_df"]["stimulus_types_source"]), {"metadata"})


if __name__ == "__main__":
    unittest.main()
