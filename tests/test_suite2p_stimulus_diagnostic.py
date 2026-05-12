import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from codeants_2pf_hcr.suite2p import (
    Suite2pStimulusLockedDiagnosticConfig,
    build_suite2p_stimulus_locked_diagnostic,
    _remap_stimulus_tables_to_frame_grid,
    run_suite2p_stimulus_locked_diagnostic_stage,
)
from codeants_2pf_hcr.activity import ActivityConfig
from codeants_2pf_hcr.plots.analysis import _suite2p_stim_panel_layout, render_suite2p_full_session_heatmap


class Suite2pStimulusDiagnosticTests(unittest.TestCase):
    def test_frame_grid_remap_places_blocks_from_trace_frame_count(self) -> None:
        df_evt = pd.DataFrame(
            {
                "event": [
                    "B1_start",
                    "B1_stim0_LLB",
                    "B1_end",
                    "B2_start",
                    "B2_stim0_RLC",
                    "B2_end",
                ],
                "time": [0.0, 2.0, 8.0, 8.0, 10.0, 16.0],
            }
        )
        df_stim = pd.DataFrame(
            {
                "block": ["B1", "B2"],
                "stim_idx": [0, 0],
                "type": ["LLB", "RLC"],
                "start": [2.0, 10.0],
                "end": [5.0, 13.0],
                "duration": [3.0, 3.0],
            }
        )

        evt, stim, meta = _remap_stimulus_tables_to_frame_grid(df_evt, df_stim, n_frames=40, fps=2.0)

        self.assertEqual(meta["frames_per_block"], 20)
        self.assertEqual(meta["block_start_frames"], {"B1": 0, "B2": 20})
        self.assertEqual(float(evt.loc[evt["event"] == "B2_start", "time"].iloc[0]), 10.0)
        self.assertEqual(float(stim.loc[stim["block"] == "B2", "start"].iloc[0]), 12.0)

    def test_frame_grid_remap_rejects_nondivisible_frame_count(self) -> None:
        df_evt = pd.DataFrame({"event": ["B1_start", "B2_start"], "time": [0.0, 8.0]})
        df_stim = pd.DataFrame({"block": ["B1", "B2"], "type": ["LLB", "RLC"], "start": [2.0, 10.0]})

        with self.assertRaisesRegex(RuntimeError, "not divisible"):
            _remap_stimulus_tables_to_frame_grid(df_evt, df_stim, n_frames=41, fps=2.0)

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
            self.assertEqual(result["full_session_heatmap_matrix"].shape, (3, 20))
            self.assertEqual(result["full_session_heatmap_rows"]["session_label"].tolist(), ["r1", "r2", "r2"])
            spans = result["full_session_stimulus_spans"]
            self.assertEqual(set(spans["stim_type"]), {"WFCl", "LAB_trajectory"})
            self.assertEqual(spans.loc[spans["stim_type"] == "WFCl", "frame_start"].iloc[0], 4)
            self.assertEqual(spans.loc[spans["stim_type"] == "WFCl", "frame_end"].iloc[0], 10)
            self.assertEqual(spans.loc[spans["stim_type"] == "WFCl", "row_start"].iloc[0], 0)
            self.assertEqual(spans.loc[spans["stim_type"] == "WFCl", "row_end"].iloc[0], 1)

    def test_two_column_stimulus_layout_groups_side_and_whole_field_stimuli(self) -> None:
        rows, slots = _suite2p_stim_panel_layout(["LLB", "RLB", "LLC", "RLC", "WFCl", "WFCo", "LAB_trajectory", "RAB_trajectory"])

        self.assertEqual(rows, ["LB", "LC", "WF", "AB_trajectory"])
        self.assertEqual(slots["LLB"], (0, 0))
        self.assertEqual(slots["RLB"], (0, 1))
        self.assertEqual(slots["WFCl"], (2, 0))
        self.assertEqual(slots["WFCo"], (2, 1))
        self.assertEqual(slots["LAB_trajectory"], (3, 0))
        self.assertEqual(slots["RAB_trajectory"], (3, 1))

    def test_stage_filters_trace_panels_to_response_active_but_keeps_heatmap_rows(self) -> None:
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

            base = np.linspace(1.0, 1.2, 12, dtype=np.float32)
            dff0 = base.copy()
            dff0[2:4] = 8.0
            dff0[7:9] = 8.0
            dff1 = base.copy()
            dff = np.vstack([dff0, dff1]).astype(np.float32)
            suite2p_by_ref_idx = {
                0: {"dff": dff, "iscell": np.asarray([[1, 0.9], [1, 0.8]], dtype=np.float32), "ops": {"fs": 1.0}},
            }

            result = run_suite2p_stimulus_locked_diagnostic_stage(
                fish_dir=fish_dir,
                fish_id=fish_id,
                suite2p_by_ref_idx=suite2p_by_ref_idx,
                config=Suite2pStimulusLockedDiagnosticConfig(
                    pre_sec=1.0,
                    post_sec=2.0,
                    zscore_min_baseline_points=1,
                    min_valid_frac=0.5,
                    min_trials_per_stimulus=1,
                    save_figures=False,
                    show_figures=False,
                ),
                activity_config=ActivityConfig(
                    min_trials_per_class=1,
                    stim_onset_delay_sec=0.0,
                    response_min_auc=5.0,
                    response_null_q=0.5,
                    response_null_min_windows=1,
                    response_null_bootstrap_n=5,
                    zscore_min_points=1,
                ),
            )

            self.assertEqual(set(result["trace_df"]["func_label"]), {1, 2})
            self.assertEqual(set(result["trace_panel_df"]["func_label"]), {1})
            self.assertEqual(set(result["full_session_heatmap_rows"]["func_label"]), {1, 2})

    def test_full_session_heatmap_renderer_uses_frame_axis_gray_r_and_stimulus_spans(self) -> None:
        matrix = np.asarray([[0.0, 1.0, 5.0], [np.nan, 2.0, 4.0]], dtype=np.float32)
        row_df = pd.DataFrame(
            {
                "session_label": ["r1", "r2"],
                "plane_idx": [0, 1],
                "func_label": [1, 1],
            }
        )
        spans = pd.DataFrame(
            {
                "stim_type": ["LLB"],
                "session_label": ["r1"],
                "plane_idx": [0],
                "frame_start": [1],
                "frame_end": [2],
                "row_start": [0],
                "row_end": [1],
            }
        )

        fig = render_suite2p_full_session_heatmap(
            matrix=matrix,
            row_df=row_df,
            stimulus_spans=spans,
            session_colors={"r1": "#111111", "r2": "#222222"},
            vmin=0.0,
            vmax=5.0,
            stim_alpha=0.2,
        )
        try:
            ax = fig.axes[0]
            self.assertEqual(ax.get_xlabel(), "Frame")
            self.assertEqual(len(ax.images), 1)
            cmap = ax.images[0].cmap
            low_rgb = np.asarray(cmap(0.0)[:3])
            high_rgb = np.asarray(cmap(1.0)[:3])
            self.assertGreater(float(low_rgb.sum()), float(high_rgb.sum()))
            self.assertGreaterEqual(len(ax.patches), 1)
        finally:
            plt.close(fig)


if __name__ == "__main__":
    unittest.main()
