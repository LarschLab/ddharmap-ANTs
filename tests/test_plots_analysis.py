import unittest
import os
from tempfile import TemporaryDirectory
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.colors import to_rgba
import numpy as np
import pandas as pd

from codeants_2pf_hcr.plots.analysis import (
    _single_fish_50l_auc_cache_stale_reasons,
    render_cohort_56h_status_donut_grid,
    render_cohort_50l_responsive_identity_donut_row,
    render_cohort_motion_auc,
    render_single_fish_50l_responsive_identity_donut,
    render_single_fish_50l_bpi_panel,
    render_single_fish_50l_composite,
    render_single_fish_50l_gene_auc_panel,
    render_single_fish_50l_global_auc_panel,
)
from codeants_2pf_hcr.plots.qa import render_single_fish_hcr_anatomy_coexpression_summary


class PlotsAnalysisTests(unittest.TestCase):
    def test_single_fish_50l_auc_cache_stale_reasons_tracks_missing_and_newer_inputs(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            points_csv = root / "motion_auc_plot_points.csv"
            counts_csv = root / "motion_auc_plot_counts.csv"
            detail_csv = root / "functional_roi_activity_identity.csv"
            status_csv = root / "hcr_activity_status.csv"
            midline_json = root / "midline_params_func_ref.json"

            def _touch(path: Path, *, mtime_ns: int) -> None:
                path.write_text(path.name)
                os.utime(path, ns=(mtime_ns, mtime_ns))

            base_ns = 1_700_000_000_000_000_000
            _touch(points_csv, mtime_ns=base_ns + 100)
            _touch(counts_csv, mtime_ns=base_ns + 100)
            _touch(detail_csv, mtime_ns=base_ns + 10)
            _touch(status_csv, mtime_ns=base_ns + 10)
            _touch(midline_json, mtime_ns=base_ns + 10)

            fresh_reasons = _single_fish_50l_auc_cache_stale_reasons(
                points_csv,
                counts_csv,
                detail_csv=detail_csv,
                hcr_status_csv=status_csv,
                midline_json=midline_json,
            )
            self.assertEqual(fresh_reasons, [])

            os.utime(detail_csv, ns=(base_ns + 200, base_ns + 200))
            detail_reasons = _single_fish_50l_auc_cache_stale_reasons(
                points_csv,
                counts_csv,
                detail_csv=detail_csv,
                hcr_status_csv=status_csv,
                midline_json=midline_json,
            )
            self.assertEqual(
                detail_reasons,
                [
                    "functional_roi_activity_identity.csv is newer than motion_auc_plot_points.csv",
                    "functional_roi_activity_identity.csv is newer than motion_auc_plot_counts.csv",
                ],
            )

            os.utime(detail_csv, ns=(base_ns + 10, base_ns + 10))
            os.utime(status_csv, ns=(base_ns + 200, base_ns + 200))
            status_reasons = _single_fish_50l_auc_cache_stale_reasons(
                points_csv,
                counts_csv,
                detail_csv=detail_csv,
                hcr_status_csv=status_csv,
                midline_json=midline_json,
            )
            self.assertEqual(
                status_reasons,
                [
                    "hcr_activity_status.csv is newer than motion_auc_plot_points.csv",
                    "hcr_activity_status.csv is newer than motion_auc_plot_counts.csv",
                ],
            )

            os.utime(status_csv, ns=(base_ns + 10, base_ns + 10))
            os.utime(midline_json, ns=(base_ns + 200, base_ns + 200))
            midline_reasons = _single_fish_50l_auc_cache_stale_reasons(
                points_csv,
                counts_csv,
                detail_csv=detail_csv,
                hcr_status_csv=status_csv,
                midline_json=midline_json,
            )
            self.assertEqual(
                midline_reasons,
                [
                    "midline_params_func_ref.json is newer than motion_auc_plot_points.csv",
                    "midline_params_func_ref.json is newer than motion_auc_plot_counts.csv",
                ],
            )

    def test_render_single_fish_50l_bpi_panel_computes_mean_auc(self) -> None:
        fig, ax = plt.subplots()
        try:
            out = render_single_fish_50l_bpi_panel(
                ax,
                pd.DataFrame(
                    {
                        "mean_bout_auc_dff": [0.2, 0.6],
                        "mean_cont_auc_dff": [0.4, 0.8],
                        "bpi": [0.3, -0.4],
                        "bpi_category": ["bout-responsive", "continuous-responsive"],
                        "response_is_active": [True, True],
                    }
                ),
                axis_label="BPI",
                title="test",
            )
            plot_df = out["plot_df"].sort_values("bpi").reset_index(drop=True)
            np.testing.assert_allclose(plot_df["mean_auc_dff"].to_numpy(dtype=float), np.array([0.7, 0.3]))
            self.assertEqual(out["n_plotted"], 2)
        finally:
            plt.close(fig)

    def test_render_single_fish_50l_bpi_panel_excludes_missing_coordinates(self) -> None:
        fig, ax = plt.subplots()
        try:
            out = render_single_fish_50l_bpi_panel(
                ax,
                pd.DataFrame(
                    {
                        "mean_bout_auc_dff": [0.2, np.nan, 0.6],
                        "mean_cont_auc_dff": [0.4, 0.5, np.nan],
                        "bpi": [0.3, 0.1, -0.2],
                        "bpi_category": ["bout-responsive", "continuous-responsive", "both-responsive"],
                        "response_is_active": [True, True, True],
                    }
                ),
                axis_label="BPI",
                title="test",
            )
            self.assertEqual(out["n_plotted"], 1)
            plot_df = out["plot_df"].reset_index(drop=True)
            self.assertAlmostEqual(float(plot_df.loc[0, "mean_auc_dff"]), 0.3, places=6)
            self.assertAlmostEqual(float(plot_df.loc[0, "bpi"]), 0.3, places=6)
        finally:
            plt.close(fig)

    def test_render_single_fish_50l_bpi_panel_excludes_response_unavailable(self) -> None:
        fig, ax = plt.subplots()
        try:
            out = render_single_fish_50l_bpi_panel(
                ax,
                pd.DataFrame(
                    {
                        "mean_bout_auc_dff": [0.2, 0.3],
                        "mean_cont_auc_dff": [0.4, 0.5],
                        "bpi": [0.3, -0.1],
                        "bpi_category": ["bout-responsive", "response unavailable"],
                        "response_is_active": [True, False],
                    }
                ),
                axis_label="BPI",
                title="test",
            )
            self.assertEqual(out["n_plotted"], 1)
            self.assertEqual(out["category_counts"], {"bout-responsive": 1})
        finally:
            plt.close(fig)

    def test_render_single_fish_50l_bpi_panel_preserves_colors_and_fill(self) -> None:
        fig, ax = plt.subplots()
        try:
            render_single_fish_50l_bpi_panel(
                ax,
                pd.DataFrame(
                    {
                        "mean_bout_auc_dff": [0.2, 0.4, 0.6],
                        "mean_cont_auc_dff": [0.4, 0.6, 0.8],
                        "bpi": [0.3, -0.3, 0.0],
                        "bpi_category": ["bout-responsive", "continuous-responsive", "weak-response"],
                        "response_is_active": [True, False, False],
                    }
                ),
                axis_label="BPI",
                title="test",
            )
            collections = [c for c in ax.collections if int(c.get_offsets().shape[0]) > 0]
            self.assertEqual(len(collections), 3)
            filled = collections[0]
            hollow_cont = collections[1]
            hollow_weak = collections[2]
            np.testing.assert_allclose(
                filled.get_facecolors()[0, :3],
                np.array([44, 127, 184], dtype=float) / 255.0,
                atol=1e-6,
            )
            self.assertEqual(int(np.count_nonzero(hollow_cont.get_facecolors()[:, -1] > 0)), 0)
            np.testing.assert_allclose(
                hollow_cont.get_edgecolors()[0, :3],
                np.array([217, 95, 14], dtype=float) / 255.0,
                atol=1e-6,
            )
            self.assertEqual(int(np.count_nonzero(hollow_weak.get_facecolors()[:, -1] > 0)), 0)
            np.testing.assert_allclose(hollow_weak.get_edgecolors()[0, :3], np.zeros(3, dtype=float), atol=1e-6)
        finally:
            plt.close(fig)

    def test_render_single_fish_50l_bpi_panel_legend_uses_hollow_low_activity_marker(self) -> None:
        fig, ax = plt.subplots()
        try:
            render_single_fish_50l_bpi_panel(
                ax,
                pd.DataFrame(
                    {
                        "mean_bout_auc_dff": [0.2, 0.4],
                        "mean_cont_auc_dff": [0.4, 0.6],
                        "bpi": [0.3, 0.0],
                        "bpi_category": ["bout-responsive", "low activity"],
                        "response_is_active": [True, False],
                    }
                ),
                axis_label="BPI",
                title="test",
            )
            legend = ax.get_legend()
            self.assertIsNotNone(legend)
            handles = legend.legend_handles
            labels = [text.get_text() for text in legend.get_texts()]
            low_idx = labels.index("Low activity (n=1)")
            low_handle = handles[low_idx]
            self.assertEqual(low_handle.get_markerfacecolor(), "none")
            np.testing.assert_allclose(
                np.array(to_rgba(low_handle.get_markeredgecolor()))[:3],
                np.array([158, 158, 158], dtype=float) / 255.0,
                atol=1e-6,
            )
        finally:
            plt.close(fig)

    def test_render_single_fish_50l_bpi_panel_zero_band_prefers_column_then_fallback(self) -> None:
        fig1, ax1 = plt.subplots()
        fig2, ax2 = plt.subplots()
        try:
            out1 = render_single_fish_50l_bpi_panel(
                ax1,
                pd.DataFrame(
                    {
                        "mean_bout_auc_dff": [0.2],
                        "mean_cont_auc_dff": [0.4],
                        "bpi": [0.3],
                        "bpi_category": ["bout-responsive"],
                        "response_is_active": [True],
                        "bpi_zero_band": [0.2],
                    }
                ),
                axis_label="BPI",
                title="test",
            )
            out2 = render_single_fish_50l_bpi_panel(
                ax2,
                pd.DataFrame(
                    {
                        "mean_bout_auc_dff": [0.2],
                        "mean_cont_auc_dff": [0.4],
                        "bpi": [0.3],
                        "bpi_category": ["bout-responsive"],
                        "response_is_active": [True],
                    }
                ),
                axis_label="BPI",
                title="test",
            )
            self.assertAlmostEqual(out1["zero_band"], 0.2, places=6)
            self.assertAlmostEqual(out2["zero_band"], 0.1, places=6)
            yvals1 = sorted({float(line.get_ydata()[0]) for line in ax1.lines})
            yvals2 = sorted({float(line.get_ydata()[0]) for line in ax2.lines})
            self.assertEqual(yvals1, [-0.2, 0.0, 0.2])
            self.assertEqual(yvals2, [-0.1, 0.0, 0.1])
        finally:
            plt.close(fig1)
            plt.close(fig2)

    def test_render_single_fish_50l_bpi_panel_fixes_y_limits(self) -> None:
        fig, ax = plt.subplots()
        try:
            out = render_single_fish_50l_bpi_panel(
                ax,
                pd.DataFrame(
                    {
                        "mean_bout_auc_dff": [0.2, 0.4],
                        "mean_cont_auc_dff": [0.4, 0.6],
                        "bpi": [0.7, -0.6],
                        "bpi_category": ["bout-responsive", "continuous-responsive"],
                        "response_is_active": [True, True],
                    }
                ),
                axis_label="BPI",
                title="test",
            )
            self.assertEqual(tuple(round(v, 6) for v in ax.get_ylim()), (-1.0, 1.0))
            self.assertEqual(tuple(round(v, 6) for v in out["y_limits"]), (-1.0, 1.0))
        finally:
            plt.close(fig)

    def test_render_single_fish_50l_global_auc_panel_uses_points_not_violin_or_box(self) -> None:
        fig, (ax, strip_ax) = plt.subplots(2, 1)
        try:
            out = render_single_fish_50l_global_auc_panel(
                ax,
                strip_ax,
                pd.DataFrame(
                    {
                        "group": ["All neurons", "All neurons"],
                        "laterality": ["ipsi", "ipsi"],
                        "stim_mode": ["bout", "continuous"],
                        "auc_dff": [0.4, 0.7],
                        "plane_idx": [0, 0],
                        "func_label": [1, 1],
                        "bpi_category": ["bout-responsive", "bout-responsive"],
                    }
                ),
                pd.DataFrame(
                    {
                        "group": ["All neurons", "All neurons"],
                        "laterality": ["ipsi", "ipsi"],
                        "stim_mode": ["bout", "continuous"],
                        "frac_responsive_used": [0.4, 0.4],
                        "frac_low_used": [0.3, 0.3],
                        "frac_other": [0.3, 0.3],
                        "n_total": [10, 10],
                    }
                ),
                "ipsi",
                "Ipsi",
                (0.0, 1.0),
            )
            self.assertEqual(len(ax.patches), 0)
            self.assertEqual(out["point_collection_count"], 1)
            self.assertGreaterEqual(len(ax.collections), 3)
        finally:
            plt.close(fig)

    def test_render_single_fish_50l_global_auc_panel_draws_pair_connectors_and_directional_means(self) -> None:
        fig, (ax, strip_ax) = plt.subplots(2, 1)
        try:
            out = render_single_fish_50l_global_auc_panel(
                ax,
                strip_ax,
                pd.DataFrame(
                    {
                        "group": ["All neurons"] * 8,
                        "laterality": ["ipsi"] * 8,
                        "stim_mode": ["bout", "continuous"] * 4,
                        "auc_dff": [0.30, 0.60, 0.45, 0.75, 0.25, 0.15, 0.10, 0.08],
                        "plane_idx": [0, 0, 0, 0, 1, 1, 2, 2],
                        "func_label": [1, 1, 2, 2, 3, 3, 4, 4],
                        "bpi_category": [
                            "bout-responsive",
                            "bout-responsive",
                            "continuous-responsive",
                            "continuous-responsive",
                            "weak-response",
                            "weak-response",
                            "low activity",
                            "response unavailable",
                        ],
                    }
                ),
                pd.DataFrame(
                    {
                        "group": ["All neurons", "All neurons"],
                        "laterality": ["ipsi", "ipsi"],
                        "stim_mode": ["bout", "continuous"],
                        "frac_responsive_used": [0.5, 0.5],
                        "frac_low_used": [0.2, 0.2],
                        "frac_other": [0.3, 0.3],
                        "n_total": [4, 4],
                    }
                ),
                "ipsi",
                "Ipsi",
                (0.0, 1.0),
            )
            self.assertEqual(out["pair_connector_count"], 4)
            self.assertEqual(out["mean_connector_count"], 2)
            self.assertEqual(out["mean_point_count"], 4)
            mean_df = out["mean_df"].sort_values(["bpi_category", "stim_mode"]).reset_index(drop=True)
            self.assertEqual(
                mean_df["bpi_category"].tolist(),
                ["bout-responsive", "bout-responsive", "continuous-responsive", "continuous-responsive"],
            )
            np.testing.assert_allclose(
                mean_df["auc_dff"].to_numpy(dtype=float),
                np.array([0.30, 0.60, 0.45, 0.75]),
                atol=1e-6,
            )
        finally:
            plt.close(fig)

    def test_render_single_fish_50l_global_auc_panel_uses_directional_and_neutral_styles(self) -> None:
        fig, (ax, strip_ax) = plt.subplots(2, 1)
        try:
            render_single_fish_50l_global_auc_panel(
                ax,
                strip_ax,
                pd.DataFrame(
                    {
                        "group": ["All neurons"] * 8,
                        "laterality": ["ipsi"] * 8,
                        "stim_mode": ["bout", "continuous"] * 4,
                        "auc_dff": [0.30, 0.60, 0.35, 0.55, 0.25, 0.20, 0.10, 0.05],
                        "plane_idx": [0, 0, 1, 1, 2, 2, 3, 3],
                        "func_label": [1, 1, 2, 2, 3, 3, 4, 4],
                        "bpi_category": [
                            "bout-responsive",
                            "bout-responsive",
                            "continuous-responsive",
                            "continuous-responsive",
                            "weak-response",
                            "weak-response",
                            "low activity",
                            "response unavailable",
                        ],
                    }
                ),
                pd.DataFrame(
                    {
                        "group": ["All neurons", "All neurons"],
                        "laterality": ["ipsi", "ipsi"],
                        "stim_mode": ["bout", "continuous"],
                        "frac_responsive_used": [0.5, 0.5],
                        "frac_low_used": [0.2, 0.2],
                        "frac_other": [0.3, 0.3],
                        "n_total": [4, 4],
                    }
                ),
                "ipsi",
                "Ipsi",
                (0.0, 1.0),
            )
            collections = [c for c in ax.collections if int(c.get_offsets().shape[0]) > 0]
            self.assertGreaterEqual(len(collections), 8)
            np.testing.assert_allclose(
                collections[0].get_facecolors()[0, :3],
                np.array([44, 127, 184], dtype=float) / 255.0,
                atol=1e-6,
            )
            np.testing.assert_allclose(
                collections[1].get_facecolors()[0, :3],
                np.array([217, 95, 14], dtype=float) / 255.0,
                atol=1e-6,
            )
            self.assertEqual(int(np.count_nonzero(collections[3].get_facecolors()[:, -1] > 0)), 0)
            np.testing.assert_allclose(
                collections[3].get_edgecolors()[0, :3],
                np.array([158, 158, 158], dtype=float) / 255.0,
                atol=1e-6,
            )
            np.testing.assert_allclose(
                collections[4].get_edgecolors()[0, :3],
                np.array([111, 111, 111], dtype=float) / 255.0,
                atol=1e-6,
            )
            mean_sizes = [float(np.asarray(c.get_sizes(), dtype=float)[0]) for c in collections[-4:]]
            self.assertTrue(all(size > 18.0 for size in mean_sizes))
        finally:
            plt.close(fig)

    def test_render_single_fish_50l_gene_auc_panel_dedicated_gene_layout(self) -> None:
        fig, (ax, strip_ax) = plt.subplots(2, 1)
        try:
            out = render_single_fish_50l_gene_auc_panel(
                ax,
                strip_ax,
                pd.DataFrame(
                    {
                        "group": ["sst1.1", "sst1.1", "npy", "npy"],
                        "laterality": ["ipsi"] * 4,
                        "stim_mode": ["bout", "continuous", "bout", "continuous"],
                        "auc_dff": [0.30, 0.50, 0.20, 0.25],
                        "response_class": ["responsive", "responsive", "low activity", "response unavailable"],
                        "response_is_active": [True, True, False, False],
                        "bpi_category": ["bout-responsive", "bout-responsive", "low activity", "response unavailable"],
                        "point_label_id": ["a", "a", "b", "b"],
                    }
                ),
                pd.DataFrame(
                    {
                        "group": ["sst1.1", "sst1.1", "npy", "npy"],
                        "laterality": ["ipsi"] * 4,
                        "stim_mode": ["bout", "continuous", "bout", "continuous"],
                        "frac_responsive_used": [1.0, 1.0, 0.0, 0.0],
                        "frac_low_used": [0.0, 0.0, 0.5, 0.5],
                        "frac_other": [0.0, 0.0, 0.5, 0.5],
                        "n_total": [1, 1, 2, 2],
                    }
                ),
                ["sst1.1", "npy"],
                np.array([0.0, 1.0], dtype=float),
                "ipsi",
                "Ipsi",
                (0.0, 1.0),
                gene_colors={"sst1.1": "#d62728", "npy": "#1f9d55"},
            )
            self.assertEqual(out["pair_connector_count"], 2)
            self.assertEqual(out["box_count"], 4)
            self.assertEqual(out["median_label_count"], 4)
            self.assertEqual(out["n_labels"]["sst1.1"], "n=1")
            self.assertEqual(out["n_labels"]["npy"], "n=2")
            self.assertGreater(len(ax.patches), 0)
        finally:
            plt.close(fig)

    def test_render_single_fish_50l_gene_auc_panel_preserves_expanded_label_top_on_shared_axes(self) -> None:
        fig = plt.figure()
        ax1 = fig.add_subplot(2, 1, 1)
        ax2 = fig.add_subplot(2, 1, 2, sharey=ax1)
        strip1 = fig.add_subplot(2, 2, 3)
        strip2 = fig.add_subplot(2, 2, 4)
        try:
            points_df = pd.DataFrame(
                {
                    "group": ["sst1.1", "sst1.1", "npy", "npy"],
                    "laterality": ["ipsi"] * 4,
                    "stim_mode": ["bout", "continuous", "bout", "continuous"],
                    "auc_dff": [0.98, 0.99, 0.97, 0.985],
                    "response_class": ["responsive"] * 4,
                    "response_is_active": [True] * 4,
                    "bpi_category": ["bout-responsive", "continuous-responsive", "bout-responsive", "continuous-responsive"],
                    "point_label_id": ["a", "a", "b", "b"],
                }
            )
            counts_df = pd.DataFrame(
                {
                    "group": ["sst1.1", "sst1.1", "npy", "npy"],
                    "laterality": ["ipsi"] * 4,
                    "stim_mode": ["bout", "continuous", "bout", "continuous"],
                    "frac_responsive_used": [1.0, 1.0, 1.0, 1.0],
                    "frac_low_used": [0.0, 0.0, 0.0, 0.0],
                    "frac_other": [0.0, 0.0, 0.0, 0.0],
                    "n_total": [1, 1, 1, 1],
                }
            )

            render_single_fish_50l_gene_auc_panel(
                ax1,
                strip1,
                points_df,
                counts_df,
                ["sst1.1", "npy"],
                np.array([0.0, 0.02], dtype=float),
                "ipsi",
                "Ipsi",
                (0.0, 1.0),
                gene_colors={"sst1.1": "#d62728", "npy": "#1f9d55"},
            )
            first_top = float(ax1.get_ylim()[1])
            self.assertGreater(first_top, 1.0)

            label_texts_1 = [txt for txt in ax1.texts if str(txt.get_text()).startswith("med=")]
            self.assertGreaterEqual(len(label_texts_1), 4)
            self.assertTrue(all(float(txt.get_position()[1]) <= first_top for txt in label_texts_1))

            render_single_fish_50l_gene_auc_panel(
                ax2,
                strip2,
                points_df,
                counts_df,
                ["sst1.1", "npy"],
                np.array([0.0, 0.02], dtype=float),
                "ipsi",
                "Ipsi again",
                (0.0, 1.0),
                gene_colors={"sst1.1": "#d62728", "npy": "#1f9d55"},
            )
            second_top = float(ax2.get_ylim()[1])
            self.assertGreaterEqual(second_top, first_top)

            label_texts_2 = [txt for txt in ax2.texts if str(txt.get_text()).startswith("med=")]
            self.assertGreaterEqual(len(label_texts_2), 4)
            self.assertTrue(all(float(txt.get_position()[1]) <= second_top for txt in label_texts_2))
        finally:
            plt.close(fig)

    def test_render_single_fish_50l_gene_auc_panel_missing_columns_raise(self) -> None:
        fig, (ax, strip_ax) = plt.subplots(2, 1)
        try:
            with self.assertRaisesRegex(RuntimeError, "Missing point columns"):
                render_single_fish_50l_gene_auc_panel(
                    ax,
                    strip_ax,
                    pd.DataFrame({"group": ["sst1.1"]}),
                    pd.DataFrame(
                        {
                            "group": ["sst1.1"],
                            "laterality": ["ipsi"],
                            "stim_mode": ["bout"],
                            "frac_responsive_used": [1.0],
                            "frac_low_used": [0.0],
                            "frac_other": [0.0],
                            "n_total": [1],
                        }
                    ),
                    ["sst1.1"],
                    np.array([0.0], dtype=float),
                    "ipsi",
                    "Ipsi",
                    (0.0, 1.0),
                )
        finally:
            plt.close(fig)

    def test_render_single_fish_50l_composite_saves_legacy_outputs_and_keys(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            out_reg = root / "registration"
            outdir = root / "plots"
            out_reg.mkdir()

            pd.DataFrame(
                {
                    "response_summary_class": [
                        "Responsive neurons",
                        "Responsive neurons",
                        "Low activity",
                        "Response unavailable",
                    ],
                    "bpi_category": [
                        "bout-responsive",
                        "continuous-responsive",
                        "low activity",
                        "response unavailable",
                    ],
                    "bpi_zero_band": [0.2, 0.2, 0.2, 0.2],
                    "response_auc_threshold": [0.05, 0.05, 0.05, 0.05],
                    "response_null_quantile": [0.99, 0.99, 0.99, 0.99],
                }
            ).to_csv(out_reg / "functional_roi_activity_identity.csv", index=False)

            point_rows = []
            count_rows = []
            specs = [
                ("All neurons", "bout-responsive", "responsive", True, 0.30, 0.54),
                ("sst1.1", "continuous-responsive", "responsive", True, 0.20, 0.62),
            ]
            for group, bpi_category, response_class, response_active, ipsi_base, contra_base in specs:
                for laterality, base in (("ipsi", ipsi_base), ("contra", contra_base)):
                    point_rows.extend(
                        [
                            {
                                "group": group,
                                "laterality": laterality,
                                "stim_mode": "bout",
                                "auc_dff": base,
                                "response_class": response_class,
                                "response_is_active": response_active,
                                "bpi_category": bpi_category,
                                "point_label_id": f"{group}-{laterality}-1",
                                "plane_idx": 0,
                                "func_label": 1,
                            },
                            {
                                "group": group,
                                "laterality": laterality,
                                "stim_mode": "continuous",
                                "auc_dff": base + 0.12,
                                "response_class": response_class,
                                "response_is_active": response_active,
                                "bpi_category": bpi_category,
                                "point_label_id": f"{group}-{laterality}-1",
                                "plane_idx": 0,
                                "func_label": 1,
                            },
                        ]
                    )
                    count_rows.extend(
                        [
                            {
                                "group": group,
                                "laterality": laterality,
                                "stim_mode": "bout",
                                "n_total": 1,
                                "frac_responsive_used": 1.0,
                                "frac_low_used": 0.0,
                                "frac_other": 0.0,
                            },
                            {
                                "group": group,
                                "laterality": laterality,
                                "stim_mode": "continuous",
                                "n_total": 1,
                                "frac_responsive_used": 1.0,
                                "frac_low_used": 0.0,
                                "frac_other": 0.0,
                            },
                        ]
                    )
            pd.DataFrame(point_rows).to_csv(out_reg / "motion_auc_plot_points.csv", index=False)
            pd.DataFrame(count_rows).to_csv(out_reg / "motion_auc_plot_counts.csv", index=False)

            out = render_single_fish_50l_composite(
                out_reg=out_reg,
                outdir=outdir,
                bpi_cells_df=pd.DataFrame(
                    {
                        "mean_bout_auc_dff": [0.2, 0.5],
                        "mean_cont_auc_dff": [0.4, 0.7],
                        "bpi": [0.3, -0.3],
                        "bpi_category": ["bout-responsive", "continuous-responsive"],
                        "response_is_active": [True, True],
                    }
                ),
                run_config={
                    "COMPOSITE_50L_FIG_WIDTH_IN": 6.0,
                    "COMPOSITE_50L_FIG_HEIGHT_IN": 5.0,
                    "COMPOSITE_50L_EXTRA_BOTTOM_HEIGHT_IN": 0.5,
                    "COMPOSITE_50L_DPI": 80,
                },
            )
            try:
                self.assertEqual(Path(out["out_path"]).name, "compound_50j_56i_unified.png")
                self.assertEqual(Path(out["pdf_path"]).name, "compound_50j_56i_unified.pdf")
                self.assertEqual(Path(out["COMPOSITE_50L_PATH"]).name, "compound_50j_56i_unified.png")
                self.assertEqual(out["FIG_50L_COMPOSITE"], out["fig"])
                self.assertEqual(out["FIG_50L_COMPOSITE_PATH"], str(out["out_path"]))
                self.assertGreater(out["FIG_50L_COMPOSITE_RGBA"].shape[0], 0)
                self.assertTrue(Path(out["out_path"]).exists())
                self.assertTrue(Path(out["pdf_path"]).exists())
                self.assertEqual(out["group_order"], ["All neurons", "sst1.1"])
                self.assertGreater(out["all_y_limits"][1], 0.66)
                self.assertEqual(out["panel_results"]["bpi"]["n_plotted"], 2)
                self.assertEqual(out["panel_results"]["global_ipsi"]["n_plotted"], 2)
                self.assertEqual(out["panel_results"]["gene_contra"]["n_plotted"], 2)
            finally:
                plt.close(out["fig"])

    def test_render_cohort_motion_auc_lane_counts_use_counts_table_denominator(self) -> None:
        with TemporaryDirectory() as tmpdir:
            cohort_outdir = Path(tmpdir) / "cohort_out"
            out = render_cohort_motion_auc(
                cohort_outdir=cohort_outdir,
                fish_specs=[],
                data_root=Path(tmpdir),
                data_mode="cluster",
                gene_order=["sst1.1"],
                gene_colors={"sst1.1": "#d62728"},
                cohort_fish_summary_df=pd.DataFrame({"fish_id": ["fishA"], "ok": [True]}),
                points_df=pd.DataFrame(
                    {
                        "fish_id": ["fishA"] * 6,
                        "group": ["All neurons"] * 4 + ["sst1.1"] * 2,
                        "laterality": ["ipsi"] * 6,
                        "stim_mode": ["bout", "continuous", "bout", "continuous", "bout", "continuous"],
                        "auc_dff": [0.30, 0.50, 0.10, 0.12, 0.42, 0.35],
                        "response_class": [
                            "responsive",
                            "responsive",
                            "low activity",
                            "low activity",
                            "responsive",
                            "responsive",
                        ],
                        "response_is_active": [True, True, False, False, True, True],
                        "bpi_category": [
                            "bout-responsive",
                            "bout-responsive",
                            "low activity",
                            "low activity",
                            "continuous-responsive",
                            "continuous-responsive",
                        ],
                        "plane_idx": [0, 0, 1, 1, 2, 2],
                        "func_label": [1, 1, 2, 2, 3, 3],
                    }
                ),
                counts_df=pd.DataFrame(
                    {
                        "fish_id": ["fishA"] * 4,
                        "group": ["All neurons", "All neurons", "sst1.1", "sst1.1"],
                        "laterality": ["ipsi"] * 4,
                        "stim_mode": ["bout", "continuous", "bout", "continuous"],
                        "n_total": [3, 3, 2, 2],
                        "frac_responsive_used": [1.0 / 3.0, 1.0 / 3.0, 0.5, 0.5],
                        "frac_low_used": [1.0 / 3.0, 1.0 / 3.0, 0.0, 0.0],
                        "frac_other": [1.0 / 3.0, 1.0 / 3.0, 0.5, 0.5],
                    }
                ),
            )
            try:
                fig = out["fig"]
                text_labels = [text.get_text() for ax in fig.axes for text in ax.texts]
                self.assertIn("n=3", text_labels)
                self.assertIn("n=2", text_labels)
            finally:
                plt.close(out["fig"])

    def _write_two_fish_fixture(self, root: Path, fish_ids: list[str], owner: str) -> None:
        for fish_id in fish_ids:
            out_reg = root / owner / fish_id / "03_analysis" / "functional" / "registration"
            out_reg.mkdir(parents=True, exist_ok=True)
            master_df = pd.DataFrame(
                {
                    "fish_id": [fish_id] * 4,
                    "plane_idx": [0, 0, 0, 0],
                    "func_label": [1, 2, 3, 4],
                    "response_is_active": [True, True, True, True],
                    "bpi_category": [
                        "bout-responsive",
                        "continuous-responsive",
                        "both-responsive",
                        "weak-response",
                    ],
                }
            )
            master_df.to_csv(out_reg / "functional_roi_activity_identity.csv", index=False)
            pairs_df = pd.DataFrame(
                {
                    "fish_id": [fish_id] * 2,
                    "gene": ["sst1.1", "npy"],
                    "conf_mask": ["m.tif", "m.tif"],
                    "conf_label": [11, 12],
                    "anat_label": [101, 102],
                    "func_label": [1, 2],
                    "plane": [0, 0],
                    "is_selected_for_analysis": [True, True],
                }
            )
            pairs_df.to_csv(out_reg / "conf_to_func_pairs.csv", index=False)

    def test_render_cohort_50l_responsive_identity_donut_row_counts(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fish_id = "A1_f01"
            owner = "ownerA"
            out_reg = root / owner / fish_id / "03_analysis" / "functional" / "registration"
            out_reg.mkdir(parents=True, exist_ok=True)

            master_df = pd.DataFrame(
                {
                    "fish_id": [fish_id] * 5,
                    "plane_idx": [0, 0, 0, 0, 0],
                    "func_label": [1, 2, 3, 4, 5],
                    "response_is_active": [True, True, True, True, False],
                    "bpi_category": [
                        "bout-responsive",
                        "continuous-responsive",
                        "both-responsive",
                        "weak-response",
                        "bout-responsive",
                    ],
                }
            )
            master_df.to_csv(out_reg / "functional_roi_activity_identity.csv", index=False)

            pairs_df = pd.DataFrame(
                {
                    "fish_id": [fish_id] * 4,
                    "gene": ["sst1.1", "sst1.1", "npy", "tac3b"],
                    "conf_mask": ["m.tif", "m.tif", "m.tif", "m.tif"],
                    "conf_label": [11, 12, 13, 14],
                    "anat_label": [101, 102, 103, 104],
                    "func_label": [1, 2, 2, 99],
                    "plane": [0, 0, 0, 0],
                    "is_selected_for_analysis": [True, True, True, True],
                }
            )
            pairs_df.to_csv(out_reg / "conf_to_func_pairs.csv", index=False)

            cohort_outdir = root / "cohort_out"
            out = render_cohort_50l_responsive_identity_donut_row(
                fish_specs=[{"owner": owner, "fish_id": fish_id}],
                data_root=root,
                data_mode="cluster",
                cohort_outdir=cohort_outdir,
                gene_order=["sst1.1", "npy", "tac3b"],
                gene_colors={"sst1.1": "#d62728", "npy": "#1f9d55", "tac3b": "#ffd400"},
            )

            counts = out["counts_df"].copy()
            inner = counts[counts["ring"] == "inner"].copy()
            outer = counts[counts["ring"] == "outer"].copy()
            n_total = int(inner["n_total_responsive"].iloc[0])

            self.assertEqual(n_total, 4)
            self.assertEqual(int(inner["count"].sum()), 4)
            self.assertEqual(int(outer["count"].sum()), 4)

            outer_sum_by_bpi = outer.groupby("bpi_category", as_index=False)["count"].sum().set_index("bpi_category")["count"].to_dict()
            inner_by_bpi = inner.set_index("bpi_category")["count"].to_dict()
            for bpi, inner_count in inner_by_bpi.items():
                self.assertEqual(int(inner_count), int(outer_sum_by_bpi.get(bpi, 0)))

            identity_counts = outer.groupby("identity_bucket", as_index=False)["count"].sum().set_index("identity_bucket")["count"].to_dict()
            self.assertEqual(int(identity_counts.get("sst1.1", 0)), 1)
            self.assertEqual(int(identity_counts.get("sst1.1/npy", 0)), 1)
            self.assertEqual(int(identity_counts.get("unidentified", 0)), 2)
            self.assertNotIn("tac3b", identity_counts)

            expected_identity_order = ["sst1.1", "npy", "tac3b", "sst1.1/npy", "unidentified"]
            self.assertEqual(out["identity_order"], expected_identity_order)
            self.assertTrue(Path(out["out_path"]).exists())
            self.assertTrue(Path(out["pdf_path"]).exists())
            self.assertTrue(Path(out["counts_csv"]).exists())
            fig = out["fig"]
            self.assertAlmostEqual(float(fig.get_figheight()), 6.2, places=1)
            self.assertGreaterEqual(float(fig.get_figwidth()), 9.0)
            self.assertEqual(len(fig.axes), 1)
            ax = fig.axes[0]
            xlim = ax.get_xlim()
            ylim = ax.get_ylim()
            self.assertAlmostEqual(float(xlim[0]), -1.2, places=2)
            self.assertAlmostEqual(float(xlim[1]), 1.2, places=2)
            self.assertAlmostEqual(float(ylim[0]), -1.2, places=2)
            self.assertAlmostEqual(float(ylim[1]), 1.2, places=2)
            geom = out["geometry"]
            self.assertAlmostEqual(float(geom["donut_scale"]), 1.0, places=6)
            self.assertAlmostEqual(float(geom["outer_ring_width"] / geom["inner_ring_width"]), 0.375, places=6)
            self.assertEqual(geom["panel_vertical_offsets"], [0.0])

    def test_render_cohort_50l_responsive_identity_donut_row_manual_scale(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fish_id = "A1_f01"
            owner = "ownerA"
            out_reg = root / owner / fish_id / "03_analysis" / "functional" / "registration"
            out_reg.mkdir(parents=True, exist_ok=True)

            master_df = pd.DataFrame(
                {
                    "fish_id": [fish_id] * 4,
                    "plane_idx": [0, 0, 0, 0],
                    "func_label": [1, 2, 3, 4],
                    "response_is_active": [True, True, True, True],
                    "bpi_category": [
                        "bout-responsive",
                        "continuous-responsive",
                        "both-responsive",
                        "weak-response",
                    ],
                }
            )
            master_df.to_csv(out_reg / "functional_roi_activity_identity.csv", index=False)

            pairs_df = pd.DataFrame(
                {
                    "fish_id": [fish_id] * 2,
                    "gene": ["sst1.1", "npy"],
                    "conf_mask": ["m.tif", "m.tif"],
                    "conf_label": [11, 12],
                    "anat_label": [101, 102],
                    "func_label": [1, 2],
                    "plane": [0, 0],
                    "is_selected_for_analysis": [True, True],
                }
            )
            pairs_df.to_csv(out_reg / "conf_to_func_pairs.csv", index=False)

            out = render_cohort_50l_responsive_identity_donut_row(
                fish_specs=[{"owner": owner, "fish_id": fish_id}],
                data_root=root,
                data_mode="cluster",
                cohort_outdir=root / "cohort_out",
                donut_scale=1.25,
                view_limit_scale=1.25,
            )
            geom = out["geometry"]
            self.assertAlmostEqual(float(geom["donut_scale"]), 1.25, places=6)
            self.assertAlmostEqual(float(geom["view_limit_scale"]), 1.25, places=6)
            self.assertAlmostEqual(float(geom["outer_ring_width"] / geom["inner_ring_width"]), 0.375, places=6)
            self.assertEqual(geom["panel_vertical_offsets"], [0.0])
            ax = out["fig"].axes[0]
            xlim = ax.get_xlim()
            self.assertAlmostEqual(float(xlim[1]), float(geom["view_limit"]), places=6)

    def test_render_cohort_50l_responsive_identity_donut_row_panel_offsets(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            owner = "ownerA"
            fish_ids = ["A1_f01", "A1_f02"]
            self._write_two_fish_fixture(root, fish_ids, owner)

            out = render_cohort_50l_responsive_identity_donut_row(
                fish_specs=[{"owner": owner, "fish_id": fish_id} for fish_id in fish_ids],
                data_root=root,
                data_mode="cluster",
                cohort_outdir=root / "cohort_out",
                panel_vertical_offsets=[0.0, 0.12],
            )
            fig = out["fig"]
            self.assertEqual(len(fig.axes), 2)
            pos0 = fig.axes[0].get_position()
            pos1 = fig.axes[1].get_position()
            self.assertAlmostEqual(float(pos0.y0 - pos1.y0), 0.12, places=3)
            geom = out["geometry"]
            self.assertEqual(geom["panel_vertical_offsets"], [0.0, 0.12])

    def test_render_cohort_50l_responsive_identity_donut_row_panel_offsets_length_guard(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            owner = "ownerA"
            fish_ids = ["A1_f01", "A1_f02"]
            self._write_two_fish_fixture(root, fish_ids, owner)
            with self.assertRaisesRegex(RuntimeError, "panel_vertical_offsets length must match"):
                render_cohort_50l_responsive_identity_donut_row(
                    fish_specs=[{"owner": owner, "fish_id": fish_id} for fish_id in fish_ids],
                    data_root=root,
                    data_mode="cluster",
                    cohort_outdir=root / "cohort_out",
                    panel_vertical_offsets=[0.1],
                )

    def test_render_single_fish_50l_responsive_identity_donut_counts_and_outputs(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fish_id = "A1_f01"
            owner = "ownerA"
            out_reg = root / owner / fish_id / "03_analysis" / "functional" / "registration"
            out_reg.mkdir(parents=True, exist_ok=True)

            master_df = pd.DataFrame(
                {
                    "fish_id": [fish_id] * 5,
                    "plane_idx": [0, 0, 0, 0, 0],
                    "func_label": [1, 2, 3, 4, 5],
                    "response_is_active": [True, True, True, True, False],
                    "bpi_category": [
                        "bout-responsive",
                        "continuous-responsive",
                        "both-responsive",
                        "weak-response",
                        "bout-responsive",
                    ],
                }
            )
            master_csv = out_reg / "functional_roi_activity_identity.csv"
            master_df.to_csv(master_csv, index=False)

            pairs_df = pd.DataFrame(
                {
                    "fish_id": [fish_id] * 4,
                    "gene": ["sst1.1", "sst1.1", "npy", "tac3b"],
                    "conf_mask": ["m.tif", "m.tif", "m.tif", "m.tif"],
                    "conf_label": [11, 12, 13, 14],
                    "anat_label": [101, 102, 103, 104],
                    "func_label": [1, 2, 2, 99],
                    "plane": [0, 0, 0, 0],
                    "is_selected_for_analysis": [True, True, True, True],
                }
            )
            conf_csv = out_reg / "conf_to_func_pairs.csv"
            pairs_df.to_csv(conf_csv, index=False)

            outdir = root / owner / fish_id / "04_plots"
            out = render_single_fish_50l_responsive_identity_donut(
                fish_id=fish_id,
                master_csv=master_csv,
                conf_func_csv=conf_csv,
                outdir=outdir,
                gene_order=["sst1.1", "npy", "tac3b"],
                gene_colors={"sst1.1": "#d62728", "npy": "#1f9d55", "tac3b": "#ffd400"},
            )

            counts = out["counts_df"].copy()
            inner = counts[counts["ring"] == "inner"].copy()
            outer = counts[counts["ring"] == "outer"].copy()
            n_total = int(inner["n_total_responsive"].iloc[0])
            self.assertEqual(n_total, 4)
            self.assertEqual(int(inner["count"].sum()), 4)
            self.assertEqual(int(outer["count"].sum()), 4)
            self.assertEqual(out["fish_id"], fish_id)

            identity_counts = outer.groupby("identity_bucket", as_index=False)["count"].sum().set_index("identity_bucket")["count"].to_dict()
            self.assertEqual(int(identity_counts.get("sst1.1", 0)), 1)
            self.assertEqual(int(identity_counts.get("sst1.1/npy", 0)), 1)
            self.assertEqual(int(identity_counts.get("unidentified", 0)), 2)
            self.assertNotIn("tac3b", identity_counts)

            self.assertTrue(Path(out["out_path"]).exists())
            self.assertTrue(Path(out["pdf_path"]).exists())
            self.assertTrue(Path(out["counts_csv"]).exists())
            self.assertTrue(Path(out["counts_wide_csv"]).exists())
            self.assertEqual(Path(out["out_path"]).name, "single_fish_50l_responsive_identity_donut.png")
            self.assertEqual(Path(out["pdf_path"]).name, "single_fish_50l_responsive_identity_donut.pdf")
            self.assertEqual(Path(out["counts_csv"]).name, "single_fish_50l_responsive_identity_donut_counts.csv")
            self.assertEqual(Path(out["counts_wide_csv"]).name, "single_fish_50l_responsive_identity_donut_counts_wide.csv")

    def test_render_single_fish_50l_responsive_identity_donut_scale_geometry(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fish_id = "A1_f01"
            owner = "ownerA"
            out_reg = root / owner / fish_id / "03_analysis" / "functional" / "registration"
            out_reg.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(
                {
                    "fish_id": [fish_id] * 4,
                    "plane_idx": [0, 0, 0, 0],
                    "func_label": [1, 2, 3, 4],
                    "response_is_active": [True, True, True, True],
                    "bpi_category": [
                        "bout-responsive",
                        "continuous-responsive",
                        "both-responsive",
                        "weak-response",
                    ],
                }
            ).to_csv(out_reg / "functional_roi_activity_identity.csv", index=False)
            pd.DataFrame(
                {
                    "fish_id": [fish_id] * 2,
                    "gene": ["sst1.1", "npy"],
                    "conf_mask": ["m.tif", "m.tif"],
                    "conf_label": [11, 12],
                    "anat_label": [101, 102],
                    "func_label": [1, 2],
                    "plane": [0, 0],
                    "is_selected_for_analysis": [True, True],
                }
            ).to_csv(out_reg / "conf_to_func_pairs.csv", index=False)

            out = render_single_fish_50l_responsive_identity_donut(
                fish_id=fish_id,
                master_csv=out_reg / "functional_roi_activity_identity.csv",
                conf_func_csv=out_reg / "conf_to_func_pairs.csv",
                outdir=root / owner / fish_id / "04_plots",
                donut_scale=1.25,
                view_limit_scale=1.25,
            )
            geom = out["geometry"]
            self.assertAlmostEqual(float(geom["donut_scale"]), 1.25, places=6)
            self.assertAlmostEqual(float(geom["view_limit_scale"]), 1.25, places=6)
            self.assertAlmostEqual(float(geom["outer_ring_width"] / geom["inner_ring_width"]), 0.375, places=6)

    def test_render_single_fish_hcr_anatomy_coexpression_summary_outputs(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fish_id = "A1_f01"
            status_csv = root / "hcr_activity_status.csv"
            pd.DataFrame(
                {
                    "fish_id": [fish_id] * 7,
                    "gene": ["sst1.1", "npy", "sst1.1", "sst1.1", "cfos", "tac3b", "pth2"],
                    "anat_label": [101, 101, 102, 102, 103, 104, 105],
                    "represented_on_func_plane": [True, True, True, True, True, False, True],
                    "functional_status": [
                        "in-plane responsive ROI",
                        "in-plane low-activity ROI",
                        "in-plane response unavailable",
                        "in-plane responsive ROI",
                        "out-of-plane anatomy label",
                        "in-plane responsive ROI",
                        "in-plane no functional ROI candidate",
                    ],
                    "dist_conf_anat_um": [1.0, 1.1, 2.0, 2.2, 3.0, 4.0, 5.0],
                    "selected_dist_um": [0.5, 0.6, 0.7, 0.8, np.nan, 0.9, np.nan],
                    "selected_overlap_px": [12, 10, 8, 9, 0, 4, 0],
                    "selected_plane": [0, 0, 1, 1, pd.NA, 1, pd.NA],
                    "selected_func_label": [1, 1, 2, 2, pd.NA, 3, pd.NA],
                }
            ).to_csv(status_csv, index=False)

            out = render_single_fish_hcr_anatomy_coexpression_summary(
                fish_id=fish_id,
                status_csv=status_csv,
                outdir=root / "plots",
                gene_order=["sst1.1", "npy", "tac3b", "pth2", "cfos"],
            )

            summary_df = out["summary_df"].copy()
            combo_counts_df = out["combo_counts_df"].copy()
            bucket_counts_df = out["bucket_counts_df"].copy()

            self.assertEqual(set(summary_df["anat_label"].astype(int).tolist()), {101, 102, 105})
            self.assertEqual(
                summary_df.set_index("anat_label")["gene_combo_label"].astype(str).to_dict(),
                {101: "sst1.1/npy", 102: "sst1.1", 105: "pth2"},
            )
            self.assertEqual(
                summary_df.set_index("anat_label")["is_putative_coexpression"].astype(bool).to_dict(),
                {101: True, 102: False, 105: False},
            )
            self.assertEqual(int(combo_counts_df.iloc[0]["n_anatomy_labels"]), 1)
            self.assertEqual(str(combo_counts_df.iloc[0]["gene_combo_label"]), "sst1.1/npy")
            self.assertEqual(
                bucket_counts_df.set_index("marker_count_bucket")["n_anatomy_labels"].astype(int).to_dict(),
                {"1": 2, "2": 1, "3+": 0},
            )
            self.assertTrue(Path(out["out_path"]).exists())
            self.assertTrue(Path(out["pdf_path"]).exists())
            self.assertTrue(Path(out["summary_csv"]).exists())
            self.assertTrue(Path(out["combo_counts_csv"]).exists())
            self.assertEqual(Path(out["out_path"]).name, "single_fish_hcr_anatomy_coexpression_summary.png")
            self.assertEqual(Path(out["summary_csv"]).name, "single_fish_hcr_anatomy_coexpression_summary.csv")

    def test_render_single_fish_hcr_anatomy_coexpression_summary_handles_no_multigene_labels(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fish_id = "A1_f01"
            status_csv = root / "hcr_activity_status.csv"
            pd.DataFrame(
                {
                    "fish_id": [fish_id, fish_id],
                    "gene": ["sst1.1", "npy"],
                    "anat_label": [101, 102],
                    "represented_on_func_plane": [True, True],
                    "functional_status": ["in-plane responsive ROI", "in-plane no functional ROI candidate"],
                    "dist_conf_anat_um": [1.0, 2.0],
                }
            ).to_csv(status_csv, index=False)

            out = render_single_fish_hcr_anatomy_coexpression_summary(
                fish_id=fish_id,
                status_csv=status_csv,
                outdir=root / "plots",
            )

            self.assertEqual(int(out["summary_df"]["is_putative_coexpression"].astype(bool).sum()), 0)
            self.assertTrue(out["combo_counts_df"].empty)

    def test_render_cohort_56h_status_donut_grid_uses_unmatched_summary(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            owner = "ownerA"
            fish_id = "A1_f01"
            out_reg = root / owner / fish_id / "03_analysis" / "functional" / "registration"
            out_reg.mkdir(parents=True, exist_ok=True)

            pd.DataFrame(
                {
                    "fish_id": [fish_id, fish_id, fish_id],
                    "gene": ["sst1.1", "sst1.1", "sst1.1"],
                    "functional_status": [
                        "in-plane responsive ROI",
                        "in-plane low-activity ROI",
                        "out-of-plane anatomy label",
                    ],
                }
            ).to_csv(out_reg / "hcr_activity_status.csv", index=False)
            pd.DataFrame(
                {
                    "fish_id": [fish_id, fish_id, fish_id],
                    "gene": ["sst1.1", "sst1.1", "sst1.1"],
                    "inner_status": ["within functional planes", "outside functional planes", "unmatched"],
                    "outer_status": ["in-plane responsive ROI", "out-of-plane anatomy label", "unmatched"],
                    "n_labels": [2, 1, 2],
                }
            ).to_csv(out_reg / "hcr_activity_status_summary.csv", index=False)

            out = render_cohort_56h_status_donut_grid(
                fish_specs=[{"owner": owner, "fish_id": fish_id}],
                data_root=root,
                data_mode="cluster",
                cohort_outdir=root / "cohort_out",
                cohort_fish_summary_df=None,
                gene_order=["sst1.1"],
            )
            counts = out["counts_df"].copy()
            row = counts[(counts["fish_id"] == fish_id) & (counts["gene"] == "sst1.1")].iloc[0]
            self.assertEqual(int(row["unmatched"]), 2)
            self.assertEqual(int(row["n_total_hq_masks"]), 5)

            ax = out["fig"].axes[0]
            inner_total = 0
            outer_total = 0
            for txt in ax.texts:
                label = str(txt.get_text()).strip()
                if not label.isdigit():
                    continue
                r = float(np.hypot(*txt.get_position()))
                if r > 0.9:
                    outer_total += int(label)
                elif r > 0.55:
                    inner_total += int(label)
            self.assertEqual(inner_total, int(row["n_total_hq_masks"]))
            self.assertEqual(outer_total, int(row["n_total_hq_masks"]))
            self.assertTrue(Path(out["out_path"]).exists())

    def test_render_cohort_56h_status_donut_grid_missing_summary_fails_fast(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            owner = "ownerA"
            fish_id = "A1_f01"
            out_reg = root / owner / fish_id / "03_analysis" / "functional" / "registration"
            out_reg.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(
                {
                    "fish_id": [fish_id],
                    "gene": ["sst1.1"],
                    "functional_status": ["in-plane responsive ROI"],
                }
            ).to_csv(out_reg / "hcr_activity_status.csv", index=False)

            with self.assertRaisesRegex(RuntimeError, r"Rerun single-fish \[50e\]"):
                render_cohort_56h_status_donut_grid(
                    fish_specs=[{"owner": owner, "fish_id": fish_id}],
                    data_root=root,
                    data_mode="cluster",
                    cohort_outdir=root / "cohort_out",
                    cohort_fish_summary_df=None,
                    gene_order=["sst1.1"],
                )


if __name__ == "__main__":
    unittest.main()
