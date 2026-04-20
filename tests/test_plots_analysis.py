import unittest
from tempfile import TemporaryDirectory
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from codeants_2pf_hcr.plots.analysis import (
    render_cohort_56h_status_donut_grid,
    render_cohort_50l_responsive_identity_donut_row,
    render_single_fish_50l_responsive_identity_donut,
    render_single_fish_50l_bpi_panel,
)


class PlotsAnalysisTests(unittest.TestCase):
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
