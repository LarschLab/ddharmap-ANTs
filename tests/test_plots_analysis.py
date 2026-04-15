import unittest
from tempfile import TemporaryDirectory
from pathlib import Path

import pandas as pd

from codeants_2pf_hcr.plots.analysis import (
    render_cohort_50l_responsive_identity_donut_row,
    render_single_fish_50l_responsive_identity_donut,
)


class PlotsAnalysisTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
