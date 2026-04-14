import unittest
from tempfile import TemporaryDirectory
from pathlib import Path

import pandas as pd

from codeants_2pf_hcr.plots.analysis import render_cohort_50l_responsive_identity_donut_row


class PlotsAnalysisTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
