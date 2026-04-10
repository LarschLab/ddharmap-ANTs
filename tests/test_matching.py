import unittest

import numpy as np
import pandas as pd

from codeants_2pf_hcr.matching import (
    build_anat_identity_lookup_df,
    build_functional_roi_master_df,
    build_hcr_activity_tables,
    gene_from_mask,
)


def _suite2p_plane(*rois: tuple[list[int], list[int]], iscell: list[bool] | None = None) -> dict:
    stat = []
    for ypix, xpix in rois:
        stat.append({"ypix": np.asarray(ypix), "xpix": np.asarray(xpix), "overlap": np.zeros(len(ypix), dtype=bool)})
    iscell_arr = np.asarray([[1.0 if flag else 0.0, 0.0] for flag in (iscell or [True] * len(stat))], dtype=float)
    return {"stat": stat, "ops": {"Ly": 6, "Lx": 6}, "iscell": iscell_arr, "plane_dir": "plane0"}


class MatchingTests(unittest.TestCase):
    def test_gene_from_mask_normalizes_sst1_names(self) -> None:
        self.assertEqual(gene_from_mask("fish_round1_channel2_sst1_2_cp_masks.tif"), "sst1.2")

    def test_build_anat_identity_lookup_df_orders_genes(self) -> None:
        results = [
            {
                "mask_path": "fish_round1_channel2_tac3b_cp_masks.tif",
                "final_pairs": pd.DataFrame({"twoP_label": [5, 5]}),
            },
            {
                "mask_path": "fish_round2_channel3_sst1_1_cp_masks.tif",
                "final_pairs": pd.DataFrame({"twoP_label": [5]}),
            },
        ]
        df = build_anat_identity_lookup_df(results, gene_order=["sst1.1", "tac3b"])
        self.assertEqual(df.loc[0, "identity_label"], "sst1.1/tac3b")

    def test_build_functional_roi_master_df_assigns_one_to_one_matches(self) -> None:
        anat = np.zeros((1, 6, 6), dtype=np.uint32)
        anat[0, 1:3, 1:3] = 1
        anat[0, 3:5, 3:5] = 2
        suite2p = {
            0: _suite2p_plane(([1, 1, 2, 2], [1, 2, 1, 2]), ([3, 3, 4, 4], [3, 4, 3, 4]), iscell=[True, False])
        }
        detail_df, _ = build_functional_roi_master_df(suite2p, [{"best_z": 0, "label": "p0"}], anat)
        self.assertEqual(detail_df["anat_label"].dropna().astype(int).tolist(), [1, 2])
        self.assertTrue(detail_df["has_unique_anat_match"].all())

    def test_build_functional_roi_master_df_keeps_unmatched_roi(self) -> None:
        anat = np.zeros((1, 6, 6), dtype=np.uint32)
        anat[0, 1:3, 1:3] = 1
        suite2p = {
            0: _suite2p_plane(([1, 1, 2, 2], [1, 2, 1, 2]), ([4, 4, 5, 5], [0, 1, 0, 1]), iscell=[True, True])
        }
        detail_df, _ = build_functional_roi_master_df(suite2p, [{"best_z": 0, "label": "p0"}], anat)
        unmatched = detail_df.loc[detail_df["func_label"] == 2].iloc[0]
        self.assertTrue(pd.isna(unmatched["anat_label"]))
        self.assertFalse(bool(unmatched["has_unique_anat_match"]))

    def test_build_hcr_activity_tables_prefers_local_responsive_candidate(self) -> None:
        anat = np.zeros((1, 6, 6), dtype=np.uint32)
        anat[0, 1:3, 1:3] = 7
        suite2p = {
            0: _suite2p_plane(
                ([1, 1, 2, 2], [1, 2, 1, 2]),
                ([1, 2], [1, 2]),
                iscell=[True, True],
            )
        }
        hcr_results = [
            {
                "mask_path": "fish_round1_channel2_tac3b_cp_masks.tif",
                "final_pairs": pd.DataFrame({"twoP_label": [7], "conf_label": [3]}),
            }
        ]
        response_lookup = pd.DataFrame(
            {
                "plane_idx": [0, 0],
                "func_label": [1, 2],
                "response_is_active": [False, True],
                "response_class": ["low activity", "bout-responsive"],
                "response_summary_class": ["Low activity", "Responsive neurons"],
            }
        )
        _, _, analysis_df, candidate_df, _ = build_hcr_activity_tables(
            suite2p,
            [{"best_z": 0, "label": "p0"}],
            anat,
            hcr_results,
            response_lookup_df=response_lookup,
        )
        self.assertEqual(int(analysis_df.iloc[0]["func_label"]), 2)
        self.assertEqual(
            sorted(candidate_df["response_summary_class"].astype(str).unique().tolist()),
            ["Low activity", "Responsive neurons"],
        )

    def test_build_hcr_activity_tables_normalizes_string_response_booleans(self) -> None:
        anat = np.zeros((1, 6, 6), dtype=np.uint32)
        anat[0, 1:3, 1:3] = 7
        suite2p = {
            0: _suite2p_plane(
                ([1, 1, 2, 2], [1, 2, 1, 2]),
                ([1, 2], [1, 2]),
                iscell=[True, True],
            )
        }
        hcr_results = [
            {
                "mask_path": "fish_round1_channel2_tac3b_cp_masks.tif",
                "final_pairs": pd.DataFrame({"twoP_label": [7], "conf_label": [3]}),
            }
        ]
        response_lookup = pd.DataFrame(
            {
                "plane_idx": [0, 0],
                "func_label": [1, 2],
                "response_is_active": ["False", "True"],
                "response_class": ["low activity", "bout-responsive"],
                "response_summary_class": ["Low activity", "Responsive neurons"],
            }
        )
        status_df, _, analysis_df, _, _ = build_hcr_activity_tables(
            suite2p,
            [{"best_z": 0, "label": "p0"}],
            anat,
            hcr_results,
            response_lookup_df=response_lookup,
        )
        self.assertEqual(int(analysis_df.iloc[0]["func_label"]), 2)
        self.assertEqual(status_df.iloc[0]["functional_status"], "in-plane active ROI")

    def test_bool_from_any_treats_fractional_float_as_true(self) -> None:
        from codeants_2pf_hcr.matching import _bool_from_any

        self.assertTrue(_bool_from_any(0.5))
        self.assertFalse(_bool_from_any(0.0))


if __name__ == "__main__":
    unittest.main()
