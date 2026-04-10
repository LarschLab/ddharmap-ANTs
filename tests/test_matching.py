import unittest

import pandas as pd

from codeants_2pf_hcr.matching import build_anat_identity_lookup_df, gene_from_mask


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


if __name__ == "__main__":
    unittest.main()
