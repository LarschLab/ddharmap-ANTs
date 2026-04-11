import unittest
from pathlib import Path

import numpy as np
import pandas as pd

from codeants_2pf_hcr.matching import (
    _regionprops_centroids_2d,
    build_anat_identity_lookup_df,
    build_functional_anatomy_debug_df,
    build_functional_roi_master_df,
    build_plane_centroid_matches,
    build_hcr_activity_tables,
    gene_from_mask,
    harmonize_functional_labels_to_anatomy,
    resample_labels_nn,
)
from codeants_2pf_hcr.plots.qa import (
    compute_anatomy_median_xy_radius_um,
    show_centroid_match_qa_stage,
)


def _suite2p_plane(*rois: tuple[list[int], list[int]], iscell: list[bool] | None = None) -> dict:
    stat = []
    for ypix, xpix in rois:
        stat.append({"ypix": np.asarray(ypix), "xpix": np.asarray(xpix), "overlap": np.zeros(len(ypix), dtype=bool)})
    iscell_arr = np.asarray([[1.0 if flag else 0.0, 0.0] for flag in (iscell or [True] * len(stat))], dtype=float)
    return {"stat": stat, "ops": {"Ly": 6, "Lx": 6}, "iscell": iscell_arr, "plane_dir": "plane0"}


class MatchingTests(unittest.TestCase):
    def test_regionprops_centroids_2d_returns_empty_schema_without_labels(self) -> None:
        df = _regionprops_centroids_2d(np.zeros((4, 4), dtype=np.uint32))
        self.assertEqual(list(df.columns), ["label", "cy", "cx"])
        self.assertTrue(df.empty)

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
        self.assertIn("gene", candidate_df.columns)
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

    def test_resample_labels_nn_identity_resizes_to_output_shape(self) -> None:
        labels = np.zeros((2, 2), dtype=np.uint32)
        labels[0, 0] = 5
        warped = resample_labels_nn(labels, output_shape=(4, 4))
        self.assertEqual(warped.shape, (4, 4))
        self.assertTrue(np.issubdtype(warped.dtype, np.unsignedinteger))
        self.assertIn(5, np.unique(warped))

    def test_harmonize_functional_labels_to_anatomy_resamples_shape_mismatch(self) -> None:
        labels = np.zeros((2, 2), dtype=np.uint32)
        labels[0, 0] = 1
        result = harmonize_functional_labels_to_anatomy(labels, {"label": "p0"}, (4, 4))
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["labels"].shape, (4, 4))

    def test_build_plane_centroid_matches_reports_overlap_filtered_pairs(self) -> None:
        func = np.zeros((4, 4), dtype=np.uint32)
        anat = np.zeros((4, 4), dtype=np.uint32)
        func[1:3, 1:3] = 3
        anat[1:3, 1:3] = 9
        result = build_plane_centroid_matches(func, anat, require_overlap=True, min_overlap=1)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["pairs_final"], 1)
        self.assertEqual(int(result["links_df"].iloc[0]["overlap_px"]), 4)

    def test_build_functional_anatomy_debug_df_reports_no_functional_labels(self) -> None:
        anat = np.zeros((1, 4, 4), dtype=np.uint32)
        df = build_functional_anatomy_debug_df(
            [{"best_z": 0, "label": "p0"}],
            anat,
            load_func_labels_for_plane_func=lambda _idx: (None, None, None),
        )
        self.assertEqual(df.iloc[0]["status"], "no_functional_labels")

    def test_bool_from_any_treats_fractional_float_as_true(self) -> None:
        from codeants_2pf_hcr.matching import _bool_from_any

        self.assertTrue(_bool_from_any(0.5))
        self.assertFalse(_bool_from_any(0.0))

    def test_compute_anatomy_median_xy_radius_um_from_labels(self) -> None:
        anat = np.zeros((1, 10, 10), dtype=np.uint32)
        anat[0, 1:3, 1:3] = 1   # area 4 px
        anat[0, 5:9, 5:9] = 2   # area 16 px
        ref = compute_anatomy_median_xy_radius_um(anat, plane_refs=[{"best_z": 0}], vox_x=1.0, vox_y=1.0)
        d1 = 2.0 * np.sqrt(4.0 / np.pi)
        d2 = 2.0 * np.sqrt(16.0 / np.pi)
        expected_median_d = float(np.median([d1, d2]))
        self.assertAlmostEqual(float(ref["median_xy_diameter_um"]), expected_median_d, places=6)
        self.assertAlmostEqual(float(ref["median_xy_radius_um"]), expected_median_d / 2.0, places=6)
        self.assertEqual(int(ref["n_labels"]), 2)

    def test_show_centroid_match_qa_stage_initial_threshold_uses_median_radius(self) -> None:
        anat = np.zeros((1, 10, 10), dtype=np.uint32)
        anat[0, 1:3, 1:3] = 1
        anat[0, 5:9, 5:9] = 2
        func = np.zeros((10, 10), dtype=np.uint32)
        func[1:3, 1:3] = 5
        plane_refs = [{"label": "p0", "best_z": 0, "tform": None}]
        result = show_centroid_match_qa_stage(
            plane_refs=plane_refs,
            anat_labels_all=anat,
            anat_stack=anat.astype(np.float32),
            out_reg=Path("/tmp"),
            outdir=Path("/tmp"),
            func_labels=[func],
            vox_anat={"X": 1.0, "Y": 1.0},
            force_recompute=True,
            render_ui=False,
        )
        ref = compute_anatomy_median_xy_radius_um(anat, plane_refs=plane_refs, vox_x=1.0, vox_y=1.0)
        self.assertAlmostEqual(float(result["initial_threshold_um"]), float(ref["median_xy_radius_um"]), places=6)

    def test_show_centroid_match_qa_stage_reports_context_when_no_links_pass_threshold(self) -> None:
        anat = np.zeros((1, 10, 10), dtype=np.uint32)
        anat[0, 1:3, 1:3] = 1
        anat[0, 7:9, 7:9] = 2
        func = np.zeros((10, 10), dtype=np.uint32)
        func[4:6, 4:6] = 5
        plane_refs = [{"label": "p0", "best_z": 0, "tform": None}]
        result = show_centroid_match_qa_stage(
            plane_refs=plane_refs,
            anat_labels_all=anat,
            anat_stack=anat.astype(np.float32),
            out_reg=Path("/tmp"),
            outdir=Path("/tmp"),
            func_labels=[func],
            vox_anat={"X": 1.0, "Y": 1.0},
            force_recompute=True,
            render_ui=False,
            initial_threshold_um=0.0,
        )
        snap = result["initial_plane_snapshot"]
        self.assertIsNotNone(snap)
        self.assertEqual(int(snap["links_at_initial_threshold"]), 0)
        self.assertGreaterEqual(int(snap["context_anat_centroids"]), 1)


if __name__ == "__main__":
    unittest.main()
