import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd
from skimage import transform
import tifffile

from codeants_2pf_hcr.matching import resample_labels_nn as matching_resample_labels_nn
from codeants_2pf_hcr.plots.qa import (
    render_functional_anatomy_center_overlay_qc_png,
    render_functional_anatomy_plane_qc_row_png,
    show_ants_registration_region_selector_stage,
    show_centroid_match_qa_stage,
    show_inplane_registration_method_comparison_stage,
    show_regional_match_review_stage,
)


class PlotsQaTests(unittest.TestCase):
    def test_functional_anatomy_center_overlay_qc_png_writes_review_artifacts(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            func_label_dir = root / "func_labels_in_anat"
            func_label_dir.mkdir()
            anatomy = np.zeros((2, 30, 30), dtype=np.uint8)
            anatomy[0, 10:20, 10:20] = 140
            anatomy[1, 8:18, 12:22] = 160
            anatomy_labels = np.zeros((2, 30, 30), dtype=np.uint16)
            anatomy_labels[0, 12:18, 12:18] = 3
            anatomy_labels[1, 10:16, 14:20] = 4
            anatomy_path = root / "anatomy.tif"
            anatomy_labels_path = root / "anatomy_labels.tif"
            tifffile.imwrite(anatomy_path, anatomy)
            tifffile.imwrite(anatomy_labels_path, anatomy_labels)

            refs = []
            for plane_idx, best_z in ((0, 0), (1, 1)):
                plane_label = f"fish_plane{plane_idx}_mcorrected_flipX"
                func_labels = np.zeros((30, 30), dtype=np.uint16)
                func_labels[11:17, 11 + plane_idx : 17 + plane_idx] = 10 + plane_idx
                tifffile.imwrite(func_label_dir / f"{plane_label}_func_mask_in_2p.tif", func_labels)
                refs.append(
                    {
                        "label": plane_label,
                        "index": plane_idx,
                        "best_z": best_z,
                        "anat_label_z_mode": "direct",
                    }
                )
            plane_refs_path = root / "plane_refs_summary.json"
            plane_refs_path.write_text(json.dumps(refs))

            result = render_functional_anatomy_center_overlay_qc_png(
                fish_id="fish",
                plane_refs_summary_path=plane_refs_path,
                anatomy_stack_path=anatomy_path,
                anatomy_labels_path=anatomy_labels_path,
                functional_labels_anatomy_dir=func_label_dir,
                out_path=root / "center_overlay.png",
                crop_size_px=12,
            )

            self.assertTrue(result["out_path"].exists())
            self.assertTrue(result["review_csv"].exists())
            self.assertEqual(result["n_planes"], 2)
            self.assertEqual(result["panel_count"], 2)
            self.assertEqual(result["review_df"]["crop_size_px"].tolist(), [12, 12])
            self.assertTrue((result["review_df"]["functional_label_count_crop"] > 0).all())
            self.assertTrue((result["review_df"]["anatomy_label_count_crop"] > 0).all())

    def test_functional_anatomy_plane_qc_row_png_writes_review_artifacts(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            ref_dir = root / "refs"
            label_dir = root / "suite2p_labels"
            positioned_dir = root / "positioned"
            ref_dir.mkdir()
            label_dir.mkdir()
            positioned_dir.mkdir()
            anatomy = np.zeros((2, 20, 20), dtype=np.uint8)
            anatomy[0, 4:14, 5:15] = 120
            anatomy[1, 2:12, 8:18] = 160
            anatomy_labels = np.zeros((2, 20, 20), dtype=np.uint16)
            anatomy_labels[0, 6:12, 7:13] = 3
            anatomy_labels[1, 4:9, 10:16] = 4
            anatomy_path = root / "anatomy.tif"
            anatomy_labels_path = root / "anatomy_labels.tif"
            tifffile.imwrite(anatomy_path, anatomy)
            tifffile.imwrite(anatomy_labels_path, anatomy_labels)

            refs = []
            for plane_idx, best_z in ((0, 0), (1, 1)):
                plane_label = f"fish_plane{plane_idx}_mcorrected_flipX"
                ref = np.zeros((20, 20), dtype=np.uint8)
                ref[3 + plane_idx : 14, 4:15] = 140
                roi_labels = np.zeros((20, 20), dtype=np.uint16)
                roi_labels[5:8, 5:8] = 1 + plane_idx
                roi_labels[10:14, 10:15] = 3 + plane_idx
                ref_path = ref_dir / f"{plane_label}_ref_norm.tif"
                tifffile.imwrite(ref_path, ref)
                tifffile.imwrite(label_dir / f"{plane_label}_suite2p_labels_native.tif", roi_labels)
                tifffile.imwrite(positioned_dir / f"{plane_label}_ants_rigid_affine_warped.tif", anatomy[best_z])
                refs.append(
                    {
                        "label": plane_label,
                        "index": plane_idx,
                        "best_z": best_z,
                        "anat_label_z_mode": "direct",
                        "reference_norm_path": str(ref_path),
                    }
                )
            plane_refs_path = root / "plane_refs_summary.json"
            plane_refs_path.write_text(json.dumps(refs))

            result = render_functional_anatomy_plane_qc_row_png(
                fish_id="fish",
                plane_refs_summary_path=plane_refs_path,
                anatomy_stack_path=anatomy_path,
                anatomy_labels_path=anatomy_labels_path,
                suite2p_label_dir=label_dir,
                out_path=root / "plane_qc.png",
                positioned_reference_dir=positioned_dir,
            )

            self.assertTrue(result["out_path"].exists())
            self.assertTrue(result["review_csv"].exists())
            self.assertEqual(result["n_planes"], 2)
            self.assertEqual(result["panel_count"], 10)
            self.assertEqual(result["review_df"]["suite2p_label_count"].tolist(), [2, 2])
            self.assertEqual(result["review_df"]["has_positioned_functional_reference"].tolist(), [True, True])

    def test_centroid_match_qa_threads_selected_ants_transform_helper(self) -> None:
        anat_labels = np.zeros((1, 6, 6), dtype=np.uint32)
        anat_labels[0, 2:4, 2:4] = 1
        func_labels = np.zeros((4, 4), dtype=np.uint32)
        func_labels[1:3, 1:3] = 2
        ants_transform = {
            "type": "ants_transformlist",
            "transformlist": ["plane0_0GenericAffine.mat"],
            "moving_shape": (4, 4),
        }
        plane_ref = {
            "label": "plane0",
            "best_z": 0,
            "suite2p": {"labels": func_labels},
            "tform_src": "ants_rigid_affine",
            "ants_transform": ants_transform,
        }
        captured_tform_funcs = []

        def fake_build_plane_centroid_matches(*args, **kwargs):
            captured_tform_funcs.append(kwargs["tform_for_plane_func"])
            return {
                "status": "ok",
                "links_df": pd.DataFrame(
                    [
                        {
                            "func_label": 2,
                            "anat_label": 1,
                            "fx_anat_px": 2.5,
                            "fy_anat_px": 2.5,
                            "ax_px": 2.5,
                            "ay_px": 2.5,
                            "dist_px": 0.0,
                            "dist_um": 0.0,
                            "overlap_px": 4,
                        }
                    ]
                ),
                "func_warped": np.asarray(args[1]),
            }

        with patch(
            "codeants_2pf_hcr.plots.qa.build_plane_centroid_matches",
            side_effect=fake_build_plane_centroid_matches,
        ):
            result = show_centroid_match_qa_stage(
                plane_refs=[plane_ref],
                anat_labels_all=anat_labels,
                use_suite2p_labels=True,
                render_ui=False,
                force_recompute=True,
            )

        self.assertTrue(result["ok"])
        self.assertGreaterEqual(len(captured_tform_funcs), 1)
        self.assertIs(captured_tform_funcs[0](plane_ref), ants_transform)
        self.assertIs(result["helpers"]["tform_for_plane"](plane_ref), ants_transform)

    def test_ants_registration_region_selector_writes_ncc_guided_per_plane_regions(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            out_reg = root / "reg"
            template = np.zeros((4, 4), dtype=np.float32)
            template[1:3, 1:3] = 1.0
            anat = np.zeros((2, 12, 12), dtype=np.float32)
            anat[0, 3:7, 4:8] = template
            anat[1, 5:9, 2:6] = template
            plane_refs = [
                {"label": "plane0", "best_z": 0, "ref_match": template},
                {"label": "plane1", "best_z": 1, "ref_match": template},
            ]

            result = show_ants_registration_region_selector_stage(
                plane_refs=plane_refs,
                anat_stack=anat,
                fish_id="fish",
                out_reg=out_reg,
                save_square=True,
                reuse_saved_square=False,
                margin_fraction=0.10,
                use_cv2=False,
            )

            self.assertTrue(result["ok"])
            payload = json.loads((out_reg / "ants_registration_region_square.json").read_text())
            self.assertEqual(payload["method"], "ncc_guided_ants_region_square")
            self.assertEqual(len(payload["regions"]), 2)
            self.assertEqual(payload["regions"][0]["ncc_xy"]["x0"], 4)
            self.assertEqual(payload["regions"][0]["ncc_xy"]["y0"], 3)
            self.assertEqual(payload["regions"][1]["ncc_xy"]["x0"], 2)
            self.assertEqual(payload["regions"][1]["ncc_xy"]["y0"], 5)
            self.assertEqual(payload["regions"][0]["requested_size_px"], 5)

    def test_regional_match_review_warps_roi_labels_into_anatomy_space(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            out_reg = root / "reg"
            out_qa = root / "qa"
            out_reg.mkdir()
            (out_reg / "regional_match_qa_square.json").write_text(
                json.dumps({"bounds_xyxy": [3, 3, 8, 8]})
            )
            anat_labels = np.zeros((1, 10, 10), dtype=np.uint32)
            anat_labels[0, 4:6, 4:6] = 7
            anat = np.zeros((1, 10, 10), dtype=np.float32)
            anat[0, 4:6, 4:6] = 1.0
            roi_labels = np.zeros((4, 4), dtype=np.uint32)
            roi_labels[1:3, 1:3] = 2
            tform = transform.SimilarityTransform(translation=(3, 3))
            plane_refs = [
                {
                    "label": "plane0",
                    "best_z": 0,
                    "ref_match": np.zeros((4, 4), dtype=np.float32),
                    "suite2p": {"labels": roi_labels},
                    "tform": tform,
                }
            ]

            with patch(
                "codeants_2pf_hcr.plots.qa.resample_labels_nn",
                wraps=matching_resample_labels_nn,
            ) as resample_mock:
                result = show_regional_match_review_stage(
                    plane_refs=plane_refs,
                    anat_labels_all=anat_labels,
                    anat_stack=anat,
                    out_reg=out_reg,
                    out_qa=out_qa,
                    render_display=False,
                    save_outputs=True,
                )

            self.assertTrue(result["ok"])
            self.assertEqual(result["rendered"], 1)
            self.assertEqual(len(result["saved_paths"]), 1)
            self.assertTrue(Path(result["saved_paths"][0]).exists())
            self.assertEqual(resample_mock.call_count, 1)
            self.assertIs(resample_mock.call_args.args[1], tform)
            self.assertEqual(resample_mock.call_args.kwargs["output_shape"], (10, 10))
            self.assertTrue(bool(result["review_df"].iloc[0]["transform_applied"]))
            self.assertEqual(int(result["review_df"].iloc[0]["n_anat_labels"]), 1)

    def test_inplane_method_comparison_saves_regional_review(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            out_reg = root / "reg"
            out_qa = root / "qa"
            out_reg.mkdir()
            (out_reg / "regional_match_qa_square.json").write_text(
                json.dumps({"center_x": 6, "center_y": 6, "size_px": 6})
            )
            anat = np.zeros((1, 12, 12), dtype=np.float32)
            anat[0, 3:9, 3:9] = 1.0
            ncc = np.zeros((12, 12), dtype=np.float32)
            ncc[4:8, 4:8] = 1.0
            ants = np.zeros((12, 12), dtype=np.float32)
            ants[2:6, 2:6] = 1.0
            plane_refs = [
                {
                    "label": "plane0",
                    "best_z": 0,
                    "inplane_registration": {
                        "ncc_xy": {"display_warped": ncc, "post_ncc": 0.7, "valid_fraction": 0.9},
                        "ants_rigid_affine": {"display_warped": ants, "post_ncc": 0.8, "valid_fraction": 0.6},
                    },
                }
            ]

            result = show_inplane_registration_method_comparison_stage(
                plane_refs=plane_refs,
                anat=anat,
                fish_id="fish",
                out_qa=out_qa,
                out_reg=out_reg,
                render_display=False,
                save_outputs=True,
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["rendered"], 1)
            self.assertTrue(result["used_regional_crop"])
            self.assertEqual(len(result["saved_paths"]), 1)
            self.assertTrue(Path(result["saved_paths"][0]).exists())

    def test_inplane_method_comparison_renders_roi_and_anatomy_boundaries(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            out_qa = root / "qa"
            anat = np.zeros((1, 10, 10), dtype=np.float32)
            anat[0, 2:8, 2:8] = 1.0
            anat_labels = np.zeros((1, 10, 10), dtype=np.uint32)
            anat_labels[0, 3:7, 3:7] = 4
            roi_labels = np.zeros((4, 4), dtype=np.uint32)
            roi_labels[1:3, 1:3] = 2
            plane_refs = [
                {
                    "label": "plane0",
                    "best_z": 0,
                    "ref_match": np.zeros((4, 4), dtype=np.float32),
                    "suite2p": {"labels": roi_labels},
                    "inplane_registration": {
                        "ncc_xy": {
                            "display_warped": np.ones((10, 10), dtype=np.float32),
                            "transform": transform.SimilarityTransform(translation=(3, 3)),
                            "post_ncc": 0.7,
                            "valid_fraction": 1.0,
                        },
                        "ants_rigid_affine": {
                            "display_warped": np.ones((10, 10), dtype=np.float32),
                            "transform": transform.SimilarityTransform(translation=(4, 3)),
                            "post_ncc": 0.8,
                            "valid_fraction": 1.0,
                        },
                    },
                }
            ]

            result = show_inplane_registration_method_comparison_stage(
                plane_refs=plane_refs,
                anat=anat,
                anat_labels_all=anat_labels,
                out_qa=out_qa,
                render_display=False,
                save_outputs=True,
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["rendered"], 1)
            self.assertEqual(result["anat_labels_src"], "anat_labels_all")
            self.assertEqual(len(result["saved_paths"]), 1)
            self.assertTrue(Path(result["saved_paths"][0]).exists())

    def test_inplane_method_comparison_infers_reversed_anatomy_label_z_mode(self) -> None:
        anat = np.zeros((3, 20, 20), dtype=np.float32)
        anat[2, 6:14, 6:14] = 1.0
        anat_labels = np.zeros((3, 20, 20), dtype=np.uint32)
        anat_labels[0, 6:14, 6:14] = 4
        anat_labels[2, 0:8, 0:8] = 9
        plane_ref = {
            "label": "plane0",
            "best_z": 2,
            "inplane_registration": {
                "ncc_xy": {"display_warped": np.ones((20, 20), dtype=np.float32), "post_ncc": 0.7},
                "ants_rigid_affine": {"display_warped": np.ones((20, 20), dtype=np.float32), "post_ncc": 0.8},
            },
        }

        result = show_inplane_registration_method_comparison_stage(
            plane_refs=[plane_ref],
            anat=anat,
            anat_labels_all=anat_labels,
            render_display=False,
            save_outputs=False,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(plane_ref["anat_label_z_mode"], "reverse")
        self.assertTrue(any("inferred anatomy-label Z mode: reverse" in line for line in result["log_lines"]))

    def test_inplane_method_comparison_falls_back_when_method_or_square_missing(self) -> None:
        anat = np.zeros((1, 8, 8), dtype=np.float32)
        plane_refs = [
            {
                "label": "plane0",
                "best_z": 0,
                "inplane_registration": {
                    "ncc_xy": {"display_warped": np.ones((8, 8), dtype=np.float32), "post_ncc": 0.5}
                },
            }
        ]

        result = show_inplane_registration_method_comparison_stage(
            plane_refs=plane_refs,
            anat=anat,
            out_qa=None,
            out_reg=None,
            render_display=False,
            save_outputs=False,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["rendered"], 1)
        self.assertFalse(result["used_regional_crop"])
        self.assertEqual(result["saved_paths"], [])
        self.assertTrue(any("full FOV" in line for line in result["log_lines"]))
