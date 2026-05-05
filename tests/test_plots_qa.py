import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import numpy as np
from skimage import transform

from codeants_2pf_hcr.plots.qa import show_ants_registration_region_selector_stage, show_inplane_registration_method_comparison_stage


class PlotsQaTests(unittest.TestCase):
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
