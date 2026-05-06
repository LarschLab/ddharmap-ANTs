from pathlib import Path
from tempfile import TemporaryDirectory
import builtins
import importlib.util
import json
import unittest
from unittest.mock import patch

import numpy as np
import tifffile

from codeants_2pf_hcr.spatial import (
    InPlaneRegistrationComparisonConfig,
    RegistrationSearchConfig,
    _select_square_region_spec_for_plane,
    apply_func_orientation,
    apply_square_region_mask,
    best_z_by_ncc,
    corrcoef_img,
    run_in_plane_registration_comparison_stage,
    run_registration_search_stage,
)


class SpatialTests(unittest.TestCase):
    def test_corrcoef_img_identity_is_one(self) -> None:
        img = np.arange(9, dtype=np.float32).reshape(3, 3)
        self.assertLess(abs(corrcoef_img(img, img) - 1.0), 1e-6)

    def test_apply_func_orientation_north_flips_both_axes_then_x(self) -> None:
        arr = np.array([[1, 2], [3, 4]], dtype=np.int32)
        out = apply_func_orientation(arr, polarity="north")
        self.assertEqual(out.tolist(), [[3, 4], [1, 2]])

    def test_best_z_by_ncc_picks_matching_plane(self) -> None:
        template = np.zeros((5, 5), dtype=np.float32)
        template[2, 2] = 1.0
        stack = np.zeros((3, 5, 5), dtype=np.float32)
        stack[1] = template
        best_z, scores = best_z_by_ncc(template, stack, use_cv2=False)
        self.assertEqual(best_z, 1)
        self.assertEqual(scores.shape, (3,))

    def test_apply_square_region_mask_preserves_shape_and_zeroes_outside(self) -> None:
        image = np.arange(25, dtype=np.float32).reshape(5, 5)
        spec = {"bounds_xyxy": [1, 1, 4, 3]}
        masked, mask, bounds = apply_square_region_mask(image, spec)

        self.assertEqual(masked.shape, image.shape)
        self.assertEqual(mask.shape, image.shape)
        self.assertEqual(bounds, (1, 1, 4, 3))
        np.testing.assert_array_equal(masked[1:3, 1:4], image[1:3, 1:4])
        self.assertEqual(float(masked[0].sum()), 0.0)
        self.assertEqual(float(masked[:, 0].sum()), 0.0)

    def test_select_square_region_spec_for_plane_preserves_legacy_single_square(self) -> None:
        spec = {"bounds_xyxy": [1, 1, 4, 3]}

        out = _select_square_region_spec_for_plane(spec, plane_idx=2, label="plane2")

        self.assertIs(out, spec)

    def test_select_square_region_spec_for_plane_uses_matching_plane_region(self) -> None:
        payload = {
            "method": "ncc_guided_ants_region_square",
            "regions": [
                {"plane_idx": 0, "plane_label": "plane0", "bounds_xyxy": [1, 1, 3, 3]},
                {"plane_idx": 1, "plane_label": "plane1", "bounds_xyxy": [4, 4, 8, 8]},
            ],
        }

        out = _select_square_region_spec_for_plane(payload, plane_idx=1, label="plane1")

        self.assertEqual(out["bounds_xyxy"], [4, 4, 8, 8])

    def test_select_square_region_spec_for_plane_rejects_missing_plane(self) -> None:
        payload = {
            "method": "ncc_guided_ants_region_square",
            "regions": [{"plane_idx": 0, "plane_label": "plane0", "bounds_xyxy": [1, 1, 3, 3]}],
        }

        with self.assertRaisesRegex(ValueError, "no region"):
            _select_square_region_spec_for_plane(payload, plane_idx=2, label="plane2")

    def test_run_registration_search_stage_updates_plane_refs_and_caches(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            anat_path = root / "anat.tif"
            out_ncc = root / "ncc"
            template = np.zeros((5, 5), dtype=np.float32)
            template[2, 2] = 1.0
            anat = np.zeros((3, 5, 5), dtype=np.float32)
            anat[1] = template
            tifffile.imwrite(anat_path, anat)

            plane_refs = [{"label": "plane0", "ref2d_raw": template.copy()}]
            result = run_registration_search_stage(
                anat_stack_path=anat_path,
                plane_refs=plane_refs,
                fish_id="L395_f11",
                out_ncc=out_ncc,
                config=RegistrationSearchConfig(
                    manual_scale=1.0,
                    scale_coarse=(1.0, 1.0, 0.1),
                    scale_fine=(0.0, 0.0),
                    scale_xfine=(0.0, 0.0),
                    scale_ufine=(0.0, 0.0),
                    scale_per_plane=False,
                    use_cv2=False,
                ),
            )

            self.assertEqual(result["best_z"], 1)
            self.assertEqual(len(result["df"]), 1)
            self.assertIn("ref_match", plane_refs[0])
            self.assertEqual(plane_refs[0]["best_z"], 1)
            self.assertTrue((out_ncc / "ncc_scale_by_fish.json").exists())
            self.assertTrue((out_ncc / "ncc_bestz_by_plane.json").exists())

    def test_in_plane_comparison_ncc_only_keeps_current_transform(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            out_ncc = root / "ncc"
            template = np.zeros((4, 4), dtype=np.float32)
            template[1:3, 1:3] = 1.0
            anat = np.zeros((1, 8, 8), dtype=np.float32)
            anat[0, 2:6, 3:7] = template
            plane_refs = [{"label": "plane0", "ref_match": template, "best_z": 0, "scale": 1.0}]

            result = run_in_plane_registration_comparison_stage(
                plane_refs=plane_refs,
                anat_f=anat,
                fish_id="TEST_FISH",
                out_ncc=out_ncc,
                config=InPlaneRegistrationComparisonConfig(
                    methods=("ncc_xy",),
                    active_method="ncc_xy",
                    use_cv2=False,
                ),
            )

            self.assertEqual(plane_refs[0]["tform_src"], "ncc_xy")
            self.assertIn("ncc_xy", plane_refs[0])
            self.assertIn("tform", plane_refs[0])
            self.assertEqual(len(result["comparison_df"]), 1)
            self.assertTrue((out_ncc / "inplane_registration_comparison" / "inplane_registration_comparison.csv").exists())

    def test_ants_import_is_deferred_until_ants_method_is_requested(self) -> None:
        real_import = builtins.__import__

        def blocked_import(name, *args, **kwargs):
            if name == "ants":
                raise ImportError("blocked ants import")
            return real_import(name, *args, **kwargs)

        template = np.zeros((4, 4), dtype=np.float32)
        template[1:3, 1:3] = 1.0
        anat = np.zeros((1, 8, 8), dtype=np.float32)
        anat[0, 2:6, 3:7] = template

        with patch("builtins.__import__", side_effect=blocked_import):
            ok_refs = [{"label": "plane0", "ref_match": template, "best_z": 0}]
            run_in_plane_registration_comparison_stage(
                plane_refs=ok_refs,
                anat_f=anat,
                config=InPlaneRegistrationComparisonConfig(
                    methods=("ncc_xy",),
                    active_method="ncc_xy",
                    use_cv2=False,
                    ants_fixed_mask_json="not-needed-for-ncc.json",
                ),
            )

            fail_refs = [{"label": "plane0", "ref_match": template, "best_z": 0}]
            with self.assertRaises(RuntimeError):
                run_in_plane_registration_comparison_stage(
                    plane_refs=fail_refs,
                    anat_f=anat,
                    config=InPlaneRegistrationComparisonConfig(
                        methods=("ants_rigid_affine",),
                        active_method="ants_rigid_affine",
                    ),
                )

    def test_active_ants_falls_back_to_ncc_when_mask_is_missing(self) -> None:
        template = np.zeros((4, 4), dtype=np.float32)
        template[1:3, 1:3] = 1.0
        anat = np.zeros((1, 8, 8), dtype=np.float32)
        anat[0, 2:6, 3:7] = template
        plane_refs = [{"label": "plane0", "ref_match": template, "best_z": 0}]

        result = run_in_plane_registration_comparison_stage(
            plane_refs=plane_refs,
            anat_f=anat,
            fish_id="TEST_FISH",
            config=InPlaneRegistrationComparisonConfig(
                methods=("ncc_xy", "ants_rigid_affine"),
                active_method="ants_rigid_affine",
                fallback_method="ncc_xy",
                use_cv2=False,
                ants_fixed_mask_json=None,
                ants_require_fixed_mask=True,
            ),
        )

        self.assertEqual(plane_refs[0]["tform_src"], "ncc_xy")
        self.assertEqual(plane_refs[0]["inplane_requested_active_method"], "ants_rigid_affine")
        self.assertEqual(plane_refs[0]["inplane_active_method"], "ncc_xy")
        self.assertIn("requires a saved fixed-region mask", plane_refs[0]["inplane_fallback_reason"])
        rows = result["comparison_df"]
        selected = rows.loc[rows["selected"]]
        self.assertEqual(len(selected), 1)
        self.assertEqual(str(selected.iloc[0]["method"]), "ncc_xy")
        self.assertEqual(str(selected.iloc[0]["requested_active_method"]), "ants_rigid_affine")
        self.assertEqual(str(selected.iloc[0]["selected_method"]), "ncc_xy")
        self.assertTrue(any("WARNING" in line and "using ncc_xy" in line for line in result["log_lines"]))

    def test_ants_without_mask_fails_when_no_fallback_is_configured(self) -> None:
        template = np.zeros((4, 4), dtype=np.float32)
        template[1:3, 1:3] = 1.0
        anat = np.zeros((1, 8, 8), dtype=np.float32)
        anat[0, 2:6, 3:7] = template

        with self.assertRaises(RuntimeError):
            run_in_plane_registration_comparison_stage(
                plane_refs=[{"label": "plane0", "ref_match": template, "best_z": 0}],
                anat_f=anat,
                config=InPlaneRegistrationComparisonConfig(
                    methods=("ants_rigid_affine",),
                    active_method="ants_rigid_affine",
                    fallback_method=None,
                    ants_fixed_mask_json=None,
                    ants_require_fixed_mask=True,
                ),
            )

    @unittest.skipUnless(importlib.util.find_spec("ants") is not None, "ANTsPy is not installed")
    def test_ants_in_plane_backend_writes_transform_metadata(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            mask_path = root / "ants_registration_region_square.json"
            mask_path.write_text(json.dumps({"bounds_xyxy": [8, 8, 26, 26]}))
            fixed = np.zeros((32, 32), dtype=np.float32)
            fixed[10:22, 12:24] = 1.0
            moving = fixed.copy()
            plane_refs = [{"label": "plane0", "ref_match": moving, "best_z": 0}]
            result = run_in_plane_registration_comparison_stage(
                plane_refs=plane_refs,
                anat_f=fixed[None, ...],
                fish_id="TEST_FISH",
                out_ncc=root / "ncc",
                config=InPlaneRegistrationComparisonConfig(
                    methods=("ants_rigid_affine",),
                    active_method="ants_rigid_affine",
                    ants_aff_iterations=(20, 10),
                    ants_aff_shrink_factors=(2, 1),
                    ants_aff_smoothing_sigmas=(1, 0),
                    ants_fixed_mask_json=mask_path,
                ),
            )

            self.assertEqual(plane_refs[0]["tform_src"], "ants_rigid_affine")
            self.assertIn("ants_transform", plane_refs[0])
            self.assertEqual(plane_refs[0]["ants_transform"]["type"], "ants_transformlist")
            self.assertEqual(tuple(plane_refs[0]["ref_warped"].shape), tuple(fixed.shape))
            row = result["comparison_df"].iloc[0]
            self.assertEqual(row["ants_fixed_mask_path"], str(mask_path))
            self.assertEqual(row["ants_fixed_mask_bounds_xyxy"], "[8, 8, 26, 26]")
            self.assertGreater(float(result["comparison_df"].iloc[0]["post_ncc"]), 0.5)


if __name__ == "__main__":
    unittest.main()
