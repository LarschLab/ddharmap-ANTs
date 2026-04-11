from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import numpy as np
import tifffile

from codeants_2pf_hcr.spatial import (
    RegistrationSearchConfig,
    apply_func_orientation,
    best_z_by_ncc,
    corrcoef_img,
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


if __name__ == "__main__":
    unittest.main()
