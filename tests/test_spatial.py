import unittest

import numpy as np

from codeants_2pf_hcr.spatial import apply_func_orientation, best_z_by_ncc, corrcoef_img


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


if __name__ == "__main__":
    unittest.main()
