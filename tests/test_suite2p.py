import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np

from codeants_2pf_hcr.suite2p import Suite2pStageConfig, load_suite2p_stage


class Suite2pStageTests(unittest.TestCase):
    def _write_minimal_suite2p_plane(self, plane_dir: Path) -> None:
        plane_dir.mkdir(parents=True)
        np.save(plane_dir / "F.npy", np.asarray([[1.0, 3.0, 5.0]], dtype=np.float32))
        np.save(plane_dir / "Fneu.npy", np.asarray([[0.1, 0.2, 0.3]], dtype=np.float32))
        np.save(plane_dir / "spks.npy", np.asarray([[0.0, 1.0, 0.0]], dtype=np.float32))
        np.save(
            plane_dir / "stat.npy",
            np.asarray(
                [{"ypix": np.asarray([1, 1, 2]), "xpix": np.asarray([1, 2, 1]), "overlap": np.asarray([False, False, False])}],
                dtype=object,
            ),
            allow_pickle=True,
        )
        np.save(plane_dir / "ops.npy", {"Ly": 4, "Lx": 5, "fs": 2.0}, allow_pickle=True)
        np.save(plane_dir / "iscell.npy", np.asarray([[1.0, 0.0]], dtype=np.float32), allow_pickle=True)

    def test_load_suite2p_stage_returns_legacy_surface(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "suite2P" / "plane0"
            self._write_minimal_suite2p_plane(root)

            plane_refs = [{"label": "fish_plane0", "index": 0}]
            result = load_suite2p_stage(
                plane_refs=plane_refs,
                suite2p_root=root.parent,
                fish_id="fish1",
                polarity="south",
                polarity_source="test",
                config=Suite2pStageConfig(verbose=False),
            )

            self.assertEqual(result["suite2p_fish_id"], "fish1")
            self.assertEqual(sorted(result["suite2p_by_ref_idx"].keys()), [0])
            self.assertEqual(len(result["func_labels"]), 1)
            self.assertIn("suite2p", plane_refs[0])
            labels = result["func_labels"][0]
            self.assertIsNotNone(labels)
            self.assertEqual(labels.shape, (4, 5))
            self.assertEqual(int(np.max(labels)), 1)
            dff = result["suite2p_by_ref_idx"][0]["dff"]
            self.assertEqual(dff.shape, (1, 3))
            f_raw = np.asarray([1.0, 3.0, 5.0], dtype=np.float32)
            f0 = np.percentile(f_raw, 10.0)
            expected = (f_raw - f0) / (f0 + 1e-6)
            self.assertTrue(np.allclose(dff[0], expected, atol=1e-6))
            self.assertEqual(result["df_sum"]["key"].tolist(), ["SUITE2P_ROOT", "SUITE2P_PLANE_GLOB", "N_PLANE_DIRS"])
            self.assertEqual(result["df_src"].loc[0, "status"], "loaded")

    def test_load_suite2p_stage_can_run_before_functional_reference_preprocessing(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "suite2P" / "plane0"
            self._write_minimal_suite2p_plane(root)

            result = load_suite2p_stage(
                plane_refs=[],
                suite2p_root=root.parent,
                fish_id="fish1",
                polarity="south",
                polarity_source="test",
                config=Suite2pStageConfig(verbose=False),
            )

            self.assertEqual(sorted(result["suite2p_by_ref_idx"].keys()), [0])
            self.assertEqual(len(result["func_labels"]), 1)
            self.assertIsNotNone(result["func_labels"][0])
            self.assertEqual(result["suite2p_by_ref_idx"][0]["ref_idx"], 0)


if __name__ == "__main__":
    unittest.main()
