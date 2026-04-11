from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import numpy as np
import tifffile

from codeants_2pf_hcr.segmentation import (
    HcrCellposeConfig,
    collect_hcr_intensity_stack_paths,
    deduplicate_hcr_intensity_targets,
    export_suite2p_native_labels_stage,
    resolve_functional_labels_for_plane,
    resolve_hcr_cellpose_model_path,
    resolve_native_suite2p_labels_for_plane,
    run_hcr_cellpose_stage,
)


class SegmentationTests(unittest.TestCase):
    def test_collect_hcr_intensity_stack_paths_filters_channel1_fullbrain_and_masks(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            rbest = root / "rbest"
            rbest.mkdir()
            keep = rbest / "fish_round1_channel2_gene.nrrd"
            skip_channel1 = rbest / "fish_round1_channel1_GCaMP.nrrd"
            skip_fullbrain = rbest / "fish_fullbrain_round2_channel2_gene.nrrd"
            skip_masks = rbest / "fish_round3_channel2_gene_cp_masks.tif"
            for path in (keep, skip_channel1, skip_fullbrain, skip_masks):
                path.write_bytes(b"")
            paths = collect_hcr_intensity_stack_paths(preproc_dir=root)
            self.assertEqual(paths, [keep])

    def test_deduplicate_hcr_intensity_targets_prefers_nrrd(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            out_masks = root / "cp_masks"
            out_masks.mkdir()
            nrrd_path = root / "gene_round1_channel2.nrrd"
            tif_path = root / "gene_round1_channel2.tif"
            candidate_pairs, duplicate_targets = deduplicate_hcr_intensity_targets([tif_path, nrrd_path], out_masks)
            self.assertEqual(duplicate_targets, 1)
            self.assertEqual(candidate_pairs[0][0], nrrd_path)

    def test_resolve_hcr_cellpose_model_path_prefers_override_then_defaults(self) -> None:
        out = resolve_hcr_cellpose_model_path(
            model_path_override="/tmp/override-model",
            cp_hcr_model_path="/tmp/run-config-model",
            cp_hcr_model_path_default="/tmp/default-model",
            nas_root="/tmp/nas",
        )
        self.assertEqual(out, "/tmp/override-model")

    def test_resolve_functional_labels_for_plane_uses_cellpose_and_orients(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            out_seg = root / "seg"
            out_seg.mkdir()
            mask_path = out_seg / "plane0_cellpose_masks.tif"
            tifffile.imwrite(mask_path, np.array([[0, 1], [2, 0]], dtype=np.uint16))
            plane_ref = {"label": "plane0", "ref_match": np.zeros((2, 2), dtype=np.float32)}

            labels, src = resolve_functional_labels_for_plane(
                plane_ref,
                0,
                use_suite2p_labels=False,
                out_seg=out_seg,
                apply_func_orientation_func=lambda arr: np.asarray(arr)[:, ::-1],
            )
            self.assertEqual(src, f"Cellpose masks: {mask_path}")
            self.assertEqual(labels.tolist(), [[1, 0], [0, 2]])

    def test_resolve_native_suite2p_labels_for_plane_prefers_plane_ref(self) -> None:
        plane_ref = {"suite2p": {"labels": np.array([[0, 1], [0, 2]], dtype=np.uint16)}}
        labels, src = resolve_native_suite2p_labels_for_plane(plane_ref, 0, func_labels=[np.ones((2, 2), dtype=np.uint16)])
        self.assertEqual(src, "plane_refs.suite2p.labels")
        self.assertEqual(labels.shape, (2, 2))

    def test_export_suite2p_native_labels_stage_writes_manifest_and_images(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            out_qa = root / "qa"
            out_raw = root / "raw"
            outdir = root / "out"
            out_qa.mkdir()
            out_raw.mkdir()
            outdir.mkdir()
            plane_refs = [
                {
                    "label": "plane0",
                    "suite2p": {"labels": np.array([[0, 1], [2, 0]], dtype=np.uint16)},
                    "ref2d": np.array([[0.0, 1.0], [2.0, 3.0]], dtype=np.float32),
                }
            ]
            result = export_suite2p_native_labels_stage(
                plane_refs=plane_refs,
                suite2p_by_ref_idx={0: {"labels": plane_refs[0]["suite2p"]["labels"]}},
                out_qa=out_qa,
                out_raw=out_raw,
                outdir=outdir,
            )
            self.assertEqual(result["status"], "exported")
            self.assertTrue(result["manifest_csv"].exists())
            self.assertEqual(len(result["manifest_df"]), 1)
            self.assertTrue((result["export_dir"] / "plane0_suite2p_labels_native.tif").exists())

    def test_run_hcr_cellpose_stage_skips_import_when_outputs_exist(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fish_dir = root / "fish"
            preproc_dir = root / "preproc"
            out_masks = fish_dir / "03_analysis" / "confocal" / "raw" / "cp_masks"
            preproc_rbest = preproc_dir / "rbest"
            preproc_rbest.mkdir(parents=True)
            out_masks.mkdir(parents=True)
            input_path = preproc_rbest / "fish_round1_channel2_gene.nrrd"
            input_path.write_bytes(b"")
            (out_masks / "fish_round1_channel2_gene_cp_masks.tif").write_bytes(b"ready")
            result = run_hcr_cellpose_stage(
                fish_dir=fish_dir,
                preproc_dir=preproc_dir,
                nas_root=root,
                config=HcrCellposeConfig(skip_if_exists=True),
            )
            self.assertEqual(result["status"], "cached")
            self.assertIn("CP_MODEL_PATH", result["bindings"])
            self.assertTrue(any("skipping Cellpose import/model load" in line for line in result["log_lines"]))


if __name__ == "__main__":
    unittest.main()
