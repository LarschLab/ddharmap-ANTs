from pathlib import Path
from tempfile import TemporaryDirectory
import json
import unittest
from unittest.mock import patch

import numpy as np
import tifffile

from codeants_2pf_hcr.segmentation import (
    AnatomyCellposeConfig,
    anatomy_mask_binding_path,
    HcrCellposeConfig,
    collect_hcr_intensity_stack_paths,
    deduplicate_hcr_intensity_targets,
    export_suite2p_native_labels_stage,
    hessian_valley_subtract,
    hessian_valley_tag,
    resolve_functional_labels_for_plane,
    resolve_hcr_cellpose_model_path,
    resolve_native_suite2p_labels_for_plane,
    run_anatomy_cellpose_stage,
    run_hcr_cellpose_stage,
)
from codeants_2pf_hcr.plots.qa import show_centroid_match_qa_stage


class SegmentationTests(unittest.TestCase):
    def test_collect_hcr_intensity_stack_paths_filters_channel1_fullbrain_and_masks(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            rbest = root / "rbest"
            rbest.mkdir()
            keep = rbest / "fish_rbest_channel2_gene.nrrd"
            keep_legacy = rbest / "fish_round1_channel2_gene.nrrd"
            skip_sidecar = rbest / "._fish_rbest_channel2_gene.nrrd"
            skip_channel1 = rbest / "fish_rbest_channel1_GCaMP.nrrd"
            skip_fullbrain = rbest / "fish_fullbrain_r2_channel2_gene.nrrd"
            skip_masks = rbest / "fish_r3_channel2_gene_cp_masks.tif"
            for path in (keep, keep_legacy, skip_sidecar, skip_channel1, skip_fullbrain, skip_masks):
                path.write_bytes(b"")
            paths = collect_hcr_intensity_stack_paths(preproc_dir=root)
            self.assertEqual(paths, [keep, keep_legacy])

    def test_collect_hcr_intensity_stack_paths_filters_explicit_sidecars(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            keep = root / "fish_rbest_channel2_gene.nrrd"
            skip_sidecar = root / "._fish_rbest_channel2_gene.nrrd"
            keep.write_bytes(b"")
            skip_sidecar.write_bytes(b"")
            paths = collect_hcr_intensity_stack_paths(hcr_intensity_paths=[skip_sidecar, keep])
            self.assertEqual(paths, [keep])

    def test_collect_hcr_intensity_stack_paths_can_restrict_to_rbest(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            rbest = root / "rbest"
            rn = root / "rn"
            rbest.mkdir()
            rn.mkdir()
            keep_rbest = rbest / "fish_rbest_channel2_gene.nrrd"
            skip_rn = rn / "fish_r2_channel2_gene.nrrd"
            keep_rbest.write_bytes(b"")
            skip_rn.write_bytes(b"")
            self.assertEqual(collect_hcr_intensity_stack_paths(preproc_dir=root, source="rbest"), [keep_rbest])

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

    def test_resolve_functional_labels_for_plane_keeps_in_memory_func_labels_orientation(self) -> None:
        plane_ref = {"label": "plane0", "ref_match": np.zeros((3, 3), dtype=np.float32)}
        oriented = np.array([[0, 0, 3], [2, 0, 0], [0, 1, 0]], dtype=np.uint16)

        labels, src = resolve_functional_labels_for_plane(
            plane_ref,
            0,
            use_suite2p_labels=False,
            func_labels=[oriented],
            apply_func_orientation_func=lambda arr: np.asarray(arr)[:, ::-1],
        )

        self.assertEqual(src, "func_labels list[0]")
        self.assertTrue(np.array_equal(labels, oriented))

    def test_resolve_functional_labels_for_plane_uses_same_suite2p_source_for_26_and_34_paths(self) -> None:
        plane_ref = {
            "label": "plane0",
            "ref_match": np.zeros((2, 2), dtype=np.float32),
            "suite2p": {"labels": np.array([[0, 1], [2, 0]], dtype=np.uint16)},
        }
        func_labels = [np.array([[9, 9], [9, 9]], dtype=np.uint16)]

        labels_26, src_26 = resolve_functional_labels_for_plane(
            plane_ref,
            0,
            use_suite2p_labels=True,
            func_labels=func_labels,
            apply_func_orientation_func=lambda arr: np.asarray(arr)[:, ::-1],
        )
        labels_34, src_34 = resolve_functional_labels_for_plane(
            plane_ref,
            0,
            use_suite2p_labels=True,
            func_labels=func_labels,
            apply_func_orientation_func=lambda arr: np.asarray(arr)[:, ::-1],
        )

        self.assertEqual(src_26, "Suite2p labels (plane_refs)")
        self.assertEqual(src_34, "Suite2p labels (plane_refs)")
        self.assertTrue(np.array_equal(labels_26, labels_34))

    def test_resolve_functional_labels_for_plane_orients_func_labels_path_once(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            labels_path = root / "func_labels.tif"
            tifffile.imwrite(labels_path, np.array([[[0, 1], [2, 0]]], dtype=np.uint16))
            plane_ref = {"label": "plane0", "index": 0, "ref_match": np.zeros((2, 2), dtype=np.float32)}

            labels, src = resolve_functional_labels_for_plane(
                plane_ref,
                0,
                use_suite2p_labels=False,
                func_labels_path=labels_path,
                apply_func_orientation_func=lambda arr: np.asarray(arr)[:, ::-1],
            )

            self.assertEqual(src, f"FUNC_LABELS_PATH[0]: {labels_path.name}")
            self.assertEqual(labels.tolist(), [[1, 0], [0, 2]])

    def test_show_centroid_match_qa_stage_threads_suite2p_policy(self) -> None:
        plane_refs = [
            {
                "label": "plane0",
                "best_z": 0,
                "ref_match": np.zeros((2, 2), dtype=np.float32),
                "suite2p": {"labels": np.array([[0, 1], [2, 0]], dtype=np.uint16)},
            }
        ]
        anat_labels = np.array([[[0, 1], [2, 0]]], dtype=np.uint16)
        func_labels = [np.array([[9, 9], [9, 9]], dtype=np.uint16)]

        result = show_centroid_match_qa_stage(
            plane_refs=plane_refs,
            anat_labels_all=anat_labels,
            func_labels=func_labels,
            use_suite2p_labels=True,
            render_ui=False,
            force_recompute=True,
        )

        labels, label_name, src_desc = result["helpers"]["load_func_labels_for_plane"](0)
        self.assertEqual(label_name, "plane0")
        self.assertEqual(src_desc, "Suite2p labels (plane_refs)")
        self.assertTrue(np.array_equal(labels, plane_refs[0]["suite2p"]["labels"]))

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
            input_path = preproc_rbest / "fish_rbest_channel2_gene.nrrd"
            input_path.write_bytes(b"")
            (out_masks / "fish_rbest_channel2_gene_cp_masks.tif").write_bytes(b"ready")
            result = run_hcr_cellpose_stage(
                fish_dir=fish_dir,
                preproc_dir=preproc_dir,
                nas_root=root,
                config=HcrCellposeConfig(skip_if_exists=True),
            )
            self.assertEqual(result["status"], "cached")
            self.assertIn("CP_MODEL_PATH", result["bindings"])
            self.assertTrue(any("skipping Cellpose import/model load" in line for line in result["log_lines"]))
            self.assertIn("python_version", result["runtime_provenance"])
            self.assertIsNone(result["runtime_provenance"]["model_sha256"])
            self.assertIsNone(result["runtime_provenance"]["cuda_available"])

    def test_run_anatomy_cellpose_stage_uses_output_root_for_cached_masks(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            analysis_dir = root / "analysis"
            output_root = analysis_dir / "structural" / "ex_vivo"
            output_root.mkdir(parents=True)
            source = root / "prepared_ex_vivo.tif"
            model = root / "model"
            mask_dir = output_root / "cp_masks"
            mask_dir.mkdir(parents=True)
            mask_path = mask_dir / "prepared_ex_vivo_8bit_cp_masks.tif"
            source_arr = np.zeros((2, 3, 4), dtype=np.uint8)
            tifffile.imwrite(source, source_arr)
            tifffile.imwrite(mask_path, np.zeros((2, 3, 4), dtype=np.uint16))
            model.write_bytes(b"model")

            result = run_anatomy_cellpose_stage(
                anat_seg_source_path=source,
                analysis_dir=analysis_dir,
                output_root=output_root,
                anat_cp_model_path=model,
                config=AnatomyCellposeConfig(skip_if_exists=True, boundary_enhancement=None),
            )

            self.assertEqual(result["status"], "cached")
            self.assertIsNone(result["boundary_enhancement"])
            self.assertEqual(result["bindings"]["ANAT_SEG_OUT_DIR"], output_root / "cp_masks")
            self.assertEqual(result["bindings"]["ANAT_SEG_CONVERT_DIR"], output_root / "raw" / "converted_nrrd_to_tif")
            self.assertEqual(
                result["runtime_provenance"]["model_sha256"],
                "9372c470eeadd5ecd9c3c74c2b3cb633f8e2f2fad799250a0f70d652b6b825e4",
            )
            self.assertIsNone(result["runtime_provenance"]["cuda_available"])

    def test_hessian_valley_tag_names_sigma_and_beta(self) -> None:
        self.assertEqual(hessian_valley_tag(1.5, 0.5), "hessian2d_s1p5_b0p50")
        self.assertEqual(hessian_valley_tag(1.5, 0.25), "hessian2d_s1p5_b0p25")

    def test_hessian_valley_subtract_darkens_gap_between_touching_nuclei(self) -> None:
        yy, xx = np.mgrid[0:40, 0:40]
        plane = 200 * np.exp(-((yy - 20) ** 2 + (xx - 15) ** 2) / 18.0) + 200 * np.exp(-((yy - 20) ** 2 + (xx - 25) ** 2) / 18.0)
        vol = np.stack([plane, plane]).astype(np.uint8)
        enhanced, scale = hessian_valley_subtract(vol, sigma_px=1.5, beta=0.5)
        self.assertEqual(enhanced.shape, vol.shape)
        self.assertEqual(enhanced.dtype, np.uint8)
        self.assertGreater(scale, 0)
        gap, centre = (0, 20, 20), (0, 20, 15)
        gap_drop = int(vol[gap]) - int(enhanced[gap])
        centre_drop = int(vol[centre]) - int(enhanced[centre])
        self.assertGreater(gap_drop, 0)
        self.assertGreater(gap_drop, centre_drop)
        self.assertGreater(int(enhanced[centre]) - int(enhanced[gap]), int(vol[centre]) - int(vol[gap]))
        plane_only, _ = hessian_valley_subtract(vol[0])
        np.testing.assert_array_equal(plane_only, enhanced[0])

    def _hessian_fixture(self, root: Path) -> tuple[Path, Path, Path, Path, np.ndarray]:
        analysis_dir = root / "analysis"
        output_root = analysis_dir / "structural" / "ex_vivo"
        (output_root / "cp_masks").mkdir(parents=True)
        source = root / "prepared_ex_vivo.tif"
        model = root / "model"
        model.write_bytes(b"model")
        source_arr = (np.arange(2 * 12 * 12).reshape(2, 12, 12) % 200).astype(np.uint8)
        tifffile.imwrite(source, source_arr)
        return analysis_dir, output_root, source, model, source_arr

    def _run_anatomy(self, analysis_dir, output_root, source, model, *, labels=1, fail=False, **cfg):
        class _Model:
            def eval(self, arr, **kwargs):
                if fail:
                    raise RuntimeError("simulated Cellpose failure")
                return np.full(arr.shape, labels, dtype=np.uint16)

        with patch("codeants_2pf_hcr.segmentation._load_cellpose_model", return_value=_Model()):
            return run_anatomy_cellpose_stage(
                anat_seg_source_path=source,
                analysis_dir=analysis_dir,
                output_root=output_root,
                anat_cp_model_path=model,
                config=AnatomyCellposeConfig(use_gpu=False, **cfg),
            )

    def test_hessian_stage_segments_binds_and_reuses_only_bound_masks(self) -> None:
        with TemporaryDirectory() as tmpdir:
            analysis_dir, output_root, source, model, source_arr = self._hessian_fixture(Path(tmpdir))
            tag = "hessian2d_s1p5_b0p50"
            mask = output_root / "cp_masks" / f"prepared_ex_vivo_{tag}_8bit_cp_masks.tif"
            input_path = output_root / "cellpose_input" / f"prepared_ex_vivo_{tag}.tif"

            first = self._run_anatomy(analysis_dir, output_root, source, model)
            self.assertEqual(first["status"], "segmented")
            self.assertEqual(first["bindings"]["ANAT_LABELS_PATH"], mask)
            expected, _ = hessian_valley_subtract(source_arr)
            np.testing.assert_array_equal(tifffile.imread(input_path), expected)
            binding = json.loads(anatomy_mask_binding_path(mask).read_text())
            self.assertEqual(binding["preprocessing"], {"method": "hessian2d", "sigma_px": 1.5, "beta": 0.5})

            second = self._run_anatomy(analysis_dir, output_root, source, model)
            self.assertEqual(second["status"], "cached")
            self.assertEqual(second["boundary_enhancement"]["tag"], tag)
            self.assertEqual(second["boundary_enhancement"]["cellpose_input_sha256"], first["boundary_enhancement"]["cellpose_input_sha256"])

            # Different parameters name a different mask; the bound one is foreign to them.
            with self.assertRaisesRegex(RuntimeError, "force_recompute"):
                self._run_anatomy(analysis_dir, output_root, source, model, hessian_beta=0.25)

    def test_hessian_stage_refuses_unbound_or_stale_masks_and_keeps_state_on_failure(self) -> None:
        with TemporaryDirectory() as tmpdir:
            analysis_dir, output_root, source, model, source_arr = self._hessian_fixture(Path(tmpdir))
            cp_masks = output_root / "cp_masks"
            input_dir = output_root / "cellpose_input"

            # A raw-input mask blocks segmentation and nothing is written.
            raw_mask = cp_masks / "prepared_ex_vivo_8bit_cp_masks.tif"
            tifffile.imwrite(raw_mask, np.zeros((2, 12, 12), dtype=np.uint16))
            with self.assertRaisesRegex(RuntimeError, "force_recompute"):
                self._run_anatomy(analysis_dir, output_root, source, model)
            self.assertFalse(input_dir.exists())
            raw_mask.unlink()

            # An exact-name mask without a binding record is not reused or overwritten.
            mask = cp_masks / "prepared_ex_vivo_hessian2d_s1p5_b0p50_8bit_cp_masks.tif"
            tifffile.imwrite(mask, np.full((2, 12, 12), 7, dtype=np.uint16))
            with self.assertRaisesRegex(RuntimeError, "no binding record"):
                self._run_anatomy(analysis_dir, output_root, source, model)
            with self.assertRaisesRegex(RuntimeError, "force_recompute"):
                self._run_anatomy(analysis_dir, output_root, source, model, skip_if_exists=False)
            self.assertEqual(int(tifffile.imread(mask).max()), 7)
            mask.unlink()

            self._run_anatomy(analysis_dir, output_root, source, model, labels=1)
            before = mask.read_bytes()
            # The source changed: the bound mask no longer matches and is kept.
            tifffile.imwrite(source, (source_arr // 2).astype(np.uint8))
            with self.assertRaisesRegex(RuntimeError, "different Cellpose input"):
                self._run_anatomy(analysis_dir, output_root, source, model)
            # A forced rerun that fails leaves the mask, binding and input untouched.
            input_before = next(input_dir.iterdir()).read_bytes()
            with self.assertRaisesRegex(RuntimeError, "simulated"):
                self._run_anatomy(analysis_dir, output_root, source, model, force_recompute=True, fail=True)
            self.assertEqual(mask.read_bytes(), before)
            self.assertEqual(next(input_dir.iterdir()).read_bytes(), input_before)
            with self.assertRaisesRegex(RuntimeError, "different Cellpose input"):
                self._run_anatomy(analysis_dir, output_root, source, model)

            # A successful forced rerun supersedes the old mask; one canonical mask remains.
            result = self._run_anatomy(analysis_dir, output_root, source, model, force_recompute=True, labels=2)
            self.assertEqual(result["status"], "segmented")
            self.assertEqual([p.name for p in cp_masks.glob("*_cp_masks.tif")], [mask.name])
            self.assertEqual(int(tifffile.imread(mask).max()), 2)
            superseded = list((output_root / "superseded_masks").rglob("*_cp_masks.tif"))
            self.assertEqual([p.read_bytes() for p in superseded], [before])
            self.assertEqual(self._run_anatomy(analysis_dir, output_root, source, model)["status"], "cached")

    def test_raw_mode_never_reuses_or_silently_replaces_hessian_masks(self) -> None:
        with TemporaryDirectory() as tmpdir:
            analysis_dir, output_root, source, model, _ = self._hessian_fixture(Path(tmpdir))
            self._run_anatomy(analysis_dir, output_root, source, model)
            with self.assertRaisesRegex(RuntimeError, "force_recompute"):
                self._run_anatomy(analysis_dir, output_root, source, model, boundary_enhancement=None)
            result = self._run_anatomy(analysis_dir, output_root, source, model, boundary_enhancement=None, force_recompute=True)
            self.assertEqual(result["status"], "segmented")
            self.assertIsNone(result["boundary_enhancement"])
            masks = sorted(p.name for p in (output_root / "cp_masks").glob("*_cp_masks.tif"))
            self.assertEqual(masks, ["prepared_ex_vivo_8bit_cp_masks.tif"])
            self.assertEqual(self._run_anatomy(analysis_dir, output_root, source, model, boundary_enhancement=None)["status"], "cached")

    def test_raw_mode_refuses_to_replace_stale_or_unreusable_masks_without_force(self) -> None:
        with TemporaryDirectory() as tmpdir:
            analysis_dir, output_root, source, model, source_arr = self._hessian_fixture(Path(tmpdir))
            raw = dict(boundary_enhancement=None)
            self._run_anatomy(analysis_dir, output_root, source, model, **raw)
            mask = output_root / "cp_masks" / "prepared_ex_vivo_8bit_cp_masks.tif"
            before = mask.read_bytes()
            with self.assertRaisesRegex(RuntimeError, "force_recompute"):
                self._run_anatomy(analysis_dir, output_root, source, model, skip_if_exists=False, **raw)
            tifffile.imwrite(source, (source_arr // 2).astype(np.uint8))
            with self.assertRaisesRegex(RuntimeError, "force_recompute"):
                self._run_anatomy(analysis_dir, output_root, source, model, **raw)
            self.assertEqual(mask.read_bytes(), before)
            # A legacy unbound raw mask of the right shape is still reused.
            anatomy_mask_binding_path(mask).unlink()
            self.assertEqual(self._run_anatomy(analysis_dir, output_root, source, model, **raw)["status"], "cached")

    def test_failed_publication_restores_previous_bundle_and_archives_are_unique(self) -> None:
        import os

        with TemporaryDirectory() as tmpdir:
            analysis_dir, output_root, source, model, _ = self._hessian_fixture(Path(tmpdir))
            self._run_anatomy(analysis_dir, output_root, source, model, labels=1)
            cp_masks = output_root / "cp_masks"
            mask = next(cp_masks.glob("*_cp_masks.tif"))
            snapshot = {p.relative_to(output_root): p.read_bytes() for p in output_root.rglob("*") if p.is_file() and "raw" not in p.parts}

            real_replace = os.replace
            calls = []

            def flaky_replace(src, dst):
                calls.append(dst)
                if len(calls) == 2:
                    raise OSError("simulated publication failure")
                return real_replace(src, dst)

            with patch("os.replace", side_effect=flaky_replace):
                with self.assertRaisesRegex(OSError, "simulated"):
                    self._run_anatomy(analysis_dir, output_root, source, model, force_recompute=True, labels=2)
            after = {p.relative_to(output_root): p.read_bytes() for p in output_root.rglob("*")
                     if p.is_file() and "raw" not in p.parts and "superseded_masks" not in p.parts}
            self.assertEqual(after, snapshot)
            self.assertEqual(self._run_anatomy(analysis_dir, output_root, source, model)["status"], "cached")
            self.assertFalse(list(output_root.glob(".staging_*")))

            # Two forced replacements keep two separate, complete archives.
            self._run_anatomy(analysis_dir, output_root, source, model, force_recompute=True, labels=2)
            self._run_anatomy(analysis_dir, output_root, source, model, force_recompute=True, labels=3)
            archived = sorted(int(tifffile.imread(p).max()) for p in (output_root / "superseded_masks").rglob("*_cp_masks.tif"))
            self.assertEqual(archived, [1, 2])
            self.assertEqual(int(tifffile.imread(mask).max()), 3)

    def test_external_labels_with_canonical_basename_are_protected(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            analysis_dir, output_root, source, model, _ = self._hessian_fixture(root)
            name = "prepared_ex_vivo_hessian2d_s1p5_b0p50_8bit_cp_masks.tif"
            external = root / "elsewhere" / name
            external.parent.mkdir()
            tifffile.imwrite(external, np.full((2, 12, 12), 7, dtype=np.uint16))

            def run(**cfg):
                with patch("codeants_2pf_hcr.segmentation._load_cellpose_model") as load:
                    load.return_value.eval.side_effect = lambda arr, **kw: np.full(arr.shape, 1, dtype=np.uint16)
                    return run_anatomy_cellpose_stage(
                        anat_seg_source_path=source, analysis_dir=analysis_dir, output_root=output_root,
                        anat_labels_path=external, anat_cp_model_path=model,
                        config=AnatomyCellposeConfig(use_gpu=False, **cfg),
                    )

            # Canonical output absent: the external same-name mask still blocks.
            with self.assertRaisesRegex(RuntimeError, "force_recompute"):
                run()
            self.assertFalse((output_root / "cp_masks" / name).exists())
            # Canonical output present and bound: the external one still blocks, never silently ignored.
            self._run_anatomy(analysis_dir, output_root, source, model)
            with self.assertRaisesRegex(RuntimeError, "force_recompute"):
                run()
            with self.assertRaisesRegex(RuntimeError, "force_recompute"):
                run(boundary_enhancement=None)
            self.assertEqual(int(tifffile.imread(external).max()), 7)
            # Passing the canonical mask itself is fine.
            with patch("codeants_2pf_hcr.segmentation._load_cellpose_model"):
                cached = run_anatomy_cellpose_stage(
                    anat_seg_source_path=source, analysis_dir=analysis_dir, output_root=output_root,
                    anat_labels_path=output_root / "cp_masks" / name, anat_cp_model_path=model,
                    config=AnatomyCellposeConfig(use_gpu=False),
                )
            self.assertEqual(cached["status"], "cached")

    def test_same_nrrd_header_treats_nan_fields_as_equal(self) -> None:
        from codeants_2pf_hcr.segmentation import _same_nrrd_header

        header = {"space directions": np.array([[np.nan] * 3, [0.5, 0, 0], [0, 0.5, 0]]), "type": "uint8"}
        self.assertTrue(_same_nrrd_header(header, {k: (v.copy() if hasattr(v, "copy") else v) for k, v in header.items()}))
        self.assertFalse(_same_nrrd_header(header, {**header, "type": "uint16"}))

    def test_cellpose_input_nrrd_keeps_reader_axis_order_for_cubes_and_stacks(self) -> None:
        import nrrd

        from codeants_2pf_hcr.segmentation import _prepare_anat_volume, _write_cellpose_input

        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            for name, shape in (("cube", (5, 5, 5)), ("stack", (8, 7, 3))):
                source = root / f"{name}.nrrd"
                data = np.arange(np.prod(shape), dtype=np.uint8).reshape(shape)
                nrrd.write(str(source), data, {"spacings": [0.5, 0.5, 2.0]})
                vol = _prepare_anat_volume(source, log_lines=[])
                out = root / f"{name}_out.nrrd"
                _write_cellpose_input(source, vol, out, log_lines=[])
                written, header = nrrd.read(str(out))
                np.testing.assert_array_equal(written, data)
                np.testing.assert_array_equal(header["spacings"], [0.5, 0.5, 2.0])
                np.testing.assert_array_equal(_prepare_anat_volume(out, log_lines=[]), vol)

if __name__ == "__main__":
    unittest.main()
