import unittest
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory

import numpy as np
import tifffile

from codeants_2pf_hcr.context import (
    ContextStageConfig,
    FunctionalOrientationStageConfig,
    build_registration_helper_stage,
    default_cellpose_model_root,
    build_fish_state_audit_df,
    infer_anatomy_stack_path,
    infer_hcr_label_paths,
    notebook_bindings_from_context,
    normalize_run_config,
    prepare_notebook_paths,
    resolve_fish_context,
    resolve_notebook_context_stage,
    resolve_voxel_context_stage,
    orient_functional_stacks_stage,
)


class ContextTests(unittest.TestCase):
    def test_normalize_run_config_forces_expected_flags(self) -> None:
        cfg = normalize_run_config({"HIGH_CONF_ONLY": True, "RECOMPUTE_WARP": False})
        self.assertTrue(cfg["HIGH_CONF_ONLY"])
        self.assertTrue(cfg["RECOMPUTE_WARP"])
        self.assertTrue(cfg["EXPORT_BEST_ROUND_LABELS_TO_RBEST"])

    def test_build_fish_state_audit_df_flags_stale_paths(self) -> None:
        audit_df = build_fish_state_audit_df(
            fish_id="L395_f11",
            state_fish_id="L395_f11",
            canonical_checks={"FISH_DIR": "/tmp/L396_f04"},
        )
        self.assertEqual(set(audit_df["status"]), {"fail", "ok"})

    def test_build_fish_state_audit_df_marks_uninitialized_expected_path_as_pending(self) -> None:
        audit_df = build_fish_state_audit_df(
            fish_id="L395_f11",
            state_fish_id="L395_f11",
            canonical_checks={"FISH_DIR": "/tmp/L395_f11"},
            expected_paths={"ANAT_SEG_OUT_DIR": {"expected": "/tmp/L395_f11/out", "current": None}},
        )
        pending_row = audit_df.loc[audit_df["key"] == "ANAT_SEG_OUT_DIR"].iloc[0]
        self.assertEqual(pending_row["status"], "pending")
        self.assertIn("not initialized yet", pending_row["detail"])

    def test_prepare_notebook_paths_discovers_unique_func_labels_path(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fish_id = "L395_f11"
            fish_dir = root / fish_id
            (fish_dir / "03_analysis").mkdir(parents=True)
            (fish_dir / "03_analysis" / "functional" / "segmentation").mkdir(parents=True)
            (fish_dir / "03_analysis" / "functional" / "segmentation" / f"{fish_id}_functional_labels.tif").touch()
            (root / "matchingMetadata.csv").write_text("fish_id,polarity\nL395_f11,south\n")
            ctx = resolve_fish_context(fish_id=fish_id, data_mode="local", local_root=root)

            paths = prepare_notebook_paths(ctx)

            self.assertEqual(
                paths["FUNC_LABELS_PATH"],
                fish_dir / "03_analysis" / "functional" / "segmentation" / f"{fish_id}_functional_labels.tif",
            )

    def test_infer_anatomy_stack_path_prefers_raw_source_over_derived_uint8(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fish_id = "L395_f11"
            fish_dir = root / fish_id
            raw_dir = fish_dir / "01_raw" / "2p" / "anatomy"
            preproc_dir = fish_dir / "02_reg" / "00_preprocessing" / "2p_anatomy"
            raw_dir.mkdir(parents=True)
            preproc_dir.mkdir(parents=True)
            raw_path = raw_dir / f"{fish_id}_anatomy_00001.tif"
            derived_path = preproc_dir / f"{fish_id}_anatomy_2P_GCaMP_uint8.tif"
            raw_path.touch()
            derived_path.touch()

            self.assertEqual(infer_anatomy_stack_path(fish_dir, fish_id), raw_path)

    def test_infer_anatomy_stack_path_prefers_nonderived_preprocessed_source_over_uint8(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fish_id = "L395_f11"
            fish_dir = root / fish_id
            preproc_dir = fish_dir / "02_reg" / "00_preprocessing" / "2p_anatomy"
            preproc_dir.mkdir(parents=True)
            source_path = preproc_dir / f"{fish_id}_anatomy_2P_GCaMP.nrrd"
            derived_path = preproc_dir / f"{fish_id}_anatomy_2P_GCaMP_uint8.tif"
            source_path.touch()
            derived_path.touch()

            self.assertEqual(infer_anatomy_stack_path(fish_dir, fish_id), source_path)

    def test_infer_anatomy_stack_path_falls_back_to_uint8_when_no_source_exists(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fish_id = "L395_f11"
            fish_dir = root / fish_id
            preproc_dir = fish_dir / "02_reg" / "00_preprocessing" / "2p_anatomy"
            preproc_dir.mkdir(parents=True)
            derived_path = preproc_dir / f"{fish_id}_anatomy_2P_GCaMP_uint8.tif"
            derived_path.touch()

            self.assertEqual(infer_anatomy_stack_path(fish_dir, fish_id), derived_path)

    def test_notebook_bindings_from_context_exposes_legacy_bindings(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fish_id = "L395_f11"
            (root / fish_id / "03_analysis").mkdir(parents=True)
            (root / "matchingMetadata.csv").write_text("fish_id,polarity\nL395_f11,south\n")
            ctx = resolve_fish_context(fish_id=fish_id, owner="Matilde", data_mode="local", local_root=root)

            bindings = notebook_bindings_from_context(ctx)

            self.assertEqual(bindings["FISH_ID"], fish_id)
            self.assertEqual(bindings["OWNER"], "Matilde")
            self.assertEqual(bindings["DATA_MODE"], "local")
            self.assertEqual(bindings["RUN_CONFIG"], dict(ctx.run_config))

    def test_default_cellpose_model_root_prefers_lowercase_local_directory(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "cellpose" / "models").mkdir(parents=True)

            self.assertEqual(
                default_cellpose_model_root(root, "local", root / "nas"),
                root / "cellpose" / "models",
            )

    def test_resolve_notebook_context_stage_accepts_local_root_override_from_config(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fish_id = "L395_f11"
            (root / fish_id / "03_analysis").mkdir(parents=True)
            (root / "matchingMetadata.csv").write_text("fish_id,polarity\nL395_f11,south\n")

            result = resolve_notebook_context_stage(
                ContextStageConfig(
                    fish_id=fish_id,
                    data_mode="local",
                    local_root_override=root,
                )
            )

            self.assertEqual(result["ctx"].local_root, root)
            self.assertEqual(result["bindings"]["DATA_ROOT"], root)

    def test_resolve_fish_context_nas_mode_does_not_require_local_root(self) -> None:
        with TemporaryDirectory() as tmpdir:
            nas_root = Path(tmpdir)
            fish_id = "L395_f11"
            fish_dir = nas_root / "Matilde" / "Microscopy" / fish_id
            (fish_dir / "03_analysis").mkdir(parents=True)

            ctx = resolve_fish_context(
                fish_id=fish_id,
                owner="Matilde",
                data_mode="nas",
                nas_root=nas_root,
            )

            self.assertIsNone(ctx.local_root)
            self.assertEqual(ctx.data_root, nas_root)
            self.assertEqual(ctx.fish_dir, fish_dir)

    def test_infer_hcr_label_paths_prefers_canonical_raw_and_aligned_labels(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fish_id = "L395_f11"
            conf_root = root / fish_id / "03_analysis" / "confocal"
            raw_cp = conf_root / "raw" / "cp_masks"
            aligned = conf_root / "aligned"
            raw_cp.mkdir(parents=True)
            aligned.mkdir(parents=True)

            raw_good = raw_cp / f"{fish_id}_round1_channel2_sst1_1_cp_masks.tif"
            raw_good.touch()
            raw_noise = raw_cp / f"{fish_id}_round1_channel2_sst1_1_cp_masks.png"
            raw_noise.touch()
            aligned_good = aligned / f"{fish_id}_round1_channel2_sst1_1_cp_masks_in_2p_labels_uint16.tif"
            aligned_good.touch()
            aligned_noise_csv = aligned / f"{fish_id}_round1_channel2_sst1_1_cp_masks_in_2p_matches.csv"
            aligned_noise_csv.touch()
            aligned_noise_within = aligned / f"{fish_id}_round1_channel2_sst1_1_cp_masks_in_2p_conf_within_labels.tif"
            aligned_noise_within.touch()

            hits = infer_hcr_label_paths(root / fish_id, fish_id)

            self.assertEqual(hits[0], raw_good)
            self.assertIn(aligned_good, hits)
            self.assertNotIn(raw_noise, hits)
            self.assertNotIn(aligned_noise_csv, hits)
            self.assertNotIn(aligned_noise_within, hits)

    def test_build_registration_helper_stage_restores_orientation_qc_helpers(self) -> None:
        helpers = build_registration_helper_stage(polarity="north")

        self.assertEqual(
            set(helpers),
            {
                "_apply_func_orientation",
                "_apply_func_orient",
                "_ensure_float32",
                "_resize_like",
                "_corr2",
            },
        )

        ref_img = np.array([[1, 2], [3, 4]], dtype=np.uint16)
        mean_img = np.array([[4, 3], [2, 1]], dtype=np.float64)

        ref_float = helpers["_ensure_float32"](ref_img)
        self.assertEqual(ref_float.dtype.kind, "f")
        self.assertEqual(str(ref_float.dtype), "float32")

        oriented = helpers["_apply_func_orient"](mean_img)
        expected = np.array([[2, 1], [4, 3]], dtype=np.float32)
        np.testing.assert_array_equal(oriented, expected)

        resized = helpers["_resize_like"](mean_img, ref_img.shape)
        self.assertEqual(resized.shape, ref_img.shape)
        self.assertEqual(str(resized.dtype), "float32")

        corr = helpers["_corr2"](oriented, ref_float)
        self.assertTrue(np.isfinite(corr))

    def test_orient_functional_stacks_stage_defaults_to_audit_without_writing_movie(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            src = root / "fish_mcorrected.tif"
            dst = root / "raw" / "fish_mcorrected_flipX.tif"
            dst.parent.mkdir()
            tifffile.imwrite(src, np.arange(8, dtype=np.uint16).reshape(2, 2, 2))

            result = orient_functional_stacks_stage(
                func_nonflipped_list=[src],
                flipped_list=[dst],
                out_raw=dst.parent,
                fish_id="fish",
                polarity="south",
                polarity_source="test",
                config=FunctionalOrientationStageConfig(),
            )

            self.assertFalse(dst.exists())
            self.assertIn("FUNCTIONAL_ORIENTATION_AUDIT_DF", result["bindings"])
            audit_df = result["audit_df"]
            self.assertEqual(audit_df.loc[0, "status"], "no_cache")
            self.assertTrue(audit_df.loc[0, "source_exists"])

    def test_orient_functional_stacks_stage_opt_in_writes_oriented_movie(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            src = root / "fish_mcorrected.tif"
            dst = root / "raw" / "fish_mcorrected_flipX.tif"
            dst.parent.mkdir()
            arr = np.array([[[1, 2], [3, 4]]], dtype=np.uint16)
            tifffile.imwrite(src, arr)

            orient_functional_stacks_stage(
                func_nonflipped_list=[src],
                flipped_list=[dst],
                out_raw=dst.parent,
                fish_id="fish",
                polarity="south",
                polarity_source="test",
                config=FunctionalOrientationStageConfig(save_oriented_stacks=True),
            )

            self.assertTrue(dst.exists())
            np.testing.assert_array_equal(tifffile.imread(dst), arr[..., ::-1])

    def test_resolve_voxel_context_stage_uses_source_paths_and_legacy_flip_aliases(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            analysis_dir = root / "analysis"
            out_reg = root / "reg"
            outdir = root / "out"
            analysis_dir.mkdir()
            out_reg.mkdir()
            outdir.mkdir()
            src = root / "fish_mcorrected.tif"
            flipped = root / "raw" / "fish_mcorrected_flipX.tif"
            flipped.parent.mkdir()
            tifffile.imwrite(src, np.zeros((1, 2, 2), dtype=np.uint16))

            result = resolve_voxel_context_stage(
                analysis_dir=analysis_dir,
                outdir=outdir,
                out_reg=out_reg,
                data_mode="local",
                func_stack_path=None,
                func_raw_stack_path=None,
                anat_stack_path=None,
                hcr_stack_paths=None,
                hcr_stack_path=None,
                vox_func_auto=None,
                vox_func_manual={"X": 1.0, "Y": 2.0, "Z": 3.0},
                vox_anat_manual=None,
                vox_hcr_manual=None,
                flipped_list=[flipped],
                func_source_list=[src],
            )

            vox_by_path = result["bindings"]["VOX_FUNC_BY_PATH"]
            self.assertEqual(vox_by_path[str(src)]["X"], 1.0)
            self.assertEqual(vox_by_path[str(flipped)]["Y"], 2.0)
            self.assertEqual(result["df_vox"].loc[0, "path"], str(src))

    def test_package_import_preserves_preconfigured_matplotlib_backend(self) -> None:
        env = dict(os.environ)
        env["MPLBACKEND"] = "svg"
        result = subprocess.run(
            [sys.executable, "-c", "import matplotlib; import codeants_2pf_hcr; print(matplotlib.get_backend())"],
            capture_output=True,
            check=True,
            text=True,
            env=env,
        )
        self.assertEqual(result.stdout.strip().lower(), "svg")


if __name__ == "__main__":
    unittest.main()
