import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from codeants_2pf_hcr.context import (
    build_fish_state_audit_df,
    infer_hcr_label_paths,
    notebook_bindings_from_context,
    normalize_run_config,
    prepare_notebook_paths,
    resolve_fish_context,
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


if __name__ == "__main__":
    unittest.main()
