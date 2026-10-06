import json
from pathlib import Path
import pytest
from codeants_2pf_hcr.native_paths import native_product_path
from codeants_2pf_hcr.native_run import promote_files, rollback_promotion, _rebind


def test_routes_preserve_native_consumers(tmp_path):
    fish = tmp_path / "L000_f00"
    assert native_product_path(fish, "register-functional-to-anatomy", "plane_refs_summary.json") == fish / "03_analysis/functional/registration/plane_refs_summary.json"
    assert native_product_path(fish, "make-motion-auc-tables", "registration/motion_auc_plot_points.csv") == fish / "03_analysis/functional/derived/motion_auc_plot_points.csv"
    assert native_product_path(fish, "make-figures", "04_plots/example.png") == fish / "04_plots/example.png"


def test_rebind_exact_products_and_keep_history(tmp_path):
    fish = tmp_path / "L000_f00"; root = "/source/run"
    value = root + "/register-functional-to-anatomy/ncc/transforms/a.mat"
    assert _rebind(value, [root], fish) == str(fish / "03_analysis/functional/ncc/transforms/a.mat")
    value = root + "/match-roi-to-anatomy/registration/functional_roi_anatomy_matches.csv"
    assert _rebind(value, [root], fish) == str(fish / "03_analysis/functional/registration/functional_roi_anatomy_matches.csv")


def test_exact_backup_and_crash_rollback(tmp_path):
    fish = tmp_path / "L000_f00"; target = fish / "03_analysis/functional/registration/a.csv"
    target.parent.mkdir(parents=True); target.write_text("old")
    source = tmp_path / "new.csv";source.write_text("new")
    journal = tmp_path / "journal"
    promote_files(fish, {source: target}, journal)
    assert target.read_text() == "new"
    assert (journal / "original-0").read_text() == "old"
    rollback_promotion(journal)
    assert target.read_text() == "old"
    rollback_promotion(journal)
    assert target.read_text() == "old"


def test_late_copy_failure_rolls_back_first_product(tmp_path, monkeypatch):
    import codeants_2pf_hcr.native_run as module
    fish = tmp_path / "L000_f00"; folder = fish / "03_analysis/functional/registration";folder.mkdir(parents=True)
    a = folder / "a.csv";a.write_text("old-a");b = folder / "b.csv";b.write_text("old-b")
    source = tmp_path / "source.csv";source.write_text("new")
    original = module.shutil.copy2
    def fail(source_path, target_path):
        if str(target_path).endswith("original-1"): raise OSError("injected backup failure")
        return original(source_path,target_path)
    monkeypatch.setattr(module.shutil,"copy2",fail)
    with pytest.raises(OSError,match="injected"):
        promote_files(fish,{source:a, source.with_name("missing.csv"):b},tmp_path / "journal")
    assert a.read_text() == "old-a" and b.read_text() == "old-b"


def test_unknown_product_fails_without_native_overwrite(tmp_path):
    fish = tmp_path / "L000_f00";source = tmp_path / "new.csv";source.write_text("new")
    with pytest.raises(ValueError,match="ownership"):
        promote_files(fish,{source:fish / "01_raw/unsafe.csv"},tmp_path / "journal")


@pytest.mark.parametrize("prior_manifest", [True, False])
def test_crash_after_publication_restores_manifest_and_products(tmp_path, prior_manifest):
    fish = tmp_path / "L000_f00"; target = fish / "03_analysis/functional/registration/a.csv"
    target.parent.mkdir(parents=True);target.write_text("old")
    manifest = tmp_path / "manifest.json"
    if prior_manifest: manifest.write_text(json.dumps({"old": True}))
    source = tmp_path / "new.csv";source.write_text("new");journal = tmp_path / "journal"
    promote_files(fish,{source:target},journal,manifest_path=manifest)
    saved = json.loads((journal / "previous_manifest.json").read_text())
    assert saved["payload"] == ({"old": True} if prior_manifest else None)
    manifest.write_text(json.dumps({"new": True}))
    rollback_promotion(journal)
    assert target.read_text() == "old"
    if prior_manifest: assert json.loads(manifest.read_text()) == {"old": True}
    else: assert not manifest.exists()
