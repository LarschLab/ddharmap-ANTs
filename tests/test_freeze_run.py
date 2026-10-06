from __future__ import annotations

import json
from pathlib import Path
import stat
import os
import subprocess
import sys

import pytest

from codeants_2pf_hcr.freeze_run import build_inventory, freeze_single_fish_run, verify_read_only_mount, _remove_staging_tree



def _write_manifest(path: Path, *, pipeline_file: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "stage_name": "example-stage",
                "fish_id": "L765_f04",
                "generated_at": "2026-09-22T00:00:00+00:00",
                "status": "warn",
                "parameters": {"seed": 0},
                "outputs": [{"path": str(pipeline_file), "exists": True}],
                "checks": [{"label": "example", "status": "warn"}],
                "errors": [],
                "warnings": ["review"],
            }
        ),
        encoding="utf-8",
    )


def test_freeze_single_fish_run_copies_verifies_and_locks(tmp_path: Path) -> None:
    pipeline_root = tmp_path / "working" / "L765_f04"
    output = pipeline_root / "score" / "result.csv"
    output.parent.mkdir(parents=True)
    output.write_text("roi,value\n1,2\n", encoding="utf-8")
    manifest_dir = tmp_path / "actual" / "pipeline_manifests"
    _write_manifest(manifest_dir / "example-stage_manifest.json", pipeline_file=output)
    repo_root = Path(__file__).resolve().parents[1]
    destination = tmp_path / "frozen" / "L765_f04-freeze"

    source_before = build_inventory([("L765_f04", pipeline_root), ("pipeline_manifests", manifest_dir)])
    metadata = freeze_single_fish_run(
        fish_id="L765_f04",
        pipeline_root=pipeline_root,
        manifest_dir=manifest_dir,
        destination=destination,
        repo_root=repo_root,
    )
    source_after = build_inventory([("L765_f04", pipeline_root), ("pipeline_manifests", manifest_dir)])

    assert source_before == source_after
    assert output.read_text(encoding="utf-8") == "roi,value\n1,2\n"
    assert (destination / "snapshot" / "L765_f04" / "score" / "result.csv").is_file()
    assert (destination / "snapshot" / "pipeline_manifests" / "example-stage_manifest.json").is_file()
    assert metadata["verification"]["status"] == "pass"
    assert metadata["verification"]["record_count"] == 2
    assert (destination / "sha256_inventory.tsv").is_file()
    stage_status = json.loads(
        (destination / "provenance" / "stage_status_compact.json").read_text(encoding="utf-8")
    )
    assert stage_status[0]["stage_name"] == "example-stage"
    assert stage_status[0]["check_counts"] == {"fail": 0, "pass": 0, "warn": 1}
    for path in [destination, *destination.rglob("*")]:
        if not path.is_symlink():
            assert stat.S_IMODE(path.stat().st_mode) & 0o222 == 0


def test_freeze_refuses_existing_destination(tmp_path: Path) -> None:
    pipeline_root = tmp_path / "L765_f04"
    pipeline_root.mkdir()
    manifest_dir = tmp_path / "pipeline_manifests"
    manifest_dir.mkdir()
    destination = tmp_path / "existing"
    destination.mkdir()
    with pytest.raises(FileExistsError, match="already exists"):
        freeze_single_fish_run(
            fish_id="L765_f04",
            pipeline_root=pipeline_root,
            manifest_dir=manifest_dir,
            destination=destination,
            repo_root=Path(__file__).resolve().parents[1],
        )


@pytest.mark.skipif(sys.platform != "darwin" or not os.environ.get("FREEZE_IMAGE_TEST_ROOT"),
                    reason="Requires explicit macOS mounted-volume test directory")
def test_read_only_image_on_mounted_volume() -> None:
    import tempfile

    test_root = Path(tempfile.mkdtemp(prefix="freeze-image-test-", dir=os.environ["FREEZE_IMAGE_TEST_ROOT"]))
    mounted = None
    try:
        pipeline_root = test_root / "working" / "L765_f04"
        output = pipeline_root / "nested" / "deeper" / "result.csv"
        output.parent.mkdir(parents=True)
        output.write_text("roi,value\n1,2\n", encoding="utf-8")
        subprocess.run(["xattr", "-wx", "com.apple.FinderInfo", "01" + "00" * 31, str(output)], check=True)
        manifests = test_root / "manifests"
        _write_manifest(manifests / "example_manifest.json", pipeline_file=output)
        before = build_inventory([("L765_f04", pipeline_root), ("pipeline_manifests", manifests)])
        destination = test_root / f"freeze-{test_root.name}"
        result = freeze_single_fish_run(
            fish_id="L765_f04", pipeline_root=pipeline_root, manifest_dir=manifests,
            destination=destination, repo_root=Path(__file__).resolve().parents[1],
            scientific_commit="dfbc3b8", protection="read-only-image",
        )
        mounted = Path(result["lock_verification"]["mountpoint"])
        assert result["lock_verification"]["write_probe_errno"] == 30
        assert verify_read_only_mount(mounted)["filesystem_writable"] is False
        assert (mounted / "snapshot" / "L765_f04" / "nested" / "deeper" / "result.csv").read_text() == output.read_text()
        with pytest.raises(OSError, match="Read-only file system"):
            (mounted / "snapshot" / "L765_f04" / "nested" / "deeper" / "result.csv").write_text("overwrite")
        assert before == build_inventory([("L765_f04", pipeline_root), ("pipeline_manifests", manifests)])
        scientific_git = json.loads((mounted / "provenance" / "scientific_git.json").read_text())
        assert scientific_git["commit"].startswith("dfbc3b8")
    finally:
        if mounted is not None:
            subprocess.run(["hdiutil", "detach", str(mounted)], check=True, capture_output=True)
        _remove_staging_tree(test_root)
