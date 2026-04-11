import tempfile
from pathlib import Path

from codeants_2pf_hcr import organize


def test_organize_moves_registration_files_to_stable_layout() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        analysis_dir = Path(tmpdir) / "03_analysis"
        analysis_dir.mkdir(parents=True, exist_ok=True)
        source = analysis_dir / "functional_roi_activity_identity.csv"
        source.write_text("a,b\n1,2\n")

        result = organize(analysis_dir, apply=True, move_unknown=False, verbose=False)

        expected = analysis_dir / "functional" / "registration" / "functional_roi_activity_identity.csv"
        assert expected.exists()
        assert not source.exists()
        assert result["n_moved"] == 1


def test_organize_skips_when_destination_exists() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        analysis_dir = Path(tmpdir) / "03_analysis"
        dst = analysis_dir / "functional" / "registration" / "hcr_activity_status.csv"
        src = analysis_dir / "hcr_activity_status.csv"
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_text("dst\n")
        src.parent.mkdir(parents=True, exist_ok=True)
        src.write_text("src\n")

        result = organize(analysis_dir, apply=True, move_unknown=False, verbose=False)

        assert src.exists()
        assert dst.read_text() == "dst\n"
        assert result["n_skipped"] == 1
