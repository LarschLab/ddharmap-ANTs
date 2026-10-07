import base64
import io
import json
from pathlib import Path

from PIL import Image
import pytest

from codeants_2pf_hcr.qc_figure_export import export_qc_figure_gallery


def test_explicit_preliminary_gallery_names_other_fish_and_preserves_gate(tmp_path):
    notebooks, manifests, output, fish = _fixture(tmp_path)
    (notebooks / "06_qc.ipynb").unlink()
    other = tmp_path / "L765_f02"
    fish.rename(other)
    manifests = other / "pipeline_manifests"
    manifest = manifests / "make-figures_manifest.json"
    payload = json.loads(manifest.read_text())
    payload["outputs"][0]["path"] = str(other / "writer.png")
    manifest.write_text(json.dumps(payload))
    gate = {"status": "review_required", "reason": "Midline unreviewed"}
    export_qc_figure_gallery(notebook_dir=notebooks, manifest_dir=manifests, output_dir=output,
                            fish_dir=other, review_gate=gate,
                            manifest_paths=tuple(manifests.glob("*_manifest.json")))
    report = json.loads((output / "gallery_manifest.json").read_text())
    assert report["fish_id"] == "L765_f02" and report["scientific_release"] == gate
    assert len(report["notebook_coverage"]) == 6
    page = (output / "index.html").read_text()
    assert "L765_f02" in page and "L765_f04" not in page
    assert "midline-review QC figure gallery" in page


def _fixture(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    notebooks = tmp_path / "executed"
    notebooks.mkdir()
    fish = tmp_path / "L765_f04"
    fish.mkdir()
    manifests = fish / "pipeline_manifests"
    manifests.mkdir()
    png = Image.new("RGB", (200, 200), "white")
    png.paste("blue", (0, 0, 100, 100))
    buffer = io.BytesIO()
    png.save(buffer, format="PNG")
    encoded = base64.b64encode(buffer.getvalue()).decode()
    for index in range(7):
        cells = [{"cell_type": "markdown", "source": [f"# QC {index}"]},
                 {"cell_type": "code", "source": ["display(figure)"], "execution_count": 1,
                  "outputs": [] if index == 0 else [{"output_type": "display_data", "data": {"image/png": encoded}}]}]
        (notebooks / f"{index:02}_qc.ipynb").write_text(json.dumps({"cells": cells}))
    image = fish / "writer.png"
    image.write_bytes(buffer.getvalue())
    (manifests / "make-figures_manifest.json").write_text(json.dumps({"stage_name": "make-figures", "outputs": [{"path": str(image)}]}))
    return notebooks, manifests, tmp_path / "gallery", fish


def test_gallery_deduplicates_without_losing_sources(tmp_path: Path) -> None:
    notebooks, manifests, output, fish = _fixture(tmp_path)
    before = {path: path.read_bytes() for root in (notebooks, manifests) for path in root.glob("*")}
    report = export_qc_figure_gallery(notebook_dir=notebooks, manifest_dir=manifests, output_dir=output, fish_dir=fish)
    assert report["figure_count"] == 1
    assert report["notebook_png_outputs"] == 6
    assert report["writer_png_references"] == 1
    manifest = json.loads((output / "gallery_manifest.json").read_text())
    assert len(manifest["figures"][0]["sources"]) == 7
    assert manifest["notebook_coverage"][0]["png_outputs"] == 0
    assert len(manifest["notebook_coverage"]) == 7
    assert (output / manifest["figures"][0]["file"]).read_bytes() == (fish / "writer.png").read_bytes()
    assert before == {path: path.read_bytes() for path in before}


def test_gallery_refuses_unexecuted_qc_cells(tmp_path: Path) -> None:
    notebooks, manifests, output, fish = _fixture(tmp_path)
    path = notebooks / "06_qc.ipynb"
    nb = json.loads(path.read_text())
    nb["cells"][1]["execution_count"] = None
    path.write_text(json.dumps(nb))
    with pytest.raises(ValueError, match="Unexecuted"):
        export_qc_figure_gallery(notebook_dir=notebooks, manifest_dir=manifests, output_dir=output, fish_dir=fish)
    assert not output.exists()


def test_gallery_refuses_stale_external_writer_png(tmp_path: Path) -> None:
    notebooks, manifests, output, fish = _fixture(tmp_path)
    external = tmp_path / "old.png"
    external.write_bytes((fish / "writer.png").read_bytes())
    (manifests / "make-figures_manifest.json").write_text(json.dumps({"outputs": [{"path": str(external)}]}))
    with pytest.raises(ValueError, match="outside actual fish folder"):
        export_qc_figure_gallery(notebook_dir=notebooks, manifest_dir=manifests, output_dir=output, fish_dir=fish)
    assert not output.exists()
