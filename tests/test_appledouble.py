"""Regression coverage for exFAT macOS metadata in operational scans."""
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from codeants_2pf_hcr.paths import (
    is_appledouble, is_visible_path, visible_glob, visible_iterdir, visible_rglob,
)
from codeants_2pf_hcr.single_fish_job import _output_metadata, _gallery
from codeants_2pf_hcr.single_fish_run import describe_path
from codeants_2pf_hcr.freeze_run import build_inventory
from test_qc_figure_export import _fixture


APPLEDOUBLE = bytes.fromhex("00 05 16 07 00 02 00 00") + b"\xff\x00provenance"


def _sidecar(real: Path) -> Path:
    path = real.with_name("._" + real.name)
    path.write_bytes(APPLEDOUBLE)
    return path


@pytest.mark.parametrize("scan", [visible_iterdir, lambda root: visible_glob(root, "*"),
                                  lambda root: visible_rglob(root, "*")])
def test_visible_scans_skip_metadata_only(tmp_path, scan):
    real = tmp_path / "real.json"
    real.write_text("{}")
    hidden = tmp_path / ".retained"
    hidden.write_text("real hidden input")
    sidecar = _sidecar(real)
    (tmp_path / ".DS_Store").write_bytes(APPLEDOUBLE)
    assert set(scan(tmp_path)) == {real, hidden}
    assert is_appledouble(sidecar)
    assert not is_appledouble(tmp_path / ".DS_Store")
    assert not is_visible_path(sidecar)
    assert is_visible_path(hidden)


def test_recursive_scans_and_product_hash_ignore_nested_metadata(tmp_path):
    nested = tmp_path / "products"
    nested.mkdir()
    real = nested / "result.csv"
    real.write_text("real bytes")
    before = describe_path(tmp_path)
    _sidecar(real)
    (nested / ".DS_Store").write_bytes(APPLEDOUBLE)
    metadata_dir = nested / "._directory"
    metadata_dir.mkdir()
    (metadata_dir / "fake.json").write_bytes(APPLEDOUBLE)
    assert list(visible_glob(tmp_path, "**/*.csv")) == [real]
    assert set(visible_rglob(tmp_path, "*")) == {nested, real}
    assert describe_path(tmp_path) == before


def test_output_metadata_skips_binary_stage_json_and_promoted_sidecars(tmp_path):
    state = tmp_path / "state/stages"
    state.mkdir(parents=True)
    outputs = tmp_path / "products"
    outputs.mkdir()
    real = outputs / "result.json"
    real.write_text("{}")
    promoted = tmp_path / "promoted.csv"
    promoted.write_text("real bytes")
    stage = state / "x.json"
    stage.write_text(json.dumps({"promoted_products": [{"path": str(promoted)}]}))
    spec = SimpleNamespace(state_root=state.parent, data_root=tmp_path, fish_id="fish",
                           stages=[SimpleNamespace(outputs=[str(outputs)])])
    before = _output_metadata(spec)
    _sidecar(stage)
    _sidecar(real)
    (outputs / ".DS_Store").write_bytes(APPLEDOUBLE)
    assert _output_metadata(spec) == before
    assert set(before) == {str(real), str(promoted)}


def test_qc05_actual_mask_discovery_line_skips_tiff_sidecars(tmp_path):
    real = tmp_path / "fish_r1_cp_masks_in_2p_labels_uint16.tif"
    real.write_bytes(b"real TIFF placeholder: discovery must not decode")
    _sidecar(real)
    (tmp_path / ".DS_Store").write_bytes(APPLEDOUBLE)
    notebook = Path(__file__).resolve().parents[1] / "notebooks/qc/05_molecular_identity_qc.ipynb"
    cells = json.loads(notebook.read_text())["cells"]
    line = next(line for cell in cells if cell["cell_type"] == "code"
                for line in cell["source"] if line.startswith("HCR_LABEL_MASKS ="))
    namespace = {"HCR_ANATOMY_DIR": tmp_path, "visible_glob": visible_glob}
    exec(line, namespace)
    assert namespace["HCR_LABEL_MASKS"] == [real]


def test_gallery_discovers_notebooks_manifests_pngs_and_reuses_inventory(tmp_path):
    notebooks, manifests, output, fish = _fixture(tmp_path)
    for path in [*notebooks.iterdir(), *manifests.iterdir()]:
        _sidecar(path)
    writer = fish / "writer.png"
    _sidecar(writer)
    # Exercise recursive manifest output discovery, not only an explicit PNG.
    images = fish / "writer_products"
    images.mkdir()
    (images / "result.png").write_bytes(writer.read_bytes())
    _sidecar(images / "result.png")
    (manifests / "make-figures_manifest.json").write_text(json.dumps(
        {"stage_name": "make-figures", "outputs": [{"path": str(images)}]}))
    args = dict(notebook_dir=notebooks, manifest_dir=manifests, output_dir=output, fish_dir=fish)
    first = _gallery(**args)
    assert first["writer_png_references"] == 1
    assert first["notebook_png_outputs"] == 6
    for path in list(output.rglob("*")):
        if path.is_file():
            _sidecar(path)
    (output / ".DS_Store").write_bytes(APPLEDOUBLE)
    assert _gallery(**args) == first
    inventory = json.loads((output / "job_inventory.json").read_text())["sha256"]
    assert all(is_visible_path(name) for name in inventory)


def test_freeze_literal_inventory_preserves_sidecars(tmp_path):
    real = tmp_path / "real.json"
    real.write_text("{}")
    sidecar = _sidecar(real)
    records = build_inventory([("snapshot", tmp_path)])
    assert {record.path for record in records} == {"snapshot/real.json", "snapshot/._real.json"}
    assert sidecar.read_bytes() == APPLEDOUBLE
