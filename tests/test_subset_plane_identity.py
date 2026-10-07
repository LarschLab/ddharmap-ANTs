from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from codeants_2pf_hcr.matching import build_functional_roi_master_df
from codeants_2pf_hcr.pipeline import _build_activity_detail_from_geometry


@pytest.mark.parametrize("indices", [(0, 1), (5, 6, 7, 8, 9), (1, 4, 9)])
def test_geometry_exports_acquisition_indices_not_list_positions(indices):
    refs = [{"index": i, "label": f"plane{i}", "best_z": 0} for i in indices]
    planes = {local: {"stat": [{"ypix": np.array([1]), "xpix": np.array([1])}],
                      "ops": {"Ly": 3, "Lx": 3}, "iscell": np.array([[1., 1.]]),
                      "plane_dir": Path(f"/fish/suite2P/plane{i}")}
              for local, i in enumerate(indices)}
    labels = np.ones((1, 3, 3), dtype=np.uint16)
    detail, summary = build_functional_roi_master_df(planes, refs, labels)
    assert detail.plane_idx.tolist() == list(indices)
    assert summary.plane_idx.tolist() == list(indices)
    assert detail.func_source.tolist() == [f"/fish/suite2P/plane{i}" for i in indices]


def test_scoring_rejects_wrong_source_even_when_all_roi_keys_match(tmp_path, monkeypatch):
    geometry = tmp_path / "geometry.csv"
    pd.DataFrame([{"plane_idx": 0, "func_label": 1,
                   "func_source": str(tmp_path / "suite2P/plane5")}]).to_csv(geometry, index=False)
    paths = SimpleNamespace(matching_metadata_csv=tmp_path / "matching.csv",
                            fish_dir=tmp_path, functional_suite2p_dir=tmp_path / "suite2P")
    monkeypatch.setattr("codeants_2pf_hcr.context.resolve_func_polarity", lambda *a, **k: ("south", "test"))
    monkeypatch.setattr("codeants_2pf_hcr.suite2p.load_suite2p_stage", lambda **k: {
        "suite2p_by_ref_idx": {0: {"dff": np.ones((1, 8)), "iscell_keep": np.array([True]),
                                  "plane_dir": tmp_path / "suite2P/plane0"}}})
    with pytest.raises(RuntimeError, match="source-plane mismatch for 1"):
        _build_activity_detail_from_geometry(paths=paths, fish_id="L765_f05", geometry_path=geometry)
