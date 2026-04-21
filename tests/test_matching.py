from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd
import tifffile

from codeants_2pf_hcr.matching import (
    FunctionalAnatomyDebugConfig,
    build_functional_anatomy_debug_stage,
)


def test_build_functional_anatomy_debug_stage_returns_debug_df_bindings() -> None:
    with TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        anat_labels_path = tmp_path / "anat_labels.tif"
        tifffile.imwrite(anat_labels_path, np.asarray([[[0, 1], [0, 1]]], dtype=np.uint16))

        plane_refs = [{"best_z": 0, "label": "plane0"}]

        def _load_func_labels_for_plane(_plane_idx: int):
            return np.asarray([[0, 1], [0, 1]], dtype=np.uint16), "func.tif", None

        result = build_functional_anatomy_debug_stage(
            plane_refs=plane_refs,
            anat_labels_path=anat_labels_path,
            vox_anat={"X": 1.0, "Y": 1.0},
            load_func_labels_for_plane_func=_load_func_labels_for_plane,
            config=FunctionalAnatomyDebugConfig(debug_enabled=True),
        )

        assert result["status"] == "ok"
        assert "df_f2a_debug" in result["bindings"]
        assert isinstance(result["debug_df"], pd.DataFrame)
        assert not result["debug_df"].empty
