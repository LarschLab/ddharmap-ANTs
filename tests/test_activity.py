from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from codeants_2pf_hcr.activity import (
    SingleFishBpiDiagnosticsConfig,
    prepare_single_fish_bpi_diagnostics_stage,
)


def test_prepare_single_fish_bpi_diagnostics_stage_backfills_response_columns() -> None:
    with TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        master_csv = tmp_path / "functional_roi_activity_identity.csv"
        pd.DataFrame(
            {
                "fish_id": ["fishA", "fishA"],
                "plane_idx": [0, 0],
                "func_label": [1, 2],
                "response_class": ["responsive", "low activity"],
                "response_summary_class": ["Responsive neurons", "Low activity"],
                "response_is_active": [True, False],
            }
        ).to_csv(master_csv, index=False)

        bpi_cells_df = pd.DataFrame(
            {
                "fish_id": ["fishA", "fishA"],
                "gene": ["sst1.1", "npy"],
                "plane_idx": [0, 0],
                "func_label": [1, 2],
                "mean_bout_zdff": [0.6, 0.1],
                "mean_cont_zdff": [0.2, 0.1],
                "bpi": [0.5, 0.01],
            }
        )

        result = prepare_single_fish_bpi_diagnostics_stage(
            bpi_cells_df,
            fish_id="fishA",
            master_detail_csv=master_csv,
            config=SingleFishBpiDiagnosticsConfig(zero_band=0.05, n_activity_bins=4),
        )

        df = result["df"]
        assert list(df["response_summary_class"].astype(str)) == ["Responsive neurons", "Low activity"]
        assert list(df["response_is_active"].astype(bool)) == [True, False]
        assert result["activity_label"] == "z-scored dF/F"
        assert result["bpi_index_col"] == "bpi"
        assert result["summary_counts"]["n_total"] == 2
        assert result["summary_counts"]["n_near_zero"] == 1
        assert "bpi_activity_df" in result["bindings"]
        assert "BPI_ACTIVITY_FISH_ID" in result["bindings"]
