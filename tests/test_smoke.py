import tempfile
import unittest
from pathlib import Path

import pandas as pd

from codeants_2pf_hcr import SmokeValidationError, run_smoke_tier


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


class SmokeTests(unittest.TestCase):
    def _write_common_tier_d_outputs(self, out_reg: Path, *, fish_id: str = "L395_f11") -> None:
        _write_csv(
            out_reg / "functional_roi_activity_identity.csv",
            [
                {
                    "plane_idx": 0,
                    "func_label": 1,
                    "plane": "p0",
                    "roi_idx": 1,
                    "plane_match_outcome": "anatomy match",
                    "has_unique_anat_match": True,
                    "anat_label": 5,
                    "identity_label": "tac3b",
                    "activity_class": "Active neurons",
                    "is_active": True,
                    "response_is_active": True,
                    "response_class": "bout-responsive",
                    "response_summary_class": "Responsive neurons",
                    "bout_response_pass": True,
                    "cont_response_pass": False,
                    "bpi": 0.8,
                    "bpi_category": "bout-responsive",
                    "suite2p_is_cell": True,
                    "suite2p_activity_class": "Active neurons",
                }
            ],
        )
        _write_csv(
            out_reg / "functional_roi_activity_bpi_cells.csv",
            [
                {
                    "plane_idx": 0,
                    "func_label": 1,
                    "response_is_active": True,
                    "response_class": "bout-responsive",
                    "response_summary_class": "Responsive neurons",
                    "bout_response_pass": True,
                    "cont_response_pass": False,
                    "bpi": 0.8,
                    "bpi_category": "bout-responsive",
                }
            ],
        )
        _write_csv(
            out_reg / "hcr_activity_status.csv",
            [
                {
                    "fish_id": fish_id,
                    "gene": "tac3b",
                    "anat_label": 5,
                    "functional_status": "in-plane active ROI",
                    "represented_on_func_plane": True,
                    "selection_rule": "local-geometry-first",
                    "match_policy_version": "v1",
                }
            ],
        )
        _write_csv(
            out_reg / "conf_to_func_pairs.csv",
            [
                {
                    "fish_id": fish_id,
                    "gene": "tac3b",
                    "conf_mask": "m.tif",
                    "conf_label": 1,
                    "anat_label": 5,
                    "func_label": 1,
                    "plane": 0,
                    "response_is_active": True,
                    "response_class": "bout-responsive",
                    "response_summary_class": "Responsive neurons",
                    "is_selected_for_analysis": True,
                    "match_policy_version": "v1",
                }
            ],
        )
        _write_csv(
            out_reg / "hcr_func_candidates.csv",
            [
                {
                    "fish_id": fish_id,
                    "gene": "tac3b",
                    "anat_label": 5,
                    "plane_idx": 0,
                    "func_label": 1,
                    "response_is_active": True,
                    "response_class": "bout-responsive",
                    "response_summary_class": "Responsive neurons",
                }
            ],
        )
        _write_csv(out_reg / "functional_roi_activity_identity_summary.csv", [{"n": 1}])
        _write_csv(out_reg / "functional_roi_activity_identity_by_plane.csv", [{"plane_idx": 0, "n": 1}])
        _write_csv(out_reg / "functional_roi_activity_bpi_summary.csv", [{"n": 1}])
        _write_csv(out_reg / "conf_to_func_pairs_raw.csv", [{"fish_id": fish_id}])
        trace_meta = out_reg.parent / "derived" / "suite2p_traces" / "suite2p_dff_traces_meta.csv"
        _write_csv(trace_meta, [{"plane": 0, "func_label": 1, "trace_path": "p0.npy"}])

    def test_tier_b_passes_with_required_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            out_reg = Path(tmpdir)
            _write_csv(
                out_reg / "functional_roi_activity_identity.csv",
                [
                    {
                        "plane_idx": 0,
                        "func_label": 1,
                        "plane": "p0",
                        "roi_idx": 1,
                        "plane_match_outcome": "anatomy match",
                        "has_unique_anat_match": True,
                        "anat_label": 5,
                        "identity_label": "tac3b",
                        "activity_class": "Active neurons",
                        "is_active": True,
                        "response_is_active": True,
                        "response_class": "bout-responsive",
                        "response_summary_class": "Responsive neurons",
                        "bout_response_pass": True,
                        "cont_response_pass": False,
                        "bpi": 0.8,
                        "bpi_category": "bout-responsive",
                        "suite2p_is_cell": True,
                        "suite2p_activity_class": "Active neurons",
                    }
                ],
            )
            _write_csv(
                out_reg / "functional_roi_activity_bpi_cells.csv",
                [
                    {
                        "plane_idx": 0,
                        "func_label": 1,
                        "response_is_active": True,
                        "response_class": "bout-responsive",
                        "response_summary_class": "Responsive neurons",
                        "bout_response_pass": True,
                        "cont_response_pass": False,
                        "bpi": 0.8,
                        "bpi_category": "bout-responsive",
                    }
                ],
            )

            result = run_smoke_tier(out_reg=out_reg, tier="B", fish_id="L395_f11")
            self.assertEqual(result["n_checks"], 3)
            self.assertEqual(result["tier"], "B")

    def test_tier_c_fails_on_missing_response_columns(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            out_reg = Path(tmpdir)
            _write_csv(
                out_reg / "functional_roi_activity_identity.csv",
                [
                    {
                        "plane_idx": 0,
                        "func_label": 1,
                        "plane": "p0",
                        "roi_idx": 1,
                        "plane_match_outcome": "anatomy match",
                        "has_unique_anat_match": True,
                        "anat_label": 5,
                        "identity_label": "tac3b",
                        "activity_class": "Active neurons",
                        "is_active": True,
                    }
                ],
            )
            with self.assertRaises(SmokeValidationError):
                run_smoke_tier(out_reg=out_reg, tier="C", fish_id="L395_f11")

    def test_tier_c_fails_on_fish_id_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            out_reg = Path(tmpdir)
            _write_csv(
                out_reg / "functional_roi_activity_identity.csv",
                [
                    {
                        "plane_idx": 0,
                        "func_label": 1,
                        "plane": "p0",
                        "roi_idx": 1,
                        "plane_match_outcome": "anatomy match",
                        "has_unique_anat_match": True,
                        "anat_label": 5,
                        "identity_label": "tac3b",
                        "activity_class": "Active neurons",
                        "is_active": True,
                        "response_is_active": True,
                        "response_class": "bout-responsive",
                        "response_summary_class": "Responsive neurons",
                        "bout_response_pass": True,
                        "cont_response_pass": False,
                        "bpi": 0.8,
                        "bpi_category": "bout-responsive",
                        "suite2p_is_cell": True,
                        "suite2p_activity_class": "Active neurons",
                    }
                ],
            )
            _write_csv(
                out_reg / "functional_roi_activity_bpi_cells.csv",
                [
                    {
                        "plane_idx": 0,
                        "func_label": 1,
                        "response_is_active": True,
                        "response_class": "bout-responsive",
                        "response_summary_class": "Responsive neurons",
                        "bout_response_pass": True,
                        "cont_response_pass": False,
                        "bpi": 0.8,
                        "bpi_category": "bout-responsive",
                    }
                ],
            )
            _write_csv(
                out_reg / "hcr_activity_status.csv",
                [
                    {
                        "fish_id": "L000_f00",
                        "gene": "tac3b",
                        "anat_label": 5,
                        "functional_status": "in-plane active ROI",
                        "represented_on_func_plane": True,
                        "selection_rule": "local-geometry-first",
                        "match_policy_version": "v1",
                    }
                ],
            )
            _write_csv(
                out_reg / "conf_to_func_pairs.csv",
                [
                    {
                        "fish_id": "L000_f00",
                        "gene": "tac3b",
                        "conf_mask": "m.tif",
                        "conf_label": 1,
                        "anat_label": 5,
                        "func_label": 1,
                        "plane": 0,
                        "response_is_active": True,
                        "response_class": "bout-responsive",
                        "response_summary_class": "Responsive neurons",
                        "is_selected_for_analysis": True,
                        "match_policy_version": "v1",
                    }
                ],
            )
            _write_csv(
                out_reg / "hcr_func_candidates.csv",
                [
                    {
                        "fish_id": "L000_f00",
                        "gene": "tac3b",
                        "anat_label": 5,
                        "plane_idx": 0,
                        "func_label": 1,
                        "response_is_active": True,
                        "response_class": "bout-responsive",
                        "response_summary_class": "Responsive neurons",
                    }
                ],
            )

            with self.assertRaises(SmokeValidationError):
                run_smoke_tier(out_reg=out_reg, tier="C", fish_id="L395_f11")

    def test_tier_d_looks_for_trace_metadata_in_derived_folder(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            out_reg = Path(tmpdir) / "registration"
            out_reg.mkdir(parents=True, exist_ok=True)
            self._write_common_tier_d_outputs(out_reg)
            result = run_smoke_tier(out_reg=out_reg, tier="D", fish_id="L395_f11")
            self.assertEqual(result["tier"], "D")

    def test_tier_e_enforces_notebook_56h_symbol_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            out_reg = Path(tmpdir) / "registration"
            out_reg.mkdir(parents=True, exist_ok=True)
            self._write_common_tier_d_outputs(out_reg)

            notebook_good = Path(tmpdir) / "ok.ipynb"
            notebook_good.write_text(
                '{"cells":[{"cell_type":"code","source":["# [56h]\\n",'
                '"from codeants_2pf_hcr.stimulus import build_prestim_baseline_windows, combine_segments, compute_zscore_stats, effective_motion_window\\n",'
                '"_combine_segments = combine_segments\\n"]}]}'
            )
            result = run_smoke_tier(
                out_reg=out_reg,
                tier="E",
                fish_id="L395_f11",
                notebook_path=notebook_good,
            )
            self.assertEqual(result["tier"], "E")

            notebook_bad = Path(tmpdir) / "bad.ipynb"
            notebook_bad.write_text(
                '{"cells":[{"cell_type":"code","source":["# [56h]\\n",'
                '"from codeants_2pf_hcr.stimulus import build_prestim_baseline_windows, compute_zscore_stats, effective_motion_window\\n",'
                '"mean, sem = _combine_segments(arr)\\n"]}]}'
            )
            with self.assertRaises(SmokeValidationError):
                run_smoke_tier(
                    out_reg=out_reg,
                    tier="E",
                    fish_id="L395_f11",
                    notebook_path=notebook_bad,
                )


if __name__ == "__main__":
    unittest.main()
