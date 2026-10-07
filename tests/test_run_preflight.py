from pathlib import Path
from types import SimpleNamespace
import numpy as np
import pandas as pd

from codeants_2pf_hcr.run_preflight import preflight_native_run, check_geometry_trace_identity, materialize_retained_graph
from codeants_2pf_hcr.single_fish_run import describe_path


def test_missing_implicit_graph_dependency_is_caught_without_writes(tmp_path):
    spec = SimpleNamespace(raw={"native_production": {}}, state_root=tmp_path / "state",
        stages=[SimpleNamespace(manifest="manifest.json", command=("python", "cli", "match-roi-to-anatomy"))])
    report = preflight_native_run(spec)
    assert not report["ok"]
    assert "register-functional-to-anatomy" in report["errors"][0]
    assert not spec.state_root.exists()


def test_midline_requires_registration_qc_and_transformed_roi_producers(tmp_path):
    spec = SimpleNamespace(raw={"native_production": {}}, state_root=tmp_path / "state",
        stages=[SimpleNamespace(manifest="m", command=("py", "cli", owner)) for owner in
                ("register-functional-to-anatomy", "make-anatomy-midline-proposal")])
    errors = preflight_native_run(spec)["errors"]
    assert any("make-functional-registration-qc" in error for error in errors)
    assert any("transform-functional-rois-to-anatomy" in error for error in errors)


def test_retained_graph_is_hash_bound_and_copied(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "plane_refs_summary.json").write_text("[]")
    spec = SimpleNamespace(raw={"native_production": {"retained_graph": {
        "register-functional-to-anatomy": {"path": str(source), "sha256": describe_path(source)["sha256"]}}}},
        state_root=tmp_path / "state", stages=[SimpleNamespace(manifest="m", command=("py", "cli", "match-roi-to-anatomy"))])
    assert preflight_native_run(spec)["ok"]
    assert not spec.state_root.exists()
    materialize_retained_graph(spec)
    assert (spec.state_root / "transaction_graph/register-functional-to-anatomy/plane_refs_summary.json").exists()
    (source / "plane_refs_summary.json").write_text("[1]")
    assert not preflight_native_run(spec)["ok"]


def test_preflight_rejects_overlapping_wrong_round_roi_keys(tmp_path):
    root = tmp_path / "03_analysis/functional/suite2P/plane0"
    root.mkdir(parents=True)
    np.save(root / "F.npy", np.ones((1, 3)))
    path = tmp_path / "geometry.csv"
    pd.DataFrame([{"plane_idx": 0, "func_label": 1, "func_source": str(root.with_name("plane5"))}]).to_csv(path, index=False)
    errors = check_geometry_trace_identity(path, fish_dir=tmp_path, eligible_planes={5})
    assert any("inconsistent Suite2p source" in error for error in errors)
    assert any("outside eligible" in error for error in errors)
