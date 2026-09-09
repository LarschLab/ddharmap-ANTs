from __future__ import annotations

import json
import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = REPO_ROOT / ".agents" / "contracts" / "scientific-artifacts.json"
REQUIRED_FIELDS = {
    "artifact_id",
    "artifact_kind",
    "renderer_owner",
    "question",
    "scope",
    "authoritative_sources",
    "population",
    "filters",
    "denominator",
    "units",
    "grouping",
    "thresholds",
    "forbidden_reinterpretations",
    "validation_tests",
}


def _contracts() -> list[dict]:
    payload = json.loads(CONTRACT_PATH.read_text())
    assert payload["contract_version"] == 1
    return payload["artifacts"]


def test_scientific_artifact_contracts_are_complete_and_unique() -> None:
    contracts = _contracts()
    identifiers = [contract["artifact_id"] for contract in contracts]
    assert len(identifiers) == len(set(identifiers))
    for contract in contracts:
        assert REQUIRED_FIELDS == set(contract), contract["artifact_id"]
        for field in REQUIRED_FIELDS - {"artifact_kind"}:
            assert contract[field], (contract["artifact_id"], field)
        assert contract["artifact_kind"] in {"table", "figure"}
        assert isinstance(contract["forbidden_reinterpretations"], list)
        assert len(contract["forbidden_reinterpretations"]) >= 2


def test_contract_owners_and_validation_tests_resolve() -> None:
    for contract in _contracts():
        module_path = REPO_ROOT / "src" / Path(*contract["renderer_owner"].split(".")).with_suffix(".py")
        assert module_path.is_file(), (contract["artifact_id"], module_path)
        for nodeid in contract["validation_tests"]:
            relative_path, test_name = nodeid.split("::", 1)
            test_path = REPO_ROOT / relative_path
            assert test_path.is_file(), nodeid
            source = test_path.read_text()
            assert re.search(rf"def {re.escape(test_name)}\s*\(", source), nodeid


def test_whole_population_contracts_keep_roi_master_authority() -> None:
    for contract in _contracts():
        if "whole-population" not in contract["scope"]:
            continue
        sources = " ".join(contract["authoritative_sources"])
        assert "functional_roi_activity_identity.csv" in sources or contract["artifact_id"] == "functional_roi_activity_identity.csv"
        assert contract["denominator"]


def test_figure_rules_point_to_machine_readable_contracts() -> None:
    source = (REPO_ROOT / ".agents" / "references" / "figure-rules.md").read_text()
    assert ".agents/contracts/scientific-artifacts.json" in source
