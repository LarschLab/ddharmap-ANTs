"""Smoke-test helpers for staged notebook outputs."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from .notebook_contract import check_notebook_contract

class SmokeValidationError(RuntimeError):
    """Raised when a smoke contract check fails."""


MASTER_BASE_REQUIRED = {
    "plane_idx",
    "func_label",
    "plane",
    "roi_idx",
    "plane_match_outcome",
    "has_unique_anat_match",
    "anat_label",
    "identity_label",
    "activity_class",
    "is_active",
}

MASTER_RESPONSE_REQUIRED = {
    "response_is_active",
    "response_class",
    "response_summary_class",
    "bout_response_pass",
    "cont_response_pass",
    "bpi",
    "bpi_category",
    "suite2p_is_cell",
    "suite2p_activity_class",
}

BPI_CELLS_REQUIRED = {
    "plane_idx",
    "func_label",
    "response_is_active",
    "response_class",
    "response_summary_class",
    "bout_response_pass",
    "cont_response_pass",
    "bpi",
    "bpi_category",
}

HCR_STATUS_REQUIRED = {
    "fish_id",
    "gene",
    "anat_label",
    "functional_status",
    "represented_on_func_plane",
    "selection_rule",
    "match_policy_version",
}

HCR_PAIRS_REQUIRED = {
    "fish_id",
    "gene",
    "conf_mask",
    "conf_label",
    "anat_label",
    "func_label",
    "plane",
    "response_is_active",
    "response_class",
    "response_summary_class",
    "is_selected_for_analysis",
    "match_policy_version",
}

HCR_CANDIDATES_REQUIRED = {
    "fish_id",
    "gene",
    "anat_label",
    "plane_idx",
    "func_label",
    "response_is_active",
    "response_class",
    "response_summary_class",
}

TIER_ORDER = ("A", "B", "C", "D", "E")


@dataclass(frozen=True)
class SmokeCheckResult:
    name: str
    path: Path
    rows: int


def _load_csv(path: Path, *, label: str, allow_empty: bool) -> pd.DataFrame:
    if not path.exists():
        raise SmokeValidationError(f"{label}: missing file at {path}")
    df = pd.read_csv(path)
    if df.empty and not allow_empty:
        raise SmokeValidationError(f"{label}: file is empty at {path}")
    return df


def _require_columns(df: pd.DataFrame, required: set[str], *, label: str, hint: str) -> None:
    missing = sorted(required - set(df.columns))
    if missing:
        raise SmokeValidationError(f"{label}: missing required columns {missing}. {hint}")


def _check_fish_id(df: pd.DataFrame, fish_id: str | None, *, label: str) -> None:
    if fish_id is None or "fish_id" not in df.columns or df.empty:
        return
    mismatched = df["fish_id"].dropna().astype(str).ne(str(fish_id))
    if bool(mismatched.any()):
        raise SmokeValidationError(f"{label}: contains rows for fish IDs other than {fish_id}")


def _validate_master_roi(path: Path, *, require_response: bool) -> SmokeCheckResult:
    df = _load_csv(
        path,
        label="[50i] functional_roi_activity_identity.csv",
        allow_empty=False,
    )
    _require_columns(
        df,
        MASTER_BASE_REQUIRED,
        label="[50i] functional_roi_activity_identity.csv",
        hint="Re-run [50h] and [50i].",
    )
    if require_response:
        _require_columns(
            df,
            MASTER_RESPONSE_REQUIRED,
            label="[50ia] functional_roi_activity_identity.csv",
            hint="Re-run [50ia] after [50i].",
        )
    return SmokeCheckResult(name="master-roi", path=path, rows=int(len(df)))


def _validate_bpi_cells(path: Path) -> SmokeCheckResult:
    df = _load_csv(
        path,
        label="[50ia] functional_roi_activity_bpi_cells.csv",
        allow_empty=False,
    )
    _require_columns(
        df,
        BPI_CELLS_REQUIRED,
        label="[50ia] functional_roi_activity_bpi_cells.csv",
        hint="Re-run [50ia].",
    )
    return SmokeCheckResult(name="bpi-cells", path=path, rows=int(len(df)))


def _validate_hcr_status(path: Path, *, fish_id: str | None) -> SmokeCheckResult:
    df = _load_csv(
        path,
        label="[50] hcr_activity_status.csv",
        allow_empty=False,
    )
    _require_columns(
        df,
        HCR_STATUS_REQUIRED,
        label="[50] hcr_activity_status.csv",
        hint="Re-run [50] after [50ia].",
    )
    _check_fish_id(df, fish_id, label="[50] hcr_activity_status.csv")
    return SmokeCheckResult(name="hcr-status", path=path, rows=int(len(df)))


def _validate_hcr_pairs(path: Path, *, fish_id: str | None) -> SmokeCheckResult:
    df = _load_csv(
        path,
        label="[50] conf_to_func_pairs.csv",
        allow_empty=True,
    )
    _require_columns(
        df,
        HCR_PAIRS_REQUIRED,
        label="[50] conf_to_func_pairs.csv",
        hint="Re-run [50] after [50ia].",
    )
    _check_fish_id(df, fish_id, label="[50] conf_to_func_pairs.csv")
    return SmokeCheckResult(name="hcr-pairs", path=path, rows=int(len(df)))


def _validate_hcr_candidates(path: Path, *, fish_id: str | None) -> SmokeCheckResult:
    df = _load_csv(
        path,
        label="[50] hcr_func_candidates.csv",
        allow_empty=False,
    )
    _require_columns(
        df,
        HCR_CANDIDATES_REQUIRED,
        label="[50] hcr_func_candidates.csv",
        hint="Re-run [50] after [50ia].",
    )
    _check_fish_id(df, fish_id, label="[50] hcr_func_candidates.csv")
    return SmokeCheckResult(name="hcr-candidates", path=path, rows=int(len(df)))


def _validate_exists(path: Path, *, label: str, hint: str) -> SmokeCheckResult:
    if not path.exists():
        raise SmokeValidationError(f"{label}: missing file at {path}. {hint}")
    rows = 0
    if path.suffix.lower() == ".csv":
        rows = int(len(pd.read_csv(path)))
    return SmokeCheckResult(name=label, path=path, rows=rows)


def _stale_regeneration_targets(out_reg: Path) -> list[tuple[Path, str]]:
    out_functional = out_reg.parent
    return [
        (out_reg / "hcr_activity_status.csv", "[50] hcr_activity_status.csv"),
        (out_reg / "conf_to_func_pairs_raw.csv", "[50] conf_to_func_pairs_raw.csv"),
        (out_reg / "conf_to_func_pairs.csv", "[50] conf_to_func_pairs.csv"),
        (out_reg / "hcr_func_candidates.csv", "[50] hcr_func_candidates.csv"),
        (out_reg / "functional_roi_activity_identity.csv", "[50i]/[50ia] functional_roi_activity_identity.csv"),
        (out_reg / "functional_roi_activity_identity_summary.csv", "[50i] functional_roi_activity_identity_summary.csv"),
        (out_reg / "functional_roi_activity_identity_by_plane.csv", "[50i] functional_roi_activity_identity_by_plane.csv"),
        (out_reg / "functional_roi_activity_bpi_cells.csv", "[50ia] functional_roi_activity_bpi_cells.csv"),
        (out_reg / "functional_roi_activity_bpi_summary.csv", "[50ia] functional_roi_activity_bpi_summary.csv"),
        (out_functional / "derived" / "suite2p_traces" / "suite2p_dff_traces_meta.csv", "[51] derived/suite2p_traces/suite2p_dff_traces_meta.csv"),
    ]


def _code_cell_by_tag(notebook_path: Path, tag: str) -> str:
    if not notebook_path.exists():
        raise SmokeValidationError(f"[notebook] missing notebook at {notebook_path}")
    notebook = json.loads(notebook_path.read_text())
    for cell in notebook.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        source = "".join(cell.get("source", []))
        if source.startswith(f"# [{tag}]"):
            return source
    raise SmokeValidationError(f"[notebook] code cell [{tag}] not found in {notebook_path}")


def _validate_notebook_symbol_contracts(notebook_path: Path) -> SmokeCheckResult:
    cell_56h = _code_cell_by_tag(notebook_path, "56h")
    import_line = (
        "from codeants_2pf_hcr.stimulus import "
        "build_prestim_baseline_windows, combine_segments, compute_zscore_stats, effective_motion_window"
    )
    if import_line not in cell_56h:
        raise SmokeValidationError(
            "[56h] notebook symbol contract failed: missing combine_segments import from codeants_2pf_hcr.stimulus."
        )
    if "_combine_segments = combine_segments" not in cell_56h:
        raise SmokeValidationError(
            "[56h] notebook symbol contract failed: missing _combine_segments alias binding."
        )
    contract = check_notebook_contract(notebook_path)
    if contract["n_required_cell_violations"] > 0:
        raise SmokeValidationError(
            f"[notebook] required cell contract violations detected: {contract['required_cell_violations']}"
        )
    if contract["n_native_stage_import_violations"] > 0:
        raise SmokeValidationError(
            f"[notebook] native stage import violations detected: {contract['native_stage_import_violations']}"
        )
    return SmokeCheckResult(name="[56h] symbol-contract", path=notebook_path, rows=0)


def run_smoke_tier(
    *,
    out_reg: str | Path,
    tier: str = "A",
    fish_id: str | None = None,
    notebook_path: str | Path = "notebooks/singleFish.ipynb",
) -> dict[str, Any]:
    tier_norm = str(tier).strip().upper()
    if tier_norm not in TIER_ORDER:
        raise ValueError(f"Unsupported tier {tier!r}. Expected one of {TIER_ORDER}.")

    out_reg_path = Path(out_reg)
    checks: list[SmokeCheckResult] = []

    checks.append(_validate_master_roi(out_reg_path / "functional_roi_activity_identity.csv", require_response=False))

    if TIER_ORDER.index(tier_norm) >= TIER_ORDER.index("B"):
        checks.append(_validate_master_roi(out_reg_path / "functional_roi_activity_identity.csv", require_response=True))
        checks.append(_validate_bpi_cells(out_reg_path / "functional_roi_activity_bpi_cells.csv"))

    if TIER_ORDER.index(tier_norm) >= TIER_ORDER.index("C"):
        checks.append(_validate_hcr_status(out_reg_path / "hcr_activity_status.csv", fish_id=fish_id))
        checks.append(_validate_hcr_pairs(out_reg_path / "conf_to_func_pairs.csv", fish_id=fish_id))
        checks.append(_validate_hcr_candidates(out_reg_path / "hcr_func_candidates.csv", fish_id=fish_id))

    if TIER_ORDER.index(tier_norm) >= TIER_ORDER.index("D"):
        for path, label in _stale_regeneration_targets(out_reg_path):
            checks.append(_validate_exists(path, label=label, hint="Re-run the cache rerun sequence from .agents/references/cache-rerun-policy.md."))

    if TIER_ORDER.index(tier_norm) >= TIER_ORDER.index("E"):
        checks.append(_validate_notebook_symbol_contracts(Path(notebook_path)))

    return {
        "tier": tier_norm,
        "out_reg": str(out_reg_path),
        "fish_id": fish_id,
        "notebook_path": str(notebook_path),
        "checks": [result.__dict__ for result in checks],
        "n_checks": int(len(checks)),
    }


__all__ = [
    "SmokeValidationError",
    "run_smoke_tier",
]
