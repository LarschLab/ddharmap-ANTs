"""Helpers to organize analysis outputs into a stable folder layout."""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any


_REGISTRATION_FILENAMES = {
    "anatomy_identity_lookup.csv",
    "conf_to_func_pairs.csv",
    "conf_to_func_pairs_raw.csv",
    "functional_roi_activity_bpi_cells.csv",
    "functional_roi_activity_bpi_summary.csv",
    "functional_roi_activity_identity.csv",
    "functional_roi_activity_identity_by_plane.csv",
    "functional_roi_activity_identity_summary.csv",
    "hcr_activity_status.csv",
    "hcr_func_candidates.csv",
}

_DERIVED_TARGETS = {
    "suite2p_dff_traces_meta.csv": Path("derived") / "suite2p_traces" / "suite2p_dff_traces_meta.csv",
}


@dataclass(frozen=True)
class OrganizeAction:
    src: Path
    dst: Path
    status: str
    reason: str


def _source_candidates(analysis_dir: Path) -> tuple[Path, ...]:
    functional_dir = analysis_dir / "functional"
    return analysis_dir, functional_dir


def _target_for_name(functional_dir: Path, name: str) -> Path | None:
    if name in _REGISTRATION_FILENAMES:
        return functional_dir / "registration" / name
    rel = _DERIVED_TARGETS.get(name)
    if rel is not None:
        return functional_dir / rel
    return None


def organize(
    analysis_dir: str | Path,
    *,
    apply: bool = False,
    move_unknown: bool = False,
    verbose: bool = False,
) -> dict[str, Any]:
    """Plan or apply deterministic output placement under analysis_dir/functional."""
    analysis_path = Path(analysis_dir)
    functional_dir = analysis_path / "functional"

    actions: list[OrganizeAction] = []
    planned: set[tuple[Path, Path]] = set()

    for source_dir in _source_candidates(analysis_path):
        if not source_dir.exists():
            continue
        for src in sorted(source_dir.glob("*")):
            if not src.is_file():
                continue
            dst = _target_for_name(functional_dir, src.name)
            if dst is None and move_unknown:
                dst = functional_dir / "derived" / "unclassified" / src.name
            if dst is None:
                continue
            if src.resolve() == dst.resolve():
                continue
            key = (src.resolve(), dst.resolve())
            if key in planned:
                continue
            planned.add(key)
            if dst.exists():
                actions.append(OrganizeAction(src=src, dst=dst, status="skipped", reason="destination exists"))
                continue
            if apply:
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(src), str(dst))
                actions.append(OrganizeAction(src=src, dst=dst, status="moved", reason="ok"))
            else:
                actions.append(OrganizeAction(src=src, dst=dst, status="planned", reason="dry-run"))

    result = {
        "analysis_dir": str(analysis_path),
        "functional_dir": str(functional_dir),
        "apply": bool(apply),
        "move_unknown": bool(move_unknown),
        "actions": [a.__dict__ for a in actions],
        "n_moved": sum(1 for a in actions if a.status == "moved"),
        "n_planned": sum(1 for a in actions if a.status == "planned"),
        "n_skipped": sum(1 for a in actions if a.status == "skipped"),
    }
    if verbose:
        prefix = "[organize]"
        print(
            f"{prefix} apply={result['apply']} moved={result['n_moved']} planned={result['n_planned']} skipped={result['n_skipped']}"
        )
        for action in actions:
            print(f"{prefix} {action.status}: {action.src} -> {action.dst} ({action.reason})")
    return result


__all__ = ["organize"]
