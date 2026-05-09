"""Runtime helpers for cross-platform path resolution and optional dependencies."""

from __future__ import annotations

import importlib
import os
from pathlib import Path
from typing import Any


LOCAL_ROOT_ENV_KEYS = (
    "CODEANTS_2PF_HCR_LOCAL_ROOT",
    "DDHARMAP_2PF_HCR_LOCAL_ROOT",
    "TWO_PF_HCR_LOCAL_ROOT",
)


class NativeDependencyError(RuntimeError):
    """Raised when an optional native dependency is unavailable for a stage."""


def candidate_local_roots() -> list[Path]:
    candidates: list[Path] = []
    seen: set[str] = set()

    def add(path_value: str | Path | None) -> None:
        if path_value in (None, ""):
            return
        path = Path(path_value).expanduser()
        key = str(path)
        if key in seen:
            return
        seen.add(key)
        candidates.append(path)

    for key in LOCAL_ROOT_ENV_KEYS:
        add(os.environ.get(key))

    home = Path.home()
    if os.name == "nt":
        add(home / "dataProcessing" / "2p_processing")
        add(home / "dataProcessing" / "2p_HCR" / "analysis" / "midThesis")
        add(home / "Documents" / "2p_HCR" / "analysis" / "midThesis")
    else:
        add(home / "dataProcessing" / "2p_processing")
        add(home / "dataProcessing" / "2p_HCR" / "analysis" / "midThesis")
        add(home / "Documents" / "2p_HCR" / "analysis" / "midThesis")
    return candidates


def default_local_root(*, strict: bool = False, fallback: str | Path | None = None) -> Path | None:
    for candidate in candidate_local_roots():
        if candidate.exists():
            return candidate
    if fallback not in (None, ""):
        return Path(fallback).expanduser()
    if strict:
        env_hint = ", ".join(LOCAL_ROOT_ENV_KEYS)
        raise RuntimeError(
            "Local data root is not configured. Set one of "
            f"{env_hint} or pass local_root/data_root explicitly."
        )
    return None


def optional_dependency_error(*, module_name: str, stage_tag: str, exc: Exception) -> NativeDependencyError:
    message = (
        f"{stage_tag} requires optional dependency '{module_name}', but it failed to load: "
        f"{type(exc).__name__}: {exc}"
    )
    if os.name == "nt" and isinstance(exc, OSError):
        message += (
            " On Windows this usually means a missing DLL/runtime dependency or an incompatible "
            "native wheel for the active environment."
        )
    return NativeDependencyError(message)


def import_optional_module(module_name: str, *, stage_tag: str) -> Any:
    try:
        return importlib.import_module(module_name)
    except Exception as exc:  # pragma: no cover - depends on local runtime
        raise optional_dependency_error(module_name=module_name, stage_tag=stage_tag, exc=exc) from exc


def build_dependency_preflight_report(modules: list[str] | None = None) -> dict[str, Any]:
    report: dict[str, Any] = {}
    for module_name in modules or ["cellpose", "ants", "SimpleITK", "nrrd", "torch"]:
        try:
            module = importlib.import_module(module_name)
            report[module_name] = {"ok": True, "version": getattr(module, "__version__", None), "error": None}
        except Exception as exc:  # pragma: no cover - depends on local runtime
            report[module_name] = {
                "ok": False,
                "version": None,
                "error": f"{type(exc).__name__}: {exc}",
            }
    return report


__all__ = [
    "LOCAL_ROOT_ENV_KEYS",
    "NativeDependencyError",
    "build_dependency_preflight_report",
    "candidate_local_roots",
    "default_local_root",
    "import_optional_module",
    "optional_dependency_error",
]
