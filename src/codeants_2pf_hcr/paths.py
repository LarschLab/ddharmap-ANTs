"""Dependency-free directory scans that exclude macOS metadata sidecars."""
from __future__ import annotations

from pathlib import Path
from typing import Iterator


def is_appledouble(path: str | Path) -> bool:
    """Return whether the basename starts with the AppleDouble prefix ``._``."""
    return Path(path).name.startswith("._")


def is_visible_path(path: str | Path) -> bool:
    """Exclude AppleDouble and .DS_Store entries, including ancestor components.

    Other hidden files remain visible. This also filters nested glob results
    below a metadata-named directory without changing traversal or ordering.
    """
    return not any(is_appledouble(part) or part == ".DS_Store" for part in Path(path).parts)


def visible_glob(directory: str | Path, pattern: str) -> Iterator[Path]:
    """Yield Path.glob results excluding macOS metadata entries."""
    return (path for path in Path(directory).glob(pattern) if is_visible_path(path))


def visible_rglob(directory: str | Path, pattern: str) -> Iterator[Path]:
    """Yield Path.rglob results excluding macOS metadata entries."""
    return (path for path in Path(directory).rglob(pattern) if is_visible_path(path))


def visible_iterdir(directory: str | Path) -> Iterator[Path]:
    """Yield Path.iterdir results excluding macOS metadata entries."""
    return (path for path in Path(directory).iterdir() if is_visible_path(path))


__all__ = ["is_appledouble", "is_visible_path", "visible_glob", "visible_rglob", "visible_iterdir"]
