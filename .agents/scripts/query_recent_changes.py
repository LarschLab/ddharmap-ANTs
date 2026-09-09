#!/usr/bin/env python3
"""Query compact entries from workflow-specific recent-change logs."""

from __future__ import annotations

import argparse
from pathlib import Path


LOG_NAMES = (
    "recent-changes-single-fish.md",
    "recent-changes-cohort.md",
)


def _entry_bounds(lines: list[str], hit: int) -> tuple[int, int]:
    start = hit
    while start > 0 and not lines[start].startswith("### "):
        start -= 1
    if not lines[start].startswith("### "):
        start = max(0, hit - 2)
    end = hit + 1
    while end < len(lines) and not lines[end].startswith("### "):
        end += 1
    return start, end


def query(repo_root: Path, term: str, limit: int) -> list[tuple[Path, int, list[str]]]:
    results: list[tuple[Path, int, list[str]]] = []
    needle = term.casefold()
    for name in LOG_NAMES:
        path = repo_root / ".agents" / "references" / name
        lines = path.read_text(errors="replace").splitlines()
        seen: set[tuple[int, int]] = set()
        for index, line in enumerate(lines):
            if needle not in line.casefold():
                continue
            start, end = _entry_bounds(lines, index)
            if (start, end) in seen:
                continue
            seen.add((start, end))
            results.append((path, index + 1, lines[start:end]))
            if len(results) >= limit:
                return results
    return results


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", required=True, help="Repository name; must be codeANTs.")
    parser.add_argument("--query", required=True, help="Case-insensitive term to find.")
    parser.add_argument("--limit", type=int, default=5)
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[2]
    if args.repo.casefold() != repo_root.name.casefold():
        parser.error(f"--repo must name {repo_root.name!r}")
    if args.limit < 1:
        parser.error("--limit must be positive")

    results = query(repo_root, args.query, args.limit)
    if not results:
        print("No matching recent-change entries.")
        return 0
    for path, line_number, entry in results:
        print(f"{path.relative_to(repo_root)}:{line_number}")
        print("\n".join(entry).rstrip())
        print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
