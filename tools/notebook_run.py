#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path

import nbformat
from nbclient import NotebookClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Execute a notebook end-to-end (or through a cell index).")
    parser.add_argument("notebook", help="Path to .ipynb notebook file.")
    parser.add_argument("--through-cell", type=int, default=None, help="Inclusive code-cell index to execute through.")
    parser.add_argument("--timeout", type=int, default=1800, help="Per-cell execution timeout (seconds).")
    parser.add_argument("--kernel", default="python3", help="Kernel name.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    nb_path = Path(args.notebook).expanduser().resolve()
    if not nb_path.exists():
        raise SystemExit(f"[notebook-run] Notebook not found: {nb_path}")

    with nb_path.open("r", encoding="utf-8") as handle:
        nb = nbformat.read(handle, as_version=4)

    if args.through_cell is not None:
        if args.through_cell < 0:
            raise SystemExit("[notebook-run] --through-cell must be >= 0")
        nb.cells = nb.cells[: args.through_cell + 1]

    client = NotebookClient(
        nb,
        timeout=int(args.timeout),
        kernel_name=args.kernel,
        allow_errors=False,
    )
    client.execute()
    if args.through_cell is None:
        print(f"[notebook-run] PASS notebook={nb_path}")
    else:
        print(f"[notebook-run] PASS notebook={nb_path} through_cell={args.through_cell}")


if __name__ == "__main__":
    main()
