#!/usr/bin/env python3

from __future__ import annotations

import argparse
import re
from pathlib import Path

import nbformat
from nbclient import NotebookClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Execute a notebook end-to-end (or through a cell index).")
    parser.add_argument("notebook", help="Path to .ipynb notebook file.")
    parser.add_argument("--through-cell", type=int, default=None, help="Inclusive code-cell index to execute through.")
    parser.add_argument("--timeout", type=int, default=1800, help="Per-cell execution timeout (seconds).")
    parser.add_argument("--kernel", default="python3", help="Kernel name.")
    parser.add_argument("--fish-id")
    parser.add_argument("--local-root", type=Path)
    parser.add_argument("--pipeline-root", type=Path)
    parser.add_argument("--output", type=Path, help="Save an executed copy; never changes the source notebook.")
    parser.add_argument("--capture-figures", action="store_true", help="Enable inline PNG MIME output in the executed copy.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    nb_path = Path(args.notebook).expanduser().resolve()
    if not nb_path.exists():
        raise SystemExit(f"[notebook-run] Notebook not found: {nb_path}")

    with nb_path.open("r", encoding="utf-8") as handle:
        nb = nbformat.read(handle, as_version=4)

    overrides = {"FISH_ID": repr(args.fish_id) if args.fish_id else None,
                 "LOCAL_ROOT": f"Path({str(args.local_root)!r})" if args.local_root else None,
                 "PIPELINE_ROOT": f"Path({str(args.pipeline_root)!r})" if args.pipeline_root else None}
    if any(overrides.values()) and not all(overrides.values()):
        raise SystemExit("fish-id, local-root, and pipeline-root must be specified together")
    for key, value in overrides.items():
        if value is None:
            continue
        count = 0
        for cell in nb.cells:
            if cell.cell_type == "code":
                cell.source, found = re.subn(rf"^{key} = .*?$", lambda _: f"{key} = {value}", cell.source, flags=re.MULTILINE)
                count += found
        if count != 1:
            raise SystemExit(f"expected one explicit {key} assignment, found {count}")

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
    if args.capture_figures:
        nb.cells.insert(0, nbformat.v4.new_code_cell("get_ipython().run_line_magic(\"matplotlib\", \"inline\")"))
    client.execute()
    if args.output:
        output = args.output.expanduser().resolve()
        if output == nb_path:
            raise SystemExit("executed output must differ from source notebook")
        output.parent.mkdir(parents=True, exist_ok=True)
        nbformat.write(nb, output)
    if args.through_cell is None:
        print(f"[notebook-run] PASS notebook={nb_path}")
    else:
        print(f"[notebook-run] PASS notebook={nb_path} through_cell={args.through_cell}")


if __name__ == "__main__":
    main()
