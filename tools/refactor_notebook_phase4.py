#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path


NB_PATH = Path("notebooks/singleFish.ipynb")


CELL_23A = """# [23a]
# Suite2p: load outputs, build label masks, compute dF/F (iscell only)
from pathlib import Path

from codeants_2pf_hcr import Suite2pStageConfig, load_suite2p_stage

USE_SUITE2P_LABELS = True
_default_suite2p_root = OUTDIR / "suite2P"
try:
    _suite2p_root_raw = SUITE2P_ROOT
except NameError:
    _suite2p_root_raw = _default_suite2p_root
SUITE2P_ROOT = Path(_suite2p_root_raw) if _suite2p_root_raw is not None else _default_suite2p_root

try:
    ASSERT_FISH_COMPATIBLE = assert_fish_compatible
except NameError:
    ASSERT_FISH_COMPATIBLE = None

try:
    POLARITY_LOCAL = POLARITY
except NameError:
    POLARITY_LOCAL = None
try:
    POLARITY_SOURCE_LOCAL = POLARITY_SOURCE
except NameError:
    POLARITY_SOURCE_LOCAL = None
try:
    PLANE_REFS_LOCAL = plane_refs if plane_refs else []
except NameError:
    PLANE_REFS_LOCAL = []
try:
    FISH_ID_LOCAL = FISH_ID
except NameError:
    FISH_ID_LOCAL = None

SUITE2P_PLANE_GLOB = "plane*"
SUITE2P_FLIP_X = None  # None=auto
DFOF_BASELINE_PCT = 10.0
DFOF_EPS = 1e-6

suite2p_result = load_suite2p_stage(
    plane_refs=PLANE_REFS_LOCAL,
    suite2p_root=SUITE2P_ROOT,
    fish_id=FISH_ID_LOCAL,
    polarity=POLARITY_LOCAL,
    polarity_source=POLARITY_SOURCE_LOCAL,
    config=Suite2pStageConfig(
        use_suite2p_labels=USE_SUITE2P_LABELS,
        plane_glob=SUITE2P_PLANE_GLOB,
        flip_x=SUITE2P_FLIP_X,
        dfof_baseline_pct=DFOF_BASELINE_PCT,
        dfof_eps=DFOF_EPS,
    ),
    assert_fish_compatible=ASSERT_FISH_COMPATIBLE,
)

SUITE2P_ROOT = Path(suite2p_result["suite2p_root"])
suite2p_planes = suite2p_result["suite2p_planes"]
suite2p_by_ref_idx = suite2p_result["suite2p_by_ref_idx"]
func_labels = suite2p_result["func_labels"]
SUITE2P_FISH_ID = suite2p_result["suite2p_fish_id"]
SUITE2P_INPUT_XY_FRAME = suite2p_result["input_xy_frame"]
df_sum = suite2p_result["df_sum"]
df_src = suite2p_result["df_src"]

try:
    display(df_sum)
    display(df_src)
except Exception:
    pass
"""


def _replace_source(notebook: dict, tag: str, new_source: str) -> None:
    for cell in notebook["cells"]:
        if cell.get("cell_type") != "code":
            continue
        src = "".join(cell.get("source", []))
        if src.startswith(f"# [{tag}]"):
            cell["source"] = [line + "\n" for line in new_source.rstrip("\n").split("\n")]
            return
    raise RuntimeError(f"Notebook cell [{tag}] not found")


def main() -> None:
    notebook = json.loads(NB_PATH.read_text())
    _replace_source(notebook, "23a", CELL_23A)
    NB_PATH.write_text(json.dumps(notebook, indent=1))


if __name__ == "__main__":
    main()
