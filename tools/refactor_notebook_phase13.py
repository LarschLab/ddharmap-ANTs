from __future__ import annotations

import json
from pathlib import Path


NOTEBOOK_PATH = Path("notebooks/singleFish.ipynb")


CELL_34A = """# [34a]
try:
    _require_fish_state_34a = require_fish_state
except NameError:
    _require_fish_state_34a = None
if _require_fish_state_34a is not None and (not _require_fish_state_34a()):
    raise SystemExit

# Debug summary for functional↔anatomy matching (run after [34])
from codeants_2pf_hcr import FunctionalAnatomyDebugConfig, build_functional_anatomy_debug_stage

try:
    _plane_refs_34a = plane_refs
except NameError:
    _plane_refs_34a = []
try:
    _anat_labels_path_34a = ANAT_LABELS_PATH
except NameError:
    _anat_labels_path_34a = None
MIN_OVERLAP_FUNC_ANAT = 1
REQUIRE_OVERLAP_FUNC_ANAT = True
MAX_LINK_DIST_PX = 50.0

DEBUG_F2A = True
debug_result_34a = build_functional_anatomy_debug_stage(
    plane_refs=_plane_refs_34a,
    anat_labels_path=_anat_labels_path_34a,
    vox_anat=globals().get('VOX_ANAT'),
    load_func_labels_for_plane_func=globals().get('_load_func_labels_for_plane'),
    config=FunctionalAnatomyDebugConfig(
        debug_enabled=DEBUG_F2A,
        min_overlap=MIN_OVERLAP_FUNC_ANAT,
        require_overlap=REQUIRE_OVERLAP_FUNC_ANAT,
        max_link_dist_px=MAX_LINK_DIST_PX,
    ),
    imread_any_func=globals().get('imread_any'),
    ensure_uint_labels_func=globals().get('_ensure_uint_labels'),
    tform_for_plane_func=globals().get('_tform_for_plane'),
    resample_labels_nn_func=globals().get('resample_labels_nn'),
)
globals().update(debug_result_34a['bindings'])
for _line in debug_result_34a['log_lines']:
    print(_line)
if not debug_result_34a['debug_df'].empty:
    try:
        from IPython.display import display
        display(debug_result_34a['debug_df'])
    except Exception:
        print(debug_result_34a['debug_df'].to_string(index=False))
"""


def main() -> None:
    notebook = json.loads(NOTEBOOK_PATH.read_text())
    for cell in notebook.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        source = "".join(cell.get("source", []))
        if source.startswith("# [34a]"):
            cell["source"] = CELL_34A.splitlines(keepends=True)
            break
    NOTEBOOK_PATH.write_text(json.dumps(notebook, indent=1))


if __name__ == "__main__":
    main()
