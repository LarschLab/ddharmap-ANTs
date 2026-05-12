#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path


NB_PATH = Path("notebooks/2PF_to_HCR.ipynb")


CELL_9 = """# HCR matching/QC helpers (antsQC-style)
from codeants_2pf_hcr import (
    build_anat_identity_lookup_df,
    build_functional_roi_master_df,
    build_hcr_activity_tables,
    compute_centroids,
    compute_label_overlap,
    gene_from_mask,
    hungarian_match,
    idx_to_um,
    nearest_neighbor_match,
    resolve_plane_transform,
    summarize_distances,
)

_tform_for_plane = resolve_plane_transform
"""


REPLACEMENTS = {
    "8": """# [8]
# Infer and cache voxel sizes (um) for func/anat[/HCR], preferring cached metadata in local mode
from pathlib import Path

from codeants_2pf_hcr import VoxelStageConfig, resolve_voxel_context_stage

try:
    DATA_MODE_LOCAL = str(DATA_MODE).strip().lower()
except NameError:
    DATA_MODE_LOCAL = 'nas'
FORCE_RECOMPUTE_VOXELS = DATA_MODE_LOCAL != "local"

voxel_result = resolve_voxel_context_stage(
    analysis_dir=ANALYSIS_DIR,
    outdir=OUTDIR,
    out_reg=OUT_REG,
    data_mode=DATA_MODE_LOCAL,
    func_stack_path=globals().get('FUNC_STACK_PATH'),
    func_raw_stack_path=globals().get('FUNC_RAW_STACK_PATH'),
    anat_stack_path=globals().get('ANAT_STACK_PATH'),
    hcr_stack_paths=globals().get('HCR_STACK_PATHS'),
    hcr_stack_path=globals().get('HCR_STACK_PATH'),
    metadata_dir=Path(FISH_DIR) / '01_raw' / '2p' / 'metadata',
    vox_func_auto=globals().get('VOX_FUNC_AUTO'),
    vox_func_manual=globals().get('VOX_FUNC_MANUAL'),
    vox_anat_manual=globals().get('VOX_ANAT_MANUAL'),
    vox_hcr_manual=globals().get('VOX_HCR_MANUAL'),
    flipped_list=globals().get('FLIPPED_LIST'),
    data_root=globals().get('DATA_ROOT'),
    local_root=globals().get('LOCAL_ROOT'),
    nas_root=globals().get('NAS_ROOT'),
    config=VoxelStageConfig(force_recompute_voxels=FORCE_RECOMPUTE_VOXELS),
)
globals().update(voxel_result['bindings'])
for _line in voxel_result['log_lines']:
    print(_line)
try:
    from IPython.display import display as _display
except Exception:
    _display = None
with pd.option_context("display.max_colwidth", 200):
    if _display:
        _display(df_vox)
    else:
        print(df_vox.to_string(index=False))
""",
    "8a": """# [8a] Debug anatomy voxel scale (cache vs header)
from codeants_2pf_hcr import build_voxel_debug_stage

voxel_debug_result = build_voxel_debug_stage(
    anat_stack_path=globals().get('ANAT_STACK_PATH'),
    voxel_cache_path=globals().get('VOX_CACHE_PATH'),
    legacy_voxel_cache_path=globals().get('LEGACY_VOX_CACHE_PATH'),
)
for _line in voxel_debug_result['log_lines']:
    print(_line)
""",
    "10": """# [10]
# 0) Orient functional stack(s) (flip X always; rotate 180 when polarity is north)
from codeants_2pf_hcr import FunctionalOrientationStageConfig, orient_functional_stacks_stage

OVERWRITE_FLIPPED = False

orientation_result = orient_functional_stacks_stage(
    func_nonflipped_list=globals().get('FUNC_NONFLIPPED_LIST'),
    flipped_list=globals().get('FLIPPED_LIST'),
    out_raw=OUT_RAW,
    fish_id=globals().get('FISH_ID'),
    polarity=globals().get('POLARITY'),
    polarity_source=globals().get('POLARITY_SOURCE'),
    resolve_func_polarity_func=globals().get('_resolve_func_polarity'),
    apply_func_orientation_func=globals().get('_apply_func_orientation'),
    config=FunctionalOrientationStageConfig(overwrite_flipped=OVERWRITE_FLIPPED),
)
globals().update(orientation_result['bindings'])
for _line in orientation_result['log_lines']:
    print(_line)
""",
}


def _replace_source(notebook: dict, match_text: str, new_source: str) -> None:
    for cell in notebook["cells"]:
        if cell.get("cell_type") != "code":
            continue
        source = "".join(cell.get("source", []))
        if source.startswith(match_text):
            cell["source"] = [line + "\n" for line in new_source.rstrip("\n").split("\n")]
            return
    raise RuntimeError(f"Notebook cell starting with {match_text!r} not found")


def main() -> None:
    notebook = json.loads(NB_PATH.read_text())
    _replace_source(notebook, "# HCR matching/QC helpers (antsQC-style)", CELL_9)
    for tag, source in REPLACEMENTS.items():
        _replace_source(notebook, f"# [{tag}]", source)
    NB_PATH.write_text(json.dumps(notebook, indent=1))


if __name__ == "__main__":
    main()
