#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path


NB_PATH = Path("notebooks/2PF_to_HCR.ipynb")


REPLACEMENTS = {
    "12": """# [12]
# Load flipped functional data and build/reuse references
import matplotlib.pyplot as plt
import numpy as np

from codeants_2pf_hcr import FunctionalReferenceConfig, build_functional_references_stage

USE_TOP_CORR_REFS = True
TOP_CORR_K = 20
TOP_CORR_SAMPLE = 20
REUSE_SAVED_REFS = True
FORCE_RECOMPUTE_REFS = False

functional_ref_result = build_functional_references_stage(
    flipped_list=globals().get('FLIPPED_LIST'),
    out_raw=OUT_RAW,
    outdir=globals().get('OUTDIR'),
    vox_func_by_path=globals().get('VOX_FUNC_BY_PATH'),
    config=FunctionalReferenceConfig(
        use_top_corr_refs=USE_TOP_CORR_REFS,
        top_corr_k=TOP_CORR_K,
        top_corr_sample=TOP_CORR_SAMPLE,
        reuse_saved_refs=REUSE_SAVED_REFS,
        force_recompute_refs=FORCE_RECOMPUTE_REFS,
    ),
)
globals().update(functional_ref_result['bindings'])
for _line in functional_ref_result['log_lines']:
    print(_line)

for _plane_ref in plane_refs:
    _img = np.asarray(_plane_ref.get('ref2d'))
    if _img.size == 0:
        continue
    _fig, _ax = plt.subplots(figsize=(4, 4))
    _ax.imshow(_img, cmap='gray')
    _ax.set_title(f"Functional reference (norm) [{_plane_ref.get('label', 'unknown')}]")
    _ax.axis('off')
    plt.show()
""",
    "14": """# [14]
# 1.5) Convert anatomy NRRD to TIFF (reorder to Z,X,Y) and use it
from codeants_2pf_hcr import AnatomyNormalizationStageConfig, normalize_anatomy_stack_stage

FORCE_RECOMPUTE_ANAT_CONVERT = False

anatomy_result = normalize_anatomy_stack_stage(
    anat_stack_path=ANAT_STACK_PATH,
    anat_stack_path_orig=globals().get('ANAT_STACK_PATH_ORIG', ANAT_STACK_PATH),
    tmp_convert_dir=globals().get('TMP_CONVERT_DIR'),
    preproc_dir=globals().get('PREPROC_DIR'),
    config=AnatomyNormalizationStageConfig(
        force_recompute_anat_convert=FORCE_RECOMPUTE_ANAT_CONVERT,
    ),
)
globals().update(anatomy_result['bindings'])
for _line in anatomy_result['log_lines']:
    print(_line)
""",
    "20": """# [20]
# Compare in-plane functional->anatomy placement methods without changing the default canonical backend.
from codeants_2pf_hcr import InPlaneRegistrationComparisonConfig, run_in_plane_registration_comparison_stage

INPLANE_REGISTRATION_METHODS = ('ncc_xy', 'ants_rigid_affine')
INPLANE_ACTIVE_METHOD = 'ncc_xy'  # keep canonical behavior until the comparison report is reviewed
INPLANE_SAVE_COMPARISON = True
INPLANE_DISPLAY_NORMALIZE_PLACED = True
INPLANE_FAIL_ON_ACTIVE_METHOD_ERROR = True

placement_result = run_in_plane_registration_comparison_stage(
    plane_refs=globals().get('plane_refs'),
    anat_f=globals().get('anat_f'),
    fish_id=globals().get('FISH_ID'),
    out_ncc=globals().get('OUT_NCC'),
    best_z=globals().get('best_z', 0),
    vox_anat=globals().get('VOX_ANAT'),
    config=InPlaneRegistrationComparisonConfig(
        methods=INPLANE_REGISTRATION_METHODS,
        active_method=INPLANE_ACTIVE_METHOD,
        save_outputs=INPLANE_SAVE_COMPARISON,
        display_normalize_placed=INPLANE_DISPLAY_NORMALIZE_PLACED,
        use_cv2=bool(globals().get('HAS_CV2', False)),
        fail_on_active_method_error=INPLANE_FAIL_ON_ACTIVE_METHOD_ERROR,
    ),
)
globals().update(placement_result['bindings'])
for _line in placement_result['log_lines']:
    print(_line)

try:
    from IPython.display import display as _display
except Exception:
    _display = None

if _display is not None and not placement_result['comparison_df'].empty:
    _display(placement_result['comparison_df'])
elif not placement_result['comparison_df'].empty:
    print(placement_result['comparison_df'].to_string(index=False))
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
    for tag, source in REPLACEMENTS.items():
        _replace_source(notebook, f"# [{tag}]", source)
    NB_PATH.write_text(json.dumps(notebook, indent=1))


if __name__ == "__main__":
    main()
