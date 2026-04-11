#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path


NB_PATH = Path("notebooks/2PF_to_HCR.ipynb")


REPLACEMENTS = {
    "16": """# [16]
try:
    REQUIRE_FISH_STATE = require_fish_state
except NameError:
    REQUIRE_FISH_STATE = None
if callable(REQUIRE_FISH_STATE) and not REQUIRE_FISH_STATE():
    raise SystemExit
# Per-plane sweep-based scaling + NCC matching (threaded)
import os

import matplotlib.pyplot as plt
import pandas as pd

from codeants_2pf_hcr import RegistrationSearchConfig, run_registration_search_stage

RESCALE_FUNC_TO_ANAT = True
FORCE_RECOMPUTE = False
NCC_MANUAL_SCALE = 1.0
NCC_SCALE_METRIC = 'max_score'  # peak_zscore|peak_delta|max_score
NCC_SCALE_COARSE = (0.70, 1.00, 0.05)
NCC_SCALE_FINE = (0.05, 0.01)  # (half_window, step)
NCC_SCALE_XFINE = (0.005, 0.001)  # (half_window, step)
NCC_SCALE_UFINE = (0.0005, 0.0001)  # (half_window, step)
NCC_SCALE_REFINE_ONLY = False
NCC_MANUAL_Z_LIMIT = 0
NCC_SCALE_PER_PLANE = True
NCC_SCALE_BACKEND = 'threads'
NCC_SCALE_WORKERS = max(1, min(len(plane_refs), os.cpu_count() or 1))
NCC_SCALE_CENTER_REL_TOL = 0.0
NCC_SCALE_CENTER_ABS_TOL = 0.0
NCC_SCALE_CENTER_PREFER_HIGH = False

registration_result = run_registration_search_stage(
    anat_stack_path=ANAT_STACK_PATH,
    plane_refs=plane_refs,
    fish_id=FISH_ID,
    out_ncc=OUT_NCC,
    vox_anat=globals().get('VOX_ANAT'),
    vox_func=globals().get('VOX_FUNC'),
    config=RegistrationSearchConfig(
        rescale_func_to_anat=RESCALE_FUNC_TO_ANAT,
        force_recompute=FORCE_RECOMPUTE,
        manual_scale=NCC_MANUAL_SCALE,
        scale_metric=NCC_SCALE_METRIC,
        scale_coarse=NCC_SCALE_COARSE,
        scale_fine=NCC_SCALE_FINE,
        scale_xfine=NCC_SCALE_XFINE,
        scale_ufine=NCC_SCALE_UFINE,
        scale_refine_only=NCC_SCALE_REFINE_ONLY,
        manual_z_limit=NCC_MANUAL_Z_LIMIT,
        scale_per_plane=NCC_SCALE_PER_PLANE,
        scale_backend=NCC_SCALE_BACKEND,
        scale_workers=NCC_SCALE_WORKERS,
        scale_center_rel_tol=NCC_SCALE_CENTER_REL_TOL,
        scale_center_abs_tol=NCC_SCALE_CENTER_ABS_TOL,
        scale_center_prefer_high=NCC_SCALE_CENTER_PREFER_HIGH,
        use_cv2=bool(globals().get('HAS_CV2', False)),
    ),
)
globals().update(registration_result['bindings'])
for _line in registration_result['log_lines']:
    print(_line)

if registration_result['scores_by_plane']:
    plt.figure(figsize=(10, 4))
    for _lbl, _scores in registration_result['scores_by_plane'].items():
        _sc_scale = registration_result['scale_by_plane'].get(_lbl, float('nan'))
        plt.plot(_scores, label=f"{_lbl} (s={_sc_scale:.4f})")
    plt.title('NCC curves (per-plane scales)')
    plt.xlabel('Z index')
    plt.ylabel('NCC score')
    plt.legend(fontsize=7, ncol=2)
    plt.tight_layout()
    plt.show()

try:
    from IPython.display import display as _display
except Exception:
    _display = None

if _display:
    _display(df)
else:
    print(df.to_string(index=False))
""",
    "22": """# [22]
# Interactive overlay: toggle channels like FIJI (all planes side-by-side)
from codeants_2pf_hcr import show_registration_overlay_stage

overlay_result = show_registration_overlay_stage(
    plane_refs=globals().get('plane_refs'),
    anat=globals().get('anat'),
    best_z=globals().get('best_z', 0),
    apply_transform_2d_func=globals().get('apply_transform_2d'),
)
for _line in overlay_result['log_lines']:
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
    for tag, source in REPLACEMENTS.items():
        _replace_source(notebook, f"# [{tag}]", source)
    NB_PATH.write_text(json.dumps(notebook, indent=1))


if __name__ == "__main__":
    main()
