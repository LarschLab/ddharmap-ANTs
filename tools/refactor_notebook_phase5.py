#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path


NB_PATH = Path("notebooks/singleFish.ipynb")


REPLACEMENTS = {
    "4": """# [4]
from codeants_2pf_hcr import ContextStageConfig, resolve_notebook_context_stage

DATA_MODE = 'local'
OWNER = 'Matilde'
FISH_ID = 'L395_f11'
MATCHING_METADATA_CSV_OVERRIDE = None
MANIFEST_OUT_OVERRIDE = None
CELLPOSE_MODEL_ROOT_OVERRIDE = None
ANAT_CP_MODEL_PATH_OVERRIDE = None
CP_HCR_MODEL_PATH_OVERRIDE = None
POLARITY_OVERRIDE = None

context_result = resolve_notebook_context_stage(
    ContextStageConfig(
        fish_id=FISH_ID,
        owner=OWNER,
        data_mode=DATA_MODE,
        matching_metadata_csv_override=MATCHING_METADATA_CSV_OVERRIDE,
        manifest_out_override=MANIFEST_OUT_OVERRIDE,
        cellpose_model_root_override=CELLPOSE_MODEL_ROOT_OVERRIDE,
        anat_cp_model_path_override=ANAT_CP_MODEL_PATH_OVERRIDE,
        cp_hcr_model_path_override=CP_HCR_MODEL_PATH_OVERRIDE,
        polarity_override=POLARITY_OVERRIDE,
    ),
)
CTX = context_result['ctx']
globals().update(context_result['bindings'])
globals().update(context_result['paths'])
RUN_CONFIG = dict(context_result['run_config'])
FUNC_STACK_PATH = FUNC_RAW_STACK_PATH
for _line in context_result['log_lines']:
    print(_line)
""",
    "4a": """# [4a]
from codeants_2pf_hcr import FishStateStageConfig, resolve_fish_state_stage

RESET_FISH_STATE_ALWAYS = True
state_result = resolve_fish_state_stage(
    fish_id=FISH_ID,
    current_state_fish_id=globals().get('STATE_FISH_ID'),
    config=FishStateStageConfig(reset_always=RESET_FISH_STATE_ALWAYS, verbose=True),
)
STATE_FISH_ID = state_result['STATE_FISH_ID']
for _line in state_result['log_lines']:
    print(_line)
""",
    "4b": """# [4b]
from codeants_2pf_hcr import build_run_config_stage

run_config_result = build_run_config_stage(
    base_run_config=dict(CTX.run_config),
    user_run_config=RUN_CONFIG if isinstance(RUN_CONFIG, dict) else {},
)
globals().update(run_config_result['bindings'])
for _line in run_config_result['log_lines']:
    print(_line)
""",
    "4c": """# [4c]
from pathlib import Path
from codeants_2pf_hcr import build_context_audit_stage

AUDIT_STRICT = True
audit_result = build_context_audit_stage(
    fish_id=FISH_ID,
    state_fish_id=globals().get('STATE_FISH_ID'),
    canonical_checks={
        'STATE_FISH_ID': globals().get('STATE_FISH_ID'),
        'FISH_DIR': FISH_DIR,
        'OUTDIR': OUTDIR,
        'OUT_REG': OUT_REG,
        'OUT_QA': OUT_QA,
        'OUT_DERIVED': OUT_DERIVED,
        'MATCHING_METADATA_CSV': MATCHING_METADATA_CSV,
    },
    expected_paths={
        'ANAT_SEG_SOURCE_PATH': {'expected': Path(str(ANAT_STACK_PATH)) if ANAT_STACK_PATH is not None else None, 'current': globals().get('ANAT_SEG_SOURCE_PATH')},
        'ANAT_SEG_OUT_DIR': {'expected': ANALYSIS_DIR / 'structural' / 'cp_masks', 'current': globals().get('ANAT_SEG_OUT_DIR')},
        'ANAT_SEG_CONVERT_DIR': {'expected': ANALYSIS_DIR / 'structural' / 'raw' / 'converted_nrrd_to_tif', 'current': globals().get('ANAT_SEG_CONVERT_DIR')},
    },
    strict=AUDIT_STRICT,
)
fish_audit_df = audit_result['fish_audit_df']
for _line in audit_result['log_lines']:
    print(_line)
display(fish_audit_df)
""",
    "6": """# [6]
import numpy as np
from codeants_2pf_hcr import (
    _find_embedded_nrrd_header,
    _ensure_uint_labels,
    _infer_voxels_from_open_tiff,
    _infer_voxels_nrrd,
    _parse_nrrd_header_text,
    _res_to_um_per_px,
    _to_um,
    apply_func_orientation,
    best_z_by_ncc,
    build_registration_helper_stage,
    corrcoef_img,
    imread_any,
    infer_voxels_tiff,
    local_unsharp,
    load_or_cache_voxels,
    norm01,
    top_correlated_mean,
    zproject_mean,
)

globals().update(build_registration_helper_stage(polarity=POLARITY))
""",
    "99-debug-fish-audit": """# [99-debug-fish-audit]
from codeants_2pf_hcr import FinalFishAuditConfig, build_final_fish_audit_stage

FISH_AUDIT_FINAL_STRICT = False
FISH_AUDIT_MAX_ROWS_PER_COL = 5000
FISH_AUDIT_INCLUDE_INTERNAL_DF = False

audit_result = build_final_fish_audit_stage(
    fish_id=FISH_ID,
    namespace=globals(),
    config=FinalFishAuditConfig(
        strict=FISH_AUDIT_FINAL_STRICT,
        max_rows_per_col=FISH_AUDIT_MAX_ROWS_PER_COL,
        include_internal_df=FISH_AUDIT_INCLUDE_INTERNAL_DF,
    ),
)
FISH_AUDIT_FINAL_REPORT = audit_result['fish_audit_final_report']
fish_audit_final_df = audit_result['fish_audit_final_df']
for _line in audit_result['log_lines']:
    print(_line)
display(fish_audit_final_df)
""",
}


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
    for tag, source in REPLACEMENTS.items():
        _replace_source(notebook, tag, source)
    NB_PATH.write_text(json.dumps(notebook, indent=1))


if __name__ == "__main__":
    main()
