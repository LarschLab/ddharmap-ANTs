#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path


NB_PATH = Path("notebooks/2PF_to_HCR.ipynb")


REPLACEMENTS = {
    "24": """# [24]
# Cellpose segmentation on HCR intensity stacks (3D)
# Uses rbest/rn intensity stacks; skips fullBrain and channel1 (GCaMP).

from codeants_2pf_hcr import HcrCellposeConfig, run_hcr_cellpose_stage

CP_SAVE_TIF = True
CP_SKIP_IF_EXISTS = True
CP_USE_ANISOTROPY = True
try:
    CP_ANISOTROPY_OVERRIDE = CP_ANISOTROPY_OVERRIDE
except NameError:
    CP_ANISOTROPY_OVERRIDE = None
try:
    CP_MODEL_PATH_OVERRIDE = CP_MODEL_PATH_OVERRIDE
except NameError:
    CP_MODEL_PATH_OVERRIDE = None

cellpose_result = run_hcr_cellpose_stage(
    fish_dir=FISH_DIR,
    preproc_dir=PREPROC_DIR,
    hcr_intensity_paths=globals().get('HCR_INTENSITY_PATHS'),
    cp_hcr_model_path=globals().get('CP_HCR_MODEL_PATH'),
    cp_hcr_model_path_default=globals().get('CP_HCR_MODEL_PATH_DEFAULT'),
    data_mode=globals().get('DATA_MODE', 'nas'),
    data_root=globals().get('DATA_ROOT', globals().get('LOCAL_ROOT', globals().get('NAS_ROOT'))),
    local_root=globals().get('LOCAL_ROOT'),
    nas_root=globals().get('NAS_ROOT'),
    config=HcrCellposeConfig(
        save_tif=CP_SAVE_TIF,
        skip_if_exists=CP_SKIP_IF_EXISTS,
        use_anisotropy=CP_USE_ANISOTROPY,
        anisotropy_override=CP_ANISOTROPY_OVERRIDE,
        model_path_override=CP_MODEL_PATH_OVERRIDE,
        data_mode=globals().get('DATA_MODE', 'nas'),
    ),
)
globals().update(cellpose_result['bindings'])
for _line in cellpose_result['log_lines']:
    print(_line)
""",
    "26": """# [26]
# Plot functional labels on top of each functional reference (per-plane)

from codeants_2pf_hcr import show_functional_label_overlay_stage

label_overlay_result = show_functional_label_overlay_stage(
    plane_refs=globals().get('plane_refs'),
    use_suite2p_labels=bool(globals().get('USE_SUITE2P_LABELS', False)),
    func_labels=globals().get('func_labels'),
    out_seg=globals().get('OUT_SEG'),
    func_labels_path=globals().get('FUNC_LABELS_PATH'),
    apply_func_orientation_func=globals().get('apply_func_orientation'),
    imread_func=globals().get('imread'),
)
for _line in label_overlay_result['log_lines']:
    print(_line)
""",
    "26a": """# [26a]
# Export native Suite2p label TIFFs for QA against the saved functional references

from codeants_2pf_hcr import export_suite2p_native_labels_stage

suite2p_label_export = export_suite2p_native_labels_stage(
    plane_refs=globals().get('plane_refs'),
    suite2p_by_ref_idx=globals().get('suite2p_by_ref_idx'),
    out_qa=OUT_QA,
    out_raw=OUT_RAW,
    outdir=OUTDIR,
    func_labels=globals().get('func_labels'),
)
SUITE2P_LABEL_EXPORT_DIR = suite2p_label_export['export_dir']
SUITE2P_LABEL_EXPORT_MANIFEST_CSV = suite2p_label_export.get('manifest_csv')
SUITE2P_LABEL_EXPORT_DF = suite2p_label_export['manifest_df']
for _line in suite2p_label_export['log_lines']:
    print(_line)
display(SUITE2P_LABEL_EXPORT_DF)
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
