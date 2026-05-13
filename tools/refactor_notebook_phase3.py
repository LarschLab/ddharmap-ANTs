#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path


NB_PATH = Path("notebooks/singleFish.ipynb")


CELL_51 = """# [51]
try:
    _require_fish_state_51 = require_fish_state
except NameError:
    _require_fish_state_51 = None
if _require_fish_state_51 is not None and (not _require_fish_state_51()):
    raise SystemExit

# Suite2p dF/F traces for HCR-identified cells whose local best responsive functional ROI is exported.
# Reused ROIs are deduplicated within gene before export so fragmented anatomy labels
# do not double-count the same functional trace.
from pathlib import Path

from codeants_2pf_hcr import (
    TraceExportConfig,
    export_suite2p_trace_metadata,
    gene_from_mask,
    resolve_conf_func_csv_analysis,
)

try:
    SUITE2P_BY_REF_IDX = suite2p_by_ref_idx
    SUITE2P_STATE_FISH_ID = SUITE2P_FISH_ID
except NameError:
    print('[Suite2p] Suite2p data not loaded; run [23a] first.')
    raise SystemExit
if SUITE2P_STATE_FISH_ID != FISH_ID:
    print('[Suite2p] Suite2p data from different fish; run [23a].')
    raise SystemExit

try:
    RUN_CONFIG_LOCAL = RUN_CONFIG if isinstance(RUN_CONFIG, dict) else {}
except NameError:
    RUN_CONFIG_LOCAL = {}

try:
    _conf_csv_override = CONF_FUNC_CSV_ANALYSIS
except NameError:
    _conf_csv_override = None

RECOMPUTE_SUITE2P_TRACE_EXPORT = True
try:
    MATCH_POLICY_VERSION = str(HCR_ACTIVITY_MATCH_POLICY)
except NameError:
    MATCH_POLICY_VERSION = 'hcr_anat_first_local_geometry_response_v4_suite2p_gate'

SUITE2P_TRACE_EXPORT_DIR = OUT_DERIVED / 'suite2p_traces'
CONF_FUNC_CSV = resolve_conf_func_csv_analysis(
    out_reg=OUT_REG,
    run_config=RUN_CONFIG_LOCAL,
    conf_func_csv=_conf_csv_override,
    fish_id=FISH_ID,
)

trace_result = export_suite2p_trace_metadata(
    fish_id=str(FISH_ID),
    out_dir=SUITE2P_TRACE_EXPORT_DIR,
    suite2p_by_ref_idx=SUITE2P_BY_REF_IDX,
    conf_func_csv=Path(CONF_FUNC_CSV),
    config=TraceExportConfig(
        recompute=RECOMPUTE_SUITE2P_TRACE_EXPORT,
        match_policy_version=MATCH_POLICY_VERSION,
    ),
    gene_from_mask_func=gene_from_mask,
)

for _line in trace_result.get('log_lines', []):
    print(_line)
print(trace_result['message'])

if trace_result['status'] in {'cached', 'exported'}:
    SUITE2P_DFF_META_CSV = str(trace_result['meta_csv'])
    SUITE2P_DFF_META_DF = trace_result['meta_df']
    try:
        display(SUITE2P_DFF_META_DF.head())
    except Exception:
        print(SUITE2P_DFF_META_DF.head().to_string(index=False))
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
    _replace_source(notebook, "51", CELL_51)
    NB_PATH.write_text(json.dumps(notebook, indent=1))


if __name__ == "__main__":
    main()
