#!/usr/bin/env python3
from __future__ import annotations

import json
import re
from pathlib import Path


NB_PATH = Path("notebooks/singleFish.ipynb")


CELL_50IA = """# [50ia]
try:
    REQUIRE_FISH_STATE = require_fish_state
except NameError:
    REQUIRE_FISH_STATE = None
if callable(REQUIRE_FISH_STATE) and not REQUIRE_FISH_STATE():
    raise SystemExit

import numpy as np
import pandas as pd
from pathlib import Path

from codeants_2pf_hcr import ActivityConfig, build_response_bpi_tables

ACTIVE_CLASS = 'Active neurons'
INACTIVE_CLASS = 'Low-quality traces'
RESPONSE_LOW = 'low activity'
RESPONSE_UNAVAILABLE = 'response unavailable'

try:
    RUN_CONFIG_LOCAL = RUN_CONFIG if isinstance(RUN_CONFIG, dict) else {}
except NameError:
    RUN_CONFIG_LOCAL = {}
try:
    FISH_DIR_LOCAL = Path(FISH_DIR) if FISH_DIR is not None else None
except NameError:
    FISH_DIR_LOCAL = None
try:
    FISH_ID_LOCAL = FISH_ID
except NameError:
    FISH_ID_LOCAL = None

FUNC_ACTIVITY_BPI_ZERO_BAND = float(RUN_CONFIG_LOCAL.get('FUNC_ACTIVITY_BPI_ZERO_BAND', 0.10))
FUNC_ACTIVITY_BPI_MIN_TRIALS_PER_CLASS = int(RUN_CONFIG_LOCAL.get('FUNC_ACTIVITY_BPI_MIN_TRIALS_PER_CLASS', 3))
FUNC_ACTIVITY_BPI_DENOM_EPS = float(RUN_CONFIG_LOCAL.get('FUNC_ACTIVITY_BPI_DENOM_EPS', 1e-6))
FUNC_ACTIVITY_BPI_EDGE_POLICY = str(RUN_CONFIG_LOCAL.get('FUNC_ACTIVITY_BPI_EDGE_POLICY', 'pad_nan'))
FUNC_ACTIVITY_BPI_MIN_VALID_FRAC = float(RUN_CONFIG_LOCAL.get('FUNC_ACTIVITY_BPI_MIN_VALID_FRAC', 0.5))
FUNC_ACTIVITY_BPI_STIM_ONSET_DELAY_SEC = float(RUN_CONFIG_LOCAL.get('STIM_ONSET_DELAY_SEC', 10.0))
FUNC_RESPONSE_MIN_AUC = float(RUN_CONFIG_LOCAL.get('FUNC_RESPONSE_MIN_AUC', 0.05))
FUNC_RESPONSE_NULL_Q = float(RUN_CONFIG_LOCAL.get('FUNC_RESPONSE_NULL_Q', 0.99))
FUNC_RESPONSE_NULL_BOOTSTRAP_N = int(RUN_CONFIG_LOCAL.get('FUNC_RESPONSE_NULL_BOOTSTRAP_N', 2000))
FUNC_RESPONSE_NULL_MIN_WINDOWS = int(RUN_CONFIG_LOCAL.get('FUNC_RESPONSE_NULL_MIN_WINDOWS', 20))
FUNC_RESPONSE_NULL_STEP_SEC = float(RUN_CONFIG_LOCAL.get('FUNC_RESPONSE_NULL_STEP_SEC', 0.5))
FUNC_RESPONSE_RNG_SEED = int(RUN_CONFIG_LOCAL.get('FUNC_RESPONSE_RNG_SEED', 50))
FUNC_ACTIVITY_BPI_STIM_TIME_SCALE = 1.0
FUNC_ACTIVITY_BPI_MEASURE_START_BLOCK = 1
FUNC_ACTIVITY_BPI_MEASURE_START_EVENT = 'start'
FUNC_ACTIVITY_BPI_REMOVE_INTERBLOCK_GAPS = bool(RUN_CONFIG_LOCAL.get('REMOVE_INTERBLOCK_GAPS', True))
FUNC_ACTIVITY_FORCE_RECOMPUTE = bool(RUN_CONFIG_LOCAL.get('FUNC_ACTIVITY_FORCE_RECOMPUTE', True))
FUNC_ACTIVITY_BPI_CELLS_CSV = OUT_REG / 'functional_roi_activity_bpi_cells.csv'
FUNC_ACTIVITY_BPI_SUMMARY_CSV = OUT_REG / 'functional_roi_activity_bpi_summary.csv'
FUNC_ACTIVITY_BPI_DETAIL_CSV = OUT_REG / 'functional_roi_activity_identity.csv'
FUNC_ACTIVITY_BPI_SUITE2P_ROOT = OUTDIR / 'suite2P'

detail_csv = FUNC_ACTIVITY_BPI_DETAIL_CSV
if not detail_csv.exists():
    print(f'[50ia] Missing ROI identity table: {detail_csv}. Run [50i] first.')
else:
    detail_df = pd.read_csv(detail_csv)
    skip_computation = False
    if (not FUNC_ACTIVITY_FORCE_RECOMPUTE) and FUNC_ACTIVITY_BPI_CELLS_CSV.exists() and FUNC_ACTIVITY_BPI_SUMMARY_CSV.exists():
        print('[50ia] BPI output CSVs exist and FUNC_ACTIVITY_FORCE_RECOMPUTE=False, skipping computation.')
        skip_computation = True

    if skip_computation:
        FUNC_ACTIVITY_IDENTITY_DF = detail_df.copy()
        FUNC_ACTIVITY_BPI_DF = pd.read_csv(FUNC_ACTIVITY_BPI_CELLS_CSV)
        FUNC_ACTIVITY_BPI_SUMMARY_DF = pd.read_csv(FUNC_ACTIVITY_BPI_SUMMARY_CSV)
        FUNC_ACTIVITY_IDENTITY_CSV = detail_csv
        FUNC_ACTIVITY_BPI_FISH_ID = FISH_ID_LOCAL
    else:
        if FISH_DIR_LOCAL is None:
            raise RuntimeError('[50ia] FISH_DIR is not available.')
        activity_result = build_response_bpi_tables(
            detail_df,
            fish_dir=FISH_DIR_LOCAL,
            fish_id=str(FISH_ID_LOCAL),
            suite2p_root=FUNC_ACTIVITY_BPI_SUITE2P_ROOT,
            config=ActivityConfig(
                active_class=ACTIVE_CLASS,
                inactive_class=INACTIVE_CLASS,
                zero_band=FUNC_ACTIVITY_BPI_ZERO_BAND,
                min_trials_per_class=FUNC_ACTIVITY_BPI_MIN_TRIALS_PER_CLASS,
                denom_eps=FUNC_ACTIVITY_BPI_DENOM_EPS,
                edge_policy=FUNC_ACTIVITY_BPI_EDGE_POLICY,
                min_valid_frac=FUNC_ACTIVITY_BPI_MIN_VALID_FRAC,
                stim_onset_delay_sec=FUNC_ACTIVITY_BPI_STIM_ONSET_DELAY_SEC,
                response_min_auc=FUNC_RESPONSE_MIN_AUC,
                response_null_q=FUNC_RESPONSE_NULL_Q,
                response_null_bootstrap_n=FUNC_RESPONSE_NULL_BOOTSTRAP_N,
                response_null_min_windows=FUNC_RESPONSE_NULL_MIN_WINDOWS,
                response_null_step_sec=FUNC_RESPONSE_NULL_STEP_SEC,
                response_rng_seed=FUNC_RESPONSE_RNG_SEED,
                stim_time_scale=FUNC_ACTIVITY_BPI_STIM_TIME_SCALE,
                measure_start_block=FUNC_ACTIVITY_BPI_MEASURE_START_BLOCK,
                measure_start_event=FUNC_ACTIVITY_BPI_MEASURE_START_EVENT,
                remove_interblock_gaps=FUNC_ACTIVITY_BPI_REMOVE_INTERBLOCK_GAPS,
            ),
        )

        detail_df = activity_result['detail_df']
        scored_bpi_df = activity_result['scored_bpi_df']
        summary_df = activity_result['summary_df']
        stim_source = activity_result['stim_source']
        stim_events = activity_result['stim_events']

        detail_csv.parent.mkdir(parents=True, exist_ok=True)
        detail_df.to_csv(detail_csv, index=False)
        scored_bpi_df.to_csv(FUNC_ACTIVITY_BPI_CELLS_CSV, index=False)
        summary_df.to_csv(FUNC_ACTIVITY_BPI_SUMMARY_CSV, index=False)

        FUNC_ACTIVITY_IDENTITY_DF = detail_df.copy()
        FUNC_ACTIVITY_BPI_DF = scored_bpi_df.copy()
        FUNC_ACTIVITY_BPI_SUMMARY_DF = summary_df.copy()
        FUNC_ACTIVITY_IDENTITY_CSV = detail_csv
        FUNC_ACTIVITY_BPI_FISH_ID = FISH_ID_LOCAL

        n_responsive = int(detail_df['response_is_active'].sum())
        n_low = int((detail_df['response_class'] == RESPONSE_LOW).sum())
        n_unavailable = int((detail_df['response_class'] == RESPONSE_UNAVAILABLE).sum())
        n_low_quality = int((~pd.Series(detail_df.get('suite2p_is_cell', False)).fillna(False).astype(bool)).sum())
        print(f'[50ia] Response/BPI categories assigned for all segmented ROIs using {len(stim_events)} stimulus windows from {stim_source}.')
        print(
            f'[50ia] thresholds: mean AUC >= {FUNC_RESPONSE_MIN_AUC:.3f} dF/F·s and '
            f'> Q{100.0 * FUNC_RESPONSE_NULL_Q:.0f} baseline null; |BPI| <= {FUNC_ACTIVITY_BPI_ZERO_BAND:.2f} -> both-responsive'
        )
        print(f'[50ia] segmented ROIs={len(detail_df)}; responsive={n_responsive}; low activity={n_low}; unavailable={n_unavailable}; low-quality traces={n_low_quality}')
        print(f'[50ia] scored ROI table -> {FUNC_ACTIVITY_BPI_CELLS_CSV}')
        print(f'[50ia] summary -> {FUNC_ACTIVITY_BPI_SUMMARY_CSV}')
        try:
            display(summary_df)
        except Exception:
            print(summary_df.to_string(index=False))
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


def _transform_source(notebook: dict, tag: str, fn) -> None:
    for cell in notebook["cells"]:
        if cell.get("cell_type") != "code":
            continue
        src = "".join(cell.get("source", []))
        if src.startswith(f"# [{tag}]"):
            updated = fn(src)
            cell["source"] = [line + "\n" for line in updated.rstrip("\n").split("\n")]
            return
    raise RuntimeError(f"Notebook cell [{tag}] not found")


def _patch_cell_50i(src: str) -> str:
    src = src.replace(
        "import numpy as np\nimport pandas as pd\nfrom pathlib import Path\n",
        "import numpy as np\nimport pandas as pd\nfrom pathlib import Path\nfrom codeants_2pf_hcr import build_anat_identity_lookup_df, build_functional_roi_master_df, gene_from_mask\n",
    )
    src = src.replace(
        "try:\n    BUILD_FUNCTIONAL_ROI_MASTER_DF = build_functional_roi_master_df\nexcept NameError:\n    BUILD_FUNCTIONAL_ROI_MASTER_DF = None\n\ntry:\n    BUILD_ANAT_IDENTITY_LOOKUP_DF = build_anat_identity_lookup_df\nexcept NameError:\n    BUILD_ANAT_IDENTITY_LOOKUP_DF = None\n\ntry:\n    GENE_FROM_MASK_FUNC = gene_from_mask\nexcept NameError:\n    GENE_FROM_MASK_FUNC = None\n",
        "BUILD_FUNCTIONAL_ROI_MASTER_DF = build_functional_roi_master_df\nBUILD_ANAT_IDENTITY_LOOKUP_DF = build_anat_identity_lookup_df\nGENE_FROM_MASK_FUNC = gene_from_mask\n",
    )
    src = src.replace(
        "        claim_matched=CLAIM_MATCHED,\n        claim_unmatched=CLAIM_UNMATCHED,\n    )\n",
        "        claim_matched=CLAIM_MATCHED,\n        claim_unmatched=CLAIM_UNMATCHED,\n        ensure_uint_labels_func=_ensure_uint_labels,\n        apply_func_orientation_func=_apply_func_orientation,\n        tform_for_plane_func=_tform_for_plane,\n        resample_labels_nn_func=resample_labels_nn,\n    )\n",
    )
    return src


def _patch_cell_50(src: str) -> str:
    src = src.replace(
        "import numpy as np\nimport pandas as pd\nfrom pathlib import Path\n",
        "import numpy as np\nimport pandas as pd\nfrom pathlib import Path\nfrom codeants_2pf_hcr import build_hcr_activity_tables, gene_from_mask\n",
    )
    src = src.replace(
        "try:\n    BUILD_HCR_ACTIVITY_TABLES = build_hcr_activity_tables\nexcept NameError:\n    BUILD_HCR_ACTIVITY_TABLES = None\n",
        "BUILD_HCR_ACTIVITY_TABLES = build_hcr_activity_tables\n",
    )
    src = src.replace(
        "try:\n    GENE_FROM_MASK_FUNC = gene_from_mask\nexcept NameError:\n    GENE_FROM_MASK_FUNC = None\n",
        "GENE_FROM_MASK_FUNC = gene_from_mask\n",
    )
    src = src.replace(
        "        selection_rule=SELECTION_RULE,\n        gene_from_mask_func=GENE_FROM_MASK_FUNC,\n    )\n",
        "        selection_rule=SELECTION_RULE,\n        gene_from_mask_func=GENE_FROM_MASK_FUNC,\n        ensure_uint_labels_func=_ensure_uint_labels,\n        apply_func_orientation_func=_apply_func_orientation,\n        tform_for_plane_func=_tform_for_plane,\n        resample_labels_nn_func=resample_labels_nn,\n    )\n",
    )
    return src


def _patch_cell_55(src: str) -> str:
    needle = "df_evt = stim_ctx['df_evt']\ndf_stim = stim_ctx['df_stim']\n"
    repl = "df_evt = stim_ctx['df_evt']\ndf_stim = stim_ctx['df_stim']\nDF_STIM_FISH_ID = FISH_ID\n"
    return src.replace(needle, repl)


def _patch_cell_56(src: str) -> str:
    src = src.replace(
        "import json\nfrom pathlib import Path\n\n\n",
        "import json\nfrom pathlib import Path\nfrom codeants_2pf_hcr import prepare_pairs_for_unique_cells\nfrom codeants_2pf_hcr.stimulus import build_prestim_baseline_windows, compute_zscore_stats, effective_motion_window\n\n\n",
    )
    pattern = re.compile(
        r"def _build_prestim_baseline_windows\(.*?def _filter_pairs_high_conf_generic",
        re.DOTALL,
    )
    repl = (
        "_build_prestim_baseline_windows = build_prestim_baseline_windows\n"
        "_compute_zscore_stats = compute_zscore_stats\n"
        "_effective_motion_window = effective_motion_window\n"
        "_prepare_pairs_for_unique_cells = prepare_pairs_for_unique_cells\n\n\n"
        "def _filter_pairs_high_conf_generic"
    )
    return re.sub(pattern, repl, src, count=1)


def _patch_cell_56h(src: str) -> str:
    src = src.replace(
        "import json\nfrom pathlib import Path\nfrom skimage.measure import regionprops_table\nfrom matplotlib.patches import Patch\n",
        "import json\nfrom pathlib import Path\nfrom skimage.measure import regionprops_table\nfrom matplotlib.patches import Patch\nfrom codeants_2pf_hcr import prepare_pairs_for_unique_cells\nfrom codeants_2pf_hcr.stimulus import build_prestim_baseline_windows, combine_segments, compute_zscore_stats, effective_motion_window\n",
    )
    pattern = re.compile(
        r"def _build_prestim_baseline_windows_local\(.*?def _filter_pairs_high_conf_generic",
        re.DOTALL,
    )
    repl = (
        "_build_prestim_baseline_windows_local = build_prestim_baseline_windows\n"
        "_combine_segments = combine_segments\n"
        "_compute_zscore_stats_local = compute_zscore_stats\n"
        "_effective_motion_window_local = effective_motion_window\n"
        "_prepare_pairs_for_unique_cells_local = prepare_pairs_for_unique_cells\n\n\n"
        "def _filter_pairs_high_conf_generic"
    )
    src = re.sub(pattern, repl, src, count=1)
    src = src.replace(
        "        summary_table = globals().get('hcr_match_summary_table', None)\n",
        "        summary_table = None\n",
    )
    return src


def _patch_cell_57(src: str) -> str:
    src = src.replace(
        "import matplotlib.pyplot as plt\nimport re\nfrom pathlib import Path\n",
        "import matplotlib.pyplot as plt\nfrom pathlib import Path\nfrom codeants_2pf_hcr import prepare_pairs_for_unique_cells\nfrom codeants_2pf_hcr.stimulus import effective_motion_window\n",
    )
    pattern = re.compile(r"def _effective_motion_span_57\(.*?if _df_stim_57 is None or _df_stim_57.empty:", re.DOTALL)
    repl = (
        "def _effective_motion_span_57(row, onset_delay_sec):\n"
        "    t0, t1, _ = effective_motion_window(\n"
        "        row.get('start', np.nan),\n"
        "        row.get('duration', np.nan),\n"
        "        row.get('end', np.nan),\n"
        "        onset_delay_sec,\n"
        "    )\n"
        "    return t0, t1\n\n"
        "if _df_stim_57 is None or _df_stim_57.empty:"
    )
    src = re.sub(pattern, repl, src, count=1)
    src = src.replace(
        "            pairs = pairs[pairs['func_label'].notna() & pairs['plane'].notna()].copy()\n",
        "            pairs = prepare_pairs_for_unique_cells(pairs, strict=False, tag='[57]').copy()\n",
    )
    return src


def main() -> None:
    notebook = json.loads(NB_PATH.read_text())
    _transform_source(notebook, "55", _patch_cell_55)
    _transform_source(notebook, "50i", _patch_cell_50i)
    _replace_source(notebook, "50ia", CELL_50IA)
    _transform_source(notebook, "50", _patch_cell_50)
    _transform_source(notebook, "56", _patch_cell_56)
    _transform_source(notebook, "56h", _patch_cell_56h)
    _transform_source(notebook, "57", _patch_cell_57)
    NB_PATH.write_text(json.dumps(notebook, indent=1))


if __name__ == "__main__":
    main()
