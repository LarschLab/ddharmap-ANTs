from __future__ import annotations

import json
from pathlib import Path


NOTEBOOK_PATH = Path("notebooks/2PF_to_HCR.ipynb")


def _rewrite_56g(source: str) -> str:
    source = source.replace(
        "import matplotlib.pyplot as plt\nfrom pathlib import Path\n",
        "import matplotlib.pyplot as plt\nfrom pathlib import Path\nfrom codeants_2pf_hcr import SingleFishBpiDiagnosticsConfig, prepare_single_fish_bpi_diagnostics_stage\n",
    )
    source = source.replace("\n_response_active_truthy = {'1', 'true', 't', 'yes', 'y'}\n", "\n")

    old_prep = """if bpi_cells_df_local is None or bpi_cells_df_local.empty:
    print('[56g] bpi_cells_df missing/empty; run [56h] first.')
else:
    df = bpi_cells_df_local.copy()
    if 'fish_id' in df.columns:
        df = df[df['fish_id'].astype(str) == str(FISH_ID)].copy()
        if df.empty:
            print('[56g] bpi_cells_df has no rows for current fish; run [56h].')
            raise SystemExit
    if 'gene' not in df.columns:
        raise RuntimeError('[56g] bpi_cells_df missing required column: gene')
    if 'plane_idx' not in df.columns and 'plane' in df.columns:
        df['plane_idx'] = pd.to_numeric(df['plane'], errors='coerce').astype('Int64')
    if 'plane_idx' not in df.columns or 'func_label' not in df.columns:
        raise RuntimeError('[56g] bpi_cells_df missing plane_idx/func_label required for response-aware low-activity annotation.')

    if {'mean_bout_zdff', 'mean_cont_zdff'}.issubset(df.columns):
        bout_col = 'mean_bout_zdff'
        cont_col = 'mean_cont_zdff'
        activity_label = 'z-scored dF/F'
    elif {'mean_bout_dff', 'mean_cont_dff'}.issubset(df.columns):
        bout_col = 'mean_bout_dff'
        cont_col = 'mean_cont_dff'
        activity_label = 'dF/F'
        print('[56g] WARNING: z-scored response columns missing in bpi_cells_df; falling back to raw dF/F.')
    else:
        raise RuntimeError('[56g] bpi_cells_df missing both z-scored and raw bout/continuous response columns.')

    if BPI_INDEX_COL not in df.columns:
        fallback_bpi = 'bpi_z' if 'bpi_z' in df.columns else 'bpi'
        print(f"[56g] BPI_INDEX_COL={BPI_INDEX_COL} missing; using {fallback_bpi}")
        BPI_INDEX_COL = fallback_bpi

    for col in [BPI_INDEX_COL, bout_col, cont_col]:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df['activity_mag'] = (df[bout_col].abs() + df[cont_col].abs()) / 2.0
    df['bpi_metric'] = df[BPI_INDEX_COL]
    df['abs_bpi'] = df['bpi_metric'].abs()
    df = df[
        np.isfinite(df['activity_mag'])
        & np.isfinite(df['bpi_metric'])
        & np.isfinite(df[bout_col])
        & np.isfinite(df[cont_col])
    ].copy()

    if df.empty:
        print('[56g] no finite cells after filtering.')
    else:
        need_lookup = ('response_class' not in df.columns) or ('response_summary_class' not in df.columns)
        if need_lookup:
            if not MASTER_DETAIL_CSV.exists():
                raise RuntimeError(f'[56g] Missing response-aware ROI table: {MASTER_DETAIL_CSV}. Run [50ia] first.')
            detail_df = pd.read_csv(MASTER_DETAIL_CSV)
            if 'fish_id' in detail_df.columns:
                detail_df = detail_df[detail_df['fish_id'].astype(str) == str(FISH_ID)].copy()
            required_detail_cols = {'plane_idx', 'func_label', 'response_class', 'response_summary_class', 'response_is_active'}
            missing_detail_cols = sorted(required_detail_cols - set(detail_df.columns))
            if missing_detail_cols:
                raise RuntimeError(f'[56g] Master ROI table missing response columns {missing_detail_cols}; rerun [50ia].')
            prior_response_class = df.get('response_class', pd.Series(pd.NA, index=df.index, dtype='object')).copy()
            prior_response_summary_class = df.get('response_summary_class', pd.Series(pd.NA, index=df.index, dtype='object')).copy()
            prior_response_is_active = df.get('response_is_active', pd.Series(pd.NA, index=df.index, dtype='object')).copy()
            detail_lookup = detail_df[['plane_idx', 'func_label', 'response_class', 'response_summary_class', 'response_is_active']].copy()
            detail_lookup['plane_idx_key'] = pd.to_numeric(detail_lookup['plane_idx'], errors='coerce').astype('Int64')
            detail_lookup['func_label_key'] = pd.to_numeric(detail_lookup['func_label'], errors='coerce').astype('Int64')
            detail_lookup = (
                detail_lookup
                .drop(columns=['plane_idx', 'func_label'])
                .drop_duplicates(subset=['plane_idx_key', 'func_label_key'], keep='last')
                .reset_index(drop=True)
            )
            detail_lookup = detail_lookup.rename(columns={
                'response_class': '_lookup_response_class',
                'response_summary_class': '_lookup_response_summary_class',
                'response_is_active': '_lookup_response_is_active',
            })
            df['plane_idx_key'] = pd.to_numeric(df['plane_idx'], errors='coerce').astype('Int64')
            df['func_label_key'] = pd.to_numeric(df['func_label'], errors='coerce').astype('Int64')
            df = df.merge(detail_lookup, on=['plane_idx_key', 'func_label_key'], how='left')
            lookup_response_class = df.get('_lookup_response_class', pd.Series(pd.NA, index=df.index, dtype='object'))
            lookup_response_summary_class = df.get('_lookup_response_summary_class', pd.Series(pd.NA, index=df.index, dtype='object'))
            lookup_response_is_active = df.get('_lookup_response_is_active', pd.Series(pd.NA, index=df.index, dtype='object'))
            df['response_class'] = lookup_response_class.where(lookup_response_class.notna(), prior_response_class)
            df['response_summary_class'] = lookup_response_summary_class.where(lookup_response_summary_class.notna(), prior_response_summary_class)
            df['response_is_active'] = lookup_response_is_active.where(lookup_response_is_active.notna(), prior_response_is_active)
            df = df.drop(columns=[
                c for c in ['_lookup_response_class', '_lookup_response_summary_class', '_lookup_response_is_active']
                if c in df.columns
            ])

        df['response_class'] = df['response_class'].fillna(RESPONSE_UNAVAILABLE).astype(str)
        if 'response_summary_class' not in df.columns:
            df['response_summary_class'] = np.where(
                df['response_class'].isin({'bout-responsive', 'continuous-responsive', 'both-responsive'}),
                RESPONSE_SUMMARY_RESPONSIVE,
                np.where(df['response_class'].eq(RESPONSE_LOW), RESPONSE_SUMMARY_LOW, RESPONSE_SUMMARY_UNAVAILABLE),
            )
        else:
            df['response_summary_class'] = df['response_summary_class'].fillna(RESPONSE_SUMMARY_UNAVAILABLE).astype(str)
        if 'response_is_active' in df.columns:
            _response_active_series = df['response_is_active']
            _response_active_numeric = pd.to_numeric(_response_active_series, errors='coerce')
            df['response_is_active'] = np.where(
                _response_active_series.isna(),
                False,
                np.where(
                    _response_active_numeric.notna(),
                    _response_active_numeric.astype(float) != 0.0,
                    _response_active_series.astype(str).str.strip().str.lower().isin(_response_active_truthy),
                ),
            ).astype(bool)
        else:
            df['response_is_active'] = df['response_summary_class'].eq(RESPONSE_SUMMARY_RESPONSIVE)

        df['is_bpi_near_zero'] = df['bpi_metric'].abs() <= float(BPI_ZERO_BAND)
        df['is_low_activity'] = df['response_summary_class'].eq(RESPONSE_SUMMARY_LOW)
        df['is_responsive'] = df['response_summary_class'].eq(RESPONSE_SUMMARY_RESPONSIVE)
        df['is_response_unavailable'] = df['response_summary_class'].eq(RESPONSE_SUMMARY_UNAVAILABLE)
        df['interpretation'] = np.select(
            [
                df['is_bpi_near_zero'] & df['is_low_activity'],
                df['is_bpi_near_zero'] & df['is_responsive'],
                df['is_bpi_near_zero'] & df['is_response_unavailable'],
            ],
            [
                'near-zero BPI + low activity',
                'near-zero BPI + responsive',
                'near-zero BPI + response unavailable',
            ],
            default='non-zero BPI',
        )

        n_total = int(len(df))
        n_nz = int(df['is_bpi_near_zero'].sum())
        n_nz_low = int((df['is_bpi_near_zero'] & df['is_low_activity']).sum())
        n_nz_resp = int((df['is_bpi_near_zero'] & df['is_responsive']).sum())
        n_nz_unavailable = int((df['is_bpi_near_zero'] & df['is_response_unavailable']).sum())

        print(
            f"[56g] cells={n_total}; BPI column={BPI_INDEX_COL}; activity columns=({bout_col}, {cont_col}); "
            f"|BPI|<= {BPI_ZERO_BAND:.3f}: {n_nz}; near-zero+low-activity={n_nz_low}; "
            f"near-zero+responsive={n_nz_resp}; near-zero+response-unavailable={n_nz_unavailable}; "
            'low-activity source=response_summary_class'
        )

        genes_present = sorted(df['gene'].dropna().astype(str).unique().tolist())
        gene_order = [g for g in GENE_ORDER if g in genes_present] + [g for g in genes_present if g not in GENE_ORDER]

        fig, axes = plt.subplots(2, 2, figsize=(14, 10.5))
"""
    new_prep = """if bpi_cells_df_local is None or bpi_cells_df_local.empty:
    print('[56g] bpi_cells_df missing/empty; run [56h] first.')
else:
    try:
        _bpi_diag_56g = prepare_single_fish_bpi_diagnostics_stage(
            bpi_cells_df_local,
            fish_id=str(FISH_ID),
            master_detail_csv=MASTER_DETAIL_CSV,
            config=SingleFishBpiDiagnosticsConfig(
                bpi_index_col=BPI_INDEX_COL,
                zero_band=float(BPI_ZERO_BAND),
                n_activity_bins=int(BPI_N_ACTIVITY_BINS),
                response_low=RESPONSE_LOW,
                response_unavailable=RESPONSE_UNAVAILABLE,
                response_summary_responsive=RESPONSE_SUMMARY_RESPONSIVE,
                response_summary_low=RESPONSE_SUMMARY_LOW,
                response_summary_unavailable=RESPONSE_SUMMARY_UNAVAILABLE,
            ),
        )
    except RuntimeError as exc:
        print(str(exc))
        raise SystemExit

    globals().update(_bpi_diag_56g['bindings'])
    for _line in _bpi_diag_56g['log_lines']:
        print(_line)

    df = _bpi_diag_56g['df'].copy()
    binned_df = _bpi_diag_56g['binned_df'].copy()
    bpi_activity_breakdown_df = _bpi_diag_56g['breakdown_df'].copy()
    activity_label = _bpi_diag_56g['activity_label']
    bout_col = _bpi_diag_56g['bout_col']
    cont_col = _bpi_diag_56g['cont_col']
    BPI_INDEX_COL = _bpi_diag_56g['bpi_index_col']
    BPI_ZERO_BAND = float(_bpi_diag_56g['bpi_zero_band'])
    _summary_56g = _bpi_diag_56g['summary_counts']
    n_total = int(_summary_56g['n_total'])
    n_nz = int(_summary_56g['n_near_zero'])
    n_nz_low = int(_summary_56g['n_near_zero_low'])
    n_nz_resp = int(_summary_56g['n_near_zero_responsive'])
    n_nz_unavailable = int(_summary_56g['n_near_zero_response_unavailable'])

    genes_present = sorted(df['gene'].dropna().astype(str).unique().tolist())
    gene_order = [g for g in GENE_ORDER if g in genes_present] + [g for g in genes_present if g not in GENE_ORDER]

    fig, axes = plt.subplots(2, 2, figsize=(14, 10.5))
"""
    source = source.replace(old_prep, new_prep)
    source = source.replace(
        """        binned_df = pd.DataFrame()
        n_bins = min(BPI_N_ACTIVITY_BINS, int(df['activity_mag'].nunique()))
        if n_bins >= 2:
            try:
                tmp = df.copy()
                tmp['_activity_bin'] = pd.qcut(df['activity_mag'], q=n_bins, duplicates='drop')
                binned_df = (
                    tmp.groupby('_activity_bin', observed=False)
                    .agg(activity_mid=('activity_mag', 'median'), median_abs_bpi=('abs_bpi', 'median'), n=('abs_bpi', 'size'))
                    .reset_index(drop=True)
                )
            except Exception:
                binned_df = pd.DataFrame()
""",
        "",
    )
    source = source.replace(
        """        breakdown = (
            df.groupby(['gene', 'interpretation', 'response_summary_class'], observed=False)
            .size()
            .rename('n')
            .reset_index()
        )
        bpi_activity_breakdown_df = breakdown.copy()

        """,
        "",
    )
    lines = source.splitlines()
    dedent = False
    for idx, line in enumerate(lines):
        if line == "    fig, axes = plt.subplots(2, 2, figsize=(14, 10.5))":
            dedent = True
            continue
        if dedent and line.startswith("        "):
            lines[idx] = line[4:]
    source = "\n".join(lines) + "\n"
    return source


def main() -> None:
    notebook = json.loads(NOTEBOOK_PATH.read_text())
    for cell in notebook.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        source = "".join(cell.get("source", []))
        if source.startswith("# [56g]"):
            source = _rewrite_56g(source)
        cell["source"] = source.splitlines(keepends=True)
    NOTEBOOK_PATH.write_text(json.dumps(notebook, indent=1))


if __name__ == "__main__":
    main()
