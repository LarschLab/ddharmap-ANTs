"""Embedded source for package-owned single-fish migration wrappers.

Generated from ``notebooks/2PF_to_HCR.ipynb`` during the contract-clean
notebook refactor. These strings preserve legacy stage behavior while the
notebook cells are thinned to public package calls.
"""

from __future__ import annotations

MIGRATED_CELL_SOURCE_BY_TAG: dict[str, str] = {'30': '# [30]\n'
       'try:\n'
       '    REQUIRE_FISH_STATE = require_fish_state\n'
       'except NameError:\n'
       '    REQUIRE_FISH_STATE = None\n'
       'if callable(REQUIRE_FISH_STATE) and not REQUIRE_FISH_STATE():\n'
       '    raise SystemExit\n'
       '# Noise-filtered diameters: q05 hard-drop + q95 low-confidence flag per modality\n'
       'import numpy as _np, matplotlib.pyplot as _plt\n'
       'from pathlib import Path\n'
       'import os\n'
       'import pandas as pd\n'
       '\n'
       "FILTER_MODALITIES = ['Anatomy', 'Functional', 'HCR']\n"
       'FILTER_LOW_QUANTILE = 0.05\n'
       'FILTER_HIGH_QUANTILE = 0.95\n'
       "LOW_CONF_COL = 'is_low_confidence_segmentation'\n"
       '\n'
       'def _apply_modality_xy_filter(\n'
       '    df_in,\n'
       '    modalities=FILTER_MODALITIES,\n'
       '    low_quantile=FILTER_LOW_QUANTILE,\n'
       '    high_quantile=FILTER_HIGH_QUANTILE,\n'
       '    low_conf_col=LOW_CONF_COL,\n'
       '):\n'
       '    """Hard-drop q05 small masks and flag q95 large masks as low-confidence per modality."""\n'
       "    if df_in is None or getattr(df_in, 'empty', True):\n"
       '        return df_in, pd.DataFrame()\n'
       "    required_cols = {'dataset', 'x_um', 'y_um'}\n"
       '    if not required_cols.issubset(df_in.columns):\n'
       "        print('[Filter] Skipped: dataset/x_um/y_um columns are required for per-modality filtering.')\n"
       '        return df_in.copy(), pd.DataFrame()\n'
       '\n'
       '    parts = []\n'
       '    summary_rows = []\n'
       "    datasets_present = [ds for ds in modalities if ds in df_in['dataset'].unique()]\n"
       "    datasets_present += [ds for ds in df_in['dataset'].unique() if ds not in modalities]\n"
       '    q_low_pct = int(round(float(low_quantile) * 100.0))\n'
       '    q_high_pct = int(round(float(high_quantile) * 100.0))\n'
       '\n'
       '    for ds in datasets_present:\n'
       "        sub = df_in[df_in['dataset'] == ds].copy()\n"
       '        n_before = int(len(sub))\n'
       "        xy_vals = sub[['x_um', 'y_um']].mean(axis=1).to_numpy(dtype=float)\n"
       '        finite = _np.isfinite(xy_vals)\n'
       '        finite_vals = xy_vals[finite]\n'
       '\n'
       '        med = float(_np.median(finite_vals)) if finite_vals.size else _np.nan\n'
       '        sd = float(_np.std(finite_vals)) if finite_vals.size else _np.nan\n'
       '        q_low = float(_np.quantile(finite_vals, low_quantile)) if finite_vals.size else _np.nan\n'
       '        q_high = float(_np.quantile(finite_vals, high_quantile)) if finite_vals.size else _np.nan\n'
       '        cutoff_low = q_low\n'
       '        cutoff_high = q_high\n'
       '\n'
       '        keep = _np.ones(n_before, dtype=bool)\n'
       '        low_conf_mask = _np.zeros(n_before, dtype=bool)\n'
       '        is_filtered = (ds in modalities) and finite_vals.size > 0\n'
       '        if is_filtered:\n'
       '            finite_idx = _np.where(finite)[0]\n'
       '            keep[finite_idx] = finite_vals >= cutoff_low\n'
       '            low_conf_mask[finite_idx] = finite_vals > cutoff_high\n'
       '\n'
       "        sub['xy_um'] = xy_vals\n"
       '        sub[low_conf_col] = low_conf_mask\n'
       '\n'
       '        n_after = int(keep.sum())\n'
       '        n_dropped = int(n_before - n_after)\n'
       '        pct_dropped = float((100.0 * n_dropped / n_before) if n_before > 0 else 0.0)\n'
       '        n_low_conf_before = int(low_conf_mask.sum())\n'
       '        n_low_conf_after = int(low_conf_mask[keep].sum())\n'
       '        pct_low_conf_after = float((100.0 * n_low_conf_after / n_after) if n_after > 0 else 0.0)\n'
       '\n'
       '        if is_filtered and n_dropped > 0:\n'
       '            print(\n'
       "                f'[Filter] {ds}: dropped {n_dropped}/{n_before} labels '\n"
       "                f'(XY < q{q_low_pct:02d}={cutoff_low:.3f} µm; median={med:.3f}, SD={sd:.3f})'\n"
       '            )\n'
       '        if is_filtered and n_low_conf_after > 0:\n'
       '            print(\n'
       "                f'[Flag] {ds}: marked {n_low_conf_after}/{n_after} labels as low-confidence '\n"
       "                f'({low_conf_col}=True; XY > q{q_high_pct:02d}={cutoff_high:.3f} µm)'\n"
       '            )\n'
       '\n'
       '        summary_rows.append({\n'
       "            'dataset': ds,\n"
       "            'is_filtered': bool(ds in modalities),\n"
       "            'n_before': n_before,\n"
       "            'n_dropped': n_dropped,\n"
       "            'n_after': n_after,\n"
       "            'pct_dropped': pct_dropped,\n"
       "            'xy_median_um': med,\n"
       "            'xy_sd_um': sd,\n"
       "            'xy_q05_um': q_low if q_low_pct == 5 else _np.nan,\n"
       "            'xy_q95_um': q_high if q_high_pct == 95 else _np.nan,\n"
       "            'xy_cutoff_um': cutoff_low,\n"
       "            'xy_flag_um': cutoff_high,\n"
       "            'n_low_confidence_q95_before_filter': n_low_conf_before,\n"
       "            'n_low_confidence_q95_after_filter': n_low_conf_after,\n"
       "            'pct_low_confidence_after_filter': pct_low_conf_after,\n"
       '        })\n'
       '        parts.append(sub.iloc[keep].copy())\n'
       '\n'
       '    df_out = pd.concat(parts, ignore_index=True) if parts else df_in.iloc[0:0].copy()\n'
       '    summary_df = pd.DataFrame(summary_rows)\n'
       '    return df_out, summary_df\n'
       '\n'
       '\n'
       '\n'
       'def _load_labels_or_none(path):\n'
       '    if path is None:\n'
       '        return None\n'
       '    try:\n'
       '        if not os.path.exists(path):\n'
       '            return None\n'
       '        arr = _ensure_uint_labels(imread(path))\n'
       '        if arr.ndim == 3 and arr.shape[-1] in (3, 4):\n'
       '            arr = arr[..., 0]\n'
       '        return arr\n'
       '    except Exception:\n'
       '        return None\n'
       '\n'
       '\n'
       'def _build_raw_diameter_df():\n'
       '    dfs = []\n'
       '\n'
       '    anat_labels = _load_labels_or_none(ANAT_LABELS_PATH)\n'
       '    if anat_labels is not None:\n'
       '        anat_3d = anat_labels if anat_labels.ndim == 3 else anat_labels[None, ...]\n'
       '        vox_a = {\n'
       "            'Z': float(VOX_ANAT.get('Z', 1.0)) if VOX_ANAT else 1.0,\n"
       "            'Y': float(VOX_ANAT.get('Y', 1.0)) if VOX_ANAT else 1.0,\n"
       "            'X': float(VOX_ANAT.get('X', 1.0)) if VOX_ANAT else 1.0,\n"
       '        }\n'
       '        df_an = diameters_um_from_array(anat_3d, vox_a)\n'
       "        df_an['dataset'] = 'Anatomy'\n"
       '        dfs.append(df_an)\n'
       '\n'
       '    func_dfs = []\n'
       '    use_plane_refs = True\n'
       '    if use_plane_refs and plane_refs:\n'
       '        vox_f = {\n'
       "            'Z': float(VOX_ANAT.get('Z', 1.0)) if VOX_ANAT else 1.0,\n"
       "            'Y': float(VOX_ANAT.get('Y', 1.0)) if VOX_ANAT else 1.0,\n"
       "            'X': float(VOX_ANAT.get('X', 1.0)) if VOX_ANAT else 1.0,\n"
       '        }\n'
       '        for p_idx, pr in enumerate(plane_refs):\n'
       '            try:\n'
       '                arr, _ = _get_labels_for_plane(pr, p_idx)\n'
       '            except Exception:\n'
       '                arr = None\n'
       '            if arr is None:\n'
       '                continue\n'
       '            arr = _ensure_uint_labels(arr)\n'
       '            if arr.ndim == 3 and arr.shape[-1] in (3, 4):\n'
       '                arr = arr[..., 0]\n'
       '            if arr.ndim == 3 and arr.shape[0] == 1:\n'
       '                arr = arr[0]\n'
       '            if arr.ndim != 2:\n'
       '                continue\n'
       '            arr3 = arr[None, ...]\n'
       '            df_i = diameters_um_from_array(arr3, vox_f)\n'
       "            df_i['plane_idx'] = p_idx\n"
       '            func_dfs.append(df_i)\n'
       '    else:\n'
       '        func_labels = _load_labels_or_none(FUNC_LABELS_PATH)\n'
       '        if func_labels is not None:\n'
       '            vox_f = {\n'
       "                'Z': 1.0,\n"
       "                'Y': float(VOX_FUNC.get('Y', 1.0)) if VOX_FUNC else 1.0,\n"
       "                'X': float(VOX_FUNC.get('X', 1.0)) if VOX_FUNC else 1.0,\n"
       '            }\n'
       '            arr3 = func_labels if func_labels.ndim == 3 else func_labels[None, ...]\n'
       '            func_dfs.append(diameters_um_from_array(arr3, vox_f))\n'
       '\n'
       '    if func_dfs:\n'
       '        df_fn = pd.concat(func_dfs, ignore_index=True)\n'
       "        df_fn['dataset'] = 'Functional'\n"
       '        dfs.append(df_fn)\n'
       '\n'
       '    hcr_paths = list(HCR_LABELS_PATHS or [])\n'
       '    if not hcr_paths and HCR_LABELS_PATH:\n'
       '        hcr_paths = [HCR_LABELS_PATH]\n'
       '    hcr_dfs = []\n'
       '    for hp in hcr_paths:\n'
       '        hl = _load_labels_or_none(hp)\n'
       '        if hl is None:\n'
       '            continue\n'
       '        h3 = hl if hl.ndim == 3 else hl[None, ...]\n'
       '        vox_h = {\n'
       "            'Z': float(VOX_HCR.get('Z', 1.0)) if VOX_HCR else 1.0,\n"
       "            'Y': float(VOX_HCR.get('Y', 1.0)) if VOX_HCR else 1.0,\n"
       "            'X': float(VOX_HCR.get('X', 1.0)) if VOX_HCR else 1.0,\n"
       '        }\n'
       '        df_h = diameters_um_from_array(h3, vox_h)\n'
       "        df_h['dataset'] = 'HCR'\n"
       '        try:\n'
       "            df_h['gene'] = gene_from_mask(hp)\n"
       '        except Exception:\n'
       "            df_h['gene'] = Path(str(hp)).name\n"
       "        df_h['file'] = Path(str(hp)).name\n"
       '        hcr_dfs.append(df_h)\n'
       '    if hcr_dfs:\n'
       '        dfs.append(pd.concat(hcr_dfs, ignore_index=True))\n'
       '\n'
       '    if not dfs:\n'
       '        return None\n'
       '    out = pd.concat(dfs, ignore_index=True)\n'
       '    return out\n'
       '\n'
       '# Try to reuse df_all from previous cell or cache\n'
       "df_all = df_all if 'df_all' in locals() else None\n"
       "if df_all is not None and ('DF_ALL_FISH_ID' in locals()) and DF_ALL_FISH_ID != FISH_ID:\n"
       "    print('[Info] Ignoring in-memory df_all from different/unknown fish; rebuilding for current fish.')\n"
       '    df_all = None\n'
       "if df_all is None or getattr(df_all, 'empty', True):\n"
       '    _diam_paths = []\n'
       '    if OUT_QA is not None:\n'
       "        _diam_paths.append(OUT_QA / 'diameters_df_all.pkl')\n"
       '    if OUTDIR is not None:\n'
       "        _diam_paths.append(OUTDIR / 'diameters_df_all.pkl')\n"
       '    for _p in _diam_paths:\n'
       '        if os.path.exists(_p):\n'
       '            try:\n'
       '                df_all = pd.read_pickle(_p)\n'
       '                print(f"[Info] Loaded df_all from cache pickle: {_p}")\n'
       '                DF_ALL_FISH_ID = FISH_ID\n'
       '                break\n'
       '            except Exception:\n'
       '                df_all = None\n'
       '\n'
       "if df_all is None or getattr(df_all, 'empty', True):\n"
       '    df_all = _build_raw_diameter_df()\n'
       "    if df_all is not None and not getattr(df_all, 'empty', True):\n"
       '        df_all = df_all\n'
       '        DF_ALL_FISH_ID = FISH_ID\n'
       '        _cache_targets = []\n'
       '        if OUT_QA is not None:\n'
       "            _cache_targets.append(OUT_QA / 'diameters_df_all.pkl')\n"
       '        if OUTDIR is not None:\n'
       "            _cache_targets.append(OUTDIR / 'diameters_df_all.pkl')\n"
       '        for _cp in _cache_targets:\n'
       '            try:\n'
       '                df_all.to_pickle(_cp)\n'
       '                break\n'
       '            except Exception:\n'
       '                pass\n'
       '\n'
       "if df_all is None or getattr(df_all, 'empty', True):\n"
       "    print('No label volumes available for noise-filtered diameter analysis.')\n"
       'else:\n'
       '    df_all_filt, df_all_filter_summary = _apply_modality_xy_filter(df_all)\n'
       '    df_all_filt = df_all_filt\n'
       '    df_all_filter_summary = df_all_filter_summary\n'
       '    LOW_CONF_SEG_COL = LOW_CONF_COL\n'
       '\n'
       '    if not df_all_filter_summary.empty:\n'
       "        _order = FILTER_MODALITIES + [ds for ds in df_all_filter_summary['dataset'].tolist() if ds not in "
       'FILTER_MODALITIES]\n'
       "        df_all_filter_summary['dataset'] = pd.Categorical(df_all_filter_summary['dataset'], categories=_order, "
       'ordered=True)\n'
       "        df_all_filter_summary = df_all_filter_summary.sort_values('dataset').reset_index(drop=True)\n"
       '        df_all_filter_summary = df_all_filter_summary\n'
       '        _q_low_pct = int(round(FILTER_LOW_QUANTILE * 100.0))\n'
       '        _q_high_pct = int(round(FILTER_HIGH_QUANTILE * 100.0))\n'
       "        print(f'Per-modality XY filter summary (q{_q_low_pct:02d} hard-drop, q{_q_high_pct:02d} low-confidence "
       "flag):')\n"
       '        try:\n'
       '            from IPython.display import display\n'
       '            display(df_all_filter_summary)\n'
       '        except Exception:\n'
       '            print(df_all_filter_summary.to_string(index=False))\n'
       '\n'
       "    cats_plot = ['Anatomy', 'Functional', 'HCR']\n"
       "    present = [c for c in cats_plot if c in df_all_filt['dataset'].unique()]\n"
       '\n'
       '    def _series_for_axis(axis_col, df):\n'
       '        ser = []\n'
       '        for ds in present:\n'
       "            vals = df.loc[df['dataset'] == ds, axis_col].to_numpy(dtype=float)\n"
       '            ser.append(vals)\n'
       '        return ser\n'
       '\n'
       "    series_x = _series_for_axis('x_um', df_all_filt)\n"
       "    series_y = _series_for_axis('y_um', df_all_filt)\n"
       "    series_z = _series_for_axis('z_um', df_all_filt)\n"
       '\n'
       '    all_vals = _np.concatenate([a for a in (series_x + series_y + series_z) if a.size]) if any((a.size for a '
       'in (series_x + series_y + series_z))) else _np.array([])\n'
       '    y_max_data = float(_np.max(all_vals)) if all_vals.size else 10.0\n'
       '    from math import ceil as _ceil\n'
       '    y_max = max(10.0, 10.0 * _ceil(y_max_data / 10.0))\n'
       '    yticks = _np.arange(0.0, y_max + 0.1, 10.0)\n'
       '\n'
       "    color_map = {'Anatomy': '#bbbbbb', 'Functional': '#88ccee', 'HCR': '#cc88ff'}\n"
       "    colors = [color_map.get(ds, '#cccccc') for ds in present]\n"
       '\n'
       '    fig, axes = _plt.subplots(1, 3, figsize=(20, 4.5), sharey=True)\n'
       "    if not hasattr(axes, '__len__'):\n"
       '        axes = [axes]\n'
       "    axis_cols = ['x_um', 'y_um', 'z_um']\n"
       "    for ax, ser, title, axis_col in zip(axes, [series_x, series_y, series_z], ['X diameter (µm)', 'Y diameter "
       "(µm)', 'Z diameter (µm)'], axis_cols):\n"
       '        if not any(a.size for a in ser):\n'
       '            ax.set_visible(False)\n'
       '            continue\n'
       '        parts = ax.violinplot(ser, showmeans=False, showmedians=False, showextrema=False)\n'
       "        for i, pc in enumerate(parts['bodies']):\n"
       '            pc.set_facecolor(colors[i])\n'
       "            pc.set_edgecolor('black')\n"
       '            pc.set_alpha(0.7)\n'
       '        x_offset = 0.18\n'
       '        y_offset = 0.02 * y_max\n'
       "        bbox_style = dict(facecolor='white', alpha=0.7, edgecolor='none', pad=1.0)\n"
       '        for i, vals in enumerate(ser, start=1):\n'
       '            if vals.size:\n'
       '                med = float(_np.median(vals))\n'
       '                sd = float(_np.std(vals))\n'
       "                ax.scatter([i], [med], color='crimson', zorder=3, s=26)\n"
       '                label_txt = "median={:.2f} µm\\nsd={:.2f} µm\\nn={:d}".format(med, sd, vals.size)\n'
       '                ax.text(i + x_offset, med + y_offset, label_txt,\n'
       "                        va='bottom', ha='left', fontsize=8, bbox=bbox_style, clip_on=False, zorder=4)\n"
       '        ax.set_title(title)\n'
       '        ax.set_xticks(range(1, len(present) + 1))\n'
       '        ax.set_xticklabels(present, rotation=0)\n'
       '        ax.set_ylim(0, y_max)\n'
       '        ax.set_yticks(yticks)\n'
       "        ax.grid(axis='y', alpha=0.2)\n"
       '    _q_low_pct = int(round(FILTER_LOW_QUANTILE * 100.0))\n'
       '    _q_high_pct = int(round(FILTER_HIGH_QUANTILE * 100.0))\n'
       '    fig.suptitle(f"Segmentation sizes stay within a comparable biological range after filtering (drop '
       'q{_q_low_pct:02d}; flag q{_q_high_pct:02d})")\n'
       '    _plt.tight_layout()\n'
       '    _plt.show()\n'
       '\n'
       '    # HCR per-file diameters (filtered)\n'
       "    if 'HCR' in present and 'file' in df_all_filt.columns and df_all_filt.loc[df_all_filt['dataset'] == 'HCR', "
       "'file'].notna().any():\n"
       "        _hcr_sub = df_all_filt[df_all_filt['dataset'] == 'HCR'].copy()\n"
       "        if 'gene' not in _hcr_sub.columns:\n"
       "            _hcr_sub['gene'] = _hcr_sub['file'].apply(gene_from_mask)\n"
       "        _files_genes = _hcr_sub[['file', 'gene']].dropna().drop_duplicates()\n"
       "        _hcr_files = _files_genes['file'].tolist()\n"
       "        _hcr_genes = _files_genes['gene'].tolist()\n"
       '        if _hcr_files:\n'
       '            fig2, axes2 = _plt.subplots(1, 3, figsize=(20, 4.5), sharey=True)\n'
       "            if not hasattr(axes2, '__len__'):\n"
       '                axes2 = [axes2]\n'
       '            for ax, axis_col in zip(axes2, axis_cols):\n'
       "                ser = [_hcr_sub.loc[_hcr_sub['file'] == fn, axis_col].to_numpy(dtype=float) for fn in "
       '_hcr_files]\n'
       '                if not any(a.size for a in ser):\n'
       '                    ax.set_visible(False)\n'
       '                    continue\n'
       '                parts = ax.violinplot(ser, showmeans=False, showmedians=False, showextrema=False)\n'
       "                for pc in parts['bodies']:\n"
       "                    pc.set_facecolor('#cc88ff')\n"
       "                    pc.set_edgecolor('black')\n"
       '                    pc.set_alpha(0.7)\n'
       '                x_offset = 0.12\n'
       '                y_offset = 0.02 * y_max\n'
       '                for i, vals in enumerate(ser, start=1):\n'
       '                    if vals.size:\n'
       '                        med = float(_np.median(vals))\n'
       '                        sd = float(_np.std(vals))\n'
       "                        ax.scatter([i], [med], color='crimson', zorder=3, s=24)\n"
       '                        ax.text(\n'
       '                            i + x_offset, med + y_offset,\n'
       '                            f"median={med:.2f} µm\\nsd={sd:.2f} µm\\nn={vals.size}",\n'
       "                            va='bottom', ha='left', fontsize=7.5,\n"
       "                            bbox=dict(facecolor='white', alpha=0.7, edgecolor='none', pad=1.0),\n"
       '                            clip_on=False, zorder=4\n'
       '                        )\n'
       '                ax.set_title(f"Filtered HCR {axis_col.split(\'_\')[0].upper()} sizes remain consistent across '
       'files")\n'
       '                ax.set_xticks(range(1, len(_hcr_files) + 1))\n'
       "                ax.set_xticklabels(_hcr_genes, rotation=30, ha='right')\n"
       '                ax.set_ylim(0, y_max)\n'
       '                ax.set_yticks(yticks)\n'
       "                ax.grid(axis='y', alpha=0.2)\n"
       '            _plt.tight_layout()\n'
       '            _plt.show()\n'
       '\n'
       "    axis_name_map = {'x_um': 'X', 'y_um': 'Y', 'z_um': 'Z'}\n"
       '    _outlier_rows = []\n'
       '    for ds in present:\n'
       "        sub = df_all_filt[df_all_filt['dataset'] == ds]\n"
       '        if sub.empty:\n'
       '            continue\n'
       '        for axis_col in axis_cols:\n'
       '            vals = sub[axis_col].to_numpy(dtype=float)\n'
       '            if not len(vals):\n'
       '                continue\n'
       '            try:\n'
       '                q1, q3 = _np.percentile(vals, [25, 75])\n'
       '                iqr = q3 - q1\n'
       '                lo = q1 - 1.5 * iqr\n'
       '                hi = q3 + 1.5 * iqr\n'
       '            except Exception:\n'
       '                lo, hi = -_np.inf, _np.inf\n'
       '            _outs = sub[(sub[axis_col] < lo) | (sub[axis_col] > hi)]\n'
       '            if _outs.empty:\n'
       '                continue\n'
       '            for _, row in _outs.iterrows():\n'
       '                entry = {\n'
       "                    'dataset': ds,\n"
       "                    'axis': axis_name_map.get(axis_col, axis_col),\n"
       "                    'diameter_um': float(row[axis_col])\n"
       '                }\n'
       "                if 'label' in row and not pd.isna(row['label']):\n"
       "                    entry['label'] = int(row['label'])\n"
       "                if ds == 'Functional' and 'plane_idx' in row and not pd.isna(row['plane_idx']):\n"
       "                    entry['plane_idx'] = int(row['plane_idx'])\n"
       "                if 'file' in row and not pd.isna(row['file']):\n"
       "                    entry['file'] = row['file']\n"
       '                _outlier_rows.append(entry)\n'
       '\n'
       '    if _outlier_rows:\n'
       '        _out_df = pd.DataFrame(_outlier_rows).sort_values(\n'
       "            ['dataset', 'axis', 'diameter_um'],\n"
       '            ascending=[True, True, False]\n'
       '        ).reset_index(drop=True)\n'
       "        print('Outlier diameters after per-modality filter (IQR fence, pooled), sorted high→low "
       "(per-plane/per-file labels):')\n"
       '        try:\n'
       '            from IPython.display import HTML, display\n'
       '            html = _out_df.to_html(index=False)\n'
       '            display(HTML(f"<div style=\'max-height:320px; overflow-y:auto\'>{html}</div>"))\n'
       '        except Exception:\n'
       '            print(_out_df.to_string(index=False))\n'
       '    else:\n'
       "        print('No diameter outliers found after filter.')\n",
 '34c': '# [34c]\n'
        'try:\n'
        '    _require_fish_state = require_fish_state\n'
        'except NameError:\n'
        '    _require_fish_state = None\n'
        'if callable(_require_fish_state) and not _require_fish_state():\n'
        '    raise SystemExit\n'
        '\n'
        'import json\n'
        'from pathlib import Path\n'
        '\n'
        'import matplotlib.pyplot as plt\n'
        'import numpy as np\n'
        'from skimage import color as _color\n'
        'from skimage import segmentation, transform\n'
        'from skimage.transform import AffineTransform\n'
        '\n'
        "REGION_QA_SQUARE_JSON_PATH = Path(OUT_REG) / 'regional_match_qa_square.json'\n"
        "REGION_QA_SQUARE_LEGACY_JSON_PATH = Path(OUT_REG) / 'regional_shift_square.json'\n"
        "REGION_QA_PLANES = 'all'\n"
        'REGION_QA_PAD_PX = 24\n'
        'REGION_QA_LABEL_ALPHA = 0.35\n'
        'REGION_QA_ANAT_OUTLINE_RGBA = (1.0, 0.2, 0.85, 0.90)\n'
        'REGION_QA_SAVE = True\n'
        "REGION_QA_OUT_DIR = Path(OUT_QA) / 'regional_match_review'\n"
        '\n'
        'try:\n'
        '    _suite2p_by_ref_idx = suite2p_by_ref_idx\n'
        'except NameError:\n'
        '    _suite2p_by_ref_idx = None\n'
        'try:\n'
        '    _plane_refs = plane_refs\n'
        'except NameError:\n'
        '    _plane_refs = None\n'
        'try:\n'
        '    _anat_labels_path = ANAT_LABELS_PATH\n'
        'except NameError:\n'
        '    _anat_labels_path = None\n'
        'try:\n'
        '    _norm01_fn = norm01\n'
        'except NameError:\n'
        '    _norm01_fn = None\n'
        'try:\n'
        '    _rescale_labels_to_ref_fn = _rescale_labels_to_ref\n'
        'except NameError:\n'
        '    _rescale_labels_to_ref_fn = None\n'
        'try:\n'
        '    _get_labels_for_plane_fn = _get_labels_for_plane\n'
        'except NameError:\n'
        '    _get_labels_for_plane_fn = None\n'
        'try:\n'
        '    _tform_for_plane_fn = _tform_for_plane\n'
        'except NameError:\n'
        '    _tform_for_plane_fn = None\n'
        '\n'
        '\n'
        'def _read_json_local(path):\n'
        '    try:\n'
        '        path = Path(path)\n'
        '        if not path.exists():\n'
        '            return None\n'
        '        return json.loads(path.read_text())\n'
        '    except Exception:\n'
        '        return None\n'
        '\n'
        '\n'
        'def _load_region_qa_square_spec():\n'
        '    for path in (REGION_QA_SQUARE_JSON_PATH, REGION_QA_SQUARE_LEGACY_JSON_PATH):\n'
        '        spec = _read_json_local(path)\n'
        '        if isinstance(spec, dict):\n'
        '            return spec, Path(path)\n'
        '    return None, None\n'
        '\n'
        '\n'
        'def _norm01_like_26(a):\n'
        '    arr = np.asarray(a, dtype=np.float32)\n'
        '    if callable(_norm01_fn):\n'
        '        try:\n'
        '            return _norm01_fn(arr)\n'
        '        except Exception:\n'
        '            pass\n'
        '    finite = np.isfinite(arr)\n'
        '    if not finite.any():\n'
        '        return np.zeros_like(arr, dtype=np.float32)\n'
        '    lo = float(np.nanpercentile(arr[finite], 1.0))\n'
        '    hi = float(np.nanpercentile(arr[finite], 99.0))\n'
        '    if hi <= lo:\n'
        '        lo = float(np.nanmin(arr[finite]))\n'
        '        hi = float(np.nanmax(arr[finite]))\n'
        '        if hi <= lo:\n'
        '            return np.zeros_like(arr, dtype=np.float32)\n'
        '    out = (arr - lo) / (hi - lo)\n'
        '    out[~finite] = 0.0\n'
        '    return np.clip(out, 0.0, 1.0).astype(np.float32)\n'
        '\n'
        '\n'
        'def _outline_rgba(label_img, rgba):\n'
        '    arr = np.asarray(label_img)\n'
        '    out = np.zeros(arr.shape + (4,), dtype=np.float32)\n'
        '    if arr.size == 0:\n'
        '        return out\n'
        "    bnd = segmentation.find_boundaries(arr, mode='outer')\n"
        '    if np.any(bnd):\n'
        '        r, g, b, a = [float(v) for v in rgba]\n'
        '        out[bnd, 0] = r\n'
        '        out[bnd, 1] = g\n'
        '        out[bnd, 2] = b\n'
        '        out[bnd, 3] = a\n'
        '    return out\n'
        '\n'
        '\n'
        'def _label_overlay_local(ref_img, label_img, alpha):\n'
        '    ref_vis = _norm01_like_26(ref_img)\n'
        '    labels = _ensure_uint_labels(label_img)\n'
        '    overlay = _color.label2rgb(labels, image=ref_vis, bg_label=0, alpha=float(alpha), image_alpha=1.0)\n'
        '    return np.asarray(overlay, dtype=np.float32)\n'
        '\n'
        '\n'
        'def _crop_from_bounds(bounds, shape, pad_px):\n'
        '    h, w = int(shape[0]), int(shape[1])\n'
        '    x0, y0, x1, y1 = [int(v) for v in bounds]\n'
        '    pad_px = int(max(0, pad_px))\n'
        '    x0 = max(0, x0 - pad_px)\n'
        '    y0 = max(0, y0 - pad_px)\n'
        '    x1 = min(w, x1 + pad_px)\n'
        '    y1 = min(h, y1 + pad_px)\n'
        '    return x0, y0, x1, y1\n'
        '\n'
        '\n'
        'def _square_bounds_from_spec(spec, shape):\n'
        '    h, w = int(shape[0]), int(shape[1])\n'
        "    if spec.get('bounds_xyxy', None) is not None:\n"
        '        try:\n'
        "            bx0, by0, bx1, by1 = [int(v) for v in spec.get('bounds_xyxy')]\n"
        '            bx0 = max(0, min(bx0, w - 1))\n'
        '            by0 = max(0, min(by0, h - 1))\n'
        '            bx1 = max(bx0 + 1, min(bx1, w))\n'
        '            by1 = max(by0 + 1, min(by1, h))\n'
        '            return bx0, by0, bx1, by1\n'
        '        except Exception:\n'
        '            pass\n'
        "    size_px = int(spec.get('size_px', min(h, w)))\n"
        "    cx = int(spec.get('center_x', w // 2))\n"
        "    cy = int(spec.get('center_y', h // 2))\n"
        '    size_px = int(max(4, min(size_px, min(h, w))))\n'
        '    half = float(size_px) / 2.0\n'
        '    x0 = int(round(float(cx) - half))\n'
        '    y0 = int(round(float(cy) - half))\n'
        '    x0 = max(0, min(x0, w - size_px))\n'
        '    y0 = max(0, min(y0, h - size_px))\n'
        '    return x0, y0, x0 + size_px, y0 + size_px\n'
        '\n'
        '\n'
        'def _resolve_plane_indices(value):\n'
        "    if value is None or str(value).lower() == 'all':\n"
        '        return sorted(int(k) for k in _suite2p_by_ref_idx.keys())\n'
        '    if isinstance(value, (list, tuple, set, np.ndarray)):\n'
        '        out = []\n'
        '        for item in value:\n'
        '            try:\n'
        '                out.append(int(item))\n'
        '            except Exception:\n'
        '                pass\n'
        '        return sorted(set(out))\n'
        '    out = []\n'
        "    for part in str(value).split(','):\n"
        '        part = str(part).strip()\n'
        '        if not part:\n'
        '            continue\n'
        '        try:\n'
        '            out.append(int(part))\n'
        '        except Exception:\n'
        '            pass\n'
        '    return sorted(set(out))\n'
        '\n'
        '\n'
        'def _rescale_labels_to_ref_local(labels, ref_shape):\n'
        '    if labels is None or ref_shape is None:\n'
        '        return labels\n'
        '    ref_shape = tuple(int(v) for v in ref_shape)\n'
        '    if tuple(np.asarray(labels).shape) == ref_shape:\n'
        '        return labels\n'
        '    if callable(_rescale_labels_to_ref_fn):\n'
        '        try:\n'
        '            out = _rescale_labels_to_ref_fn(labels, ref_shape)\n'
        '            if out is not None:\n'
        '                return _ensure_uint_labels(out)\n'
        '        except Exception:\n'
        '            pass\n'
        '    try:\n'
        '        out = transform.resize(\n'
        '            np.asarray(labels, dtype=np.float32),\n'
        '            ref_shape,\n'
        '            order=0,\n'
        "            mode='edge',\n"
        '            preserve_range=True,\n'
        '            anti_aliasing=False,\n'
        '        )\n'
        '        return _ensure_uint_labels(out)\n'
        '    except Exception:\n'
        '        return _ensure_uint_labels(labels)\n'
        '\n'
        '\n'
        'def _get_labels_like_26(pr, p_idx, plane_data):\n'
        '    if callable(_get_labels_for_plane_fn):\n'
        '        try:\n'
        '            labels, src_desc = _get_labels_for_plane_fn(pr, p_idx)\n'
        '            if labels is not None:\n'
        '                return _ensure_uint_labels(labels), src_desc\n'
        '        except Exception:\n'
        '            pass\n'
        "    ref_shape = np.asarray(pr.get('ref_match')).shape if pr.get('ref_match') is not None else None\n"
        "    s2p = pr.get('suite2p', None)\n"
        '    if isinstance(s2p, dict):\n'
        "        arr = s2p.get('labels', None)\n"
        '        if arr is not None:\n'
        "            return _rescale_labels_to_ref_local(_ensure_uint_labels(arr), ref_shape), 'Suite2p labels "
        "(plane_refs fallback)'\n"
        '    if isinstance(plane_data, dict):\n'
        "        arr = plane_data.get('labels', None)\n"
        '        if arr is not None:\n'
        "            return _rescale_labels_to_ref_local(_ensure_uint_labels(arr), ref_shape), 'Suite2p labels "
        "(plane_data fallback)'\n"
        '    return None, None\n'
        '\n'
        '\n'
        'def _get_ref_like_26(pr):\n'
        "    ref_img = pr.get('ref_match', None)\n"
        "    ref_src = 'ref_match'\n"
        '    if ref_img is None:\n'
        "        ref_img = pr.get('ref2d_raw', pr.get('ref2d'))\n"
        "        ref_src = 'ref2d'\n"
        '    if ref_img is None:\n'
        '        return None, ref_src\n'
        '    ref_img = np.asarray(ref_img, dtype=np.float32)\n'
        '    if ref_img.ndim == 3 and ref_img.shape[-1] in (3, 4):\n'
        '        ref_img = ref_img[..., 0]\n'
        '    if ref_img.ndim != 2:\n'
        '        return None, ref_src\n'
        '    return ref_img, ref_src\n'
        '\n'
        '\n'
        'def _invert_tform_local(tform):\n'
        '    if tform is None:\n'
        '        return AffineTransform()\n'
        '    try:\n'
        '        mat = np.asarray(tform.params, dtype=float)\n'
        '        if mat.shape == (3, 3):\n'
        '            return AffineTransform(matrix=np.linalg.inv(mat))\n'
        '    except Exception:\n'
        '        pass\n'
        '    return AffineTransform()\n'
        '\n'
        '\n'
        'def _warp_labels_local(label_img, tform, output_shape):\n'
        '    if tform is None:\n'
        '        return _ensure_uint_labels(label_img)\n'
        '    return _ensure_uint_labels(resample_labels_nn(label_img, tform, output_shape=output_shape))\n'
        '\n'
        '\n'
        'def _transform_bounds_to_ref(bounds_xyxy, tform_inv, output_shape):\n'
        '    x0, y0, x1, y1 = [float(v) for v in bounds_xyxy]\n'
        '    pts = np.array([\n'
        '        [x0, y0],\n'
        '        [x1, y0],\n'
        '        [x1, y1],\n'
        '        [x0, y1],\n'
        '    ], dtype=float)\n'
        '    try:\n'
        '        pts_t = np.asarray(tform_inv(pts), dtype=float)\n'
        '    except Exception:\n'
        '        pts_t = pts.copy()\n'
        '    xs = pts_t[:, 0]\n'
        '    ys = pts_t[:, 1]\n'
        '    h, w = int(output_shape[0]), int(output_shape[1])\n'
        '    bx0 = max(0, int(np.floor(np.nanmin(xs))))\n'
        '    by0 = max(0, int(np.floor(np.nanmin(ys))))\n'
        '    bx1 = min(w, int(np.ceil(np.nanmax(xs))))\n'
        '    by1 = min(h, int(np.ceil(np.nanmax(ys))))\n'
        '    bx1 = max(bx0 + 1, bx1)\n'
        '    by1 = max(by0 + 1, by1)\n'
        '    return bx0, by0, bx1, by1\n'
        '\n'
        '\n'
        'if not _suite2p_by_ref_idx:\n'
        "    print('[34c] suite2p_by_ref_idx missing; run [23a] first.')\n"
        'elif not _plane_refs:\n'
        "    print('[34c] plane_refs missing; run [16] first.')\n"
        'elif _anat_labels_path is None:\n'
        "    print('[34c] ANAT_LABELS_PATH missing; run the anatomy setup cells first.')\n"
        'else:\n'
        '    square_spec, square_path = _load_region_qa_square_spec()\n'
        '    if not isinstance(square_spec, dict):\n'
        "        print('[34c] no saved QA square found; run [22d] first.')\n"
        '    else:\n'
        '        anat_labels_all = _ensure_uint_labels(imread_any(_anat_labels_path))\n'
        '        if anat_labels_all.ndim != 3:\n'
        "            raise RuntimeError('[34c] anatomy labels must be a 3D label stack (Z,Y,X)')\n"
        '\n'
        '        plane_records = []\n'
        '        for p_idx in _resolve_plane_indices(REGION_QA_PLANES):\n'
        '            plane_data = _suite2p_by_ref_idx.get(int(p_idx), {})\n'
        '            pr = _plane_refs[int(p_idx)] if 0 <= int(p_idx) < len(_plane_refs) else {}\n'
        '            try:\n'
        "                best_z = int(pr.get('best_z', -1))\n"
        '            except Exception:\n'
        '                best_z = -1\n'
        '            if best_z < 0 or best_z >= int(anat_labels_all.shape[0]):\n'
        '                continue\n'
        '\n'
        '            labels_display, labels_src = _get_labels_like_26(pr, int(p_idx), plane_data)\n'
        '            if labels_display is None:\n'
        "                print(f'[34c] skip plane {p_idx}: no functional labels in [26] space')\n"
        '                continue\n'
        '\n'
        '            ref_img, ref_src = _get_ref_like_26(pr)\n'
        '            if ref_img is None:\n'
        "                print(f'[34c] skip plane {p_idx}: no functional reference in [26] space')\n"
        '                continue\n'
        '            if tuple(np.asarray(ref_img).shape) != tuple(np.asarray(labels_display).shape):\n'
        "                print(f'[34c] skip plane {p_idx}: [26] label/ref shape mismatch "
        "{np.asarray(labels_display).shape} vs {np.asarray(ref_img).shape}')\n"
        '                continue\n'
        '\n'
        '            anat_slice = _ensure_uint_labels(anat_labels_all[best_z])\n'
        '            bounds_anat = _square_bounds_from_spec(square_spec, anat_slice.shape)\n'
        '            ax0, ay0, ax1, ay1 = bounds_anat\n'
        '            square_labels = np.unique(anat_slice[ay0:ay1, ax0:ax1])\n'
        '            square_labels = [int(v) for v in square_labels if int(v) != 0]\n'
        '            if square_labels:\n'
        '                anat_labels_sel_img = np.where(np.isin(anat_slice, np.asarray(square_labels, '
        'dtype=np.int64)), anat_slice, 0).astype(anat_slice.dtype, copy=False)\n'
        '            else:\n'
        '                anat_labels_sel_img = np.zeros_like(anat_slice, dtype=anat_slice.dtype)\n'
        '\n'
        "            tform_use = _tform_for_plane_fn(pr) if callable(_tform_for_plane_fn) else pr.get('tform', None)\n"
        '            inv_tform = _invert_tform_local(tform_use)\n'
        '            anat_display = _warp_labels_local(anat_labels_sel_img, inv_tform, '
        'output_shape=labels_display.shape)\n'
        '            bounds_display = _transform_bounds_to_ref(bounds_anat, inv_tform, labels_display.shape)\n'
        '\n'
        '            plane_records.append({\n'
        "                'plane_idx': int(p_idx),\n"
        "                'plane_label': str(pr.get('label', f'plane{p_idx}')),\n"
        "                'best_z': int(best_z),\n"
        "                'ref_img': np.asarray(ref_img, dtype=np.float32),\n"
        "                'ref_src': str(ref_src),\n"
        "                'labels_display': _ensure_uint_labels(labels_display),\n"
        "                'labels_src': str(labels_src),\n"
        "                'anat_display': _ensure_uint_labels(anat_display),\n"
        "                'crop_bounds': tuple(int(v) for v in bounds_display),\n"
        "                'n_anat_labels': int(len(square_labels)),\n"
        "                'square_bounds_anat': tuple(int(v) for v in bounds_anat),\n"
        '            })\n'
        '\n'
        '        if not plane_records:\n'
        "            print('[34c] no planes available for regional QA rendering.')\n"
        '        else:\n'
        '            REGION_QA_OUT_DIR.mkdir(parents=True, exist_ok=True)\n'
        '            saved_paths = []\n'
        '            for rec in plane_records:\n'
        "                x0, y0, x1, y1 = _crop_from_bounds(rec['crop_bounds'], rec['labels_display'].shape, "
        'REGION_QA_PAD_PX)\n'
        "                overlay = _label_overlay_local(rec['ref_img'], rec['labels_display'], REGION_QA_LABEL_ALPHA)\n"
        '                fig, ax = plt.subplots(1, 1, figsize=(5.8, 5.8))\n'
        '                ax.imshow(overlay)\n'
        "                ax.imshow(_outline_rgba(rec['anat_display'], REGION_QA_ANAT_OUTLINE_RGBA))\n"
        '                ax.set_xlim(x0, x1)\n'
        '                ax.set_ylim(y1, y0)\n'
        '                ax.set_title(f"{rec[\'plane_label\']}: the selected square stays on the same anatomy '
        'neighborhood (best z={rec[\'best_z\']}, labels={rec[\'n_anat_labels\']})")\n'
        "                ax.axis('off')\n"
        '                fig.suptitle(\n'
        '                    f"Functional labels land on the same anatomy region in the selected QA square '
        '(ref={rec[\'ref_src\']}, labels={rec[\'labels_src\']})",\n'
        '                    y=0.98,\n'
        '                )\n'
        '                plt.tight_layout()\n'
        '                plt.show()\n'
        '                if REGION_QA_SAVE:\n'
        '                    out_path = REGION_QA_OUT_DIR / '
        'f"regional_match_review_plane{int(rec[\'plane_idx\'])}.png"\n'
        "                    fig.savefig(out_path, dpi=180, bbox_inches='tight')\n"
        '                    saved_paths.append(str(out_path))\n'
        '                plt.close(fig)\n'
        '            if REGION_QA_SAVE and saved_paths:\n'
        "                print(f'[34c] rendered {len(saved_paths)} plane QA figure(s) from square {square_path} -> "
        "{REGION_QA_OUT_DIR}')\n",
 '38': '# [38]\n'
       'from pathlib import Path\n'
       'import pandas as pd\n'
       '\n'
       'try:\n'
       '    DATA_MODE = str(DATA_MODE).strip().lower()\n'
       'except NameError:\n'
       "    DATA_MODE = 'nas'\n"
       'NAS_ROOT = Path(NAS_ROOT)\n'
       'try:\n'
       '    DATA_ROOT = Path(DATA_ROOT)\n'
       'except NameError:\n'
       '    try:\n'
       '        DATA_ROOT = Path(LOCAL_ROOT)\n'
       '    except NameError:\n'
       '        DATA_ROOT = Path(NAS_ROOT)\n'
       'try:\n'
       '    _MATCHING_METADATA_CSV_OVERRIDE = MATCHING_METADATA_CSV_OVERRIDE\n'
       'except NameError:\n'
       '    _MATCHING_METADATA_CSV_OVERRIDE = None\n'
       'if _MATCHING_METADATA_CSV_OVERRIDE in (None, "", False):\n'
       '    MATCHING_METADATA_CSV = Path((DATA_ROOT / "matchingMetadata.csv") if DATA_MODE == "local" else (NAS_ROOT / '
       '"Danin" / "matchingMetadata.csv"))\n'
       'else:\n'
       '    MATCHING_METADATA_CSV = Path(_MATCHING_METADATA_CSV_OVERRIDE)\n'
       'try:\n'
       '    _MANIFEST_OUT_OVERRIDE = MANIFEST_OUT_OVERRIDE\n'
       'except NameError:\n'
       '    _MANIFEST_OUT_OVERRIDE = None\n'
       'if _MANIFEST_OUT_OVERRIDE in (None, "", False):\n'
       '    MANIFEST_OUT = Path((DATA_ROOT / "confocal_mask_manifest.csv") if DATA_MODE == "local" else (NAS_ROOT / '
       '"Danin" / "confocal_mask_manifest.csv"))\n'
       'else:\n'
       '    MANIFEST_OUT = Path(_MANIFEST_OUT_OVERRIDE)\n'
       'MASK_GLOB = "03_analysis/confocal/raw/cp_masks/*round{round_idx}*_cp_masks.tif"\n'
       '# Override per fish/round if filenames differ (e.g., Cellpose). Example structure:\n'
       '# HARDCODED_MASKS = {\n'
       '#     "L395_f11": {\n'
       '#         1: ["/path/to/L395_f11_round1_mask.tif"],\n'
       '#         2: ["/path/to/L395_f11_round2_mask.nrrd"],\n'
       '#     }\n'
       '# }\n'
       'HARDCODED_MASKS = {}\n'
       'try:\n'
       '    NOTEBOOK_OWNER = OWNER\n'
       'except NameError:\n'
       '    NOTEBOOK_OWNER = None\n'
       'try:\n'
       '    NOTEBOOK_FISH_ID = FISH_ID\n'
       'except NameError:\n'
       '    NOTEBOOK_FISH_ID = None\n'
       '\n'
       '\n'
       'def owner_root(data_root, owner):\n'
       '    root = Path(data_root)\n'
       '    if DATA_MODE == "local":\n'
       '        return root\n'
       '    base = root / owner\n'
       '    mic = base / "Microscopy"\n'
       '    return mic if mic.exists() else base\n'
       '\n'
       'def find_transform_pair(fish_dir, src_round, target_tags, include_rbest=False):\n'
       '    tm_root = fish_dir / "02_reg"\n'
       '    candidates = sorted(tm_root.glob("**/transMatrices/*"), key=lambda p: p.stat().st_mtime, reverse=True)\n'
       '\n'
       '    s = str(src_round).lower()\n'
       '    src_tokens = []\n'
       '    if s in ("rbest", "best"):\n'
       '        src_tokens.append("rbest")\n'
       '    else:\n'
       '        idx = None\n'
       '        try:\n'
       '            idx = int(re.sub(r"[^0-9]", "", s))\n'
       '        except Exception:\n'
       '            idx = None\n'
       '        if idx is not None:\n'
       '            src_tokens.extend([f"round{idx}", f"r{idx}"])\n'
       '    if include_rbest and "rbest" not in src_tokens:\n'
       '        src_tokens.append("rbest")\n'
       '\n'
       '    warp = affine = None\n'
       '    for f in candidates:\n'
       '        name = f.name.lower()\n'
       '        if not any(tok in name for tok in src_tokens):\n'
       '            continue\n'
       '        if not any(tag in name for tag in target_tags):\n'
       '            continue\n'
       '        if "inverse" in name:\n'
       '            continue\n'
       '        if ("warp" in name) and name.endswith(".nii.gz") and warp is None:\n'
       '            warp = f\n'
       '        if ("affine" in name) and f.suffix == ".mat" and affine is None:\n'
       '            affine = f\n'
       '        if warp and affine:\n'
       '            break\n'
       '    return warp, affine\n'
       '\n'
       'def resolve_fish_dir(data_root, owner, fish_id):\n'
       '    root = Path(data_root)\n'
       '    if DATA_MODE == "local":\n'
       '        candidates = [root / fish_id]\n'
       '    else:\n'
       '        base = root / owner\n'
       '        mic = base / "Microscopy"\n'
       '        candidates = [mic / fish_id, base / fish_id]\n'
       '    for c in candidates:\n'
       '        if (c / "03_analysis/confocal/raw").exists():\n'
       '            return c\n'
       '    for c in candidates:\n'
       '        if c.exists():\n'
       '            return c\n'
       '    return candidates[0]\n'
       '\n'
       'def build_manifest_rows(best_row):\n'
       '    fish_id = str(best_row.fish_id)\n'
       '    best_round = str(best_row.best_round).lower()\n'
       '    best_idx = int(best_round.lstrip("r"))\n'
       '    num_rounds = int(best_row.num_rounds)\n'
       '    owner = str(NOTEBOOK_OWNER) if NOTEBOOK_OWNER else str(best_row.owner)\n'
       '    fish_dir = resolve_fish_dir(DATA_ROOT, owner, fish_id)\n'
       '    ref = fish_dir / "02_reg" / "00_preprocessing" / "2p_anatomy" / f"{fish_id}_anatomy_2P_GCaMP.nrrd"\n'
       '    if not ref.exists():\n'
       '        print(f"[WARN] Missing 2P anatomy for {fish_id}")\n'
       '        return []\n'
       '    best2p_warp, best2p_aff = find_transform_pair(fish_dir, best_idx, target_tags=["2p", "ref"], '
       'include_rbest=True)\n'
       '    if not (best2p_warp and best2p_aff):\n'
       '        print(f"[WARN] Missing best->2p transforms for {fish_id} ({best_round})")\n'
       '        return []\n'
       '    rows = []\n'
       '    overrides = HARDCODED_MASKS.get(fish_id, {})\n'
       '    for round_idx in range(1, num_rounds + 1):\n'
       '        if round_idx in overrides:\n'
       '            masks = []\n'
       '            for p in overrides[round_idx]:\n'
       '                p = Path(p)\n'
       '                if p.exists():\n'
       '                    masks.append(p)\n'
       '                else:\n'
       '                    print(f"[WARN] Override mask missing for {fish_id} round {round_idx}: {p}")\n'
       '        else:\n'
       '            masks = list(fish_dir.glob(MASK_GLOB.format(round_idx=round_idx)))\n'
       '        if not masks:\n'
       '            print(f"[WARN] No masks found for {fish_id} round {round_idx}")\n'
       '            continue\n'
       '        if round_idx != best_idx:\n'
       '            r_to_best = find_transform_pair(fish_dir, round_idx, target_tags=[f"r{best_idx}", '
       'f"round{best_idx}", "rbest"])\n'
       '            if not all(r_to_best):\n'
       '                print(f"[WARN] Missing r{round_idx}->r{best_idx} transforms for {fish_id}")\n'
       '                continue\n'
       '        for mask in masks:\n'
       '            out_dir = mask.parent / "aligned"\n'
       '            out_dir.mkdir(parents=True, exist_ok=True)\n'
       '            out_path = out_dir / f"{mask.stem}_in_2p{mask.suffix}"\n'
       '            transform_chain = [best2p_warp, best2p_aff]\n'
       '            if round_idx != best_idx:\n'
       '                transform_chain += [r_to_best[0], r_to_best[1]]\n'
       '            label = f"r{round_idx}_to_2p" if round_idx == best_idx else f"r{round_idx}_to_2p_via_r{best_idx}"\n'
       '            rows.append({\n'
       '                "moving": str(mask),\n'
       '                "reference": str(ref),\n'
       '                "transforms": "; ".join(str(t) for t in transform_chain if t),\n'
       '                "output": str(out_path),\n'
       '                "fish_id": fish_id,\n'
       '                "label": label,\n'
       '                "owner": owner\n'
       '            })\n'
       '    return rows\n'
       '\n'
       'meta_df = pd.read_csv(MATCHING_METADATA_CSV)\n'
       'required = {"fish_id", "best_round", "num_rounds"}\n'
       'if NOTEBOOK_OWNER is None:\n'
       '    required.add("owner")\n'
       'missing = required - set(meta_df.columns)\n'
       'if missing:\n'
       '    raise ValueError(f"{MATCHING_METADATA_CSV} missing columns: {missing}")\n'
       'if NOTEBOOK_FISH_ID:\n'
       '    meta_df = meta_df[meta_df["fish_id"].astype(str) == str(NOTEBOOK_FISH_ID)]\n'
       '    if meta_df.empty:\n'
       '        print(f"[WARN] No rows for fish_id={NOTEBOOK_FISH_ID} in {MATCHING_METADATA_CSV}")\n'
       '\n'
       'manifest_rows = []\n'
       'for row in meta_df.itertuples():\n'
       '    manifest_rows.extend(build_manifest_rows(row))\n'
       '\n'
       'manifest_df = pd.DataFrame(manifest_rows)\n'
       "pd.set_option('display.max_colwidth', None)\n"
       'def _rel_path_str(p, base):\n'
       '    if p is None:\n'
       '        return None\n'
       '    try:\n'
       '        return str(Path(p).relative_to(base))\n'
       '    except Exception:\n'
       '        return str(p)\n'
       '\n'
       'def _rel_transforms(s, base):\n'
       '    try:\n'
       "        parts = [t.strip() for t in str(s).split(';') if t.strip()]\n"
       "        return '; '.join(_rel_path_str(p, base) for p in parts)\n"
       '    except Exception:\n'
       '        return str(s)\n'
       '\n'
       'print(f"[INFO] built {len(manifest_df)} manifest rows")\n'
       'if not manifest_df.empty:\n'
       '    MANIFEST_OUT.parent.mkdir(parents=True, exist_ok=True)\n'
       '    manifest_df.to_csv(MANIFEST_OUT, index=False)\n'
       '    display_df = manifest_df.copy()\n'
       "    for col in ('moving','reference','output','transforms'):\n"
       '        if col not in display_df.columns:\n'
       '            continue\n'
       '        if col == "transforms":\n'
       '            display_df[col] = display_df.apply(lambda r: _rel_transforms(r[col], owner_root(DATA_ROOT, '
       "r.get('owner'))), axis=1)\n"
       '        else:\n'
       '            display_df[col] = display_df.apply(lambda r: _rel_path_str(r[col], owner_root(DATA_ROOT, '
       "r.get('owner'))), axis=1)\n"
       '    from IPython.display import HTML as _HTML, display as _display\n'
       '    MANIFEST_DISPLAY_MAX_ROWS = 0\n'
       "    MANIFEST_DISPLAY_MAX_HEIGHT = '400px'\n"
       '    def _df_scrollable(df):\n'
       '        if MANIFEST_DISPLAY_MAX_ROWS and len(df) > MANIFEST_DISPLAY_MAX_ROWS:\n'
       '            df = df.head(MANIFEST_DISPLAY_MAX_ROWS)\n'
       '        html = df.to_html(index=False)\n'
       '        return _HTML(f"<div style=\\"max-height:{MANIFEST_DISPLAY_MAX_HEIGHT}; overflow:auto; border:1px solid '
       '#ccc;\\">{html}</div>")\n'
       '    _display(_df_scrollable(display_df))\n'
       'else:\n'
       '    print("[WARN] Manifest is empty; check warnings above.")',
 '40': '# [40]\n'
       'try:\n'
       '    REQUIRE_FISH_STATE = require_fish_state\n'
       'except NameError:\n'
       '    REQUIRE_FISH_STATE = None\n'
       'if callable(REQUIRE_FISH_STATE) and not REQUIRE_FISH_STATE():\n'
       '    raise SystemExit\n'
       '\n'
       'try:\n'
       '    PLANE_REFS_LOCAL = plane_refs if plane_refs else []\n'
       'except NameError:\n'
       '    PLANE_REFS_LOCAL = []\n'
       'try:\n'
       '    BEST_Z_LOCAL = int(best_z)\n'
       'except Exception:\n'
       '    BEST_Z_LOCAL = 0\n'
       'try:\n'
       '    TFORM_LOCAL = tform if tform is not None else None\n'
       'except NameError:\n'
       '    TFORM_LOCAL = None\n'
       'try:\n'
       '    DATA_MODE_LOCAL = DATA_MODE\n'
       'except NameError:\n'
       '    DATA_MODE_LOCAL = None\n'
       'try:\n'
       '    DATA_ROOT_LOCAL = DATA_ROOT\n'
       'except NameError:\n'
       '    DATA_ROOT_LOCAL = None\n'
       'try:\n'
       '    FUNC_RAW_STACK_PATH_LOCAL = FUNC_RAW_STACK_PATH\n'
       'except NameError:\n'
       '    FUNC_RAW_STACK_PATH_LOCAL = None\n'
       'try:\n'
       '    ANAT_STACK_PATH_LOCAL = ANAT_STACK_PATH\n'
       'except NameError:\n'
       '    ANAT_STACK_PATH_LOCAL = None\n'
       'try:\n'
       '    HCR_STACK_PATH_LOCAL = HCR_STACK_PATH\n'
       'except NameError:\n'
       '    HCR_STACK_PATH_LOCAL = None\n'
       'try:\n'
       '    REF_BUILD_STRATEGY_LOCAL = REF_BUILD_STRATEGY\n'
       'except NameError:\n'
       '    REF_BUILD_STRATEGY_LOCAL = None\n'
       'try:\n'
       '    NCC_SCALE_PER_PLANE_LOCAL = bool(NCC_SCALE_PER_PLANE)\n'
       'except NameError:\n'
       '    NCC_SCALE_PER_PLANE_LOCAL = False\n'
       'try:\n'
       '    VOX_FUNC_LOCAL = VOX_FUNC\n'
       'except NameError:\n'
       '    VOX_FUNC_LOCAL = None\n'
       'try:\n'
       '    VOX_ANAT_LOCAL = VOX_ANAT\n'
       'except NameError:\n'
       '    VOX_ANAT_LOCAL = None\n'
       'try:\n'
       '    VOX_HCR_LOCAL = VOX_HCR\n'
       'except NameError:\n'
       '    VOX_HCR_LOCAL = None\n'
       'try:\n'
       '    RNG_SEED_LOCAL = RNG_SEED\n'
       'except NameError:\n'
       '    RNG_SEED_LOCAL = None\n'
       'try:\n'
       '    FORCE_RECOMPUTE_HCR_WARP_LOCAL = bool(FORCE_RECOMPUTE_HCR_WARP)\n'
       'except NameError:\n'
       '    try:\n'
       '        FORCE_RECOMPUTE_HCR_WARP_LOCAL = bool(FORCE_RECOMPUTE)\n'
       '    except NameError:\n'
       '        FORCE_RECOMPUTE_HCR_WARP_LOCAL = False\n'
       'try:\n'
       '    BIGWARP_FROM_METADATA_LOCAL = bool(BIGWARP_FROM_METADATA)\n'
       'except NameError:\n'
       '    BIGWARP_FROM_METADATA_LOCAL = None\n'
       'try:\n'
       '    BIGWARP_ENABLED_LOCAL = bool(BIGWARP_ENABLED)\n'
       'except NameError:\n'
       '    BIGWARP_ENABLED_LOCAL = None\n'
       'try:\n'
       '    BIGWARP_NGFF_PATH_LOCAL = BIGWARP_NGFF_PATH\n'
       'except NameError:\n'
       '    BIGWARP_NGFF_PATH_LOCAL = None\n'
       'try:\n'
       '    BIGWARP_NGFF_LEVEL_LOCAL = BIGWARP_NGFF_LEVEL\n'
       'except NameError:\n'
       '    BIGWARP_NGFF_LEVEL_LOCAL = None\n'
       'try:\n'
       '    BIGWARP_NGFF_INFO_LOCAL = BIGWARP_NGFF_INFO\n'
       'except NameError:\n'
       '    BIGWARP_NGFF_INFO_LOCAL = None\n'
       'try:\n'
       '    SAVE_RN_TO_RBEST_OUTPUTS_LOCAL = bool(SAVE_RN_TO_RBEST_OUTPUTS)\n'
       'except NameError:\n'
       '    SAVE_RN_TO_RBEST_OUTPUTS_LOCAL = None\n'
       'try:\n'
       '    RN_TO_RBEST_LABELS_DIR_LOCAL = RN_TO_RBEST_LABELS_DIR\n'
       'except NameError:\n'
       '    RN_TO_RBEST_LABELS_DIR_LOCAL = None\n'
       'try:\n'
       '    RN_TO_RBEST_INTENSITY_DIR_LOCAL = RN_TO_RBEST_INTENSITY_DIR\n'
       'except NameError:\n'
       '    RN_TO_RBEST_INTENSITY_DIR_LOCAL = None\n'
       'try:\n'
       '    BIGWARP_TRANSFORM_TYPE_LOCAL = BIGWARP_TRANSFORM_TYPE\n'
       'except NameError:\n'
       '    BIGWARP_TRANSFORM_TYPE_LOCAL = None\n'
       '\n'
       '# Build per-plane metadata if available\n'
       'planes = []\n'
       'if PLANE_REFS_LOCAL:\n'
       '    for i, pr in enumerate(PLANE_REFS_LOCAL):\n'
       "        label = pr.get('label', f'plane{i}')\n"
       "        tform_plane = pr.get('tform', None)\n"
       "        tform_src = pr.get('tform_src', None)\n"
       "        ncc_xy = pr.get('ncc_xy', None)\n"
       "        best_z_plane = int(pr.get('best_z', BEST_Z_LOCAL))\n"
       '        tform_params = tform_plane.params.tolist() if tform_plane is not None else None\n'
       "        ref_shape = pr.get('ref_shape', None)\n"
       "        ref_scaled_shape = pr.get('ref_scaled_shape', None)\n"
       "        plane_scale = pr.get('scale', None)\n"
       '        try:\n'
       '            plane_scale = float(plane_scale) if plane_scale is not None else None\n'
       '        except Exception:\n'
       '            plane_scale = None\n'
       '        planes.append({\n'
       "            'plane': i,\n"
       "            'label': label,\n"
       "            'best_z': best_z_plane,\n"
       "            'scale': plane_scale,\n"
       "            'tform_src': tform_src,\n"
       "            'tform_params_2x3': tform_params,\n"
       "            'ncc_xy': ncc_xy,\n"
       "            'ref_shape': tuple(ref_shape) if ref_shape is not None else None,\n"
       "            'ref_scaled_shape': tuple(ref_scaled_shape) if ref_scaled_shape is not None else None,\n"
       '        })\n'
       '\n'
       'meta = {\n'
       "    'data_mode': str(DATA_MODE_LOCAL) if DATA_MODE_LOCAL is not None else None,\n"
       "    'data_root': str(DATA_ROOT_LOCAL) if DATA_ROOT_LOCAL else None,\n"
       "    'FUNC_STACK_PATH': str(FUNC_RAW_STACK_PATH_LOCAL) if FUNC_RAW_STACK_PATH_LOCAL else None,\n"
       "    'ANAT_STACK_PATH': str(ANAT_STACK_PATH_LOCAL) if ANAT_STACK_PATH_LOCAL else None,\n"
       "    'HCR_STACK_PATH': str(HCR_STACK_PATH_LOCAL) if HCR_STACK_PATH_LOCAL else None,\n"
       "    'best_z': BEST_Z_LOCAL,\n"
       "    'transform_params_2x3': TFORM_LOCAL.params.tolist() if TFORM_LOCAL is not None else None,\n"
       "    'ref_build_strategy': str(REF_BUILD_STRATEGY_LOCAL) if REF_BUILD_STRATEGY_LOCAL is not None else None,\n"
       "    'ncc_scale_mode': 'per_plane' if NCC_SCALE_PER_PLANE_LOCAL else 'single',\n"
       "    'voxels': {\n"
       "        'func': VOX_FUNC_LOCAL,\n"
       "        'anat': VOX_ANAT_LOCAL,\n"
       "        'hcr': VOX_HCR_LOCAL,\n"
       '    },\n'
       "    'voxel_cache': str(VOX_CACHE_PATH),\n"
       "    'rng_seed': RNG_SEED_LOCAL,\n"
       "    'hcr_warp': {\n"
       "        'force_recompute': FORCE_RECOMPUTE_HCR_WARP_LOCAL,\n"
       "        'bigwarp_metadata_flag': BIGWARP_FROM_METADATA_LOCAL,\n"
       "        'bigwarp_enabled': BIGWARP_ENABLED_LOCAL,\n"
       "        'bigwarp_ngff_path': str(BIGWARP_NGFF_PATH_LOCAL) if BIGWARP_NGFF_PATH_LOCAL else None,\n"
       "        'bigwarp_ngff_level': str(BIGWARP_NGFF_LEVEL_LOCAL) if BIGWARP_NGFF_LEVEL_LOCAL else None,\n"
       "        'bigwarp_ngff_info': BIGWARP_NGFF_INFO_LOCAL,\n"
       "        'save_rn_to_rbest_outputs': SAVE_RN_TO_RBEST_OUTPUTS_LOCAL,\n"
       "        'rn_to_rbest_labels_dir': str(RN_TO_RBEST_LABELS_DIR_LOCAL) if RN_TO_RBEST_LABELS_DIR_LOCAL else "
       'None,\n'
       "        'rn_to_rbest_intensity_dir': str(RN_TO_RBEST_INTENSITY_DIR_LOCAL) if RN_TO_RBEST_INTENSITY_DIR_LOCAL "
       'else None,\n'
       "        'bigwarp_transform_type': str(BIGWARP_TRANSFORM_TYPE_LOCAL) if BIGWARP_TRANSFORM_TYPE_LOCAL else "
       'None,\n'
       '    },\n'
       "    'planes': planes,\n"
       '}\n'
       '\n'
       "with open(OUT_REG / 'run_metadata.json', 'w') as f:\n"
       '    json.dump(meta, f, indent=2)\n'
       '\n'
       "print('Wrote', OUT_REG / 'run_metadata.json')\n",
 '41': '# HCR→2P configuration (external FIJI BigWarp workflow)\n'
       'import re, json\n'
       'from pathlib import Path\n'
       'import pandas as pd\n'
       'import numpy as np\n'
       '\n'
       "MATCHING_METADATA_CSV = DATA_ROOT / 'matchingMetadata.csv' if str(DATA_MODE).strip().lower() == 'local' else "
       "(NAS_ROOT / 'Danin' / 'matchingMetadata.csv')\n"
       'BEST_ROUND_OVERRIDE = None\n'
       'FORCE_RECOMPUTE_HCR_WARP = False\n'
       'HCR_CP_MASK_DIR = FISH_DIR / "03_analysis" / "confocal" / "raw" / "cp_masks"\n'
       'HCR_ALIGNED_DIR = FISH_DIR / "03_analysis" / "confocal" / "aligned"\n'
       'HCR_RN_TO_RBEST_DIR = FISH_DIR / "03_analysis" / "confocal" / "rn_to_rbest"\n'
       'RN_TO_RBEST_LABELS_DIR = HCR_RN_TO_RBEST_DIR / "labels"\n'
       'RN_TO_RBEST_INTENSITY_DIR = HCR_RN_TO_RBEST_DIR / "intensity"\n'
       '\n'
       'HCR_ALIGNED_DIR.mkdir(parents=True, exist_ok=True)\n'
       'HCR_RN_TO_RBEST_DIR.mkdir(parents=True, exist_ok=True)\n'
       '\n'
       'PREPROC_DIR = FISH_DIR / "02_reg" / "00_preprocessing"\n'
       'RBEST_TM_DIR = FISH_DIR / "02_reg" / "01_rbest-2p" / "transMatrices"\n'
       'RN_TO_RBEST_TM_DIR = FISH_DIR / "02_reg" / "02_rn-rbest" / "transMatrices"\n'
       'ANAT_INT_PATH = PREPROC_DIR / "2p_anatomy" / f"{FISH_ID}_anatomy_2P_GCaMP.nrrd"\n'
       '\n'
       "MATCH_METHOD = 'nn'\n"
       'MAX_DISTANCE_UM = 10.0\n'
       'REQUIRE_OVERLAP = True\n'
       'MIN_OVERLAP_VOXELS = 1\n'
       "DEDUP_BY_TWOP = 'max_overlap'\n"
       'IOU_MIN = 0.05\n'
       'USE_FRAC_FILTERS = False\n'
       'MIN_OVERLAP_FRAC_CONF = 0.0\n'
       'MIN_OVERLAP_FRAC_TWOP = 0.0\n'
       'PLOT_USE_FINAL_1TO1 = True\n'
       'INVERT_AFFINE = False  # Match applyTransform.py: forward transform chain, no affine inversion.\n'
       '\n'
       '\n'
       'def _parse_round_from_name(path: Path):\n'
       '    m = re.search(r"round(\\d+)", path.name.lower())\n'
       '    return int(m.group(1)) if m else None\n'
       '\n'
       '\n'
       'def _as_bool(value, default=False):\n'
       '    if value is None:\n'
       '        return bool(default)\n'
       '    try:\n'
       '        if pd.isna(value):\n'
       '            return bool(default)\n'
       '    except Exception:\n'
       '        pass\n'
       '    if isinstance(value, bool):\n'
       '        return value\n'
       '    if isinstance(value, (int, float)):\n'
       '        return bool(value)\n'
       '    s = str(value).strip().lower()\n'
       '    if s in {"1", "true", "t", "yes", "y", "on"}:\n'
       '        return True\n'
       '    if s in {"0", "false", "f", "no", "n", "off", "", "nan", "none", "null"}:\n'
       '        return False\n'
       '    return bool(default)\n'
       '\n'
       '\n'
       'def _load_matching_row(fish_id: str):\n'
       '    df = pd.read_csv(MATCHING_METADATA_CSV)\n'
       '    row = df.loc[df["fish_id"].astype(str) == str(fish_id)]\n'
       '    if row.empty:\n'
       '        raise ValueError(f"No metadata row found for fish_id={fish_id} in {MATCHING_METADATA_CSV}")\n'
       '    return df, row.iloc[0]\n'
       '\n'
       '\n'
       '_META_DF, _META_ROW = _load_matching_row(FISH_ID)\n'
       '\n'
       '\n'
       'def _load_best_round(fish_id: str):\n'
       '    if BEST_ROUND_OVERRIDE is not None:\n'
       '        val = str(BEST_ROUND_OVERRIDE).lower().lstrip("r")\n'
       '        return int(val), None\n'
       '    best_round = int(str(_META_ROW["best_round"]).lower().lstrip("r"))\n'
       '    num_rounds = int(_META_ROW["num_rounds"])\n'
       '    return best_round, num_rounds\n'
       '\n'
       '\n'
       'def _find_intensity_for_round(round_idx: int):\n'
       '    sub = PREPROC_DIR / ("rbest" if round_idx == best_round_idx else "rn")\n'
       '    candidates = sorted(sub.glob("*"))\n'
       '    chosen = None\n'
       '    for p in candidates:\n'
       '        name = p.name.lower()\n'
       '        if f"round{round_idx}" in name and "channel1" in name and "gcamp" in name and p.suffix.lower() == '
       "'.nrrd':\n"
       '            chosen = p\n'
       '            break\n'
       '    if chosen is None:\n'
       '        hits = [p for p in candidates if f"round{round_idx}" in p.name.lower() and p.suffix.lower() == '
       "'.nrrd']\n"
       '        if hits:\n'
       '            chosen = hits[0]\n'
       '    return chosen\n'
       '\n'
       '\n'
       'def _find_hcr_intensity_for_mask(mask_path: Path, round_idx: int):\n'
       '    """Find the preprocessed HCR intensity that corresponds to a cp-mask TIFF."""\n'
       '    sub = PREPROC_DIR / ("rbest" if round_idx == best_round_idx else "rn")\n'
       '    if not sub.exists():\n'
       '        return None\n'
       '\n'
       '    stem = Path(mask_path).stem\n'
       "    base = stem[:-9] if stem.endswith('_cp_masks') else stem\n"
       '    exact = sub / f"{base}.nrrd"\n'
       '    if exact.exists():\n'
       '        return exact\n'
       '\n'
       '    ch_match = re.search(r"channel(\\d+)", stem.lower())\n'
       '    if not ch_match:\n'
       '        return None\n'
       '    ch = int(ch_match.group(1))\n'
       '\n'
       '    pattern = f"{FISH_ID}_round{round_idx}_channel{ch}_*.nrrd"\n'
       '    candidates = [p for p in sorted(sub.glob(pattern)) if "gcamp" not in p.name.lower()]\n'
       '    if not candidates:\n'
       '        return None\n'
       '\n'
       '    gene_match = re.search(rf"round{round_idx}_channel{ch}_(.+?)(?:_cp_masks)?$", stem.lower())\n'
       '    gene_token = gene_match.group(1) if gene_match else None\n'
       '    if gene_token:\n'
       '        for p in candidates:\n'
       '            if gene_token in p.stem.lower():\n'
       '                return p\n'
       '\n'
       '    return candidates[0]\n'
       '\n'
       '\n'
       'def _find_transform(trans_dir: Path, round_idx: int, target_tag: str, kind: str):\n'
       '    """Flexible transform lookup for transMatrices."""\n'
       '    if trans_dir is None or not trans_dir.exists():\n'
       '        return None\n'
       '    if kind not in {"warp", "affine"}:\n'
       '        raise ValueError(f"Unknown transform kind: {kind}")\n'
       '\n'
       '    suffix = "1Warp.nii.gz" if kind == "warp" else "0GenericAffine.mat"\n'
       '    round_idx = int(round_idx)\n'
       '    target_tag = str(target_tag or "").lower().strip()\n'
       '\n'
       '    all_files = [\n'
       '        p for p in sorted(trans_dir.glob(f"*{suffix}"))\n'
       '        if "inverse" not in p.name.lower()\n'
       '    ]\n'
       '    if not all_files:\n'
       '        return None\n'
       '\n'
       '    def _has_source_round(name: str) -> bool:\n'
       '        if re.search(rf"(^|[_-])round{round_idx}(?=([_.-]|$))", name):\n'
       '            return True\n'
       '        if re.search(rf"(^|[_-])r{round_idx}(?=([_.-]|$))", name):\n'
       '            return True\n'
       '        if best_round_idx is not None and int(round_idx) == int(best_round_idx):\n'
       '            if re.search(r"(^|[_-])rbest(?=([_.-]|$))", name):\n'
       '                return True\n'
       '        return False\n'
       '\n'
       '    def _target_aliases(tag: str):\n'
       '        if tag == "to_2p":\n'
       '            return ["to_2p", "in_2p", "to2p"]\n'
       '        m = re.match(r"to_r(\\d+)$", tag)\n'
       '        if m:\n'
       '            trg = int(m.group(1))\n'
       '            aliases = [f"to_r{trg}", f"in_r{trg}"]\n'
       '            if best_round_idx is not None and int(trg) == int(best_round_idx):\n'
       '                aliases += ["to_rbest", "in_rbest"]\n'
       '            return aliases\n'
       '        return [tag] if tag else []\n'
       '\n'
       '    source_hits = [p for p in all_files if _has_source_round(p.name.lower())]\n'
       '\n'
       '    target_aliases = _target_aliases(target_tag)\n'
       '    if target_aliases:\n'
       '        both_hits = [\n'
       '            p for p in source_hits\n'
       '            if any(alias in p.name.lower() for alias in target_aliases)\n'
       '        ]\n'
       '        if len(both_hits) == 1:\n'
       '            return both_hits[0]\n'
       '        if len(both_hits) > 1:\n'
       '            print(f"[HCR] Ambiguous {kind} for round r{round_idx}, target {target_tag}; using '
       '{both_hits[0].name}")\n'
       '            return both_hits[0]\n'
       '\n'
       '    if len(source_hits) == 1:\n'
       '        return source_hits[0]\n'
       '    if len(source_hits) > 1:\n'
       '        print(f"[HCR] Ambiguous {kind} for round r{round_idx}; using {source_hits[0].name}")\n'
       '        return source_hits[0]\n'
       '\n'
       '    if len(all_files) == 1:\n'
       '        return all_files[0]\n'
       '\n'
       '    print(f"[HCR] Missing unique {kind} for round r{round_idx} in {trans_dir}")\n'
       '    return None\n'
       '\n'
       '\n'
       'best_round_idx, num_rounds = _load_best_round(FISH_ID)\n'
       '\n'
       'BIGWARP_FROM_METADATA = _as_bool(_META_ROW.get("bigwarp", False), default=False) if "bigwarp" in '
       '_META_DF.columns else False\n'
       'USE_EXTERNAL_FIJI_BIGWARP = bool(BIGWARP_FROM_METADATA)\n'
       'PREPARE_RN_TO_RBEST_LABELS = bool(USE_EXTERNAL_FIJI_BIGWARP)\n'
       'PREPARE_RN_TO_RBEST_INTENSITY = bool(USE_EXTERNAL_FIJI_BIGWARP)\n'
       'PREPARE_RN_TO_RBEST_LABELS = bool(PREPARE_RN_TO_RBEST_LABELS and USE_EXTERNAL_FIJI_BIGWARP)\n'
       'PREPARE_RN_TO_RBEST_INTENSITY = bool(PREPARE_RN_TO_RBEST_INTENSITY and USE_EXTERNAL_FIJI_BIGWARP)\n'
       'PREPARE_RN_TO_RBEST_LABELS = PREPARE_RN_TO_RBEST_LABELS\n'
       'PREPARE_RN_TO_RBEST_INTENSITY = PREPARE_RN_TO_RBEST_INTENSITY\n'
       "RN_TO_RBEST_INTENSITY_FORMAT = 'tif'\n"
       'if RN_TO_RBEST_INTENSITY_FORMAT not in {"tif", "tiff", "nrrd"}:\n'
       '    print(f"[HCR] Unsupported RN_TO_RBEST_INTENSITY_FORMAT={RN_TO_RBEST_INTENSITY_FORMAT}; using \'tif\'")\n'
       '    RN_TO_RBEST_INTENSITY_FORMAT = "tif"\n'
       '\n'
       'if PREPARE_RN_TO_RBEST_LABELS:\n'
       '    RN_TO_RBEST_LABELS_DIR.mkdir(parents=True, exist_ok=True)\n'
       'if PREPARE_RN_TO_RBEST_INTENSITY:\n'
       '    RN_TO_RBEST_INTENSITY_DIR.mkdir(parents=True, exist_ok=True)\n'
       '\n'
       'HCR_ALIGNED_DIRS = [HCR_ALIGNED_DIR]\n'
       '\n'
       'print(f"[HCR] fish={FISH_ID} best_round=r{best_round_idx} num_rounds={num_rounds}")\n'
       'print(f"[HCR] ANAT_INT_PATH: {ANAT_INT_PATH}")\n'
       'print(f"[HCR] RBEST_TM_DIR: {RBEST_TM_DIR}")\n'
       'print(f"[HCR] RN_TO_RBEST_TM_DIR: {RN_TO_RBEST_TM_DIR}")\n'
       'print(f"[HCR] FORCE_RECOMPUTE_HCR_WARP={FORCE_RECOMPUTE_HCR_WARP}")\n'
       'print(f"[HCR] bigwarp metadata={BIGWARP_FROM_METADATA} external_fiji={USE_EXTERNAL_FIJI_BIGWARP}")\n'
       'print(f"[HCR] PREPARE_RN_TO_RBEST_LABELS={PREPARE_RN_TO_RBEST_LABELS}")\n'
       'print(f"[HCR] PREPARE_RN_TO_RBEST_INTENSITY={PREPARE_RN_TO_RBEST_INTENSITY} '
       'format={RN_TO_RBEST_INTENSITY_FORMAT}")\n'
       'print(f"[HCR] HCR_ALIGNED_DIR: {HCR_ALIGNED_DIR}")\n'
       'print(f"[HCR] rn->rbest labels dir: {RN_TO_RBEST_LABELS_DIR}")\n'
       'print(f"[HCR] rn->rbest intensity dir: {RN_TO_RBEST_INTENSITY_DIR}")\n',
 '44': '### [44] Matching and QC for each warped mask (overlap-first; NN by default)\n'
       'try:\n'
       '    REQUIRE_FISH_STATE = require_fish_state\n'
       'except NameError:\n'
       '    REQUIRE_FISH_STATE = None\n'
       'if callable(REQUIRE_FISH_STATE) and not REQUIRE_FISH_STATE():\n'
       '    raise SystemExit\n'
       'import numpy as np\n'
       'import pandas as pd\n'
       'import tifffile as tiff\n'
       'import json\n'
       'from pathlib import Path\n'
       'from scipy.optimize import linear_sum_assignment\n'
       'from skimage.measure import regionprops_table\n'
       '\n'
       'if not warp_results:\n'
       '    print("[HCR] No warped masks to match.")\n'
       'else:\n'
       '    if ANAT_LABELS_PATH is None or not Path(ANAT_LABELS_PATH).exists():\n'
       '        raise RuntimeError("ANAT_LABELS_PATH is missing; cannot run QC.")\n'
       '    anat_labels = imread_any(ANAT_LABELS_PATH)\n'
       '    anat_labels = _ensure_uint_labels(anat_labels)\n'
       '    vox_anat_um = {\n'
       '        "dz": float(VOX_ANAT.get("Z", 1.0)) if VOX_ANAT else 1.0,\n'
       '        "dy": float(VOX_ANAT.get("Y", 1.0)) if VOX_ANAT else 1.0,\n'
       '        "dx": float(VOX_ANAT.get("X", 1.0)) if VOX_ANAT else 1.0,\n'
       '    }\n'
       '\n'
       '    hcr_match_results = []\n'
       '    total_conf_masks = 0\n'
       '    total_after_overlap = 0\n'
       '    total_after_twop = 0\n'
       '    total_after_conf = 0\n'
       '    masks_processed = 0\n'
       '    hcr_match_table_rows = []\n'
       '\n'
       '    # Parameters (prefer upstream values; otherwise use safe defaults)\n'
       '    REQUIRE_OVERLAP = True\n'
       '    MIN_OVERLAP_VOXELS = 1\n'
       '    MAX_DISTANCE_UM = np.inf\n'
       "    MATCH_METHOD = 'nn'\n"
       "    DEDUP_BY_TWOP = 'closest'\n"
       "    DEDUP_BY_CONF = 'closest'\n"
       '    IOU_MIN = 0.05\n'
       '    USE_FRAC_FILTERS = False\n'
       '    MIN_OVERLAP_FRAC_CONF = 0.0\n'
       '    MIN_OVERLAP_FRAC_TWOP = 0.0\n'
       '    PLOT_USE_FINAL_1TO1 = False\n'
       '    MIN_XY_HCR_UM = 3.0\n'
       '\n'
       '    def _hlog(*args, **kwargs):\n'
       '        return None\n'
       '    for res in warp_results:\n'
       '        conf_labels_2p = np.array(res["warped"], copy=True)\n'
       '        if conf_labels_2p.shape != anat_labels.shape and conf_labels_2p.ndim == 3:\n'
       '            # Compatibility path for legacy caches written in XYZ instead of ZYX.\n'
       '            conf_labels_2p_t = np.transpose(conf_labels_2p, (2, 1, 0))\n'
       '            if conf_labels_2p_t.shape == anat_labels.shape:\n'
       '                _hlog(\n'
       '                    f"[HCR] Auto-corrected legacy axis order for {Path(res[\'mask_path\']).name}: "\n'
       '                    f"{conf_labels_2p.shape} -> {conf_labels_2p_t.shape}"\n'
       '                )\n'
       '                conf_labels_2p = conf_labels_2p_t\n'
       '        if conf_labels_2p.shape != anat_labels.shape:\n'
       '            _hlog(f"[HCR] Shape mismatch confocal vs anat: {conf_labels_2p.shape} vs {anat_labels.shape}")\n'
       '            continue\n'
       '\n'
       '        df_conf = compute_centroids(conf_labels_2p)\n'
       '        df_2p = compute_centroids(anat_labels)\n'
       '        P_conf_um = idx_to_um(df_conf, vox_anat_um)\n'
       '        P_2p_um = idx_to_um(df_2p, vox_anat_um)\n'
       '\n'
       '        labels_conf = df_conf["label"].to_numpy()\n'
       '        labels_2p = df_2p["label"].to_numpy()\n'
       '\n'
       '        overlap_df = None\n'
       '        matches = None\n'
       '\n'
       '        if REQUIRE_OVERLAP:\n'
       '            overlap_df = compute_label_overlap(conf_labels_2p, anat_labels, '
       'min_overlap_voxels=int(MIN_OVERLAP_VOXELS))\n'
       '            if overlap_df.empty:\n'
       '                _hlog(f"[HCR] No overlapping labels found for {res[\'mask_path\']}; relax MIN_OVERLAP_VOXELS '
       'or check inputs.")\n'
       '                continue\n'
       '\n'
       '            conf_coords = dict(zip(labels_conf, P_conf_um))\n'
       '            twoP_coords = dict(zip(labels_2p, P_2p_um))\n'
       "            overlap_df['distance_um'] = overlap_df.apply(\n"
       "                lambda r: float(np.linalg.norm(conf_coords[int(r['conf_label'])] - "
       "twoP_coords[int(r['twoP_label'])])),\n"
       '                axis=1,\n'
       '            )\n'
       '\n'
       "            if MATCH_METHOD == 'hungarian':\n"
       "                conf_ids = sorted(overlap_df['conf_label'].unique())\n"
       "                twoP_ids = sorted(overlap_df['twoP_label'].unique())\n"
       '                cost_fill = 1e9\n'
       '                cost = np.full((len(conf_ids), len(twoP_ids)), cost_fill, dtype=float)\n'
       '                conf_idx = {c: i for i, c in enumerate(conf_ids)}\n'
       '                twoP_idx = {t: i for i, t in enumerate(twoP_ids)}\n'
       '                for _, r in overlap_df.iterrows():\n'
       "                    cost[conf_idx[int(r['conf_label'])], twoP_idx[int(r['twoP_label'])]] = "
       "float(r['distance_um'])\n"
       '                row_ind, col_ind = linear_sum_assignment(cost)\n'
       '                keep = cost[row_ind, col_ind] < cost_fill\n'
       '                row_ind, col_ind = row_ind[keep], col_ind[keep]\n'
       '                matches = pd.DataFrame({\n'
       "                    'conf_label': np.array(conf_ids, dtype=int)[row_ind],\n"
       "                    'twoP_label': np.array(twoP_ids, dtype=int)[col_ind],\n"
       "                    'distance_um': cost[row_ind, col_ind],\n"
       '                })\n'
       "                matches = matches.merge(overlap_df[['conf_label','twoP_label','overlap_voxels']], "
       "on=['conf_label','twoP_label'], how='left')\n"
       '            else:  # default to NN within overlap\n'
       "                best_idx = overlap_df.groupby('conf_label')['distance_um'].idxmin()\n"
       '                matches = overlap_df.loc[best_idx, '
       "['conf_label','twoP_label','distance_um','overlap_voxels']].copy()\n"
       '\n'
       "            matches['within_gate'] = matches['distance_um'] <= float(MAX_DISTANCE_UM)\n"
       '\n'
       '        else:\n'
       '            if MATCH_METHOD == "nn":\n'
       '                dists, nn = nearest_neighbor_match(P_conf_um, P_2p_um)\n'
       '                matched_twoP_labels = labels_2p[nn]\n'
       '                matched_conf_labels = labels_conf\n'
       '            elif MATCH_METHOD == "hungarian":\n'
       '                dists, col_ind, row_ind = hungarian_match(P_conf_um, P_2p_um, max_cost=np.inf)\n'
       '                matched_conf_labels = labels_conf[row_ind]\n'
       '                matched_twoP_labels = labels_2p[col_ind]\n'
       '            else:\n'
       '                raise ValueError(\'MATCH_METHOD must be "nn" or "hungarian"\')\n'
       '\n'
       '            valid = dists <= float(MAX_DISTANCE_UM)\n'
       '            matches = pd.DataFrame({\n'
       '                "conf_label": matched_conf_labels,\n'
       '                "twoP_label": matched_twoP_labels,\n'
       '                "distance_um": dists,\n'
       '                "within_gate": valid\n'
       '            }).sort_values("distance_um", ascending=True).reset_index(drop=True)\n'
       '\n'
       '            overlap_df = compute_label_overlap(conf_labels_2p, anat_labels, '
       'min_overlap_voxels=int(MIN_OVERLAP_VOXELS))\n'
       '            matches = matches.merge(overlap_df[["conf_label", "twoP_label", "overlap_voxels"]], '
       'on=["conf_label", "twoP_label"], how="left")\n'
       '            matches["overlap_voxels"] = matches["overlap_voxels"].fillna(0).astype(int)\n'
       '            if REQUIRE_OVERLAP:\n'
       '                matches["within_gate"] = matches["within_gate"] & (matches["overlap_voxels"] >= '
       'int(MIN_OVERLAP_VOXELS))\n'
       '\n'
       "        if 'overlap_voxels' not in matches.columns:\n"
       "            matches['overlap_voxels'] = 0\n"
       '\n'
       '        matches = matches.sort_values("distance_um", ascending=True).reset_index(drop=True)\n'
       '\n'
       '        # Counts at each step (left-to-right filter flow for summary table)\n'
       '        n_pairs_overlap_candidates = int(len(overlap_df)) if overlap_df is not None else int(len(matches))\n'
       '        after_overlap = int(len(matches))  # after overlap-based pair selection (NN/Hungarian)\n'
       '        after_twop = None\n'
       "        if DEDUP_BY_TWOP in {'closest','max_overlap'}:\n"
       "            m = matches['within_gate'].to_numpy()\n"
       '            if m.any():\n'
       '                sub = matches.loc[m].copy()\n'
       "                if DEDUP_BY_TWOP == 'closest':\n"
       "                    sub = sub.sort_values(['twoP_label','distance_um'], ascending=[True, True])\n"
       '                else:\n'
       "                    sub = sub.sort_values(['twoP_label','overlap_voxels','distance_um'], ascending=[True, "
       'False, True])\n'
       "                keep_idx = sub.drop_duplicates(subset=['twoP_label'], keep='first').index\n"
       '                drop_idx = sub.index.difference(keep_idx)\n'
       '                if len(drop_idx) > 0:\n'
       "                    matches.loc[drop_idx, 'within_gate'] = False\n"
       "            after_twop = int(matches['within_gate'].sum())\n"
       '        if after_twop is None:\n'
       "            after_twop = int(matches['within_gate'].sum())\n"
       '\n'
       '        after_conf = None\n'
       "        if DEDUP_BY_CONF in {'closest','max_overlap'}:\n"
       "            m = matches['within_gate'].to_numpy()\n"
       '            if m.any():\n'
       '                sub = matches.loc[m].copy()\n'
       "                if DEDUP_BY_CONF == 'closest':\n"
       "                    sub = sub.sort_values(['conf_label','distance_um'], ascending=[True, True])\n"
       '                else:\n'
       "                    sub = sub.sort_values(['conf_label','overlap_voxels','distance_um'], ascending=[True, "
       'False, True])\n'
       "                keep_idx = sub.drop_duplicates(subset=['conf_label'], keep='first').index\n"
       '                drop_idx = sub.index.difference(keep_idx)\n'
       '                if len(drop_idx) > 0:\n'
       "                    matches.loc[drop_idx, 'within_gate'] = False\n"
       "            after_conf = int(matches['within_gate'].sum())\n"
       '        if after_conf is None:\n'
       "            after_conf = int(matches['within_gate'].sum())\n"
       '\n'
       '        total_masks = int(len(labels_conf))\n'
       '\n'
       '        def label_volumes(arr):\n'
       '            labels, counts = np.unique(arr, return_counts=True)\n'
       '            s = pd.Series(counts, index=labels)\n'
       "            return s.drop(index=0, errors='ignore').astype(int)\n"
       '\n'
       '        conf_warped_labels = conf_labels_2p\n'
       '        twoP_labels = anat_labels\n'
       '\n'
       '        conf_vol_s = label_volumes(conf_warped_labels)\n'
       '        twoP_vol_s = label_volumes(twoP_labels)\n'
       '\n'
       "        matches['conf_vol'] = matches['conf_label'].map(conf_vol_s).fillna(0).astype(int)\n"
       "        matches['twoP_vol'] = matches['twoP_label'].map(twoP_vol_s).fillna(0).astype(int)\n"
       '\n'
       "        den = matches['conf_vol'] + matches['twoP_vol'] - matches['overlap_voxels']\n"
       "        matches['iou'] = np.divide(matches['overlap_voxels'], den, out=np.zeros_like(den, dtype=float), "
       'where=(den > 0))\n'
       "        matches['overlap_frac_conf'] = np.divide(matches['overlap_voxels'], matches['conf_vol'], "
       "out=np.zeros_like(matches['conf_vol'], dtype=float), where=(matches['conf_vol'] > 0))\n"
       "        matches['overlap_frac_twoP'] = np.divide(matches['overlap_voxels'], matches['twoP_vol'], "
       "out=np.zeros_like(matches['twoP_vol'], dtype=float), where=(matches['twoP_vol'] > 0))\n"
       '\n'
       "        acc = matches.loc[matches['within_gate']].copy()\n"
       "        conf_counts = acc['conf_label'].value_counts()\n"
       "        twop_counts = acc['twoP_label'].value_counts()\n"
       '\n'
       '        def _pair_type(row):\n'
       "            if not row['within_gate']:\n"
       "                return 'rejected'\n"
       "            cm = int(conf_counts.get(row['conf_label'], 0))\n"
       "            tm = int(twop_counts.get(row['twoP_label'], 0))\n"
       "            if cm == 1 and tm == 1: return '1-1'\n"
       "            if cm > 1 and tm == 1:  return 'merge'\n"
       "            if cm == 1 and tm > 1:  return 'split'\n"
       "            return 'complex'\n"
       '\n'
       "        matches['pair_type'] = matches.apply(_pair_type, axis=1)\n"
       '\n'
       '        if USE_FRAC_FILTERS:\n'
       "            ok_frac = (matches['overlap_frac_conf'] >= float(MIN_OVERLAP_FRAC_CONF)) & "
       "(matches['overlap_frac_twoP'] >= float(MIN_OVERLAP_FRAC_TWOP))\n"
       '        else:\n'
       '            ok_frac = True\n'
       '\n'
       "        matches['quality'] = np.where(\n"
       "            matches['within_gate'] & (matches['iou'] >= float(IOU_MIN)) & ok_frac, 'good',\n"
       "            np.where(matches['within_gate'], 'iffy', 'rejected')\n"
       '        )\n'
       '\n'
       "        final_pairs = matches[(matches['pair_type'] == '1-1') & (matches['quality'] == "
       "'good')].copy().sort_values(['distance_um', 'twoP_label', 'conf_label'])\n"
       "        review = matches[((matches['pair_type'].isin(['split','merge','complex'])) & matches['within_gate']) | "
       "((matches['pair_type'] == '1-1') & (matches['quality'] != "
       "'good'))].sort_values(['pair_type','iou','distance_um'], ascending=[True, False, True]).copy()\n"
       '\n'
       "        gate_mask = (matches['distance_um'] <= float(MAX_DISTANCE_UM))\n"
       '        if bool(REQUIRE_OVERLAP):\n'
       "            gate_mask = gate_mask & (matches['overlap_voxels'] >= int(MIN_OVERLAP_VOXELS))\n"
       '\n'
       '        n_pairs_after_gate = int(gate_mask.sum())\n'
       "        n_pairs_after_dedup_conf = int(matches['within_gate'].sum())\n"
       "        n_pairs_after_filter_pair_type_1to1 = int((matches['within_gate'] & (matches['pair_type'] == "
       "'1-1')).sum())\n"
       "        n_pairs_after_filter_iou_quality_good = int((matches['within_gate'] & (matches['pair_type'] == '1-1') "
       "& (matches['quality'] == 'good')).sum())\n"
       '        n_pairs_final = int(final_pairs.shape[0])\n'
       '\n'
       '        qc = pd.Series({\n'
       "            'pairs_all_overlap_candidates': int(n_pairs_overlap_candidates),\n"
       "            'pairs_after_overlap_pair_selection': int(after_overlap),\n"
       "            'pairs_after_filter_distance_overlap_gate': int(n_pairs_after_gate),\n"
       "            'pairs_after_filter_dedup_twoP': int(after_twop),\n"
       "            'pairs_after_filter_dedup_conf': int(n_pairs_after_dedup_conf),\n"
       "            'pairs_after_filter_pair_type_1to1': int(n_pairs_after_filter_pair_type_1to1),\n"
       "            'pairs_after_filter_iou_quality_good': int(n_pairs_after_filter_iou_quality_good),\n"
       "            'pairs_final': int(n_pairs_final),\n"
       '            # Legacy aliases kept for compatibility with older downstream cells.\n'
       "            'gate_only_pairs': int(n_pairs_after_gate),\n"
       "            'accepted_pairs_after_dedup': int(n_pairs_after_dedup_conf),\n"
       "            'final_1to1_good': int(n_pairs_final),\n"
       "            'splits_among_accepted': int((matches['pair_type'] == 'split').sum()),\n"
       "            'merges_among_accepted': int((matches['pair_type'] == 'merge').sum()),\n"
       "            'complex_among_accepted': int((matches['pair_type'] == 'complex').sum()),\n"
       "        }, name='QC Summary')\n"
       '\n'
       "        save_base = res['save_base']\n"
       '        matches_csv = Path(f"{save_base}_matches.csv")\n'
       '        matches.to_csv(matches_csv, index=False)\n'
       '        final_pairs.to_csv(Path(f"{save_base}_final_pairs.csv"), index=False)\n'
       '        review.to_csv(Path(f"{save_base}_review.csv"), index=False)\n'
       '\n'
       '        if PLOT_USE_FINAL_1TO1 and not final_pairs.empty:\n'
       '            matches_for_plots = final_pairs.copy()\n'
       "            key_final = set(map(tuple, matches_for_plots[['conf_label','twoP_label']].to_numpy()))\n"
       "            accepted_mask = matches[['conf_label','twoP_label']].apply(tuple, "
       'axis=1).isin(key_final).to_numpy()\n'
       '        else:\n'
       "            matches_for_plots = matches.loc[matches['within_gate']].copy()\n"
       "            accepted_mask = matches['within_gate'].to_numpy()\n"
       '\n'
       "        dz_um = float(vox_anat_um['dz'])\n"
       '        scope_df = matches_for_plots\n'
       "        conf_ids = np.unique(scope_df['conf_label'].to_numpy(dtype=int)); conf_ids = conf_ids[conf_ids != 0]\n"
       "        twoP_ids = np.unique(scope_df['twoP_label'].to_numpy(dtype=int)); twoP_ids = twoP_ids[twoP_ids != 0]\n"
       '\n'
       '        conf_warped_labels = conf_labels_2p\n'
       '        twoP_labels = anat_labels\n'
       '\n'
       '        conf_within_mask = np.isin(conf_warped_labels, conf_ids)\n'
       '        twoP_within_mask = np.isin(twoP_labels, twoP_ids)\n'
       '\n'
       '        tiff.imwrite(Path(f"{save_base}_conf_within_mask.tif"), (conf_within_mask.astype(np.uint8) * 255), '
       "imagej=True, metadata={'axes': 'ZYX', 'spacing': dz_um, 'unit': 'um'}, compression='deflate')\n"
       '        tiff.imwrite(Path(f"{save_base}_twoP_within_mask.tif"), (twoP_within_mask.astype(np.uint8) * 255), '
       "imagej=True, metadata={'axes': 'ZYX', 'spacing': dz_um, 'unit': 'um'}, compression='deflate')\n"
       '\n'
       '        conf_within_labels = np.where(conf_within_mask, conf_warped_labels, 0)\n'
       '        twoP_within_labels = np.where(twoP_within_mask, twoP_labels, 0)\n'
       '\n'
       '        def _min_unsigned_dtype(max_val: int):\n'
       '            import numpy as _np\n'
       '            if max_val <= _np.iinfo(_np.uint16).max:\n'
       '                return _np.uint16\n'
       '            elif max_val <= _np.iinfo(_np.uint32).max:\n'
       '                return _np.uint32\n'
       '            return _np.uint64\n'
       '\n'
       '        conf_dtype = _min_unsigned_dtype(int(conf_within_labels.max()))\n'
       '        twoP_dtype = _min_unsigned_dtype(int(twoP_within_labels.max()))\n'
       '        conf_within_labels = conf_within_labels.astype(conf_dtype, copy=False)\n'
       '        twoP_within_labels = twoP_within_labels.astype(twoP_dtype, copy=False)\n'
       '\n'
       '        def _imwrite_with_meta(path, arr):\n'
       '            import numpy as _np\n'
       '            if arr.dtype in (_np.uint8, _np.uint16):\n'
       "                tiff.imwrite(path, arr, imagej=True, metadata={'axes': 'ZYX', 'spacing': dz_um, 'unit': 'um'}, "
       "compression='deflate')\n"
       '            else:\n'
       "                tiff.imwrite(path, arr, compression='deflate')\n"
       '\n'
       '        _imwrite_with_meta(Path(f"{save_base}_conf_within_labels.tif"), conf_within_labels)\n'
       '        _imwrite_with_meta(Path(f"{save_base}_twoP_within_labels.tif"), twoP_within_labels)\n'
       '\n'
       '        rgb = np.zeros(conf_within_mask.shape + (3,), dtype=np.uint8)\n'
       '        rgb[..., 0] = np.where(conf_within_mask, 242, 0)\n'
       '        rgb[..., 1] = np.where(conf_within_mask,  84, 0)\n'
       '        rgb[..., 2] = np.where(conf_within_mask, 166, 0)\n'
       '        rgb[..., 0] = np.clip(rgb[..., 0] + np.where(twoP_within_mask,  51, 0), 0, 255)\n'
       '        rgb[..., 1] = np.clip(rgb[..., 1] + np.where(twoP_within_mask, 166, 0), 0, 255)\n'
       '        rgb[..., 2] = np.clip(rgb[..., 2] + np.where(twoP_within_mask, 255, 0), 0, 255)\n'
       '        tiff.imwrite(Path(f"{save_base}_within_gate_overlay_rgb.tif"), rgb, photometric=\'rgb\', '
       "compression='deflate')\n"
       '\n'
       "        summary = summarize_distances(matches['distance_um'].to_numpy(), matches['within_gate'].to_numpy())\n"
       '        _hlog(\n'
       '            f"Counts — overlap candidates: {n_pairs_overlap_candidates} | after overlap pairing: '
       '{after_overlap} | "\n'
       '            f"after distance/overlap gate: {n_pairs_after_gate} | after dedup twoP: {after_twop} | "\n'
       '            f"after dedup conf: {n_pairs_after_dedup_conf} | after 1-to-1 filter: '
       '{n_pairs_after_filter_pair_type_1to1} | "\n'
       '            f"after IoU quality filter: {n_pairs_after_filter_iou_quality_good} | final pairs: '
       '{n_pairs_final}"\n'
       '        )\n'
       "        _hlog('Summary:', json.dumps(summary, indent=2))\n"
       '\n'
       "        filter_stats = res.get('filter_stats') if isinstance(res, dict) else {}\n"
       '        if not isinstance(filter_stats, dict):\n'
       '            filter_stats = {}\n'
       '        hcr_match_table_rows.append({\n'
       "            'round': int(res['round']),\n"
       "            'mask': Path(res['mask_path']).name,\n"
       "            'gene': gene_from_mask(res['mask_path']) if callable(gene_from_mask) else "
       "Path(res['mask_path']).name,\n"
       "            'conf_labels_after_prewarp_filter': int(total_masks),\n"
       "            'pairs_all_overlap_candidates': int(qc.get('pairs_all_overlap_candidates', 0)),\n"
       "            'pairs_after_overlap_pair_selection': int(qc.get('pairs_after_overlap_pair_selection', 0)),\n"
       "            'pairs_after_filter_distance_overlap_gate': int(qc.get('pairs_after_filter_distance_overlap_gate', "
       '0)),\n'
       "            'pairs_after_filter_dedup_twoP': int(qc.get('pairs_after_filter_dedup_twoP', 0)),\n"
       "            'pairs_after_filter_dedup_conf': int(qc.get('pairs_after_filter_dedup_conf', 0)),\n"
       "            'pairs_after_filter_pair_type_1to1': int(qc.get('pairs_after_filter_pair_type_1to1', 0)),\n"
       "            'pairs_after_filter_iou_quality_good': int(qc.get('pairs_after_filter_iou_quality_good', 0)),\n"
       "            'pairs_final': int(qc.get('pairs_final', 0)),\n"
       "            'dist_median_um': float(summary.get('median', np.nan)),\n"
       "            'dist_p90_um': float(summary.get('p90', np.nan)),\n"
       "            'dist_max_um': float(summary.get('max', np.nan)),\n"
       "            'n_dropped_q05_prewarp': int(filter_stats.get('n_dropped_low_q', 0)),\n"
       "            'n_low_conf_q95_prewarp': int(filter_stats.get('n_low_confidence_high_q', 0)),\n"
       "            'matches_csv': str(matches_csv),\n"
       '        })\n'
       '\n'
       '        total_conf_masks += total_masks\n'
       '        total_after_overlap += after_overlap\n'
       '        total_after_twop += after_twop\n'
       '        total_after_conf += after_conf\n'
       '        masks_processed += 1\n'
       '\n'
       '        hcr_match_results.append({\n'
       "            'round': res['round'],\n"
       "            'mask_path': res['mask_path'],\n"
       "            'matches': matches,\n"
       "            'final_pairs': final_pairs,\n"
       "            'review': review,\n"
       "            'summary': summary,\n"
       "            'qc': qc,\n"
       "            'matches_csv': matches_csv,\n"
       "            'df_conf': df_conf,\n"
       "            'df_2p': df_2p,\n"
       "            'P_conf_um': P_conf_um,\n"
       "            'P_2p_um': P_2p_um,\n"
       "            'warped': conf_labels_2p,\n"
       '        })\n'
       '\n'
       '    if masks_processed > 0:\n'
       '        _hlog(f"[HCR] Global counts — conf masks: {total_conf_masks} | after overlap: {total_after_overlap} | '
       'after dedup twoP: {total_after_twop} | after dedup conf: {total_after_conf}")\n'
       '    else:\n'
       '        _hlog("[HCR] No warped masks processed.")\n'
       '\n'
       '    hcr_match_summary_table = pd.DataFrame(hcr_match_table_rows)\n'
       '    if not hcr_match_summary_table.empty:\n'
       "        hcr_match_summary_table = hcr_match_summary_table.sort_values(['round', 'gene', "
       "'mask']).reset_index(drop=True)\n"
       '        _flow_cols = [\n'
       "            'round',\n"
       "            'mask',\n"
       "            'gene',\n"
       "            'conf_labels_after_prewarp_filter',\n"
       "            'pairs_all_overlap_candidates',\n"
       "            'pairs_after_overlap_pair_selection',\n"
       "            'pairs_after_filter_distance_overlap_gate',\n"
       "            'pairs_after_filter_dedup_twoP',\n"
       "            'pairs_after_filter_dedup_conf',\n"
       "            'pairs_after_filter_pair_type_1to1',\n"
       "            'pairs_after_filter_iou_quality_good',\n"
       "            'pairs_final',\n"
       "            'dist_median_um',\n"
       "            'dist_p90_um',\n"
       "            'dist_max_um',\n"
       "            'n_dropped_q05_prewarp',\n"
       "            'n_low_conf_q95_prewarp',\n"
       "            'matches_csv',\n"
       '        ]\n'
       '        _tail_cols = [c for c in hcr_match_summary_table.columns if c not in _flow_cols]\n'
       '        hcr_match_summary_table = hcr_match_summary_table[_flow_cols + _tail_cols]\n'
       '    hcr_match_summary_table = hcr_match_summary_table\n'
       '    if not hcr_match_summary_table.empty:\n'
       "        _hlog('[HCR] Per-mask matching summary table:')\n"
       '        try:\n'
       '            display(hcr_match_summary_table)\n'
       '        except Exception:\n'
       '            print(hcr_match_summary_table.to_string(index=False))\n'
       '\n'
       "    best_round_matches = [r for r in hcr_match_results if r['round'] == best_round_idx]\n"
       '    if best_round_matches:\n'
       "        hcr_primary_matches = best_round_matches[0]['matches']\n"
       "        hcr_primary_final_pairs = best_round_matches[0]['final_pairs']\n"
       '        _hlog(f"[HCR] Primary results from round r{best_round_idx}: {len(hcr_primary_final_pairs)} final '
       'pairs")\n'
       '    else:\n'
       '        hcr_primary_matches = None\n'
       '        hcr_primary_final_pairs = None\n'
       '        _hlog("[HCR] No primary results (best round missing)")',
 '46': '# [46] Functional↔Anatomy per-plane matching summary (viewer prep/debug)\n'
       'try:\n'
       '    REQUIRE_FISH_STATE = require_fish_state\n'
       'except NameError:\n'
       '    REQUIRE_FISH_STATE = None\n'
       'if callable(REQUIRE_FISH_STATE) and not REQUIRE_FISH_STATE():\n'
       '    raise SystemExit\n'
       'import numpy as np\n'
       'import pandas as pd\n'
       'from skimage.measure import regionprops_table\n'
       'from scipy.optimize import linear_sum_assignment as _lsa\n'
       '\n'
       'try:\n'
       '    PLANE_REFS_LOCAL = plane_refs\n'
       'except NameError:\n'
       '    PLANE_REFS_LOCAL = []\n'
       'try:\n'
       '    VOX_ANAT_LOCAL = VOX_ANAT\n'
       'except NameError:\n'
       '    VOX_ANAT_LOCAL = None\n'
       'try:\n'
       '    GET_LABELS_FOR_PLANE = _get_labels_for_plane\n'
       'except NameError:\n'
       '    GET_LABELS_FOR_PLANE = None\n'
       'try:\n'
       '    LOAD_FUNC_LABELS_FOR_PLANE = _load_func_labels_for_plane\n'
       'except NameError:\n'
       '    LOAD_FUNC_LABELS_FOR_PLANE = None\n'
       '\n'
       'MIN_OVERLAP_FUNC_ANAT = 1\n'
       'REQUIRE_OVERLAP_FUNC_ANAT = True\n'
       'MAX_DIST_FUNC_ANAT = np.inf\n'
       "PLANE_MATCH_METHOD = 'nn'\n"
       '\n'
       'if not PLANE_REFS_LOCAL:\n'
       "    print('No plane_refs available; skipping per-plane match summary.')\n"
       'else:\n'
       "    DZ = float(VOX_ANAT_LOCAL.get('Z', 1.0)) if VOX_ANAT_LOCAL else 1.0\n"
       "    DY = float(VOX_ANAT_LOCAL.get('Y', 1.0)) if VOX_ANAT_LOCAL else 1.0\n"
       "    DX = float(VOX_ANAT_LOCAL.get('X', 1.0)) if VOX_ANAT_LOCAL else 1.0\n"
       '    twoP_labels_all = _ensure_uint_labels(imread_any(ANAT_LABELS_PATH))\n'
       '    z_size = twoP_labels_all.shape[0]\n'
       '\n'
       '    def load_func_for_plane(pr, p_idx):\n'
       '        if callable(GET_LABELS_FOR_PLANE):\n'
       '            try:\n'
       '                arr, desc = GET_LABELS_FOR_PLANE(pr, p_idx)\n'
       '                return arr, desc\n'
       '            except Exception:\n'
       '                pass\n'
       '        if callable(LOAD_FUNC_LABELS_FOR_PLANE):\n'
       '            try:\n'
       '                arr, _, desc = LOAD_FUNC_LABELS_FOR_PLANE(p_idx)\n'
       '                return arr, desc\n'
       '            except Exception:\n'
       '                pass\n'
       '        return None, None\n'
       '\n'
       '    def norm_func_labels(arr):\n'
       '        arr = _ensure_uint_labels(arr)\n'
       '        if arr.ndim == 3 and arr.shape[-1] in (3, 4):\n'
       '            arr = arr[..., 0]\n'
       '        if arr.ndim == 3 and arr.shape[0] == 1:\n'
       '            arr = arr[0]\n'
       '        return arr\n'
       '\n'
       '    plane_anat_ids = {}\n'
       '    plane_match_rows = []\n'
       '\n'
       '    for p_idx, pr in enumerate(PLANE_REFS_LOCAL):\n'
       "        lbl = pr.get('label', f'plane{p_idx}')\n"
       "        bz = int(pr.get('best_z', 0))\n"
       '        if bz < 0 or bz >= z_size:\n'
       '            continue\n'
       '\n'
       '        anat_slice = _ensure_uint_labels(twoP_labels_all[bz])\n'
       '        func_raw, desc = load_func_for_plane(pr, p_idx)\n'
       '        if func_raw is None:\n'
       '            continue\n'
       '        func_raw = norm_func_labels(func_raw)\n'
       '        if func_raw.ndim != 2:\n'
       '            continue\n'
       '\n'
       '        tform_use = _tform_for_plane(pr)\n'
       '        func_warped = func_raw.copy() if tform_use is None else resample_labels_nn(func_raw, tform_use, '
       'output_shape=anat_slice.shape)\n'
       '        func_warped = _ensure_uint_labels(func_warped)\n'
       '\n'
       "        f_props = regionprops_table(func_warped, properties=('label', 'centroid'))\n"
       "        a_props = regionprops_table(anat_slice, properties=('label', 'centroid'))\n"
       "        fdf = pd.DataFrame(f_props).rename(columns={'centroid-0': 'cy', 'centroid-1': 'cx'})\n"
       "        adf2d = pd.DataFrame(a_props).rename(columns={'centroid-0': 'cy', 'centroid-1': 'cx'})\n"
       "        fdf = fdf[fdf['label'] != 0].reset_index(drop=True)\n"
       "        adf2d = adf2d[adf2d['label'] != 0].reset_index(drop=True)\n"
       '        if fdf.empty or adf2d.empty:\n'
       '            continue\n'
       '\n'
       '        overlap_df = compute_label_overlap(func_warped, anat_slice, min_overlap_voxels=1)\n'
       '        if overlap_df.empty:\n'
       '            continue\n'
       "        overlap_df = overlap_df.rename(columns={'conf_label': 'func_label', 'twoP_label': 'anat_label', "
       "'overlap_voxels': 'overlap_px'})\n"
       '\n'
       "        F = fdf[['cx', 'cy']].to_numpy()\n"
       "        A = adf2d[['cx', 'cy']].to_numpy()\n"
       '        D = np.sqrt(((F[:, None, :] - A[None, :, :]) ** 2).sum(axis=2))\n'
       "        if PLANE_MATCH_METHOD == 'hungarian':\n"
       '            row_ind, col_ind = _lsa(D)\n'
       '        else:\n'
       '            row_ind = np.arange(F.shape[0], dtype=int)\n'
       '            col_ind = np.argmin(D, axis=1).astype(int)\n'
       '\n'
       '        links = []\n'
       '        for r, c in zip(row_ind, col_ind):\n'
       '            fxp, fyp = float(F[r, 0]), float(F[r, 1])\n'
       '            axp, ayp = float(A[c, 0]), float(A[c, 1])\n'
       '            dx_um = (fxp - axp) * DX\n'
       '            dy_um = (fyp - ayp) * DY\n'
       '            dist_um = float(np.sqrt(dx_um * dx_um + dy_um * dy_um))\n'
       "            links.append({'func_label': int(fdf.iloc[r]['label']), 'anat_label': int(adf2d.iloc[c]['label']), "
       "'dist_um': dist_um})\n"
       '        links_df = pd.DataFrame(links)\n'
       '\n'
       '        n_func_labels = int(len(fdf))\n'
       '        n_anat_labels = int(len(adf2d))\n'
       '        n_pairs_all_initial = int(len(links_df))\n'
       "        links_df = links_df.merge(overlap_df, on=['func_label', 'anat_label'], how='left')\n"
       "        links_df['overlap_px'] = links_df['overlap_px'].fillna(0).astype(int)\n"
       "        n_pairs_with_any_overlap = int((links_df['overlap_px'] >= 1).sum())\n"
       "        links_after_overlap = links_df[links_df['overlap_px'] >= MIN_OVERLAP_FUNC_ANAT].copy() if "
       'REQUIRE_OVERLAP_FUNC_ANAT else links_df.copy()\n'
       '        n_pairs_after_filter_overlap = int(len(links_after_overlap))\n'
       "        links_after_dist = links_after_overlap[links_after_overlap['dist_um'] <= MAX_DIST_FUNC_ANAT].copy() if "
       'np.isfinite(MAX_DIST_FUNC_ANAT) and MAX_DIST_FUNC_ANAT > 0 else links_after_overlap.copy()\n'
       '        n_pairs_after_filter_distance = int(len(links_after_dist))\n'
       '        n_pairs_final = int(n_pairs_after_filter_distance)\n'
       '\n'
       '        if n_pairs_final > 0:\n'
       "            plane_anat_ids[lbl] = set(links_after_dist['anat_label'].astype(int).tolist())\n"
       '\n'
       '        plane_match_rows.append({\n'
       "            'plane': lbl,\n"
       "            'best_z': int(bz),\n"
       "            'assignment_method': PLANE_MATCH_METHOD,\n"
       "            'n_func_labels': int(n_func_labels),\n"
       "            'n_anat_labels': int(n_anat_labels),\n"
       "            'pairs_all_initial': int(n_pairs_all_initial),\n"
       "            'pairs_with_any_overlap': int(n_pairs_with_any_overlap),\n"
       "            'pairs_after_filter_overlap_min_px': int(n_pairs_after_filter_overlap),\n"
       "            'pairs_after_filter_distance_um': int(n_pairs_after_filter_distance),\n"
       "            'pairs_final': int(n_pairs_final),\n"
       '        })\n'
       '\n'
       '    plane_match_summary_df = pd.DataFrame(plane_match_rows)\n'
       '    if not plane_match_summary_df.empty:\n'
       '        _flow_cols = '
       "['plane','best_z','assignment_method','n_func_labels','n_anat_labels','pairs_all_initial','pairs_with_any_overlap','pairs_after_filter_overlap_min_px','pairs_after_filter_distance_um','pairs_final']\n"
       '        _tail_cols = [c for c in plane_match_summary_df.columns if c not in _flow_cols]\n'
       '        plane_match_summary_df = plane_match_summary_df[_flow_cols + _tail_cols]\n'
       '\n'
       "    plane_match_summary = plane_match_summary_df.to_dict('records')\n"
       '\n'
       '    if not plane_match_summary_df.empty:\n'
       '        try:\n'
       '            display(plane_match_summary_df)\n'
       '        except Exception:\n'
       '            print(plane_match_summary_df.to_string(index=False))\n',
 '47': '# [47]\n'
       'try:\n'
       '    _require_fish_state_47 = require_fish_state\n'
       'except NameError:\n'
       '    _require_fish_state_47 = None\n'
       'if _require_fish_state_47 is not None and (not _require_fish_state_47()):\n'
       '    raise SystemExit\n'
       '# 3D viewer: per-plane functional/anatomy with confocal overlays (simple opacity scheme, plane highlights)\n'
       'SKIP_47 = True\n'
       'if SKIP_47:\n'
       "    print('[47] SKIP_47=True; skipping per-plane 3D viewer generation.')\n"
       'else:\n'
       '    import plotly.graph_objects as go\n'
       '\n'
       '    try:\n'
       '        _vox_anat_47 = VOX_ANAT\n'
       '    except NameError:\n'
       '        _vox_anat_47 = {}\n'
       '    try:\n'
       '        _get_labels_for_plane_47 = _get_labels_for_plane\n'
       '    except NameError:\n'
       '        _get_labels_for_plane_47 = None\n'
       '    try:\n'
       '        _load_func_labels_for_plane_47 = _load_func_labels_for_plane\n'
       '    except NameError:\n'
       '        _load_func_labels_for_plane_47 = None\n'
       '    try:\n'
       '        _hcr_match_results_47 = hcr_match_results\n'
       '    except NameError:\n'
       '        _hcr_match_results_47 = []\n'
       '    import numpy as np\n'
       '    import pandas as pd\n'
       '    from skimage.measure import marching_cubes, regionprops_table\n'
       '    from scipy.optimize import linear_sum_assignment as _lsa\n'
       '\n'
       '\n'
       '    # Anatomy centroids\n'
       "    anat_props3d = regionprops_table(twoP_labels_all, properties=('label', 'centroid', 'bbox'))\n"
       "    adf3d = pd.DataFrame(anat_props3d).rename(columns={'centroid-0': 'cz', 'centroid-1': 'cy', 'centroid-2': "
       "'cx'})\n"
       "    adf3d = adf3d[adf3d['label'] != 0].reset_index(drop=True)\n"
       "    anat_centroid_map = {int(r['label']): {'x': float(r['cx']), 'y': float(r['cy']), 'z': float(r['cz'])} for "
       '_, r in adf3d.iterrows()}\n'
       '\n'
       '    # Median anatomy diameter and distance gate (defaults to 0.5× median diameter unless overridden)\n'
       '    try:\n'
       '        adf3d = '
       "adf3d.rename(columns={'bbox-0':'zmin','bbox-1':'ymin','bbox-2':'xmin','bbox-3':'zmax','bbox-4':'ymax','bbox-5':'xmax'})\n"
       "        adf3d['diam_um'] = ((adf3d['xmax'] - adf3d['xmin'])*DX + (adf3d['ymax'] - adf3d['ymin'])*DY + "
       "(adf3d['zmax'] - adf3d['zmin'])*DZ)/3.0\n"
       "        median_diam_um = float(adf3d['diam_um'].median()) if adf3d['diam_um'].size else 0.0\n"
       '    except Exception:\n'
       '        median_diam_um = 0.0\n'
       '\n'
       '    MAX_DIST_FUNC_ANAT = 0.5 * median_diam_um if median_diam_um > 0 else np.inf\n'
       '\n'
       '    # Tunables\n'
       '    MIN_OVERLAP_FUNC_ANAT = 1\n'
       '    REQUIRE_OVERLAP_FUNC_ANAT = True\n'
       '    STEP_PL = 2\n'
       '    STEP_CONF = STEP_PL\n'
       '    PAIR_LINE_WIDTH_FUNC = 2\n'
       "    PAIR_LINE_COLOR_FUNC = 'gray'\n"
       '    PAIR_LINE_WIDTH_CONF = 3\n'
       "    PAIR_LINE_COLOR_CONF = 'magenta'\n"
       '    OPACITY_FUNC_ALL = 0.05\n'
       '    OPACITY_FUNC_PAIRED = 0.05\n'
       '    OPACITY_ANAT_PAIRED = 0.10\n'
       '    OPACITY_CONF_ALL = 0.05\n'
       '    OPACITY_CONF_PLANE = 0.20\n'
       '\n'
       '    if not plane_refs:\n'
       "        print('No plane_refs available for per-plane viewer.')\n"
       '    else:\n'
       "        DZ = float(_vox_anat_47.get('Z', 1.0)) if _vox_anat_47 else 1.0\n"
       "        DY = float(_vox_anat_47.get('Y', 1.0)) if _vox_anat_47 else 1.0\n"
       "        DX = float(_vox_anat_47.get('X', 1.0)) if _vox_anat_47 else 1.0\n"
       '\n'
       '        twoP_labels_all = _ensure_uint_labels(imread_any(ANAT_LABELS_PATH))\n'
       '        z_size = twoP_labels_all.shape[0]\n'
       '        y_size = twoP_labels_all.shape[1]\n'
       '        x_size = twoP_labels_all.shape[2]\n'
       '        z_max_um = (z_size - 1) * DZ\n'
       '        y_max_um = (y_size - 1) * DY\n'
       '        x_max_um = (x_size - 1) * DX\n'
       '\n'
       '        def load_func_for_plane(pr, p_idx):\n'
       '            if _get_labels_for_plane_47 is not None:\n'
       '                try:\n'
       '                    arr, desc = _get_labels_for_plane_47(pr, p_idx)\n'
       '                    return arr, desc\n'
       '                except Exception:\n'
       '                    pass\n'
       '            if _load_func_labels_for_plane_47 is not None:\n'
       '                try:\n'
       '                    arr, _, desc = _load_func_labels_for_plane_47(p_idx)\n'
       '                    return arr, desc\n'
       '                except Exception:\n'
       '                    pass\n'
       '            return None, None\n'
       '\n'
       '        def norm_func_labels(arr):\n'
       '            arr = _ensure_uint_labels(arr)\n'
       '            if arr.ndim == 3 and arr.shape[-1] in (3, 4):\n'
       '                arr = arr[..., 0]\n'
       '            if arr.ndim == 3 and arr.shape[0] == 1:\n'
       '                arr = arr[0]\n'
       '            return arr\n'
       '\n'
       '        def build_surface(mask_bool, step):\n'
       '            if mask_bool is None or not np.any(mask_bool):\n'
       '                return (np.array([]),) * 6\n'
       '            try:\n'
       '                verts, faces, _, _ = marching_cubes(mask_bool.astype(np.uint8), level=0.5, spacing=(DZ, DY, '
       'DX), step_size=step)\n'
       '                i, j, k = faces.T.astype(np.int32, copy=False)\n'
       '                zc, yc, xc = verts[:, 0], verts[:, 1], verts[:, 2]\n'
       '                return (xc, yc, zc, i, j, k)\n'
       '            except RuntimeError:\n'
       '                return (np.array([]),) * 6\n'
       '\n'
       '        # Confocal overlays: renumber per mask to avoid label collisions\n'
       '        conf_union = None\n'
       '        conf_centroid_map = {}  # new_label -> dict\n'
       '        conf_pairs = []         # dict with new conf_label, anat_label\n'
       '        anat_to_conf = {}       # anat_label -> set(new conf labels)\n'
       '        next_conf = 1\n'
       '        if _hcr_match_results_47:\n'
       '            # build union with remapped labels\n'
       '            conf_union = np.zeros_like(twoP_labels_all, dtype=np.int32)\n'
       '            for mask_idx, res in enumerate(_hcr_match_results_47):\n'
       "                warped = res.get('warped')\n"
       "                df_conf = res.get('df_conf'); df_2p = res.get('df_2p')\n"
       "                fp = res.get('final_pairs')\n"
       '                if warped is None:\n'
       '                    continue\n'
       '                label_map = {}\n'
       '                if df_conf is not None:\n'
       "                    for lbl, row in df_conf.set_index('label').iterrows():\n"
       '                        label_map[int(lbl)] = next_conf\n'
       "                        conf_centroid_map[next_conf] = {'x': float(row['x']), 'y': float(row['y']), 'z': "
       "float(row['z'])}\n"
       '                        next_conf += 1\n'
       '                # remap warped labels\n'
       '                remapped = np.zeros_like(warped, dtype=np.int32)\n'
       '                for orig, newlbl in label_map.items():\n'
       '                    remapped = np.where(warped == orig, newlbl, remapped)\n'
       '                conf_union = np.where(remapped > 0, remapped, conf_union)\n'
       '                if fp is None or fp.empty or df_conf is None or df_2p is None:\n'
       '                    continue\n'
       "                amap = df_2p.set_index('label')[['x','y','z']].to_dict('index')\n"
       '                for _, r in fp.iterrows():\n'
       "                    al = int(r['twoP_label']); cl_orig = int(r['conf_label'])\n"
       '                    new_cl = label_map.get(cl_orig)\n'
       '                    c = conf_centroid_map.get(new_cl); a = amap.get(al)\n'
       '                    if new_cl is None or c is None or a is None:\n'
       '                        continue\n'
       "                    dx_um = (c['x'] - float(a['x'])) * DX\n"
       "                    dy_um = (c['y'] - float(a['y'])) * DY\n"
       "                    dz_um = (c['z'] - float(a['z'])) * DZ\n"
       "                    conf_pairs.append({'conf_label': new_cl, 'anat_label': al, 'dist_um': "
       'float(np.sqrt(dx_um*dx_um + dy_um*dy_um + dz_um*dz_um))})\n'
       '                    anat_to_conf.setdefault(al, set()).add(new_cl)\n'
       '\n'
       '        traces = []\n'
       '        plane_spans = []\n'
       '\n'
       '        # Global confocal mesh (always visible)\n'
       '        conf_trace_idx = None\n'
       '        if conf_union is not None and np.any(conf_union):\n'
       '            xC, yC, zC, iC, jC, kC = build_surface(conf_union > 0, STEP_CONF)\n'
       "            traces.append(go.Mesh3d(x=xC, y=yC, z=zC, i=iC, j=jC, k=kC, name='Confocal masks (all)', "
       "color='magenta', opacity=OPACITY_CONF_ALL, visible=True))\n"
       '            conf_trace_idx = 0\n'
       '\n'
       '        for p_idx, pr in enumerate(plane_refs):\n'
       "            lbl = pr.get('label', f'plane{p_idx}')\n"
       '            def _skip(reason):\n'
       '                print(f"[viewer] skip {lbl}: {reason}")\n'
       "            bz = int(pr.get('best_z', 0))\n"
       '            if bz < 0 or bz >= z_size:\n'
       '                _skip(f"best_z {bz} out of bounds (0..{z_size-1})")\n'
       '                continue\n'
       '\n'
       '            anat_slice = _ensure_uint_labels(twoP_labels_all[bz])\n'
       '\n'
       '            func_raw, desc = load_func_for_plane(pr, p_idx)\n'
       '            if func_raw is None:\n'
       "                _skip('no functional labels for plane')\n"
       '                continue\n'
       '\n'
       '            func_raw = norm_func_labels(func_raw)\n'
       '            if func_raw.ndim != 2:\n'
       '\n'
       "                _skip(f'functional labels shape {func_raw.shape} not 2D')\n"
       '                continue\n'
       '\n'
       '            tform_use = _tform_for_plane(pr)\n'
       '            if tform_use is None:\n'
       '                func_warped = func_raw.copy()\n'
       '            else:\n'
       '                func_warped = resample_labels_nn(func_raw, tform_use, output_shape=anat_slice.shape)\n'
       '            func_warped = _ensure_uint_labels(func_warped)\n'
       '\n'
       "            f_props = regionprops_table(func_warped, properties=('label', 'centroid'))\n"
       "            a_props = regionprops_table(anat_slice, properties=('label', 'centroid'))\n"
       "            fdf = pd.DataFrame(f_props).rename(columns={'centroid-0': 'cy', 'centroid-1': 'cx'})\n"
       "            adf2d = pd.DataFrame(a_props).rename(columns={'centroid-0': 'cy', 'centroid-1': 'cx'})\n"
       "            fdf = fdf[fdf['label'] != 0].reset_index(drop=True)\n"
       "            adf2d = adf2d[adf2d['label'] != 0].reset_index(drop=True)\n"
       '            if fdf.empty:\n'
       "                _skip('functional labels contain no nonzero regions')\n"
       '                continue\n'
       '            if adf2d.empty:\n'
       "                _skip('anatomy slice has no labels')\n"
       '                continue\n'
       '\n'
       '            overlap_df = compute_label_overlap(func_warped, anat_slice, min_overlap_voxels=1)\n'
       '            if overlap_df.empty:\n'
       "                _skip('no func↔anat overlap at this plane')\n"
       '                continue\n'
       "            overlap_df = overlap_df.rename(columns={'conf_label': 'func_label', 'twoP_label': 'anat_label', "
       "'overlap_voxels': 'overlap_px'})\n"
       '\n'
       "            F = fdf[['cx', 'cy']].to_numpy()\n"
       "            A = adf2d[['cx', 'cy']].to_numpy()\n"
       '            D = np.sqrt(((F[:, None, :] - A[None, :, :]) ** 2).sum(axis=2))\n'
       '            row_ind, col_ind = _lsa(D)\n'
       '            links = []\n'
       '            for r, c in zip(row_ind, col_ind):\n'
       '                fxp, fyp = float(F[r, 0]), float(F[r, 1]); axp, ayp = float(A[c, 0]), float(A[c, 1])\n'
       '                dist_px = float(D[r, c])\n'
       '                dx_um = (fxp - axp) * DX; dy_um = (fyp - ayp) * DY\n'
       '                dist_um = float(np.sqrt(dx_um * dx_um + dy_um * dy_um))\n'
       "                links.append({'func_label': int(fdf.iloc[r]['label']), 'anat_label': "
       "int(adf2d.iloc[c]['label']), 'fx_px': fxp, 'fy_px': fyp, 'ax_px': axp, 'ay_px': ayp, 'dist_px': dist_px, "
       "'dist_um': dist_um})\n"
       '            links_df = pd.DataFrame(links)\n'
       '            n_func_labels = len(fdf); n_anat_labels = len(adf2d); n_initial = len(links_df)\n'
       '\n'
       "            links_df = links_df.merge(overlap_df, on=['func_label', 'anat_label'], how='left')\n"
       "            links_df['overlap_px'] = links_df['overlap_px'].fillna(0).astype(int)\n"
       '            n_after_overlap = len(links_df)\n'
       '            if REQUIRE_OVERLAP_FUNC_ANAT:\n'
       "                links_df = links_df[links_df['overlap_px'] >= MIN_OVERLAP_FUNC_ANAT]\n"
       '            n_after_overlap_filter = len(links_df)\n'
       '\n'
       '            dist_gate = MAX_DIST_FUNC_ANAT if np.isfinite(MAX_DIST_FUNC_ANAT) and MAX_DIST_FUNC_ANAT > 0 else '
       'None\n'
       '            n_before_dist = len(links_df)\n'
       '            if dist_gate is not None:\n'
       "                links_df = links_df[links_df['dist_um'] <= dist_gate]\n"
       '            n_after_dist = len(links_df)\n'
       '\n'
       '            print(f"[viewer] {lbl}: func={n_func_labels}, anat={n_anat_labels}, matches={n_initial} -> overlap '
       '{n_after_overlap_filter}/{n_after_overlap} -> dist {n_after_dist}/{n_before_dist}")\n'
       '            if links_df.empty:\n'
       "                _skip('no matches after overlap/distance filters')\n"
       '                continue\n'
       '\n'
       '\n'
       "            func_ids = links_df['func_label'].unique()\n"
       "            anat_ids = links_df['anat_label'].unique()\n"
       '\n'
       '            func_mask = np.zeros_like(twoP_labels_all, dtype=bool); func_mask[bz] = func_warped > 0\n'
       '            func_mask_p = np.zeros_like(twoP_labels_all, dtype=bool); func_mask_p[bz] = np.isin(func_warped, '
       'func_ids)\n'
       '            anat_mask_p = np.isin(twoP_labels_all, anat_ids)\n'
       '\n'
       "            # Confocal masks paired with this plane's anatomy labels (3D volume) using remapped labels\n"
       '            conf_plane_mask = np.zeros_like(conf_union, dtype=bool)\n'
       '            conf_ids_plane = set()\n'
       '            if anat_to_conf and conf_union is not None:\n'
       '                for al in anat_ids.tolist():\n'
       '                    conf_ids_plane.update(anat_to_conf.get(int(al), set()))\n'
       '                if conf_ids_plane:\n'
       '                    conf_plane_mask = np.isin(conf_union, list(conf_ids_plane))\n'
       '            if not conf_ids_plane:\n'
       '                print(f"No conf labels for plane {lbl} (anat_ids {anat_ids.tolist()})")\n'
       '\n'
       '            xF, yF, zF, iF, jF, kF = build_surface(func_mask, STEP_PL)\n'
       '            xFP, yFP, zFP, iFP, jFP, kFP = build_surface(func_mask_p, STEP_PL)\n'
       '            xAP, yAP, zAP, iAP, jAP, kAP = build_surface(anat_mask_p, STEP_PL)\n'
       '            xCPm, yCPm, zCPm, iCPm, jCPm, kCPm = build_surface(conf_plane_mask, STEP_CONF)\n'
       '\n'
       "            fx_um = links_df['fx_px'].to_numpy() * DX\n"
       "            fy_um = links_df['fy_px'].to_numpy() * DY\n"
       '            fz_um = np.full_like(fx_um, bz * DZ)\n'
       '\n'
       '            ax_um = []\n'
       '            ay_um = []\n'
       '            az_um = []\n'
       "            for lbl in links_df['anat_label']:\n"
       '                c = anat_centroid_map.get(int(lbl))\n'
       '                if c is None:\n'
       '                    ax_um.append(np.nan); ay_um.append(np.nan); az_um.append(np.nan)\n'
       '                else:\n'
       "                    ax_um.append(c['x'] * DX); ay_um.append(c['y'] * DY); az_um.append(c['z'] * DZ)\n"
       '\n'
       '            fxp = fx_um.tolist(); fyp = fy_um.tolist(); fzp = fz_um.tolist()\n'
       '            axp = ax_um; ayp = ay_um; azp = az_um\n'
       '            lx = []; ly = []; lz = []; htext = []\n'
       '            for fxu, fyu, axu, ayu, azu, du, ov in zip(fx_um, fy_um, ax_um, ay_um, az_um, '
       "links_df['dist_um'].to_numpy(), links_df['overlap_px'].to_numpy()):\n"
       '                if np.isnan(axu) or np.isnan(ayu) or np.isnan(azu):\n'
       '                    continue\n'
       '                lx += [fxu, axu, None]; ly += [fyu, ayu, None]; lz += [bz * DZ, azu, None]\n'
       '                htext.append(f"d={du:.2f} µm | ov={int(ov)} px")\n'
       '\n'
       '            # Confocal centroids/links for anatomy labels in this plane (final_pairs only, remapped labels)\n'
       '            conf_x = []; conf_y = []; conf_z = []; conf_lx = []; conf_ly = []; conf_lz = []; conf_text = []\n'
       '            if conf_pairs and conf_centroid_map:\n'
       '                anat_id_set = set(anat_ids.tolist())\n'
       '                for pair in conf_pairs:\n'
       "                    if pair['anat_label'] not in anat_id_set:\n"
       '                        continue\n'
       "                    ccent = conf_centroid_map.get(pair['conf_label'])\n"
       "                    acent = anat_centroid_map.get(pair['anat_label'])\n"
       '                    if ccent is None or acent is None:\n'
       '                        continue\n'
       "                    cx_um = ccent['x'] * DX; cy_um = ccent['y'] * DY; cz_um = ccent['z'] * DZ\n"
       "                    ax_um_c = acent['x'] * DX; ay_um_c = acent['y'] * DY; az_um_c = acent['z'] * DZ\n"
       '                    conf_x.append(cx_um); conf_y.append(cy_um); conf_z.append(cz_um)\n'
       '                    conf_lx += [cx_um, ax_um_c, None]; conf_ly += [cy_um, ay_um_c, None]; conf_lz += [cz_um, '
       'az_um_c, None]\n'
       '                    conf_text.append(f"conf {pair[\'conf_label\']} ↔ anat {pair[\'anat_label\']}")\n'
       '\n'
       '            start = len(traces)\n'
       '            traces.extend([\n'
       '                go.Mesh3d(x=xF, y=yF, z=zF, i=iF, j=jF, k=kF, name=f"Func {lbl} (all)", color=\'#99ffcc\', '
       'opacity=OPACITY_FUNC_ALL, visible=False),\n'
       '                go.Mesh3d(x=xFP, y=yFP, z=zFP, i=iFP, j=jFP, k=kFP, name=f"Func {lbl} (paired)", '
       "color='#00cc88', opacity=OPACITY_FUNC_PAIRED, visible=False),\n"
       '                go.Mesh3d(x=xAP, y=yAP, z=zAP, i=iAP, j=jAP, k=kAP, name=f"Anat paired (3D)", '
       "color='#00b7ff', opacity=OPACITY_ANAT_PAIRED, visible=False),\n"
       "                go.Scatter3d(x=fxp, y=fyp, z=fzp, mode='markers', name='Func centroids', marker=dict(size=3, "
       "color='#00cc88'), visible=False),\n"
       "                go.Scatter3d(x=axp, y=ayp, z=azp, mode='markers', name='Anat centroids (3D)', "
       "marker=dict(size=3, color='#00b7ff'), visible=False),\n"
       "                go.Scatter3d(x=lx, y=ly, z=lz, mode='lines', name='Pairs', "
       "line=dict(color=PAIR_LINE_COLOR_FUNC, width=PAIR_LINE_WIDTH_FUNC), hoverinfo='text', text=htext, "
       'visible=False),\n'
       "                go.Scatter3d(x=conf_x, y=conf_y, z=conf_z, mode='markers', name='Conf centroids', "
       "marker=dict(size=3, color='magenta'), visible=False),\n"
       "                go.Scatter3d(x=conf_lx, y=conf_ly, z=conf_lz, mode='lines', name='Conf↔Anat links', "
       "line=dict(color=PAIR_LINE_COLOR_CONF, width=PAIR_LINE_WIDTH_CONF), hoverinfo='text', text=conf_text, "
       'visible=False),\n'
       "                go.Mesh3d(x=xCPm, y=yCPm, z=zCPm, i=iCPm, j=jCPm, k=kCPm, name='Conf paired (plane)', "
       "color='red', opacity=OPACITY_CONF_PLANE, visible=False),\n"
       '            ])\n'
       '            plane_spans.append((lbl, start, bz))\n'
       '\n'
       '        if plane_spans:\n'
       '            total = len(traces)\n'
       '            init_vis = [False] * total\n'
       '            if conf_trace_idx is not None:\n'
       '                init_vis[conf_trace_idx] = True\n'
       '            lbl0, s0, bz0 = plane_spans[0]\n'
       '            init_vis[s0:s0 + 9] = [True] * 9\n'
       '            for i, tr in enumerate(traces):\n'
       '                tr.visible = init_vis[i]\n'
       '\n'
       '            buttons = []\n'
       '            for lbl, start, bz in plane_spans:\n'
       '                vis = [False] * total\n'
       '                if conf_trace_idx is not None:\n'
       '                    vis[conf_trace_idx] = True\n'
       '                vis[start:start + 9] = [True] * 9\n'
       "                buttons.append(dict(label=lbl, method='update', args=[{'visible': vis}, {'title': f'{lbl} @ "
       "Z={bz}'}]))\n"
       '\n'
       '            fig = go.Figure(data=traces)\n'
       '            fig.update_layout(\n'
       '                width=1500, height=1250,\n'
       "                title=f'{lbl0} @ Z={bz0}',\n"
       "                scene=dict(xaxis=dict(title='X (µm)', range=[0, x_max_um]), yaxis=dict(title='Y (µm)', "
       "range=[0, y_max_um]), zaxis=dict(title='Z (µm)', range=[0, z_max_um]), aspectmode='data'),\n"
       "                updatemenus=[dict(buttons=buttons, direction='down')]\n"
       '            )\n'
       '            fig.show()\n'
       '        else:\n'
       "            print('No plane data to plot (missing masks or matches).')\n",
 '47b': '# [47b]\n'
        'try:\n'
        '    REQUIRE_FISH_STATE = require_fish_state\n'
        'except NameError:\n'
        '    REQUIRE_FISH_STATE = None\n'
        'if callable(REQUIRE_FISH_STATE) and not REQUIRE_FISH_STATE():\n'
        '    raise SystemExit\n'
        '\n'
        '# 3D viewer: warped HCR masks in 2P anatomy space with transparent functional-plane overlays.\n'
        'import numpy as np\n'
        'from pathlib import Path\n'
        '\n'
        'try:\n'
        '    PLANE_REFS_LOCAL = plane_refs\n'
        'except NameError:\n'
        '    PLANE_REFS_LOCAL = []\n'
        'try:\n'
        '    WARP_RESULTS_LOCAL = warp_results\n'
        'except NameError:\n'
        '    WARP_RESULTS_LOCAL = []\n'
        'try:\n'
        '    VOX_ANAT_LOCAL = VOX_ANAT\n'
        'except NameError:\n'
        '    VOX_ANAT_LOCAL = None\n'
        'try:\n'
        '    GENE_FROM_MASK = gene_from_mask\n'
        'except NameError:\n'
        '    GENE_FROM_MASK = None\n'
        '\n'
        'try:\n'
        '    import plotly.graph_objects as go\n'
        '    from skimage.measure import marching_cubes\n'
        'except Exception as _e:\n'
        "    print(f'[47b] Missing plotting dependency: {_e}')\n"
        'else:\n'
        '    if not PLANE_REFS_LOCAL:\n'
        "        print('[47b] Need plane_refs; run the functional-plane registration cells first.')\n"
        '    elif not WARP_RESULTS_LOCAL:\n'
        "        print('[47b] Need warp_results; run [43] first.')\n"
        '    elif ANAT_LABELS_PATH is None:\n'
        "        print('[47b] Need ANAT_LABELS_PATH; run the anatomy/HCR setup cells first.')\n"
        '    else:\n'
        '        RC = RUN_CONFIG if isinstance(RUN_CONFIG, dict) else {}\n'
        "        DEFAULT_GENE_ORDER = ['sst1.1', 'sst1.2', 'npy', 'tac3b', 'pth2', 'cfos', 'cort']\n"
        '        DEFAULT_GENE_COLORS = {\n'
        "            'sst1.1': '#d62728', 'sst1.2': '#d61ad2', 'npy': '#1f9d55', 'tac3b': '#ffd400',\n"
        "            'pth2': '#00bcd4', 'cfos': '#ff7f0e', 'cort': '#8c564b',\n"
        '        }\n'
        "        GENE_ORDER = list(RC.get('GENE_ORDER', DEFAULT_GENE_ORDER))\n"
        '        GENE_COLORS = dict(DEFAULT_GENE_COLORS)\n'
        "        _gc = RC.get('GENE_COLORS', {})\n"
        '        if isinstance(_gc, dict):\n'
        '            GENE_COLORS.update(_gc)\n'
        '\n'
        '        VIEWER_47B_STEP = 2\n'
        '        VIEWER_47B_HCR_OPACITY = 0.30\n'
        '        VIEWER_47B_ANAT_OPACITY = 0.05\n'
        '        VIEWER_47B_PLANE_OPACITY = 0.10\n'
        "        VIEWER_47B_PLANE_COLOR = '#7aa6c2'\n"
        '\n'
        '        twoP_labels_all = _ensure_uint_labels(imread_any(ANAT_LABELS_PATH))\n'
        "        DZ = float(VOX_ANAT_LOCAL.get('Z', 1.0)) if VOX_ANAT_LOCAL else 1.0\n"
        "        DY = float(VOX_ANAT_LOCAL.get('Y', 1.0)) if VOX_ANAT_LOCAL else 1.0\n"
        "        DX = float(VOX_ANAT_LOCAL.get('X', 1.0)) if VOX_ANAT_LOCAL else 1.0\n"
        '\n'
        '        z_size, y_size, x_size = twoP_labels_all.shape\n'
        '        z_max_um = (z_size - 1) * DZ\n'
        '        y_max_um = (y_size - 1) * DY\n'
        '        x_max_um = (x_size - 1) * DX\n'
        '\n'
        '        def _infer_gene(mask_path):\n'
        '            if callable(GENE_FROM_MASK):\n'
        '                try:\n'
        '                    return str(GENE_FROM_MASK(mask_path))\n'
        '                except Exception:\n'
        '                    pass\n'
        '            name = Path(str(mask_path)).name\n'
        "            m = __import__('re').search(r'channel\\d+_(.+?)_cp_masks', name)\n"
        '            gene = m.group(1) if m else name\n'
        "            return gene.replace('sst1_', 'sst1.')\n"
        '\n'
        '        def _build_surface(mask_bool, step_size):\n'
        '            if mask_bool is None or not np.any(mask_bool):\n'
        '                return (np.array([]),) * 6\n'
        '            try:\n'
        '                verts, faces, _, _ = marching_cubes(mask_bool.astype(np.uint8), level=0.5, spacing=(DZ, DY, '
        'DX), step_size=int(step_size))\n'
        '                i, j, k = faces.T.astype(np.int32, copy=False)\n'
        '                zc, yc, xc = verts[:, 0], verts[:, 1], verts[:, 2]\n'
        '                return xc, yc, zc, i, j, k\n'
        '            except Exception:\n'
        '                return (np.array([]),) * 6\n'
        '\n'
        '        gene_masks = {}\n'
        '        for res in WARP_RESULTS_LOCAL:\n'
        "            warped = np.asarray(res.get('warped')) if res.get('warped') is not None else None\n"
        "            if warped is None or getattr(warped, 'ndim', 0) != 3:\n"
        '                continue\n'
        "            gene = _infer_gene(res.get('mask_path', 'unknown'))\n"
        '            mask_bool = warped > 0\n'
        '            gene_masks[gene] = np.logical_or(gene_masks.get(gene, np.zeros_like(mask_bool, dtype=bool)), '
        'mask_bool)\n'
        '\n'
        '        genes_present = [g for g, m in gene_masks.items() if np.any(m)]\n'
        '        if not genes_present:\n'
        "            print('[47b] No non-empty warped HCR masks available to plot.')\n"
        '        else:\n'
        '            gene_order_plot = [g for g in GENE_ORDER if g in genes_present] + [g for g in '
        'sorted(genes_present) if g not in GENE_ORDER]\n'
        '            traces = []\n'
        '            always_visible = []\n'
        '            gene_trace_idx = {}\n'
        '            plane_trace_indices = []\n'
        '            for p_idx, pr in enumerate(PLANE_REFS_LOCAL):\n'
        "                bz = pr.get('best_z', np.nan)\n"
        '                if not np.isfinite(bz):\n'
        '                    continue\n'
        '                z_um = float(bz) * DZ\n'
        "                label = str(pr.get('label', f'plane{p_idx}'))\n"
        '                traces.append(go.Mesh3d(\n'
        '                    x=[0.0, x_max_um, x_max_um, 0.0], y=[0.0, 0.0, y_max_um, y_max_um], z=[z_um, z_um, z_um, '
        'z_um],\n'
        "                    i=[0, 0], j=[1, 2], k=[2, 3], name='Functional planes' if not plane_trace_indices else "
        'label,\n'
        '                    color=VIEWER_47B_PLANE_COLOR, opacity=VIEWER_47B_PLANE_OPACITY, visible=True,\n'
        "                    hovertext=f'{label}<br>best_z={float(bz):.2f}<br>z_um={z_um:.2f}', hoverinfo='text',\n"
        '                    showlegend=(len(plane_trace_indices) == 0),\n'
        '                ))\n'
        '                plane_trace_indices.append(len(traces) - 1)\n'
        '                always_visible.append(len(traces) - 1)\n'
        '\n'
        '            for gene in gene_order_plot:\n'
        '                xG, yG, zG, iG, jG, kG = _build_surface(gene_masks.get(gene), VIEWER_47B_STEP)\n'
        '                if xG.size == 0:\n'
        '                    continue\n'
        "                traces.append(go.Mesh3d(x=xG, y=yG, z=zG, i=iG, j=jG, k=kG, name=f'HCR {gene}', "
        "color=GENE_COLORS.get(gene, '#777777'), opacity=VIEWER_47B_HCR_OPACITY, visible=True, hoverinfo='skip', "
        'showlegend=True))\n'
        '                gene_trace_idx[gene] = len(traces) - 1\n'
        '\n'
        '            if not gene_trace_idx:\n'
        "                print('[47b] All gene meshes were empty after surface extraction.')\n"
        '            else:\n'
        '                total = len(traces)\n'
        '                vis_all = [False] * total\n'
        '                for idx in always_visible:\n'
        '                    vis_all[idx] = True\n'
        '                for idx in gene_trace_idx.values():\n'
        '                    vis_all[idx] = True\n'
        '                for i, tr in enumerate(traces):\n'
        '                    tr.visible = vis_all[i]\n'
        '\n'
        "                buttons = [dict(label='All genes', method='update', args=[{'visible': vis_all}, {'title': "
        "'HCR masks in 2P anatomy space: all genes'}])]\n"
        '                for gene in gene_order_plot:\n'
        '                    vis = [False] * total\n'
        '                    for idx in always_visible:\n'
        '                        vis[idx] = True\n'
        '                    idx = gene_trace_idx.get(gene)\n'
        '                    if idx is not None:\n'
        '                        vis[idx] = True\n'
        "                    buttons.append(dict(label=gene, method='update', args=[{'visible': vis}, {'title': f'HCR "
        "masks in 2P anatomy space: {gene}'}]))\n"
        '\n'
        '                fig = go.Figure(data=traces)\n'
        "                fig.update_layout(width=1450, height=1100, title='HCR masks in 2P anatomy space: all genes', "
        "scene=dict(xaxis=dict(title='X (µm)', range=[0, x_max_um]), yaxis=dict(title='Y (µm)', range=[0, y_max_um]), "
        "zaxis=dict(title='Z (µm)', range=[0, z_max_um]), aspectmode='data'), legend=dict(itemsizing='constant'), "
        "updatemenus=[dict(buttons=buttons, direction='down', x=0.01, y=1.08, xanchor='left', yanchor='top')], "
        'margin=dict(l=10, r=10, t=60, b=10))\n'
        '\n'
        '                FIG_47B = fig\n'
        '                FIG_47B_FISH_ID = FISH_ID\n'
        '\n'
        "                out_dir = HCR_QA_DIR if 'HCR_QA_DIR' in locals() else (HCR_ALIGNED_DIR.parent / 'qa')\n"
        '                out_dir.mkdir(parents=True, exist_ok=True)\n'
        "                out_html = out_dir / 'hcr_gene_plane_coverage_viewer.html'\n"
        '                try:\n'
        "                    fig.write_html(str(out_html), include_plotlyjs='cdn')\n"
        '                    FIG_47B_HTML_PATH = str(out_html)\n'
        "                    print(f'[47b] Saved interactive viewer to {out_html}')\n"
        '                except Exception as _e:\n'
        "                    print(f'[47b] Could not save HTML viewer: {_e}')\n"
        '\n'
        '                fig.show()\n',
 '50i': '# [50i]\n'
        'try:\n'
        '    REQUIRE_FISH_STATE = require_fish_state\n'
        'except NameError:\n'
        '    REQUIRE_FISH_STATE = None\n'
        'if callable(REQUIRE_FISH_STATE) and not REQUIRE_FISH_STATE():\n'
        '    raise SystemExit\n'
        '\n'
        '# Build and save the authoritative per-ROI table for all Suite2p-segmented cells.\n'
        '# Matching policy:\n'
        '#   1) match all functional ROIs to all anatomy labels on each plane\n'
        '#   2) gate candidate pairs by overlap / distance before assignment\n'
        '#   3) solve one 1-to-1 ROI<->anatomy assignment with an unmatched option\n'
        '#   4) attach molecular identity afterward from the matched anatomy label\n'
        'import numpy as np\n'
        'import pandas as pd\n'
        'from pathlib import Path\n'
        'from codeants_2pf_hcr import build_anat_identity_lookup_df, build_functional_roi_master_df, gene_from_mask\n'
        '\n'
        "ACTIVE_CLASS = 'Active neurons'\n"
        "INACTIVE_CLASS = 'Low-quality traces'\n"
        "IDENTITY_NONE = 'no identity assigned'\n"
        "CLAIM_MATCHED = 'matched unique anatomy'\n"
        "CLAIM_DUPLICATE = 'duplicate anatomy claim lost'\n"
        "CLAIM_UNMATCHED = 'no anatomy claim'\n"
        "PLANE_UNAVAILABLE = 'plane unavailable'\n"
        '\n'
        "OUT_ROI_IDENTITY_CSV = OUT_REG / 'functional_roi_activity_identity.csv'\n"
        "OUT_ROI_IDENTITY_SUMMARY_CSV = OUT_REG / 'functional_roi_activity_identity_summary.csv'\n"
        "OUT_ROI_IDENTITY_PLANE_SUMMARY_CSV = OUT_REG / 'functional_roi_activity_identity_by_plane.csv'\n"
        "OUT_ROI_IDENTITY_ANAT_CSV = OUT_REG / 'anatomy_identity_lookup.csv'\n"
        '\n'
        "FUNC_MATCH_OK = 'anatomy match'\n"
        "FUNC_NO_SLOT = 'no 1-to-1 anatomy slot'\n"
        "FUNC_NO_OVERLAP = 'no anatomy overlap candidate'\n"
        "FUNC_LOST_OVERLAP = 'overlap candidate lost in 1-to-1 assignment'\n"
        "FUNC_TOO_FAR = 'anatomy centroid distance above threshold'\n"
        "FUNC_NO_ANAT = 'no anatomy labels on plane'\n"
        'MIN_OVERLAP_FUNC_ANAT = 1\n'
        'REQUIRE_OVERLAP_FUNC_ANAT = True\n'
        'MAX_DIST_FUNC_ANAT = float(np.inf)\n'
        "MATCH_POLICY_VERSION = 'roi_all_vs_anat_all_candidate_gated_v1'\n"
        "DEFAULT_GENE_ORDER = ['sst1.1', 'sst1.2', 'npy', 'tac3b', 'pth2', 'cfos', 'cort']\n"
        '\n'
        'try:\n'
        '    SUITE2P_BY_REF_IDX = suite2p_by_ref_idx if suite2p_by_ref_idx else {}\n'
        'except NameError:\n'
        '    SUITE2P_BY_REF_IDX = {}\n'
        '\n'
        'try:\n'
        '    PLANE_REFS_LOCAL = plane_refs if plane_refs else []\n'
        'except NameError:\n'
        '    PLANE_REFS_LOCAL = []\n'
        '\n'
        'try:\n'
        '    HCR_MATCH_RESULTS_LOCAL = hcr_match_results if hcr_match_results else []\n'
        'except NameError:\n'
        '    HCR_MATCH_RESULTS_LOCAL = []\n'
        '\n'
        'try:\n'
        '    ANAT_LABELS_PATH_LOCAL = Path(ANAT_LABELS_PATH) if ANAT_LABELS_PATH is not None else None\n'
        'except NameError:\n'
        '    ANAT_LABELS_PATH_LOCAL = None\n'
        '\n'
        'BUILD_FUNCTIONAL_ROI_MASTER_DF = build_functional_roi_master_df\n'
        'BUILD_ANAT_IDENTITY_LOOKUP_DF = build_anat_identity_lookup_df\n'
        'GENE_FROM_MASK_FUNC = gene_from_mask\n'
        '\n'
        'try:\n'
        '    FISH_ID_LOCAL = FISH_ID\n'
        'except NameError:\n'
        '    FISH_ID_LOCAL = None\n'
        '\n'
        'try:\n'
        '    VOX_ANAT_LOCAL = VOX_ANAT if isinstance(VOX_ANAT, dict) else {}\n'
        'except NameError:\n'
        '    VOX_ANAT_LOCAL = {}\n'
        '\n'
        'try:\n'
        '    RUN_CONFIG_LOCAL = RUN_CONFIG if isinstance(RUN_CONFIG, dict) else {}\n'
        'except NameError:\n'
        '    RUN_CONFIG_LOCAL = {}\n'
        '\n'
        'if not SUITE2P_BY_REF_IDX:\n'
        "    print('[50i] Need suite2p_by_ref_idx; run [23a] first.')\n"
        'elif not PLANE_REFS_LOCAL:\n'
        "    print('[50i] Need plane_refs; run the functional-plane registration cells first.')\n"
        'elif not HCR_MATCH_RESULTS_LOCAL:\n'
        "    print('[50i] Need hcr_match_results; run [44] first.')\n"
        'elif ANAT_LABELS_PATH_LOCAL is None or not ANAT_LABELS_PATH_LOCAL.exists():\n'
        "    print('[50i] Need ANAT_LABELS_PATH; run the anatomy setup cells first.')\n"
        'elif BUILD_FUNCTIONAL_ROI_MASTER_DF is None or BUILD_ANAT_IDENTITY_LOOKUP_DF is None:\n'
        "    print('[50i] Matching helpers missing; run the utility cells first.')\n"
        'else:\n'
        "    DX = float(VOX_ANAT_LOCAL.get('X', 1.0)) if VOX_ANAT_LOCAL else 1.0\n"
        "    DY = float(VOX_ANAT_LOCAL.get('Y', 1.0)) if VOX_ANAT_LOCAL else 1.0\n"
        "    GENE_ORDER = list(RUN_CONFIG_LOCAL.get('GENE_ORDER', DEFAULT_GENE_ORDER))\n"
        '    gene_order_map = {str(g): i for i, g in enumerate(GENE_ORDER)}\n'
        '\n'
        '    twoP_labels_all = _ensure_uint_labels(imread_any(ANAT_LABELS_PATH_LOCAL))\n'
        '    anat_identity_df = BUILD_ANAT_IDENTITY_LOOKUP_DF(\n'
        '        HCR_MATCH_RESULTS_LOCAL,\n'
        '        gene_order=GENE_ORDER,\n'
        '        gene_from_mask_func=GENE_FROM_MASK_FUNC,\n'
        '    )\n'
        "    anat_identity_map = dict(zip(anat_identity_df['anat_label'], anat_identity_df['identity_label'])) if not "
        'anat_identity_df.empty else {}\n'
        "    anat_identity_gene_count = dict(zip(anat_identity_df['anat_label'], "
        "anat_identity_df['identity_gene_count'])) if not anat_identity_df.empty else {}\n"
        '\n'
        '    detail_df, plane_meta_df = BUILD_FUNCTIONAL_ROI_MASTER_DF(\n'
        '        SUITE2P_BY_REF_IDX,\n'
        '        PLANE_REFS_LOCAL,\n'
        '        twoP_labels_all,\n'
        '        dx_um=DX,\n'
        '        dy_um=DY,\n'
        '        fish_id=FISH_ID_LOCAL,\n'
        '        active_class=ACTIVE_CLASS,\n'
        '        inactive_class=INACTIVE_CLASS,\n'
        '        require_overlap=REQUIRE_OVERLAP_FUNC_ANAT,\n'
        '        min_overlap=MIN_OVERLAP_FUNC_ANAT,\n'
        '        max_dist_um=MAX_DIST_FUNC_ANAT,\n'
        '        plane_unavailable=PLANE_UNAVAILABLE,\n'
        '        func_match_ok=FUNC_MATCH_OK,\n'
        '        func_no_slot=FUNC_NO_SLOT,\n'
        '        func_no_overlap=FUNC_NO_OVERLAP,\n'
        '        func_lost_overlap=FUNC_LOST_OVERLAP,\n'
        '        func_too_far=FUNC_TOO_FAR,\n'
        '        func_no_anat=FUNC_NO_ANAT,\n'
        '        claim_matched=CLAIM_MATCHED,\n'
        '        claim_unmatched=CLAIM_UNMATCHED,\n'
        '        ensure_uint_labels_func=_ensure_uint_labels,\n'
        '        apply_func_orientation_func=_apply_func_orientation,\n'
        '        tform_for_plane_func=_tform_for_plane,\n'
        '        resample_labels_nn_func=resample_labels_nn,\n'
        '    )\n'
        '\n'
        '    if detail_df.empty:\n'
        "        print('[50i] No functional ROIs available after reconstruction.')\n"
        '    else:\n'
        "        detail_df['match_policy_version'] = MATCH_POLICY_VERSION\n"
        "        detail_df['matching_scope'] = 'all_segmented_functional_rois'\n"
        "        detail_df['identity_label'] = detail_df['anat_label'].map(anat_identity_map)\n"
        "        detail_df['identity_gene_count'] = detail_df['anat_label'].map(anat_identity_gene_count)\n"
        "        detail_df['has_identity_assigned'] = detail_df['identity_label'].notna()\n"
        "        detail_df['identity_display_label'] = "
        "detail_df['identity_label'].where(detail_df['has_identity_assigned'], IDENTITY_NONE)\n"
        "        detail_df['identity_display_label'] = detail_df['identity_display_label'].astype(str)\n"
        '\n'
        "        detail_df['n_total_rois_all_planes'] = int(len(detail_df))\n"
        "        detail_df['n_total_active_rois'] = int((detail_df['activity_class'] == ACTIVE_CLASS).sum())\n"
        "        detail_df['n_total_inactive_rois'] = int((detail_df['activity_class'] == INACTIVE_CLASS).sum())\n"
        '\n'
        '        def _identity_sort_key(label):\n'
        '            if label == IDENTITY_NONE:\n'
        '                return (10**6, label)\n'
        "            genes = tuple(str(label).split('/'))\n"
        '            ords = [gene_order_map.get(g, 10**6) for g in genes]\n'
        '            return (min(ords) if ords else 10**6, len(genes), tuple(ords), str(label))\n'
        '\n'
        '        summary_df = (\n'
        "            detail_df.groupby(['activity_class', 'identity_display_label'], as_index=False)\n"
        '            .size()\n'
        "            .rename(columns={'size': 'n_rois'})\n"
        '        )\n'
        "        summary_df['n_activity_total'] = summary_df.groupby('activity_class')['n_rois'].transform('sum')\n"
        "        summary_df['n_all_segmented'] = int(len(detail_df))\n"
        "        summary_df['pct_within_activity_class'] = np.where(\n"
        "            summary_df['n_activity_total'] > 0,\n"
        "            100.0 * summary_df['n_rois'] / summary_df['n_activity_total'],\n"
        '            np.nan,\n'
        '        )\n'
        "        summary_df['pct_of_all_segmented'] = np.where(\n"
        "            summary_df['n_all_segmented'] > 0,\n"
        "            100.0 * summary_df['n_rois'] / summary_df['n_all_segmented'],\n"
        '            np.nan,\n'
        '        )\n'
        '        activity_order = {ACTIVE_CLASS: 0, INACTIVE_CLASS: 1}\n'
        "        summary_df['activity_order'] = summary_df['activity_class'].map(activity_order).fillna(10**6)\n"
        "        summary_df['identity_order'] = summary_df['identity_display_label'].map(lambda s: "
        '_identity_sort_key(str(s)))\n'
        "        summary_df = summary_df.sort_values(['activity_order', "
        "'identity_order']).drop(columns=['activity_order', 'identity_order']).reset_index(drop=True)\n"
        '\n'
        '        plane_summary_df = (\n'
        "            detail_df.groupby(['activity_class', 'plane', 'plane_idx'], as_index=False)\n"
        '            .agg(\n'
        "                n_rois=('func_label', 'size'),\n"
        "                n_unique_anat_match=('has_unique_anat_match', 'sum'),\n"
        "                n_identity_assigned=('has_identity_assigned', 'sum'),\n"
        '            )\n'
        '        )\n'
        "        plane_summary_df['pct_unique_anat_match'] = np.where(\n"
        "            plane_summary_df['n_rois'] > 0,\n"
        "            100.0 * plane_summary_df['n_unique_anat_match'] / plane_summary_df['n_rois'],\n"
        '            np.nan,\n'
        '        )\n'
        "        plane_summary_df['pct_identity_assigned'] = np.where(\n"
        "            plane_summary_df['n_rois'] > 0,\n"
        "            100.0 * plane_summary_df['n_identity_assigned'] / plane_summary_df['n_rois'],\n"
        '            np.nan,\n'
        '        )\n'
        "        plane_summary_df['activity_order'] = "
        "plane_summary_df['activity_class'].map(activity_order).fillna(10**6)\n"
        "        plane_summary_df = plane_summary_df.sort_values(['activity_order', 'plane_idx', "
        "'plane']).drop(columns=['activity_order']).reset_index(drop=True)\n"
        '\n'
        '        OUT_ROI_IDENTITY_CSV.parent.mkdir(parents=True, exist_ok=True)\n'
        '        detail_df.to_csv(OUT_ROI_IDENTITY_CSV, index=False)\n'
        '        summary_df.to_csv(OUT_ROI_IDENTITY_SUMMARY_CSV, index=False)\n'
        '        plane_summary_df.to_csv(OUT_ROI_IDENTITY_PLANE_SUMMARY_CSV, index=False)\n'
        '        if not anat_identity_df.empty:\n'
        '            anat_identity_df.to_csv(OUT_ROI_IDENTITY_ANAT_CSV, index=False)\n'
        '\n'
        '        FUNC_ACTIVITY_IDENTITY_DF = detail_df.copy()\n'
        '        FUNC_ACTIVITY_IDENTITY_SUMMARY_DF = summary_df.copy()\n'
        '        FUNC_ACTIVITY_IDENTITY_PLANE_SUMMARY_DF = plane_summary_df.copy()\n'
        '        FUNC_ACTIVITY_IDENTITY_PLANE_META_DF = plane_meta_df.copy()\n'
        '        ANAT_IDENTITY_LOOKUP_DF = anat_identity_df.copy()\n'
        '        FUNC_ACTIVITY_IDENTITY_CSV = OUT_ROI_IDENTITY_CSV\n'
        '        FUNC_ACTIVITY_IDENTITY_SUMMARY_CSV = OUT_ROI_IDENTITY_SUMMARY_CSV\n'
        '        FUNC_ACTIVITY_IDENTITY_PLANE_SUMMARY_CSV = OUT_ROI_IDENTITY_PLANE_SUMMARY_CSV\n'
        '        ANAT_IDENTITY_LOOKUP_CSV = OUT_ROI_IDENTITY_ANAT_CSV\n'
        '        FUNC_ACTIVITY_IDENTITY_FISH_ID = FISH_ID_LOCAL\n'
        '        FUNC_ACTIVITY_IDENTITY_MATCH_POLICY = MATCH_POLICY_VERSION\n'
        '\n'
        '        n_total = int(len(detail_df))\n'
        "        n_active = int((detail_df['activity_class'] == ACTIVE_CLASS).sum())\n"
        "        n_inactive = int((detail_df['activity_class'] == INACTIVE_CLASS).sum())\n"
        "        n_ident_active = int(((detail_df['activity_class'] == ACTIVE_CLASS) & "
        "detail_df['has_identity_assigned']).sum())\n"
        "        n_ident_inactive = int(((detail_df['activity_class'] == INACTIVE_CLASS) & "
        "detail_df['has_identity_assigned']).sum())\n"
        "        n_matched = int((detail_df['plane_match_outcome'].astype(str) == FUNC_MATCH_OK).sum())\n"
        '        print(\n'
        "            f'[50i] Saved authoritative ROI activity/identity table for {n_total} segmented functional ROIs "
        "'\n"
        "            f'({n_active} active, {n_inactive} inactive).'\n"
        '        )\n'
        '        print(\n'
        "            f'[50i] Geometry-matched ROIs={n_matched}; identity assigned: '\n"
        "            f'active={n_ident_active}, inactive={n_ident_inactive}.'\n"
        '        )\n'
        '        print(\n'
        "            f'[50i] Policy={MATCH_POLICY_VERSION}; '\n"
        "            f'REQUIRE_OVERLAP_FUNC_ANAT={REQUIRE_OVERLAP_FUNC_ANAT}; '\n"
        "            f'MIN_OVERLAP_FUNC_ANAT={MIN_OVERLAP_FUNC_ANAT}; '\n"
        '            f\'MAX_DIST_FUNC_ANAT={MAX_DIST_FUNC_ANAT if np.isfinite(MAX_DIST_FUNC_ANAT) else "inf"}\'\n'
        '        )\n'
        "        print(f'[50i] detail -> {OUT_ROI_IDENTITY_CSV}')\n"
        "        print(f'[50i] summary -> {OUT_ROI_IDENTITY_SUMMARY_CSV}')\n"
        '        try:\n'
        '            display(summary_df)\n'
        '        except Exception:\n'
        '            print(summary_df.to_string(index=False))\n'
        '        try:\n'
        '            display(plane_summary_df)\n'
        '        except Exception:\n'
        '            print(plane_summary_df.to_string(index=False))\n',
 '50ia': '# [50ia]\n'
         'try:\n'
         '    REQUIRE_FISH_STATE = require_fish_state\n'
         'except NameError:\n'
         '    REQUIRE_FISH_STATE = None\n'
         'if callable(REQUIRE_FISH_STATE) and not REQUIRE_FISH_STATE():\n'
         '    raise SystemExit\n'
         '\n'
         'import numpy as np\n'
         'import pandas as pd\n'
         'from pathlib import Path\n'
         '\n'
         'from codeants_2pf_hcr import ActivityConfig, build_response_bpi_tables\n'
         '\n'
         "ACTIVE_CLASS = 'Active neurons'\n"
         "INACTIVE_CLASS = 'Low-quality traces'\n"
         "RESPONSE_LOW = 'low activity'\n"
         "RESPONSE_UNAVAILABLE = 'response unavailable'\n"
         '\n'
         'try:\n'
         '    RUN_CONFIG_LOCAL = RUN_CONFIG if isinstance(RUN_CONFIG, dict) else {}\n'
         'except NameError:\n'
         '    RUN_CONFIG_LOCAL = {}\n'
         'try:\n'
         '    FISH_DIR_LOCAL = Path(FISH_DIR) if FISH_DIR is not None else None\n'
         'except NameError:\n'
         '    FISH_DIR_LOCAL = None\n'
         'try:\n'
         '    FISH_ID_LOCAL = FISH_ID\n'
         'except NameError:\n'
         '    FISH_ID_LOCAL = None\n'
         '\n'
         "FUNC_ACTIVITY_BPI_ZERO_BAND = float(RUN_CONFIG_LOCAL.get('FUNC_ACTIVITY_BPI_ZERO_BAND', 0.50))\n"
         "FUNC_ACTIVITY_BPI_MIN_TRIALS_PER_CLASS = int(RUN_CONFIG_LOCAL.get('FUNC_ACTIVITY_BPI_MIN_TRIALS_PER_CLASS', "
         '3))\n'
         "FUNC_ACTIVITY_BPI_DENOM_EPS = float(RUN_CONFIG_LOCAL.get('FUNC_ACTIVITY_BPI_DENOM_EPS', 1e-6))\n"
         "FUNC_ACTIVITY_BPI_EDGE_POLICY = str(RUN_CONFIG_LOCAL.get('FUNC_ACTIVITY_BPI_EDGE_POLICY', 'pad_nan'))\n"
         "FUNC_ACTIVITY_BPI_MIN_VALID_FRAC = float(RUN_CONFIG_LOCAL.get('FUNC_ACTIVITY_BPI_MIN_VALID_FRAC', 0.5))\n"
         "FUNC_ACTIVITY_BPI_STIM_ONSET_DELAY_SEC = float(RUN_CONFIG_LOCAL.get('STIM_ONSET_DELAY_SEC', 10.0))\n"
         "FUNC_RESPONSE_MIN_AUC = float(RUN_CONFIG_LOCAL.get('FUNC_RESPONSE_MIN_AUC', 0.05))\n"
         "FUNC_RESPONSE_NULL_Q = float(RUN_CONFIG_LOCAL.get('FUNC_RESPONSE_NULL_Q', 0.99))\n"
         "FUNC_RESPONSE_NULL_BOOTSTRAP_N = int(RUN_CONFIG_LOCAL.get('FUNC_RESPONSE_NULL_BOOTSTRAP_N', 2000))\n"
         "FUNC_RESPONSE_NULL_MIN_WINDOWS = int(RUN_CONFIG_LOCAL.get('FUNC_RESPONSE_NULL_MIN_WINDOWS', 20))\n"
         "FUNC_RESPONSE_NULL_STEP_SEC = float(RUN_CONFIG_LOCAL.get('FUNC_RESPONSE_NULL_STEP_SEC', 0.5))\n"
         "FUNC_RESPONSE_RNG_SEED = int(RUN_CONFIG_LOCAL.get('FUNC_RESPONSE_RNG_SEED', 50))\n"
         'FUNC_ACTIVITY_BPI_STIM_TIME_SCALE = 1.0\n'
         'FUNC_ACTIVITY_BPI_MEASURE_START_BLOCK = 1\n'
         "FUNC_ACTIVITY_BPI_MEASURE_START_EVENT = 'start'\n"
         "FUNC_ACTIVITY_BPI_REMOVE_INTERBLOCK_GAPS = bool(RUN_CONFIG_LOCAL.get('REMOVE_INTERBLOCK_GAPS', True))\n"
         "FUNC_ACTIVITY_FORCE_RECOMPUTE = bool(RUN_CONFIG_LOCAL.get('FUNC_ACTIVITY_FORCE_RECOMPUTE', True))\n"
         "FUNC_ACTIVITY_BPI_CELLS_CSV = OUT_REG / 'functional_roi_activity_bpi_cells.csv'\n"
         "FUNC_ACTIVITY_BPI_SUMMARY_CSV = OUT_REG / 'functional_roi_activity_bpi_summary.csv'\n"
         "FUNC_ACTIVITY_BPI_DETAIL_CSV = OUT_REG / 'functional_roi_activity_identity.csv'\n"
         "FUNC_ACTIVITY_BPI_SUITE2P_ROOT = OUTDIR / 'suite2P'\n"
         "FUNC_ACTIVITY_PREIDENTITY_CSV = OUT_QA / 'suite2p_response_bpi_cells_23c.csv'\n"
         '\n'
         'detail_csv = FUNC_ACTIVITY_BPI_DETAIL_CSV\n'
         'if not detail_csv.exists():\n'
         "    print(f'[50ia] Missing ROI identity table: {detail_csv}. Run [50i] first.')\n"
         'else:\n'
         '    detail_df = pd.read_csv(detail_csv)\n'
         '    skip_computation = False\n'
         '    if (not FUNC_ACTIVITY_FORCE_RECOMPUTE) and FUNC_ACTIVITY_BPI_CELLS_CSV.exists() and '
         'FUNC_ACTIVITY_BPI_SUMMARY_CSV.exists():\n'
         "        print('[50ia] BPI output CSVs exist and FUNC_ACTIVITY_FORCE_RECOMPUTE=False, skipping "
         "computation.')\n"
         '        skip_computation = True\n'
         '\n'
         '    if skip_computation:\n'
         '        FUNC_ACTIVITY_IDENTITY_DF = detail_df.copy()\n'
         '        FUNC_ACTIVITY_BPI_DF = pd.read_csv(FUNC_ACTIVITY_BPI_CELLS_CSV)\n'
         '        FUNC_ACTIVITY_BPI_SUMMARY_DF = pd.read_csv(FUNC_ACTIVITY_BPI_SUMMARY_CSV)\n'
         '        FUNC_ACTIVITY_IDENTITY_CSV = detail_csv\n'
         '        FUNC_ACTIVITY_BPI_FISH_ID = FISH_ID_LOCAL\n'
         '    else:\n'
         '        if FISH_DIR_LOCAL is None:\n'
         "            raise RuntimeError('[50ia] FISH_DIR is not available.')\n"
         '        precomputed_scored_bpi_df = None\n'
         '        if FUNC_ACTIVITY_PREIDENTITY_CSV.exists():\n'
         '            precomputed_scored_bpi_df = pd.read_csv(FUNC_ACTIVITY_PREIDENTITY_CSV)\n'
         "            print(f'[50ia] Reusing pre-identity response calls from {FUNC_ACTIVITY_PREIDENTITY_CSV}')\n"
         '        activity_result = build_response_bpi_tables(\n'
         '            detail_df,\n'
         '            fish_dir=FISH_DIR_LOCAL,\n'
         '            fish_id=str(FISH_ID_LOCAL),\n'
         '            suite2p_root=FUNC_ACTIVITY_BPI_SUITE2P_ROOT,\n'
         '            precomputed_scored_bpi_df=precomputed_scored_bpi_df,\n'
         '            config=ActivityConfig(\n'
         '                active_class=ACTIVE_CLASS,\n'
         '                inactive_class=INACTIVE_CLASS,\n'
         '                zero_band=FUNC_ACTIVITY_BPI_ZERO_BAND,\n'
         '                min_trials_per_class=FUNC_ACTIVITY_BPI_MIN_TRIALS_PER_CLASS,\n'
         '                denom_eps=FUNC_ACTIVITY_BPI_DENOM_EPS,\n'
         '                edge_policy=FUNC_ACTIVITY_BPI_EDGE_POLICY,\n'
         '                min_valid_frac=FUNC_ACTIVITY_BPI_MIN_VALID_FRAC,\n'
         '                stim_onset_delay_sec=FUNC_ACTIVITY_BPI_STIM_ONSET_DELAY_SEC,\n'
         '                response_min_auc=FUNC_RESPONSE_MIN_AUC,\n'
         '                response_null_q=FUNC_RESPONSE_NULL_Q,\n'
         '                response_null_bootstrap_n=FUNC_RESPONSE_NULL_BOOTSTRAP_N,\n'
         '                response_null_min_windows=FUNC_RESPONSE_NULL_MIN_WINDOWS,\n'
         '                response_null_step_sec=FUNC_RESPONSE_NULL_STEP_SEC,\n'
         '                response_rng_seed=FUNC_RESPONSE_RNG_SEED,\n'
         '                stim_time_scale=FUNC_ACTIVITY_BPI_STIM_TIME_SCALE,\n'
         '                measure_start_block=FUNC_ACTIVITY_BPI_MEASURE_START_BLOCK,\n'
         '                measure_start_event=FUNC_ACTIVITY_BPI_MEASURE_START_EVENT,\n'
         '                remove_interblock_gaps=FUNC_ACTIVITY_BPI_REMOVE_INTERBLOCK_GAPS,\n'
         '            ),\n'
         '        )\n'
         '\n'
         "        detail_df = activity_result['detail_df']\n"
         "        scored_bpi_df = activity_result['scored_bpi_df']\n"
         "        summary_df = activity_result['summary_df']\n"
         "        stim_source = activity_result['stim_source']\n"
         "        stim_events = activity_result['stim_events']\n"
         '\n'
         '        detail_csv.parent.mkdir(parents=True, exist_ok=True)\n'
         '        detail_df.to_csv(detail_csv, index=False)\n'
         '        scored_bpi_df.to_csv(FUNC_ACTIVITY_BPI_CELLS_CSV, index=False)\n'
         '        summary_df.to_csv(FUNC_ACTIVITY_BPI_SUMMARY_CSV, index=False)\n'
         '\n'
         '        FUNC_ACTIVITY_IDENTITY_DF = detail_df.copy()\n'
         '        FUNC_ACTIVITY_BPI_DF = scored_bpi_df.copy()\n'
         '        FUNC_ACTIVITY_BPI_SUMMARY_DF = summary_df.copy()\n'
         '        FUNC_ACTIVITY_IDENTITY_CSV = detail_csv\n'
         '        FUNC_ACTIVITY_BPI_FISH_ID = FISH_ID_LOCAL\n'
         '\n'
         "        n_responsive = int(detail_df['response_is_active'].sum())\n"
         "        n_low = int((detail_df['response_class'] == RESPONSE_LOW).sum())\n"
         "        n_unavailable = int((detail_df['response_class'] == RESPONSE_UNAVAILABLE).sum())\n"
         "        n_low_quality = int((~pd.Series(detail_df.get('suite2p_is_cell', "
         'False)).fillna(False).astype(bool)).sum())\n'
         '        if precomputed_scored_bpi_df is not None:\n'
         "            print(f'[50ia] Response/BPI categories merged for all segmented ROIs from pre-identity "
         "Suite2p response calls ({FUNC_ACTIVITY_PREIDENTITY_CSV}).')\n"
         '        else:\n'
         "            print(f'[50ia] Response/BPI categories assigned for all segmented ROIs using {len(stim_events)} "
         "stimulus windows from {stim_source}.')\n"
         '        print(\n'
         "            f'[50ia] thresholds: mean AUC >= {FUNC_RESPONSE_MIN_AUC:.3f} dF/F·s and '\n"
         "            f'> Q{100.0 * FUNC_RESPONSE_NULL_Q:.0f} baseline null; |BPI| <= "
         "{FUNC_ACTIVITY_BPI_ZERO_BAND:.2f} -> both-responsive'\n"
         '        )\n'
         "        print(f'[50ia] segmented ROIs={len(detail_df)}; responsive={n_responsive}; low activity={n_low}; "
         "unavailable={n_unavailable}; low-quality traces={n_low_quality}')\n"
         "        print(f'[50ia] scored ROI table -> {FUNC_ACTIVITY_BPI_CELLS_CSV}')\n"
         "        print(f'[50ia] summary -> {FUNC_ACTIVITY_BPI_SUMMARY_CSV}')\n"
         '        try:\n'
         '            display(summary_df)\n'
         '        except Exception:\n'
         '            print(summary_df.to_string(index=False))\n',
 '51': '# [51]\n'
       'try:\n'
       '    _require_fish_state_51 = require_fish_state\n'
       'except NameError:\n'
       '    _require_fish_state_51 = None\n'
       'if _require_fish_state_51 is not None and (not _require_fish_state_51()):\n'
       '    raise SystemExit\n'
       '\n'
       '# Suite2p dF/F traces for HCR-identified cells whose local best responsive functional ROI is exported.\n'
       '# Reused ROIs are deduplicated within gene before export so fragmented anatomy labels\n'
       '# do not double-count the same functional trace.\n'
       'from pathlib import Path\n'
       '\n'
       'from codeants_2pf_hcr import (\n'
       '    TraceExportConfig,\n'
       '    export_suite2p_trace_metadata,\n'
       '    gene_from_mask,\n'
       '    resolve_conf_func_csv_analysis,\n'
       ')\n'
       '\n'
       'try:\n'
       '    SUITE2P_BY_REF_IDX = suite2p_by_ref_idx\n'
       '    SUITE2P_STATE_FISH_ID = SUITE2P_FISH_ID\n'
       'except NameError:\n'
       "    print('[Suite2p] Suite2p data not loaded; run [23a] first.')\n"
       '    raise SystemExit\n'
       'if SUITE2P_STATE_FISH_ID != FISH_ID:\n'
       "    print('[Suite2p] Suite2p data from different fish; run [23a].')\n"
       '    raise SystemExit\n'
       '\n'
       'try:\n'
       '    RUN_CONFIG_LOCAL = RUN_CONFIG if isinstance(RUN_CONFIG, dict) else {}\n'
       'except NameError:\n'
       '    RUN_CONFIG_LOCAL = {}\n'
       '\n'
       'try:\n'
       '    _conf_csv_override = CONF_FUNC_CSV_ANALYSIS\n'
       'except NameError:\n'
       '    _conf_csv_override = None\n'
       '\n'
       'RECOMPUTE_SUITE2P_TRACE_EXPORT = True\n'
       'try:\n'
       '    MATCH_POLICY_VERSION = str(HCR_ACTIVITY_MATCH_POLICY)\n'
       'except NameError:\n'
       "    MATCH_POLICY_VERSION = 'hcr_anat_first_local_geometry_response_v4_suite2p_gate'\n"
       '\n'
       "SUITE2P_TRACE_EXPORT_DIR = OUT_DERIVED / 'suite2p_traces'\n"
       'CONF_FUNC_CSV = resolve_conf_func_csv_analysis(\n'
       '    out_reg=OUT_REG,\n'
       '    run_config=RUN_CONFIG_LOCAL,\n'
       '    conf_func_csv=_conf_csv_override,\n'
       '    fish_id=FISH_ID,\n'
       ')\n'
       '\n'
       'trace_result = export_suite2p_trace_metadata(\n'
       '    fish_id=str(FISH_ID),\n'
       '    out_dir=SUITE2P_TRACE_EXPORT_DIR,\n'
       '    suite2p_by_ref_idx=SUITE2P_BY_REF_IDX,\n'
       '    conf_func_csv=Path(CONF_FUNC_CSV),\n'
       '    config=TraceExportConfig(\n'
       '        recompute=RECOMPUTE_SUITE2P_TRACE_EXPORT,\n'
       '        match_policy_version=MATCH_POLICY_VERSION,\n'
       '    ),\n'
       '    gene_from_mask_func=gene_from_mask,\n'
       ')\n'
       '\n'
       "for _line in trace_result.get('log_lines', []):\n"
       '    print(_line)\n'
       "print(trace_result['message'])\n"
       '\n'
       "if trace_result['status'] in {'cached', 'exported'}:\n"
       "    SUITE2P_DFF_META_CSV = str(trace_result['meta_csv'])\n"
       "    SUITE2P_DFF_META_DF = trace_result['meta_df']\n"
       '    try:\n'
       '        display(SUITE2P_DFF_META_DF.head())\n'
       '    except Exception:\n'
       '        print(SUITE2P_DFF_META_DF.head().to_string(index=False))\n',
 '54': '# [54]\n'
       'try:\n'
       '    REQUIRE_FISH_STATE = require_fish_state\n'
       'except NameError:\n'
       '    REQUIRE_FISH_STATE = None\n'
       'if callable(REQUIRE_FISH_STATE) and not REQUIRE_FISH_STATE():\n'
       '    raise SystemExit\n'
       '# Functional plane masks + intensity warped into 2P space (per plane, using stored transforms)\n'
       'import numpy as np\n'
       'import tifffile as tiff\n'
       'from pathlib import Path\n'
       '\n'
       'RECOMPUTE_FUNC_WARP_EXPORT = False\n'
       '\n'
       'if not plane_refs:\n'
       "    print('No plane_refs available; run earlier cells to populate them.')\n"
       'else:\n'
       '    try:\n'
       '        anat_full = _ensure_uint_labels(imread_any(ANAT_LABELS_PATH))\n'
       '    except Exception as e:\n'
       '        anat_full = None\n'
       "        print('Could not load ANAT_LABELS_PATH:', e)\n"
       '    if anat_full is None:\n'
       "        print('Missing anatomy labels; cannot warp functional data.')\n"
       '    else:\n'
       "        DZ = float(VOX_ANAT.get('Z', 1.0)) if VOX_ANAT else 1.0\n"
       "        DY = float(VOX_ANAT.get('Y', 1.0)) if VOX_ANAT else 1.0\n"
       "        DX = float(VOX_ANAT.get('X', 1.0)) if VOX_ANAT else 1.0\n"
       '        mask_outpaths = []\n'
       '        ref_outpaths = []\n'
       "        if '_rescale_labels_to_ref' not in locals():\n"
       '            def _rescale_labels_to_ref(labels, ref_shape):\n'
       '                if labels is None or ref_shape is None:\n'
       '                    return labels\n'
       '                try:\n'
       '                    if labels.shape == ref_shape:\n'
       '                        return labels\n'
       '                except Exception:\n'
       '                    return labels\n'
       '                try:\n'
       '                    from skimage.transform import resize\n'
       '                    return resize(labels.astype(float), ref_shape, order=0, preserve_range=True, '
       'anti_aliasing=False).astype(labels.dtype)\n'
       '                except Exception:\n'
       '                    return labels\n'
       '\n'
       '\n'
       '        def _load_func_plane(pr, p_idx):\n'
       '            try:\n'
       '                arr, _ = _get_labels_for_plane(pr, p_idx)\n'
       '                if arr is not None:\n'
       '                    return arr\n'
       '            except Exception:\n'
       '                pass\n'
       '            if False:\n'
       '                try:\n'
       '                    arr, _ = _get_labels_for_plane(pr, p_idx)\n'
       '                    if arr is not None:\n'
       '                        return arr\n'
       '                except Exception:\n'
       '                    pass\n'
       "            if callable(_load_func_labels_for_plane) if '_load_func_labels_for_plane' in locals() else False:\n"
       '                try:\n'
       '                    arr, _, _ = _load_func_labels_for_plane(p_idx)\n'
       '                    if arr is not None:\n'
       '                        return arr\n'
       '                except Exception:\n'
       '                    pass\n'
       '            if FUNC_LABELS_PATH is not None:\n'
       '                try:\n'
       '                    arr = _ensure_uint_labels(imread_any(FUNC_LABELS_PATH))\n'
       '                    if arr.ndim == 3 and p_idx < arr.shape[0]:\n'
       '                        return arr[p_idx]\n'
       '                    if arr.ndim == 2:\n'
       '                        return arr\n'
       '                except Exception:\n'
       '                    pass\n'
       '            # search output dirs for masks named like plane label\n'
       "            lbl = pr.get('label', f'plane{p_idx}')\n"
       '            bases = []\n'
       '            if OUT_SEG is not None:\n'
       '                bases.append(Path(OUT_SEG))\n'
       "                bases.append(Path(OUT_SEG) / 'cp_masks')\n"
       '            if OUTDIR is not None:\n'
       '                bases.append(Path(OUTDIR))\n'
       '            for base in bases:\n'
       '                if not base.exists():\n'
       '                    continue\n'
       '                for pat in [f"{lbl}*mask*.tif", f"{lbl}*labels*.tif", f"{lbl}*func*.tif"]:\n'
       '                    for p in base.glob(pat):\n'
       '                        try:\n'
       '                            arr = _ensure_uint_labels(imread_any(p))\n'
       '                            if arr is not None:\n'
       '                                return arr\n'
       '                        except Exception:\n'
       '                            pass\n'
       '            return None\n'
       '\n'
       '        def _load_func_ref(pr, p_idx, lbl=None):\n'
       '            def _try_path(path):\n'
       '                if not path:\n'
       '                    return None\n'
       '                try:\n'
       '                    return imread_any(path)\n'
       '                except Exception:\n'
       '                    return None\n'
       '            def _try_outdir_guess(label):\n'
       '                bases = []\n'
       '                if OUT_DERIVED is not None:\n'
       '                    bases.append(Path(OUT_DERIVED))\n'
       '                if OUT_RAW is not None:\n'
       '                    bases.append(Path(OUT_RAW))\n'
       '                if OUT_REG is not None:\n'
       '                    bases.append(Path(OUT_REG))\n'
       '                if OUTDIR is not None:\n'
       '                    bases.append(Path(OUTDIR))\n'
       '                patterns = [\n'
       '                    f"{label}*func_ref*.tif",\n'
       '                    f"{label}*func_ref*.tiff",\n'
       '                    f"{label}*func_img*.tif",\n'
       '                    f"{label}*func*.tif",\n'
       '                    f"{label}*ref_norm*.tif",\n'
       '                    f"{label}*ref_raw*.tif",\n'
       '                ]\n'
       '                for base in bases:\n'
       '                    if not base.exists():\n'
       '                        continue\n'
       '                    for pat in patterns:\n'
       '                        for p in base.glob(pat):\n'
       '                            if p.is_file():\n'
       '                                arr = _try_path(p)\n'
       '                                if arr is not None:\n'
       '                                    return arr\n'
       '                return None\n'
       '            # prefer references already attached to plane_refs (from cell [12])\n'
       "            for key in ('ref_match', 'ref2d', 'ref2d_raw'):\n"
       '                if key in pr and pr[key] is not None:\n'
       '                    return pr[key]\n'
       '            # per-plane hints\n'
       "            for key in ('func_ref', 'func_ref_path', 'func_img', 'func_img_path'):\n"
       '                arr = _try_path(pr.get(key))\n'
       '                if arr is not None:\n'
       '                    return arr\n'
       '            # global fallbacks from canonical path variables\n'
       '            for candidate in [\n'
       "                FUNC_REF_PATH if 'FUNC_REF_PATH' in locals() else None,\n"
       "                FUNC_IMG_PATH if 'FUNC_IMG_PATH' in locals() else None,\n"
       "                FUNC_PATH if 'FUNC_PATH' in locals() else None,\n"
       '            ]:\n'
       '                arr = _try_path(candidate)\n'
       '                if arr is not None:\n'
       '                    return arr\n'
       '            # OUTDIR search using plane label\n'
       '            if lbl is not None:\n'
       '                arr = _try_outdir_guess(lbl)\n'
       '                if arr is not None:\n'
       '                    return arr\n'
       '            return None\n'
       '\n'
       '        def _as_2d(arr, p_idx):\n'
       '            if arr is None:\n'
       '                return None\n'
       '            if arr.ndim == 3 and arr.shape[-1] in (3, 4):\n'
       '                arr = arr[..., 0]\n'
       '            if arr.ndim == 3 and p_idx < arr.shape[0]:\n'
       '                arr = arr[p_idx]\n'
       '            if arr.ndim == 3 and arr.shape[0] == 1:\n'
       '                arr = arr[0]\n'
       '            if arr.ndim != 2:\n'
       '                return None\n'
       '            return arr\n'
       '\n'
       '        for p_idx, pr in enumerate(plane_refs):\n'
       "            lbl = pr.get('label', f'plane{p_idx}')\n"
       '            out_path = OUT_DERIVED / f"{lbl}_func_mask_in_2p.tif"\n'
       '            out_path_ref = OUT_DERIVED / f"{lbl}_func_ref_in_2p.tif"\n'
       '            if (not RECOMPUTE_FUNC_WARP_EXPORT) and out_path.exists() and out_path_ref.exists():\n'
       '                mask_outpaths.append(out_path)\n'
       '                ref_outpaths.append(out_path_ref)\n'
       '                print(f"[warp] Reusing cached outputs for {lbl}: {out_path.name}, {out_path_ref.name}")\n'
       '                continue\n'
       '            # --- Masks ---\n'
       '            func_raw = _load_func_plane(pr, p_idx)\n'
       '            func_raw = _as_2d(func_raw, p_idx)\n'
       '            ref_shape = None\n'
       '            try:\n'
       "                if pr.get('ref_match') is not None:\n"
       "                    ref_shape = np.asarray(pr.get('ref_match')).shape\n"
       "                elif pr.get('ref2d_raw') is not None:\n"
       "                    ref_shape = np.asarray(pr.get('ref2d_raw')).shape\n"
       "                elif pr.get('ref2d') is not None:\n"
       "                    ref_shape = np.asarray(pr.get('ref2d')).shape\n"
       '            except Exception:\n'
       '                ref_shape = None\n'
       '            if ref_shape is not None:\n'
       '                try:\n'
       '                    func_raw = _rescale_labels_to_ref(func_raw, ref_shape)\n'
       '                except Exception:\n'
       '                    pass\n'
       '            if func_raw is None:\n'
       '                print(f"[warp] Missing functional labels for {lbl}; skipping masks.")\n'
       '            else:\n'
       "                bz = int(pr.get('best_z', 0))\n"
       '                anat_slice = anat_full[bz] if anat_full.ndim == 3 and bz < anat_full.shape[0] else anat_full\n'
       '                tform_use = _tform_for_plane(pr)\n'
       '                try:\n'
       '                    if tform_use is None:\n'
       '                        if func_raw.shape == anat_slice.shape:\n'
       '                            func_warped = func_raw\n'
       '                        else:\n'
       '                            from skimage.transform import AffineTransform\n'
       '                            func_warped = resample_labels_nn(func_raw, AffineTransform(), '
       'output_shape=anat_slice.shape)\n'
       '                    else:\n'
       '                        func_warped = resample_labels_nn(func_raw, tform_use, output_shape=anat_slice.shape)\n'
       '                except Exception as e:\n'
       '                    print(f"[warp] Resample failed for mask {lbl}: {e}")\n'
       '                    func_warped = None\n'
       '                if func_warped is not None:\n'
       '                    func_warped = _ensure_uint_labels(func_warped)\n'
       '                    out_path = OUT_DERIVED / f"{lbl}_func_mask_in_2p.tif"\n'
       '                    try:\n'
       '                        tiff_kwargs = {\n'
       "                            'imagej': True,\n"
       "                            'compression': 'deflate',\n"
       "                            'metadata': {'axes': 'YX', 'spacing': DY, 'unit': 'um'}\n"
       '                        }\n'
       '                        dtype = np.uint16 if func_warped.max() <= np.iinfo(np.uint16).max else np.uint32\n'
       '                        tiff.imwrite(out_path, func_warped.astype(dtype), **tiff_kwargs)\n'
       '                        mask_outpaths.append(out_path)\n'
       '                        print(f"[warp] Saved {out_path} (mask, shape={func_warped.shape}, best_z={bz})")\n'
       '                    except Exception as e:\n'
       '                        print(f"[warp] Failed to save {out_path}: {e}")\n'
       '\n'
       '            # --- Intensity reference ---\n'
       '            func_ref = _load_func_ref(pr, p_idx, lbl)\n'
       '            func_ref = _as_2d(func_ref, p_idx)\n'
       '            if func_ref is None:\n'
       '                print(f"[warp] Missing functional reference for {lbl}; skipping intensity.")\n'
       '                continue\n'
       "            bz = int(pr.get('best_z', 0))\n"
       '            anat_slice = anat_full[bz] if anat_full.ndim == 3 and bz < anat_full.shape[0] else anat_full\n'
       '            tform_use = _tform_for_plane(pr)\n'
       '            try:\n'
       '                if tform_use is None:\n'
       '                    if func_ref.shape == anat_slice.shape:\n'
       '                        func_ref_warped = func_ref\n'
       '                    else:\n'
       '                        from skimage.transform import AffineTransform\n'
       '                        resampler = resample_image\n'
       '                        func_ref_warped = resampler(func_ref, AffineTransform(), '
       'output_shape=anat_slice.shape)\n'
       '                else:\n'
       '                    resampler = resample_image\n'
       '                    func_ref_warped = resampler(func_ref, tform_use, output_shape=anat_slice.shape)\n'
       '            except Exception as e:\n'
       '                print(f"[warp] Resample failed for reference {lbl}: {e}")\n'
       '                continue\n'
       '            func_ref_warped = np.asarray(func_ref_warped, dtype=np.float32)\n'
       '            out_path_ref = OUT_DERIVED / f"{lbl}_func_ref_in_2p.tif"\n'
       '            try:\n'
       '                tiff_kwargs = {\n'
       "                    'imagej': True,\n"
       "                    'compression': 'deflate',\n"
       "                    'metadata': {'axes': 'YX', 'spacing': DY, 'unit': 'um'}\n"
       '                }\n'
       '                tiff.imwrite(out_path_ref, func_ref_warped, **tiff_kwargs)\n'
       '                ref_outpaths.append(out_path_ref)\n'
       '                print(f"[warp] Saved {out_path_ref} (intensity, shape={func_ref_warped.shape}, best_z={bz})")\n'
       '            except Exception as e:\n'
       '                print(f"[warp] Failed to save {out_path_ref}: {e}")\n'
       '\n'
       '        if mask_outpaths:\n'
       "            print('Warped functional masks saved:' + ''.join(map(str, mask_outpaths)))\n"
       '        if ref_outpaths:\n'
       "            print('Warped functional references saved:' + ''.join(map(str, ref_outpaths)))\n",
 '56d': '# [56d]\n'
        '# Side-by-side composite: [53a] (left) + [56] (right)\n'
        'import numpy as np\n'
        'import matplotlib.pyplot as plt\n'
        'from pathlib import Path\n'
        '\n'
        'try:\n'
        '    REQUIRE_FISH_STATE = require_fish_state\n'
        'except NameError:\n'
        '    REQUIRE_FISH_STATE = None\n'
        'if callable(REQUIRE_FISH_STATE) and not REQUIRE_FISH_STATE():\n'
        '    raise SystemExit\n'
        '\n'
        '\n'
        'def _fig_to_rgba(fig):\n'
        '    try:\n'
        '        fig.canvas.draw()\n'
        '        return np.asarray(fig.canvas.buffer_rgba()).copy()\n'
        '    except Exception:\n'
        '        return None\n'
        '\n'
        '\n'
        'try:\n'
        '    FIG_53A_RGBA_LOCAL = FIG_53A_RGBA\n'
        'except NameError:\n'
        '    FIG_53A_RGBA_LOCAL = None\n'
        'try:\n'
        '    FIG_53A_LAST_LOCAL = FIG_53A_LAST\n'
        'except NameError:\n'
        '    FIG_53A_LAST_LOCAL = None\n'
        'try:\n'
        '    FIG_53A_FISH_ID_LOCAL = FIG_53A_FISH_ID\n'
        'except NameError:\n'
        '    FIG_53A_FISH_ID_LOCAL = None\n'
        'try:\n'
        '    FIG_56_RGBA_LOCAL = FIG_56_RGBA\n'
        'except NameError:\n'
        '    FIG_56_RGBA_LOCAL = None\n'
        'try:\n'
        '    FIG_56_LAST_LOCAL = FIG_56_LAST\n'
        'except NameError:\n'
        '    FIG_56_LAST_LOCAL = None\n'
        'try:\n'
        '    FIG_56_FISH_ID_LOCAL = FIG_56_FISH_ID\n'
        'except NameError:\n'
        '    FIG_56_FISH_ID_LOCAL = None\n'
        '\n'
        'left = FIG_53A_RGBA_LOCAL\n'
        'if left is None and FIG_53A_LAST_LOCAL is not None:\n'
        '    left = _fig_to_rgba(FIG_53A_LAST_LOCAL)\n'
        '\n'
        'right = FIG_56_RGBA_LOCAL\n'
        'if right is None and FIG_56_LAST_LOCAL is not None:\n'
        '    right = _fig_to_rgba(FIG_56_LAST_LOCAL)\n'
        '\n'
        'if FIG_53A_FISH_ID_LOCAL not in (None, FISH_ID):\n'
        "    print('[56d] [53a] figure is from a different fish; rerun [53a].')\n"
        '    left = None\n'
        'if FIG_56_FISH_ID_LOCAL not in (None, FISH_ID):\n'
        "    print('[56d] [56] figure is from a different fish; rerun [56].')\n"
        '    right = None\n'
        '\n'
        'if left is None or right is None:\n'
        '    missing = []\n'
        '    if left is None:\n'
        "        missing.append('[53a]')\n"
        '    if right is None:\n'
        "        missing.append('[56]')\n"
        '    print(f"[56d] Missing rendered figure(s): {\', \'.join(missing)}. Run those cells first.")\n'
        'else:\n'
        '    left_ar = float(left.shape[1]) / float(left.shape[0])\n'
        '    right_ar = float(right.shape[1]) / float(right.shape[0])\n'
        '    fig_w = max(14.0, 7.0 * (left_ar + right_ar))\n'
        '    fig_h = max(6.0, min(12.0, fig_w * 0.45))\n'
        '\n'
        '    fig, axes = plt.subplots(1, 2, figsize=(fig_w, fig_h))\n'
        '    axes[0].imshow(left)\n'
        "    axes[0].axis('off')\n"
        "    axes[0].set_title('Registration and segmentation evidence supports matching')\n"
        '\n'
        '    axes[1].imshow(right)\n'
        "    axes[1].axis('off')\n"
        "    axes[1].set_title('Gene-linked responses show stimulus-locked structure')\n"
        '\n'
        "    fig.suptitle('Geometric alignment and stimulus responses tell a consistent story', fontsize=14)\n"
        '    plt.tight_layout(rect=[0, 0, 1, 0.96])\n'
        '\n'
        '    FIG_53A_56_COMBINED = fig\n'
        '    try:\n'
        '        fig.canvas.draw()\n'
        '        FIG_53A_56_COMBINED_RGBA = np.asarray(fig.canvas.buffer_rgba()).copy()\n'
        '    except Exception:\n'
        '        FIG_53A_56_COMBINED_RGBA = None\n'
        '\n'
        "    out_dir = OUT_QA if 'OUT_QA' in locals() else OUT_REG if 'OUT_REG' in locals() else OUTDIR\n"
        '    if out_dir is not None:\n'
        '        out_dir = Path(out_dir)\n'
        '        out_dir.mkdir(parents=True, exist_ok=True)\n'
        "        out_path = out_dir / 'composite_53a_56.png'\n"
        "        fig.savefig(out_path, dpi=200, bbox_inches='tight')\n"
        '        FIG_53A_56_COMBINED_PATH = str(out_path)\n'
        "        print(f'[56d] Saved composite to {out_path}')\n"
        '\n'
        '    plt.show()\n'}

__all__ = ["MIGRATED_CELL_SOURCE_BY_TAG"]
