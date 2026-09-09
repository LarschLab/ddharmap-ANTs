# [50l]
try:
    REQUIRE_FISH_STATE = require_fish_state
except NameError:
    REQUIRE_FISH_STATE = None
if callable(REQUIRE_FISH_STATE) and not REQUIRE_FISH_STATE():
    raise SystemExit

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.colors import to_rgb
from matplotlib.gridspec import GridSpec, GridSpecFromSubplotSpec
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
from pathlib import Path

try:
    RUN_CONFIG_LOCAL = RUN_CONFIG if isinstance(RUN_CONFIG, dict) else {}
except NameError:
    RUN_CONFIG_LOCAL = {}

RESPONSE_ACTIVE = 'Responsive neurons'
RESPONSE_LOW = 'Low activity'
RESPONSE_UNAVAILABLE = 'Response unavailable'
RESPONSE_LOW_PLOT = 'low activity'

BPI_BOUT = 'bout-responsive'
BPI_CONT = 'continuous-responsive'
BPI_BOTH = 'both-responsive'
BPI_LOW = 'low activity'
BPI_UNAVAILABLE = 'response unavailable'

DEFAULT_GENE_ORDER = ['sst1.1', 'sst1.2', 'npy', 'tac3b', 'pth2', 'cfos', 'cort']
DEFAULT_GENE_COLORS = {
    'sst1.1': '#d62728',
    'sst1.2': '#d61ad2',
    'npy': '#1f9d55',
    'tac3b': '#ffd400',
    'pth2': '#00bcd4',
    'cfos': '#ff7f0e',
    'cort': '#8c564b',
}
GENE_ORDER = list(RUN_CONFIG_LOCAL.get('GENE_ORDER', DEFAULT_GENE_ORDER))
GENE_COLORS = dict(DEFAULT_GENE_COLORS)
if isinstance(RUN_CONFIG_LOCAL.get('GENE_COLORS', None), dict):
    GENE_COLORS.update(RUN_CONFIG_LOCAL['GENE_COLORS'])

COMPOSITE_50L_PATH = OUT_QA / 'compound_50j_56i_unified.png'
SAVE_COMPOSITE_50L = True
COMPOSITE_50L_DPI = 300

COMPOSITE_50L_FIG_WIDTH_IN = float(RUN_CONFIG_LOCAL.get('COMPOSITE_50L_FIG_WIDTH_IN', 17.2))
COMPOSITE_50L_FIG_HEIGHT_IN = float(RUN_CONFIG_LOCAL.get('COMPOSITE_50L_FIG_HEIGHT_IN', 20))
COMPOSITE_50L_LEFT = float(RUN_CONFIG_LOCAL.get('COMPOSITE_50L_LEFT', 0.06))
COMPOSITE_50L_RIGHT = float(RUN_CONFIG_LOCAL.get('COMPOSITE_50L_RIGHT', 0.985))
COMPOSITE_50L_TOP = float(RUN_CONFIG_LOCAL.get('COMPOSITE_50L_TOP', 0.93))
COMPOSITE_50L_BOTTOM = float(RUN_CONFIG_LOCAL.get('COMPOSITE_50L_BOTTOM', 0.15))
COMPOSITE_50L_WSPACE = float(RUN_CONFIG_LOCAL.get('COMPOSITE_50L_WSPACE', 0.24))
COMPOSITE_50L_HSPACE = float(RUN_CONFIG_LOCAL.get('COMPOSITE_50L_HSPACE', 0.34))
COMPOSITE_50L_AUC_PAIR_WSPACE = float(RUN_CONFIG_LOCAL.get('COMPOSITE_50L_AUC_PAIR_WSPACE', 0.08))
COMPOSITE_50L_AUC_GROUP_GAP_RATIO = float(RUN_CONFIG_LOCAL.get('COMPOSITE_50L_AUC_GROUP_GAP_RATIO', 0.65))
COMPOSITE_50L_TITLE_Y = float(RUN_CONFIG_LOCAL.get('COMPOSITE_50L_TITLE_Y', 0.975))
COMPOSITE_50L_AUC_LEGEND_Y = float(RUN_CONFIG_LOCAL.get('COMPOSITE_50L_AUC_LEGEND_Y', 0.03))
COMPOSITE_50L_AUC_LEGEND_NCOL = int(RUN_CONFIG_LOCAL.get('COMPOSITE_50L_AUC_LEGEND_NCOL', 4))

COMPOSITE_50L_TITLE_FONTSIZE = float(RUN_CONFIG_LOCAL.get('COMPOSITE_50L_TITLE_FONTSIZE', 15))
COMPOSITE_50L_PANEL_TITLE_FONTSIZE = float(RUN_CONFIG_LOCAL.get('COMPOSITE_50L_PANEL_TITLE_FONTSIZE', 12))
COMPOSITE_50L_AXIS_LABEL_FONTSIZE = float(RUN_CONFIG_LOCAL.get('COMPOSITE_50L_AXIS_LABEL_FONTSIZE', 10))
COMPOSITE_50L_TICK_FONTSIZE = float(RUN_CONFIG_LOCAL.get('COMPOSITE_50L_TICK_FONTSIZE', 9))
COMPOSITE_50L_LEGEND_FONTSIZE = float(RUN_CONFIG_LOCAL.get('COMPOSITE_50L_LEGEND_FONTSIZE', 9))
COMPOSITE_50L_LEGEND_TITLE_FONTSIZE = float(RUN_CONFIG_LOCAL.get('COMPOSITE_50L_LEGEND_TITLE_FONTSIZE', 10))
COMPOSITE_50L_ANNOT_FONTSIZE = float(RUN_CONFIG_LOCAL.get('COMPOSITE_50L_ANNOT_FONTSIZE', 8.5))
COMPOSITE_50L_DONUT_INNER_LABEL_FONTSIZE = float(RUN_CONFIG_LOCAL.get('COMPOSITE_50L_DONUT_INNER_LABEL_FONTSIZE', 10.5))
COMPOSITE_50L_DONUT_OUTER_LABEL_FONTSIZE = float(RUN_CONFIG_LOCAL.get('COMPOSITE_50L_DONUT_OUTER_LABEL_FONTSIZE', 9.5))
COMPOSITE_50L_DONUT_CENTER_FONTSIZE = float(RUN_CONFIG_LOCAL.get('COMPOSITE_50L_DONUT_CENTER_FONTSIZE', 10))

AUC_PAIR_MODE_OFFSET = 0.22
AUC_POINT_JITTER = 0.055
AUC_POINT_SIZE_ALL = 5.0
AUC_POINT_SIZE_GENE = 18.0
AUC_BOX_WIDTH = 0.34
AUC_COUNT_BAR_WIDTH = 0.34
AUC_CONT_LIGHTEN = 0.55
AUC_PAIR_LINE_WIDTH_ALL = float(RUN_CONFIG_LOCAL.get('AUC_56I_PAIR_LINE_WIDTH_ALL', 1.0))
AUC_PAIR_LINE_WIDTH_GENE = float(RUN_CONFIG_LOCAL.get('AUC_56I_PAIR_LINE_WIDTH_GENE', 1.0))
AUC_PAIR_LINE_ALPHA_BOUT = float(RUN_CONFIG_LOCAL.get('AUC_56I_PAIR_LINE_ALPHA_BOUT', 0.3))
AUC_PAIR_LINE_ALPHA_CONT = float(RUN_CONFIG_LOCAL.get('AUC_56I_PAIR_LINE_ALPHA_CONT', 0.4))
AUC_PAIR_LINE_ALPHA_BOTH = float(RUN_CONFIG_LOCAL.get('AUC_56I_PAIR_LINE_ALPHA_BOTH', 0.4))
AUC_PAIR_LINE_ALPHA_OTHER = float(RUN_CONFIG_LOCAL.get('AUC_56I_PAIR_LINE_ALPHA_OTHER', 0.07))
AUC_Y_MIN = float(RUN_CONFIG_LOCAL.get('AUC_56I_Y_MIN', 0.0))
AUC_Y_MAX = float(RUN_CONFIG_LOCAL.get('AUC_56I_Y_MAX', 1.0))
AUC_ALL_Y_MIN = RUN_CONFIG_LOCAL.get('COMPOSITE_50L_AUC_ALL_Y_MIN', 0.0)
AUC_ALL_Y_MAX = RUN_CONFIG_LOCAL.get('COMPOSITE_50L_AUC_ALL_Y_MAX', None)
AUC_GENE_Y_MIN = RUN_CONFIG_LOCAL.get('COMPOSITE_50L_AUC_GENE_Y_MIN', 0.0)
AUC_GENE_Y_MAX = RUN_CONFIG_LOCAL.get('COMPOSITE_50L_AUC_GENE_Y_MAX', None)

ACTIVITY_RING_WIDTH = 0.28
BPI_RING_WIDTH = 0.10
RING_GAP = 0.02
OUTER_LABEL_PAD = 0.10
OUTER_COUNT_MIN_PCT = 2.5
OUTER_LABEL_MIN_Y_GAP = 0.20
ACTIVITY_LABEL_MIN_PCT = 6.0

response_colors = {
    RESPONSE_ACTIVE: '#1b9e77',
    RESPONSE_LOW: '#8d8d8d',
    RESPONSE_UNAVAILABLE: '#d9d9d9',
}
bpi_colors = {
    BPI_BOUT: '#2c7fb8',
    BPI_CONT: '#d95f0e',
    BPI_BOTH: '#c7b37a',
    BPI_LOW: '#9e9e9e',
    BPI_UNAVAILABLE: '#ececec',
}
bpi_short = {
    BPI_BOUT: 'Bout-responsive',
    BPI_CONT: 'Cont.-responsive',
    BPI_BOTH: 'Both-responsive',
    BPI_LOW: 'Low activity',
    BPI_UNAVAILABLE: 'Unavailable',
}

count_strip_color_map = {
    'bout': {
        'responsive': '#4a4a4a',
        'low': '#b7b7b7',
        'other': '#ececec',
        'edge': '#d7d7d7',
    },
    'continuous': {
        'responsive': '#7b7b7b',
        'low': '#d2d2d2',
        'other': '#f7f7f7',
        'edge': '#e3e3e3',
    },
}


def _blend_color_local(color, blend_frac=0.55, blend_target='#ffffff'):
    try:
        src = np.asarray(to_rgb(color), dtype=float)
        dst = np.asarray(to_rgb(blend_target), dtype=float)
    except Exception:
        return color
    blend_frac = float(np.clip(blend_frac, 0.0, 1.0))
    return tuple((1.0 - blend_frac) * src + blend_frac * dst)


def _to_bool_series_local(series):
    if series is None:
        return pd.Series(dtype=bool)
    if pd.api.types.is_bool_dtype(series):
        return series.fillna(False).astype(bool)
    return series.astype(str).str.strip().str.lower().isin({'1', 'true', 't', 'yes', 'y'})


def _wedge_midpoint(wedge, radius):
    theta = np.deg2rad((float(wedge.theta1) + float(wedge.theta2)) / 2.0)
    return theta, float(radius) * np.cos(theta), float(radius) * np.sin(theta)


def _tangent_rotation(theta_rad):
    theta_deg = ((float(np.rad2deg(theta_rad)) + 180.0) % 360.0) - 180.0
    rot = theta_deg - 90.0
    if rot < -90.0:
        rot += 180.0
    elif rot > 90.0:
        rot -= 180.0
    return rot


def _contrast_text_color(color):
    try:
        r, g, b = to_rgb(color)
    except Exception:
        return 'black'
    luminance = 0.2126 * r + 0.7152 * g + 0.0722 * b
    return 'white' if luminance < 0.52 else 'black'


def _optional_float_local(val):
    if val is None:
        return None
    try:
        if isinstance(val, str) and val.strip().lower() in {'', 'none', 'auto'}:
            return None
        return float(val)
    except Exception:
        return None


try:
    ROI_DETAIL_CSV = Path(FUNC_ACTIVITY_IDENTITY_CSV)
except NameError:
    ROI_DETAIL_CSV = OUT_REG / 'functional_roi_activity_identity.csv'
AUC_POINTS_CSV = OUT_REG / 'motion_auc_plot_points.csv'
AUC_COUNTS_CSV = OUT_REG / 'motion_auc_plot_counts.csv'

missing_paths = []
if not ROI_DETAIL_CSV.exists():
    missing_paths.append(f'master ROI table: {ROI_DETAIL_CSV} (run [50i]/[50ia])')
if not AUC_POINTS_CSV.exists():
    missing_paths.append(f'motion AUC plot points: {AUC_POINTS_CSV} (run [56i])')
if not AUC_COUNTS_CSV.exists():
    missing_paths.append(f'motion AUC plot counts: {AUC_COUNTS_CSV} (run [56i])')

if missing_paths:
    print('[50l] Missing prerequisite table(s).')
    for item in missing_paths:
        print(f'  - {item}')
    raise SystemExit

detail_df = pd.read_csv(ROI_DETAIL_CSV)
points_df = pd.read_csv(AUC_POINTS_CSV)
counts_df = pd.read_csv(AUC_COUNTS_CSV)

if detail_df.empty:
    print('[50l] Master ROI table is empty; nothing to plot.')
    raise SystemExit
if points_df.empty:
    print('[50l] motion_auc_plot_points.csv is empty; rerun [56i].')
    raise SystemExit
if counts_df.empty:
    print('[50l] motion_auc_plot_counts.csv is empty; rerun [56i].')
    raise SystemExit

required_detail_cols = {'response_summary_class', 'bpi_category'}
required_points_cols = {
    'group',
    'laterality',
    'stim_mode',
    'auc_dff',
    'response_class',
    'response_is_active',
    'bpi_category',
    'point_label_id',
}
required_counts_cols = {
    'group',
    'laterality',
    'stim_mode',
    'n_total',
    'frac_responsive_used',
    'frac_low_used',
    'frac_other',
}

missing_detail_cols = sorted(required_detail_cols - set(detail_df.columns))
missing_points_cols = sorted(required_points_cols - set(points_df.columns))
missing_counts_cols = sorted(required_counts_cols - set(counts_df.columns))
if missing_detail_cols:
    print(f'[50l] Master ROI table missing columns {missing_detail_cols}; rerun [50ia].')
    raise SystemExit
if missing_points_cols:
    print(f'[50l] motion_auc_plot_points.csv missing columns {missing_points_cols}; rerun [56i].')
    raise SystemExit
if missing_counts_cols:
    print(f'[50l] motion_auc_plot_counts.csv missing columns {missing_counts_cols}; rerun [56i].')
    raise SystemExit

detail_df['response_summary_class'] = detail_df['response_summary_class'].astype(str)
detail_df['bpi_category'] = detail_df['bpi_category'].astype(str)
points_df['group'] = points_df['group'].astype(str)
points_df['laterality'] = points_df['laterality'].astype(str)
points_df['stim_mode'] = points_df['stim_mode'].astype(str)
points_df['response_class'] = points_df['response_class'].astype(str)
points_df['bpi_category'] = points_df['bpi_category'].fillna(BPI_UNAVAILABLE).astype(str)
points_df['response_is_active'] = _to_bool_series_local(points_df['response_is_active'])
points_df['auc_dff'] = pd.to_numeric(points_df['auc_dff'], errors='coerce')
points_df['point_label_id'] = points_df['point_label_id'].astype(str)

counts_df['group'] = counts_df['group'].astype(str)
counts_df['laterality'] = counts_df['laterality'].astype(str)
counts_df['stim_mode'] = counts_df['stim_mode'].astype(str)
for col in ['n_total', 'frac_responsive_used', 'frac_low_used', 'frac_other']:
    counts_df[col] = pd.to_numeric(counts_df[col], errors='coerce')

response_order = [RESPONSE_ACTIVE, RESPONSE_LOW, RESPONSE_UNAVAILABLE]
bpi_order = [BPI_BOUT, BPI_CONT, BPI_BOTH, BPI_LOW, BPI_UNAVAILABLE]
laterality_order = ['ipsi', 'contra']
mode_order = ['bout', 'continuous']
mode_offsets = {'bout': -AUC_PAIR_MODE_OFFSET, 'continuous': AUC_PAIR_MODE_OFFSET}

genes_present = [g for g in points_df['group'].dropna().astype(str).unique().tolist() if g != 'All neurons']
ordered_genes = [g for g in GENE_ORDER if g in genes_present]
ordered_genes.extend([g for g in genes_present if g not in ordered_genes])
group_order = ['All neurons'] + ordered_genes
positions = np.arange(len(group_order), dtype=float)

group_color_map = {'All neurons': '#4c4c4c'}
for gene in ordered_genes:
    group_color_map[gene] = GENE_COLORS.get(gene, '#666666')
mode_color_map = {
    group: {
        'bout': group_color_map[group],
        'continuous': _blend_color_local(group_color_map[group], blend_frac=AUC_CONT_LIGHTEN),
    }
    for group in group_order
}

response_counts = (
    detail_df.groupby('response_summary_class', as_index=False)
    .size()
    .rename(columns={'size': 'n_rois'})
)
response_counts['response_order'] = response_counts['response_summary_class'].map(
    {k: i for i, k in enumerate(response_order)}
).fillna(10**6)
response_counts = response_counts.sort_values('response_order').reset_index(drop=True)

bpi_counts = (
    detail_df.groupby(['response_summary_class', 'bpi_category'], as_index=False)
    .size()
    .rename(columns={'size': 'n_rois'})
)
bpi_counts['response_order'] = bpi_counts['response_summary_class'].map(
    {k: i for i, k in enumerate(response_order)}
).fillna(10**6)
bpi_counts['bpi_order'] = bpi_counts['bpi_category'].map(
    {k: i for i, k in enumerate(bpi_order)}
).fillna(10**6)
bpi_counts = bpi_counts.sort_values(['response_order', 'bpi_order']).reset_index(drop=True)

response_sizes = response_counts['n_rois'].astype(float).tolist()
response_labels = response_counts['response_summary_class'].astype(str).tolist()
response_cols = [response_colors.get(lbl, '#cccccc') for lbl in response_labels]

bpi_sizes = bpi_counts['n_rois'].astype(float).tolist()
bpi_labels = bpi_counts['bpi_category'].astype(str).tolist()
bpi_cols = [bpi_colors.get(lbl, '#cccccc') for lbl in bpi_labels]

def _plot_auc_block(ax, strip_ax, source_points_df, source_counts_df, groups, positions, laterality, panel_title, y_limits, show_ylabel=False, show_count_ylabel=False, hide_y_ticklabels=False):
    sub = source_points_df[source_points_df['laterality'] == laterality].copy()
    box_data = []
    box_positions = []
    box_colors = []
    for group_idx, group in enumerate(groups):
        group_sub = sub[sub['group'] == group].copy()
        for stim_mode in mode_order:
            vals = group_sub.loc[group_sub['stim_mode'] == stim_mode, 'auc_dff'].dropna().to_numpy(dtype=float)
            box_data.append(vals)
            box_positions.append(positions[group_idx] + mode_offsets[stim_mode])
            box_colors.append(mode_color_map[group][stim_mode])
    bp = ax.boxplot(
        box_data,
        positions=box_positions,
        widths=AUC_BOX_WIDTH,
        patch_artist=True,
        showfliers=False,
        medianprops={'color': '#1a1a1a', 'linewidth': 1.15},
        whiskerprops={'color': '#707070', 'linewidth': 0.9},
        capprops={'color': '#707070', 'linewidth': 0.9},
        boxprops={'linewidth': 0.9, 'edgecolor': '#707070'},
    )
    for patch, box_color in zip(bp['boxes'], box_colors):
        patch.set_facecolor(box_color)
        patch.set_alpha(0.24)
        patch.set_edgecolor(box_color)

    block_rng = np.random.default_rng(56)
    for group_idx, group in enumerate(groups):
        gsub = sub[sub['group'] == group].copy()
        if gsub.empty:
            continue
        point_ids = gsub['point_label_id'].dropna().astype(str).drop_duplicates().tolist()
        point_jitter = {
            point_id: float(block_rng.uniform(-AUC_POINT_JITTER, AUC_POINT_JITTER))
            for point_id in point_ids
        }
        gsub['pair_jitter'] = gsub['point_label_id'].map(point_jitter).fillna(0.0).astype(float)
        gsub['x_pos'] = (
            positions[group_idx]
            + gsub['stim_mode'].map(mode_offsets).astype(float)
            + gsub['pair_jitter']
        )

        line_width = AUC_PAIR_LINE_WIDTH_ALL if group == 'All neurons' else AUC_PAIR_LINE_WIDTH_GENE
        pair_meta = (
            gsub[['point_label_id', 'bpi_category', 'response_is_active']]
            .drop_duplicates(subset=['point_label_id'])
            .set_index('point_label_id')
        )
        pair_lookup = gsub.pivot_table(
            index='point_label_id',
            columns='stim_mode',
            values=['x_pos', 'auc_dff'],
            aggfunc='first',
        ).copy()
        pair_lookup[('meta', 'bpi_category')] = pair_lookup.index.map(pair_meta['bpi_category'])
        pair_lookup[('meta', 'response_is_active')] = (
            pd.Series(pair_lookup.index, index=pair_lookup.index)
            .map(pair_meta['response_is_active'])
            .fillna(False)
            .astype(bool)
        )
        if {('auc_dff', 'bout'), ('auc_dff', 'continuous')}.issubset(set(pair_lookup.columns)):
            paired = pair_lookup.dropna(
                subset=[('auc_dff', 'bout'), ('auc_dff', 'continuous')],
                how='any',
            )
            for _, prow in paired.iterrows():
                if group == 'All neurons' and bool(prow.get(('meta', 'response_is_active'), False)):
                    bpi_cat = str(prow.get(('meta', 'bpi_category'), BPI_UNAVAILABLE))
                    line_color = bpi_colors.get(bpi_cat, '#666666')
                    if bpi_cat == BPI_BOUT:
                        line_alpha = AUC_PAIR_LINE_ALPHA_BOUT
                    elif bpi_cat == BPI_CONT:
                        line_alpha = AUC_PAIR_LINE_ALPHA_CONT
                    elif bpi_cat == BPI_BOTH:
                        line_alpha = AUC_PAIR_LINE_ALPHA_BOTH
                    else:
                        line_alpha = AUC_PAIR_LINE_ALPHA_OTHER
                else:
                    line_color = group_color_map.get(group, '#666666')
                    line_alpha = AUC_PAIR_LINE_ALPHA_OTHER
                ax.plot(
                    [prow[('x_pos', 'bout')], prow[('x_pos', 'continuous')]],
                    [prow[('auc_dff', 'bout')], prow[('auc_dff', 'continuous')]],
                    color=line_color,
                    linewidth=line_width,
                    alpha=line_alpha,
                    zorder=1,
                )

        size = AUC_POINT_SIZE_ALL if group == 'All neurons' else AUC_POINT_SIZE_GENE
        alpha = 0.12 if group == 'All neurons' else 0.82
        for stim_mode in mode_order:
            msub = gsub[gsub['stim_mode'] == stim_mode].copy()
            if msub.empty:
                continue
            mode_color = mode_color_map[group][stim_mode]
            responsive_part = msub[msub['response_is_active']].copy()
            low_part = msub[(~msub['response_is_active']) & (msub['response_class'] == RESPONSE_LOW_PLOT)].copy()
            unavailable_part = msub[(~msub['response_is_active']) & (msub['response_class'] != RESPONSE_LOW_PLOT)].copy()
            if not responsive_part.empty:
                ax.scatter(
                    responsive_part['x_pos'],
                    responsive_part['auc_dff'],
                    s=size,
                    facecolors=mode_color,
                    edgecolors='none',
                    alpha=alpha,
                    zorder=3,
                )
            if not low_part.empty:
                ax.scatter(
                    low_part['x_pos'],
                    low_part['auc_dff'],
                    s=size,
                    facecolors='none',
                    edgecolors=mode_color,
                    linewidths=0.7,
                    alpha=min(1.0, alpha + 0.06),
                    zorder=3,
                )
            if not unavailable_part.empty:
                ax.scatter(
                    unavailable_part['x_pos'],
                    unavailable_part['auc_dff'],
                    s=max(10.0, size * 0.7),
                    marker='x',
                    color=mode_color,
                    linewidths=0.7,
                    alpha=min(1.0, alpha + 0.08),
                    zorder=3,
                )

    ax.axhline(0.0, color='#d0d0d0', linewidth=0.9, zorder=0)
    ax.set_xlim(float(positions.min()) - 0.65, float(positions.max()) + 0.35)
    ax.set_ylim(*y_limits)
    ax.set_xticks(positions)
    ax.set_xticklabels([])
    ax.tick_params(axis='x', which='both', bottom=False, labelbottom=False, length=0)
    ax.tick_params(axis='y', labelsize=COMPOSITE_50L_TICK_FONTSIZE)
    if hide_y_ticklabels:
        ax.tick_params(axis='y', labelleft=False)
    if show_ylabel:
        ax.set_ylabel('Mean motion-window AUC (dF/F·s)', fontsize=COMPOSITE_50L_AXIS_LABEL_FONTSIZE)
    ax.set_title(panel_title, fontsize=COMPOSITE_50L_PANEL_TITLE_FONTSIZE)

    csub = source_counts_df[source_counts_df['laterality'] == laterality].copy()
    csub = csub.set_index(['group', 'stim_mode']).reindex(
        pd.MultiIndex.from_product([groups, mode_order], names=['group', 'stim_mode'])
    ).reset_index()
    for stim_mode in mode_order:
        mcounts = csub[csub['stim_mode'] == stim_mode].copy()
        xvals = positions + mode_offsets[stim_mode]
        palette = count_strip_color_map[stim_mode]
        strip_ax.bar(
            xvals,
            mcounts['frac_responsive_used'].fillna(0.0).to_numpy(dtype=float),
            width=AUC_COUNT_BAR_WIDTH,
            color=palette['responsive'],
            edgecolor='none',
        )
        strip_ax.bar(
            xvals,
            mcounts['frac_low_used'].fillna(0.0).to_numpy(dtype=float),
            width=AUC_COUNT_BAR_WIDTH,
            bottom=mcounts['frac_responsive_used'].fillna(0.0).to_numpy(dtype=float),
            color=palette['low'],
            edgecolor='none',
        )
        strip_ax.bar(
            xvals,
            mcounts['frac_other'].fillna(0.0).to_numpy(dtype=float),
            width=AUC_COUNT_BAR_WIDTH,
            bottom=(mcounts['frac_responsive_used'].fillna(0.0) + mcounts['frac_low_used'].fillna(0.0)).to_numpy(dtype=float),
            color=palette['other'],
            edgecolor=palette['edge'],
            linewidth=0.35,
        )
    paired_totals = csub.pivot(index='group', columns='stim_mode', values='n_total').reindex(groups)
    for group_idx, group in enumerate(groups):
        bout_total = paired_totals.loc[group, 'bout'] if 'bout' in paired_totals.columns else np.nan
        cont_total = paired_totals.loc[group, 'continuous'] if 'continuous' in paired_totals.columns else np.nan
        if pd.isna(bout_total) and pd.isna(cont_total):
            continue
        if pd.isna(bout_total):
            label = f'n={int(cont_total)}'
        elif pd.isna(cont_total):
            label = f'n={int(bout_total)}'
        elif int(bout_total) == int(cont_total):
            label = f'n={int(bout_total)}'
        else:
            label = f'nB/C={int(bout_total)}/{int(cont_total)}'
        strip_ax.text(positions[group_idx], 1.03, label, ha='center', va='bottom', fontsize=COMPOSITE_50L_ANNOT_FONTSIZE)
    strip_ax.set_ylim(0.0, 1.10)
    strip_ax.set_yticks([])
    strip_ax.set_xticks(positions)
    strip_ax.set_xticklabels(groups, rotation=32, ha='right', fontsize=COMPOSITE_50L_TICK_FONTSIZE)
    if show_count_ylabel:
        strip_ax.set_ylabel('Count', fontsize=COMPOSITE_50L_AXIS_LABEL_FONTSIZE)
    strip_ax.spines['top'].set_visible(False)
    strip_ax.spines['right'].set_visible(False)
    strip_ax.spines['left'].set_visible(False)


def _resolve_y_limits_local(source_points_df, y_min_override, y_max_override):
    y_min = _optional_float_local(y_min_override)
    y_max = _optional_float_local(y_max_override)
    if y_min is None or not np.isfinite(y_min):
        y_min = 0.0
    vals = pd.to_numeric(source_points_df.get('auc_dff', pd.Series(dtype=float)), errors='coerce').to_numpy(dtype=float)
    vals = vals[np.isfinite(vals)]
    if y_max is None or not np.isfinite(y_max):
        if vals.size:
            y_max = float(np.nanmax(vals)) + 0.2
            if not np.isfinite(y_max) or y_max <= y_min:
                y_max = y_min + 1.0
        else:
            y_max = y_min + 1.0
    if y_max <= y_min:
        y_max = y_min + 1.0
    return float(y_min), float(y_max)

fig = plt.figure(
    figsize=(COMPOSITE_50L_FIG_WIDTH_IN, COMPOSITE_50L_FIG_HEIGHT_IN),
    dpi=COMPOSITE_50L_DPI,
)
gs = GridSpec(
    8,
    8,
    figure=fig,
    wspace=COMPOSITE_50L_WSPACE,
    hspace=COMPOSITE_50L_HSPACE,
)

ax_donut = fig.add_subplot(gs[0:4, 2:6], aspect='equal')
lower_gs = GridSpecFromSubplotSpec(
    4,
    5,
    subplot_spec=gs[4:8, :],
    width_ratios=[1.0, 1.0, COMPOSITE_50L_AUC_GROUP_GAP_RATIO, 3.0, 3.0],
    hspace=0.08,
    wspace=COMPOSITE_50L_AUC_PAIR_WSPACE,
)
ax_all_ipsi = fig.add_subplot(lower_gs[0:3, 0:1])
ax_all_contra = fig.add_subplot(lower_gs[0:3, 1:2], sharey=ax_all_ipsi)
ax_gene_ipsi = fig.add_subplot(lower_gs[0:3, 3:4])
ax_gene_contra = fig.add_subplot(lower_gs[0:3, 4:5], sharey=ax_gene_ipsi)
ax_all_strip_ipsi = fig.add_subplot(lower_gs[3:4, 0:1], sharex=ax_all_ipsi)
ax_all_strip_contra = fig.add_subplot(lower_gs[3:4, 1:2], sharex=ax_all_contra)
ax_gene_strip_ipsi = fig.add_subplot(lower_gs[3:4, 3:4], sharex=ax_gene_ipsi)
ax_gene_strip_contra = fig.add_subplot(lower_gs[3:4, 4:5], sharex=ax_gene_contra)

outer_radius = 1.08
inner_outer_radius = outer_radius - BPI_RING_WIDTH - RING_GAP
centre_radius = inner_outer_radius - ACTIVITY_RING_WIDTH
response_mid_radius = inner_outer_radius - (ACTIVITY_RING_WIDTH / 2.0)
bpi_mid_radius = outer_radius - (BPI_RING_WIDTH / 2.0)
outer_label_radius = outer_radius + OUTER_LABEL_PAD

outer_wedges, _ = ax_donut.pie(
    bpi_sizes,
    radius=outer_radius,
    labels=None,
    colors=bpi_cols,
    startangle=90,
    counterclock=False,
    wedgeprops=dict(width=BPI_RING_WIDTH, edgecolor='white', linewidth=1.0),
)
inner_wedges, _ = ax_donut.pie(
    response_sizes,
    radius=inner_outer_radius,
    labels=None,
    colors=response_cols,
    startangle=90,
    counterclock=False,
    wedgeprops=dict(width=ACTIVITY_RING_WIDTH, edgecolor='white', linewidth=1.0),
)

total_n = int(len(detail_df))
total_response = float(sum(response_sizes)) if response_sizes else 0.0
for wedge, label, val, color in zip(inner_wedges, response_labels, response_sizes, response_cols):
    if total_response <= 0 or float(val) <= 0:
        continue
    pct = 100.0 * float(val) / total_response
    if pct < ACTIVITY_LABEL_MIN_PCT:
        continue
    theta, x, y = _wedge_midpoint(wedge, response_mid_radius)
    ax_donut.text(
        x,
        y,
        f'{label}\n{int(round(val))}',
        ha='center',
        va='center',
        rotation=_tangent_rotation(theta),
        rotation_mode='anchor',
        fontsize=COMPOSITE_50L_DONUT_INNER_LABEL_FONTSIZE,
        color=_contrast_text_color(color),
    )

total_bpi = float(sum(bpi_sizes)) if bpi_sizes else 0.0
outer_label_items = []
for wedge, label, val, color in zip(outer_wedges, bpi_labels, bpi_sizes, bpi_cols):
    if total_bpi <= 0 or float(val) <= 0:
        continue
    theta, x, y = _wedge_midpoint(wedge, bpi_mid_radius)
    pct = 100.0 * float(val) / total_bpi
    if pct >= OUTER_COUNT_MIN_PCT:
        ax_donut.text(
            x,
            y,
            f'{int(round(val))}',
            ha='center',
            va='center',
            rotation=_tangent_rotation(theta),
            rotation_mode='anchor',
            fontsize=COMPOSITE_50L_ANNOT_FONTSIZE,
            color=_contrast_text_color(color),
        )
    if label == BPI_UNAVAILABLE:
        continue
    side = 1.0 if np.cos(theta) >= 0 else -1.0
    outer_label_items.append({
        'label': f"{bpi_short.get(label, label)} (n={int(round(val))})",
        'side': side,
        'anchor_x': (outer_radius + 0.01) * np.cos(theta),
        'anchor_y': (outer_radius + 0.01) * np.sin(theta),
        'text_x': side * outer_label_radius,
        'text_y': (outer_label_radius - 0.10) * np.sin(theta),
        'ha': 'left' if side > 0 else 'right',
    })

y_limit = outer_label_radius - 0.06
for side in (-1.0, 1.0):
    side_items = [item for item in outer_label_items if item['side'] == side]
    side_items.sort(key=lambda d: d['text_y'])
    prev_y = -np.inf
    for item in side_items:
        if item['text_y'] - prev_y < OUTER_LABEL_MIN_Y_GAP:
            item['text_y'] = prev_y + OUTER_LABEL_MIN_Y_GAP
        prev_y = item['text_y']
    prev_y = np.inf
    for item in reversed(side_items):
        if prev_y - item['text_y'] < OUTER_LABEL_MIN_Y_GAP:
            item['text_y'] = prev_y - OUTER_LABEL_MIN_Y_GAP
        prev_y = item['text_y']
    for item in side_items:
        item['text_y'] = float(np.clip(item['text_y'], -y_limit, y_limit))

for item in outer_label_items:
    rad = 0.16 if item['side'] > 0 else -0.16
    ax_donut.annotate(
        item['label'],
        xy=(item['anchor_x'], item['anchor_y']),
        xytext=(item['text_x'], item['text_y']),
        ha=item['ha'],
        va='center',
        fontsize=COMPOSITE_50L_DONUT_OUTER_LABEL_FONTSIZE,
        color='black',
        arrowprops=dict(
            arrowstyle='-',
            color='black',
            linewidth=0.8,
            shrinkA=0,
            shrinkB=0,
            connectionstyle=f'arc3,rad={rad}',
        ),
    )

centre_circle = plt.Circle((0, 0), centre_radius, fc='white', ec='white')
ax_donut.add_artist(centre_circle)
zero_band = float(pd.to_numeric(detail_df.get('bpi_zero_band', pd.Series([0.10])), errors='coerce').dropna().iloc[0])
auc_thr = float(pd.to_numeric(detail_df.get('response_auc_threshold', pd.Series([0.05])), errors='coerce').dropna().iloc[0])
null_q = float(pd.to_numeric(detail_df.get('response_null_quantile', pd.Series([0.99])), errors='coerce').dropna().iloc[0])
ax_donut.text(0, 0.10, f'n = {total_n}', ha='center', va='center', fontsize=COMPOSITE_50L_PANEL_TITLE_FONTSIZE, fontweight='bold')
ax_donut.text(
    0,
    -0.14,
    f'inner=response\nouter=stim bias\nAUC ≥ {auc_thr:.3f}\n> Q{100.0 * null_q:.0f} baseline\n|BPI| ≤ {zero_band:.2f} -> both',
    ha='center',
    va='center',
    fontsize=COMPOSITE_50L_DONUT_CENTER_FONTSIZE,
)
ax_donut.set_title('[50j] Whole functional-plane response summary', fontsize=COMPOSITE_50L_PANEL_TITLE_FONTSIZE, pad=10)
ax_donut.set_xlim(-1.66, 1.72)
ax_donut.set_ylim(-1.38, 1.38)

all_points_df = points_df[points_df['group'] == 'All neurons'].copy()
gene_points_df = points_df[points_df['group'] != 'All neurons'].copy()
all_counts_df = counts_df[counts_df['group'] == 'All neurons'].copy()
gene_counts_df = counts_df[counts_df['group'] != 'All neurons'].copy()
all_groups = ['All neurons']
all_positions = np.array([0.0], dtype=float)
gene_groups = ordered_genes
gene_positions = np.arange(len(gene_groups), dtype=float)
all_y_limits = _resolve_y_limits_local(all_points_df, AUC_ALL_Y_MIN, AUC_ALL_Y_MAX)
gene_y_limits = _resolve_y_limits_local(gene_points_df, AUC_GENE_Y_MIN, AUC_GENE_Y_MAX)

_plot_auc_block(
    ax_all_ipsi,
    ax_all_strip_ipsi,
    all_points_df,
    all_counts_df,
    all_groups,
    all_positions,
    'ipsi',
    'All neurons\nIpsi',
    all_y_limits,
    show_ylabel=True,
    show_count_ylabel=True,
    hide_y_ticklabels=False,
)
_plot_auc_block(
    ax_all_contra,
    ax_all_strip_contra,
    all_points_df,
    all_counts_df,
    all_groups,
    all_positions,
    'contra',
    'All neurons\nContra',
    all_y_limits,
    show_ylabel=False,
    show_count_ylabel=False,
    hide_y_ticklabels=True,
)
_plot_auc_block(
    ax_gene_ipsi,
    ax_gene_strip_ipsi,
    gene_points_df,
    gene_counts_df,
    gene_groups,
    gene_positions,
    'ipsi',
    'Genes\nIpsi',
    gene_y_limits,
    show_ylabel=False,
    show_count_ylabel=False,
    hide_y_ticklabels=False,
)
_plot_auc_block(
    ax_gene_contra,
    ax_gene_strip_contra,
    gene_points_df,
    gene_counts_df,
    gene_groups,
    gene_positions,
    'contra',
    'Genes\nContra',
    gene_y_limits,
    show_ylabel=False,
    show_count_ylabel=False,
    hide_y_ticklabels=True,
)

fig.suptitle('Functional response overview: population response classes and motion-window AUC', y=COMPOSITE_50L_TITLE_Y, fontsize=COMPOSITE_50L_TITLE_FONTSIZE)

legend_56i = fig.legend(
    handles=[
        Patch(facecolor='#555555', edgecolor='none', label='Bout box / responsive point shade'),
        Patch(facecolor=_blend_color_local('#555555', blend_frac=AUC_CONT_LIGHTEN), edgecolor='none', label='Continuous box / responsive point shade'),
        Line2D([0], [0], color=bpi_colors[BPI_BOUT], linewidth=2.0, alpha=max(0.25, AUC_PAIR_LINE_ALPHA_BOUT), label='All-neuron pair line: bout-responsive'),
        Line2D([0], [0], color=bpi_colors[BPI_CONT], linewidth=2.0, alpha=max(0.25, AUC_PAIR_LINE_ALPHA_CONT), label='All-neuron pair line: cont.-responsive'),
        Line2D([0], [0], color=bpi_colors[BPI_BOTH], linewidth=2.0, alpha=max(0.25, AUC_PAIR_LINE_ALPHA_BOTH), label='All-neuron pair line: both-responsive'),
        Line2D([0], [0], marker='o', color='none', markerfacecolor='#555555', markeredgecolor='none', markersize=6, label='Responsive ROI point'),
        Line2D([0], [0], marker='o', color='none', markerfacecolor='none', markeredgecolor='#555555', markersize=6, label='Low-response ROI point'),
        Line2D([0], [0], marker='x', color='#555555', linestyle='none', markersize=6, label='Response-unavailable ROI point'),
        Patch(facecolor='#4a4a4a', edgecolor='none', label='Count strip: responsive used'),
        Patch(facecolor='#b7b7b7', edgecolor='none', label='Count strip: low-response used'),
        Patch(facecolor='#ececec', edgecolor='#d7d7d7', label='Count strip: excluded / no usable ROI'),
    ],
    loc='lower center',
    bbox_to_anchor=(0.5, COMPOSITE_50L_AUC_LEGEND_Y),
    frameon=False,
    fontsize=COMPOSITE_50L_LEGEND_FONTSIZE,
    title='[56i] Motion-window AUC view',
    title_fontsize=COMPOSITE_50L_LEGEND_TITLE_FONTSIZE,
    ncol=COMPOSITE_50L_AUC_LEGEND_NCOL,
    columnspacing=1.2,
    handletextpad=0.55,
)

fig.subplots_adjust(
    left=COMPOSITE_50L_LEFT,
    right=COMPOSITE_50L_RIGHT,
    top=COMPOSITE_50L_TOP,
    bottom=COMPOSITE_50L_BOTTOM,
)

FIG_50L_COMPOSITE = fig
try:
    fig.canvas.draw()
    FIG_50L_COMPOSITE_RGBA = np.asarray(fig.canvas.buffer_rgba()).copy()
except Exception:
    FIG_50L_COMPOSITE_RGBA = None

if SAVE_COMPOSITE_50L:
    COMPOSITE_50L_PATH.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(COMPOSITE_50L_PATH, dpi=COMPOSITE_50L_DPI, bbox_inches='tight')
    FIG_50L_COMPOSITE_PATH = str(COMPOSITE_50L_PATH)
    print(f'[50l] saved composite to {COMPOSITE_50L_PATH}')

plt.show()
