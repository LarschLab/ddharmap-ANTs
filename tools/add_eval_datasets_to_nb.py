import json
from pathlib import Path


CFG_CELL = """
# --- Evaluation datasets (pre-warped confocal in 2P space) ---
# Set these to your pre-transformed confocal label stacks (already in 2P grid).
EVAL_DATASETS = {
    # Example placeholders — update to your files
    'ants':     {'name': 'ANTs',     'conf_labels_2p_path': '/path/to/confocal_labels_ANTs_in_2P.tif'},
    'bigwarp':  {'name': 'BigWarp',  'conf_labels_2p_path': '/path/to/confocal_labels_BigWarp_in_2P.tif'},
    'baseline': {'name': 'Baseline', 'conf_labels_2p_path': '/path/to/confocal_labels_baseline_in_2P.tif'},
}
""".strip("\n")


FREEZE_CELL = """
# --- Freeze evaluation pairs (final 1–1 good) ---
assert 'final_pairs' in globals(), 'Run the QC cell first to create final_pairs.'
# Only the label mapping is needed to evaluate across datasets
eval_pairs = final_pairs[['conf_label','twoP_label']].copy()
print(f'Frozen evaluation pairs: {len(eval_pairs)}')
""".strip("\n")


PRECOMP_CELL = """
# --- Precompute centroids per dataset (confocal in 2P space) ---
import numpy as np
import pandas as pd
from pathlib import Path
import tifffile as tiff

# Helper: generic label loader
def _load_labels_any(path: str):
    p = str(path)
    if p.endswith(('.tif', '.tiff')):
        return tiff.imread(p)
    if p.endswith('.npy'):
        return np.load(p)
    if p.endswith('.npz'):
        obj = np.load(p, allow_pickle=True)
        for k in ('masks','labels','arr_0'):
            if k in obj: return obj[k]
    raise RuntimeError(f'Unsupported label format: {p}')

# Build 2P coordinate LUT once
assert 'df_2p' in globals() and 'P_2p_um' in globals(), 'Need 2P centroids and coords.'
_twoP_lut = dict(zip(df_2p['label'].to_numpy(), P_2p_um))

# Cache per dataset
EVAL_CACHE = {}
for key, meta in EVAL_DATASETS.items():
    path = meta.get('conf_labels_2p_path')
    if not path or not Path(path).exists():
        print(f"[WARN] Skipping '{key}' — missing file: {path}")
        continue
    arr = _load_labels_any(path)
    df_conf_ds = compute_centroids(arr)
    P_conf_ds  = idx_to_um(df_conf_ds, VOX_2P)  # already in 2P grid
    conf_lut   = dict(zip(df_conf_ds['label'].to_numpy(), P_conf_ds))
    EVAL_CACHE[key] = {
        'name': meta.get('name', key),
        'conf_arr': arr,
        'df_conf': df_conf_ds,
        'P_conf_um': P_conf_ds,
        'conf_lut': conf_lut,
    }

print('Eval datasets cached:', [f"{k}({v['name']})" for k,v in EVAL_CACHE.items()])
""".strip("\n")


COMPARE_CELL = """
# --- Comparative summary across datasets (frozen eval_pairs) ---
import numpy as np, pandas as pd

def _eval_distances_for_pairs(conf_lut: dict, twoP_lut: dict, pairs_df: pd.DataFrame, drop_missing=True):
    ds = []
    miss = 0
    for _, r in pairs_df.iterrows():
        a = conf_lut.get(int(r['conf_label']))
        b = twoP_lut.get(int(r['twoP_label']))
        if a is None or b is None:
            miss += 1
            if not drop_missing:
                ds.append(np.nan)
            continue
        dz, dy, dx = a[0]-b[0], a[1]-b[1], a[2]-b[2]
        ds.append(float(np.sqrt(dz*dz + dy*dy + dx*dx)))
    arr = np.asarray(ds, dtype=float)
    if drop_missing:
        arr = arr[np.isfinite(arr)]
    return arr, miss

assert 'EVAL_CACHE' in globals() and 'eval_pairs' in globals(), 'Run precompute + freeze cells first.'
_twoP_lut = dict(zip(df_2p['label'].to_numpy(), P_2p_um))

rows = []
_all_tidy = []
for key, payload in EVAL_CACHE.items():
    dists, dropped = _eval_distances_for_pairs(payload['conf_lut'], _twoP_lut, eval_pairs, drop_missing=True)
    stats = {
        'dataset': payload['name'],
        'n': int(dists.size),
        'median': float(np.median(dists)) if dists.size else 0.0,
        'p90': float(np.percentile(dists, 90)) if dists.size else 0.0,
        'mean': float(np.mean(dists)) if dists.size else 0.0,
        'max': float(np.max(dists)) if dists.size else 0.0,
        'dropped_pairs': int(dropped),
    }
    rows.append(stats)
    _all_tidy += [{'dataset': payload['name'], 'distance_um': float(x)} for x in dists]

compare_df = pd.DataFrame(rows).sort_values('dataset')
print('Comparative summary (final 1–1 good, frozen pairs):')
try:
    display(compare_df)
except Exception:
    print(compare_df.to_string(index=False))

dist_by_dataset = pd.DataFrame(_all_tidy)
""".strip("\n")


VIEWER_CELL = """
# --- 3D viewer: switch datasets (confocal) + fixed 2P background ---
import numpy as np
import plotly.graph_objects as go
from skimage.measure import marching_cubes

assert 'EVAL_CACHE' in globals() and len(EVAL_CACHE) > 0, 'Run precompute datasets cell.'

# Colors
CONF_COLOR = '#f254a6'  # confocal (magenta)
TWO_P_COLOR = '#33a6ff' # 2P (azure)
PAIR_LINE_COLOR = 'red'
PAIR_LINE_WIDTH = 5
OPACITY = 0.10
STEP_SIZE = 1

# Voxel spacing (µm)
dz = float(VOX_2P.get('dz', 1.0))
dy = float(VOX_2P.get('dy', 1.0))
dx = float(VOX_2P.get('dx', 1.0))

# Build 2P background mesh once
mask_2p = (masks_2p > 0)
if np.any(mask_2p):
    vT, fT, _, _ = marching_cubes(mask_2p.astype(np.uint8), level=0.5, spacing=(dz, dy, dx), step_size=STEP_SIZE)
    iT, jT, kT = fT.T.astype(np.int32, copy=False)
    zT, yT, xT = vT[:, 0], vT[:, 1], vT[:, 2]
    t_twoP = go.Mesh3d(x=xT, y=yT, z=zT, i=iT, j=jT, k=kT, name='2P mask', color=TWO_P_COLOR, opacity=OPACITY, lighting=dict(ambient=0.5))
else:
    t_twoP = go.Mesh3d(x=[], y=[], z=[], i=[], j=[], k=[], name='2P mask', color=TWO_P_COLOR, opacity=OPACITY)

# 2P centroids
Z2, Y2, X2 = P_2p_um[:,0], P_2p_um[:,1], P_2p_um[:,2]
pts_twoP = go.Scatter3d(x=X2, y=Y2, z=Z2, mode='markers', name='2P centroids', marker=dict(size=2, color=TWO_P_COLOR), showlegend=True)

# Build per-dataset traces (conf mesh, conf centroids, pair lines)
all_traces = [t_twoP, pts_twoP]
trace_groups = {}

# LUTs
_twoP_lut = dict(zip(df_2p['label'].to_numpy(), P_2p_um))

for key, payload in EVAL_CACHE.items():
    name = payload['name']
    arr = payload['conf_arr']
    conf_mask = (arr > 0)
    if np.any(conf_mask):
        vC, fC, _, _ = marching_cubes(conf_mask.astype(np.uint8), level=0.5, spacing=(dz, dy, dx), step_size=STEP_SIZE)
        iC, jC, kC = fC.T.astype(np.int32, copy=False)
        zC, yC, xC = vC[:, 0], vC[:, 1], vC[:, 2]
        t_conf = go.Mesh3d(x=xC, y=yC, z=zC, i=iC, j=jC, k=kC, name=f'{name} conf mask', color=CONF_COLOR, opacity=OPACITY, lighting=dict(ambient=0.5))
    else:
        t_conf = go.Mesh3d(x=[], y=[], z=[], i=[], j=[], k=[], name=f'{name} conf mask', color=CONF_COLOR, opacity=OPACITY)

    P_conf = payload['P_conf_um']
    Zc, Yc, Xc = P_conf[:,0], P_conf[:,1], P_conf[:,2]
    pts_conf = go.Scatter3d(x=Xc, y=Yc, z=Zc, mode='markers', name=f'{name} conf centroids', marker=dict(size=2, color=CONF_COLOR), showlegend=True)

    # Pair lines using frozen eval_pairs
    xl, yl, zl = [], [], []
    conf_lut = payload['conf_lut']
    for _, r in eval_pairs.iterrows():
        a = conf_lut.get(int(r['conf_label']))
        b = _twoP_lut.get(int(r['twoP_label']))
        if a is None or b is None:
            continue
        x0, y0, z0 = a[2], a[1], a[0]
        x1, y1, z1 = b[2], b[1], b[0]
        xl += [x0, x1, None]; yl += [y0, y1, None]; zl += [z0, z1, None]
    pair_lines = go.Scatter3d(x=xl, y=yl, z=zl, mode='lines', name=f'{name} pairs', line=dict(color=PAIR_LINE_COLOR, width=PAIR_LINE_WIDTH), hoverinfo='skip', showlegend=True)

    idx0 = len(all_traces)
    all_traces += [t_conf, pts_conf, pair_lines]
    trace_groups[key] = [idx0, idx0+1, idx0+2]

# Initial visibility: 2P background + first dataset
visible = [True, True] + [False]*(len(all_traces)-2)
first_key = next(iter(trace_groups))
for i in trace_groups[first_key]:
    visible[i] = True

fig = go.Figure(data=all_traces)

# Dropdown to switch dataset
buttons = []
for key, idxs in trace_groups.items():
    vis = [True, True] + [False]*(len(all_traces)-2)
    for i in idxs:
        vis[i] = True
    buttons.append(dict(label=EVAL_CACHE[key]['name'], method='update', args=[{'visible': vis}, {'title': f"3D view — dataset: {EVAL_CACHE[key]['name']}"}]))

fig.update_layout(
    width=1400, height=900,
    title=f"3D view — dataset: {EVAL_CACHE[first_key]['name']}",
    scene=dict(xaxis_title='x (µm)', yaxis_title='y (µm)', zaxis_title='z (µm)', aspectmode='data'),
    legend=dict(x=0.02, y=0.95, font=dict(size=10)),
    updatemenus=[dict(type='dropdown', direction='down', x=1.05, y=0.95, showactive=True, xanchor='left', yanchor='top', buttons=buttons)],
)

fig.show()
""".strip("\n")


VIOLIN_CELL = """
# --- Violin plot: distance per dataset (final 1–1 good, frozen pairs) ---
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

assert 'dist_by_dataset' in globals() and not dist_by_dataset.empty, 'Run comparative summary cell first.'

cats = list(dist_by_dataset['dataset'].unique())
series = [dist_by_dataset.loc[dist_by_dataset['dataset']==c, 'distance_um'].to_numpy(float) for c in cats]

plt.figure(figsize=(7,4))
parts = plt.violinplot(series, showmeans=False, showmedians=False, showextrema=False)
for pc in parts['bodies']:
    pc.set_facecolor('#87bfff'); pc.set_edgecolor('black'); pc.set_alpha(0.7)

# Add medians and n annotations
for i, d in enumerate(series, start=1):
    if d.size:
        med = float(np.median(d))
        plt.scatter([i], [med], color='crimson', zorder=3, s=25)
        plt.text(i, med, f" n={d.size}", va='bottom', ha='center', fontsize=8)

plt.xticks(range(1, len(cats)+1), cats)
plt.ylabel('distance (µm)')
plt.title('Final 1–1 good pairs — distance per dataset')
plt.tight_layout(); plt.show()
""".strip("\n")


def insert_cells(nb_path: Path) -> None:
    d = json.loads(nb_path.read_text())
    cells = d.get('cells', [])

    # 1) Insert dataset config near the main config block
    cfg_idx = None
    for i,c in enumerate(cells):
        if c.get('cell_type')=='code' and 'MATCH_METHOD' in ''.join(c.get('source', [])):
            cfg_idx = i
            break
    if cfg_idx is None:
        cfg_idx = 0
    cells.insert(cfg_idx+1, {
        'cell_type':'code','execution_count':None,'metadata':{'tags':['eval','config']},'outputs':[],
        'source':[line+'\n' for line in CFG_CELL.splitlines()]
    })

    # 2) Insert freeze + precompute + compare right after QC (look for 'final_pairs (head):')
    qc_idx = None
    for i,c in enumerate(cells):
        if c.get('cell_type')=='code' and 'final_pairs (head):' in ''.join(c.get('source', [])):
            qc_idx = i
            break
    insert_at = (qc_idx+1) if qc_idx is not None else len(cells)
    to_add = [
        {'src': FREEZE_CELL, 'tags': ['eval','freeze']},
        {'src': PRECOMP_CELL, 'tags': ['eval','precompute']},
        {'src': COMPARE_CELL, 'tags': ['eval','summary']},
        {'src': VIEWER_CELL, 'tags': ['eval','viewer']},
        {'src': VIOLIN_CELL, 'tags': ['eval','violin']},
    ]
    for item in to_add:
        cells.insert(insert_at, {
            'cell_type':'code','execution_count':None,'metadata':{'tags':item['tags']},'outputs':[],
            'source':[line+'\n' for line in item['src'].splitlines()]
        })
        insert_at += 1

    d['cells'] = cells
    nb_path.write_text(json.dumps(d, indent=1, ensure_ascii=False))


def main():
    nb = Path('antsQC.ipynb')
    if not nb.exists():
        raise SystemExit('antsQC.ipynb not found')
    insert_cells(nb)
    print('Inserted evaluation dataset cells into antsQC.ipynb')


if __name__ == '__main__':
    main()

