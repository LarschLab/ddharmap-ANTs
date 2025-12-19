# Auto-extracted notebook helpers.
# Source notebooks: antsQC.ipynb, 2PF_to_HCR.ipynb
import numpy as np
import pandas as pd
from scipy.spatial import cKDTree
from scipy.optimize import linear_sum_assignment
from skimage.measure import regionprops_table
from skimage import measure

__all__ = [
    '_to_um',
    '_res_to_um_per_px',
    '_ensure_uint_labels',
    '_regionprops_centroids_2d',
    'diameters_um_from_array',
    'compute_centroids',
    'idx_to_um',
    'nearest_neighbor_match',
    'hungarian_match',
    'compute_label_overlap',
    'summarize_distances',
    'label_volumes',
    'map_labels_to_values',
]

def _to_um(val, unit):
    try:
        v = float(val)
    except Exception:
        return None
    if unit is None:
        return None
    u = str(unit).lower()
    if u in ('µm', 'um', 'micron', 'micrometer', 'micrometre'):
        return v
    if u in ('nm', 'nanometer', 'nanometre'):
        return v / 1000.0
    if u in ('mm', 'millimeter', 'millimetre'):
        return v * 1000.0
    if u in ('cm', 'centimeter', 'centimetre'):
        return v * 10000.0
    if u in ('in', 'inch', 'inches'):
        return v * 25400.0
    return None

def _res_to_um_per_px(res_tag, unit_tag):
    try:
        num, den = getattr(res_tag, 'value', (None, None))
        if num is None or den is None:
            v = float(getattr(res_tag, 'value', None))
            ppu = v
        else:
            ppu = float(num) / float(den)
    except Exception:
        return None
    unit_val = getattr(unit_tag, 'value', unit_tag)
    try:
        u = str(unit_val).upper()
    except Exception:
        u = 'NONE'
    if u == '2' or 'INCH' in u:
        return 25400.0 / ppu
    if u == '3' or 'CENTIMETER' in u or 'CM' in u:
        return 10000.0 / ppu
    return None

def _ensure_uint_labels(arr):
    arr = np.asarray(arr)
    if not np.issubdtype(arr.dtype, np.integer):
        arr = arr.astype(np.int64)
    return arr

def _regionprops_centroids_2d(label_img):
    tbl = measure.regionprops_table(label_img, properties=['label', 'centroid'])
    df = pd.DataFrame(tbl).rename(columns={'centroid-0': 'cy', 'centroid-1': 'cx'})
    df = df[df['label'] != 0].reset_index(drop=True)
    return df

def diameters_um_from_array(arr, vox, axis_order=('Z','Y','X')):
    """Compute per-label diameters along Z/Y/X in µm from a 3D label array.
    Expects vox like {'Z': dz, 'Y': dy, 'X': dx}.
    """
    from skimage.measure import regionprops_table
    arr = np.asarray(arr)
    if arr.ndim != 3:
        raise ValueError('diameters_um_from_array expects a 3D label array (Z,Y,X)')
    props = regionprops_table(arr, properties=('label','bbox'))
    df = pd.DataFrame(props)
    if df.empty:
        return pd.DataFrame(columns=['label','z_um','y_um','x_um'])
    df = df.rename(columns={
        'bbox-0':'zmin','bbox-1':'ymin','bbox-2':'xmin',
        'bbox-3':'zmax','bbox-4':'ymax','bbox-5':'xmax'
    })
    dz = float(vox.get('Z', 1.0)); dy = float(vox.get('Y', 1.0)); dx = float(vox.get('X', 1.0))
    df['z_um'] = (df['zmax'] - df['zmin']) * dz
    df['y_um'] = (df['ymax'] - df['ymin']) * dy
    df['x_um'] = (df['xmax'] - df['xmin']) * dx
    df = df[['label','z_um','y_um','x_um']].copy()
    df['label'] = df['label'].astype(int)
    return df

def compute_centroids(mask: np.ndarray) -> pd.DataFrame:
    props = regionprops_table(mask, properties=('label','centroid'))
    df = pd.DataFrame(props)
    # regionprops_table returns centroid-0 (z), centroid-1 (y), centroid-2 (x)
    df = df.rename(columns={'centroid-0':'z','centroid-1':'y','centroid-2':'x'})
    df = df[df['label'] != 0].reset_index(drop=True)
    return df

def idx_to_um(df: pd.DataFrame, vox: dict) -> np.ndarray:
    return np.column_stack([df['z'].to_numpy()*vox['dz'],
                           df['y'].to_numpy()*vox['dy'],
                           df['x'].to_numpy()*vox['dx']])

def nearest_neighbor_match(P_src_um: np.ndarray, P_dst_um: np.ndarray):
    tree = cKDTree(P_dst_um)
    dists, nn = tree.query(P_src_um, k=1)
    return dists, nn

def hungarian_match(P_src_um: np.ndarray, P_dst_um: np.ndarray, max_cost=np.inf):
    # Compute cost matrix lazily in blocks if needed; for moderate sizes do dense
    from scipy.spatial.distance import cdist
    C = cdist(P_src_um, P_dst_um)
    if np.isfinite(max_cost):
        C[C > max_cost] = max_cost
    row_ind, col_ind = linear_sum_assignment(C)
    dists = C[row_ind, col_ind]
    return dists, col_ind, row_ind

def compute_label_overlap(conf_labels_2p: np.ndarray, twop_labels: np.ndarray, min_overlap_voxels=1) -> pd.DataFrame:
    assert conf_labels_2p.shape == twop_labels.shape, 'Label volumes must share shape'
    a = conf_labels_2p.ravel()
    b = twop_labels.ravel()
    # Exclude background early
    m = (a != 0) & (b != 0)
    if not m.any():
        return pd.DataFrame(columns=['conf_label','twoP_label','overlap_voxels'], dtype=int)
    a = a[m].astype(np.int64, copy=False)
    b = b[m].astype(np.int64, copy=False)
    # Combine pairs into a single 64-bit key (safe for uint32 labels)
    key = (a << 32) | b
    uniq, counts = np.unique(key, return_counts=True)
    conf = (uniq >> 32).astype(np.int64)
    twop = (uniq & ((1<<32)-1)).astype(np.int64)
    df = pd.DataFrame({'conf_label': conf, 'twoP_label': twop, 'overlap_voxels': counts.astype(int)})
    if min_overlap_voxels > 1:
        df = df[df['overlap_voxels'] >= int(min_overlap_voxels)].reset_index(drop=True)
    return df

def summarize_distances(dists: np.ndarray, valid_mask: np.ndarray) -> dict:
    dists = np.asarray(dists)
    valid_mask = np.asarray(valid_mask, dtype=bool)
    if dists.size == 0:
        return {
            'n': 0, 'mean': 0.0, 'median': 0.0, 'p90': 0.0, 'max': 0.0,
            'within_gate': 0, 'within_gate_frac': 0.0
        }
    return {
        'n': int(dists.size),
        'mean': float(np.mean(dists)),
        'median': float(np.median(dists)),
        'p90': float(np.percentile(dists, 90)),
        'max': float(np.max(dists)),
        'within_gate': int(valid_mask.sum()),
        'within_gate_frac': float(valid_mask.mean())
    }

def label_volumes(arr):
    labels, counts = np.unique(arr, return_counts=True)
    s = pd.Series(counts, index=labels)
    return s.drop(index=0, errors='ignore').astype(int)

def map_labels_to_values(label_vol, mapping, default_value, dtype):
    max_label = int(label_vol.max())
    lut = np.full(max_label + 1, default_value, dtype=dtype)
    for k, v in mapping.items():
        k = int(k)
        if 0 < k <= max_label:
            lut[k] = v
    return lut[label_vol]
