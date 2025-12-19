# Auto-extracted notebook helpers.
# Source notebooks: antsQC.ipynb, 2PF_to_HCR.ipynb
import numpy as np
from scipy import ndimage as ndi
from skimage import transform

__all__ = [
    'zproject_mean',
    'norm01',
    'local_unsharp',
    'corrcoef_img',
    'top_correlated_mean',
    'rescale_to_match_xy',
]

def zproject_mean(stack):
    return stack.mean(axis=0)

def norm01(img):
    img = img.astype(np.float32)
    m, M = np.percentile(img, (1, 99))
    if M <= m:
        M = img.max(); m = img.min()
    out = np.clip((img - m) / (M - m + 1e-6), 0, 1)
    return out

def local_unsharp(img, blur_sigma=1.0, amount=0.6):
    base = ndi.gaussian_filter(img, blur_sigma)
    return np.clip(base + amount*(img - base), 0, 1)

def corrcoef_img(a, b):
    # Pearson correlation between 2D arrays
    a = a.astype(np.float32); b = b.astype(np.float32)
    am = a.mean(); bm = b.mean()
    num = ((a - am)*(b - bm)).sum()
    den = np.sqrt(((a - am)**2).sum() * ((b - bm)**2).sum()) + 1e-8
    return float(num / den)

def top_correlated_mean(stack_t, take_k=20, pre_smooth_sigma=0.5):
    """Suite2p-like: build crisp reference by selecting top-K frames most correlated to a provisional mean."""
    T, H, W = stack_t.shape
    # Provisional mean
    m0 = stack_t.mean(axis=0)
    # Optional pre-smoothing to reduce shot noise
    if pre_smooth_sigma and pre_smooth_sigma > 0:
        m0s = ndi.gaussian_filter(m0, pre_smooth_sigma)
    else:
        m0s = m0
    # Correlate each frame with provisional mean
    corrs = np.empty(T, dtype=np.float32)
    for i in range(T):
        fi = stack_t[i]
        if pre_smooth_sigma and pre_smooth_sigma > 0:
            fi = ndi.gaussian_filter(fi, pre_smooth_sigma)
        corrs[i] = corrcoef_img(fi, m0s)
    # Take top-K
    k = min(take_k, T)
    idx = np.argsort(corrs)[-k:]
    ref = stack_t[idx].mean(axis=0)
    return ref, idx, corrs

def rescale_to_match_xy(img, vox_src, vox_dst, order=1):
    try:
        sy = float(vox_src.get('Y')) / float(vox_dst.get('Y'))
        sx = float(vox_src.get('X')) / float(vox_dst.get('X'))
    except Exception:
        return img
    if not np.isfinite(sy) or not np.isfinite(sx):
        return img
    if abs(sy - 1.0) < 1e-3 and abs(sx - 1.0) < 1e-3:
        return img
    out_shape = (max(1, int(round(img.shape[0] * sy))), max(1, int(round(img.shape[1] * sx))))
    return transform.resize(img, out_shape, order=order, preserve_range=True, anti_aliasing=True).astype(np.float32)
