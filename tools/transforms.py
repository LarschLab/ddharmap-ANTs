# Auto-extracted notebook helpers.
# Source notebooks: antsQC.ipynb, 2PF_to_HCR.ipynb
from pathlib import Path
import json
import numpy as np
from skimage import feature, registration, img_as_float32, measure
from skimage.transform import warp, SimilarityTransform, AffineTransform
from .image_ops import norm01

__all__ = [
    'estimate_inplane_transform',
    'apply_transform_2d',
    'resample_labels_nn',
    'apply_anat_to_hcr_warp_2d',
    '_apply_tform_points_xy',
    '_ants_clone_geometry',
    'warp_cache_candidates',
    'load_cached_warp',
    'warp_metadata_path',
    'read_warp_metadata',
    'write_warp_metadata',
]

def estimate_inplane_transform(mov, ref, method='similarity'):
    """Estimate 2D transform from moving image (mov) to reference (ref).
    Tries ORB+RANSAC; falls back to phase cross-correlation (shift only)."""
    m = norm01(mov); r = norm01(ref)
    # ORB keypoints
    try:
        detector = feature.ORB(n_keypoints=2000, fast_threshold=0.05)
        detector.detect_and_extract(img_as_float32(m))
        kp1 = detector.keypoints; d1 = detector.descriptors
        detector.detect_and_extract(img_as_float32(r))
        kp2 = detector.keypoints; d2 = detector.descriptors
        if len(kp1) >= 10 and len(kp2) >= 10 and d1 is not None and d2 is not None:
            matches12 = feature.match_descriptors(d1, d2, cross_check=True, max_ratio=0.8)
            src = kp1[matches12[:, 0]][:, ::-1]  # (x,y)
            dst = kp2[matches12[:, 1]][:, ::-1]
            if method == 'similarity':
                model, inliers = measure.ransac((src, dst), SimilarityTransform,
                                                min_samples=3, residual_threshold=2.0, max_trials=2000)
            else:
                model, inliers = measure.ransac((src, dst), AffineTransform,
                                                min_samples=3, residual_threshold=2.0, max_trials=2000)
            if model is not None:
                return model
    except Exception as e:
        pass
    # Fallback: phase correlation for shift
    shift, _, _ = registration.phase_cross_correlation(r, m, upsample_factor=10)
    tform = SimilarityTransform(translation=(shift[1], shift[0]))
    return tform

def apply_transform_2d(img, tform, output_shape=None, order=1, preserve_range=True):
    if output_shape is None:
        output_shape = img.shape
    warped = warp(img, inverse_map=tform.inverse, output_shape=output_shape, order=order,
                  preserve_range=preserve_range, mode='constant', cval=0.0, clip=True)
    return warped

def resample_labels_nn(img, tform, output_shape=None):
    # nearest-neighbor for label images
    return apply_transform_2d(img, tform, output_shape=output_shape, order=0, preserve_range=True)

def apply_anat_to_hcr_warp_2d(slice_img, z_index, warp3d_func):
    """Hook to apply a 3D warp (anatomy→HCR) to a 2D slice.
    `warp3d_func` should accept (z,y,x) indices or coordinates and return warped image in HCR coords.
    For now this is a placeholder you can implement with your BigWarp/ANTs output.
    """
    return warp3d_func(slice_img, z_index)

def _apply_tform_points_xy(tform, x, y):
    pts = np.stack([x, y], axis=1)
    pts_t = tform(pts)
    return pts_t[:,0], pts_t[:,1]

def _ants_clone_geometry(dst_img, like_img):
    dst_img.set_spacing(like_img.spacing)
    dst_img.set_origin(like_img.origin)
    dst_img.set_direction(like_img.direction)
    return dst_img

def warp_cache_candidates(save_basename: str):
    from pathlib import Path
    # Prefer *.npy (faster load) over TIFF if both exist
    return [Path(f'{save_basename}_labels_int32.npy'), Path(f'{save_basename}_labels_uint16.tif')]

def load_cached_warp(save_basename: str):
    import numpy as np
    import tifffile as tiff
    for c in warp_cache_candidates(save_basename):
        if c.exists():
            if c.suffix == '.npy':
                return np.load(c, mmap_mode='r'), c
            if c.suffix == '.tif':
                return tiff.imread(str(c)), c
    return None, None

def warp_metadata_path(save_basename: str) -> Path:
    return Path(f'{save_basename}_warp_meta.json')

def read_warp_metadata(save_basename: str):
    p = warp_metadata_path(save_basename)
    if not p.exists():
        return None
    return json.loads(p.read_text())

def write_warp_metadata(save_basename: str, metadata: dict) -> None:
    p = warp_metadata_path(save_basename)
    p.write_text(json.dumps(metadata, indent=2))
