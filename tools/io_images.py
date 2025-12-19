# Auto-extracted notebook helpers.
# Source notebooks: antsQC.ipynb, 2PF_to_HCR.ipynb
from pathlib import Path
import numpy as np
import tifffile as tiff
from tifffile import imread, TiffFile
from .labels import _to_um, _res_to_um_per_px

__all__ = [
    'imread_any',
    'infer_voxels_tiff',
    '_vox_complete',
    '_detect_spacing_from_nrrd',
    '_merge_spacing',
    'load_labels_any',
]

def imread_any(path):
    p = Path(path)
    ext = p.suffix.lower()
    if ext == '.nrrd':
        try:
            import nrrd
            data, _ = nrrd.read(str(p))
            return np.asarray(data)
        except Exception:
            try:
                import SimpleITK as sitk
                data = sitk.GetArrayFromImage(sitk.ReadImage(str(p)))
                return np.asarray(data)
            except Exception as e_sitk:
                raise ImportError('Reading .nrrd requires pynrrd or SimpleITK') from e_sitk
    return imread(path)

def infer_voxels_tiff(path):
    vox = {'Z': None, 'Y': None, 'X': None}
    try:
        with TiffFile(str(path)) as tf:
            # OME-XML
            omexml = None
            try:
                omexml = tf.ome_metadata
            except Exception:
                omexml = None
            if omexml:
                try:
                    import xml.etree.ElementTree as ET
                    root = ET.fromstring(omexml)
                    # find any Pixels element regardless of namespace
                    pix = root.find('.//{*}Pixels')
                    if pix is not None:
                        px = pix.attrib.get('PhysicalSizeX'); pxu = pix.attrib.get('PhysicalSizeXUnit')
                        py = pix.attrib.get('PhysicalSizeY'); pyu = pix.attrib.get('PhysicalSizeYUnit')
                        pz = pix.attrib.get('PhysicalSizeZ'); pzu = pix.attrib.get('PhysicalSizeZUnit')
                        if py is not None:
                            v = _to_um(py, pyu or 'um')
                            if v: vox['Y'] = v
                        if px is not None:
                            v = _to_um(px, pxu or 'um')
                            if v: vox['X'] = v
                        if pz is not None:
                            v = _to_um(pz, pzu or 'um')
                            if v: vox['Z'] = v
                except Exception:
                    pass
            # ImageJ metadata (Z spacing)
            try:
                ij = tf.imagej_metadata or {}
                if isinstance(ij, dict):
                    zsp = ij.get('spacing', None)
                    unit = ij.get('unit', 'um')
                    if vox['Z'] is None and zsp is not None:
                        vz = _to_um(zsp, unit)
                        if vz: vox['Z'] = vz
            except Exception:
                pass
            # Resolution tags → X/Y
            try:
                page0 = tf.pages[0]
                xr = page0.tags.get('XResolution', None)
                yr = page0.tags.get('YResolution', None)
                ru = page0.tags.get('ResolutionUnit', None)
                if vox['X'] is None and xr is not None and ru is not None:
                    vx = _res_to_um_per_px(xr, ru)
                    if vx: vox['X'] = vx
                if vox['Y'] is None and yr is not None and ru is not None:
                    vy = _res_to_um_per_px(yr, ru)
                    if vy: vox['Y'] = vy
            except Exception:
                pass
            # Parse ImageDescription for XY pixel size if still missing
            try:
                page0 = tf.pages[0]
                desc = None
                try:
                    desc = page0.description
                except Exception:
                    pass
                if desc is None:
                    try:
                        tag = page0.tags.get('ImageDescription', None)
                        desc = getattr(tag, 'value', None)
                    except Exception:
                        desc = None
                if desc is not None:
                    try:
                        text = desc.decode('utf-8', 'ignore') if isinstance(desc, (bytes, bytearray)) else str(desc)
                    except Exception:
                        text = str(desc)
                    kv = {}
                    for line in text.replace('\r', '\n').split('\n'):
                        if '=' in line:
                            k, v = line.split('=', 1)
                            kv[k.strip()] = v.strip()
                    unit = kv.get('unit', kv.get('Unit', 'um'))
                    px = kv.get('pixelWidth') or kv.get('PixelWidth') or kv.get('XPixelSize') or kv.get('micronsPerPixelX') or kv.get('MicronsPerPixelX') or kv.get('umPerPixelX') or kv.get('UmPerPixelX') or kv.get('X_UM_PER_PIXEL')
                    py = kv.get('pixelHeight') or kv.get('PixelHeight') or kv.get('YPixelSize') or kv.get('micronsPerPixelY') or kv.get('MicronsPerPixelY') or kv.get('umPerPixelY') or kv.get('UmPerPixelY') or kv.get('Y_UM_PER_PIXEL')
                    both = kv.get('PixelSizeUm') or kv.get('pixelSizeUm') or kv.get('PixelSize')
                    if both is not None:
                        try:
                            val = float(both)
                            if vox['X'] is None: vox['X'] = val
                            if vox['Y'] is None: vox['Y'] = val
                        except Exception:
                            pass
                    if vox['X'] is None and px is not None:
                        vx = _to_um(px, unit)
                        if vx: vox['X'] = vx
                    if vox['Y'] is None and py is not None:
                        vy = _to_um(py, unit)
                        if vy: vox['Y'] = vy
            except Exception:
                pass
    except Exception:
        pass
    # 5) Final fallback: try ANTs (as in antsQC) if available; works for NRRD/TIFF and reads spacing header
    try:
        import ants  # type: ignore
        img = ants.image_read(str(path))
        sp = tuple(float(s) for s in img.spacing)  # (dx,dy[,dz])
        if len(sp) >= 2:
            if vox['X'] is None: vox['X'] = sp[0]*1.0  # dx (µm)
            if vox['Y'] is None: vox['Y'] = sp[1]*1.0  # dy (µm)
        if len(sp) >= 3 and vox['Z'] is None:
            vox['Z'] = sp[2]*1.0  # dz (µm)
    except Exception:
        pass
    return vox

def _vox_complete(vox):
    try:
        return vox and all(vox.get(ax) is not None for ax in ("X","Y","Z"))
    except Exception:
        return False

def _detect_spacing_from_nrrd(path):
    try:
        import ants  # type: ignore
    except Exception:
        return None
    from pathlib import Path
    if path is None or not Path(path).exists():
        return None
    img = ants.image_read(path)
    sp = tuple(float(s) for s in img.spacing)  # (dx, dy, dz) or (dx, dy)
    if len(sp) == 3:
        return {"dx": sp[0], "dy": sp[1], "dz": sp[2]}
    if len(sp) == 2:
        return {"dx": sp[0], "dy": sp[1]}
    return None

def _merge_spacing(current: dict, detected: dict | None) -> tuple[dict, bool]:
    if detected is None:
        return current, False
    new = dict(current)
    for k in ("dz", "dy", "dx"):
        if k in detected:
            new[k] = detected[k]
    return new, True

def load_labels_any(path: str) -> np.ndarray:
    assert path is not None and Path(path).exists(), f'Label file not found: {path}'
    if path.endswith('.npy') or path.endswith('.npz'):
        obj = np.load(path, allow_pickle=True)
        if isinstance(obj, np.lib.npyio.NpzFile):
            # Try common keys
            for k in ('masks','labels','arr_0'):
                if k in obj: return np.asarray(obj[k])
            raise RuntimeError(f'Unsupported npz structure in {path}')
        else:
            arr = obj
            if isinstance(arr, np.ndarray):
                # Some Cellpose *_seg.npy are dicts; handle both
                if arr.dtype == object and arr.shape == () and isinstance(arr.item(), dict):
                    d = arr.item()
                    for k in ('masks','labels'):
                        if k in d: return np.asarray(d[k])
                    raise RuntimeError('Dict npy has no masks/labels key')
                return arr
            # Fallback if np.load returns a Python object (rare)
            try:
                d = arr.item()
                for k in ('masks','labels'):
                    if k in d: return np.asarray(d[k])
            except Exception:
                pass
            raise RuntimeError(f'Unsupported npy content in {path}')
    elif path.endswith('.tif') or path.endswith('.tiff'):
        return tiff.imread(path)
    else:
        raise RuntimeError(f'Unsupported label format: {path}')
