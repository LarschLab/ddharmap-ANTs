# Auto-extracted notebook helpers.
# Source notebooks: antsQC.ipynb, 2PF_to_HCR.ipynb
from pathlib import Path

__all__ = [
    'infer_anat_labels_path',
    'infer_hcr_label_paths',
    'owner_root',
    '_find_transform',
    'fs_info',
]

def infer_anat_labels_path(fish_dir, fish_id):
    cand_dirs = [
        fish_dir / '03_analysis' / 'structural' / 'cp_masks',
        fish_dir / '03_analysis' / 'functional' / 'masks',
    ]
    patterns = [
        f"*{fish_id}*anatomy*cp_masks*.tif",
        f"*{fish_id}*cp_masks*.tif",
        '*anatomy*cp_masks*.tif',
        '*cp_masks*.tif',
    ]
    for d in cand_dirs:
        if not d.exists():
            continue
        for pat in patterns:
            hits = sorted(d.glob(pat))
            if hits:
                return hits[0]
    return None

def infer_hcr_label_paths(fish_dir, fish_id):
    d = fish_dir / '03_analysis' / 'confocal' / 'cp_masks'
    if not d.exists():
        return []
    patterns = [
        f"{fish_id}_round*_cp_masks*.tif",
        f"{fish_id}_round*.tif",
        "*round*_cp_masks*.tif",
    ]
    hits = []
    for pat in patterns:
        hits.extend(sorted(d.glob(pat)))
    uniq = []
    seen = set()
    for h in hits:
        if h not in seen:
            uniq.append(h)
            seen.add(h)
    return uniq

def owner_root(nas_root, owner):
    base = Path(nas_root) / owner
    mic = base / "Microscopy"
    return mic if mic.exists() else base

def _find_transform(trans_dir: Path, round_idx: int, target_tag: str, kind: str):
    if trans_dir is None or not trans_dir.exists():
        return None
    hits = []
    for p in sorted(trans_dir.glob("*")):
        name = p.name.lower()
        if f"round{round_idx}" not in name:
            continue
        if target_tag not in name:
            continue
        if "inverse" in name:
            continue
        if kind == "warp" and name.endswith("warp.nii.gz"):
            hits.append(p)
        if kind == "affine" and name.endswith(".mat"):
            hits.append(p)
    return hits[0] if hits else None

def fs_info(path: str) -> dict:
    import os, time
    exists = os.path.exists(path) if path else False
    size_b = os.path.getsize(path) if exists else None
    mtime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(os.path.getmtime(path))) if exists else None
    return {'exists': exists, 'size_bytes': size_b, 'size_MB': (size_b/1e6 if size_b else None), 'modified': mtime}
