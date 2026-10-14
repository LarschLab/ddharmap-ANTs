"""Bind manual ex-vivo identity calls to the single-fish pipeline's own locations.

Flow: ex-vivo masks are reviewed per marker stack (``manual_identity``); the
gate warps every ex-vivo mask into in-vivo 2P space with the accepted
ex-vivo->2P ANTs transforms, matches them to in-vivo anatomy labels with the
HCR matching code, and writes only the two ROI-centric identity tables to the
assign-hcr-identity location. Continuation reruns assign-hcr-identity on the
manual aligned bundle (``conf_label`` is the ex-vivo mask ID) and the
downstream writers, promoting products exactly like a native production run.
"""
from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
from typing import Callable

import numpy as np
import pandas as pd

from .manual_identity import CALLS, Dataset, atomic_write, positive_label_id, prepare_dataset, sha256

FISH_ID_RE = re.compile(r"^[A-Z][0-9]{3}_f[0-9]{2}$")
IDENTITY_SOURCE = "manual_exvivo"
ASSIGN_STAGE = "assign-hcr-identity"
ROI_TABLES = ("functional_roi_activity_identity.csv", "anatomy_identity_lookup.csv")
GATE_AWAITING = "awaiting_continuation"
GATE_CONTINUED = "continued"
# Production order after identity; midline materialization is identity-independent.
CONTINUATION_STAGES = (
    ("090-attach-hcr-identity", "assign-hcr-identity"),
    ("100-export-canonical-tables", "export-canonical-tables"),
    ("110-generate-qa", "make-qa-report"),
    ("130-generate-motion-auc-tables", "make-motion-auc-tables"),
    ("140-generate-final-figures", "make-figures"),
)
REPO_ROOT = Path(__file__).resolve().parents[2]


def _now():
    return datetime.now(timezone.utc)


def _stamp():
    return _now().strftime('%Y%m%dT%H%M%S%fZ')


def fish_pipeline_paths(fish_id, local_root, pipeline_root=None):
    """Native fish trees are their own pipeline root; others use the default."""
    from .native_paths import is_native_root
    from .pipeline import SingleFishPipelineConfig, resolve_pipeline_paths
    if not FISH_ID_RE.match(str(fish_id)):
        raise ValueError(f'Invalid fish ID: {fish_id}')
    fish_dir = Path(local_root) / fish_id
    if pipeline_root is None and is_native_root(fish_dir):
        pipeline_root = fish_dir
    return resolve_pipeline_paths(SingleFishPipelineConfig(fish_id=fish_id, local_root=Path(local_root),
                                                           pipeline_root=pipeline_root, dry_run=False))


def manual_identity_root(paths):
    from .pipeline import ex_vivo_structural_root
    return ex_vivo_structural_root(paths) / 'manual_identity'


def exvivo_anatomy_path(paths):
    return paths.anatomy_preproc_dir / 'ex_vivo' / f'{paths.fish_dir.name}_exvivo_anatomy_2P_GCaMP_uint8_manual_oriented.nrrd'


def exvivo_labels_path(paths):
    from .pipeline import ex_vivo_structural_root
    hits = sorted(p for p in (ex_vivo_structural_root(paths) / 'cp_masks').glob('*_cp_masks.tif') if not p.name.startswith('._'))
    if len(hits) != 1:
        raise FileNotFoundError(f'Expected one ex-vivo Cellpose mask TIFF in {ex_vivo_structural_root(paths) / "cp_masks"}; found {len(hits)}')
    return hits[0]


def invivo_anatomy_intensity_path(paths):
    return paths.anatomy_preproc_dir / f'{paths.fish_dir.name}_anatomy_2P_GCaMP.nrrd'


def product_path(paths, stage, relative):
    """Where a stage product lives for downstream consumers of this fish."""
    from .native_paths import is_native_root, native_product_path
    from .pipeline import _stage_root
    if is_native_root(paths.pipeline_root):
        return native_product_path(paths.pipeline_root, stage, relative)
    return _stage_root(paths, stage) / relative


def gene_token(gene):
    """File token understood by matching.gene_from_mask (``sst1_1`` -> ``sst1.1``)."""
    return str(gene).replace('.', '_')


def _gene_from_token(token):
    return token.replace('sst1_', 'sst1.')


def discover_marker_stacks(paths):
    """Registered HCR stacks in ex-vivo space with their exact source and transform chain."""
    from .hcr_warp import _bridge_transform_pair
    fish = paths.fish_dir.name
    reg = paths.preproc_dir.parent
    rbest_to_exvivo = _bridge_transform_pair(reg / '03_rbest-exvivo' / 'transMatrices', source_tokens=('rbest',), target_tag='to_exvivo')
    pattern = re.compile(rf'^{re.escape(fish)}_(rbest|r\d+)_(.+?)(_via_rbest)?_in_exvivo\.nrrd$')
    stacks = []
    for directory in (reg / '03_rbest-exvivo' / 'aligned', reg / '05_rn-exvivo' / 'aligned'):
        for image in sorted(directory.glob(f'{fish}_*_in_exvivo.nrrd')):
            match = pattern.match(image.name)
            if image.name.startswith('._') or not match or match.group(2).lower() == 'gcamp':
                continue
            round_id, token, via = match.group(1), match.group(2), bool(match.group(3))
            source_dir = paths.preproc_dir / ('rbest' if round_id == 'rbest' else 'rn')
            sources = sorted(source_dir.glob(f'{fish}_{round_id}_channel*_{token}.nrrd'))
            if len(sources) != 1:
                raise FileNotFoundError(f'Expected one source intensity for {image.name} in {source_dir}')
            channel = re.search(r'_channel(\d+)_', sources[0].name).group(1)
            if round_id == 'rbest':
                chain = rbest_to_exvivo
            elif via:
                to_rbest = _bridge_transform_pair(reg / '02_rn-rbest' / 'transMatrices', source_tokens=(round_id,), target_tag='to_rbest')
                chain = None if to_rbest is None or rbest_to_exvivo is None else (*rbest_to_exvivo, *to_rbest)
            else:
                chain = _bridge_transform_pair(reg / '05_rn-exvivo' / 'transMatrices', source_tokens=(round_id,), target_tag='to_exvivo')
            if not chain:
                raise FileNotFoundError(f'Missing transform chain to ex vivo for {image.name}')
            gene = _gene_from_token(token)
            stacks.append(dict(stack_id=f'{round_id}-{gene}' + ('-via-rbest' if via else ''), gene=gene, round_id=round_id,
                               source_channel=f'channel{channel}', source=sources[0], image=image,
                               transforms=tuple(Path(p) for p in chain)))
    if not stacks:
        raise FileNotFoundError(f'No registered HCR stacks in ex-vivo space for {fish}')
    return stacks


def _ants():
    os.environ.setdefault('KMP_DUPLICATE_LIB_OK', 'TRUE')
    import ants
    return ants


def _coverage_and_validation(stack, fixed_path, coverage_path):
    """Warp the source acquisition domain; re-warp the source to prove the chain."""
    ants = _ants()
    fixed = ants.image_read(str(fixed_path))
    moving = ants.image_read(str(stack['source']))
    transforms = [str(p) for p in stack['transforms']]
    domain = moving.new_image_like(np.ones(moving.shape, dtype='float32'))
    coverage = ants.apply_transforms(fixed=fixed, moving=domain, transformlist=transforms, interpolator='nearestNeighbor')
    covered = coverage.numpy() > 0.5
    coverage_img = fixed.new_image_like(covered.astype('float32')).clone('unsigned char')
    coverage_path.parent.mkdir(parents=True, exist_ok=True)
    ants.image_write(coverage_img, str(coverage_path))
    rewarped = ants.apply_transforms(fixed=fixed, moving=moving, transformlist=transforms, interpolator='linear').numpy()
    accepted = ants.image_read(str(stack['image'])).numpy()
    if rewarped.shape != accepted.shape:
        raise ValueError(f'Registered stack grid differs from ex-vivo anatomy: {stack["image"]}')
    a, b = rewarped[covered].astype(float), accepted[covered].astype(float)
    r = float(np.corrcoef(a, b)[0, 1]) if a.size > 1 and a.std() > 0 and b.std() > 0 else float('nan')
    return dict(coverage_fraction=float(covered.mean()), chain_reproduction_pearson_r=r)


def _bind_exvivo_label_grid(paths, labels_path, anatomy_path):
    """Masks inherit the Cellpose input grid; require that provenance, never infer it."""
    from .pipeline import ex_vivo_structural_root
    import tifffile
    manifest_path = ex_vivo_structural_root(paths) / 'manifests' / 'segment-ex-vivo-anatomy-cellpose_manifest.json'
    if not manifest_path.is_file():
        raise FileNotFoundError(f'Ex-vivo segmentation manifest missing: {manifest_path}')
    manifest = json.loads(manifest_path.read_text())
    sources = [r['path'] for r in manifest.get('inputs', []) if r.get('label') == 'prepared ex vivo anatomy stack']
    outputs = [r['path'] for r in manifest.get('outputs', []) if r.get('label') == 'ex vivo anatomy Cellpose masks']
    name = lambda value: re.split(r'[\\/]', str(value))[-1]
    if manifest.get('status') == 'fail' or [name(s) for s in sources] != [anatomy_path.name] or [name(s) for s in outputs] != [labels_path.name]:
        raise ValueError(f'Ex-vivo segmentation manifest does not bind {labels_path.name} to {anatomy_path.name}')
    with tifffile.TiffFile(labels_path) as tif:
        shape = list(tif.series[0].shape)
    return manifest_path, shape


def _functional_region_in_exvivo(dataset, fixed_img, allowed_z, transforms, inverse_warp):
    """Pull the in-vivo slab union back onto the original ex-vivo image grid."""
    ants = _ants()
    # ANTs image arrays are XYZ, while the app and allowed_z use ZYX.
    slabs = np.broadcast_to(allowed_z[None, None, :], fixed_img.shape).astype('float32')
    moving = fixed_img.new_image_like(slabs)
    fixed = ants.image_read(dataset.manifest['anatomy']['path'])
    mapped = ants.apply_transforms(fixed=fixed, moving=moving,
        transformlist=[str(transforms[1]), str(inverse_warp)],
        whichtoinvert=[True, False], interpolator='nearestNeighbor')
    region = np.transpose(mapped.numpy(), (2, 1, 0)) > 0.5
    if region.shape != dataset.labels.shape:
        raise ValueError('Mapped functional region differs from the ex-vivo image grid')
    return region


def apply_functional_review_scope(dataset, local_root, *, pipeline_root=None, log=print):
    """Restrict new review calls to this fish's functional best-Z slabs (±10).

    Keep the immutable annotation inventory and complete geometry unchanged.
    Cache the per-fish, per-mask scope separately so existing sessions reopen.
    """
    from .hcr_warp import _bridge_transform_pair, _warp_zyx_labels_with_ants
    paths = fish_pipeline_paths(dataset.manifest['fish_id'], local_root, pipeline_root)
    raw_refs_path = product_path(paths, 'register-functional-to-anatomy', 'plane_refs_summary.json')
    geometry_refs_path = product_path(paths, 'match-roi-to-anatomy', 'registration/plane_refs_summary_geometry.json')
    refs_path = geometry_refs_path if geometry_refs_path.is_file() else raw_refs_path
    refs = json.loads(refs_path.read_text())
    if not isinstance(refs, list) or not refs:
        raise ValueError(f'No functional plane references for {paths.fish_dir.name}: {refs_path}')
    planes = []
    for row in refs:
        index, z = row.get('index'), row.get('best_z')
        if isinstance(index, bool) or not isinstance(index, int) or index < 0:
            raise ValueError('Functional plane index must be a nonnegative global integer')
        if isinstance(z, bool) or not isinstance(z, int) or z < 0:
            raise ValueError(f'Invalid best_z for functional plane {index}')
        planes.append(dict(plane_idx=index, best_z=z))
    indices = {p['plane_idx'] for p in planes}
    if len(indices) != len(planes):
        raise ValueError('Duplicate global functional plane indices')
    # The accepted geometry table holds the fish's eligible planes (e.g. R2-only
    # L765_f05: 5-9 of 10 acquired). Without it, every Suite2p plane must be covered.
    geometry_path = product_path(paths, 'match-roi-to-anatomy', 'registration/functional_roi_anatomy_matches.csv')
    if geometry_path.is_file():
        geometry = pd.read_csv(geometry_path, usecols=['plane_idx'])
        expected = set(geometry.plane_idx.dropna().astype(int))
    else:
        expected = {int(p.name[5:]) for p in paths.functional_suite2p_dir.glob('plane*')
                    if p.is_dir() and p.name[5:].isdigit() and not p.name.startswith('._')}
    if refs_path != raw_refs_path:
        raw_refs = json.loads(raw_refs_path.read_text())
        if not isinstance(raw_refs, list) or not raw_refs or any(
                isinstance(p.get('index'), bool) or not isinstance(p.get('index'), int) for p in raw_refs):
            raise ValueError('Invalid global plane inventory in registration references')
        expected |= {p['index'] for p in raw_refs}
    if expected and indices != expected:
        raise ValueError(f'Functional plane references {sorted(indices)} differ from fish inventory {sorted(expected)}')
    transforms = _bridge_transform_pair(paths.preproc_dir.parent / '04_exvivo-2p' / 'transMatrices',
                                        source_tokens=('exvivo', 'ex_vivo', 'ex-vivo'), target_tag='to_2p')
    if not transforms:
        raise FileNotFoundError('Functional review scope requires the ex vivo->2P transform pair')
    inverse_warp = transforms[0].with_name(transforms[0].name.replace('1Warp.nii.gz', '1InverseWarp.nii.gz'))
    if not inverse_warp.is_file():
        raise FileNotFoundError(f'Mapped functional region requires the saved inverse warp: {inverse_warp}')
    fixed = invivo_anatomy_intensity_path(paths)
    inputs = list(dict.fromkeys([raw_refs_path, refs_path])) + [fixed, Path(dataset.manifest['anatomy']['path']),
              Path(dataset.manifest['labels']['path']), *map(Path, transforms), inverse_warp]
    provenance = dict(region_schema_version=1, fish_id=paths.fish_dir.name, dataset_fingerprint=dataset.fingerprint,
                      half_width_planes=10, planes=sorted(planes, key=lambda p: p['plane_idx']),
                      expected_plane_indices=sorted(expected),
                      inputs=[dict(path=str(p), sha256=sha256(p)) for p in inputs])
    cache_path = manual_identity_root(paths) / 'functional_review_scopes' / f'{dataset.manifest["dataset_id"]}.json'
    cached = _read_json(cache_path)
    region_spec = (cached or {}).get('region_in_exvivo', {})
    region_path = Path(region_spec.get('path', ''))
    region_valid = (region_path.is_file() and sha256(region_path) == region_spec.get('sha256')
                    and region_spec.get('axes') == 'ZYX' and region_spec.get('grid') == dataset.manifest['grid'])
    if cached is None or cached.get('provenance') != provenance or not region_valid:
        log(f'Computing functional review scope for {paths.fish_dir.name} (best Z ±10)')
        warped, fixed_img = _warp_zyx_labels_with_ants(labels_zyx=dataset.labels,
            moving_intensity_path=Path(dataset.manifest['anatomy']['path']),
            fixed_intensity_path=fixed, transformlist=transforms)
        z_size = warped.shape[0]
        allowed = np.zeros(z_size, dtype=bool)
        for plane in planes:
            z = plane['best_z']
            if z >= z_size:
                raise ValueError(f'Functional best_z {z} is outside in-vivo anatomy depth {z_size}')
            allowed[max(0, z - 10):min(z_size, z + 11)] = True
        # Use the fixed intensity grid: anatomy-label TIFF Z reversal is irrelevant here.
        ids = sorted(set(map(int, np.unique(warped[allowed]))) & set(dataset.eligible))
        region = _functional_region_in_exvivo(dataset, fixed_img, allowed, transforms, inverse_warp)
        # Content-address the volume so concurrent cache refreshes cannot mix regions.
        import tempfile
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=cache_path.parent, suffix='.npy', delete=False) as handle:
            temporary = Path(handle.name)
            try:
                np.save(handle, region.astype('uint8'), allow_pickle=False)
                handle.flush()
                os.fsync(handle.fileno())
                digest = sha256(temporary)
                region_path = cache_path.parent / f'{dataset.manifest["dataset_id"]}-region-{digest[:12]}.npy'
                temporary.replace(region_path)
            finally:
                temporary.unlink(missing_ok=True)
        edges = np.diff(np.r_[False, allowed, False].astype(int))
        intervals = [[int(lo), int(hi - 1)] for lo, hi in zip(np.flatnonzero(edges == 1), np.flatnonzero(edges == -1))]
        cached = dict(provenance=provenance, eligible_label_ids=ids,
                      invivo_z_intervals=intervals,
                      region_in_exvivo=dict(path=str(region_path), sha256=digest, axes='ZYX', grid=dataset.manifest['grid']),
                      policy='any warped cell voxel intersects the union of inclusive in-vivo best_z ±10 slabs')
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write(cache_path, json.dumps(cached, indent=2))
    ids = cached['eligible_label_ids']
    if not ids or not set(ids).issubset(dataset.eligible):
        raise ValueError(f'No valid cells in functional review scope for {paths.fish_dir.name}')
    region = np.load(region_path, allow_pickle=False)
    if region.shape != dataset.labels.shape or not np.all(np.isin(region, (0, 1))):
        raise ValueError('Invalid cached functional region volume')
    dataset.review_region = region.astype(bool)
    dataset.review_eligible = list(ids)
    dataset.review_scope = cached
    return cached


def prepare_fish_dataset(fish_id, local_root, *, pipeline_root=None, log=print):
    """Discover pipeline inputs by convention and write a hash-bound dataset manifest."""
    import SimpleITK as sitk
    paths = fish_pipeline_paths(fish_id, local_root, pipeline_root)
    anatomy = exvivo_anatomy_path(paths)
    if not anatomy.is_file():
        raise FileNotFoundError(f'Manually oriented ex-vivo anatomy missing: {anatomy}')
    labels = exvivo_labels_path(paths)
    segmentation_manifest, label_shape = _bind_exvivo_label_grid(paths, labels, anatomy)
    labels_sha = sha256(labels)
    dataset_id = f'{fish_id}_exvivo_{labels_sha[:12]}'
    root = manual_identity_root(paths) / dataset_id
    manifest_path = root / 'dataset_manifest.json'
    if manifest_path.exists():
        apply_functional_review_scope(Dataset(manifest_path), local_root, pipeline_root=pipeline_root, log=log)
        return manifest_path
    reader = sitk.ImageFileReader()
    reader.SetFileName(str(anatomy))
    reader.ReadImageInformation()
    grid = dict(shape_zyx=list(reversed(reader.GetSize())), spacing_xyz=list(reader.GetSpacing()),
                origin_xyz=list(reader.GetOrigin()), direction_xyz=list(reader.GetDirection()))
    if label_shape != grid['shape_zyx']:
        raise ValueError(f'Ex-vivo mask shape {label_shape} differs from its Cellpose input {grid["shape_zyx"]}')
    import tifffile
    ids = np.unique(tifffile.imread(labels))
    root.mkdir(parents=True, exist_ok=True)
    eligibility = root / 'eligibility.json'
    atomic_write(eligibility, json.dumps(dict(
        policy_source='All ex-vivo Cellpose masks remain in the annotation inventory; '
                      'functional review scope is computed separately per fish (reviewer decision 2026-10-12); '
                      f'segmentation provenance {segmentation_manifest}',
        eligible_label_ids=[int(n) for n in ids if n > 0]), indent=2))
    markers, validation = [], {}
    stacks = discover_marker_stacks(paths)
    genes = [s['gene'] for s in stacks]
    for stack in stacks:
        log(f'Coverage and transform check: {stack["stack_id"]}')
        coverage = root / 'coverage' / f'{stack["stack_id"]}_coverage_in_exvivo.nrrd'
        report = _coverage_and_validation(stack, anatomy, coverage)
        if not report['chain_reproduction_pearson_r'] >= 0.99:
            raise ValueError(f'Transform chain does not reproduce {stack["image"].name}: r={report["chain_reproduction_pearson_r"]}')
        validation[stack['stack_id']] = report
        markers.append(dict(stack_id=stack['stack_id'], gene=stack['gene'], round_id=stack['round_id'],
                            source_channel=stack['source_channel'], source_path=str(stack['source']),
                            source_sha256=sha256(stack['source']),
                            transform_provenance=[str(p) for p in stack['transforms']],
                            image=dict(path=str(stack['image'])), coverage=dict(path=str(coverage))))
    duplicated = sorted({g for g in genes if genes.count(g) > 1})
    config = dict(schema_version=1, dataset_id=dataset_id, fish_id=fish_id, axes='ZYX', units='um',
                  coordinate_frame='exvivo_manual_oriented', grid=grid,
                  anatomy=dict(path=str(anatomy)), labels=dict(path=str(labels), grid=grid),
                  eligibility=dict(path=str(eligibility)), markers=markers,
                  segmentation_manifest=str(segmentation_manifest), transform_validation=validation)
    if duplicated:
        # Repeated acquisitions need an explicit reviewer choice before export.
        config['unresolved_primary_stack_genes'] = duplicated
    atomic_write(root / 'input_config.json', json.dumps(config, indent=2))
    prepare_dataset(root / 'input_config.json', manifest_path)
    apply_functional_review_scope(Dataset(manifest_path), local_root, pipeline_root=pipeline_root, log=log)
    return manifest_path


def list_datasets(local_root):
    """Fish folders and their manual-identity datasets/sessions for the launcher."""
    rows = []
    for fish_dir in sorted(Path(local_root).iterdir()):
        if not fish_dir.is_dir() or not FISH_ID_RE.match(fish_dir.name):
            continue
        root = fish_dir / '03_analysis' / 'structural' / 'ex_vivo' / 'manual_identity'
        datasets = sorted(p.parent for p in root.glob('*/dataset_manifest.json')) if root.exists() else []
        rows.append(dict(fish_id=fish_dir.name, datasets=datasets,
                         has_exvivo_masks=any((fish_dir / '03_analysis/structural/ex_vivo/cp_masks').glob('*_cp_masks.tif')),
                         gate=_read_json(root / 'gate.json') if root.exists() else None))
    return rows


def _read_json(path):
    path = Path(path)
    return json.loads(path.read_text()) if path.is_file() else None


def session_dir(dataset):
    return dataset.path.parent / 'session'


def _identity_calls(dataset, frozen):
    """Exported positives per gene, with an audit row for every label/gene."""
    rows = pd.DataFrame(frozen['annotations'])
    rows['exvivo_label_id'] = rows['exvivo_label_id'].map(positive_label_id)
    expected = {(s, n) for s in dataset.markers for n in dataset.eligible}
    keys = list(zip(rows.marker_stack_id, rows.exvivo_label_id))
    if len(keys) != len(set(keys)) or set(keys) != expected or not rows.call.isin(CALLS).all():
        raise ValueError('Invalid snapshot annotation inventory')
    primary = dataset.manifest.get('primary_stack_by_gene', {})
    positives, audit = {}, []
    for (label, gene), group in rows.groupby(['exvivo_label_id', 'gene']):
        chosen = primary.get(gene) if len(group) > 1 else group.iloc[0].marker_stack_id
        if chosen not in set(group.marker_stack_id):
            raise ValueError(f'Declare primary_stack_by_gene for {gene}')
        decision = group.loc[group.marker_stack_id == chosen].iloc[0]
        assessed = set(group.loc[group.call.isin(['positive', 'negative']), 'call'])
        outcome = decision.call
        if len(assessed) > 1:
            outcome = 'replicate_conflict'
        elif decision.issue_blocks_export:
            outcome = 'blocking_issue'
        elif decision.call == 'positive':
            positives.setdefault(gene, dict(stack=chosen, labels=set()))['labels'].add(int(label))
        audit.append(dict(exvivo_label_id=int(label), gene=gene, primary_stack_id=chosen, call=decision.call, outcome=outcome))
    return positives, pd.DataFrame(audit)


def plan_identity_gate(dataset, local_root, *, pipeline_root=None):
    """Destinations the gate writes or retires; the app asks before any replacement."""
    from .pipeline import ASSIGN_HCR_IDENTITY_CSVS, stage_manifest_path
    paths = fish_pipeline_paths(dataset.manifest['fish_id'], local_root, pipeline_root)
    destinations = {name: product_path(paths, ASSIGN_STAGE, f'registration/{name}') for name in ROI_TABLES}
    retire = [product_path(paths, ASSIGN_STAGE, f'registration/{name}') for name in ASSIGN_HCR_IDENTITY_CSVS if name not in ROI_TABLES]
    manifest = stage_manifest_path(paths, ASSIGN_STAGE)
    return dict(fish_id=dataset.manifest['fish_id'], pipeline_root=str(paths.pipeline_root),
                destinations={k: str(v) for k, v in destinations.items()},
                replace=[str(p) for p in destinations.values() if p.exists()],
                retire=[str(p) for p in retire if p.exists()],
                manifest=str(manifest), manifest_exists=manifest.exists())


def _promote(paths, files, retire, journal, manifest_path):
    """Native trees use the production promotion journal; staged roots get the same backups."""
    from .native_paths import is_native_root
    from .native_run import promote_files, rollback_promotion
    from .single_fish_run import _file_hash, _write_json_atomic
    if is_native_root(paths.pipeline_root):
        promote_files(paths.fish_dir, files, journal, manifest_path=manifest_path)
    else:
        journal.mkdir(parents=True, exist_ok=False)
        _write_json_atomic(journal / 'previous_manifest.json', {'path': str(manifest_path), 'payload': _read_json(manifest_path)})
        for index, (source, destination) in enumerate(files.items()):
            destination.parent.mkdir(parents=True, exist_ok=True)
            old = None
            if destination.exists():
                shutil.copy2(destination, journal / f'original-{index}')
                old = _file_hash(destination)
            tmp = destination.with_name(f'.{destination.name}.promote-{os.getpid()}')
            shutil.copy2(source, tmp)
            _write_json_atomic(journal / f'intent-{index:05}.json', {'destination': str(destination), 'backup': str(journal / f'original-{index}'),
                               'old_sha256': old, 'new_sha256': _file_hash(source), 'state': 'replaced'})
            tmp.replace(destination)
    try:
        retired = journal / 'retired'
        for path in retire:
            retired.mkdir(exist_ok=True)
            record = dict(path=str(path), sha256=_file_hash(path))
            shutil.copy2(path, retired / path.name)
            if _file_hash(retired / path.name) != record['sha256']:
                raise RuntimeError(f'Retirement backup checksum mismatch: {path}')
            path.unlink()
            _write_json_atomic(retired / (path.name + '.json'), record)
    except Exception:
        for record_path in (journal / 'retired').glob('*.json'):
            record = json.loads(record_path.read_text())
            if not Path(record['path']).exists():
                shutil.copy2(journal / 'retired' / Path(record['path']).name, record['path'])
        rollback_promotion(journal)
        raise


def run_identity_gate(dataset, frozen_path, local_root, *, pipeline_root=None, confirmed=(), log=print):
    """Write the two ROI-centric identity tables and stop at the continuation gate."""
    import tifffile
    from .hcr_warp import _filter_hcr_labels_for_direct_warp, _warp_zyx_labels_with_ants, _write_warped_label_tiff, build_hcr_label_metrics
    from .matching import (FunctionalRoiIdentityConfig, attach_identity_to_functional_roi_geometry_df,
                           build_anat_identity_lookup_df, build_hcr_anatomy_match_tables, gene_from_mask)
    from .pipeline import (PIPELINE_MANIFEST_VERSION, StageCheckRecord, StageManifest, _anatomy_zyx_spacing_from_voxel_cache,
                           _hcr_match_results_from_staged_final_pairs, describe_manifest_path, stage_manifest_path,
                           stage_manifest_to_json)
    from .context import infer_anat_labels_path
    from .hcr_warp import _bridge_transform_pair
    frozen_bytes = Path(frozen_path).read_bytes()
    frozen = json.loads(frozen_bytes)
    if frozen['dataset_fingerprint'] != dataset.fingerprint or frozen['manifest'] != dataset.manifest:
        raise ValueError('Frozen annotation dataset mismatch')
    fish = dataset.manifest['fish_id']
    paths = fish_pipeline_paths(fish, local_root, pipeline_root)
    plan = plan_identity_gate(dataset, local_root, pipeline_root=pipeline_root)
    unconfirmed = sorted(set(plan['replace'] + plan['retire']) - set(map(str, confirmed)))
    if unconfirmed:
        raise PermissionError('Replacement not confirmed: ' + ', '.join(unconfirmed))
    positives, audit = _identity_calls(dataset, frozen)
    for stack, group in pd.DataFrame(frozen['annotations']).groupby('marker_stack_id'):
        assessed = set(group.loc[group.call.isin(['positive', 'negative']), 'exvivo_label_id'].map(int))
        if assessed:
            _, coverage = dataset.marker_data(stack)
            if assessed & (set(np.unique(dataset.labels[coverage == 0])) - {0}):
                raise ValueError('Frozen positive/negative calls have incomplete cell coverage')
    work = dataset.path.parent / 'exports' / f'r{frozen["revision"]:06d}-{_stamp()}'
    aligned, registration = work / 'aligned', work / 'registration'
    aligned.mkdir(parents=True)
    registration.mkdir()
    reg = paths.preproc_dir.parent
    exvivo_to_2p = _bridge_transform_pair(reg / '04_exvivo-2p' / 'transMatrices', source_tokens=('exvivo', 'ex_vivo', 'ex-vivo'), target_tag='to_2p')
    if exvivo_to_2p is None:
        raise FileNotFoundError(f'Missing ex vivo->2P transform pair in {reg / "04_exvivo-2p" / "transMatrices"}')
    anatomy_2p = invivo_anatomy_intensity_path(paths)
    anat_labels_path = infer_anat_labels_path(paths.fish_dir, fish)
    if anat_labels_path is None:
        raise FileNotFoundError(f'In-vivo anatomy labels missing for {fish}')
    log('Warping ex-vivo masks into in-vivo 2P space')
    _ants()
    exvivo_anatomy = Path(dataset.manifest['anatomy']['path'])
    warped, fixed_img = _warp_zyx_labels_with_ants(labels_zyx=dataset.labels, moving_intensity_path=exvivo_anatomy,
                                                    fixed_intensity_path=anatomy_2p, transformlist=exvivo_to_2p)
    spacing = dict(zip(('dx', 'dy', 'dz'), dataset.manifest['grid']['spacing_xyz']))
    metrics_all = build_hcr_label_metrics(dataset.labels, fish_id=fish, mask_path=dataset.manifest['labels']['path'],
                                          voxel_zyx_um=spacing, transformed_labels_zyx=warped, measurement_space='native_exvivo')
    log('Matching ex-vivo masks to in-vivo anatomy labels')
    anat_labels = tifffile.imread(anat_labels_path)
    # Geometry is matched once for all ex-vivo masks, independent of identity.
    matches, final_pairs, review, _ = build_hcr_anatomy_match_tables(
        warped, anat_labels, vox_anat_um=_anatomy_zyx_spacing_from_voxel_cache(paths), label_quality_df=metrics_all)
    paired = set(pd.to_numeric(final_pairs.get('conf_label', pd.Series(dtype=float))).dropna().astype(int))
    metric_rows = []
    gene_order = list(FunctionalRoiIdentityConfig().default_gene_order)
    for gene, chosen in sorted(positives.items()):
        marker = dataset.markers[chosen['stack']]
        labels = sorted(chosen['labels'])
        uid = f'{fish}_exvivo_{marker["round_id"]}_{marker["source_channel"]}_{gene_token(gene)}_cp_masks'
        base = aligned / f'{uid}_in_2p'
        subset = np.where(np.isin(warped, labels), warped, 0)
        _write_warped_label_tiff(Path(f'{base}_labels_uint16.tif'), subset, like_img=fixed_img)
        for suffix, frame in (('matches', matches), ('final_pairs', final_pairs), ('review', review)):
            frame.loc[pd.to_numeric(frame['conf_label']).isin(labels)].to_csv(f'{base}_{suffix}.csv', index=False)
        atomic_write(Path(f'{base}_warp_meta.json'), json.dumps(dict(
            method='ants_same_fish_labels_to_2p', output_space='2p', route='manual_exvivo_masks_exvivo_to_2p',
            identity_source=IDENTITY_SOURCE, gene=gene, marker_stack_id=chosen['stack'],
            mask=dataset.manifest['labels']['path'], mask_sha256=dataset.manifest['labels']['sha256'],
            moving_intensity=str(exvivo_anatomy), fixed_intensity=str(anatomy_2p),
            transformlist=[str(p) for p in exvivo_to_2p], whichtoinvert=[False, False], interpolator='nearestNeighbor',
            positive_exvivo_label_ids=labels, outputs={'primary': f'{base}_labels_uint16.tif'},
            # Same table-driven stats as HCR masks; consumed by the status summary.
            filter_stats=_filter_hcr_labels_for_direct_warp(np.where(np.isin(dataset.labels, labels), dataset.labels, 0))[1]), indent=2))
        metric_rows.append(metrics_all.loc[metrics_all.conf_label.isin(labels)].assign(
            mask_uid=uid, mask_path=f'{base}_labels_uint16.tif', gene=gene))
    (pd.concat(metric_rows, ignore_index=True) if metric_rows else metrics_all.iloc[0:0]).to_csv(aligned / 'hcr_label_metrics.csv', index=False)
    audit['matched_anat_label'] = audit.exvivo_label_id.map(
        final_pairs.assign(conf_label=pd.to_numeric(final_pairs.conf_label)).set_index('conf_label')['twoP_label'] if len(final_pairs) else {})
    audit.loc[audit.outcome.eq('positive') & ~audit.exvivo_label_id.isin(paired), 'outcome'] = 'positive_unmatched_geometry'
    audit.loc[audit.outcome.eq('positive'), 'outcome'] = 'exported'
    audit.to_csv(work / 'manual_identity_audit.csv', index=False)
    pd.DataFrame(frozen['annotations']).to_csv(work / 'manual_annotations.csv', index=False)
    (work / 'annotations_snapshot.json').write_bytes(frozen_bytes)
    log('Building ROI identity tables')
    lookup = build_anat_identity_lookup_df(_hcr_match_results_from_staged_final_pairs(aligned), gene_order=gene_order,
                                           gene_from_mask_func=gene_from_mask)
    geometry_path = product_path(paths, 'match-roi-to-anatomy', 'registration/functional_roi_anatomy_matches.csv')
    score_path = product_path(paths, 'score-activity-bpi', 'registration/functional_roi_activity_identity.csv')
    score = pd.read_csv(score_path)
    roi = attach_identity_to_functional_roi_geometry_df(pd.read_csv(geometry_path), lookup, passthrough_df=score,
                                                        identity_none=FunctionalRoiIdentityConfig().identity_none)
    lookup.to_csv(registration / 'anatomy_identity_lookup.csv', index=False)
    roi.to_csv(registration / 'functional_roi_activity_identity.csv', index=False)
    checks = _roi_table_checks(roi, score)
    if any(c.status == 'fail' for c in checks):
        raise ValueError('ROI identity table checks failed: ' + '; '.join(f'{c.label}: {c.observed}' for c in checks if c.status == 'fail'))
    destinations = {registration / name: Path(plan['destinations'][name]) for name in ROI_TABLES}
    manifest_path = stage_manifest_path(paths, ASSIGN_STAGE)
    journal_root = (paths.functional_dir / 'run_state' / 'manual_identity') if paths.pipeline_root == paths.fish_dir else dataset.path.parent / 'journals'
    journal = journal_root / f'gate-{_stamp()}'
    _promote(paths, destinations, [Path(p) for p in plan['retire']], journal, manifest_path)
    manifest = StageManifest(
        manifest_version=PIPELINE_MANIFEST_VERSION, stage_name=ASSIGN_STAGE, fish_id=fish, status='warn', dry_run=False,
        generated_at=_now().isoformat(),
        inputs=(describe_manifest_path(geometry_path, label='staged ROI/anatomy geometry matches'),
                describe_manifest_path(score_path, label='response-aware ROI activity input'),
                describe_manifest_path(Path(frozen_path), label='frozen manual identity annotations'),
                describe_manifest_path(dataset.path, label='manual identity dataset manifest'),
                describe_manifest_path(aligned, label='manual ex-vivo/anatomy aligned bundle')),
        outputs=tuple(describe_manifest_path(p, label=p.name) for p in destinations.values()),
        checks=checks + (StageCheckRecord(label='identity gate', status='warn',
                                          detail='Manual identity stops after the two ROI-centric tables; HCR-centric tables and downstream stages run only on explicit continuation.',
                                          expected=GATE_CONTINUED, observed=GATE_AWAITING),),
        parameters=dict(local_root=str(local_root), pipeline_root=str(paths.pipeline_root), identity_source=IDENTITY_SOURCE,
                        identity_gate=GATE_AWAITING, hcr_anatomy_root=str(aligned), manual_identity_dataset=str(dataset.path),
                        annotations_snapshot=str(Path(frozen_path).resolve()), annotations_sha256=sha256(frozen_path),
                        gene_order=gene_order, retired_hcr_centric_outputs=plan['retire'], promotion_journal=str(journal),
                        source_policy='ROI identity from manual ex-vivo marker calls; ex-vivo masks warped to 2P with accepted ex-vivo->2P transforms and matched with build_hcr_anatomy_match_tables; conf_label is the ex-vivo mask ID'),
        warnings=('identity gate: HCR-centric tables pending continuation',), errors=())
    atomic_write(manifest_path, stage_manifest_to_json(manifest))
    gate = dict(schema_version=1, status=GATE_AWAITING, fish_id=fish, dataset=str(dataset.path), export=str(work),
                aligned=str(aligned), snapshot=str(Path(frozen_path).resolve()), revision=frozen['revision'],
                pipeline_root=str(paths.pipeline_root), local_root=str(local_root), journal=str(journal),
                outputs={str(d): sha256(d) for d in destinations.values()}, created_at=_now().isoformat(),
                summary=audit.groupby(['gene', 'outcome']).size().unstack(fill_value=0).to_dict('index'))
    atomic_write(manual_identity_root(paths) / 'gate.json', json.dumps(gate, indent=2))
    return gate


def _roi_table_checks(roi, score):
    from .pipeline import StageCheckRecord
    keys = ['plane_idx', 'func_label']
    identity = {'identity_label', 'has_identity_assigned', 'identity_display_label', 'identity_gene_count', 'identity_genes'}
    same_columns = list(roi.columns) == list(score.columns)
    same_keys = roi[keys].apply(tuple, axis=1).tolist() == score[keys].apply(tuple, axis=1).tolist()
    common = [c for c in score.columns if c not in identity and c in roi.columns]
    unchanged = roi[common].fillna('<NA>').astype(str).equals(score[common].fillna('<NA>').astype(str))
    label = roi['identity_label'].fillna('').astype(str)
    consistent = bool((roi['has_identity_assigned'].astype(bool) == label.ne('')).all())
    unique = bool(roi.get('has_unique_anat_match', pd.Series(True, index=roi.index)).astype(bool)[label.ne('')].all())
    def check(name, ok, detail):
        return StageCheckRecord(label=name, status='pass' if ok else 'fail', detail=detail, expected='True', observed=str(bool(ok)))
    return (check('ROI master schema equals score-activity-bpi master', same_columns, 'Column set and order are the score master contract.'),
            check('ROI keys and order unchanged', same_keys, 'One row per functional ROI in score-master order.'),
            check('non-identity ROI fields unchanged', unchanged, 'Geometry and response fields pass through untouched.'),
            check('identity flag consistency', consistent, 'has_identity_assigned == (identity_label != "").'),
            check('identity implies unique anatomy match', unique, 'Identity only on uniquely matched ROIs.'))


def read_gate(fish_id, local_root, *, pipeline_root=None):
    return _read_json(manual_identity_root(fish_pipeline_paths(fish_id, local_root, pipeline_root)) / 'gate.json')


def _stage_commands(paths, aligned, graph, python):
    """Production commands from the fish run spec, re-rooted; defaults otherwise."""
    fish, local_root = paths.fish_dir.name, str(paths.data_root)
    spec_path = paths.functional_dir / 'run_specs' / f'{fish}.json'
    spec = _read_json(spec_path) or {}
    by_name = {s['name']: s['command'] for s in spec.get('stages', [])}
    pinned = (spec.get('interpreter') or {}).get('executable')
    # Match the production interpreter when the fish has a run spec.
    python = python or (pinned if pinned and Path(pinned).is_file() else sys.executable)
    commands = []
    for name, writer in CONTINUATION_STAGES:
        command = list(by_name.get(name) or [python, 'tools/single_fish_pipeline.py', writer, '--fish-id', fish,
                                             '--local-root', local_root, '--pipeline-root', str(paths.pipeline_root),
                                             '--strict', '--write-manifest', '--force-recompute'])
        spec_root = (spec.get('paths') or {}).get('data_root')
        if spec_root and str(Path(spec_root)) != str(paths.data_root):
            # A relocated tree (e.g. a staging copy) keeps every path inside itself.
            command = [str(paths.data_root) + c[len(str(Path(spec_root))):] if c.startswith(str(Path(spec_root)) + '/') or c == str(Path(spec_root)) else c
                       for c in command]
        command[0] = python
        command[command.index('--pipeline-root') + 1] = str(graph)
        command = [c for c in command if c != '--write-manifest']
        if writer == ASSIGN_STAGE:
            if '--hcr-anatomy-root' in command:
                command[command.index('--hcr-anatomy-root') + 1] = str(aligned)
            else:
                command += ['--hcr-anatomy-root', str(aligned)]
            command += ['--identity-source', IDENTITY_SOURCE]
            for flag, stage, relative in (('--activity-input-path', 'score-activity-bpi', 'registration/functional_roi_activity_identity.csv'),
                                          ('--roi-anatomy-root', 'match-roi-to-anatomy', 'registration')):
                if flag not in command:
                    command += [flag, str(product_path(paths, stage, relative))]
        if '--force-recompute' not in command:
            command.append('--force-recompute')
        commands.append((name, writer, command))
    return commands


def _seed_graph(paths, graph, rerun, log):
    """Copy the latest committed native graph, verifying every product against the fish tree."""
    from .native_paths import native_product_path
    from .single_fish_run import _file_hash
    candidates = sorted((p for p in (paths.functional_dir / 'run_state').glob('*/transaction_graph') if (p / 'score-activity-bpi').is_dir()),
                        key=lambda p: p.stat().st_mtime)
    if not candidates:
        raise FileNotFoundError('No committed native transaction graph to continue from')
    source = candidates[-1]
    log(f'Seeding stage graph from {source}')
    for stage_dir in sorted(source.iterdir()):
        if not stage_dir.is_dir() or stage_dir.name in rerun:
            continue
        for path in stage_dir.rglob('*'):
            if not path.is_file() or path.name.startswith('._'):
                continue
            relative = path.relative_to(stage_dir).as_posix()
            try:
                product = native_product_path(paths.fish_dir, stage_dir.name, relative)
            except ValueError:
                product = None
            if product is not None and product.is_file() and path.suffix.lower() not in {'.json', '.csv', '.md', '.html', '.txt'}:
                if _file_hash(product) != _file_hash(path):
                    raise RuntimeError(f'Native product differs from committed graph: {product}')
            target = graph / stage_dir.name / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target)
    return source


def continue_after_gate(fish_id, local_root, *, pipeline_root=None, python=None, log=print):
    """Run assign-hcr-identity in manual mode, then the downstream writers, promoting each."""
    from .native_paths import is_native_root, native_product_path
    from .native_run import _relocate_tree, _rebind, promote_files, rollback_promotion
    from .pipeline import describe_manifest_path, stage_manifest_path
    from .single_fish_run import _write_json_atomic
    paths = fish_pipeline_paths(fish_id, local_root, pipeline_root)
    gate_path = manual_identity_root(paths) / 'gate.json'
    gate = _read_json(gate_path)
    if not gate or gate['status'] != GATE_AWAITING:
        raise ValueError('No identity gate awaiting continuation for this fish')
    for path, digest in gate['outputs'].items():
        if sha256(path) != digest:
            raise ValueError(f'Gate ROI table changed since export: {path}')
    native = is_native_root(paths.pipeline_root)
    run_root = paths.functional_dir / 'run_state' / f'{_stamp()}_manual_identity' if native else Path(gate['export']) / 'continuation'
    graph = run_root / 'transaction_graph' if native else paths.pipeline_root
    rerun = {writer for _, writer in CONTINUATION_STAGES}
    if native:
        _seed_graph(paths, graph, rerun, log)
    env = dict(os.environ, PYTHONPATH=str(REPO_ROOT / 'src'), MPLBACKEND='Agg', PYTHONHASHSEED='0', KMP_DUPLICATE_LIB_OK='TRUE')
    results = []
    for name, writer, command in _stage_commands(paths, Path(gate['aligned']), graph, python):
        log(f'Running {writer}')
        log_path = run_root / 'logs' / f'{name}.log'
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open('w') as handle:
            code = subprocess.run(command, cwd=REPO_ROOT, env=env, stdout=handle, stderr=subprocess.STDOUT, text=True).returncode
        manifest = _last_stage_manifest(log_path.read_text(), writer)
        if code or manifest is None or manifest.get('status') not in {'pass', 'warn'} or manifest.get('errors') or any(c.get('status') == 'fail' for c in manifest.get('checks', ())):
            raise RuntimeError(f'{writer} failed; see {log_path}')
        manifest_path = stage_manifest_path(paths, writer)
        if native:
            output_dir = graph / writer
            roots = [str(graph)]
            _relocate_tree(output_dir, roots, paths.fish_dir)
            if writer == 'make-qa-report':
                from .pipeline import _write_qa_report_pdf
                _write_qa_report_pdf(json.loads((output_dir / 'qa_report_summary.json').read_text()), output_dir / 'qa_report.pdf')
            manifest = json.loads(_rebind(json.dumps(manifest), roots, paths.fish_dir))
            products = {p: native_product_path(paths.fish_dir, writer, p.relative_to(output_dir).as_posix())
                        for p in output_dir.rglob('*') if p.is_file() and not p.name.startswith('._')}
            journal = run_root / 'history' / f'{name}-{_stamp()}'
            promote_files(paths.fish_dir, products, journal, manifest_path=manifest_path)
            try:
                manifest['source_writer_inputs'] = manifest.get('inputs', [])
                manifest['outputs'] = [asdict(describe_manifest_path(d, label=d.name)) for d in products.values()]
                manifest['generated_at'] = _now().isoformat()
                manifest['parameters'].pop('log_lines', None)
                manifest['parameters'].update(pipeline_root=str(paths.fish_dir), native_layout='existing-fish-tree-v1',
                                              output_root=str(paths.fish_dir), execution_root=str(graph))
                _write_json_atomic(manifest_path, manifest)
                _write_json_atomic(journal / 'stage_commit.json', {'stage': name, 'manifest': str(manifest_path)})
            except Exception:
                rollback_promotion(journal)
                raise
        else:
            _write_json_atomic(manifest_path, manifest)
        results.append(dict(stage=writer, status=manifest['status'], log=str(log_path)))
    gate.update(status=GATE_CONTINUED, continued_at=_now().isoformat(), continuation=dict(run_root=str(run_root), results=results))
    atomic_write(gate_path, json.dumps(gate, indent=2))
    return gate


def _last_stage_manifest(text, stage):
    decoder, found = json.JSONDecoder(), None
    for index, character in enumerate(text):
        if character == '{':
            try:
                candidate, _ = decoder.raw_decode(text[index:])
            except ValueError:
                continue
            if isinstance(candidate, dict) and candidate.get('stage_name') == stage:
                found = candidate
    return found
