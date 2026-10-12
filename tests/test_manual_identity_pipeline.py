"""Manual ex-vivo identity on a synthetic native fish tree with identity ANTs transforms."""
import json
import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

os.environ.setdefault('KMP_DUPLICATE_LIB_OK', 'TRUE')
ants = pytest.importorskip('ants')
tifffile = pytest.importorskip('tifffile')

from codeants_2pf_hcr import manual_identity_pipeline as mip
from codeants_2pf_hcr.manual_identity import AnnotationSession, Dataset

FISH = 'Z001_f01'
SHAPE = (8, 40, 40)


def _image(path, zyx, dtype='float32'):
    image = ants.from_numpy(np.transpose(zyx, (2, 1, 0)).astype('float32'), spacing=(1., 1., 2.))
    if dtype != 'float32':
        image = image.clone(dtype)
    path.parent.mkdir(parents=True, exist_ok=True)
    ants.image_write(image, str(path))
    return image


def _identity_transforms(directory, stem, like):
    directory.mkdir(parents=True, exist_ok=True)
    ants.write_transform(ants.create_ants_transform(transform_type='AffineTransform', dimension=3),
                         str(directory / f'{stem}_0GenericAffine.mat'))
    field = ants.from_numpy(np.zeros((*like.shape, 3), dtype='float32'), origin=like.origin, spacing=like.spacing,
                            direction=like.direction, has_components=True)
    ants.image_write(field, str(directory / f'{stem}_1Warp.nii.gz'))


def make_fish(root):
    fish = root / FISH
    analysis, reg = fish / '03_analysis', fish / '02_reg'
    exvivo = np.zeros(SHAPE, dtype=np.uint16)
    # Cells exceed the HCR label-quality floor of 200 native voxels.
    exvivo[1:5, 3:11, 3:11] = 11
    exvivo[2:6, 22:30, 22:30] = 22
    exvivo[1:5, 3:11, 22:30] = 33
    anatomy = (exvivo > 0) * 100.0 + 5
    like = _image(reg / '00_preprocessing/2p_anatomy/ex_vivo' / f'{FISH}_exvivo_anatomy_2P_GCaMP_uint8_manual_oriented.nrrd', anatomy)
    _image(reg / '00_preprocessing/2p_anatomy' / f'{FISH}_anatomy_2P_GCaMP.nrrd', anatomy)
    _identity_transforms(reg / '04_exvivo-2p/transMatrices', f'{FISH}_exvivo_GCaMP_to_2p', like)
    _identity_transforms(reg / '03_rbest-exvivo/transMatrices', f'{FISH}_rbest_GCaMP_to_exvivo', like)
    _identity_transforms(reg / '05_rn-exvivo/transMatrices', f'{FISH}_r2_GCaMP_to_exvivo', like)
    for round_id, channel, gene, folder in (('rbest', 3, 'pth2', '03_rbest-exvivo'), ('r2', 2, 'sst1_2', '05_rn-exvivo')):
        signal = anatomy * (np.arange(SHAPE[2])[None, None, :] + 1)
        _image(reg / '00_preprocessing' / ('rbest' if round_id == 'rbest' else 'rn') / f'{FISH}_{round_id}_channel{channel}_{gene}.nrrd', signal)
        _image(reg / folder / 'aligned' / f'{FISH}_{round_id}_{gene}_in_exvivo.nrrd', signal)
    masks = analysis / 'structural/ex_vivo/cp_masks' / f'{FISH}_exvivo_anatomy_2P_GCaMP_uint8_manual_oriented_8bit_cp_masks.tif'
    masks.parent.mkdir(parents=True)
    tifffile.imwrite(masks, exvivo)
    manifests = analysis / 'structural/ex_vivo/manifests'
    manifests.mkdir(parents=True)
    (manifests / 'segment-ex-vivo-anatomy-cellpose_manifest.json').write_text(json.dumps(dict(
        stage_name='segment-ex-vivo-anatomy-cellpose', status='pass',
        inputs=[dict(label='prepared ex vivo anatomy stack', path='Y:\\x\\' + like_name(FISH))],
        outputs=[dict(label='ex vivo anatomy Cellpose masks', path=str(masks))])))
    invivo = np.select([exvivo == 11, exvivo == 22, exvivo == 33], [101, 102, 103], 0).astype(np.uint16)
    labels = analysis / 'structural/cp_masks' / f'{FISH}_anatomy_00001_uint8_8bit_cp_masks.tif'
    labels.parent.mkdir(parents=True)
    tifffile.imwrite(labels, invivo)
    functional = analysis / 'functional'
    (functional / 'pipeline_manifests').mkdir(parents=True)
    (functional / 'pipeline_manifests/native_layout.json').write_text(json.dumps(dict(fish_id=FISH, pipeline_root=str(fish), layout='existing-fish-tree-v1')))
    geometry = pd.DataFrame(dict(fish_id=FISH, plane_idx=[0, 0, 1, 1], func_label=[1, 2, 1, 2],
                                 selected_anat_label=[101., 102., 103., np.nan], has_unique_anat_match=[True, True, True, False],
                                 anat_label=[101., 102., 103., np.nan]))
    (functional / 'registration').mkdir(parents=True)
    (functional / 'registration/plane_refs_summary.json').write_text(json.dumps([
        dict(index=0, best_z=2), dict(index=1, best_z=4)]))
    geometry.to_csv(functional / 'registration/functional_roi_anatomy_matches.csv', index=False)
    score = geometry.assign(is_active=[True, False, True, False], identity_label='', has_identity_assigned=False,
                            identity_display_label='', bpi=[.5, -.2, .1, 0.])
    score_dir = functional / 'derived/score-activity-bpi/registration'
    score_dir.mkdir(parents=True)
    score.to_csv(score_dir / 'functional_roi_activity_identity.csv', index=False)
    return fish


def like_name(fish):
    return f'{fish}_exvivo_anatomy_2P_GCaMP_uint8_manual_oriented.nrrd'


def test_prepare_review_gate_writes_roi_tables_at_assign_location(tmp_path):
    fish = make_fish(tmp_path)
    manifest = mip.prepare_fish_dataset(FISH, tmp_path, log=lambda *_: None)
    assert manifest.parent.parent == fish / '03_analysis/structural/ex_vivo/manual_identity'
    dataset = Dataset(manifest)
    assert set(dataset.markers) == {'rbest-pth2', 'r2-sst1.2'}
    assert dataset.eligible == [11, 22, 33]
    validation = json.loads((manifest.parent / 'input_config.json').read_text())['transform_validation']
    assert all(v['chain_reproduction_pearson_r'] > .999 and v['coverage_fraction'] == 1 for v in validation.values())
    assert mip.prepare_fish_dataset(FISH, tmp_path) == manifest  # idempotent
    listed = mip.list_datasets(tmp_path)
    assert listed[0]['fish_id'] == FISH and listed[0]['datasets'] == [manifest.parent]

    session = AnnotationSession(dataset, mip.session_dir(dataset), 'tester')
    session.set_call('rbest-pth2', 11, 'positive')
    session.set_call('r2-sst1.2', 11, 'positive')
    session.set_call('r2-sst1.2', 22, 'positive')
    session.set_call('rbest-pth2', 33, 'positive', issue='registration_mismatch', blocking=True)
    session.set_call('rbest-pth2', 22, 'negative')
    frozen = session.freeze()
    session.close()

    # Pre-existing HCR-route identity outputs must be confirmed before replacement.
    assign = fish / '03_analysis/functional/derived/assign-hcr-identity/registration'
    assign.mkdir(parents=True)
    (assign / 'hcr_activity_status.csv').write_text('gene,anat_label\npth2,999\n')
    (assign / 'anatomy_identity_lookup.csv').write_text('old\n')
    plan = mip.plan_identity_gate(dataset, tmp_path)
    assert plan['retire'] == [str(assign / 'hcr_activity_status.csv')]
    assert plan['replace'] == [str(assign / 'anatomy_identity_lookup.csv')]
    with pytest.raises(PermissionError):
        mip.run_identity_gate(dataset, frozen, tmp_path, log=lambda *_: None)
    gate = mip.run_identity_gate(dataset, frozen, tmp_path, confirmed=plan['replace'] + plan['retire'], log=lambda *_: None)

    lookup = pd.read_csv(assign / 'anatomy_identity_lookup.csv')
    assert lookup[['anat_label', 'identity_label']].values.tolist() == [[101, 'sst1.2/pth2'], [102, 'sst1.2']]
    assert list(lookup.columns) == ['anat_label', 'identity_label', 'identity_gene_count', 'identity_genes']
    roi = pd.read_csv(assign / 'functional_roi_activity_identity.csv', keep_default_na=False)
    score = pd.read_csv(fish / '03_analysis/functional/derived/score-activity-bpi/registration/functional_roi_activity_identity.csv')
    assert list(roi.columns) == list(score.columns)
    assert roi.identity_label.tolist() == ['sst1.2/pth2', 'sst1.2', '', '']
    assert roi.identity_display_label.tolist()[2:] == ['no identity assigned'] * 2
    assert not (assign / 'hcr_activity_status.csv').exists()
    retired = Path(gate['journal']) / 'retired' / 'hcr_activity_status.csv'
    assert retired.read_text().startswith('gene,anat_label')
    stage = json.loads((fish / '03_analysis/functional/pipeline_manifests/assign-hcr-identity_manifest.json').read_text())
    assert stage['parameters']['identity_source'] == 'manual_exvivo'
    assert stage['parameters']['identity_gate'] == mip.GATE_AWAITING
    assert stage['status'] == 'warn' and not any(c['status'] == 'fail' for c in stage['checks'])
    assert {Path(o['path']).name for o in stage['outputs']} == set(mip.ROI_TABLES)
    assert mip.read_gate(FISH, tmp_path)['status'] == mip.GATE_AWAITING

    aligned = Path(gate['aligned'])
    pairs = pd.read_csv(next(aligned.glob('*_channel3_pth2_cp_masks_in_2p_final_pairs.csv')))
    # conf_label is the ex-vivo mask ID; blocking-issue positives are not exported.
    assert pairs[['conf_label', 'twoP_label']].values.tolist() == [[11, 101]]
    meta = json.loads(next(aligned.glob('*pth2*_warp_meta.json')).read_text())
    assert meta['identity_source'] == 'manual_exvivo' and meta['filter_stats']['n_labels_after'] == 1
    metrics = pd.read_csv(aligned / 'hcr_label_metrics.csv')
    assert set(metrics.gene) == {'pth2', 'sst1.2'} and set(metrics.measurement_space) == {'native_exvivo'}
    audit = pd.read_csv(Path(gate['export']) / 'manual_identity_audit.csv').set_index(['exvivo_label_id', 'gene'])
    assert audit.loc[(33, 'pth2'), 'outcome'] == 'blocking_issue'
    assert audit.loc[(11, 'pth2'), 'outcome'] == 'exported'
    assert audit.loc[(22, 'pth2'), 'outcome'] == 'negative'


def test_continuation_requires_unchanged_gate_tables(tmp_path, monkeypatch):
    fish = make_fish(tmp_path)
    dataset = Dataset(mip.prepare_fish_dataset(FISH, tmp_path, log=lambda *_: None))
    session = AnnotationSession(dataset, mip.session_dir(dataset), 'tester')
    session.set_call('rbest-pth2', 11, 'positive')
    frozen = session.freeze()
    session.close()
    gate = mip.run_identity_gate(dataset, frozen, tmp_path, log=lambda *_: None)
    table = Path(next(iter(gate['outputs'])))
    table.write_text(table.read_text() + '\n')
    with pytest.raises(ValueError, match='changed since export'):
        mip.continue_after_gate(FISH, tmp_path, log=lambda *_: None)


def test_manual_identity_source_replaces_hcr_control_parity(tmp_path):
    from codeants_2pf_hcr.pipeline import (SingleFishPipelineConfig, _build_staged_comparison_checks,
                                           current_identity_source, resolve_pipeline_paths, stage_manifest_path)
    paths = resolve_pipeline_paths(SingleFishPipelineConfig(fish_id=FISH, local_root=tmp_path, pipeline_root=tmp_path / 'graph'))
    assert current_identity_source(paths) == 'hcr'
    manifest = stage_manifest_path(paths, 'assign-hcr-identity')
    manifest.parent.mkdir(parents=True)
    manifest.write_text(json.dumps(dict(parameters=dict(identity_source='manual_exvivo'))))
    assert current_identity_source(paths) == 'manual_exvivo'
    checks = _build_staged_comparison_checks(paths, 'export-canonical-tables')
    assert checks and all(c.label.startswith('manual identity output present') for c in checks)


def test_functional_review_scope_union_edges_cache_and_complete_fish(tmp_path, monkeypatch):
    fish = make_fish(tmp_path)
    manifest = mip.prepare_fish_dataset(FISH, tmp_path, log=lambda *_: None)
    dataset = Dataset(manifest)
    refs = fish / '03_analysis/functional/registration/plane_refs_summary.json'
    refs.write_text(json.dumps([dict(index=0, best_z=5), dict(index=1, best_z=50)]))
    # Fixed-space masks: 11 touches the upper inclusive edge, 22 lies in the gap,
    # 33 touches the lower edge of the second plane. Their native centroids differ.
    warped = np.zeros((70, 40, 40), dtype=np.uint16)
    warped[15, 1, 1] = 11
    warped[25, 1, 1] = 22
    warped[40, 1, 1] = 33
    calls = []
    def warp(**kwargs):
        calls.append(kwargs)
        return warped, None
    monkeypatch.setattr('codeants_2pf_hcr.hcr_warp._warp_zyx_labels_with_ants', warp)
    scope = mip.apply_functional_review_scope(dataset, tmp_path, log=lambda *_: None)
    assert dataset.review_eligible == [11, 33]
    assert dataset.eligible == [11, 22, 33]  # immutable annotation inventory
    assert scope['provenance']['fish_id'] == FISH
    mip.apply_functional_review_scope(dataset, tmp_path)
    assert len(calls) == 1  # hashed cache is reusable
    refs.write_text(json.dumps([dict(index=0, best_z=5), dict(index=1, best_z=25)]))
    mip.apply_functional_review_scope(dataset, tmp_path, log=lambda *_: None)
    assert dataset.review_eligible == [11, 22] and len(calls) == 2
    # Accepted geometry may override raw NCC best_z; that is the review authority.
    effective = refs.with_name('plane_refs_summary_geometry.json')
    effective.write_text(json.dumps([dict(index=0, best_z=5), dict(index=1, best_z=50)]))
    mip.apply_functional_review_scope(dataset, tmp_path, log=lambda *_: None)
    assert dataset.review_eligible == [11, 33] and len(calls) == 3
    effective.write_text(json.dumps([dict(index=0, best_z=5), dict(index=1, best_z=25)]))
    mip.apply_functional_review_scope(dataset, tmp_path, log=lambda *_: None)
    assert dataset.review_eligible == [11, 22] and len(calls) == 4
    effective.unlink()
    refs.write_text(json.dumps([dict(index=0, best_z=5)]))
    with pytest.raises(ValueError, match='fish inventory'):
        mip.apply_functional_review_scope(dataset, tmp_path)


def test_scope_preserves_existing_calls_and_blocks_new_outside_calls(tmp_path):
    fish = make_fish(tmp_path)
    dataset = Dataset(mip.prepare_fish_dataset(FISH, tmp_path, log=lambda *_: None))
    directory = mip.session_dir(dataset)
    with_session = AnnotationSession(dataset, directory, 'reviewer')
    with_session.set_call('rbest-pth2', 22, 'positive')
    with_session.close()
    dataset.review_eligible = [11, 33]
    session = AnnotationSession(dataset, directory, 'reviewer')
    try:
        assert session.rows[('rbest-pth2', 22)]['call'] == 'positive'
        with pytest.raises(ValueError, match='outside'):
            session.set_call('rbest-pth2', 22, 'negative')
        session.set_call('rbest-pth2', 11, 'positive')
        frozen = json.loads(session.freeze().read_text())
        assert len(frozen['annotations']) == 6
    finally:
        session.close()
