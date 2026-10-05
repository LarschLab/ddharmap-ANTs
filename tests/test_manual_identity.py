import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from codeants_2pf_hcr.manual_identity import Dataset, AnnotationSession, export_manual_identity, prepare_dataset, sha256


def make_dataset(root):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    shape = [8, 24, 24]
    grid = dict(shape_zyx=shape, spacing_xyz=[1., 1., 2.], origin_xyz=[0., 0., 0.], direction_xyz=[1., 0., 0., 0., 1., 0., 0., 0., 1.])
    labels = np.zeros(shape, dtype=np.uint16)
    labels[2:5, 5:10, 5:10] = 11
    labels[3:6, 14:19, 14:19] = 22
    anatomy = labels.astype(float) * 4
    def asset(name, data):
        path = root / (name + '.npy')
        np.save(path, data)
        return dict(path=str(path), sha256=sha256(path), grid=grid)
    eligibility = root / 'eligibility.json'
    eligibility.write_text(json.dumps(dict(policy_source='fixture existing eligibility', eligible_label_ids=[11, 22])))
    manifest = dict(schema_version=1, dataset_id='fixture-v1', fish_id='fixture', axes='ZYX', units='um',
                    coordinate_frame='exvivo_manual_oriented', grid=grid,
                    anatomy=asset('anatomy', anatomy), labels=asset('labels', labels),
                    eligibility=dict(path=str(eligibility), sha256=sha256(eligibility)), markers=[])
    for gene in ('pth2', 'cort'):
        manifest['markers'].append(dict(stack_id=gene + '-r1', gene=gene, round_id='r1', source_channel='ch1',
            source_sha256='f' * 64, transform_provenance=['fixture-r1-to-exvivo'],
            image=asset(gene, anatomy * (0.5 if gene == 'cort' else 1)),
            coverage=asset(gene + '-coverage', np.ones(shape, dtype=np.uint8))))
    path = root / 'manifest.json'
    path.write_text(json.dumps(manifest))
    return Dataset(path)


@pytest.fixture
def dataset(tmp_path):
    return make_dataset(tmp_path / 'data')


def export_inputs(dataset, root):
    geometry = pd.DataFrame(dict(plane_idx=[0, 0, 1], func_label=[1, 2, 1], anat_label=[101, 102, 101], selected_anat_label=[101, 102, 101], overlap=[.8, .9, .7]))
    geometry_path = root / 'geometry.csv'
    geometry.to_csv(geometry_path, index=False)
    master = geometry.assign(identity_label='legacy', identity_gene_count=1, has_identity_assigned=True,
                             identity_display_label='legacy', response_is_active=[True, False, True], bpi=[.5, -.2, .4])
    master_path = root / 'master.csv'
    master.to_csv(master_path, index=False)
    invivo_path = root / 'invivo_labels.npy'
    np.save(invivo_path, np.where(dataset.labels == 11, 101, np.where(dataset.labels == 22, 102, 0)).astype(np.uint16))
    mapping = dict(schema_version=1, fish_id='fixture', exvivo_labels_sha256=dataset.manifest['labels']['sha256'],
        invivo_labels_sha256=sha256(invivo_path), invivo_labels=dict(path=str(invivo_path), sha256=sha256(invivo_path)), geometry_sha256=sha256(geometry_path), transform_provenance=['reviewed transform'],
        reviewer='scientist', reviewed_at_utc='2026-10-05T12:00:00Z',
        rows=[dict(exvivo_label_id=11, anat_label=101, status='accepted', metrics=dict(overlap=.8)),
              dict(exvivo_label_id=22, anat_label=102, status='accepted', metrics=dict(overlap=.9))])
    mapping_path = root / 'mapping.json'
    mapping_path.write_text(json.dumps(mapping))
    return geometry_path, master_path, mapping_path


def test_independent_calls_resume_and_history(dataset, tmp_path):
    session = AnnotationSession(dataset, tmp_path / 'session', 'reviewer')
    session.set_call('pth2-r1', 11, 'positive')
    session.set_call('cort-r1', 11, 'positive')
    session.set_call('cort-r1', 22, 'unassessable', issue='poor_coverage', blocking=True)
    session.undo()
    assert session.rows[('cort-r1', 22)]['call'] == 'unreviewed'
    session.redo()
    frozen = session.freeze()
    session.close()
    resumed = AnnotationSession(dataset, tmp_path / 'session', 'reviewer2')
    assert resumed.rows[('pth2-r1', 11)]['call'] == 'positive'
    assert resumed.rows[('cort-r1', 11)]['call'] == 'positive'
    assert resumed.rows[('cort-r1', 22)]['issue_blocks_export']
    assert len(json.loads(frozen.read_text())['annotations']) == 4
    resumed.close()


def test_session_lock_and_changed_manifest(dataset, tmp_path):
    path = tmp_path / 'session'
    first = AnnotationSession(dataset, path, 'one')
    with pytest.raises(ValueError, match='locked'):
        AnnotationSession(dataset, path, 'two')
    first.close()
    dataset.manifest['coordinate_frame'] = 'changed'
    with pytest.raises(ValueError, match='changed'):
        AnnotationSession(dataset, path, 'two')
    assert not (path / 'writer.lock').exists()


def test_journal_recovers_truncated_tail(dataset, tmp_path):
    path = tmp_path / 'session'
    first = AnnotationSession(dataset, path, 'one')
    first.set_call('pth2-r1', 11, 'positive')
    first.close()
    with (path / 'annotation_events.jsonl').open('ab') as f:
        f.write(b'{"partial":')
    recovered = AnnotationSession(dataset, path, 'two')
    assert recovered.rows[('pth2-r1', 11)]['call'] == 'positive'
    assert list(path.glob('journal-tail-*'))
    recovered.close()


def test_input_hash_and_grid_rejection(dataset):
    spec = dataset.manifest['markers'][0]['image']
    original = np.load(spec['path'])
    np.save(spec['path'], original + 1)
    with pytest.raises(ValueError, match='hash'):
        Dataset(dataset.path)
    spec['sha256'] = sha256(spec['path'])
    spec['grid']['spacing_xyz'] = [9, 1, 2]
    dataset.path.write_text(json.dumps(dataset.manifest))
    with pytest.raises(ValueError, match='grid'):
        Dataset(dataset.path)


def test_coverage_cannot_be_negative_or_positive(dataset, tmp_path):
    marker = dataset.markers['pth2-r1']
    coverage = np.load(marker['coverage']['path'])
    coverage[2, 5, 5] = 0
    np.save(marker['coverage']['path'], coverage)
    marker['coverage']['sha256'] = sha256(marker['coverage']['path'])
    session = AnnotationSession(dataset, tmp_path / 'session', 'one')
    for call in ('positive', 'negative'):
        with pytest.raises(ValueError, match='coverage'):
            session.set_call('pth2-r1', 11, call)
    session.set_call('pth2-r1', 11, 'unassessable')
    session.close()


def test_export_coexpression_and_preserves_roi_contract(dataset, tmp_path):
    session = AnnotationSession(dataset, tmp_path / 'session', 'one')
    session.set_call('pth2-r1', 11, 'positive')
    session.set_call('cort-r1', 11, 'positive')
    session.set_call('cort-r1', 22, 'negative')
    frozen = session.freeze()
    session.close()
    geometry, master, mapping = export_inputs(dataset, tmp_path)
    output = tmp_path / 'export'
    result = export_manual_identity(dataset, frozen, mapping, geometry, master, output, ['cort', 'pth2'])
    assert result.identity_label.fillna('').tolist() == ['cort/pth2', '', 'cort/pth2']
    assert result.has_identity_assigned.tolist() == [True, False, True]
    assert result.identity_gene_count.iloc[0] == 2
    old = pd.read_csv(master)
    pd.testing.assert_frame_equal(result.drop(columns=['identity_label', 'identity_gene_count', 'has_identity_assigned', 'identity_display_label']),
        old.drop(columns=['identity_label', 'identity_gene_count', 'has_identity_assigned', 'identity_display_label']))
    with pytest.raises(FileExistsError):
        export_manual_identity(dataset, frozen, mapping, geometry, master, output, ['cort', 'pth2'])
    assert json.loads((output / 'manual_identity_export_manifest.json').read_text())['hcr_centric_supported'] is False


def test_ambiguous_mapping_and_blocking_issue_are_audited(dataset, tmp_path):
    session = AnnotationSession(dataset, tmp_path / 'session', 'one')
    session.set_call('pth2-r1', 11, 'positive')
    session.set_call('cort-r1', 22, 'positive', blocking=True, issue='registration_mismatch')
    frozen = session.freeze()
    session.close()
    geometry, master, mapping_path = export_inputs(dataset, tmp_path)
    mapping = json.loads(mapping_path.read_text())
    mapping['rows'][0]['status'] = 'ambiguous'
    mapping_path.write_text(json.dumps(mapping))
    output = tmp_path / 'export'
    result = export_manual_identity(dataset, frozen, mapping_path, geometry, master, output, [])
    assert not result.has_identity_assigned.any()
    assert set(pd.read_csv(output / 'manual_identity_mapping_audit.csv').outcome) >= {'unmapped_or_ambiguous', 'blocking_issue'}


def test_competing_mapping_and_changed_geometry_fail(dataset, tmp_path):
    session = AnnotationSession(dataset, tmp_path / 'session', 'one')
    frozen = session.freeze()
    session.close()
    geometry, master, mapping_path = export_inputs(dataset, tmp_path)
    mapping = json.loads(mapping_path.read_text())
    mapping['rows'][1]['anat_label'] = 101
    mapping_path.write_text(json.dumps(mapping))
    with pytest.raises(ValueError, match='Competing'):
        export_manual_identity(dataset, frozen, mapping_path, geometry, master, tmp_path / 'export', [])
    mapping['rows'][1]['anat_label'] = 102
    mapping_path.write_text(json.dumps(mapping))
    frame = pd.read_csv(master)
    frame.loc[0, 'overlap'] = .1
    frame.to_csv(master, index=False)
    with pytest.raises(AssertionError):
        export_manual_identity(dataset, frozen, mapping_path, geometry, master, tmp_path / 'export', [])


def test_prepare_binds_and_validates_inputs(dataset, tmp_path):
    output = tmp_path / 'prepared.json'
    assert prepare_dataset(dataset.path, output) == output.resolve()
    prepared = Dataset(output)
    assert prepared.fingerprint == dataset.fingerprint
    with pytest.raises(FileExistsError):
        prepare_dataset(dataset.path, output)


def test_canonical_and_first_cohort_consumer(dataset, tmp_path):
    from codeants_2pf_hcr.manual_identity import export_manual_canonical_tables
    from codeants_2pf_hcr.multifish import annotate_session_anat_label_duplicates
    session = AnnotationSession(dataset, tmp_path / 'session', 'one')
    session.set_call('pth2-r1', 11, 'positive')
    frozen = session.freeze()
    session.close()
    geometry, master, mapping = export_inputs(dataset, tmp_path)
    manual_root = tmp_path / 'manual'
    export_manual_identity(dataset, frozen, mapping, geometry, master, manual_root, ['pth2', 'cort'])
    score_root = tmp_path / 'score'
    score_root.mkdir()
    (score_root / 'functional_roi_activity_identity.csv').write_bytes(master.read_bytes())
    pd.DataFrame(dict(plane_idx=[0, 0, 1], func_label=[1, 2, 1], bpi=[.5, -.2, .4])).to_csv(score_root / 'functional_roi_activity_bpi_cells.csv', index=False)
    pd.DataFrame(dict(n_roi=[3])).to_csv(score_root / 'functional_roi_activity_bpi_summary.csv', index=False)
    output = export_manual_canonical_tables(manual_root, score_root, tmp_path / 'canonical')
    frame = pd.read_csv(output / 'functional_roi_activity_identity.csv')
    # First cohort consumer's identity-preserving geometry deduplication.
    frame['fish_id'] = 'fixture'
    frame['session_label'] = 'r1'
    frame['has_unique_anat_match'] = True
    frame['selected_overlap_px'] = [8, 9, 7]
    frame['selected_dist_um'] = [1., 1., 2.]
    downstream = annotate_session_anat_label_duplicates(frame)
    assert downstream.identity_label.fillna('').tolist() == ['pth2', '', 'pth2']
    assert downstream.bpi.tolist() == [.5, -.2, .4]
    assert not (output / 'conf_to_func_pairs.csv').exists()
    assert json.loads((output / 'manual_identity_export_manifest.json').read_text())['canonical_scope'] == 'roi_only'
    (manual_root / 'conf_to_func_pairs.csv').write_text('fake\n')
    with pytest.raises(ValueError, match='Mixed'):
        export_manual_canonical_tables(manual_root, score_root, tmp_path / 'mixed')


def test_conflicting_marker_replicates_are_not_ored(dataset, tmp_path):
    manifest = dataset.manifest
    repeat = dict(manifest['markers'][0], stack_id='pth2-r2', round_id='r2')
    manifest['markers'].append(repeat)
    manifest['primary_stack_by_gene'] = {'pth2': 'pth2-r1'}
    dataset.path.write_text(json.dumps(manifest))
    dataset = Dataset(dataset.path)
    session = AnnotationSession(dataset, tmp_path / 'session', 'one')
    session.set_call('pth2-r1', 11, 'positive')
    session.set_call('pth2-r2', 11, 'negative')
    snapshot = session.freeze()
    session.close()
    geometry, master, mapping = export_inputs(dataset, tmp_path)
    output = tmp_path / 'manual'
    result = export_manual_identity(dataset, snapshot, mapping, geometry, master, output, [])
    assert not result.has_identity_assigned.any()
    assert 'replicate_conflict' in pd.read_csv(output / 'manual_identity_mapping_audit.csv').outcome.tolist()


def test_native_nrrd_geometry_is_verified(dataset, tmp_path):
    sitk = pytest.importorskip('SimpleITK')
    array = dataset.anatomy
    image = sitk.GetImageFromArray(array)
    image.SetSpacing((1, 1, 2))
    image.SetOrigin((1, 0, 0))
    path = tmp_path / 'offset.nrrd'
    sitk.WriteImage(image, str(path))
    dataset.manifest['anatomy'] = dict(path=str(path), sha256=sha256(path))
    dataset.path.write_text(json.dumps(dataset.manifest))
    with pytest.raises(ValueError, match='origin_xyz'):
        Dataset(dataset.path)


def test_pipeline_manual_source_selection_and_legacy_guard(dataset, tmp_path):
    from codeants_2pf_hcr.pipeline import SingleFishPipelineConfig, run_single_fish_export_canonical_tables_stage
    session = AnnotationSession(dataset, tmp_path / 'session', 'one')
    session.set_call('pth2-r1', 11, 'positive')
    snapshot = session.freeze()
    session.close()
    geometry, master, mapping = export_inputs(dataset, tmp_path)
    manual = tmp_path / 'manual'
    export_manual_identity(dataset, snapshot, mapping, geometry, master, manual, ['pth2'])
    score = tmp_path / 'score'
    score.mkdir()
    (score / 'functional_roi_activity_identity.csv').write_bytes(master.read_bytes())
    for name in ['functional_roi_activity_bpi_cells.csv', 'functional_roi_activity_bpi_summary.csv']:
        (score / name).write_text('n\n3\n')
    config = SingleFishPipelineConfig(fish_id='fixture', local_root=tmp_path, pipeline_root=tmp_path / 'pipeline')
    result = run_single_fish_export_canonical_tables_stage(config, score_input_root=score, manual_identity_root=manual)
    assert result.status == 'warn'
    assert result.parameters['canonical_scope'] == 'roi_only'
    assert not result.errors
    assert all(record.exists for record in result.outputs)
    with pytest.raises(ValueError, match='manual_identity_root'):
        run_single_fish_export_canonical_tables_stage(config, score_input_root=score, hcr_input_root=manual)
    mixed = run_single_fish_export_canonical_tables_stage(config, score_input_root=score, hcr_input_root=manual, manual_identity_root=manual)
    assert mixed.status == 'fail'
    assert 'cannot be combined' in mixed.errors[0]


def test_committed_decision_survives_checkpoint_error_with_history(dataset, tmp_path, monkeypatch):
    session = AnnotationSession(dataset, tmp_path / 'session', 'one')
    def fail():
        raise OSError('checkpoint storage failure')
    monkeypatch.setattr(session, 'snapshot', fail)
    session.set_call('pth2-r1', 11, 'positive')
    assert session.rows[('pth2-r1', 11)]['call'] == 'positive'
    assert len(session.undo_stack) == 1
    assert 'storage failure' in session.last_snapshot_error
    session.undo()
    assert session.rows[('pth2-r1', 11)]['call'] == 'unreviewed'
    session.redo()
    assert session.rows[('pth2-r1', 11)]['call'] == 'positive'
    session.close()
    resumed = AnnotationSession(dataset, tmp_path / 'session', 'two')
    assert resumed.rows[('pth2-r1', 11)]['call'] == 'positive'
    resumed.close()


@pytest.mark.parametrize('bad_id', [11.9, True, '11'])
def test_rejects_noninteger_correspondence_and_eligibility(dataset, tmp_path, bad_id):
    session = AnnotationSession(dataset, tmp_path / 'session', 'one')
    session.set_call('pth2-r1', 11, 'positive')
    frozen = session.freeze()
    session.close()
    geometry, master, mapping_path = export_inputs(dataset, tmp_path)
    mapping = json.loads(mapping_path.read_text())
    mapping['rows'][0]['exvivo_label_id'] = bad_id
    mapping_path.write_text(json.dumps(mapping))
    with pytest.raises(ValueError, match='integer'):
        export_manual_identity(dataset, frozen, mapping_path, geometry, master, tmp_path / 'export', [])
    eligibility_path = Path(dataset.manifest['eligibility']['path'])
    eligibility = json.loads(eligibility_path.read_text())
    eligibility['eligible_label_ids'] = [bad_id, 22]
    eligibility_path.write_text(json.dumps(eligibility))
    dataset.manifest['eligibility']['sha256'] = sha256(eligibility_path)
    dataset.path.write_text(json.dumps(dataset.manifest))
    with pytest.raises(ValueError, match='integer'):
        Dataset(dataset.path)


def test_rejects_missing_geometry_and_score_columns(dataset, tmp_path):
    from codeants_2pf_hcr.manual_identity import export_manual_canonical_tables
    session = AnnotationSession(dataset, tmp_path / 'session', 'one')
    frozen = session.freeze()
    session.close()
    geometry, master, mapping = export_inputs(dataset, tmp_path)
    frame = pd.read_csv(geometry)
    frame['new_geometry'] = [1, 2, 3]
    frame.to_csv(geometry, index=False)
    mapping_data = json.loads(mapping.read_text())
    mapping_data['geometry_sha256'] = sha256(geometry)
    mapping.write_text(json.dumps(mapping_data))
    with pytest.raises(ValueError, match='lacks geometry'):
        export_manual_identity(dataset, frozen, mapping, geometry, master, tmp_path / 'rejected', [])
    frame.drop(columns='new_geometry').to_csv(geometry, index=False)
    mapping_data['geometry_sha256'] = sha256(geometry)
    mapping.write_text(json.dumps(mapping_data))
    manual_root = tmp_path / 'manual'
    export_manual_identity(dataset, frozen, mapping, geometry, master, manual_root, [])
    score_root = tmp_path / 'score'
    score_root.mkdir()
    scored = pd.read_csv(master)
    scored['new_response_field'] = [1, 2, 3]
    scored.to_csv(score_root / 'functional_roi_activity_identity.csv', index=False)
    with pytest.raises(ValueError, match='lacks scored'):
        export_manual_canonical_tables(manual_root, score_root, tmp_path / 'canonical')


def test_identity_genes_rebuilt_and_provenance_is_portable(dataset, tmp_path):
    session = AnnotationSession(dataset, tmp_path / 'session', 'one')
    session.set_call('pth2-r1', 11, 'positive')
    frozen = session.freeze()
    session.close()
    geometry, master, mapping = export_inputs(dataset, tmp_path)
    frame = pd.read_csv(master)
    frame['identity_genes'] = '["legacy"]'
    frame.to_csv(master, index=False)
    output = tmp_path / 'manual'
    result = export_manual_identity(dataset, frozen, mapping, geometry, master, output, ['pth2'])
    assert result.identity_genes.tolist() == [['pth2'], [], ['pth2']]
    manifest = json.loads((output / 'manual_identity_export_manifest.json').read_text())
    for name in ('dataset_manifest.json', 'reviewed_correspondence.json', 'annotations_snapshot.json'):
        assert (output / name).is_file()
        assert manifest['output_sha256'][name] == sha256(output / name)
    audit = pd.read_csv(output / 'manual_identity_mapping_audit.csv')
    assert {'anat_label', 'mapping_status', 'mapping_metrics', 'mapping_reason'} <= set(audit.columns)


def test_journal_failure_blocks_following_mutation_and_freeze_until_reopen(dataset, tmp_path, monkeypatch):
    import codeants_2pf_hcr.manual_identity as module
    path = tmp_path / 'session'
    session = AnnotationSession(dataset, path, 'one')
    with monkeypatch.context() as patch:
        def fail_fsync(fd):
            raise OSError('disk fsync failed')
        patch.setattr(module.os, 'fsync', fail_fsync)
        with pytest.raises(OSError, match='durability is uncertain'):
            session.set_call('pth2-r1', 11, 'positive')
    assert session.journal_failed
    for operation in [lambda: session.set_call('cort-r1', 11, 'positive'),
                      session.undo, session.redo, session.freeze, session.snapshot]:
        with pytest.raises(OSError, match='Close and reopen'):
            operation()
    session.close()
    # The complete surviving event is recovered; no duplicate revision was appended.
    resumed = AnnotationSession(dataset, path, 'two')
    assert resumed.rows[('pth2-r1', 11)]['call'] == 'positive'
    assert resumed.rows[('cort-r1', 11)]['call'] == 'unreviewed'
    resumed.set_call('cort-r1', 11, 'positive')
    assert resumed.revision == 2
    resumed.close()
