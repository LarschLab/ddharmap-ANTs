import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from codeants_2pf_hcr.manual_identity import Dataset, AnnotationSession, prepare_dataset, sha256


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


def test_prepare_binds_and_validates_inputs(dataset, tmp_path):
    output = tmp_path / 'prepared.json'
    assert prepare_dataset(dataset.path, output) == output.resolve()
    prepared = Dataset(output)
    assert prepared.fingerprint == dataset.fingerprint
    with pytest.raises(FileExistsError):
        prepare_dataset(dataset.path, output)


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
def test_rejects_noninteger_eligibility(dataset, bad_id):
    eligibility_path = Path(dataset.manifest['eligibility']['path'])
    eligibility = json.loads(eligibility_path.read_text())
    eligibility['eligible_label_ids'] = [bad_id, 22]
    eligibility_path.write_text(json.dumps(eligibility))
    dataset.manifest['eligibility']['sha256'] = sha256(eligibility_path)
    dataset.path.write_text(json.dumps(dataset.manifest))
    with pytest.raises(ValueError, match='integer'):
        Dataset(dataset.path)


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
