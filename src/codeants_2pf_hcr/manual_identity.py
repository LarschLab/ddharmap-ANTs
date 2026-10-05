"""Independent per-stack manual identity review and geometry-only ROI export."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import socket
from datetime import datetime, timezone
import uuid

import numpy as np
import pandas as pd

CALLS = ('unreviewed', 'positive', 'negative', 'uncertain', 'unassessable')


def positive_label_id(value):
    """Validate exact IDs before normalization; reject fractional values and bools."""
    if isinstance(value, (bool, np.bool_)):
        raise ValueError('Label ID must be a positive integer, not a boolean')
    if isinstance(value, (int, np.integer)):
        if value > 0:
            return int(value)
    elif isinstance(value, (float, np.floating)) and np.isfinite(value) and value > 0 and int(value) == value:
        return int(value)
    raise ValueError('Label ID must be an exact positive integer')


def sha256(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def atomic_write(path, text):
    path = Path(path)
    tmp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    try:
        with tmp.open('w', encoding='utf8') as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def read_volume(path):
    """Read NRRD with physical geometry; .npy uses explicit manifest geometry."""
    path = Path(path)
    if path.suffix == '.npy':
        return np.load(path, allow_pickle=False), None
    if path.suffix.lower() in ('.tif', '.tiff'):
        import tifffile
        return tifffile.imread(path), None
    if path.suffix.lower() != '.nrrd':
        raise ValueError('Use NRRD or explicit-grid NPY/TIFF volumes')
    import SimpleITK as sitk
    image = sitk.ReadImage(str(path))
    if image.GetDimension() != 3:
        raise ValueError('Expected a 3D scalar volume')
    grid = dict(shape_zyx=list(reversed(image.GetSize())),
                spacing_xyz=list(image.GetSpacing()), origin_xyz=list(image.GetOrigin()),
                direction_xyz=list(image.GetDirection()))
    return sitk.GetArrayFromImage(image), grid


class Dataset:
    def __init__(self, manifest_path):
        self.path = Path(manifest_path).resolve()
        self.manifest = json.loads(self.path.read_text())
        m = self.manifest
        for key in ('dataset_id', 'fish_id', 'coordinate_frame', 'grid', 'anatomy', 'labels', 'eligibility', 'markers'):
            if key not in m:
                raise ValueError(f'Missing manifest field: {key}')
        if m.get('schema_version') != 1 or m.get('axes') != 'ZYX' or m.get('units') != 'um':
            raise ValueError('Require schema_version=1, axes=ZYX, units=um')
        grid = m['grid']
        if len(grid['shape_zyx']) != 3 or any(int(n) != n or n <= 0 for n in grid['shape_zyx']):
            raise ValueError('Invalid shape')
        if len(grid['spacing_xyz']) != 3 or not np.all(np.isfinite(grid['spacing_xyz'])) or min(grid['spacing_xyz']) <= 0:
            raise ValueError('Invalid physical spacing')
        direction = np.asarray(grid['direction_xyz'], dtype=float).reshape(3, 3)
        if not np.allclose(direction.T @ direction, np.eye(3)) or len(grid['origin_xyz']) != 3 or not np.all(np.isfinite(grid['origin_xyz'])):
            raise ValueError('Invalid physical direction/origin')
        self.anatomy = self.volume(m['anatomy'])
        self.labels = self.volume(m['labels'])
        if not np.issubdtype(self.labels.dtype, np.integer) or self.labels.min() < 0:
            raise ValueError('Labels must be nonnegative integers')
        eligibility = json.loads(self.asset(m['eligibility']).read_text())
        if not eligibility.get('policy_source'):
            raise ValueError('Eligibility must cite its upstream policy/source')
        self.eligible = [positive_label_id(n) for n in eligibility['eligible_label_ids']]
        if not self.eligible or len(set(self.eligible)) != len(self.eligible) or any(n <= 0 for n in self.eligible):
            raise ValueError('Invalid eligible label inventory')
        if not set(self.eligible).issubset(set(np.unique(self.labels))):
            raise ValueError('Eligible labels absent from label image')
        self.markers = {}
        self._marker_cache = None
        for marker in m['markers']:
            for field in ('stack_id', 'gene', 'round_id', 'source_channel', 'source_sha256', 'transform_provenance', 'image', 'coverage'):
                if field not in marker or marker[field] in ('', None, []):
                    raise ValueError(f'Missing marker field: {field}')
            if marker['stack_id'] in self.markers:
                raise ValueError('Duplicate marker stack ID')
            # Verify every supplied stack now; load intensities on demand afterwards.
            self.volume(marker['image'])
            coverage = self.volume(marker['coverage'])
            if not np.all(np.isin(coverage, (0, 1))):
                raise ValueError('Coverage must be binary')
            self.markers[marker['stack_id']] = marker
        if not self.markers:
            raise ValueError('No marker stacks')
        self.fingerprint = hashlib.sha256(json.dumps(m, sort_keys=True).encode()).hexdigest()

    def asset(self, spec):
        path = Path(spec['path'])
        if not path.is_absolute():
            path = self.path.parent / path
        if sha256(path) != spec['sha256']:
            raise ValueError(f'Input hash mismatch: {path}')
        return path

    def volume(self, spec):
        array, grid = read_volume(self.asset(spec))
        if list(array.shape) != self.manifest['grid']['shape_zyx'] or not np.all(np.isfinite(array)):
            raise ValueError('Volume shape or finite-value check failed')
        if grid is not None:
            for key, values in grid.items():
                if not np.allclose(values, self.manifest['grid'][key], rtol=0, atol=1e-6):
                    raise ValueError(f'Physical grid mismatch: {key}')
        elif spec.get('grid') != self.manifest['grid']:
            raise ValueError('NPY/TIFF inputs require explicit matching grid metadata')
        return array

    def marker_data(self, stack):
        if self._marker_cache is None or self._marker_cache[0] != stack:
            marker = self.markers[stack]
            self._marker_cache = (stack, self.volume(marker['image']), self.volume(marker['coverage']))
        return self._marker_cache[1:]


class AnnotationSession:
    """One-writer session; durable journal owns decisions, snapshots are derived."""
    def __init__(self, dataset, directory, reviewer):
        if not reviewer.strip():
            raise ValueError('Reviewer is required')
        self.dataset = dataset
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.reviewer = reviewer
        self.lock = self.directory / 'writer.lock'
        self.token = uuid.uuid4().hex
        try:
            fd = os.open(self.lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            raise ValueError('Session already locked; inspect writer.lock and recover only after confirming its process is gone') from None
        with os.fdopen(fd, 'w') as f:
            json.dump(dict(pid=os.getpid(), host=socket.gethostname(), token=self.token), f)
        try:
            self._load()
        except BaseException:
            self.close()
            raise

    def _load(self):
        manifest = self.directory / 'dataset_manifest.json'
        if manifest.exists():
            if json.loads(manifest.read_text()) != self.dataset.manifest:
                raise ValueError('Dataset changed; explicit migration required')
        else:
            atomic_write(manifest, json.dumps(self.dataset.manifest, indent=2))
        self.rows = {}
        for stack, marker in self.dataset.markers.items():
            for label in self.dataset.eligible:
                self.rows[(stack, label)] = dict(schema_version=1, dataset_id=self.dataset.manifest['dataset_id'],
                    fish_id=self.dataset.manifest['fish_id'], marker_stack_id=stack, gene=marker['gene'],
                    round_id=marker['round_id'], source_channel=marker['source_channel'], exvivo_label_id=label,
                    call='unreviewed', issue_code='', issue_blocks_export=False, note='', reviewer='', updated_at_utc='', revision=0)
        self.revision = 0
        self.undo_stack, self.redo_stack = [], []
        self.last_snapshot_error = None
        journal = self.directory / 'annotation_events.jsonl'
        if journal.exists():
            data = journal.read_bytes()
            complete_end = data.rfind(b'\n') + 1
            if complete_end != len(data):
                # A power-loss partial final event is not a committed decision.
                atomic_write(self.directory / ('journal-tail-' + uuid.uuid4().hex + '.txt'), data[complete_end:].decode('utf8', errors='replace'))
                with journal.open('r+b') as f:
                    f.truncate(complete_end)
            for line in data[:complete_end].splitlines():
                event = json.loads(line)
                key = (event['stack'], event['label'])
                if event['revision'] != self.revision + 1 or self.rows[key] != event['old']:
                    raise ValueError('Inconsistent annotation journal')
                self.validate_row(event['new'])
                self.rows[key] = event['new']
                self.revision = event['revision']
        self.view = {}
        path = self.directory / 'session.json'
        if path.exists():
            self.view = json.loads(path.read_text()).get('view', {})
        self.snapshot()

    def validate_row(self, row):
        if row['call'] not in CALLS or not isinstance(row['issue_blocks_export'], bool):
            raise ValueError('Invalid call or blocking flag')

    def set_call(self, stack, label, call, *, issue='', blocking=False, note=''):
        if call not in CALLS:
            raise ValueError('Unknown annotation call')
        if call in ('positive', 'negative'):
            _, coverage = self.dataset.marker_data(stack)
            if not np.all(coverage[self.dataset.labels == label]):
                raise ValueError('Incomplete cell coverage: use uncertain or unassessable')
        key = (stack, positive_label_id(label))
        old = self.rows[key].copy()
        new = dict(old, call=call, issue_code=issue, issue_blocks_export=bool(blocking), note=note)
        self._record(key, new, 'decision')
        self.undo_stack.append((key, old, new))
        self.redo_stack.clear()
        self.checkpoint()

    def _record(self, key, row, action):
        row = dict(row, reviewer=self.reviewer, updated_at_utc=datetime.now(timezone.utc).isoformat(), revision=self.revision + 1)
        self.validate_row(row)
        event = dict(event_id=uuid.uuid4().hex, revision=row['revision'], action=action,
                     stack=key[0], label=key[1], old=self.rows[key], new=row)
        with (self.directory / 'annotation_events.jsonl').open('a') as f:
            f.write(json.dumps(event) + '\n')
            f.flush()
            os.fsync(f.fileno())
        self.rows[key] = row
        self.revision += 1

    def undo(self):
        if self.undo_stack:
            key, old, new = self.undo_stack[-1]
            self._record(key, old, 'undo')
            self.undo_stack.pop()
            self.redo_stack.append((key, old, new))
            self.checkpoint()

    def redo(self):
        if self.redo_stack:
            key, old, new = self.redo_stack[-1]
            self._record(key, new, 'redo')
            self.redo_stack.pop()
            self.undo_stack.append((key, old, new))
            self.checkpoint()

    def checkpoint(self):
        try:
            self.snapshot()
        except OSError as exc:
            self.last_snapshot_error = str(exc)
        else:
            self.last_snapshot_error = None

    def snapshot(self):
        frame = pd.DataFrame(self.rows.values())
        atomic_write(self.directory / 'annotations.csv', frame.to_csv(index=False))
        atomic_write(self.directory / 'session.json', json.dumps(dict(schema_version=1, revision=self.revision,
                     dataset_fingerprint=self.dataset.fingerprint, reviewer=self.reviewer, view=self.view), indent=2))

    def freeze(self):
        path = self.directory / f'annotations-r{self.revision:06d}.json'
        contents = json.dumps(dict(schema_version=1, dataset_fingerprint=self.dataset.fingerprint,
            manifest=self.dataset.manifest, revision=self.revision, annotations=list(self.rows.values())), indent=2)
        if path.exists() and path.read_text() != contents:
            raise ValueError('Frozen snapshot already exists with different contents')
        atomic_write(path, contents)
        return path

    def close(self):
        if self.lock.exists() and json.loads(self.lock.read_text()).get('token') == self.token:
            self.lock.unlink()


def export_manual_identity(dataset, frozen_path, correspondence_path, geometry_path, roi_master_path, output_dir, gene_order):
    """Produce isolated ROI-compatible tables; never fabricate HCR-centric labels."""
    from .matching import attach_identity_to_functional_roi_geometry_df
    frozen_bytes = Path(frozen_path).read_bytes()
    frozen = json.loads(frozen_bytes)
    if frozen['dataset_fingerprint'] != dataset.fingerprint or frozen['manifest'] != dataset.manifest:
        raise ValueError('Frozen annotation dataset mismatch')
    for row in frozen['annotations']:
        row['exvivo_label_id'] = positive_label_id(row['exvivo_label_id'])
    rows = pd.DataFrame(frozen['annotations'])
    expected = {(s, n) for s in dataset.markers for n in dataset.eligible}
    keys = list(zip(rows.marker_stack_id, rows.exvivo_label_id))
    if len(keys) != len(set(keys)) or set(keys) != expected or not rows.call.isin(CALLS).all():
        raise ValueError('Invalid snapshot annotation inventory')
    for row in frozen['annotations']:
        marker = dataset.markers[row['marker_stack_id']]
        if row['dataset_id'] != dataset.manifest['dataset_id'] or row['gene'] != marker['gene'] or row['fish_id'] != dataset.manifest['fish_id'] or not isinstance(row['issue_blocks_export'], bool):
            raise ValueError('Invalid annotation provenance')
    correspondence_bytes = Path(correspondence_path).read_bytes()
    mapping = json.loads(correspondence_bytes)
    for row in mapping['rows']:
        row['exvivo_label_id'] = positive_label_id(row['exvivo_label_id'])
        if row['status'] == 'accepted':
            row['anat_label'] = positive_label_id(row['anat_label'])
    for key, expected_value in [('schema_version', 1), ('fish_id', dataset.manifest['fish_id']),
                                ('exvivo_labels_sha256', dataset.manifest['labels']['sha256']),
                                ('geometry_sha256', sha256(geometry_path))]:
        if mapping.get(key) != expected_value:
            raise ValueError(f'Correspondence binding mismatch: {key}')
    for key in ('invivo_labels_sha256', 'transform_provenance', 'reviewer', 'reviewed_at_utc'):
        if not mapping.get(key):
            raise ValueError(f'Missing correspondence provenance: {key}')
    target_spec = mapping.get('invivo_labels')
    if not target_spec or target_spec.get('sha256') != mapping['invivo_labels_sha256']:
        raise ValueError('Correspondence requires the exact in-vivo label asset')
    target_path = Path(target_spec['path'])
    if not target_path.is_absolute():
        target_path = Path(correspondence_path).resolve().parent / target_path
    if sha256(target_path) != target_spec['sha256']:
        raise ValueError('In-vivo label asset hash mismatch')
    target_labels, _ = read_volume(target_path)
    if target_labels.ndim != 3 or not np.issubdtype(target_labels.dtype, np.integer):
        raise ValueError('Invalid in-vivo label image')
    target_ids = set(np.unique(target_labels)) - {0}
    accepted = [r for r in mapping['rows'] if r['status'] == 'accepted']
    if any(r['anat_label'] not in target_ids for r in accepted):
        raise ValueError('Correspondence target absent from in-vivo labels')
    if any(not r.get('metrics') for r in accepted):
        raise ValueError('Accepted correspondence requires geometry metrics')
    if len({r['exvivo_label_id'] for r in mapping['rows']}) != len(mapping['rows']):
        raise ValueError('Duplicate correspondence source')
    if len({r['anat_label'] for r in accepted}) != len(accepted):
        raise ValueError('Competing ex-vivo cells require geometry resolution')
    links = {int(r['exvivo_label_id']): int(r['anat_label']) for r in accepted}
    if any(n not in dataset.eligible or target <= 0 for n, target in links.items()):
        raise ValueError('Invalid correspondence label')
    geometry = pd.read_csv(geometry_path)
    master = pd.read_csv(roi_master_path)
    if not set(pd.to_numeric(geometry['anat_label'], errors='raise').dropna().astype(int)).issubset(target_ids):
        raise ValueError('ROI geometry refers to labels absent from in-vivo label image')
    roi_keys = ['plane_idx', 'func_label']
    if geometry.duplicated(roi_keys).any() or master.duplicated(roi_keys).any():
        raise ValueError('Duplicate ROI keys')
    if set(map(tuple, geometry[roi_keys].to_numpy())) != set(map(tuple, master[roi_keys].to_numpy())):
        raise ValueError('ROI inventory differs')
    missing_geometry = set(geometry.columns) - set(master.columns)
    if missing_geometry:
        raise ValueError(f'ROI master lacks geometry columns: {sorted(missing_geometry)}')
    # Protect every common geometry column against a mismatched master.
    common = [c for c in geometry if c in master and c not in roi_keys]
    left = geometry.set_index(roi_keys).sort_index()[common]
    right = master.set_index(roi_keys).sort_index()[common]
    pd.testing.assert_frame_equal(left, right, check_dtype=False)
    mapping_rows = {r['exvivo_label_id']: r for r in mapping['rows']}
    for stack, group in rows.groupby('marker_stack_id'):
        assessed_ids = set(group.loc[group.call.isin(['positive', 'negative']), 'exvivo_label_id'])
        if assessed_ids:
            _, coverage = dataset.marker_data(stack)
            uncovered_ids = set(np.unique(dataset.labels[coverage == 0])) - {0}
            if assessed_ids & uncovered_ids:
                raise ValueError('Frozen positive/negative calls have incomplete cell coverage')
    primary = dataset.manifest.get('primary_stack_by_gene', {})
    memberships, audit = {}, []
    for (label, gene), group in rows.groupby(['exvivo_label_id', 'gene']):
        chosen = primary.get(gene)
        if len(group) == 1 and chosen is None:
            chosen = group.iloc[0].marker_stack_id
        if chosen not in set(group.marker_stack_id):
            raise ValueError(f'Declare primary_stack_by_gene for {gene}')
        decision = group.loc[group.marker_stack_id == chosen].iloc[0]
        assessed = set(group.loc[group.call.isin(['positive', 'negative']), 'call'])
        reason = 'not_positive'
        if len(assessed) > 1:
            reason = 'replicate_conflict'
        elif decision.issue_blocks_export:
            reason = 'blocking_issue'
        elif decision.call == 'positive':
            if label not in links:
                reason = 'unmapped_or_ambiguous'
            else:
                reason = 'exported'
                memberships.setdefault(links[label], set()).add(gene)
        correspondence = mapping_rows.get(label, {})
        audit.append(dict(exvivo_label_id=int(label), gene=gene, primary_stack_id=chosen, outcome=reason,
            anat_label=correspondence.get('anat_label'), mapping_status=correspondence.get('status', 'missing'),
            mapping_reason=correspondence.get('reason', ''), mapping_metrics=json.dumps(correspondence.get('metrics', {}))))
    order = {gene: i for i, gene in enumerate(gene_order)}
    lookup_rows = []
    for label, genes in sorted(memberships.items()):
        genes = sorted(genes, key=lambda g: (order.get(g, 10**6), g))
        lookup_rows.append(dict(anat_label=label, identity_label='/'.join(genes), identity_gene_count=len(genes), identity_genes=genes))
    lookup = pd.DataFrame(lookup_rows, columns=['anat_label', 'identity_label', 'identity_gene_count', 'identity_genes'])
    result = attach_identity_to_functional_roi_geometry_df(geometry, lookup, passthrough_df=master)
    if 'identity_genes' in master.columns:
        by_label = {r['anat_label']: r['identity_genes'] for r in lookup_rows}
        result['identity_genes'] = result['anat_label'].map(lambda label: by_label.get(label, []))
    output = Path(output_dir)
    # New directory is mandatory: no canonical overwrite or accidental source mixing.
    output.mkdir(parents=True, exist_ok=False)
    lookup.to_csv(output / 'anatomy_identity_lookup.csv', index=False)
    result.to_csv(output / 'functional_roi_activity_identity.csv', index=False)
    pd.DataFrame(audit).to_csv(output / 'manual_identity_mapping_audit.csv', index=False)
    rows.to_csv(output / 'manual_annotations.csv', index=False)
    atomic_write(output / 'dataset_manifest.json', json.dumps(dataset.manifest, indent=2))
    (output / 'reviewed_correspondence.json').write_bytes(correspondence_bytes)
    (output / 'annotations_snapshot.json').write_bytes(frozen_bytes)
    provenance = dict(schema_version=1, fish_id=dataset.manifest['fish_id'], identity_source='manual_exvivo', dataset_fingerprint=dataset.fingerprint,
        annotations_sha256=hashlib.sha256(frozen_bytes).hexdigest(), correspondence_sha256=hashlib.sha256(correspondence_bytes).hexdigest(),
        geometry_sha256=sha256(geometry_path), roi_master_sha256=sha256(roi_master_path), gene_order=gene_order,
        source_dataset_manifest=str(dataset.path), source_annotations_snapshot=str(Path(frozen_path).resolve()),
        source_correspondence=str(Path(correspondence_path).resolve()),
        hcr_centric_supported=False, hcr_centric_reason='Manual assignments do not identify confocal segmented labels')
    provenance['output_sha256'] = {p.name: sha256(p) for p in output.iterdir() if p.is_file()}
    atomic_write(output / 'manual_identity_export_manifest.json', json.dumps(provenance, indent=2))
    return result


def prepare_dataset(config_path, output_path):
    """Bind explicitly registered assets to content hashes; never infer alignment."""
    config_path, output_path = Path(config_path).resolve(), Path(output_path).resolve()
    config = json.loads(config_path.read_text())
    if output_path.exists():
        raise FileExistsError(output_path)
    def bind(spec):
        spec = dict(spec)
        path = Path(spec['path'])
        if not path.is_absolute():
            path = config_path.parent / path
        spec['path'] = str(path.resolve())
        spec['sha256'] = sha256(path)
        return spec
    for field in ('anatomy', 'labels', 'eligibility'):
        config[field] = bind(config[field])
    for marker in config['markers']:
        for field in ('image', 'coverage'):
            marker[field] = bind(marker[field])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    # Validate before exposing the prepared manifest.
    temporary = output_path.with_name(output_path.name + '.' + uuid.uuid4().hex + '.validation')
    try:
        temporary.write_text(json.dumps(config, indent=2))
        Dataset(temporary)
        atomic_write(output_path, json.dumps(config, indent=2))
    finally:
        temporary.unlink(missing_ok=True)
    return output_path


def export_manual_canonical_tables(manual_root, score_root, output):
    """Assemble ROI-only canonical exports; HCR-dependent consumers fail closed."""
    import shutil
    manual_root, score_root, output = Path(manual_root), Path(score_root), Path(output)
    manifest = json.loads((manual_root / 'manual_identity_export_manifest.json').read_text())
    if manifest.get('identity_source') != 'manual_exvivo' or manifest.get('hcr_centric_supported') is not False:
        raise ValueError('Expected an isolated manual identity export')
    for name, digest in manifest['output_sha256'].items():
        if sha256(manual_root / name) != digest:
            raise ValueError(f'Manual export changed: {name}')
    identity = pd.read_csv(manual_root / 'functional_roi_activity_identity.csv')
    scored = pd.read_csv(score_root / 'functional_roi_activity_identity.csv')
    keys = ['plane_idx', 'func_label']
    identity_columns = {'identity_label', 'identity_gene_count', 'identity_genes', 'has_identity_assigned', 'identity_display_label'}
    if identity.duplicated(keys).any() or scored.duplicated(keys).any():
        raise ValueError('Duplicate canonical ROI keys')
    missing_scored = set(scored.columns) - identity_columns - set(identity.columns)
    if missing_scored:
        raise ValueError(f'Manual ROI master lacks scored columns: {sorted(missing_scored)}')
    common = [c for c in scored if c in identity and c not in identity_columns and c not in keys]
    pd.testing.assert_frame_equal(identity.set_index(keys).sort_index()[common],
                                  scored.set_index(keys).sort_index()[common], check_dtype=False)
    score_names = ['functional_roi_activity_bpi_cells.csv', 'functional_roi_activity_bpi_summary.csv']
    for name in score_names:
        if not (score_root / name).is_file():
            raise FileNotFoundError(score_root / name)
    if any((manual_root / name).exists() for name in ['hcr_activity_status.csv', 'conf_to_func_pairs.csv', 'hcr_func_candidates.csv']):
        raise ValueError('Mixed HCR-centric and manual identity sources')
    output.mkdir(parents=True, exist_ok=False)
    for name in ['functional_roi_activity_identity.csv', 'anatomy_identity_lookup.csv', 'manual_annotations.csv', 'manual_identity_mapping_audit.csv', 'dataset_manifest.json', 'annotations_snapshot.json', 'reviewed_correspondence.json']:
        shutil.copyfile(manual_root / name, output / name)
    for name in score_names:
        shutil.copyfile(score_root / name, output / name)
    manifest = dict(manifest, source_manual_root=str(manual_root.resolve()), source_score_root=str(score_root.resolve()),
                    canonical_scope='roi_only', unsupported_consumers=['HCR-centric trace/status exports', 'HCR-dependent compound figures'])
    manifest['output_sha256'] = {p.name: sha256(p) for p in output.iterdir() if p.is_file()}
    atomic_write(output / 'manual_identity_export_manifest.json', json.dumps(manifest, indent=2))
    return output


def recover_session_lock(directory, expected_pid):
    """Explicit recovery of a dead local writer; never take over a live session."""
    path = Path(directory) / 'writer.lock'
    lock = json.loads(path.read_text())
    if lock['host'] != socket.gethostname() or lock['pid'] != expected_pid or expected_pid <= 0:
        raise ValueError('Writer host/PID does not match recovery request')
    try:
        os.kill(expected_pid, 0)
    except ProcessLookupError:
        if json.loads(path.read_text()) != lock:
            raise ValueError('Lock changed during recovery')
        path.unlink()
    else:
        raise ValueError('Writer process still exists; close it before recovery')
