"""Independent per-stack manual identity review on fixed ex-vivo masks."""
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
        self.review_eligible = list(self.eligible)
        self.review_scope = None
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
        self.journal_failed = False
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

    def require_writable_journal(self):
        if self.journal_failed:
            raise OSError('Journal durability is uncertain. Close and reopen this session before editing or exporting.')

    def set_call(self, stack, label, call, *, issue='', blocking=False, note=''):
        self.set_calls(stack, [label], call, issue=issue, blocking=blocking, note=note)

    def set_calls(self, stack, labels, call, *, issue='', blocking=False, note=''):
        """Record one call for many cells as a single journal write and undo step."""
        self.require_writable_journal()
        if call not in CALLS:
            raise ValueError('Unknown annotation call')
        labels = [positive_label_id(label) for label in labels]
        if not set(labels).issubset(self.dataset.review_eligible):
            raise ValueError('Cell is outside this fish’s functional review scope')
        if call in ('positive', 'negative'):
            uncovered = self.uncovered_labels(stack, labels)
            if uncovered:
                raise ValueError(f'Incomplete cell coverage for {len(uncovered)} cell(s): use uncertain or unassessable')
        changes = []
        for label in labels:
            key = (stack, label)
            old = self.rows[key].copy()
            changes.append((key, old, dict(old, call=call, issue_code=issue, issue_blocks_export=bool(blocking), note=note)))
        if not changes:
            return
        self._record_many([(key, new) for key, _, new in changes], 'decision')
        self.undo_stack.append(changes)
        self.redo_stack.clear()
        self.checkpoint()

    def uncovered_labels(self, stack, labels):
        """Cells whose mask leaves the stack's valid acquisition domain."""
        cache = self.__dict__.setdefault('_uncovered', {})
        if stack not in cache:
            _, coverage = self.dataset.marker_data(stack)
            cache[stack] = {int(n) for n in np.unique(self.dataset.labels[coverage == 0])} - {0}
        return sorted(set(labels) & cache[stack])

    def _record(self, key, row, action):
        self._record_many([(key, row)], action)

    def _record_many(self, items, action):
        self.require_writable_journal()
        stamp = datetime.now(timezone.utc).isoformat()
        rows, events = [], []
        for offset, (key, row) in enumerate(items, start=1):
            row = dict(row, reviewer=self.reviewer, updated_at_utc=stamp, revision=self.revision + offset)
            self.validate_row(row)
            rows.append((key, row))
            events.append(json.dumps(dict(event_id=uuid.uuid4().hex, revision=row['revision'], action=action,
                                          stack=key[0], label=key[1], old=self.rows[key], new=row)) + '\n')
        try:
            with (self.directory / 'annotation_events.jsonl').open('a') as f:
                f.write(''.join(events))
                f.flush()
                os.fsync(f.fileno())
        except OSError as exc:
            # A complete or partial event may already exist. Never append a second
            # event with the same revision or freeze divergent in-memory state.
            self.journal_failed = True
            raise OSError('Journal durability is uncertain. Close and reopen this session before editing or exporting.') from exc
        for key, row in rows:
            self.rows[key] = row
        self.revision += len(rows)

    def undo(self):
        self.require_writable_journal()
        if self.undo_stack:
            changes = self.undo_stack[-1]
            self._record_many([(key, old) for key, old, _ in changes], 'undo')
            self.undo_stack.pop()
            self.redo_stack.append(changes)
            self.checkpoint()

    def redo(self):
        self.require_writable_journal()
        if self.redo_stack:
            changes = self.redo_stack[-1]
            self._record_many([(key, new) for key, _, new in changes], 'redo')
            self.redo_stack.pop()
            self.undo_stack.append(changes)
            self.checkpoint()

    def save_view(self):
        """Persist view state and pending marks without rewriting the call table."""
        try:
            atomic_write(self.directory / 'session.json', json.dumps(dict(schema_version=1, revision=self.revision,
                         dataset_fingerprint=self.dataset.fingerprint, reviewer=self.reviewer, view=self.view), indent=2))
        except OSError as exc:
            self.last_snapshot_error = str(exc)

    def checkpoint(self):
        try:
            self.snapshot()
        except OSError as exc:
            self.last_snapshot_error = str(exc)
        else:
            self.last_snapshot_error = None

    def snapshot(self):
        self.require_writable_journal()
        frame = pd.DataFrame(self.rows.values())
        atomic_write(self.directory / 'annotations.csv', frame.to_csv(index=False))
        atomic_write(self.directory / 'session.json', json.dumps(dict(schema_version=1, revision=self.revision,
                     dataset_fingerprint=self.dataset.fingerprint, reviewer=self.reviewer, view=self.view), indent=2))

    def freeze(self):
        self.require_writable_journal()
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
