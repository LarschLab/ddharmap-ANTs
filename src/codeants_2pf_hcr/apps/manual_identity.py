"""Napari viewer for fixed-mask, independent per-marker identity annotation."""
from __future__ import annotations

import argparse
from pathlib import Path

from codeants_2pf_hcr.manual_identity import AnnotationSession, CALLS, Dataset


def create_viewer(dataset, session, *, show=True, controls=None):
    # GUI dependencies stay optional for headless pipeline consumers.
    import napari
    import numpy as np
    from scipy import ndimage
    from qtpy.QtCore import Qt
    from qtpy.QtGui import QImage, QPixmap
    from qtpy.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel, QComboBox,
        QPushButton, QLineEdit, QCheckBox, QMessageBox, QScrollArea)

    viewer = napari.Viewer(title=f"Manual HCR identity — {dataset.manifest['fish_id']}", show=show)
    spacing = tuple(reversed(dataset.manifest['grid']['spacing_xyz']))
    anatomy = viewer.add_image(dataset.anatomy, name='Ex-vivo GCaMP', colormap='gray', scale=spacing)
    stack_ids = list(dataset.markers)
    first_stack = session.view.get('stack', stack_ids[0])
    if first_stack not in stack_ids:
        first_stack = stack_ids[0]
    intensity, coverage = dataset.marker_data(first_stack)
    marker_layer = viewer.add_image(intensity, name='HCR: ' + first_stack, colormap='magenta', blending='additive', scale=spacing)
    coverage_layer = viewer.add_image((coverage == 0).astype(np.uint8), name='Outside valid coverage',
                                     colormap='cyan', blending='additive', opacity=0.25, scale=spacing,
                                     contrast_limits=(0, 1))
    labels = viewer.add_labels(dataset.labels, name='Fixed anatomy masks', scale=spacing, opacity=0.2)
    labels.editable = False
    labels.contour = 1
    selected = viewer.add_labels(np.zeros_like(dataset.labels), name='Selected cell', scale=spacing, opacity=0.8)
    selected.editable = False
    selected.contour = 1
    centers = ndimage.center_of_mass(np.ones(dataset.labels.shape, dtype=np.uint8), dataset.labels, dataset.eligible)
    centers = {label: tuple(int(round(v)) for v in center) for label, center in zip(dataset.eligible, centers)}
    state = {'label': session.view.get('label', dataset.eligible[0]), 'stack': first_stack, 'updating': False}
    panel = QWidget()
    layout = QVBoxLayout(panel)
    stack_select = QComboBox()
    for stack in stack_ids:
        m = dataset.markers[stack]
        stack_select.addItem(f"{m['gene']} | {m['round_id']} | {stack}", stack)
    stack_select.setCurrentIndex(stack_ids.index(first_stack))
    filter_select = QComboBox()
    filter_select.addItems(['unreviewed', 'all', *CALLS[1:], 'issue-flagged'])
    filter_select.setCurrentText(session.view.get('filter', 'unreviewed'))
    cell_select = QComboBox()
    status = QLabel()
    status.setWordWrap(True)
    issue = QComboBox()
    issue.addItems(['', 'merged_anatomy_label', 'registration_mismatch', 'poor_coverage', 'bleed_through', 'other'])
    blocking = QCheckBox('Issue blocks identity export')
    note = QLineEdit()
    note.setPlaceholderText('Optional evidence note')
    intro = QLabel('One marker stack at a time; existing identities and functional data are hidden.')
    intro.setWordWrap(True)
    layout.addWidget(intro)
    layout.addWidget(stack_select)
    layout.addWidget(filter_select)
    layout.addWidget(cell_select)
    layout.addWidget(status)
    layout.addWidget(issue)
    layout.addWidget(blocking)
    layout.addWidget(note)
    call_buttons = {}
    for call in CALLS:
        button = QPushButton(call.title())
        layout.addWidget(button)
        call_buttons[call] = button
    previous, next_cell = QPushButton('Previous cell'), QPushButton('Next cell')
    nav = QHBoxLayout()
    nav.addWidget(previous)
    nav.addWidget(next_cell)
    layout.addLayout(nav)
    undo, redo, freeze = QPushButton('Undo'), QPushButton('Redo'), QPushButton('Freeze annotation snapshot')
    for button in (undo, redo, freeze):
        layout.addWidget(button)
    xz, yz = QLabel(), QLabel()
    xz.setMinimumSize(260, 130)
    yz.setMinimumSize(260, 130)
    layout.addWidget(QLabel('XZ / YZ through selected cell (GCaMP + current HCR)'))
    layout.addWidget(xz)
    layout.addWidget(yz)
    layout.addStretch()
    scroll = QScrollArea()
    scroll.setWidgetResizable(True)
    scroll.setMinimumWidth(490)
    scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
    scroll.setWidget(panel)
    viewer.window.add_dock_widget(scroll, name='Identity review', area='right')

    def orthogonal_pixmap(a, h, mask, scale_y, scale_x):
        def norm(array, limits):
            lo, hi = limits
            return np.clip((array.astype(float) - lo) / max(hi - lo, 1e-9), 0, 1)
        a = norm(a, anatomy.contrast_limits)
        h = norm(h, marker_layer.contrast_limits) * marker_layer.opacity
        if not marker_layer.visible:
            h = h * 0
        if not anatomy.visible:
            a = a * 0
        rgb = np.stack([np.clip(a + h, 0, 1), a, np.clip(a + h, 0, 1)], axis=-1)
        edge = mask & ~ndimage.binary_erosion(mask)
        rgb[edge] = (1, 1, 0)
        rgb = np.ascontiguousarray((rgb * 255).astype(np.uint8))
        image = QImage(rgb.data, rgb.shape[1], rgb.shape[0], rgb.strides[0], QImage.Format_RGB888).copy()
        width = 290
        height = max(1, int(width * rgb.shape[0] * scale_y / (rgb.shape[1] * scale_x)))
        return QPixmap.fromImage(image).scaled(width, height, Qt.IgnoreAspectRatio, Qt.SmoothTransformation)

    def update_orthos(*_):
        z, y, x = centers[state['label']]
        mask = dataset.labels == state['label']
        # Crop with substantial neighboring context; physical aspect ratio is retained.
        ys, xs = slice(max(0, y - 60), y + 61), slice(max(0, x - 60), x + 61)
        zs = slice(max(0, z - 30), z + 31)
        xz.setPixmap(orthogonal_pixmap(dataset.anatomy[zs, y, xs], marker_layer.data[zs, y, xs], mask[zs, y, xs], spacing[0], spacing[2]))
        yz.setPixmap(orthogonal_pixmap(dataset.anatomy[zs, ys, x], marker_layer.data[zs, ys, x], mask[zs, ys, x], spacing[0], spacing[1]))

    def select_cell(label):
        if label not in centers:
            return
        state['label'] = label
        selected.data = np.where(dataset.labels == label, 1, 0).astype(np.uint8)
        labels.selected_label = label
        z, y, x = centers[label]
        viewer.dims.set_point(0, z * spacing[0])
        viewer.camera.center = (z * spacing[0], y * spacing[1], x * spacing[2])
        row = session.rows[(state['stack'], label)]
        issue.setCurrentText(row['issue_code'])
        blocking.setChecked(row['issue_blocks_export'])
        note.setText(row['note'])
        refresh_status()
        update_orthos()
        save_view()

    def refresh_status():
        rows = [r for (stack, _), r in session.rows.items() if stack == state['stack']]
        counts = {call: sum(r['call'] == call for r in rows) for call in CALLS}
        current = session.rows[(state['stack'], state['label'])]['call']
        status.setText(f"Cell {state['label']}: {current}\n" + ', '.join(f'{call}: {n}' for call, n in counts.items()))

    def refresh_queue(*_):
        choice = filter_select.currentText()
        cell_select.blockSignals(True)
        cell_select.clear()
        for label in dataset.eligible:
            row = session.rows[(state['stack'], label)]
            if choice == 'all' or row['call'] == choice or (choice == 'issue-flagged' and row['issue_code']):
                cell_select.addItem(str(label), label)
        ix = cell_select.findData(state['label'])
        cell_select.setCurrentIndex(max(0, ix))
        cell_select.blockSignals(False)
        for button in call_buttons.values():
            button.setEnabled(bool(cell_select.count()) and not session.journal_failed)
        if cell_select.count():
            select_cell(cell_select.currentData())
        else:
            refresh_status()
            status.setText(status.text() + '\nNo cells in this queue.')

    def save_view():
        session.view.update(stack=state['stack'], label=state['label'], filter=filter_select.currentText(),
            anatomy_contrast=list(anatomy.contrast_limits), zoom=float(viewer.camera.zoom))
        session.view.setdefault('stacks', {})[state['stack']] = dict(
            contrast=list(marker_layer.contrast_limits), opacity=float(marker_layer.opacity))
        session.checkpoint()

    def change_stack(*_):
        save_view()
        state['stack'] = stack_select.currentData()
        data, valid = dataset.marker_data(state['stack'])
        marker_layer.data = data
        marker_layer.name = 'HCR: ' + state['stack']
        settings = session.view.get('stacks', {}).get(state['stack'])
        if settings:
            marker_layer.contrast_limits = settings['contrast']
            marker_layer.opacity = settings['opacity']
        else:
            marker_layer.reset_contrast_limits()
            marker_layer.opacity = 1
        coverage_layer.data = (valid == 0).astype(np.uint8)
        refresh_queue()
        update_orthos()

    def decision(call):
        if not cell_select.count():
            return
        try:
            session.set_call(state['stack'], state['label'], call, issue=issue.currentText(),
                             blocking=blocking.isChecked(), note=note.text())
            refresh_queue()
            report_checkpoint()
        except (ValueError, OSError) as exc:
            if session.journal_failed:
                for button in [*call_buttons.values(), undo, redo, freeze]:
                    button.setEnabled(False)
            title = 'Session needs reopening' if session.journal_failed else 'Decision not saved'
            QMessageBox.warning(panel, title, str(exc))

    def move(direction):
        if cell_select.count():
            cell_select.setCurrentIndex((cell_select.currentIndex() + direction) % cell_select.count())

    def report_checkpoint():
        if session.last_snapshot_error:
            QMessageBox.warning(panel, 'Decision saved to journal',
                'The decision is saved in the journal, but CSV/session checkpoints could not be updated. '
                'Resolve the storage problem before continuing: ' + session.last_snapshot_error)

    def history(action):
        try:
            getattr(session, action)()
            refresh_queue()
            report_checkpoint()
        except OSError as exc:
            QMessageBox.warning(panel, 'History could not be saved', str(exc))

    def freeze_snapshot():
        save_view()
        try:
            QMessageBox.information(panel, 'Frozen annotations', str(session.freeze()))
        except (ValueError, OSError) as exc:
            QMessageBox.warning(panel, 'Snapshot could not be written', str(exc))

    @labels.mouse_drag_callbacks.append
    def click_mask(layer, event):
        if event.type == 'mouse_press' and event.button == 1:
            label = layer.get_value(event.position, world=True)
            if label is not None and int(label) in centers:
                select_cell(int(label))

    stack_select.currentIndexChanged.connect(change_stack)
    filter_select.currentIndexChanged.connect(refresh_queue)
    cell_select.currentIndexChanged.connect(lambda *_: select_cell(cell_select.currentData()))
    for call, button in call_buttons.items():
        button.clicked.connect(lambda checked=False, call=call: decision(call))
    previous.clicked.connect(lambda: move(-1))
    next_cell.clicked.connect(lambda: move(1))
    undo.clicked.connect(lambda: history('undo'))
    redo.clicked.connect(lambda: history('redo'))
    freeze.clicked.connect(freeze_snapshot)
    for key, call in zip(('1', '2', '3', '4', '5'), CALLS):
        viewer.bind_key(key, lambda v, call=call: decision(call), overwrite=True)
    viewer.bind_key('n', lambda v: move(1), overwrite=True)
    viewer.bind_key('p', lambda v: move(-1), overwrite=True)
    for layer in (anatomy, marker_layer):
        layer.events.contrast_limits.connect(update_orthos)
        layer.events.opacity.connect(update_orthos)
        layer.events.visible.connect(update_orthos)
    if session.view.get('anatomy_contrast'):
        anatomy.contrast_limits = session.view['anatomy_contrast']
    settings = session.view.get('stacks', {}).get(first_stack)
    if settings:
        marker_layer.contrast_limits = settings['contrast']
        marker_layer.opacity = settings['opacity']
    if session.view.get('zoom'):
        viewer.camera.zoom = session.view['zoom']
    refresh_queue()
    viewer.layers.selection.active = labels
    # Expose controls for deterministic GUI checks without requiring manual clicks.
    if controls is None:
        controls = {}
    controls.update(dict(panel=panel, stack=stack_select, queue=filter_select,
        cells=cell_select, calls=call_buttons, undo=undo, redo=redo, freeze=freeze, status=status, save_view=save_view, orthogonal_labels=(xz, yz)))
    return viewer


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--session', type=Path, required=True)
    parser.add_argument('--reviewer', required=True)
    args = parser.parse_args(argv)
    dataset = Dataset(args.manifest)
    session = AnnotationSession(dataset, args.session, args.reviewer)
    try:
        import napari
        controls = {}
        viewer = create_viewer(dataset, session, controls=controls)
        napari.run()
        controls['save_view']()
    finally:
        session.close()
    return 0
