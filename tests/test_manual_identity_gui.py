"""Real Qt widget interactions on a deterministic three-dimensional fixture."""
import os

import pytest

pytest.importorskip('napari')

from codeants_2pf_hcr.manual_identity import AnnotationSession
from codeants_2pf_hcr.apps.manual_identity import create_viewer
from test_manual_identity import make_dataset


def test_viewer_mark_approve_workflow_and_controls(tmp_path):
    import json
    import numpy as np
    from qtpy.QtWidgets import QApplication, QDockWidget, QPushButton
    dataset = make_dataset(tmp_path / 'data')
    session = AnnotationSession(dataset, tmp_path / 'session', 'GUI reviewer')
    controls = {}
    viewer = create_viewer(dataset, session, show=bool(os.environ.get('MANUAL_IDENTITY_SCREENSHOT')), controls=controls)
    try:
        app = QApplication.instance()
        assert not viewer.layers['Ex-vivo masks'].editable
        # Selecting never moves the view.
        center = tuple(viewer.camera.center)
        controls['select_cell'](22)
        assert tuple(viewer.camera.center) == center
        assert 'Selected cell 22' in controls['header'].text()
        # Click-to-mark toggles pending marks, persisted without new calls.
        controls['toggle_mark'](11)
        controls['toggle_mark'](22)
        controls['toggle_mark'](22)
        assert controls['pending']['pth2-r1'] == {11}
        assert json.loads((tmp_path / 'session/session.json').read_text())['view']['pending'] == {'pth2-r1': [11]}
        assert session.revision == 0 and 'Approve 1 as positive' in controls['approve_button'].text()
        controls['approve']('positives')
        assert session.rows[('pth2-r1', 11)]['call'] == 'positive'
        assert session.rows[('pth2-r1', 22)]['call'] == 'unreviewed'
        assert session.rows[('cort-r1', 11)]['call'] == 'unreviewed'
        assert controls['pending']['pth2-r1'] == set()
        # Marker stacks stay independent; one approval is one undo step.
        controls['stack'].setCurrentIndex(1)
        controls['toggle_mark'](11)
        controls['approve']('with_negatives')
        assert session.rows[('cort-r1', 11)]['call'] == 'positive'
        assert session.rows[('cort-r1', 22)]['call'] == 'negative'
        controls['undo'].click()
        assert session.rows[('cort-r1', 22)]['call'] == 'unreviewed'
        controls['undo'].click()
        assert session.rows[('cort-r1', 11)]['call'] == 'unreviewed'
        controls['redo'].click()
        assert session.rows[('cort-r1', 11)]['call'] == 'positive'
        # Single-cell call still records uncertain/unassessable for the selected cell.
        controls['select_cell'](22)
        controls['detail'].setChecked(True)
        controls['calls']['uncertain'].click()
        assert session.rows[('cort-r1', 22)]['call'] == 'uncertain'
        # Histogram auto contrast stays within the stack's intensities.
        hist = controls['histogram']
        hist.controls['auto'].click()
        low, high = viewer.layers['HCR: cort-r1'].contrast_limits
        data = dataset.marker_data('cort-r1')[0]
        assert data.min() <= low < high <= data.max()
        # Our panels cannot be closed; every dock can be restored.
        docks = viewer.window._qt_window.findChildren(QDockWidget)
        ours = [d for d in docks if d.windowTitle() in ('Identity review', 'HCR contrast')]
        assert len(ours) == 2 and not any(d.features() & QDockWidget.DockWidgetClosable for d in ours)
        panels = [d for d in docks if 'console' not in d.windowTitle().lower()]
        for dock in docks:
            dock.hide()
        controls['restore_panels']()
        assert all(not d.isHidden() for d in panels) and len(panels) < len(docks) + 1
        # Every review button explains itself.
        controls['detail'].setChecked(True)
        assert all(b.toolTip() for b in controls['panel'].findChildren(QPushButton))
        # Side views span the full depth across the visible area; Z spacing is twice XY.
        controls['update_orthos']()
        bounds = controls['ortho_state']['ortho']
        widths = (bounds['x1'] - bounds['x0'], bounds['y1'] - bounds['y0'])
        for label, width in zip(controls['orthogonal_labels'], widths):
            pixmap = label.pixmap()
            assert pixmap.height() / pixmap.width() == pytest.approx(8 * 2 / width, rel=.05)
        # Zooming does not rescale the side views.
        sizes = [label.pixmap().size() for label in controls['orthogonal_labels']]
        for zoom in (0.5, 3.0, 7.0):
            viewer.camera.zoom = zoom
            app.processEvents()
            controls['update_orthos']()
            assert [label.pixmap().size() for label in controls['orthogonal_labels']] == sizes
        # Clicking a side view jumps to that Z slice and moves the other view's cut.
        controls['pick_ortho']('xz', 0.5, 0.99)
        assert viewer.dims.current_step[0] == 7
        assert controls['ortho_state']['cursor'][1] == bounds['x0'] + widths[0] // 2
        controls['pick_ortho']('yz', 0.0, 0.0)
        assert viewer.dims.current_step[0] == 0 and controls['ortho_state']['cursor'][0] == bounds['y0']
        # Right-click on a cell cuts the side views through its centre without moving Z or the view.
        z_before, center = viewer.dims.current_step[0], tuple(viewer.camera.center)
        controls['cut_through'](None, 22)
        cell_z, cell_y, cell_x = [int(v) for v in np.argwhere(dataset.labels == 22).mean(0).round()]
        assert controls['ortho_state']['cursor'] == [cell_y, cell_x]
        assert viewer.dims.current_step[0] == z_before and tuple(viewer.camera.center) == center
        # The image crosshair leaves the cell at its centre uncovered.
        segments = viewer.layers['Side-view crosshair'].data
        assert segments
        for seg in segments:
            assert min(np.hypot(*(pt - [cell_y, cell_x])) for pt in seg) >= 14
        path = os.environ.get('MANUAL_IDENTITY_SCREENSHOT')
        if path:
            controls['toggle_mark'](11)
            viewer.window.resize(1500, 1000)
            app.processEvents()
            from qtpy.QtTest import QTest
            QTest.qWait(400)
            viewer.screenshot(path=path, canvas_only=False)
    finally:
        viewer.close()
        session.close()


def test_launcher_lists_fish_and_gates_actions(tmp_path):
    from qtpy.QtCore import QSettings
    from codeants_2pf_hcr.apps.cellmatcher import build_launcher
    from test_manual_identity_pipeline import FISH, make_fish
    # Keep the reviewer's real launcher preferences untouched.
    QSettings.setDefaultFormat(QSettings.IniFormat)
    QSettings.setPath(QSettings.IniFormat, QSettings.UserScope, str(tmp_path / 'settings'))
    data = tmp_path / 'data'
    make_fish(data)
    (data / 'Z002_f01').mkdir()
    app, window = build_launcher()
    try:
        window.root.setText(str(data))
        window.refresh()
        assert [window.table.item(r, 0).text() for r in range(window.table.rowCount())] == [FISH, 'Z002_f01']
        window.table.selectRow(1)
        assert not window.prepare_button.isEnabled() and not window.review_button.isEnabled()
        assert 'No ex-vivo Cellpose masks' in window.status.text()
        window.table.selectRow(0)
        assert window.prepare_button.isEnabled() and not window.review_button.isEnabled()
        assert not window.continue_button.isEnabled()
        from codeants_2pf_hcr import manual_identity_pipeline as mip
        mip.prepare_fish_dataset(FISH, data, log=lambda *_: None)
        window.refresh()
        window.table.selectRow(0)
        assert window.review_button.isEnabled()
        assert window.table.item(0, 3).text() == 'not started'
        window.reviewer.setText('GUI reviewer')
        window.show_viewers = False
        viewer, controls = window.open_review()
        lock = next((data / FISH).rglob('session/writer.lock'))
        labels = [b.text() for b in controls['panel'].findChildren(type(controls['undo']))]
        assert 'Finish && export identity' in labels
        controls['detail'].setChecked(True)
        controls['calls']['positive'].click()
        viewer.window._qt_window.close()
        app.processEvents()
        assert not lock.exists() and window.viewers == []
        assert window.table.item(0, 3).text().startswith('1/')
        path = os.environ.get('MANUAL_IDENTITY_LAUNCHER_SCREENSHOT')
        if path:
            window.show()
            app.processEvents()
            window.grab().save(path)
    finally:
        window.close()


def test_functional_scope_limits_queue_and_bulk_negatives_preserves_pending(tmp_path):
    dataset = make_dataset(tmp_path / 'data')
    session = AnnotationSession(dataset, tmp_path / 'session', 'GUI reviewer')
    session.view['pending'] = {'pth2-r1': [22]}
    dataset.review_eligible = [11]
    controls = {}
    viewer = create_viewer(dataset, session, show=False, controls=controls)
    try:
        assert controls['cells'].count() == 1
        assert controls['cells'].itemData(0) == 11
        controls['toggle_mark'](22)
        assert controls['pending']['pth2-r1'] == {22}
        controls['approve']('with_negatives')
        assert session.rows[('pth2-r1', 11)]['call'] == 'negative'
        assert session.rows[('pth2-r1', 22)]['call'] == 'unreviewed'
        controls['save_view']()
        assert session.view['pending']['pth2-r1'] == [22]
    finally:
        viewer.close()
        session.close()


def test_z_scroll_reuses_images_and_invalidates_changed_content(tmp_path):
    from qtpy.QtWidgets import QApplication, QCheckBox
    from qtpy.QtTest import QTest
    dataset = make_dataset(tmp_path / 'data')
    session = AnnotationSession(dataset, tmp_path / 'session', 'GUI reviewer')
    controls = {}
    viewer = create_viewer(dataset, session, show=False, controls=controls)
    try:
        state = controls['ortho_state']
        update = controls['update_orthos']
        crosshair = viewer.layers['Side-view crosshair']
        update()
        bases = state['ortho_bases']
        initial = [label.pixmap().toImage() for label in controls['orthogonal_labels']]
        events = []
        crosshair.events.data.connect(lambda event: events.append(event))
        viewer.dims.set_current_step(0, (viewer.dims.current_step[0] + 3) % 8)
        QTest.qWait(80)  # Exercise the real debounced dims-event connection.
        QApplication.instance().processEvents()
        assert state['ortho_bases'] is bases
        assert not events
        assert any(label.pixmap().toImage() != image for label, image in zip(controls['orthogonal_labels'], initial))
        # Returning to the same Z gives exactly the same pixels (no accumulated cursors).
        viewer.dims.set_current_step(0, (viewer.dims.current_step[0] - 3) % 8)
        update()
        assert all(label.pixmap().toImage() == image for label, image in zip(controls['orthogonal_labels'], initial))

        def changed(action):
            before = state['ortho_bases']
            action()
            update()
            assert state['ortho_bases'] is not before

        for layer in (viewer.layers['Ex-vivo GCaMP'], viewer.layers['HCR: pth2-r1']):
            changed(lambda: setattr(layer, 'contrast_limits', (0, 60)))
            changed(lambda: setattr(layer, 'gamma', 1.4))
            changed(lambda: setattr(layer, 'opacity', 0.6))
            changed(lambda: setattr(layer, 'visible', False))
            changed(lambda: layer.events.data())  # In-place refresh also invalidates.
        changed(lambda: controls['toggle_mark'](11))
        changed(lambda: session.set_call('pth2-r1', 22, 'positive'))
        changed(lambda: controls['select_cell'](22))
        positive_toggle = next(box for box in controls['panel'].findChildren(QCheckBox)
                               if box.text() == 'Show recorded positives (green)')
        changed(lambda: positive_toggle.setChecked(False))
        crosshair_toggle = next(box for box in controls['panel'].findChildren(QCheckBox)
                                if box.text() == 'Faint crosshair in image')
        crosshair_toggle.setChecked(False)
        assert not crosshair.visible
        crosshair_toggle.setChecked(True)
        assert crosshair.visible
        changed(lambda: controls['stack'].setCurrentIndex(1))
        changed(lambda: state.__setitem__('ortho_width', 300))
        changed(lambda: controls['cut_through']([0, 4, 4]))
        assert events
        # Cached and explicitly rebuilt pixels agree after all invalidations.
        images = [label.pixmap().toImage() for label in controls['orthogonal_labels']]
        state.pop('ortho_cache_key')
        update()
        assert all(label.pixmap().toImage() == image for label, image in zip(controls['orthogonal_labels'], images))
    finally:
        viewer.close()
        session.close()


def test_mapped_region_overlay_toggle_sideviews_and_z_cache(tmp_path):
    import numpy as np
    from qtpy.QtWidgets import QApplication
    from qtpy.QtTest import QTest
    dataset = make_dataset(tmp_path / 'data')
    dataset.review_region = np.zeros(dataset.labels.shape, dtype=bool)
    dataset.review_region[:, :, :12] = True
    dataset.review_scope = {'invivo_z_intervals': [[0, 10], [30, 40]]}
    session = AnnotationSession(dataset, tmp_path / 'session', 'GUI reviewer')
    controls = {}
    viewer = create_viewer(dataset, session, show=False, controls=controls)
    try:
        scope = controls['layers']['scope']
        state = controls['ortho_state']
        update = controls['update_orthos']
        update()
        assert scope.visible and not scope.editable
        assert scope.data[3, 10, 3] == 0  # unshaded inside
        assert scope.data[3, 10, 11] == 2  # blue boundary
        assert scope.data[3, 10, 18] == 1  # shaded outside
        assert viewer.dims.axis_labels[0] == 'Ex-vivo Z'
        assert 'in-vivo Z 0–10, 30–40' in controls['scope_info'].text()
        bases = state['ortho_bases']
        viewer.dims.set_current_step(0, 7)
        QTest.qWait(80)
        QApplication.instance().processEvents()
        assert state['ortho_bases'] is bases
        controls['scope_toggle'].setChecked(False)
        assert not scope.visible and state['ortho_bases'] is not bases
        hidden = [w.pixmap().toImage() for w in controls['orthogonal_labels']]
        controls['scope_toggle'].setChecked(True)
        assert scope.visible
        assert any(w.pixmap().toImage() != old for w, old in zip(controls['orthogonal_labels'], hidden))
        scope.visible = False  # napari layer-list action stays in sync
        assert not controls['scope_toggle'].isChecked()
        controls['save_view']()
        assert session.view['show_functional_region'] is False
        assert session.revision == 0 and controls['cells'].count() == 2
    finally:
        viewer.close()
        session.close()
