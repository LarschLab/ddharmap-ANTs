"""Real Qt widget interactions on a deterministic three-dimensional fixture."""
import os

import pytest

pytest.importorskip('napari')

from codeants_2pf_hcr.manual_identity import AnnotationSession
from codeants_2pf_hcr.apps.manual_identity import create_viewer
from test_manual_identity import make_dataset


def test_viewer_independent_calls_and_controls(tmp_path):
    from qtpy.QtWidgets import QApplication
    dataset = make_dataset(tmp_path / 'data')
    session = AnnotationSession(dataset, tmp_path / 'session', 'GUI reviewer')
    controls = {}
    viewer = create_viewer(dataset, session, show=bool(os.environ.get('MANUAL_IDENTITY_SCREENSHOT')), controls=controls)
    try:
        app = QApplication.instance()
        assert not viewer.layers['Fixed anatomy masks'].editable
        controls['calls']['positive'].click()
        app.processEvents()
        assert session.rows[('pth2-r1', 11)]['call'] == 'positive'
        assert session.rows[('cort-r1', 11)]['call'] == 'unreviewed'
        controls['stack'].setCurrentIndex(1)
        controls['cells'].setCurrentIndex(controls['cells'].findData(11))
        controls['calls']['positive'].click()
        app.processEvents()
        assert session.rows[('cort-r1', 11)]['call'] == 'positive'
        controls['undo'].click()
        assert session.rows[('cort-r1', 11)]['call'] == 'unreviewed'
        controls['redo'].click()
        assert session.rows[('cort-r1', 11)]['call'] == 'positive'
        controls['queue'].setCurrentText('uncertain')
        assert controls['cells'].count() == 0
        assert 'No cells' in controls['status'].text()
        from napari.utils.key_bindings import coerce_keybinding
        before = {key: row.copy() for key, row in session.rows.items()}
        viewer.keymap[coerce_keybinding('2')](viewer)
        assert session.rows == before
        # Z spacing is twice XY: physical aspect is twice raw pixel aspect.
        for label in controls['orthogonal_labels']:
            pixmap = label.pixmap()
            assert pixmap.height() / pixmap.width() == pytest.approx(8 * 2 / 24, abs=.01)
        # Native widget render captures panel layout without relying on OpenGL.
        path = os.environ.get('MANUAL_IDENTITY_SCREENSHOT')
        if path:
            controls['queue'].setCurrentText('all')
            controls['cells'].setCurrentIndex(0)
            viewer.window.resize(1400, 1100)
            app.processEvents()
            from qtpy.QtTest import QTest
            QTest.qWait(250)
            viewer.screenshot(path=path, canvas_only=False)
    finally:
        viewer.close()
        session.close()
