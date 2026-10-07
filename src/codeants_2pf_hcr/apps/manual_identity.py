"""Napari viewer for fixed-mask, independent per-marker identity annotation.

Primary workflow: scroll through Z, click every cell that looks HCR-positive to
mark it, then approve all marks at once. The view never moves on its own.
"""
from __future__ import annotations

import argparse
from pathlib import Path

from codeants_2pf_hcr.manual_identity import AnnotationSession, CALLS, Dataset

TIPS = {
    'stack': 'Marker stack to review. Each HCR channel/round is called independently, so a cell can be '
             'positive for several markers. Marks and calls belong to the stack shown here.',
    'selected': 'The cell you last clicked or picked from the list. It is outlined in yellow in the image '
                'and shown in the XZ/YZ side views below.',
    'approve': 'Record every marked cell of this stack as "positive" in one step. The step is journaled and can be '
               'undone. You can choose whether unmarked cells become "negative".',
    'clear': 'Remove all pending marks for this stack without recording anything.',
    'counts': 'Pending = marked but not yet approved. Positive/negative = recorded calls. '
              'Unreviewed cells never count as negative.',
    'show_positive': 'Fill cells already recorded as positive in green so you can see what is done.',
    'goto': 'Move the view to the selected cell (Z slice and centre). The view otherwise never moves on its own.',
    'follow': 'When on, picking a cell from the list or with N/P also moves the view to it.',
    'queue': 'Which cells the list below offers: unreviewed cells, a specific call, or cells with a flagged issue.',
    'cells': 'Pick a cell by ID. Use N / P to step through the list.',
    'call': {'unreviewed': 'Reset this cell to "not looked at yet". Shortcut 1.',
             'positive': 'This cell clearly contains HCR signal for this marker. Shortcut 2.',
             'negative': 'This cell was inspected and clearly lacks signal for this marker. Requires full coverage. Shortcut 3.',
             'uncertain': 'Signal is ambiguous. Not exported as identity; kept for later review. Shortcut 4.',
             'unassessable': 'The cell cannot be judged (outside the imaged volume, damaged, mis-registered). Shortcut 5.'},
    'issue': 'Optional reason something looks wrong with this cell (merged mask, registration mismatch, …). '
             'Saved with the next call you make.',
    'blocking': 'Tick when the issue means this cell must not receive an identity, whatever its call.',
    'note': 'Free-text evidence note saved with the next call (e.g. "dim but punctate").',
    'undo': 'Undo the last recorded step (one approval counts as one step).',
    'redo': 'Redo the step you just undid.',
    'freeze': 'Not needed to save your work: every click, approval and undo is saved immediately and restored '
              'when you reopen the fish. This writes an extra immutable, numbered snapshot of all calls '
              '(Finish & export does this for you).',
    'hist': 'Distribution of HCR intensity inside the imaged area of this stack (log counts). Drag the red '
            'line to hide background below it; drag the blue line to saturate bright signal above it.',
    'gamma': 'Brightness curve. Lower values brighten dim signal without changing the cut-offs.',
    'auto': 'Set the cut-offs to the 1st and 99.8th percentile of intensities inside the imaged area.',
    'hcr_visible': 'Show or hide the HCR channel to compare against the GCaMP anatomy.',
    'hcr_opacity': 'Blend strength of the HCR channel over the GCaMP anatomy.',
    'gcamp_opacity': 'Brightness of the GCaMP anatomy underneath the HCR signal.',
}
CURSOR_COLOR = '#ff314f'
ORTHO_SPAN_UM = 200  # Fixed width of the side views; independent of zoom so they stay stable and cheap.
SELECTED_COLOR = (1.0, 0.95, 0.0, 1.0)
PENDING_COLOR = (1.0, 0.55, 0.0, 0.55)
POSITIVE_COLOR = (0.2, 0.9, 0.35, 0.35)


def _label_colormap(labels, color):
    from napari.utils.colormaps import DirectLabelColormap
    return DirectLabelColormap(color_dict={None: (0, 0, 0, 0), **{int(n): color for n in labels}})


def _histogram_widget(marker_layer, sample_provider):
    """HCR intensity histogram with draggable low/high cut-offs and gamma."""
    import numpy as np
    from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg
    from matplotlib.figure import Figure
    from qtpy.QtCore import Qt
    from qtpy.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QLabel, QSlider, QPushButton, QDoubleSpinBox

    widget = QWidget()
    layout = QVBoxLayout(widget)
    layout.setContentsMargins(0, 0, 0, 0)
    layout.setSpacing(4)
    figure = Figure(figsize=(2.6, 1.3), tight_layout=True)
    canvas = FigureCanvasQTAgg(figure)
    canvas.setToolTip(TIPS['hist'])
    canvas.setMinimumHeight(120)
    axes = figure.add_subplot(111)
    layout.addWidget(canvas)
    row = QHBoxLayout()
    low_box, high_box = QDoubleSpinBox(), QDoubleSpinBox()
    for box, name, tip in ((low_box, 'Hide <', 'red'), (high_box, 'Sat. >', 'blue')):
        box.setDecimals(1)
        box.setRange(-1e9, 1e9)
        box.setButtonSymbols(QDoubleSpinBox.NoButtons)
        box.setMinimumWidth(50)
        box.setToolTip(TIPS['hist'] + f' This box sets the {tip} line.')
        label = QLabel(name)
        label.setToolTip(box.toolTip())
        row.addWidget(label)
        row.addWidget(box, 1)
    layout.addLayout(row)
    gamma = QSlider(Qt.Horizontal)
    gamma.setRange(20, 300)
    gamma.setToolTip(TIPS['gamma'])
    gamma_label = QLabel()
    gamma_label.setToolTip(TIPS['gamma'])
    gamma_row = QHBoxLayout()
    gamma_row.addWidget(gamma_label)
    gamma_row.addWidget(gamma, 1)
    layout.addLayout(gamma_row)
    auto = QPushButton('Auto contrast')
    auto.setToolTip(TIPS['auto'])
    layout.addWidget(auto)
    state = dict(lines={}, drag=None, values=None, busy=False)

    def draw():
        values = state['values']
        axes.clear()
        if values is not None and values.size:
            counts, edges = np.histogram(values, bins=200)
            axes.fill_between(edges[:-1], np.log10(counts + 1), step='post', color='#b04aa0', alpha=0.8)
        low, high = marker_layer.contrast_limits
        state['lines'] = {'low': axes.axvline(low, color='#e0403a', lw=2), 'high': axes.axvline(high, color='#3a7be0', lw=2)}
        axes.set_yticks([])
        # Match napari's dark theme.
        figure.patch.set_facecolor('#262930')
        axes.set_facecolor('#262930')
        axes.tick_params(labelsize=6, colors='#d6d6d6', pad=1)
        for spine in axes.spines.values():
            spine.set_color('#5a5f6b')
        canvas.draw_idle()

    def sync_from_layer(*_):
        if state['busy']:
            return
        state['busy'] = True
        low, high = marker_layer.contrast_limits
        low_box.setValue(low)
        high_box.setValue(high)
        gamma.setValue(int(round(marker_layer.gamma * 100)))
        gamma_label.setText(f'Gamma {marker_layer.gamma:.2f}')
        for key, value in (('low', low), ('high', high)):
            if key in state['lines']:
                state['lines'][key].set_xdata([value, value])
        canvas.draw_idle()
        state['busy'] = False

    def set_limits(low, high):
        if high <= low:
            return
        marker_layer.contrast_limits_range = (min(low, marker_layer.contrast_limits_range[0]), max(high, marker_layer.contrast_limits_range[1]))
        marker_layer.contrast_limits = (low, high)

    def refresh_data():
        state['values'] = sample_provider()
        draw()
        sync_from_layer()

    def on_press(event):
        if event.inaxes is not axes or event.xdata is None:
            return
        low, high = marker_layer.contrast_limits
        state['drag'] = 'low' if abs(event.xdata - low) <= abs(event.xdata - high) else 'high'
        on_motion(event)

    def on_motion(event):
        if state['drag'] is None or event.xdata is None:
            return
        low, high = marker_layer.contrast_limits
        if state['drag'] == 'low':
            set_limits(min(event.xdata, high - 1e-6), high)
        else:
            set_limits(low, max(event.xdata, low + 1e-6))

    def on_release(_):
        state['drag'] = None

    def auto_contrast():
        values = state['values']
        if values is not None and values.size:
            set_limits(*np.percentile(values, [1, 99.8]))

    canvas.mpl_connect('button_press_event', on_press)
    canvas.mpl_connect('motion_notify_event', on_motion)
    canvas.mpl_connect('button_release_event', on_release)
    low_box.editingFinished.connect(lambda: set_limits(low_box.value(), marker_layer.contrast_limits[1]))
    high_box.editingFinished.connect(lambda: set_limits(marker_layer.contrast_limits[0], high_box.value()))
    gamma.valueChanged.connect(lambda v: None if state['busy'] else setattr(marker_layer, 'gamma', v / 100))
    auto.clicked.connect(auto_contrast)
    marker_layer.events.contrast_limits.connect(sync_from_layer)
    marker_layer.events.gamma.connect(sync_from_layer)
    widget.refresh_data = refresh_data
    widget.controls = dict(canvas=canvas, low=low_box, high=high_box, gamma=gamma, auto=auto)
    return widget


def create_viewer(dataset, session, *, show=True, controls=None):
    # GUI dependencies stay optional for headless pipeline consumers.
    import napari
    import numpy as np
    from scipy import ndimage
    from qtpy.QtCore import Qt, QObject, QEvent, QTimer
    from qtpy.QtGui import QImage, QPixmap, QPainter, QPen, QColor
    from qtpy.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel, QComboBox, QDockWidget, QGroupBox,
        QPushButton, QLineEdit, QCheckBox, QMessageBox, QScrollArea, QSlider, QFormLayout, QGridLayout)

    viewer = napari.Viewer(title=f"cellMatcher — {dataset.manifest['fish_id']}", show=show)
    spacing = tuple(reversed(dataset.manifest['grid']['spacing_xyz']))
    anatomy = viewer.add_image(dataset.anatomy, name='Ex-vivo GCaMP', colormap='gray', scale=spacing)
    stack_ids = list(dataset.markers)
    first_stack = session.view.get('stack', stack_ids[0])
    if first_stack not in stack_ids:
        first_stack = stack_ids[0]
    intensity, coverage = dataset.marker_data(first_stack)
    marker_layer = viewer.add_image(intensity, name='HCR: ' + first_stack, colormap='magenta', blending='additive', scale=spacing)
    coverage_layer = viewer.add_image((coverage == 0).astype(np.uint8), name='Outside imaged area',
                                     colormap='cyan', blending='additive', opacity=0.25, scale=spacing,
                                     contrast_limits=(0, 1))
    labels = viewer.add_labels(dataset.labels, name='Ex-vivo masks', scale=spacing, opacity=0.25)
    labels.contour = 1
    positive_layer = viewer.add_labels(dataset.labels, name='Recorded positive', scale=spacing, opacity=1)
    pending_layer = viewer.add_labels(dataset.labels, name='Marked (pending)', scale=spacing, opacity=1)
    selected = viewer.add_labels(dataset.labels, name='Selected cell', scale=spacing, opacity=1)
    selected.contour = 3
    for layer in (labels, positive_layer, pending_layer, selected):
        layer.editable = False
    centers = ndimage.center_of_mass(np.ones(dataset.labels.shape, dtype=np.uint8), dataset.labels, dataset.eligible)
    centers = {label: tuple(int(round(v)) for v in center) for label, center in zip(dataset.eligible, centers)}
    pending = {stack: set(map(int, labels_)) & set(centers) for stack, labels_ in session.view.get('pending', {}).items()}
    state = {'label': session.view.get('label', dataset.eligible[0]), 'stack': first_stack}
    if state['label'] not in centers:
        state['label'] = dataset.eligible[0]
    # Where the side views cut (y, x); Z is the image's current slice.
    state['cursor'] = [dataset.labels.shape[1] // 2, dataset.labels.shape[2] // 2]
    # Faint crosshair in the image with an open centre so the cell under it stays visible.
    CROSSHAIR_GAP = 14  # data pixels each side (~ one cell diameter overall)
    crosshair = viewer.add_shapes(name='Side-view crosshair', shape_type='line', edge_color=CURSOR_COLOR,
                                  edge_width=0.6, opacity=0.35, scale=spacing[1:])
    crosshair.editable = False

    panel = QWidget()
    layout = QVBoxLayout(panel)
    layout.setContentsMargins(4, 4, 4, 4)
    layout.setSpacing(6)
    header = QLabel()
    header.setWordWrap(True)
    header.setTextFormat(Qt.RichText)
    header.setToolTip(TIPS['selected'])
    header.setStyleSheet('QLabel { padding: 3px 5px; border: 2px solid #f2e600; border-radius: 5px; }')
    layout.addWidget(header)
    stack_select = QComboBox()
    stack_select.setToolTip(TIPS['stack'])
    for stack in stack_ids:
        m = dataset.markers[stack]
        stack_select.addItem(f"{m['gene']} · {m['round_id']} · {stack}", stack)
    stack_select.setCurrentIndex(stack_ids.index(first_stack))
    stack_select.setSizeAdjustPolicy(QComboBox.AdjustToMinimumContentsLengthWithIcon)
    stack_select.setMinimumContentsLength(12)
    layout.addWidget(stack_select)

    mark_box = QGroupBox('Mark positive cells')
    mark_box.setToolTip('Scroll through Z and click every cell that contains HCR signal to mark it (orange). '
                        'Click again to unmark. Option/Alt-click only selects a cell (yellow outline) without marking it. '
                        'When done, approve all marks at once. Marks are kept when you close the window.')
    mark_layout = QVBoxLayout(mark_box)
    mark_layout.setContentsMargins(6, 4, 6, 4)
    mark_layout.setSpacing(4)
    howto = QLabel('Click = mark/unmark · ⌥-click = select only')
    howto.setToolTip(mark_box.toolTip())
    mark_layout.addWidget(howto)
    counts = QLabel()
    counts.setWordWrap(True)
    counts.setToolTip(TIPS['counts'])
    mark_layout.addWidget(counts)
    approve = QPushButton()
    approve.setToolTip(TIPS['approve'])
    approve.setStyleSheet('QPushButton { font-weight: bold; padding: 4px; }')
    clear = QPushButton('Clear')
    clear.setToolTip(TIPS['clear'])
    show_positive = QCheckBox('Show recorded positives (green)')
    show_positive.setChecked(True)
    show_positive.setToolTip(TIPS['show_positive'])
    approve_row = QHBoxLayout()
    approve_row.addWidget(approve, 1)
    approve_row.addWidget(clear)
    mark_layout.addLayout(approve_row)
    mark_layout.addWidget(show_positive)
    layout.addWidget(mark_box)

    detail = QGroupBox('Single-cell call (optional)')
    detail.setCheckable(True)
    detail.setChecked(False)
    detail.setToolTip('Record uncertain/unassessable calls, flag issues, or change one cell at a time.')
    detail_body = QWidget()
    detail_layout = QVBoxLayout(detail_body)
    detail_outer = QVBoxLayout(detail)
    detail_outer.addWidget(detail_body)
    detail.toggled.connect(detail_body.setVisible)
    detail_body.setVisible(False)
    goto = QPushButton('Go to selected cell')
    goto.setToolTip(TIPS['goto'])
    follow = QCheckBox('Follow selection (move view)')
    follow.setToolTip(TIPS['follow'])
    filter_select = QComboBox()
    filter_select.addItems(['unreviewed', 'all', *CALLS[1:], 'issue-flagged'])
    filter_select.setCurrentText(session.view.get('filter', 'unreviewed'))
    filter_select.setToolTip(TIPS['queue'])
    cell_select = QComboBox()
    cell_select.setToolTip(TIPS['cells'])
    issue = QComboBox()
    issue.addItems(['', 'merged_anatomy_label', 'registration_mismatch', 'poor_coverage', 'bleed_through', 'other'])
    issue.setToolTip(TIPS['issue'])
    blocking = QCheckBox('Issue blocks identity export')
    blocking.setToolTip(TIPS['blocking'])
    note = QLineEdit()
    note.setPlaceholderText('Optional evidence note')
    note.setToolTip(TIPS['note'])
    for widget in (goto, follow, QLabel('Cell list'), filter_select, cell_select):
        detail_layout.addWidget(widget)
    previous, next_cell = QPushButton('◀ Previous (P)'), QPushButton('Next (N) ▶')
    previous.setToolTip('Select the previous cell in the list above (shortcut P). Moves the view only if "Follow selection" is on.')
    next_cell.setToolTip('Select the next cell in the list above (shortcut N). Moves the view only if "Follow selection" is on.')
    nav = QHBoxLayout()
    nav.addWidget(previous)
    nav.addWidget(next_cell)
    detail_layout.addLayout(nav)
    call_buttons = {}
    calls_row = QHBoxLayout()
    for call in CALLS:
        button = QPushButton(call.title())
        button.setToolTip(TIPS['call'][call])
        calls_row.addWidget(button)
        call_buttons[call] = button
    detail_layout.addLayout(calls_row)
    for widget in (issue, blocking, note):
        detail_layout.addWidget(widget)
    layout.addWidget(detail)

    history = QHBoxLayout()
    undo, redo, freeze = QPushButton('Undo'), QPushButton('Redo'), QPushButton('Snapshot')
    for button, tip in ((undo, 'undo'), (redo, 'redo'), (freeze, 'freeze')):
        button.setToolTip(TIPS[tip])
        history.addWidget(button)
    layout.addLayout(history)
    ortho_tip = (f'Side views of the whole stack depth over a fixed {ORTHO_SPAN_UM} µm around the cut, at true physical aspect '
                 'ratio (GCaMP grey, HCR magenta); they do not rescale with zoom. The red lines show where you are: the horizontal line is the current Z slice, '
                 'the vertical line is where the other side view cuts. Click or drag in a side view to jump to that Z slice and '
                 'move the cut; right-click a cell in the image to cut through it (clicking to mark a cell does the same). A faint '
                 'crosshair with an open centre shows the cut position in the image. '
                 'Outlines: selected cell yellow, marked orange, recorded positives green (if shown). Use them to check that signal '
                 'sits inside a cell in depth, and to spot positive-looking cells you have not marked yet.')

    class OrthoView(QLabel):
        # Reports clicks/drags as fractions (u, v) of the drawn image.
        def __init__(self, on_pick):
            super().__init__()
            self.on_pick = on_pick

        def pick(self, pos):
            pixmap = self.pixmap()
            if pixmap is None or pixmap.isNull():
                return
            u = (pos.x() - (self.width() - pixmap.width()) / 2) / pixmap.width()
            v = (pos.y() - (self.height() - pixmap.height()) / 2) / pixmap.height()
            if 0 <= u < 1 and 0 <= v < 1:
                self.on_pick(u, v)

        def mousePressEvent(self, event):
            if event.button() == Qt.LeftButton:
                self.pick(event.pos())

        def mouseMoveEvent(self, event):
            if event.buttons() & Qt.LeftButton:
                self.pick(event.pos())

    ortho_grid = QGridLayout()
    ortho_grid.setSpacing(4)
    xz, yz = OrthoView(lambda u, v: pick_ortho('xz', u, v)), OrthoView(lambda u, v: pick_ortho('yz', u, v))
    captions, ortho_cells = {}, {}
    for label_widget, name in ((xz, 'xz'), (yz, 'yz')):
        label_widget.setToolTip(ortho_tip)
        label_widget.setMinimumSize(60, 40)
        label_widget.setAlignment(Qt.AlignCenter)
        label_widget.setCursor(Qt.CrossCursor)
        cell = QWidget()
        column = QVBoxLayout(cell)
        column.setContentsMargins(0, 0, 0, 0)
        column.setSpacing(1)
        captions[name] = QLabel(name.upper())
        captions[name].setAlignment(Qt.AlignHCenter)
        captions[name].setToolTip(ortho_tip)
        column.addWidget(captions[name])
        column.addWidget(label_widget)
        ortho_cells[name] = cell
    ortho_grid.addWidget(ortho_cells['xz'], 0, 0)
    ortho_grid.addWidget(ortho_cells['yz'], 1, 0)
    layout.addLayout(ortho_grid)
    show_crosshair = QCheckBox('Faint crosshair in image')
    show_crosshair.setChecked(True)
    show_crosshair.setToolTip('Thin, faint red lines in the image marking where the side views cut. '
                              'The centre is left open so the cell you are looking at is never covered.')
    layout.addWidget(show_crosshair)
    layout.addStretch()
    scroll = QScrollArea()
    scroll.setWidgetResizable(True)
    scroll.setMinimumWidth(260)
    scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
    scroll.setWidget(panel)
    review_dock = viewer.window.add_dock_widget(scroll, name='Identity review', area='right')

    def coverage_sample():
        data, valid = dataset.marker_data(state['stack'])
        step = max(1, int(round((data.size / 2_000_000) ** (1 / 3))))
        values = data[::step, ::step, ::step][valid[::step, ::step, ::step] > 0]
        return values.astype(float)

    hist = _histogram_widget(marker_layer, coverage_sample)
    left = QWidget()
    left_layout = QVBoxLayout(left)
    left_layout.setContentsMargins(4, 4, 4, 4)
    left_layout.addWidget(hist)
    hcr_visible = QCheckBox('Show HCR')
    hcr_visible.setChecked(True)
    hcr_visible.setToolTip(TIPS['hcr_visible'])
    hcr_opacity, gcamp_opacity = QSlider(Qt.Horizontal), QSlider(Qt.Horizontal)
    hcr_opacity.setToolTip(TIPS['hcr_opacity'])
    gcamp_opacity.setToolTip(TIPS['gcamp_opacity'])
    for slider in (hcr_opacity, gcamp_opacity):
        slider.setRange(0, 100)
    left_layout.addWidget(hcr_visible)
    sliders = QFormLayout()
    sliders.setContentsMargins(0, 0, 0, 0)
    sliders.addRow('HCR', hcr_opacity)
    sliders.addRow('GCaMP', gcamp_opacity)
    sliders.labelForField(hcr_opacity).setToolTip(TIPS['hcr_opacity'])
    sliders.labelForField(gcamp_opacity).setToolTip(TIPS['gcamp_opacity'])
    left_layout.addLayout(sliders)
    left_layout.addStretch()
    contrast_dock = viewer.window.add_dock_widget(left, name='HCR contrast', area='left')
    # Our panels cannot be closed; napari's own panels can be restored from the menu.
    for dock in (review_dock, contrast_dock):
        dock.setFeatures(QDockWidget.DockWidgetMovable | QDockWidget.DockWidgetFloatable)
    qt_window = viewer.window._qt_window
    qt_viewer = viewer.window._qt_viewer
    napari_docks = [d for d in (getattr(qt_viewer, 'dockLayerControls', None), getattr(qt_viewer, 'dockLayerList', None)) if d]
    # The image gets the space: napari's layer panels share one tab group with HCR contrast.
    LEFT_WIDTH, RIGHT_WIDTH = 250, 300

    def arrange_panels():
        for dock in napari_docks:
            dock.setFloating(False)
            qt_window.tabifyDockWidget(contrast_dock, dock)
        contrast_dock.show()
        contrast_dock.raise_()
        qt_window.resizeDocks([contrast_dock, review_dock], [LEFT_WIDTH, RIGHT_WIDTH], Qt.Horizontal)

    def restore_panels():
        for dock in qt_window.findChildren(QDockWidget):
            if 'console' in dock.windowTitle().lower():
                continue  # The developer console stays opt-in.
            dock.setFloating(False)
            dock.show()
        arrange_panels()
    menu = qt_window.menuBar().addMenu('cellMatcher')
    restore_action = menu.addAction('Restore all panels')
    restore_action.setShortcut('Ctrl+Shift+R')
    restore_action.triggered.connect(restore_panels)

    def outline(lab, wanted):
        # Boundary pixels of the wanted labels, kept separate where wanted cells touch.
        boundary = np.zeros(lab.shape, dtype=bool)
        boundary[1:] |= lab[1:] != lab[:-1]
        boundary[:-1] |= lab[:-1] != lab[1:]
        boundary[:, 1:] |= lab[:, 1:] != lab[:, :-1]
        boundary[:, :-1] |= lab[:, :-1] != lab[:, 1:]
        boundary[[0, -1], :] = True
        boundary[:, [0, -1]] = True
        return boundary & np.isin(lab, list(wanted)) if wanted else np.zeros(lab.shape, dtype=bool)

    def orthogonal_pixmap(a, h, lab, scale_y, scale_x, line_row, line_col):
        def norm(array, limits, gamma=1.0):
            lo, hi = limits
            return np.clip((array.astype(float) - lo) / max(hi - lo, 1e-9), 0, 1) ** gamma
        a = norm(a, anatomy.contrast_limits) * anatomy.opacity
        h = norm(h, marker_layer.contrast_limits, marker_layer.gamma) * marker_layer.opacity
        if not marker_layer.visible:
            h = h * 0
        if not anatomy.visible:
            a = a * 0
        rgb = np.stack([np.clip(a + h, 0, 1), a, np.clip(a + h, 0, 1)], axis=-1)
        positives = ({r['exvivo_label_id'] for r in stack_rows() if r['call'] == 'positive'}
                     if show_positive.isChecked() else set())
        marked = pending.get(state['stack'], set())
        for wanted, color in ((positives - marked, POSITIVE_COLOR), (marked, PENDING_COLOR), ({state['label']}, SELECTED_COLOR)):
            rgb[outline(lab, wanted & set(np.unique(lab).tolist()))] = color[:3]
        rgb = np.ascontiguousarray((rgb * 255).astype(np.uint8))
        image = QImage(rgb.data, rgb.shape[1], rgb.shape[0], rgb.strides[0], QImage.Format_RGB888).copy()
        # Fit the physical extent into the panel (half its width when side by side), capped in height.
        box_w = state.get('ortho_width') or 260
        box_h = 400
        phys_h, phys_w = rgb.shape[0] * scale_y, rgb.shape[1] * scale_x
        factor = min(box_w / phys_w, box_h / phys_h)
        width, height = max(1, int(phys_w * factor)), max(1, int(phys_h * factor))
        pixmap = QPixmap.fromImage(image).scaled(width, height, Qt.IgnoreAspectRatio, Qt.SmoothTransformation)
        # Position indicators: current Z slice (horizontal) and the other view's cut (vertical).
        painter = QPainter(pixmap)
        pen = QPen(QColor(CURSOR_COLOR))
        pen.setWidth(1)
        painter.setPen(pen)
        row_y = int((line_row + 0.5) * height / rgb.shape[0])
        col_x = int((line_col + 0.5) * width / rgb.shape[1])
        painter.drawLine(0, row_y, width, row_y)
        painter.drawLine(col_x, 0, col_x, height)
        painter.end()
        return pixmap

    class _Resize(QObject):
        def eventFilter(self, obj, event):
            # Only a real panel resize re-renders (not a scrollbar appearing), so the views cannot oscillate.
            if event.type() == QEvent.Resize:
                width = max(160, scroll.width() - 36)
                if abs(width - (state.get('ortho_width') or 0)) > 8:
                    state['ortho_width'] = width
                    schedule_orthos()
            return False
    resize_watch = _Resize(scroll)
    scroll.installEventFilter(resize_watch)

    def current_z():
        return int(min(max(viewer.dims.current_step[0], 0), dataset.labels.shape[0] - 1))

    def visible_world():
        # World (y, x) corners of the image canvas, as napari maps them (handles Retina scaling).
        try:
            corners = np.asarray(viewer.window._qt_viewer.canvas._viewbox_corners_in_world)
            return corners[0][-2:], corners[1][-2:]
        except Exception:  # Headless or a napari without this helper.
            height, width = viewer._canvas_size
            zoom = max(float(viewer.camera.zoom), 1e-9)
            centre = np.asarray(viewer.camera.center[-2:])
            half = np.array([height, width]) / (2 * zoom)
            return centre - half, centre + half

    def fov_bounds():
        # Data-index extent (y0, y1, x0, x1) of the area visible in the image canvas.
        top_left, bottom_right = visible_world()
        bounds = []
        for lo_w, hi_w, step, n in ((top_left[0], bottom_right[0], spacing[1], dataset.labels.shape[1]),
                                    (top_left[1], bottom_right[1], spacing[2], dataset.labels.shape[2])):
            lo_w, hi_w = min(lo_w, hi_w), max(lo_w, hi_w)
            lo = int(np.floor(lo_w / step))
            hi = int(np.ceil(hi_w / step))
            lo, hi = max(0, lo), min(n, hi)
            if hi - lo < 8:  # Keep a usable view when zoomed far in or panned off the stack.
                mid = min(max((lo + hi) // 2, 4), n - 4)
                lo, hi = max(0, mid - 4), min(n, mid + 4)
            bounds += [lo, hi]
        return tuple(bounds)

    def ortho_window(centre, step, n):
        # Fixed-width window around the cut, shifted (not shrunk) at the volume edge.
        width = min(n, int(round(ORTHO_SPAN_UM / step)))
        lo = min(max(centre - width // 2, 0), n - width)
        return lo, lo + width

    def keep_cursor_in_view(*_):
        # Panning/zooming only redraws when the cut has to move back into the visible area.
        y0, y1, x0, x1 = fov_bounds()
        cursor = state['cursor']
        clamped = [min(max(cursor[0], y0), y1 - 1), min(max(cursor[1], x0), x1 - 1)]
        if clamped != cursor:
            cursor[:] = clamped
            schedule_orthos()

    def update_orthos(*_):
        y, x = state['cursor']
        z = current_z()
        y0, y1 = ortho_window(y, spacing[1], dataset.labels.shape[1])
        x0, x1 = ortho_window(x, spacing[2], dataset.labels.shape[2])
        state['ortho'] = dict(y0=y0, y1=y1, x0=x0, x1=x1)
        xz.setPixmap(orthogonal_pixmap(dataset.anatomy[:, y, x0:x1], marker_layer.data[:, y, x0:x1],
                                       dataset.labels[:, y, x0:x1], spacing[0], spacing[2], z, x - x0))
        yz.setPixmap(orthogonal_pixmap(dataset.anatomy[:, y0:y1, x], marker_layer.data[:, y0:y1, x],
                                       dataset.labels[:, y0:y1, x], spacing[0], spacing[1], z, y - y0))
        captions['xz'].setText(f'XZ at y {y}  ·  z {z}')
        captions['yz'].setText(f'YZ at x {x}  ·  z {z}')
        ny, nx = dataset.labels.shape[1:]
        g = CROSSHAIR_GAP
        segments = []
        if x - g > 0:
            segments.append([[y, 0], [y, x - g]])
        if x + g < nx - 1:
            segments.append([[y, x + g], [y, nx - 1]])
        if y - g > 0:
            segments.append([[0, x], [y - g, x]])
        if y + g < ny - 1:
            segments.append([[y + g, x], [ny - 1, x]])
        crosshair.data = []
        if segments:
            crosshair.add_lines([np.array(seg, float) for seg in segments], edge_color=CURSOR_COLOR, edge_width=0.6)
        crosshair.visible = show_crosshair.isChecked()

    ortho_timer = QTimer()
    ortho_timer.setSingleShot(True)
    ortho_timer.setInterval(30)
    ortho_timer.timeout.connect(update_orthos)

    def schedule_orthos(*_):
        ortho_timer.start()

    def set_cursor(y=None, x=None, z=None):
        if y is not None:
            state['cursor'][0] = int(min(max(y, 0), dataset.labels.shape[1] - 1))
        if x is not None:
            state['cursor'][1] = int(min(max(x, 0), dataset.labels.shape[2] - 1))
        if z is not None and int(z) != current_z():
            viewer.dims.set_current_step(0, int(z))  # Triggers a redraw through the dims event.
        update_orthos()

    def cut_through(position, cell=None):
        # Side views cut through the cell's centre, or the clicked point off-cell; Z and FOV stay put.
        if cell is not None:
            set_cursor(y=centers[cell][1], x=centers[cell][2])
        else:
            set_cursor(y=position[-2] / spacing[1], x=position[-1] / spacing[2])

    def pick_ortho(view, u, v):
        bounds = state['ortho']
        z = int(v * dataset.labels.shape[0])
        if view == 'xz':
            set_cursor(x=bounds['x0'] + int(u * (bounds['x1'] - bounds['x0'])), z=z)
        else:
            set_cursor(y=bounds['y0'] + int(u * (bounds['y1'] - bounds['y0'])), z=z)

    def stack_rows():
        return [r for (stack, _), r in session.rows.items() if stack == state['stack']]

    def refresh_overlays():
        marked = pending.get(state['stack'], set())
        pending_layer.colormap = _label_colormap(marked, PENDING_COLOR)
        positives = [r['exvivo_label_id'] for r in stack_rows() if r['call'] == 'positive'] if show_positive.isChecked() else []
        positive_layer.colormap = _label_colormap(positives, POSITIVE_COLOR)
        selected.colormap = _label_colormap([state['label']], SELECTED_COLOR)

    def refresh_status():
        rows = stack_rows()
        tally = {call: sum(r['call'] == call for r in rows) for call in CALLS}
        marked = len(pending.get(state['stack'], ()))
        counts.setText(f'<b>{marked}</b> pending · <span style="color:#4fdc6a">{tally["positive"]} positive</span> · '
                       f'{tally["negative"]} negative<br>{tally["uncertain"]} uncertain · {tally["unassessable"]} unassessable · '
                       f'{tally["unreviewed"]} unreviewed')
        approve.setText(f'Approve {marked} as positive…')
        approve.setEnabled(not session.journal_failed)
        clear.setEnabled(bool(marked))
        row = session.rows[(state['stack'], state['label'])]
        marker = dataset.markers[state['stack']]
        flag = ' · <span style="color:#ff9a1a">marked</span>' if state['label'] in pending.get(state['stack'], ()) else ''
        z = centers[state['label']][0]
        header.setText(f'<b>Selected cell {state["label"]}</b> (z {z}){flag}<br>'
                       f'{marker["gene"]} · {marker["round_id"]}: <b>{row["call"]}</b>'
                       + (f' · issue: {row["issue_code"]}' if row['issue_code'] else ''))

    def move_view_to(label):
        z, y, x = centers[label]
        viewer.dims.set_point(0, z * spacing[0])
        viewer.camera.center = (z * spacing[0], y * spacing[1], x * spacing[2])

    def select_cell(label, *, move=False):
        if label not in centers:
            return
        state['label'] = label
        if move:
            move_view_to(label)
            state['cursor'][:] = centers[label][1:]
        row = session.rows[(state['stack'], label)]
        issue.setCurrentText(row['issue_code'])
        blocking.setChecked(row['issue_blocks_export'])
        note.setText(row['note'])
        refresh_overlays()
        refresh_status()
        update_orthos()

    def save_view():
        session.view.update(stack=state['stack'], label=state['label'], filter=filter_select.currentText(),
            anatomy_contrast=list(anatomy.contrast_limits), zoom=float(viewer.camera.zoom),
            pending={stack: sorted(labels_) for stack, labels_ in pending.items() if labels_})
        session.view.setdefault('stacks', {})[state['stack']] = dict(
            contrast=list(marker_layer.contrast_limits), opacity=float(marker_layer.opacity), gamma=float(marker_layer.gamma))
        session.save_view()

    def toggle_mark(label):
        marked = pending.setdefault(state['stack'], set())
        marked.symmetric_difference_update({label})
        select_cell(label)
        save_view()

    def refresh_queue(*_):
        choice = filter_select.currentText()
        cell_select.blockSignals(True)
        cell_select.clear()
        for label in dataset.eligible:
            row = session.rows[(state['stack'], label)]
            if choice == 'all' or row['call'] == choice or (choice == 'issue-flagged' and row['issue_code']):
                cell_select.addItem(str(label), label)
        ix = cell_select.findData(state['label'])
        if ix >= 0:
            cell_select.setCurrentIndex(ix)
        cell_select.blockSignals(False)
        for button in call_buttons.values():
            button.setEnabled(not session.journal_failed)
        select_cell(state['label'])

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
            marker_layer.gamma = settings.get('gamma', 1.0)
        else:
            marker_layer.reset_contrast_limits()
            marker_layer.opacity = 1
            marker_layer.gamma = 1.0
        coverage_layer.data = (valid == 0).astype(np.uint8)
        hist.refresh_data()
        refresh_queue()

    def report(exc):
        title = 'Session needs reopening' if session.journal_failed else 'Not saved'
        QMessageBox.warning(panel, title, str(exc))

    def report_checkpoint():
        if session.last_snapshot_error:
            QMessageBox.warning(panel, 'Saved to journal',
                'The decision is saved in the journal, but the CSV/session checkpoint could not be updated. '
                'Resolve the storage problem before continuing: ' + session.last_snapshot_error)

    def decision(call):
        try:
            session.set_call(state['stack'], state['label'], call, issue=issue.currentText(),
                             blocking=blocking.isChecked(), note=note.text())
            pending.get(state['stack'], set()).discard(state['label'])
            refresh_queue()
            save_view()
            report_checkpoint()
        except (ValueError, OSError) as exc:
            report(exc)

    def approve_marked(choice=None):
        """choice: None asks; 'positives' or 'with_negatives' skips the dialog."""
        marked = sorted(pending.get(state['stack'], ()))
        uncovered = set(session.uncovered_labels(state['stack'], marked))
        ready = [n for n in marked if n not in uncovered]
        others = [r['exvivo_label_id'] for r in stack_rows() if r['call'] == 'unreviewed' and r['exvivo_label_id'] not in set(marked)]
        others_uncovered = set(session.uncovered_labels(state['stack'], others))
        others_covered = [n for n in others if n not in others_uncovered]
        if not ready and choice is None:
            QMessageBox.information(panel, 'Nothing to approve', 'Mark cells by clicking them in the image first.')
            return
        if choice is None:
            box = QMessageBox(panel)
            box.setWindowTitle('Approve marked cells')
            gene = dataset.markers[state['stack']]['gene']
            text = f'Record <b>{len(ready)}</b> marked cell(s) as <b>{gene}-positive</b> in stack {state["stack"]}?'
            if uncovered:
                text += (f'<br><br>{len(uncovered)} marked cell(s) extend outside the imaged area and cannot be positive; '
                         'they stay marked so you can call them uncertain or unassessable.')
            text += (f'<br><br>{len(others_covered)} other fully imaged cell(s) are still unreviewed. Leave them unreviewed '
                     '(they never count as negative), or record them as negative only if you have looked at the whole stack.')
            box.setText(text)
            only = box.addButton('Approve positives only', QMessageBox.AcceptRole)
            with_negatives = box.addButton(f'Approve + {len(others_covered)} others negative', QMessageBox.AcceptRole)
            box.addButton(QMessageBox.Cancel)
            box.setDefaultButton(only)
            box.exec_()
            choice = {only: 'positives', with_negatives: 'with_negatives'}.get(box.clickedButton())
        if choice not in ('positives', 'with_negatives'):
            return
        try:
            if ready:
                session.set_calls(state['stack'], ready, 'positive')
            if choice == 'with_negatives' and others_covered:
                session.set_calls(state['stack'], others_covered, 'negative')
            pending[state['stack']] = set(uncovered)
            refresh_queue()
            save_view()
            report_checkpoint()
        except (ValueError, OSError) as exc:
            report(exc)

    def clear_marks():
        pending[state['stack']] = set()
        refresh_overlays()
        refresh_status()
        update_orthos()
        save_view()

    def move(direction):
        if cell_select.count():
            cell_select.setCurrentIndex((cell_select.currentIndex() + direction) % cell_select.count())

    def history_action(action):
        try:
            getattr(session, action)()
            refresh_queue()
            report_checkpoint()
        except OSError as exc:
            report(exc)

    def freeze_snapshot():
        save_view()
        try:
            QMessageBox.information(panel, 'Frozen annotations', str(session.freeze()))
        except (ValueError, OSError) as exc:
            report(exc)

    @viewer.mouse_drag_callbacks.append
    def click_cell(_viewer, event):
        # A press-release without movement is a click; dragging still pans/zooms the view.
        if event.button not in (1, 2):
            return
        dragged = False
        yield
        while event.type == 'mouse_move':
            dragged = True
            yield
        if dragged:
            return
        value = labels.get_value(event.position, world=True)
        cell = int(value) if value is not None and int(value) in centers else None
        if event.button == 2:
            cut_through(event.position, cell)  # Right-click only moves the side views.
            return
        cut_through(event.position, cell)
        if cell is None:
            return
        if 'Alt' in {str(m) for m in event.modifiers} or any(getattr(m, 'name', '') == 'Alt' for m in event.modifiers):
            select_cell(int(value))
        else:
            toggle_mark(int(value))

    stack_select.currentIndexChanged.connect(change_stack)
    filter_select.currentIndexChanged.connect(refresh_queue)
    cell_select.currentIndexChanged.connect(lambda *_: select_cell(cell_select.currentData(), move=follow.isChecked()))
    for call, button in call_buttons.items():
        button.clicked.connect(lambda checked=False, call=call: decision(call))
    previous.clicked.connect(lambda: move(-1))
    next_cell.clicked.connect(lambda: move(1))
    goto.clicked.connect(lambda: move_view_to(state['label']))
    approve.clicked.connect(lambda: approve_marked())
    clear.clicked.connect(clear_marks)
    show_positive.toggled.connect(lambda *_: (refresh_overlays(), update_orthos()))
    show_crosshair.toggled.connect(lambda on: setattr(crosshair, 'visible', on))
    viewer.camera.events.center.connect(keep_cursor_in_view)
    viewer.camera.events.zoom.connect(keep_cursor_in_view)
    viewer.dims.events.current_step.connect(schedule_orthos)
    undo.clicked.connect(lambda: history_action('undo'))
    redo.clicked.connect(lambda: history_action('redo'))
    freeze.clicked.connect(freeze_snapshot)
    hcr_visible.toggled.connect(lambda on: setattr(marker_layer, 'visible', on))
    hcr_opacity.valueChanged.connect(lambda v: setattr(marker_layer, 'opacity', v / 100))
    gcamp_opacity.valueChanged.connect(lambda v: setattr(anatomy, 'opacity', v / 100))
    marker_layer.events.opacity.connect(lambda *_: hcr_opacity.setValue(int(round(marker_layer.opacity * 100))))
    anatomy.events.opacity.connect(lambda *_: gcamp_opacity.setValue(int(round(anatomy.opacity * 100))))
    marker_layer.events.visible.connect(lambda *_: hcr_visible.setChecked(marker_layer.visible))
    for key, call in zip(('1', '2', '3', '4', '5'), CALLS):
        viewer.bind_key(key, lambda v, call=call: decision(call), overwrite=True)
    viewer.bind_key('n', lambda v: move(1), overwrite=True)
    viewer.bind_key('p', lambda v: move(-1), overwrite=True)
    viewer.bind_key('m', lambda v: toggle_mark(state['label']), overwrite=True)
    for layer in (anatomy, marker_layer):
        layer.events.contrast_limits.connect(update_orthos)
        layer.events.opacity.connect(update_orthos)
        layer.events.visible.connect(update_orthos)
    marker_layer.events.gamma.connect(update_orthos)
    if session.view.get('anatomy_contrast'):
        anatomy.contrast_limits = session.view['anatomy_contrast']
    settings = session.view.get('stacks', {}).get(first_stack)
    if settings:
        marker_layer.contrast_limits = settings['contrast']
        marker_layer.opacity = settings['opacity']
        marker_layer.gamma = settings.get('gamma', 1.0)
    hcr_opacity.setValue(int(round(marker_layer.opacity * 100)))
    gcamp_opacity.setValue(int(round(anatomy.opacity * 100)))
    if session.view.get('zoom'):
        viewer.camera.zoom = session.view['zoom']
    state['cursor'][:] = [int(viewer.camera.center[-2] / spacing[1]), int(viewer.camera.center[-1] / spacing[2])]
    hist.refresh_data()
    refresh_queue()
    viewer.layers.selection.active = marker_layer
    arrange_panels()
    QTimer.singleShot(0, arrange_panels)  # Again once the window has its final size.
    # Expose controls for deterministic GUI checks without requiring manual clicks.
    if controls is None:
        controls = {}
    controls.update(dict(panel=panel, stack=stack_select, queue=filter_select, cells=cell_select, calls=call_buttons,
        undo=undo, redo=redo, freeze=freeze, status=counts, header=header, save_view=save_view,
        orthogonal_labels=(xz, yz), pick_ortho=pick_ortho, cut_through=cut_through, ortho_state=state, update_orthos=update_orthos, toggle_mark=toggle_mark, select_cell=select_cell, approve=approve_marked,
        approve_button=approve, clear=clear, pending=pending, histogram=hist, restore_panels=restore_panels,
        detail=detail, goto=goto, layers=dict(selected=selected, pending=pending_layer, positive=positive_layer)))
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
