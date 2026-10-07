"""cellMatcher: launcher for manual ex-vivo identity review, export gate, and continuation."""
from __future__ import annotations

import logging
import os
from pathlib import Path
import sys
import traceback

DEFAULT_DATA_ROOT = '/Volumes/dataDrive/dataProcessing/2p_processing'
LOG_DIR = Path.home() / 'Library' / 'Logs' / 'cellMatcher'
ICON = Path(__file__).resolve().parent / 'resources' / 'cellMatcher.png'
log = logging.getLogger('cellMatcher')


def _configure_logging():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(filename=LOG_DIR / 'cellMatcher.log', level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s')
    return LOG_DIR / 'cellMatcher.log'


def _review_progress(dataset_dir):
    import pandas as pd
    table = dataset_dir / 'session' / 'annotations.csv'
    if not table.is_file():
        return 'not started'
    calls = pd.read_csv(table, usecols=['call']).call
    return f'{int((calls != "unreviewed").sum())}/{len(calls)} calls'


def build_launcher(controls=None):
    from qtpy.QtCore import QEvent, QObject, QSettings, QThread, Qt, Signal
    from qtpy.QtGui import QIcon
    from qtpy.QtWidgets import (QApplication, QFileDialog, QHBoxLayout, QHeaderView, QLabel, QLineEdit, QMainWindow,
        QMessageBox, QPlainTextEdit, QPushButton, QTableWidget, QTableWidgetItem, QVBoxLayout, QWidget, QDialog,
        QDialogButtonBox, QAbstractItemView)
    from codeants_2pf_hcr import manual_identity_pipeline as mip

    class Worker(QThread):
        line = Signal(str)
        done = Signal(object, object)

        def __init__(self, function):
            super().__init__()
            self.function = function

        def run(self):
            try:
                result = self.function(self.line.emit)
            except Exception as exc:
                log.exception('Task failed')
                self.done.emit(None, f'{exc}\n\n{traceback.format_exc()}')
            else:
                self.done.emit(result, None)

    class TaskDialog(QDialog):
        """Progress log for long writers; stays open so the outcome can be read."""
        def __init__(self, parent, title, function, on_done):
            super().__init__(parent)
            self.setWindowTitle(title)
            self.resize(720, 420)
            layout = QVBoxLayout(self)
            self.text = QPlainTextEdit(readOnly=True)
            layout.addWidget(self.text)
            self.close_button = QPushButton('Close')
            self.close_button.setEnabled(False)
            self.close_button.clicked.connect(self.accept)
            layout.addWidget(self.close_button)
            self.worker = Worker(function)
            self.worker.line.connect(self.append)
            self.worker.done.connect(lambda result, error: self.finish(result, error, on_done))
            self.worker.start()

        def append(self, text):
            log.info(text)
            self.text.appendPlainText(text)

        def finish(self, result, error, on_done):
            self.close_button.setEnabled(True)
            if error:
                self.append('FAILED: ' + error)
                QMessageBox.critical(self, 'cellMatcher', error.split('\n\n')[0])
            else:
                self.append('Done.')
                on_done(result)

        def reject(self):
            if self.worker.isRunning():
                return  # Writers are journaled; never abandon one mid-promotion.
            super().reject()

    def confirm(parent, title, text, details):
        box = QMessageBox(QMessageBox.Warning, title, text, QMessageBox.Cancel | QMessageBox.Ok, parent)
        box.setDetailedText(details)
        box.setDefaultButton(QMessageBox.Cancel)
        return box.exec_() == QMessageBox.Ok

    class CloseWatcher(QObject):
        """Release the session's writer lock when the review window closes."""
        def __init__(self, callback, parent):
            super().__init__(parent)
            self.callback = callback

        def eventFilter(self, obj, event):
            if event.type() == QEvent.Close:
                self.callback()
            return False

    class Launcher(QMainWindow):
        def __init__(self):
            super().__init__()
            self.settings = QSettings('LarschLab', 'cellMatcher')
            self.setWindowTitle('cellMatcher')
            self.resize(900, 520)
            central = QWidget()
            layout = QVBoxLayout(central)
            row = QHBoxLayout()
            row.addWidget(QLabel('Data root'))
            self.root = QLineEdit(self.settings.value('data_root', DEFAULT_DATA_ROOT))
            browse = QPushButton('Browse…')
            browse.clicked.connect(self.browse)
            row.addWidget(self.root, 1)
            row.addWidget(browse)
            layout.addLayout(row)
            row = QHBoxLayout()
            row.addWidget(QLabel('Reviewer'))
            self.reviewer = QLineEdit(self.settings.value('reviewer', ''))
            self.reviewer.setPlaceholderText('Your name (recorded with every call)')
            row.addWidget(self.reviewer, 1)
            layout.addLayout(row)
            self.table = QTableWidget(0, 5)
            self.table.setHorizontalHeaderLabels(['Fish', 'Ex-vivo masks', 'Dataset', 'Review', 'Identity gate'])
            self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
            self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
            self.table.setSelectionMode(QAbstractItemView.SingleSelection)
            self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
            self.table.itemSelectionChanged.connect(self.update_buttons)
            self.table.doubleClicked.connect(lambda *_: self.open_review())
            layout.addWidget(self.table, 1)
            buttons = QHBoxLayout()
            self.refresh_button = QPushButton('Refresh')
            self.prepare_button = QPushButton('Prepare dataset')
            self.review_button = QPushButton('Open review')
            self.continue_button = QPushButton('Continue pipeline')
            for button, slot in ((self.refresh_button, self.refresh), (self.prepare_button, self.prepare),
                                 (self.review_button, self.open_review), (self.continue_button, self.continue_pipeline)):
                button.clicked.connect(slot)
                buttons.addWidget(button)
            layout.addLayout(buttons)
            self.status = QLabel()
            self.status.setWordWrap(True)
            layout.addWidget(self.status)
            self.setCentralWidget(central)
            self.rows = []
            self.viewers = []
            self.show_viewers = True
            self.refresh()

        def browse(self):
            path = QFileDialog.getExistingDirectory(self, 'Data root', self.root.text())
            if path:
                self.root.setText(path)
                self.refresh()

        def refresh(self):
            self.settings.setValue('data_root', self.root.text())
            self.table.setRowCount(0)
            try:
                self.rows = mip.list_datasets(self.root.text())
            except OSError as exc:
                self.rows = []
                self.status.setText(f'Data root unavailable: {exc}')
            for row in self.rows:
                dataset = row['datasets'][-1] if row['datasets'] else None
                gate = row['gate'] or {}
                values = [row['fish_id'], 'yes' if row['has_exvivo_masks'] else '—',
                          dataset.name if dataset else '—', _review_progress(dataset) if dataset else '—',
                          gate.get('status', '—').replace('_', ' ')]
                index = self.table.rowCount()
                self.table.insertRow(index)
                for column, value in enumerate(values):
                    self.table.setItem(index, column, QTableWidgetItem(value))
            self.update_buttons()

        def selected(self):
            rows = self.table.selectionModel().selectedRows() if self.table.selectionModel() else []
            return self.rows[rows[0].row()] if rows else None

        def update_buttons(self):
            row = self.selected()
            self.prepare_button.setEnabled(bool(row and row['has_exvivo_masks']))
            self.review_button.setEnabled(bool(row and row['datasets']))
            self.continue_button.setEnabled(bool(row and (row['gate'] or {}).get('status') == mip.GATE_AWAITING))
            if row and not row['has_exvivo_masks']:
                self.status.setText('No ex-vivo Cellpose masks yet: expected under 03_analysis/structural/ex_vivo/cp_masks/.')
            elif row and not row['datasets']:
                self.status.setText('Prepare the dataset to bind masks, registered HCR stacks, and coverage.')
            else:
                self.status.setText('')

        def reviewer_name(self):
            name = self.reviewer.text().strip()
            if not name:
                QMessageBox.information(self, 'cellMatcher', 'Enter your reviewer name first.')
                return None
            self.settings.setValue('reviewer', name)
            return name

        def prepare(self):
            row = self.selected()
            root = self.root.text()
            TaskDialog(self, f'Prepare {row["fish_id"]}',
                       lambda emit: mip.prepare_fish_dataset(row['fish_id'], root, log=emit),
                       lambda _: self.refresh()).exec_()

        def open_review(self):
            row = self.selected()
            reviewer = self.reviewer_name()
            if not row or not row['datasets'] or not reviewer:
                return
            from codeants_2pf_hcr.manual_identity import AnnotationSession, Dataset
            from codeants_2pf_hcr.apps.manual_identity import create_viewer
            try:
                dataset = Dataset(row['datasets'][-1] / 'dataset_manifest.json')
                session = AnnotationSession(dataset, mip.session_dir(dataset), reviewer)
            except Exception as exc:
                log.exception('Open review failed')
                QMessageBox.critical(self, 'cellMatcher', str(exc))
                return
            controls = {}
            viewer = create_viewer(dataset, session, show=self.show_viewers, controls=controls)
            viewer.title = f'cellMatcher — {dataset.manifest["fish_id"]}'
            finish = QPushButton('Finish && export identity')
            finish.setToolTip('Freeze calls, write the two ROI identity tables, and stop at the gate.')
            panel_layout = controls['panel'].layout()
            panel_layout.insertWidget(panel_layout.count() - 1, finish)
            finish.clicked.connect(lambda: self.finish_and_export(dataset, session, controls, viewer))

            state = {'closed': False}

            def closed():
                if state['closed']:
                    return
                state['closed'] = True
                try:
                    controls['save_view']()
                finally:
                    session.close()
                    self.viewers.remove(viewer)
                    self.refresh()
            watcher = CloseWatcher(closed, viewer.window._qt_window)
            viewer.window._qt_window.installEventFilter(watcher)
            self.viewers.append(viewer)
            return viewer, controls

        def finish_and_export(self, dataset, session, controls, viewer):
            import pandas as pd
            controls['save_view']()
            try:
                frozen = session.freeze()
            except (OSError, ValueError) as exc:
                QMessageBox.warning(viewer.window._qt_window, 'Snapshot could not be written', str(exc))
                return
            unapproved = sum(len(v) for v in controls['pending'].values())
            if unapproved and QMessageBox.question(
                    viewer.window._qt_window, 'Unapproved marks',
                    f'{unapproved} marked cell(s) are not approved yet and will not be exported. Continue anyway?') != QMessageBox.Yes:
                return
            calls = pd.DataFrame(session.rows.values())
            counts = calls.groupby(['gene', 'call']).size().unstack(fill_value=0)
            root = self.root.text()
            plan = mip.plan_identity_gate(dataset, root)
            details = 'Calls per gene:\n' + counts.to_string() + '\n\nWrites:\n' + '\n'.join(plan['destinations'].values())
            if plan['replace']:
                details += '\n\nReplaces (backed up first):\n' + '\n'.join(plan['replace'])
            if plan['retire']:
                details += '\n\nMoves aside (backed up; recomputed on Continue):\n' + '\n'.join(plan['retire'])
            unreviewed = int((calls.call == 'unreviewed').sum())
            text = (f'Export identity from snapshot {frozen.name}?\n\n'
                    + (f'{unreviewed} calls are still unreviewed and will not count as positive.\n\n' if unreviewed else '')
                    + ('Existing identity outputs will be replaced. ' if plan['replace'] or plan['retire'] else '')
                    + 'The pipeline stops after the two ROI identity tables.')
            if not confirm(viewer.window._qt_window, 'Finish & export', text, details):
                return
            confirmed = plan['replace'] + plan['retire']

            def done(gate):
                lines = [f'{gene}: ' + ', '.join(f'{k}={v}' for k, v in outcome.items()) for gene, outcome in gate['summary'].items()]
                QMessageBox.information(viewer.window._qt_window, 'Identity gate',
                                        'ROI identity tables written. Pipeline stopped at the identity gate.\n\n' + '\n'.join(lines))
                self.refresh()
            TaskDialog(viewer.window._qt_window, 'Finish & export',
                       lambda emit: mip.run_identity_gate(dataset, frozen, root, confirmed=confirmed, log=emit), done).exec_()

        def continue_pipeline(self):
            row = self.selected()
            gate = row['gate']
            details = f'Gate export: {gate["export"]}\nSnapshot: {gate["snapshot"]}\n\nStages:\n' + '\n'.join(w for _, w in mip.CONTINUATION_STAGES)
            if not confirm(self, 'Continue pipeline',
                           f'Continue {row["fish_id"]} past the identity gate?\n\nHCR-centric tables (conf_label = ex-vivo mask ID), canonical tables, '
                           'QA report, motion-AUC tables and figures will be recomputed and replace the current products (backed up first).',
                           details):
                return
            root = self.root.text()
            TaskDialog(self, f'Continue {row["fish_id"]}',
                       lambda emit: mip.continue_after_gate(row['fish_id'], root, log=emit),
                       lambda _: self.refresh()).exec_()

    app = QApplication.instance() or QApplication(sys.argv)
    app.setApplicationName('cellMatcher')
    if ICON.is_file():
        app.setWindowIcon(QIcon(str(ICON)))
    window = Launcher()
    if controls is not None:
        controls['launcher'] = window
    return app, window


def main(argv=None):
    os.environ.setdefault('KMP_DUPLICATE_LIB_OK', 'TRUE')
    log_path = _configure_logging()
    try:
        import napari  # noqa: F401  (initialises napari's Qt application settings)
        app, window = build_launcher()
        window.show()
        return app.exec_()
    except Exception:
        log.exception('cellMatcher failed to start')
        try:
            from qtpy.QtWidgets import QApplication, QMessageBox
            QApplication.instance() or QApplication(sys.argv)
            QMessageBox.critical(None, 'cellMatcher', f'cellMatcher failed to start.\nSee {log_path}')
        except Exception:
            pass
        raise


if __name__ == '__main__':
    raise SystemExit(main())
