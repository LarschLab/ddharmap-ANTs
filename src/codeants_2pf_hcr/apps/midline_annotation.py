"""Native, review-only GUI for legacy functional-plane midline annotation."""
from __future__ import annotations

import argparse
from pathlib import Path
import sys

import numpy as np
from PySide6.QtCore import QEvent, Qt
from PySide6.QtGui import QColor, QImage, QPainter, QPen, QPixmap
from PySide6.QtWidgets import (
    QApplication, QComboBox, QFormLayout, QHBoxLayout, QLabel, QLineEdit,
    QMainWindow, QMessageBox, QPushButton, QSplitter, QTextEdit, QVBoxLayout,
    QWidget,
)

from codeants_2pf_hcr.plots.qc_midline import (
    MidlineAnnotationSession,
    load_legacy_midline_annotation_session,
    load_native_functional_midline_annotation_session,
)


TRAINING_FISH = (
    "L395_f06", "L395_f10", "L395_f11", "L396_f01", "L396_f02", "L396_f03",
    "L396_f04", "L396_f05", "L396_f06", "L396_f07", "L396_f08", "L432_f01",
    "L432_f02", "L432_f03", "L432_f04", "L432_f05", "L432_f06", "L432_f07",
    "L432_f08", "L432_f09", "L432_f10",
)
DEFAULT_LEGACY_ROOT = Path("/Volumes/jlarsch/default/D2c/07_Data/Matilde/Microscopy")


def _to_pixmap(image: np.ndarray) -> QPixmap:
    gray = np.clip(np.asarray(image, dtype=float) * 255, 0, 255).astype(np.uint8)
    qimage = QImage(gray.data, gray.shape[1], gray.shape[0], gray.strides[0], QImage.Format.Format_Grayscale8).copy()
    return QPixmap.fromImage(qimage)


class AnnotationCanvas(QWidget):
    """Anatomy canvas that records exactly two image-grid clicks per plane."""
    def __init__(self, owner: "MidlineAnnotationWindow") -> None:
        super().__init__(); self.owner = owner; self.setMinimumSize(620, 620); self.setFocusPolicy(Qt.FocusPolicy.StrongFocus)

    def _image_rect(self):
        pixmap = _to_pixmap(self.owner.current_image())
        available = self.rect().adjusted(12, 12, -12, -12)
        scaled = pixmap.scaled(available.size(), Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation)
        return pixmap, scaled, (self.width() - scaled.width()) / 2, (self.height() - scaled.height()) / 2

    def paintEvent(self, _event) -> None:
        painter = QPainter(self); painter.fillRect(self.rect(), QColor("#15171a"))
        pixmap, scaled, x0, y0 = self._image_rect(); painter.drawPixmap(int(x0), int(y0), scaled)
        sy, sx = scaled.height() / pixmap.height(), scaled.width() / pixmap.width()
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        painter.setPen(QPen(QColor("#ff4dd2"), 1))
        for x, y in self.owner.current_centroids():
            painter.drawEllipse(int(x0 + x * sx - 2), int(y0 + y * sy - 2), 4, 4)
        line = self.owner.current_line()
        if line:
            length = max(pixmap.width(), pixmap.height()) * 2; angle = np.deg2rad(line["theta_deg"])
            painter.setPen(QPen(QColor("#19d3ff"), 2))
            painter.drawLine(int(x0 + (line["x0"] - length * np.cos(angle)) * sx), int(y0 + (line["y0"] - length * np.sin(angle)) * sy), int(x0 + (line["x0"] + length * np.cos(angle)) * sx), int(y0 + (line["y0"] + length * np.sin(angle)) * sy))

    def mousePressEvent(self, event) -> None:
        self.setFocus(Qt.FocusReason.MouseFocusReason)
        if event.button() != Qt.MouseButton.LeftButton: return
        pixmap, scaled, x0, y0 = self._image_rect()
        x, y = event.position().x(), event.position().y()
        if not (x0 <= x <= x0 + scaled.width() and y0 <= y <= y0 + scaled.height()): return
        self.owner.add_point(((x - x0) * pixmap.width() / scaled.width(), (y - y0) * pixmap.height() / scaled.height()))


class MidlineAnnotationWindow(QMainWindow):
    def __init__(self, fish_roots: list[Path], session_loader=load_legacy_midline_annotation_session) -> None:
        super().__init__(); self.fish_roots = fish_roots; self.session_loader = session_loader; self.sessions: dict[Path, MidlineAnnotationSession] = {}; self.points: list[tuple[float, float]] = []
        self.setWindowTitle("Manual Midline Annotation — review-only"); self.resize(1180, 820)
        self.fish_select = QComboBox(); self.fish_select.addItems([path.name for path in fish_roots]); self.fish_select.currentIndexChanged.connect(self._change_fish)
        self.plane_select = QComboBox(); self.plane_select.currentIndexChanged.connect(self._change_plane)
        self.status = QLabel(); self.reviewer = QLineEdit(); self.notes = QTextEdit(); self.notes.setFixedHeight(75); self.canvas = AnnotationCanvas(self)
        copy = QPushButton("Copy prior line"); copy.clicked.connect(self.copy_prior); clear = QPushButton("Clear current line"); clear.clicked.connect(self.clear_current); save = QPushButton("Save accepted sidecar"); save.clicked.connect(self.save)
        controls = QWidget(); form = QFormLayout(controls); form.addRow("Fish", self.fish_select); form.addRow("Functional plane", self.plane_select); form.addRow(copy); form.addRow(clear); form.addRow("Reviewer", self.reviewer); form.addRow("Notes", self.notes); form.addRow(save); form.addRow("Status", self.status)
        splitter = QSplitter(Qt.Orientation.Horizontal); splitter.addWidget(controls); splitter.addWidget(self.canvas); splitter.setStretchFactor(1, 1); self.setCentralWidget(splitter)
        QApplication.instance().installEventFilter(self); self._change_fish()

    @property
    def session(self) -> MidlineAnnotationSession: return self.sessions[self.fish_roots[self.fish_select.currentIndex()]]
    @property
    def plane(self) -> int: return int(self.plane_select.currentData())
    def _change_fish(self, *_args) -> None:
        root = self.fish_roots[self.fish_select.currentIndex()]
        if root not in self.sessions: self.sessions[root] = self.session_loader(root)
        self.plane_select.blockSignals(True); self.plane_select.clear()
        for plane in sorted(self.session.plane_best_z): self.plane_select.addItem(f"{plane}  (anatomy z={self.session.plane_best_z[plane]})", plane)
        self.plane_select.blockSignals(False); self._change_plane()
    def _change_plane(self, *_args) -> None: self.points = []; self._refresh()
    def _move_fish(self, direction: int) -> None:
        self.fish_select.setCurrentIndex((self.fish_select.currentIndex() + direction) % self.fish_select.count())
        self.canvas.setFocus(Qt.FocusReason.ShortcutFocusReason)
    def _move_plane(self, direction: int) -> None:
        self.plane_select.setCurrentIndex((self.plane_select.currentIndex() + direction) % self.plane_select.count())
        self.canvas.setFocus(Qt.FocusReason.ShortcutFocusReason)
    def eventFilter(self, watched, event):
        """Keep text entry intact while making canvas/control focus navigable."""
        if event.type() == QEvent.Type.KeyPress and not isinstance(QApplication.focusWidget(), (QLineEdit, QTextEdit)):
            handlers = {Qt.Key.Key_Q: lambda: self._move_fish(-1), Qt.Key.Key_E: lambda: self._move_fish(1), Qt.Key.Key_A: lambda: self._move_plane(-1), Qt.Key.Key_D: lambda: self._move_plane(1)}
            handler = handlers.get(event.key())
            if handler is not None:
                handler(); return True
        return super().eventFilter(watched, event)
    def current_image(self) -> np.ndarray: return self.session.anatomy_plane(self.plane)
    def current_centroids(self) -> np.ndarray: return self.session.functional_roi_centroids(self.plane)
    def current_line(self): return self.session.lines_by_plane.get(self.plane)
    def add_point(self, point: tuple[float, float]) -> None:
        self.points.append(point)
        if len(self.points) == 2: self.session.set_line_from_points(self.plane, self.points); self.points = []
        self._refresh()
    def copy_prior(self) -> None:
        try: self.session.copy_previous(self.plane)
        except ValueError as exc: self.status.setText(str(exc)); return
        self._refresh()
    def clear_current(self) -> None: self.session.lines_by_plane.pop(self.plane, None); self._refresh()
    def _refresh(self) -> None:
        complete, total = len(self.session.lines_by_plane), len(self.session.plane_best_z)
        self.status.setText(f"{self.session.fish_id}: {complete}/{total} planes annotated. Click two endpoints to set or correct; Q/E fish, A/D plane (outside Reviewer/Notes)."); self.canvas.update()
    def save(self) -> None:
        self.session.accepted = True
        try: target = self.session.write_sidecar(self.fish_roots[self.fish_select.currentIndex()] / "03_analysis/functional/midline_annotation_reviews", reviewer=self.reviewer.text(), notes=self.notes.toPlainText())
        except (ValueError, OSError) as exc: self.session.accepted = False; QMessageBox.warning(self, "Cannot save annotation", str(exc)); return
        QMessageBox.information(self, "Accepted review sidecar saved", str(target)); self.status.setText(f"Saved: {target.name}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Launch review-only manual midline annotation.")
    parser.add_argument("--fish-root", action="append", type=Path)
    parser.add_argument("--training-cohort", action="store_true")
    parser.add_argument("--native-functional", action="store_true", help="Annotate persisted native functional reference grids without anatomy registration.")
    parser.add_argument("--legacy-root", type=Path, default=DEFAULT_LEGACY_ROOT)
    args = parser.parse_args(argv)
    roots = args.fish_root or ([args.legacy_root / fish for fish in TRAINING_FISH] if args.training_cohort else [])
    if not roots: parser.error("provide --fish-root or --training-cohort")
    missing = [str(path) for path in roots if not path.is_dir()]
    if missing: parser.error("missing fish roots: " + ", ".join(missing))
    eligible, unavailable = [], []
    loader = load_native_functional_midline_annotation_session if args.native_functional else load_legacy_midline_annotation_session
    for root in roots:
        try:
            loader(root)
        except (FileNotFoundError, ValueError) as exc:
            unavailable.append(f"{root.name}: {exc}")
        else:
            eligible.append(root)
    if unavailable:
        print("Excluded from annotation because persisted anatomy-grid evidence is incomplete:\n" + "\n".join(unavailable), file=sys.stderr)
    if not eligible: parser.error("no selected fish have complete annotation inputs")
    app = QApplication.instance() or QApplication(sys.argv); window = MidlineAnnotationWindow(eligible, session_loader=loader); window.show(); return app.exec()


if __name__ == "__main__": raise SystemExit(main())
