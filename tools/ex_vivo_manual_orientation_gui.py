#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from codeants_2pf_hcr import (
    ManualAnatomyOrientationConfig,
    apply_manual_anatomy_orientation_stage,
)


BRAIN_ATLAS_REPO = Path("/Users/ddharmap/gitRepo/brainAtlas")
if str(BRAIN_ATLAS_REPO) not in sys.path:
    sys.path.insert(0, str(BRAIN_ATLAS_REPO))

try:
    from PySide6.QtCore import Qt
    from PySide6.QtWidgets import (
        QApplication,
        QCheckBox,
        QDoubleSpinBox,
        QHBoxLayout,
        QLabel,
        QListWidget,
        QListWidgetItem,
        QMainWindow,
        QMessageBox,
        QPushButton,
        QSpinBox,
        QVBoxLayout,
        QWidget,
    )
    from brain_atlas_preprocess.widgets import RotationPreview
except Exception as exc:  # pragma: no cover - import failure is reported by main.
    QT_IMPORT_ERROR = exc
else:
    QT_IMPORT_ERROR = None


@dataclass(frozen=True)
class ManualOrientationItem:
    fish_id: str
    input_path: Path
    source_metadata_path: Path

    @property
    def output_path(self) -> Path:
        return self.input_path.with_name(f"{self.input_path.stem}_manual_oriented.nrrd")

    @property
    def output_metadata_path(self) -> Path:
        return self.output_path.with_name(self.output_path.name + ".json")

    @property
    def review_path(self) -> Path:
        return self.input_path.with_name(f"{self.input_path.stem}_manual_orientation_review.json")

    @property
    def qc_path(self) -> Path:
        return self.input_path.with_name(f"{self.fish_id}_exvivo_manual_orientation_qc.png")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Review and apply manual XY orientation for ex vivo 2P anatomy NRRDs."
    )
    parser.add_argument(
        "fish_dirs",
        nargs="+",
        type=Path,
        help="Fish Microscopy folders containing 02_reg/00_preprocessing/2p_anatomy/ex_vivo outputs.",
    )
    parser.add_argument(
        "--crop-size-px",
        type=int,
        default=750,
        help="Initial crop overlay size. Cropping is disabled unless the crop checkbox is enabled.",
    )
    return parser.parse_args()


def _item_from_fish_dir(fish_dir: Path) -> ManualOrientationItem:
    fish_dir = fish_dir.expanduser().resolve()
    fish_id = fish_dir.name
    ex_dir = fish_dir / "02_reg" / "00_preprocessing" / "2p_anatomy" / "ex_vivo"
    input_path = ex_dir / f"{fish_id}_exvivo_anatomy_2P_GCaMP_uint8.nrrd"
    if not input_path.exists():
        raise FileNotFoundError(f"Missing ex vivo pre-rotation NRRD: {input_path}")
    source_metadata_path = input_path.with_name(input_path.name + ".json")
    if not source_metadata_path.exists():
        raise FileNotFoundError(f"Missing ex vivo preprocessing metadata: {source_metadata_path}")
    return ManualOrientationItem(
        fish_id=fish_id,
        input_path=input_path,
        source_metadata_path=source_metadata_path,
    )


def _read_nrrd_zyx(path: Path) -> np.ndarray:
    import nrrd

    data, _header = nrrd.read(str(path), index_order="C")
    return np.asarray(data)


def _normalize_for_preview(image: np.ndarray) -> np.ndarray:
    arr = np.asarray(image, dtype=np.float32)
    finite = np.isfinite(arr)
    if not finite.any():
        return np.zeros(arr.shape, dtype=np.uint8)
    low, high = np.percentile(arr[finite], [1, 99.5])
    if not np.isfinite(low) or not np.isfinite(high) or high <= low:
        low = float(arr[finite].min())
        high = float(arr[finite].max())
    if high <= low:
        return np.zeros(arr.shape, dtype=np.uint8)
    arr = np.clip((arr - low) / (high - low), 0.0, 1.0)
    return np.rint(arr * 255.0).astype(np.uint8)


def _load_review(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text())
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _manual_orientation_from_metadata(path: Path) -> dict[str, Any]:
    data = _load_review(path)
    manual = data.get("manual_orientation", data)
    return manual if isinstance(manual, dict) else {}


def _write_qc(item: ManualOrientationItem) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    source = _read_nrrd_zyx(item.input_path)
    oriented = _read_nrrd_zyx(item.output_path)
    source_mip = _normalize_for_preview(np.max(source, axis=0))
    oriented_mip = _normalize_for_preview(np.max(oriented, axis=0))

    fig, axes = plt.subplots(1, 2, figsize=(8, 4), constrained_layout=True)
    axes[0].imshow(source_mip, cmap="gray")
    axes[0].set_title("pre-manual maxZ")
    axes[1].imshow(oriented_mip, cmap="gray")
    axes[1].set_title("manual-oriented maxZ")
    for ax in axes:
        ax.axis("off")
    fig.suptitle(f"{item.fish_id} ex vivo manual orientation")
    fig.savefig(item.qc_path, dpi=160)
    plt.close(fig)


class ManualOrientationWindow(QMainWindow):
    def __init__(self, items: list[ManualOrientationItem], crop_size_px: int) -> None:
        super().__init__()
        self.items = items
        self.current_index = 0
        self.preview_arrays: dict[Path, np.ndarray] = {}
        self.setWindowTitle("codeANTs ex vivo manual orientation")

        self.list_widget = QListWidget()
        for item in items:
            row = QListWidgetItem(item.fish_id)
            row.setData(Qt.ItemDataRole.UserRole, str(item.input_path))
            self.list_widget.addItem(row)
        self.list_widget.currentRowChanged.connect(self._select_index)

        self.preview = RotationPreview()
        self.preview.set_crop_size(crop_size_px)
        self.preview.angleChanged.connect(self._angle_changed)
        self.preview.cropCenterChanged.connect(self._crop_center_changed)

        self.angle_input = QDoubleSpinBox()
        self.angle_input.setRange(-720.0, 720.0)
        self.angle_input.setDecimals(2)
        self.angle_input.setSingleStep(0.25)
        self.angle_input.valueChanged.connect(self._angle_input_changed)

        self.crop_enabled = QCheckBox("Apply crop")
        self.crop_enabled.setChecked(True)
        self.crop_enabled.stateChanged.connect(lambda _state: self._save_review_for_current())

        self.crop_size_input = QSpinBox()
        self.crop_size_input.setRange(1, 10000)
        self.crop_size_input.setValue(int(crop_size_px))
        self.crop_size_input.valueChanged.connect(self._crop_size_changed)

        save_button = QPushButton("Save Review")
        save_button.clicked.connect(self._save_review_for_current)
        apply_button = QPushButton("Apply Current")
        apply_button.clicked.connect(self._apply_current)
        apply_all_button = QPushButton("Apply All Reviewed")
        apply_all_button.clicked.connect(self._apply_all_reviewed)
        next_button = QPushButton("Next")
        next_button.clicked.connect(self._next_item)

        controls = QHBoxLayout()
        controls.addWidget(QLabel("Angle"))
        controls.addWidget(self.angle_input)
        controls.addWidget(self.crop_enabled)
        controls.addWidget(QLabel("Crop size"))
        controls.addWidget(self.crop_size_input)
        controls.addWidget(save_button)
        controls.addWidget(apply_button)
        controls.addWidget(apply_all_button)
        controls.addWidget(next_button)

        right = QVBoxLayout()
        right.addWidget(self.preview, stretch=1)
        right.addLayout(controls)

        root = QHBoxLayout()
        root.addWidget(self.list_widget, stretch=0)
        root.addLayout(right, stretch=1)

        central = QWidget()
        central.setLayout(root)
        self.setCentralWidget(central)
        self.resize(1100, 760)
        self.list_widget.setCurrentRow(0)

    def _current_item(self) -> ManualOrientationItem:
        return self.items[self.current_index]

    def _select_index(self, index: int) -> None:
        if index < 0 or index >= len(self.items):
            return
        self.current_index = index
        item = self._current_item()
        arr = self.preview_arrays.get(item.input_path)
        if arr is None:
            arr = _read_nrrd_zyx(item.input_path)
            self.preview_arrays[item.input_path] = arr
        mip = _normalize_for_preview(np.max(arr, axis=0))
        review = _manual_orientation_from_metadata(item.review_path)
        if not review and item.output_metadata_path.exists():
            review = _manual_orientation_from_metadata(item.output_metadata_path)
        angle = float(review.get("rotation_degrees", 0.0) or 0.0)
        crop_center = review.get("crop_center_yx")
        crop_size = int(review.get("crop_size_px") or self.crop_size_input.value())
        crop_is_enabled = True if not review else (crop_center is not None or review.get("crop_size_px") is not None)

        self.preview.set_image(mip, 920)
        self.preview.set_angle(angle)
        self.preview.set_crop_center(tuple(crop_center) if crop_center is not None else None)
        self.preview.set_crop_size(crop_size)
        self.angle_input.blockSignals(True)
        self.angle_input.setValue(angle)
        self.angle_input.blockSignals(False)
        self.crop_size_input.blockSignals(True)
        self.crop_size_input.setValue(crop_size)
        self.crop_size_input.blockSignals(False)
        self.crop_enabled.blockSignals(True)
        self.crop_enabled.setChecked(bool(crop_is_enabled))
        self.crop_enabled.blockSignals(False)
        self.statusBar().showMessage(str(item.input_path))

    def _angle_changed(self, angle: float) -> None:
        self.angle_input.blockSignals(True)
        self.angle_input.setValue(float(angle))
        self.angle_input.blockSignals(False)
        self._save_review_for_current()

    def _angle_input_changed(self, angle: float) -> None:
        self.preview.set_angle(float(angle))
        self._save_review_for_current()

    def _crop_center_changed(self, _center_yx: object) -> None:
        self.crop_enabled.setChecked(True)
        self._save_review_for_current()

    def _crop_size_changed(self, size_px: int) -> None:
        self.preview.set_crop_size(int(size_px))
        self._save_review_for_current()

    def _review_payload(self, item: ManualOrientationItem) -> dict[str, Any]:
        crop_center = self.preview._crop_center_yx if self.crop_enabled.isChecked() else None
        crop_size = int(self.crop_size_input.value()) if self.crop_enabled.isChecked() else None
        preview_angle = float(self.preview._angle)
        return {
            "stage": "ex_vivo_manual_orientation_gui",
            "fish_id": item.fish_id,
            "input_path": str(item.input_path),
            "source_metadata_path": str(item.source_metadata_path),
            "output_path": str(item.output_path),
            "manual_orientation": {
                "rotation_degrees": preview_angle,
                "crop_center_yx": list(crop_center) if crop_center is not None else None,
                "crop_size_px": crop_size,
                "interpolation": "linear",
                "expand_canvas": True,
                "rot90_k": 0,
                "flip_x": False,
                "flip_y": False,
                "flip_z": False,
            },
        }

    def _save_review_for_current(self) -> None:
        if not self.items:
            return
        item = self._current_item()
        item.review_path.write_text(json.dumps(self._review_payload(item), indent=2, sort_keys=True))
        self.statusBar().showMessage(f"Saved review: {item.review_path}", 1200)

    def _apply_item_from_review(self, item: ManualOrientationItem) -> Path:
        review = _manual_orientation_from_metadata(item.review_path)
        if not review:
            raise RuntimeError(f"No saved review parameters for {item.fish_id}: {item.review_path}")
        result = apply_manual_anatomy_orientation_stage(
            input_path=item.input_path,
            source_metadata_path=item.source_metadata_path,
            config=ManualAnatomyOrientationConfig(
                force_recompute=True,
                rotation_degrees=float(review.get("rotation_degrees", 0.0) or 0.0),
                crop_center_yx=tuple(review["crop_center_yx"]) if review.get("crop_center_yx") is not None else None,
                crop_size_px=int(review["crop_size_px"]) if review.get("crop_size_px") is not None else None,
                interpolation=str(review.get("interpolation", "linear")),
                expand_canvas=bool(review.get("expand_canvas", True)),
                rot90_k=int(review.get("rot90_k", 0) or 0),
                flip_x=bool(review.get("flip_x", False)),
                flip_y=bool(review.get("flip_y", False)),
                flip_z=bool(review.get("flip_z", False)),
            ),
        )
        _write_qc(item)
        try:
            item.review_path.unlink()
        except FileNotFoundError:
            pass
        return result["bindings"]["MANUAL_ORIENTED_ANAT_NRRD"]

    def _apply_current(self) -> None:
        self._save_review_for_current()
        item = self._current_item()
        try:
            out_path = self._apply_item_from_review(item)
        except Exception as exc:
            QMessageBox.critical(self, "Apply failed", str(exc))
            return
        applied = _manual_orientation_from_metadata(item.output_metadata_path)
        QMessageBox.information(
            self,
            "Applied",
            "Wrote:\n"
            f"{out_path}\n\n"
            f"Stored angle: {float(applied.get('rotation_degrees', 0.0)):.2f} deg\n"
            f"Crop size: {applied.get('crop_size_px')}\n\n"
            f"QC:\n{item.qc_path}",
        )

    def _apply_all_reviewed(self) -> None:
        self._save_review_for_current()
        written: list[Path] = []
        try:
            for item in self.items:
                if item.review_path.exists():
                    written.append(self._apply_item_from_review(item))
        except Exception as exc:
            QMessageBox.critical(self, "Apply failed", str(exc))
            return
        QMessageBox.information(self, "Applied", "Wrote:\n" + "\n".join(str(path) for path in written))

    def _next_item(self) -> None:
        self._save_review_for_current()
        next_index = min(self.current_index + 1, len(self.items) - 1)
        self.list_widget.setCurrentRow(next_index)


def main() -> int:
    if QT_IMPORT_ERROR is not None:
        raise SystemExit(f"Could not import PySide6/brainAtlas GUI components: {QT_IMPORT_ERROR}")
    args = _parse_args()
    try:
        items = [_item_from_fish_dir(path) for path in args.fish_dirs]
    except Exception as exc:
        raise SystemExit(str(exc)) from exc
    app = QApplication(sys.argv)
    window = ManualOrientationWindow(items, crop_size_px=int(args.crop_size_px))
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
