"""Review-only anatomy polarity inference from raw in-vivo 2P stacks.

The classifier compares both repository polarity conventions (north=flipY,
south=flipX) with a template built from manually labeled fish.  It deliberately
does not edit fish metadata or feed predictions into canonical preprocessing.
"""

from __future__ import annotations

from dataclasses import dataclass
import csv
import json
from pathlib import Path
from typing import Iterable, Mapping, Sequence

import numpy as np
from skimage import exposure, filters, transform
from skimage.feature import hog
import tifffile

from .context import normalize_polarity_value, read_raw_metadata_polarity


@dataclass(frozen=True)
class AnatomyPolarityConfig:
    projection_percentiles: tuple[int, ...] = (90, 95, 99)
    feature_shape: tuple[int, int] = (128, 128)
    hog_orientations: int = 9
    hog_pixels_per_cell: tuple[int, int] = (16, 16)
    hog_cells_per_block: tuple[int, int] = (2, 2)
    include_edge_features: bool = True
    calibration_fraction: float = 1.0


@dataclass(frozen=True)
class AnatomyPolarityModel:
    templates: Mapping[tuple[int, bool], np.ndarray]
    calibration_floor: float
    config: AnatomyPolarityConfig


@dataclass(frozen=True)
class AnatomyPolarityPrediction:
    fish_id: str
    prediction: str
    status: str
    median_margin: float
    min_abs_margin: float
    unanimous: bool
    variant_count: int
    anatomy_stack_path: Path


def discover_raw_anatomy_stack(fish_dir: Path | str) -> Path:
    hits = discover_raw_anatomy_stacks(fish_dir)
    if len(hits) != 1:
        raise ValueError(
            f"Expected exactly one raw in-vivo anatomy TIFF for {Path(fish_dir).name}; "
            f"found {len(hits)}: {[path.name for path in hits]}"
        )
    return hits[0]


def discover_raw_anatomy_stacks(fish_dir: Path | str) -> tuple[Path, ...]:
    anatomy_dir = Path(fish_dir) / "01_raw" / "2p" / "anatomy"
    hits = sorted(
        path
        for path in anatomy_dir.glob("*.tif*")
        if path.is_file()
        and "ex_vivo" not in path.name.lower()
        and "postclear" not in path.name.lower()
        and "2p_cort" not in path.name.lower()
    )
    return tuple(hits)


def read_anatomy_projection_variants(
    path: Path | str,
    percentiles: Sequence[int] = (90, 95, 99),
) -> dict[int, np.ndarray]:
    """Read legacy TIFF pages directly and return normalized XY projections."""
    target = Path(path)
    pages: list[np.ndarray] = []
    page_shape: tuple[int, int] | None = None
    with tifffile.TiffFile(target) as tif:
        for page in tif.pages:
            data = np.asarray(page.asarray())
            if data.ndim != 2:
                continue
            if page_shape is None:
                page_shape = tuple(int(value) for value in data.shape)
            if tuple(data.shape) == page_shape:
                pages.append(data)
    if not pages:
        raise ValueError(f"No consistent 2D TIFF pages found in {target}")

    stack = np.stack(pages).astype(np.float32)
    raw_min = float(np.min(stack))
    if raw_min < 0:
        stack = stack - raw_min

    projections: dict[int, np.ndarray] = {}
    for percentile in percentiles:
        value = int(percentile)
        if not 0 < value <= 100:
            raise ValueError(f"Projection percentile must be in (0, 100], got {value}")
        projection = np.percentile(stack, value, axis=0).astype(np.float32)
        black, white = np.percentile(projection, (2.0, 99.7))
        projections[value] = np.clip(
            (projection - float(black)) / (float(white) - float(black) + 1e-6),
            0.0,
            1.0,
        ).astype(np.float32)
    return projections


def orient_anatomy_projection(projection: np.ndarray, polarity: str) -> np.ndarray:
    value = normalize_polarity_value(polarity)
    if value == "north":
        return np.asarray(projection)[::-1, :]
    if value == "south":
        return np.asarray(projection)[:, ::-1]
    raise ValueError(f"Expected north/south polarity, got {polarity!r}")


def _feature_vector(projection: np.ndarray, *, edge: bool, config: AnatomyPolarityConfig) -> np.ndarray:
    image = transform.resize(
        np.asarray(projection, dtype=np.float32),
        config.feature_shape,
        preserve_range=True,
        anti_aliasing=True,
    ).astype(np.float32)
    image = exposure.equalize_adapthist(np.clip(image, 0.0, 1.0), clip_limit=0.02).astype(np.float32)
    if edge:
        image = np.hypot(filters.sobel_h(image), filters.sobel_v(image))
    vector = hog(
        image,
        orientations=int(config.hog_orientations),
        pixels_per_cell=tuple(config.hog_pixels_per_cell),
        cells_per_block=tuple(config.hog_cells_per_block),
        feature_vector=True,
    ).astype(np.float32)
    return vector / (float(np.linalg.norm(vector)) + 1e-8)


def _variant_keys(config: AnatomyPolarityConfig) -> tuple[tuple[int, bool], ...]:
    edge_modes = (False, True) if config.include_edge_features else (False,)
    return tuple((int(percentile), edge) for percentile in config.projection_percentiles for edge in edge_modes)


def _oriented_features(
    projections: Mapping[int, np.ndarray],
    config: AnatomyPolarityConfig,
) -> dict[tuple[int, bool, str], np.ndarray]:
    output: dict[tuple[int, bool, str], np.ndarray] = {}
    for percentile, edge in _variant_keys(config):
        if percentile not in projections:
            raise KeyError(f"Missing {percentile}th-percentile projection")
        for polarity in ("north", "south"):
            output[(percentile, edge, polarity)] = _feature_vector(
                orient_anatomy_projection(projections[percentile], polarity),
                edge=edge,
                config=config,
            )
    return output


def build_anatomy_polarity_model(
    reference_projections: Mapping[str, Mapping[int, np.ndarray]],
    labels: Mapping[str, str],
    *,
    config: AnatomyPolarityConfig | None = None,
    calibration_floor: float = 0.0,
) -> AnatomyPolarityModel:
    cfg = config or AnatomyPolarityConfig()
    missing = sorted(set(reference_projections) - set(labels))
    if missing:
        raise ValueError(f"Missing polarity labels for reference fish: {missing}")
    features = {fish_id: _oriented_features(projections, cfg) for fish_id, projections in reference_projections.items()}
    templates: dict[tuple[int, bool], np.ndarray] = {}
    for percentile, edge in _variant_keys(cfg):
        vectors = []
        for fish_id in sorted(reference_projections):
            polarity = normalize_polarity_value(labels[fish_id])
            if polarity not in {"north", "south"}:
                raise ValueError(f"Invalid reference polarity for {fish_id}: {labels[fish_id]!r}")
            vectors.append(features[fish_id][(percentile, edge, polarity)])
        templates[(percentile, edge)] = np.mean(vectors, axis=0).astype(np.float32)
    return AnatomyPolarityModel(templates=templates, calibration_floor=float(calibration_floor), config=cfg)


def predict_anatomy_polarity(
    fish_id: str,
    projections: Mapping[int, np.ndarray],
    anatomy_stack_path: Path | str,
    model: AnatomyPolarityModel,
) -> tuple[AnatomyPolarityPrediction, tuple[float, ...]]:
    features = _oriented_features(projections, model.config)
    margins: list[float] = []
    for percentile, edge in _variant_keys(model.config):
        template = model.templates[(percentile, edge)]
        north = float(features[(percentile, edge, "north")] @ template)
        south = float(features[(percentile, edge, "south")] @ template)
        margins.append(north - south)
    votes = tuple("north" if margin > 0 else "south" for margin in margins)
    unanimous = len(set(votes)) == 1
    min_abs_margin = min(abs(margin) for margin in margins)
    accepted = unanimous and min_abs_margin >= float(model.calibration_floor)
    prediction = votes[0] if accepted else "review"
    result = AnatomyPolarityPrediction(
        fish_id=str(fish_id),
        prediction=prediction,
        status="predicted" if accepted else "review",
        median_margin=float(np.median(margins)),
        min_abs_margin=float(min_abs_margin),
        unanimous=bool(unanimous),
        variant_count=len(margins),
        anatomy_stack_path=Path(anatomy_stack_path),
    )
    return result, tuple(float(value) for value in margins)


def cross_validate_anatomy_polarity(
    reference_projections: Mapping[str, Mapping[int, np.ndarray]],
    labels: Mapping[str, str],
    *,
    config: AnatomyPolarityConfig | None = None,
) -> list[dict[str, object]]:
    """Hold out an entire acquisition prefix such as L396 for each fish."""
    cfg = config or AnatomyPolarityConfig()
    rows: list[dict[str, object]] = []
    for fish_id in sorted(reference_projections):
        group = str(fish_id).split("_", 1)[0]
        train_ids = [candidate for candidate in reference_projections if str(candidate).split("_", 1)[0] != group]
        if len(train_ids) < 2:
            raise ValueError(f"Too few reference fish after holding out acquisition group {group}")
        train_projections = {candidate: reference_projections[candidate] for candidate in train_ids}
        train_labels = {candidate: labels[candidate] for candidate in train_ids}
        model = build_anatomy_polarity_model(train_projections, train_labels, config=cfg)
        prediction, margins = predict_anatomy_polarity(
            fish_id,
            reference_projections[fish_id],
            Path(f"{fish_id}.tif"),
            model,
        )
        true_polarity = normalize_polarity_value(labels[fish_id])
        voted = "north" if float(np.median(margins)) > 0 else "south"
        rows.append(
            {
                "fish_id": fish_id,
                "held_out_group": group,
                "true_polarity": true_polarity,
                "predicted_polarity": voted,
                "correct": voted == true_polarity,
                "unanimous": prediction.unanimous,
                "median_margin": prediction.median_margin,
                "min_abs_margin": prediction.min_abs_margin,
                "training_fish_count": len(train_ids),
            }
        )
    return rows


def _raw_metadata_labels(fish_dirs: Mapping[str, Path]) -> tuple[dict[str, str], dict[str, str]]:
    labels: dict[str, str] = {}
    sources: dict[str, str] = {}
    for fish_id, fish_dir in sorted(fish_dirs.items()):
        polarity, source = read_raw_metadata_polarity(fish_dir)
        if polarity in {"north", "south"}:
            labels[fish_id] = polarity
            sources[fish_id] = source
    return labels, sources


def _fish_dirs(microscopy_roots: Iterable[Path | str]) -> dict[str, Path]:
    output: dict[str, Path] = {}
    for root_value in microscopy_roots:
        root = Path(root_value)
        for fish_dir in sorted(path for path in root.iterdir() if path.is_dir()):
            previous = output.get(fish_dir.name)
            if previous is not None and previous != fish_dir:
                raise ValueError(f"Fish {fish_dir.name} exists under multiple microscopy roots")
            output[fish_dir.name] = fish_dir
    return output


def run_anatomy_polarity_prediction(
    *,
    microscopy_roots: Sequence[Path | str],
    target_microscopy_roots: Sequence[Path | str] | None = None,
    output_dir: Path | str,
    exclude_fish_prefixes: Sequence[str] = (),
    target_fish_ids: Sequence[str] | None = None,
    include_all_fish: bool = False,
    config: AnatomyPolarityConfig | None = None,
) -> dict[str, object]:
    """Run reference validation and write review-only predictions plus QC."""
    cfg = config or AnatomyPolarityConfig()
    fish_dirs = _fish_dirs(microscopy_roots)
    labels_all, label_sources = _raw_metadata_labels(fish_dirs)
    target_fish_dirs = _fish_dirs(target_microscopy_roots) if target_microscopy_roots else fish_dirs
    excluded = tuple(str(value) for value in exclude_fish_prefixes)
    reference_ids = sorted(
        fish_id
        for fish_id in labels_all
        if fish_id in fish_dirs and not any(fish_id.startswith(prefix) for prefix in excluded)
    )
    labels = {fish_id: labels_all[fish_id] for fish_id in reference_ids}
    counts = {polarity: sum(value == polarity for value in labels.values()) for polarity in ("north", "south")}
    if min(counts.values()) < 2:
        raise ValueError(f"Need at least two reference fish per polarity, got {counts}")

    if include_all_fish and target_fish_ids is not None:
        raise ValueError("include_all_fish and target_fish_ids are mutually exclusive")
    if include_all_fish:
        targets = sorted(
            fish_id for fish_id in target_fish_dirs if not any(fish_id.startswith(prefix) for prefix in excluded)
        )
    elif target_fish_ids is None:
        targets: list[str] = []
        for fish_id, fish_dir in sorted(fish_dirs.items()):
            if fish_id in labels_all or any(fish_id.startswith(prefix) for prefix in excluded):
                continue
            raw_polarity, _ = read_raw_metadata_polarity(fish_dir)
            if raw_polarity in {"north", "south"}:
                continue
            try:
                discover_raw_anatomy_stack(fish_dir)
            except ValueError:
                continue
            targets.append(fish_id)
    else:
        targets = [str(value) for value in target_fish_ids]

    reference_projections: dict[str, dict[int, np.ndarray]] = {}
    reference_paths: dict[str, Path] = {}
    for fish_id in reference_ids:
        if fish_id not in fish_dirs:
            raise FileNotFoundError(f"Fish directory not found for {fish_id}")
        anatomy_path = discover_raw_anatomy_stack(fish_dirs[fish_id])
        reference_paths[fish_id] = anatomy_path
        reference_projections[fish_id] = read_anatomy_projection_variants(anatomy_path, cfg.projection_percentiles)
    validation_rows = cross_validate_anatomy_polarity(reference_projections, labels, config=cfg)
    valid_floors = [
        float(row["min_abs_margin"])
        for row in validation_rows
        if bool(row["correct"]) and bool(row["unanimous"])
    ]
    validation_pass = len(valid_floors) == len(validation_rows)
    calibration_floor = min(valid_floors) * float(cfg.calibration_fraction) if validation_pass else float("inf")
    model = build_anatomy_polarity_model(
        reference_projections,
        labels,
        config=cfg,
        calibration_floor=calibration_floor,
    )

    predictions: list[AnatomyPolarityPrediction] = []
    prediction_rows: list[dict[str, object]] = []
    qc_projections: dict[str, np.ndarray] = {}
    for fish_id in targets:
        if fish_id not in fish_dirs:
            raise FileNotFoundError(f"Fish directory not found for {fish_id}")
        raw_polarity, raw_source = read_raw_metadata_polarity(fish_dirs[fish_id])
        known_polarity = raw_polarity if raw_polarity in {"north", "south"} else None
        known_source = raw_source if raw_polarity in {"north", "south"} else label_sources.get(fish_id, "")
        anatomy_candidates = discover_raw_anatomy_stacks(fish_dirs[fish_id])
        if not anatomy_candidates:
            prediction_rows.append(
                {
                    "sample_id": fish_id,
                    "fish_id": fish_id,
                    "stack_index": "",
                    "stack_count": 0,
                    "prediction": "missing",
                    "status": "missing_anatomy",
                    "median_north_minus_south": "",
                    "min_abs_margin": "",
                    "unanimous": "",
                    "variant_count": 0,
                    "known_polarity": known_polarity or "",
                    "known_source": known_source,
                    "agreement": "",
                    "evaluation_role": "missing_anatomy",
                    "anatomy_stack_path": "",
                }
            )
            continue
        for stack_index, anatomy_path in enumerate(anatomy_candidates, start=1):
            sample_id = fish_id if len(anatomy_candidates) == 1 else f"{fish_id}__stack{stack_index}"
            if fish_id in reference_projections and anatomy_path == reference_paths[fish_id]:
                projections = reference_projections[fish_id]
            else:
                projections = read_anatomy_projection_variants(anatomy_path, cfg.projection_percentiles)

            prediction_model = model
            evaluation_role = "unlabeled"
            if fish_id in labels:
                group = fish_id.split("_", 1)[0]
                held_out_ids = [candidate for candidate in reference_ids if candidate.split("_", 1)[0] != group]
                prediction_model = build_anatomy_polarity_model(
                    {candidate: reference_projections[candidate] for candidate in held_out_ids},
                    {candidate: labels[candidate] for candidate in held_out_ids},
                    config=cfg,
                    calibration_floor=calibration_floor,
                )
                evaluation_role = "held_out_manual_reference"
            elif known_polarity in {"north", "south"}:
                evaluation_role = "out_of_sample_known_metadata"

            prediction, _ = predict_anatomy_polarity(sample_id, projections, anatomy_path, prediction_model)
            predictions.append(prediction)
            qc_projections[sample_id] = projections[max(cfg.projection_percentiles)]
            agreement: bool | str = ""
            if prediction.prediction in {"north", "south"} and known_polarity in {"north", "south"}:
                agreement = prediction.prediction == known_polarity
            prediction_rows.append(
                {
                    "sample_id": sample_id,
                    "fish_id": fish_id,
                    "stack_index": stack_index,
                    "stack_count": len(anatomy_candidates),
                    "prediction": prediction.prediction,
                    "status": prediction.status,
                    "median_north_minus_south": prediction.median_margin,
                    "min_abs_margin": prediction.min_abs_margin,
                    "unanimous": prediction.unanimous,
                    "variant_count": prediction.variant_count,
                    "known_polarity": known_polarity or "",
                    "known_source": known_source,
                    "agreement": agreement,
                    "evaluation_role": evaluation_role,
                    "anatomy_stack_path": str(prediction.anatomy_stack_path),
                }
            )

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    validation_csv = out_dir / "anatomy_polarity_validation.csv"
    prediction_csv = out_dir / "anatomy_polarity_predictions.csv"
    summary_json = out_dir / "anatomy_polarity_summary.json"
    with validation_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(validation_rows[0]))
        writer.writeheader()
        writer.writerows(validation_rows)
    with prediction_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(prediction_rows[0]) if prediction_rows else ["fish_id"])
        writer.writeheader()
        writer.writerows(prediction_rows)

    from .plots.orientation import render_anatomy_polarity_qc

    qc_png = out_dir / "anatomy_polarity_qc.png"
    render_anatomy_polarity_qc(rows=prediction_rows, projections=qc_projections, output_path=qc_png)
    comparable_rows = [row for row in prediction_rows if row["agreement"] in {True, False}]
    summary = {
        "status": "pass" if validation_pass else "review",
        "reference_fish_count": len(reference_ids),
        "reference_counts": counts,
        "excluded_fish_prefixes": list(excluded),
        "validation_correct": sum(bool(row["correct"]) for row in validation_rows),
        "validation_count": len(validation_rows),
        "validation_all_unanimous": all(bool(row["unanimous"]) for row in validation_rows),
        "calibration_floor": calibration_floor if np.isfinite(calibration_floor) else None,
        "target_fish_count": len(targets),
        "prediction_count": len(predictions),
        "predicted_count": sum(item.status == "predicted" for item in predictions),
        "review_count": sum(item.status == "review" for item in predictions),
        "missing_anatomy_count": sum(row["status"] == "missing_anatomy" for row in prediction_rows),
        "known_comparison_count": len(comparable_rows),
        "known_agreement_count": sum(row["agreement"] is True for row in comparable_rows),
        "metadata_writes": False,
        "outputs": {
            "validation_csv": str(validation_csv),
            "prediction_csv": str(prediction_csv),
            "qc_png": str(qc_png),
        },
    }
    summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary


__all__ = [
    "AnatomyPolarityConfig",
    "AnatomyPolarityModel",
    "AnatomyPolarityPrediction",
    "build_anatomy_polarity_model",
    "cross_validate_anatomy_polarity",
    "discover_raw_anatomy_stack",
    "discover_raw_anatomy_stacks",
    "orient_anatomy_projection",
    "predict_anatomy_polarity",
    "read_anatomy_projection_variants",
    "run_anatomy_polarity_prediction",
]
