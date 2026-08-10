import numpy as np
import tifffile

from codeants_2pf_hcr.orientation import (
    AnatomyPolarityConfig,
    build_anatomy_polarity_model,
    cross_validate_anatomy_polarity,
    discover_raw_anatomy_stack,
    orient_anatomy_projection,
    predict_anatomy_polarity,
    read_anatomy_projection_variants,
)


def _canonical_image(offset: int = 0) -> np.ndarray:
    image = np.zeros((96, 96), dtype=np.float32)
    image[12 + offset : 70 + offset, 18:34] = 0.8
    image[20 + offset : 38 + offset, 34:82] = 1.0
    image[65 + offset : 78 + offset, 25:48] = 0.45
    return image


def _raw_from_canonical(image: np.ndarray, polarity: str) -> np.ndarray:
    return image[::-1, :] if polarity == "north" else image[:, ::-1]


def _variants(image: np.ndarray) -> dict[int, np.ndarray]:
    return {90: image, 95: image, 99: image}


def test_orientation_convention_matches_pipeline_effective_flips():
    image = np.arange(12).reshape(3, 4)
    np.testing.assert_array_equal(orient_anatomy_projection(image, "north"), image[::-1, :])
    np.testing.assert_array_equal(orient_anatomy_projection(image, "south"), image[:, ::-1])


def test_projection_reader_handles_negative_legacy_pages(tmp_path):
    path = tmp_path / "anatomy.tif"
    stack = np.stack([np.arange(64).reshape(8, 8) + index - 20 for index in range(5)]).astype(np.int16)
    tifffile.imwrite(path, stack, photometric="minisblack")
    projections = read_anatomy_projection_variants(path, (90, 99))
    assert set(projections) == {90, 99}
    assert projections[99].shape == (8, 8)
    assert float(projections[99].min()) >= 0.0
    assert float(projections[99].max()) <= 1.0


def test_reference_model_predicts_both_polarities_and_cross_validates_groups():
    labels = {"L100_f01": "north", "L200_f01": "south", "L300_f01": "north", "L400_f01": "south"}
    projections = {
        fish_id: _variants(_raw_from_canonical(_canonical_image(index % 2), polarity))
        for index, (fish_id, polarity) in enumerate(labels.items())
    }
    config = AnatomyPolarityConfig(include_edge_features=True)
    validation = cross_validate_anatomy_polarity(projections, labels, config=config)
    assert all(row["correct"] for row in validation)
    assert all(row["unanimous"] for row in validation)

    model = build_anatomy_polarity_model(projections, labels, config=config)
    for polarity in ("north", "south"):
        raw = _raw_from_canonical(_canonical_image(), polarity)
        result, margins = predict_anatomy_polarity("target", _variants(raw), "target.tif", model)
        assert result.prediction == polarity
        assert result.unanimous
        assert len(margins) == 6


def test_prediction_abstains_when_calibration_floor_is_not_met():
    labels = {"L100_f01": "north", "L200_f01": "south"}
    projections = {fish_id: _variants(_raw_from_canonical(_canonical_image(), polarity)) for fish_id, polarity in labels.items()}
    model = build_anatomy_polarity_model(projections, labels, calibration_floor=1.0)
    result, _ = predict_anatomy_polarity(
        "target",
        _variants(_raw_from_canonical(_canonical_image(), "north")),
        "target.tif",
        model,
    )
    assert result.prediction == "review"
    assert result.status == "review"


def test_discover_raw_anatomy_stack_excludes_ex_vivo_and_postclear(tmp_path):
    anatomy_dir = tmp_path / "L100_f01" / "01_raw" / "2p" / "anatomy"
    anatomy_dir.mkdir(parents=True)
    expected = anatomy_dir / "L100_f01_anatomy_00001.tif"
    for path in (
        expected,
        anatomy_dir / "L100_f01_anatomy_ex_vivo_00001.tif",
        anatomy_dir / "L100_f01_anatomy_postClear_00001.tif",
    ):
        path.touch()
    assert discover_raw_anatomy_stack(anatomy_dir.parents[2]) == expected
