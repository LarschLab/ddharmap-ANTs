import json

import numpy as np
import pandas as pd
import tifffile

from codeants_2pf_hcr.z_drift import (
    FunctionalZDriftConfig,
    _interval_reference,
    _interval_reference_from_tiff,
    _resolve_drift_input_provenance,
    interval_block_labels,
    interval_bounds,
    load_functional_frame_selection,
    load_functional_polarity,
    load_plane_scales,
    quadratic_peak_z,
    summarize_session_drift,
)
from codeants_2pf_hcr.spatial import apply_func_orientation


def test_interval_bounds_cover_movie_without_overlap():
    bounds = interval_bounds(5502, 6)
    assert bounds == ((0, 917), (917, 1834), (1834, 2751), (2751, 3668), (3668, 4585), (4585, 5502))


def test_interval_block_labels_name_retained_block_thirds():
    assert interval_block_labels(
        excluded_block_frame_count=1834,
        retained_frame_count=3668,
        interval_count=6,
    ) == (
        "Block 1\nfirst third",
        "Block 1\nmiddle third",
        "Block 1\nfinal third",
        "Block 2\nfirst third",
        "Block 2\nmiddle third",
        "Block 2\nfinal third",
    )


def test_interval_block_labels_reject_misaligned_windows():
    with np.testing.assert_raises_regex(ValueError, "divide evenly"):
        interval_block_labels(excluded_block_frame_count=1834, retained_frame_count=3668, interval_count=5)


def test_quadratic_peak_z_recovers_fractional_peak():
    z = np.arange(9, dtype=float)
    scores = -((z - 4.25) ** 2)
    assert np.isclose(quadratic_peak_z(scores), 4.25)


def test_quadratic_peak_z_keeps_edge_peak_discrete():
    assert quadratic_peak_z(np.array([3.0, 2.0, 1.0])) == 0.0


def test_load_plane_scales_parses_pipeline_cache(tmp_path):
    path = tmp_path / "scales.json"
    path.write_text(json.dumps({"per_fish": {"L765_f03": {"planes": {"L765_f03_plane2_mcorrected_flipX": {"scale": 1.17}}}}}))
    assert load_plane_scales(path, "L765_f03") == {2: ("L765_f03_plane2_mcorrected_flipX", 1.17)}


def test_load_functional_polarity_requires_matching_pass_manifest(tmp_path):
    path = tmp_path / "prepare-functional-reference-stacks_manifest.json"
    path.write_text(json.dumps({"fish_id": "L765_f02", "status": "pass", "parameters": {"polarity": "north", "polarity_source": "metadata.csv:fish_orientation"}}))
    assert load_functional_polarity(path, "L765_f02") == ("north", "metadata.csv:fish_orientation")
    with np.testing.assert_raises_regex(ValueError, "fish mismatch"):
        load_functional_polarity(path, "L765_f03")


def test_load_functional_polarity_rejects_missing_orientation(tmp_path):
    path = tmp_path / "prepare-functional-reference-stacks_manifest.json"
    path.write_text(json.dumps({"fish_id": "L765_f02", "status": "pass", "parameters": {"polarity": None}}))
    with np.testing.assert_raises_regex(ValueError, "no valid north/south polarity"):
        load_functional_polarity(path, "L765_f02")


def test_load_functional_frame_selection_requires_verified_block_exclusion(tmp_path):
    path = tmp_path / "prepare-functional-reference-stacks_manifest.json"
    path.write_text(json.dumps({
        "fish_id": "L765_f02",
        "status": "pass",
        "parameters": {
            "exclude_first_block": True,
            "frame_selection_by_plane": {
                "L765_f02_plane0_mcorrected_flipX": {
                    "decision": "excluded_first_selected_tiff_block",
                    "frame_start": 1834,
                    "source_frame_count": 5502,
                    "reference_frame_count": 3668,
                }
            },
        },
    }))
    assert load_functional_frame_selection(path, "L765_f02") == {
        0: {
            "decision": "excluded_first_selected_tiff_block",
            "frame_start": 1834,
            "source_frame_count": 5502,
            "reference_frame_count": 3668,
        }
    }


def test_load_functional_frame_selection_rejects_unexcluded_manifest(tmp_path):
    path = tmp_path / "prepare-functional-reference-stacks_manifest.json"
    path.write_text(json.dumps({"fish_id": "L765_f02", "status": "pass", "parameters": {"exclude_first_block": False}}))
    with np.testing.assert_raises_regex(ValueError, "does not require first-block exclusion"):
        load_functional_frame_selection(path, "L765_f02")


def test_direct_source_provenance_excludes_block0_without_writing_references(tmp_path):
    fish_id = "L758_f04"
    fish_dir = tmp_path / fish_id
    metadata_dir = fish_dir / "01_raw" / "2p" / "metadata"
    motion_dir = fish_dir / "02_reg" / "00_preprocessing" / "2p_functional" / "02_motionCorrected"
    preprocessing_path = motion_dir.parent / "01_individualPlanes" / f"{fish_id}_preprocessing_metadata.json"
    metadata_dir.mkdir(parents=True)
    motion_dir.mkdir(parents=True)
    preprocessing_path.parent.mkdir(parents=True)
    metadata_dir.joinpath(f"{fish_id}_metadata.csv").write_text(
        "parameter,value\nfish_orientation,top-right\n"
    )
    preprocessing_path.write_text(json.dumps({
        "sessions": [{
            "session_label": "r1",
            "output_planes": [0],
            "selected_tiffs": [
                f"/raw/{fish_id}_00001.tif",
                f"/raw/{fish_id}_00002.tif",
                f"/raw/{fish_id}_00003.tif",
            ],
        }],
    }))
    movie_path = motion_dir / f"{fish_id}_plane0_mcorrected.tif"
    tifffile.imwrite(movie_path, np.zeros((6, 4, 4), dtype=np.uint16))

    polarity, source, selections, mode = _resolve_drift_input_provenance(
        fish_id=fish_id,
        motion_corrected_dir=motion_dir,
        preprocessing_metadata_path=preprocessing_path,
        plane_indices=[0],
        functional_reference_manifest_path=None,
        fish_dir=fish_dir,
    )

    assert polarity == "south"
    assert source.endswith(":fish_orientation")
    assert mode == "direct_source_validation"
    assert selections[0]["frame_start"] == 2
    assert selections[0]["reference_frame_count"] == 4
    assert selections[0]["decision"] == "excluded_first_selected_tiff_block"
    assert not list(tmp_path.rglob("*ref_raw.tif"))


def test_interval_reference_applies_resolved_north_polarity():
    frame = np.arange(16, dtype=np.float32).reshape(4, 4)
    movie = np.stack([frame] * 4)
    config = FunctionalZDriftConfig(sampled_frames_per_interval=4, top_correlated_frames=2)
    reference, sampled = _interval_reference(movie, 0, 4, config, polarity="north")
    assert sampled == 4
    assert np.array_equal(reference, apply_func_orientation(frame, polarity="north", flip_x=True))


def test_interval_reference_reads_selected_tiff_pages_without_memmap(tmp_path):
    frame = np.arange(16, dtype=np.float32).reshape(4, 4)
    movie = np.stack([frame + index for index in range(8)]).astype(np.float32)
    path = tmp_path / "movie.tif"
    tifffile.imwrite(path, movie, photometric="minisblack")
    config = FunctionalZDriftConfig(sampled_frames_per_interval=4, top_correlated_frames=2)

    expected, expected_count = _interval_reference(movie, 2, 8, config, polarity="south")
    with tifffile.TiffFile(path) as tif:
        observed, observed_count = _interval_reference_from_tiff(tif, 2, 8, config, polarity="south")

    assert observed_count == expected_count == 4
    np.testing.assert_array_equal(observed, expected)


def test_session_summary_requires_coherent_plane_motion():
    rows = []
    for plane in range(5):
        for interval in range(6):
            rows.append({"fish_id": "L765_f03", "session": "r1", "plane_index": plane, "interval_index": interval, "best_z_subslice": 30 - plane + interval})
    summary = summarize_session_drift(pd.DataFrame(rows), FunctionalZDriftConfig())
    assert summary.loc[0, "evidence_tier"] == "coherent_progressive_drift"
    assert summary.loc[0, "same_direction_plane_fraction"] == 1.0


def test_session_summary_separates_initial_transient_from_drift():
    rows = []
    trajectory = [10, 20, 20.2, 20.4, 20.6, 20.8]
    for plane in range(5):
        for interval, shift in enumerate(trajectory):
            rows.append({"fish_id": "L765_f05", "session": "r2", "plane_index": plane, "interval_index": interval, "best_z_subslice": 30 - plane + shift})
    summary = summarize_session_drift(pd.DataFrame(rows), FunctionalZDriftConfig())
    assert summary.loc[0, "evidence_tier"] == "initial_transient_then_stable"
