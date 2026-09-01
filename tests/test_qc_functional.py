import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pytest
import tifffile

from codeants_2pf_hcr.plots.qc_functional import (
    functional_registration_plane_review,
    load_functional_reference_drift_qc,
    load_functional_registration_qc,
    render_functional_registration_all_planes,
    render_functional_reference_drift_summary,
    render_functional_reference_artifacts,
    render_functional_registration_plane,
    render_functional_registration_artifacts,
    show_functional_anatomy_zoom_viewer,
)


FISH_ID = "L765_f04"


def _write_png(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    plt.imsave(path, np.ones((8, 10, 3), dtype=float) * 0.5)


def _write_manifest(path: Path, *, status: str = "pass", parameters=None, outputs=None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "fish_id": FISH_ID,
                "status": status,
                "generated_at": "2026-08-11T10:00:00+00:00",
                "parameters": parameters or {},
                "outputs": outputs or [],
            }
        )
    )


def _make_drift_bundle(tmp_path: Path) -> tuple[Path, Path]:
    ref_manifest = tmp_path / "prepare-functional-reference-stacks_manifest.json"
    ref_tiff = tmp_path / f"{FISH_ID}_plane0_mcorrected_flipX_ref_norm.tif"
    tifffile.imwrite(ref_tiff, np.arange(80, dtype=np.uint16).reshape(8, 10))
    _write_manifest(
        ref_manifest,
        parameters={
            "exclude_first_block": True,
            "frame_selection_by_plane": {
                f"{FISH_ID}_plane0_mcorrected_flipX": {
                    "decision": "excluded_first_selected_tiff_block",
                    "frame_start": 10,
                    "source_frame_count": 30,
                    "reference_frame_count": 20,
                }
            },
        },
        outputs=[{"path": str(ref_tiff), "required": True}],
    )
    drift_root = tmp_path / "z_drift"
    drift_root.mkdir()
    _write_manifest(drift_root / "z_drift_manifest.json")
    pd.DataFrame(
        [
            {
                "fish_id": FISH_ID,
                "session": "r1",
                "plane_index": 0,
                "interval_index": index,
                "interval_label": label,
                "best_z_subslice": 10.0 + index * 0.1,
            }
            for index, label in enumerate(("Block 1\nfirst third", "Block 1\nmiddle third", "Block 1\nfinal third"))
        ]
    ).to_csv(drift_root / "z_drift_intervals.csv", index=False)
    pd.DataFrame(
        [
            {
                "fish_id": FISH_ID,
                "session": "r1",
                "plane_index": 0,
                "interval_index": interval,
                "anatomy_z": z,
                "ncc": 1.0 - abs(z - 10) * 0.1,
            }
            for interval in range(3)
            for z in range(9, 12)
        ]
    ).to_csv(drift_root / "z_drift_ncc_profiles.csv", index=False)
    pd.DataFrame(
        [{"fish_id": FISH_ID, "session": "r1", "evidence_tier": "below_threshold_stability", "consensus_shift": 0.2}]
    ).to_csv(drift_root / "z_drift_session_summary.csv", index=False)
    _write_png(drift_root / "z_drift_tracks.png")
    _write_png(drift_root / "z_drift_ncc_profiles.png")
    return ref_manifest, drift_root


def test_functional_reference_drift_qc_loads_block0_and_response_evidence(tmp_path: Path) -> None:
    ref_manifest, drift_root = _make_drift_bundle(tmp_path)
    response_path = tmp_path / "suite2p_response_bpi_cells_23c.csv"
    response_path.write_text("fish_id\nL765_f04\n")

    bundle = load_functional_reference_drift_qc(
        fish_id=FISH_ID,
        functional_reference_manifest_path=ref_manifest,
        z_drift_root=drift_root,
        response_evidence_paths=[response_path, tmp_path / "missing_response.csv"],
    )

    assert bundle["block0_provenance"].loc[0, "frame_start"] == 10
    assert bundle["response_evidence"]["available"].tolist() == [True, False]
    figure = render_functional_reference_drift_summary(bundle)
    assert figure.axes
    plt.close(figure)
    reference_figure = render_functional_reference_artifacts(bundle)
    assert reference_figure.axes
    plt.close(reference_figure)


def test_functional_reference_drift_qc_fails_on_missing_profile_png(tmp_path: Path) -> None:
    ref_manifest, drift_root = _make_drift_bundle(tmp_path)
    (drift_root / "z_drift_ncc_profiles.png").unlink()

    with pytest.raises(FileNotFoundError, match="NCC-profile PNG"):
        load_functional_reference_drift_qc(
            fish_id=FISH_ID,
            functional_reference_manifest_path=ref_manifest,
            z_drift_root=drift_root,
        )


def _make_registration_bundle(tmp_path: Path) -> dict[str, Path]:
    manifests = {}
    for stage in (
        "prepare-in-vivo-anatomy-stack",
        "register-functional-to-anatomy",
        "transform-functional-rois-to-anatomy",
        "make-functional-registration-qc",
    ):
        path = tmp_path / "manifests" / f"{stage}_manifest.json"
        _write_manifest(path)
        manifests[stage] = path

    registration_root = tmp_path / "register-functional-to-anatomy"
    registration_root.mkdir()
    (registration_root / "plane_refs_summary.json").write_text(
        json.dumps([{"index": 0, "label": f"{FISH_ID}_plane0_mcorrected_flipX", "best_z": 1, "scale": 1.1}])
    )
    registration_table = registration_root / "registration" / "tforms_by_plane.csv"
    registration_table.parent.mkdir()
    pd.DataFrame(
        [{"plane_index": 0, "label": f"{FISH_ID}_plane0_mcorrected_flipX", "best_z": 1, "m00": 1.0}]
    ).to_csv(registration_table, index=False)

    transformed_root = tmp_path / "transform-functional-rois-to-anatomy"
    transformed_label = transformed_root / "functional" / "anatomy" / f"{FISH_ID}_plane0_func_mask_in_2p.tif"
    transformed_label.parent.mkdir(parents=True)
    tifffile.imwrite(transformed_label, np.arange(80, dtype=np.uint16).reshape(8, 10))
    anatomy_stack = tmp_path / f"{FISH_ID}_anatomy_2P_GCaMP.tif"
    tifffile.imwrite(anatomy_stack, np.arange(240, dtype=np.uint16).reshape(3, 8, 10), photometric="minisblack")

    qc_root = tmp_path / "make-functional-registration-qc"
    qa = qc_root / "qa"
    qa.mkdir(parents=True)
    pd.DataFrame([{"plane_idx": 0, "plane_label": f"{FISH_ID}_plane0_mcorrected_flipX", "best_z": 1, "best_ncc": 0.91}]).to_csv(
        qa / "functional_ncc_depth_profiles.csv", index=False
    )
    for name in (
        "functional_anatomy_plane_qc_rows.png",
        "functional_anatomy_intensity_overlay_rows.png",
        "functional_anatomy_center_label_overlay_200px.png",
        "functional_ncc_depth_profiles.png",
    ):
        _write_png(qa / name)
    return {
        **manifests,
        "registration_root": registration_root,
        "transformed_root": transformed_root,
        "qc_root": qc_root,
        "anatomy_stack": anatomy_stack,
    }


def test_functional_registration_qc_loads_all_stage_evidence(tmp_path: Path) -> None:
    paths = _make_registration_bundle(tmp_path)
    bundle = load_functional_registration_qc(
        fish_id=FISH_ID,
        anatomy_preparation_manifest_path=paths["prepare-in-vivo-anatomy-stack"],
        anatomy_stack_path=paths["anatomy_stack"],
        registration_manifest_path=paths["register-functional-to-anatomy"],
        roi_transform_manifest_path=paths["transform-functional-rois-to-anatomy"],
        qc_manifest_path=paths["make-functional-registration-qc"],
        registration_root=paths["registration_root"],
        transformed_roi_root=paths["transformed_root"],
        qc_root=paths["qc_root"],
    )

    assert set(bundle["manifest_inventory"]["status"]) == {"pass"}
    plane_review = functional_registration_plane_review(bundle, plane_index=0)
    assert {"plane_refs", "transforms", "ncc_profiles", "transformed_labels"}.issubset(set(plane_review["source"]))
    plane_figure = render_functional_registration_plane(bundle, plane_index=0)
    assert len(plane_figure.axes) == 2
    plt.close(plane_figure)
    all_planes = render_functional_registration_all_planes(bundle)
    assert len(all_planes.axes) == 2
    plt.close(all_planes)
    viewer = show_functional_anatomy_zoom_viewer(bundle, enable_widgets=False)
    assert viewer["plane_indices"] == (0,)
    plt.close(viewer["figure"])
    figure = render_functional_registration_artifacts(
        bundle,
        artifact_names=["functional_anatomy_center_label_overlay_200px.png"],
    )
    assert len(figure.axes) == 1
    plt.close(figure)


def test_functional_registration_qc_accepts_a_legacy_anatomy_stack_without_manifest(tmp_path: Path) -> None:
    paths = _make_registration_bundle(tmp_path)
    paths["prepare-in-vivo-anatomy-stack"].unlink()
    anatomy_stack = tmp_path / f"{FISH_ID}_anatomy_2P_GCaMP.nrrd"
    anatomy_stack.write_bytes(b"legacy anatomy")

    bundle = load_functional_registration_qc(
        fish_id=FISH_ID,
        anatomy_preparation_manifest_path=paths["prepare-in-vivo-anatomy-stack"],
        anatomy_stack_path=anatomy_stack,
        registration_manifest_path=paths["register-functional-to-anatomy"],
        roi_transform_manifest_path=paths["transform-functional-rois-to-anatomy"],
        qc_manifest_path=paths["make-functional-registration-qc"],
        registration_root=paths["registration_root"],
        transformed_roi_root=paths["transformed_root"],
        qc_root=paths["qc_root"],
    )

    inventory = bundle["manifest_inventory"].set_index("artifact")
    assert inventory.loc["anatomy preparation", "status"] == "legacy_unmanifested"


def test_functional_registration_qc_rejects_manifest_fish_mismatch(tmp_path: Path) -> None:
    paths = _make_registration_bundle(tmp_path)
    paths["register-functional-to-anatomy"].write_text(json.dumps({"fish_id": "L765_f03", "status": "pass"}))

    with pytest.raises(ValueError, match="Fish mismatch"):
        load_functional_registration_qc(
            fish_id=FISH_ID,
            anatomy_preparation_manifest_path=paths["prepare-in-vivo-anatomy-stack"],
            registration_manifest_path=paths["register-functional-to-anatomy"],
            roi_transform_manifest_path=paths["transform-functional-rois-to-anatomy"],
            qc_manifest_path=paths["make-functional-registration-qc"],
            registration_root=paths["registration_root"],
            transformed_roi_root=paths["transformed_root"],
            qc_root=paths["qc_root"],
        )
