from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from codeants_2pf_hcr.qc_notebooks import (
    QCNotebookConfig,
    QCNotebookContext,
    build_qc_stage_inventory,
    load_qc_csv,
    load_qc_image,
    load_qc_json,
    resolve_qc_notebook_context,
    write_qc_review_record,
)


FISH_ID = "L765_f04"


def _fish_root(tmp_path: Path) -> tuple[Path, Path]:
    local_root = tmp_path / "data"
    fish_dir = local_root / FISH_ID
    fish_dir.mkdir(parents=True)
    return local_root, fish_dir


def test_context_validates_fish_id_and_direct_fish_folder(tmp_path: Path) -> None:
    local_root, _ = _fish_root(tmp_path)
    with pytest.raises(ValueError, match="Invalid fish_id"):
        resolve_qc_notebook_context(QCNotebookConfig("fish4", local_root))
    with pytest.raises(FileNotFoundError, match="directly below"):
        resolve_qc_notebook_context(QCNotebookConfig("L765_f05", local_root))


def test_context_resolves_explicit_pipeline_root_and_stage_containment(tmp_path: Path) -> None:
    local_root, fish_dir = _fish_root(tmp_path)
    pipeline_root = tmp_path / "staged" / FISH_ID
    context = resolve_qc_notebook_context(
        QCNotebookConfig(FISH_ID, local_root, pipeline_root=pipeline_root)
    )

    assert context.fish_dir == fish_dir.resolve()
    assert context.pipeline_root == pipeline_root.resolve()
    assert context.provenance["pipeline_root_explicit"] is True
    assert context.stage_paths["match-roi-to-anatomy"].is_relative_to(context.pipeline_root)


def test_context_rejects_wrong_fish_manifest_inside_pipeline_root(tmp_path: Path) -> None:
    local_root, _ = _fish_root(tmp_path)
    pipeline_root = tmp_path / "staged" / FISH_ID
    manifest = pipeline_root / "match-roi-to-anatomy" / "writer_manifest.json"
    manifest.parent.mkdir(parents=True)
    manifest.write_text(
        json.dumps(
            {
                "fish_id": "L765_f05",
                "parameters": {"pipeline_root": str(pipeline_root.resolve())},
            }
        )
    )

    with pytest.raises(ValueError, match="does not match requested fish"):
        resolve_qc_notebook_context(
            QCNotebookConfig(FISH_ID, local_root, pipeline_root=pipeline_root)
        )


def test_context_rejects_wrong_root_manifest_inside_pipeline_root(tmp_path: Path) -> None:
    local_root, _ = _fish_root(tmp_path)
    pipeline_root = tmp_path / "staged" / FISH_ID
    manifest = pipeline_root / "match-roi-to-anatomy" / "writer_manifest.json"
    manifest.parent.mkdir(parents=True)
    manifest.write_text(
        json.dumps(
            {
                "fish_id": FISH_ID,
                "parameters": {"pipeline_root": str(tmp_path / "other")},
            }
        )
    )

    with pytest.raises(ValueError, match="does not match selected root"):
        resolve_qc_notebook_context(
            QCNotebookConfig(FISH_ID, local_root, pipeline_root=pipeline_root)
        )


def test_inventory_reports_stage_artifacts_and_matching_manifest(tmp_path: Path) -> None:
    local_root, fish_dir = _fish_root(tmp_path)
    pipeline_root = tmp_path / "staged" / FISH_ID
    artifact = pipeline_root / "match-roi-to-anatomy" / "registration" / "matches.csv"
    artifact.parent.mkdir(parents=True)
    artifact.write_text("fish_id,value\nL765_f04,1\n")

    canonical_manifest = (
        fish_dir
        / "03_analysis"
        / "functional"
        / "pipeline_manifests"
        / "match-roi-to-anatomy_manifest.json"
    )
    canonical_manifest.parent.mkdir(parents=True)
    canonical_manifest.write_text(
        json.dumps(
            {
                "fish_id": FISH_ID,
                "stage_name": "match-roi-to-anatomy",
                "status": "pass",
                "parameters": {"pipeline_root": str(pipeline_root.resolve())},
                "outputs": [
                    {
                        "path": str(artifact),
                        "label": "matches",
                        "required": True,
                    }
                ],
            }
        )
    )
    context = resolve_qc_notebook_context(
        QCNotebookConfig(FISH_ID, local_root, pipeline_root=pipeline_root)
    )
    inventory = build_qc_stage_inventory(context).set_index("stage_name")

    row = inventory.loc["match-roi-to-anatomy"]
    assert bool(row["stage_root_exists"])
    assert bool(row["manifest_exists"])
    assert row["manifest_status"] == "pass"
    assert row["artifact_count"] == 1
    assert row["artifact_exists_count"] == 1
    assert bool(row["required_artifacts_exist"])


def test_read_only_loaders_parse_and_validate_identity(tmp_path: Path) -> None:
    csv_path = tmp_path / "table.csv"
    csv_path.write_text("fish_id,value\nL765_f04,3\n")
    json_path = tmp_path / "data.json"
    json_path.write_text('{"state": "computed_unreviewed"}')
    image_path = tmp_path / "image.png"
    from PIL import Image

    Image.new("L", (3, 2), color=7).save(image_path)

    frame = load_qc_csv(csv_path, expected_fish_id=FISH_ID)
    assert isinstance(frame, pd.DataFrame)
    assert frame["value"].tolist() == [3]
    assert load_qc_json(json_path)["state"] == "computed_unreviewed"
    assert load_qc_image(image_path).size == (3, 2)
    with pytest.raises(ValueError, match="contains fish IDs"):
        load_qc_csv(csv_path, expected_fish_id="L765_f05")


def test_review_writer_hashes_artifacts_and_only_writes_review_sidecar(tmp_path: Path) -> None:
    local_root, _ = _fish_root(tmp_path)
    pipeline_root = tmp_path / "staged" / FISH_ID
    artifact = pipeline_root / "match-roi-to-anatomy" / "registration" / "matches.csv"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"fish_id,value\nL765_f04,1\n")
    context = resolve_qc_notebook_context(
        QCNotebookConfig(FISH_ID, local_root, pipeline_root=pipeline_root)
    )

    review_path = write_qc_review_record(
        context,
        stage_name="match-roi-to-anatomy",
        decision="accepted",
        reviewer="DD",
        reviewed_artifacts=[artifact],
        notes="Plane review complete.",
        timestamp=datetime(2026, 8, 11, 9, 30, tzinfo=timezone.utc),
    )
    assert review_path == (
        pipeline_root
        / "match-roi-to-anatomy"
        / "reviews"
        / "review_20260811T093000.000000Z.json"
    ).resolve()
    payload = json.loads(review_path.read_text())
    assert payload["fish_id"] == FISH_ID
    assert payload["stage_name"] == "match-roi-to-anatomy"
    assert payload["promoted_outputs"] is False
    assert payload["reviewed_artifacts"][0]["sha256"] == hashlib.sha256(
        artifact.read_bytes()
    ).hexdigest()
    assert artifact.read_bytes() == b"fish_id,value\nL765_f04,1\n"


def test_review_writer_refuses_ambiguous_stage_cross_stage_artifact_and_overwrite(
    tmp_path: Path,
) -> None:
    local_root, _ = _fish_root(tmp_path)
    pipeline_root = tmp_path / "staged" / FISH_ID
    artifact = pipeline_root / "match-roi-to-anatomy" / "registration" / "matches.csv"
    artifact.parent.mkdir(parents=True)
    artifact.write_text("a\n1\n")
    other = pipeline_root / "assign-hcr-identity" / "registration" / "identity.csv"
    other.parent.mkdir(parents=True)
    other.write_text("a\n1\n")
    context = resolve_qc_notebook_context(
        QCNotebookConfig(FISH_ID, local_root, pipeline_root=pipeline_root)
    )
    kwargs = {
        "context": context,
        "decision": "accepted",
        "reviewer": "DD",
        "timestamp": "2026-08-11T09:30:00+00:00",
    }

    with pytest.raises(ValueError, match="Unknown or ambiguous"):
        write_qc_review_record(
            stage_name="geometry", reviewed_artifacts=[artifact], **kwargs
        )
    with pytest.raises(ValueError, match="must belong to stage"):
        write_qc_review_record(
            stage_name="match-roi-to-anatomy", reviewed_artifacts=[other], **kwargs
        )
    write_qc_review_record(
        stage_name="match-roi-to-anatomy", reviewed_artifacts=[artifact], **kwargs
    )
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        write_qc_review_record(
            stage_name="match-roi-to-anatomy", reviewed_artifacts=[artifact], **kwargs
        )


def test_review_writer_rejects_forged_ambiguous_fish_context(tmp_path: Path) -> None:
    local_root, fish_dir = _fish_root(tmp_path)
    context = resolve_qc_notebook_context(QCNotebookConfig(FISH_ID, local_root))
    artifact = context.stage_paths["match-roi-to-anatomy"] / "artifact.csv"
    artifact.parent.mkdir(parents=True)
    artifact.write_text("a\n1\n")
    forged = QCNotebookContext(
        fish_id=FISH_ID,
        local_root=context.local_root,
        fish_dir=fish_dir.parent / "L765_f05",
        pipeline_root=context.pipeline_root,
        stage_paths=context.stage_paths,
        manifest_paths=context.manifest_paths,
        provenance=context.provenance,
    )
    with pytest.raises(ValueError, match="fish_dir is ambiguous"):
        write_qc_review_record(
            forged,
            stage_name="match-roi-to-anatomy",
            decision="accepted",
            reviewer="DD",
            reviewed_artifacts=[artifact],
        )
