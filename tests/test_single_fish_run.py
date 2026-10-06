import hashlib
import csv
import json
from pathlib import Path
import subprocess
import sys

import pytest

from codeants_2pf_hcr.single_fish_run import (
    RunSpecError,
    build_run_plan,
    build_run_status,
    execute_run,
    describe_path,
    load_run_spec,
    verify_run,
    validate_spec_environment,
    audit_run_artifacts,
    _json_hash,
)


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(("git", "-C", str(repo), *args), check=True, text=True, stdout=subprocess.PIPE)
    return result.stdout.strip()


def _build_spec(tmp_path: Path) -> tuple[Path, Path, Path]:
    repo = tmp_path / "repo"
    data = tmp_path / "data"
    pipeline = tmp_path / "pipeline"
    fish = data / "L000_f00"
    repo.mkdir()
    fish.mkdir(parents=True)
    pipeline.mkdir()
    source = fish / "source.txt"
    source.write_text("one\n", encoding="utf-8")
    writer = repo / "writer.py"
    writer.write_text(
        "from pathlib import Path\n"
        "import sys\n"
        "Path(sys.argv[2]).parent.mkdir(parents=True, exist_ok=True)\n"
        "Path(sys.argv[2]).write_text(Path(sys.argv[1]).read_text())\n",
        encoding="utf-8",
    )
    subprocess.run(("git", "init", "-q", str(repo)), check=True)
    subprocess.run(("git", "-C", str(repo), "config", "user.email", "tests@example.org"), check=True)
    subprocess.run(("git", "-C", str(repo), "config", "user.name", "Tests"), check=True)
    subprocess.run(("git", "-C", str(repo), "add", "writer.py"), check=True)
    subprocess.run(("git", "-C", str(repo), "commit", "-qm", "fixture"), check=True)
    commit = _git(repo, "rev-parse", "HEAD")
    pin_hash = hashlib.sha256(source.read_bytes()).hexdigest()
    spec = {
        "schema_version": 1,
        "fish_id": "L000_f00",
        "paths": {
            "repo_root": str(repo),
            "data_root": str(data),
            "pipeline_root": str(pipeline),
            "state_root": "{pipeline_root}/state",
        },
        "git": {"commit": commit, "policy": "exact"},
        "interpreter": {
            "executable": sys.executable,
            "python_version": ".".join(str(value) for value in sys.version_info[:3]),
            "packages": {},
        },
        "provenance_pins": [{"name": "source", "path": "{fish_dir}/source.txt", "sha256": pin_hash}],
        "stages": [
            {
                "name": "010-copy",
                "command": ["{python}", "writer.py", "{fish_dir}/source.txt", "{pipeline_root}/copy/output.txt"],
                "inputs": ["{fish_dir}/source.txt"],
                "outputs": ["{pipeline_root}/copy/output.txt"],
            }
        ],
    }
    spec_path = tmp_path / "spec.json"
    spec_path.write_text(json.dumps(spec), encoding="utf-8")
    return spec_path, source, pipeline


def test_run_resume_verify_and_input_invalidation(tmp_path: Path) -> None:
    spec_path, source, pipeline = _build_spec(tmp_path)
    spec = load_run_spec(spec_path)
    plan = build_run_plan(spec, resume=False)
    assert plan["environment"]["ok"] is True
    assert plan["stages"][0]["action"] == "run"

    first = execute_run(spec, resume=False)
    assert first["status"] == "pass"
    assert (pipeline / "copy" / "output.txt").read_text() == "one\n"
    assert verify_run(spec)["ok"] is True
    assert execute_run(spec, resume=True)["results"] == [
        {"stage": "010-copy", "action": "skip", "status": "pass"}
    ]

    source.write_text("two\n", encoding="utf-8")
    assert build_run_status(spec)["stages"][0]["current"] is False
    # The accepted-source pin also blocks execution until deliberately updated.
    assert build_run_plan(spec, resume=True)["environment"]["ok"] is False


def test_resume_reruns_when_unpinned_input_changes(tmp_path: Path) -> None:
    spec_path, source, pipeline = _build_spec(tmp_path)
    raw = json.loads(spec_path.read_text())
    raw["provenance_pins"] = []
    spec_path.write_text(json.dumps(raw))
    spec = load_run_spec(spec_path)
    assert execute_run(spec, resume=False)["status"] == "pass"
    source.write_text("two\n", encoding="utf-8")
    resumed = execute_run(spec, resume=True)
    assert resumed["results"][0]["action"] == "run"
    assert (pipeline / "copy" / "output.txt").read_text() == "two\n"


def test_verify_detects_output_tampering(tmp_path: Path) -> None:
    spec_path, _, pipeline = _build_spec(tmp_path)
    spec = load_run_spec(spec_path)
    execute_run(spec, resume=False)
    (pipeline / "copy" / "output.txt").write_text("tampered\n")
    verified = verify_run(spec)
    assert verified["ok"] is False
    assert "outputs changed" in verified["stages"][0]["reasons"]


@pytest.mark.parametrize("field", ["inputs", "outputs", "optional_inputs"])
@pytest.mark.parametrize("bad", ["", "   ", ".", "relative/input.csv", None])
def test_artifact_declarations_reject_cwd_sentinels(tmp_path, field, bad):
    path, _, _ = _build_spec(tmp_path)
    raw = json.loads(path.read_text())
    raw["stages"][0][field] = [bad]
    path.write_text(json.dumps(raw))
    with pytest.raises(RunSpecError):
        load_run_spec(path)


def test_describe_blank_path_never_hashes_checkout():
    with pytest.raises(RunSpecError):
        describe_path("")


def test_stage_subprocess_has_closed_stdin(tmp_path):
    import os
    from codeants_2pf_hcr.single_fish_run import _run_command
    assert _run_command([sys.executable, "-c", "import sys; assert sys.stdin.read() == ''"],
                        cwd=tmp_path, env=os.environ, log_path=tmp_path / "stdin.log") == 0


def test_spec_rejects_duplicate_stages(tmp_path: Path) -> None:
    spec_path, _, _ = _build_spec(tmp_path)
    raw = json.loads(spec_path.read_text())
    raw["stages"].append(raw["stages"][0])
    spec_path.write_text(json.dumps(raw))
    with pytest.raises(RunSpecError, match="unique"):
        load_run_spec(spec_path)


def test_symlink_hash_tracks_target_content(tmp_path: Path) -> None:
    target = tmp_path / "target.txt"
    link = tmp_path / "link.txt"
    target.write_text("one\n")
    link.symlink_to(target)
    first = describe_path(link)
    target.write_text("two\n")
    second = describe_path(link)
    assert first["kind"] == "symlink"
    assert first["sha256"] != second["sha256"]


def test_resume_rechecks_downstream_after_upstream_changes(tmp_path: Path) -> None:
    spec_path, source, pipeline = _build_spec(tmp_path)
    raw = json.loads(spec_path.read_text())
    raw["provenance_pins"] = []
    raw["stages"].append({"name": "020-copy", "depends_on": ["010-copy"],
        "command": ["{python}", "writer.py", "{pipeline_root}/copy/output.txt", "{pipeline_root}/final.txt"],
        "inputs": ["{pipeline_root}/copy/output.txt"], "outputs": ["{pipeline_root}/final.txt"]})
    spec_path.write_text(json.dumps(raw))
    spec = load_run_spec(spec_path)
    assert execute_run(spec, resume=False)["status"] == "pass"
    source.write_text("two\n")
    result = execute_run(spec, resume=True)
    assert [row["action"] for row in result["results"]] == ["run", "run"]
    assert (pipeline / "final.txt").read_text() == "two\n"
    assert verify_run(spec)["ok"]


def test_optional_input_creation_invalidates_stage(tmp_path: Path) -> None:
    spec_path, _, pipeline = _build_spec(tmp_path)
    raw = json.loads(spec_path.read_text())
    raw["stages"][0]["optional_inputs"] = [str(pipeline / "optional.txt")]
    spec_path.write_text(json.dumps(raw))
    spec = load_run_spec(spec_path)
    assert execute_run(spec, resume=False)["status"] == "pass"
    (pipeline / "optional.txt").write_text("created")
    assert not verify_run(spec)["ok"]


def test_writer_exit_zero_does_not_accept_stale_manifest(tmp_path: Path) -> None:
    spec_path, _, pipeline = _build_spec(tmp_path)
    manifest = pipeline / "manifest.json"
    manifest.write_text(json.dumps({"fish_id": "L000_f00", "status": "pass", "errors": [],
        "dry_run": False, "generated_at": "2000-01-01T00:00:00Z", "parameters": {"pipeline_root": str(pipeline)}}))
    raw = json.loads(spec_path.read_text()); raw["stages"][0]["manifest"] = str(manifest)
    spec_path.write_text(json.dumps(raw))
    result = execute_run(load_run_spec(spec_path), resume=False)
    assert result["status"] == "fail"


def test_argument_pin_cannot_silently_drift(tmp_path: Path) -> None:
    spec_path, _, _ = _build_spec(tmp_path)
    raw = json.loads(spec_path.read_text()); raw["stages"][0]["argument_pins"] = {"--seed": "50"}
    spec_path.write_text(json.dumps(raw))
    with pytest.raises(RunSpecError, match="violates pin"):
        load_run_spec(spec_path)


def test_auxiliary_interpreter_is_actually_verified(tmp_path: Path) -> None:
    spec_path, _, _ = _build_spec(tmp_path)
    raw = json.loads(spec_path.read_text())
    raw["auxiliary_interpreters"] = [{"executable": sys.executable, "python_version": sys.version.split()[0], "packages": {}}]
    spec_path.write_text(json.dumps(raw))
    assert validate_spec_environment(load_run_spec(spec_path))["ok"]


def test_scientific_description_must_agree_with_writer_pins(tmp_path: Path) -> None:
    spec_path, _, _ = _build_spec(tmp_path)
    raw = json.loads(spec_path.read_text())
    raw["stages"][0]["parameter_pins"] = {"activity_config": {"response_rng_seed": 50}}
    raw["scientific_parameters"] = {"activity_config": {"response_rng_seed": 50}, "response_rng_seed": 51}
    spec_path.write_text(json.dumps(raw))
    with pytest.raises(RunSpecError, match="contradicts"):
        load_run_spec(spec_path)


def test_real_table_audit_rejects_upper_tail_only_payload(tmp_path: Path) -> None:
    spec_path, _, root = _build_spec(tmp_path)
    raw = json.loads(spec_path.read_text())
    raw["scientific_parameters"] = {"response_null_q_total_two_sided": .99, "response_window_sec": 12.5}
    base = {"fish_id": "L000_f00", "plane_idx": "0", "func_label": "1", "anat_label": "2", "selected_anat_label": "2", "has_unique_anat_match": "True"}
    signed = {**base, "bout_positive_response_pass": "False", "bout_negative_response_pass": "True", "cont_positive_response_pass": "False", "cont_negative_response_pass": "False", "response_has_suppression": "True", "response_is_active": "True"}
    window = {"fish_id": "L000_f00", "plane_idx": "0", "session_label": "r1", "block": "B1", "stim_idx": "0", "stim_type": "LB", "used_for_bpi": "True", "scored_duration_sec": "12.5"}
    early = {"fish_id": "L000_f00", "stim_type": "LB", "null_lower_quantile_familywise": ".005", "null_upper_quantile_familywise": ".995", "response_pass": "True", "positive_response_pass": "False", "negative_response_pass": "True"}
    def write(relative, row):
        p = root / relative; p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(row));w.writeheader();w.writerow(row)
    write("match-roi-to-anatomy/registration/functional_roi_anatomy_matches.csv", base)
    for stage in ("score-activity-bpi", "assign-hcr-identity", "export-canonical-tables"):
        write(stage + "/registration/functional_roi_activity_identity.csv", signed)
    write("score-activity-bpi/registration/scored_stimulus_windows.csv", window)
    ep = "prepare-functional-reference-stacks/qa/suite2p_presented_stimulus_response_23c.csv"
    write(ep, early)
    raw["artifact_baseline"] = {"geometry_assignment_sha256": _json_hash([[base[k] for k in ("plane_idx", "func_label", "anat_label", "selected_anat_label", "has_unique_anat_match")]]), "bpi_pool_sha256": _json_hash([[window[k] for k in ("plane_idx", "session_label", "block", "stim_idx", "stim_type")]])}
    spec_path.write_text(json.dumps(raw));spec = load_run_spec(spec_path)
    assert audit_run_artifacts(spec)["ok"]
    early["response_pass"] = "False";write(ep, early)
    result = audit_run_artifacts(spec)
    assert not result["ok"]
    assert next(c for c in result["checks"] if c["label"] == "23c family-wise both tails")["status"] == "fail"
