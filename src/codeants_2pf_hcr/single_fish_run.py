"""Deterministic, declarative orchestration for the single-fish stage DAG.

This module owns orchestration and provenance only. Scientific work remains in
the existing package stages invoked through ``tools/single_fish_pipeline.py``.
"""

from __future__ import annotations
from codeants_2pf_hcr.paths import visible_rglob

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import csv
import math
import importlib.metadata
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from typing import Any, Mapping, Sequence


RUN_SPEC_VERSION = 1
RUN_STATE_VERSION = 1
_FISH_ID_RE = re.compile(r"^[A-Z][0-9]{3}_f[0-9]{2}$")


class RunSpecError(ValueError):
    """Raised when a declarative run specification is incomplete or invalid."""


@dataclass(frozen=True)
class StageSpec:
    name: str
    command: tuple[str, ...]
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]
    validators: tuple[tuple[str, ...], ...] = ()
    depends_on: tuple[str, ...] = ()
    manifest: str | None = None
    parameter_pins: Mapping[str, Any] | None = None
    optional_inputs: tuple[str, ...] = ()


@dataclass(frozen=True)
class RunSpec:
    path: Path
    fish_id: str
    repo_root: Path
    data_root: Path
    pipeline_root: Path
    state_root: Path
    python: Path
    python_version: str
    packages: Mapping[str, str]
    git_commit: str
    git_policy: str
    authoritative_paths: tuple[str, ...]
    environment: Mapping[str, str]
    provenance_pins: tuple[Mapping[str, str], ...]
    stages: tuple[StageSpec, ...]
    raw: Mapping[str, Any]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _json_hash(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def _expand(value: str, variables: Mapping[str, str]) -> str:
    try:
        return value.format_map(variables)
    except KeyError as exc:
        raise RunSpecError(f"unknown placeholder {exc.args[0]!r} in {value!r}") from exc


def _declared_path(value: Any, variables: Mapping[str, str]) -> str:
    """Reject accidental cwd hashing; all artifact declarations are absolute."""
    if not isinstance(value, str) or not value.strip():
        raise RunSpecError("artifact paths must be nonblank strings")
    expanded = _expand(value, variables)
    if not Path(expanded).is_absolute():
        raise RunSpecError(f"artifact path must be absolute: {expanded!r}")
    return expanded


def load_run_spec(path: Path | str) -> RunSpec:
    """Load and strictly validate one JSON single-fish run specification."""
    spec_path = Path(path).expanduser().resolve()
    raw = json.loads(spec_path.read_text(encoding="utf-8"))
    if raw.get("schema_version") != RUN_SPEC_VERSION:
        raise RunSpecError(f"schema_version must be {RUN_SPEC_VERSION}")
    fish_id = str(raw.get("fish_id", ""))
    if not _FISH_ID_RE.fullmatch(fish_id):
        raise RunSpecError(f"invalid fish_id: {fish_id!r}")
    paths = raw.get("paths") or {}
    interpreter = raw.get("interpreter") or {}
    git = raw.get("git") or {}
    required_paths = ("repo_root", "data_root", "pipeline_root", "state_root")
    missing = [key for key in required_paths if not paths.get(key)]
    if missing:
        raise RunSpecError(f"missing paths: {', '.join(missing)}")
    if not interpreter.get("executable") or not interpreter.get("python_version"):
        raise RunSpecError("interpreter executable and python_version are required")
    if not re.fullmatch(r"[0-9a-f]{40}", str(git.get("commit", ""))):
        raise RunSpecError("git.commit must be a full 40-character commit")
    git_policy = str(git.get("policy", "exact"))
    if git_policy not in {"exact", "scientific-surface"}:
        raise RunSpecError("git.policy must be exact or scientific-surface")
    authoritative_paths = tuple(str(value) for value in git.get("authoritative_paths", ()))
    if git_policy == "scientific-surface" and not authoritative_paths:
        raise RunSpecError("scientific-surface policy requires authoritative_paths")

    variables = {
        "fish_id": fish_id,
        "repo_root": str(Path(paths["repo_root"]).expanduser()),
        "data_root": str(Path(paths["data_root"]).expanduser()),
        "pipeline_root": str(Path(paths["pipeline_root"]).expanduser()),
        "python": str(Path(interpreter["executable"]).expanduser()),
        "spec_dir": str(spec_path.parent),
    }
    variables["fish_dir"] = str(Path(variables["data_root"]) / fish_id)
    stages: list[StageSpec] = []
    names: set[str] = set()
    for record in raw.get("stages") or ():
        name = str(record.get("name", ""))
        if not name or name in names:
            raise RunSpecError(f"stage names must be nonempty and unique: {name!r}")
        command = tuple(_expand(str(value), variables) for value in record.get("command") or ())
        if not command:
            raise RunSpecError(f"stage {name!r} has no command")
        validators = tuple(
            tuple(_expand(str(value), variables) for value in validator)
            for validator in record.get("validators") or ()
        )
        if any(not validator for validator in validators):
            raise RunSpecError(f"stage {name!r} has an empty validator")
        dependencies = tuple(record.get("depends_on", ()))
        if any(dependency not in names for dependency in dependencies):
            raise RunSpecError(f"stage {name!r} dependencies must precede it")
        for flag, value in (record.get("argument_pins") or {}).items():
            if flag not in command or (value is not True and command[command.index(flag) + 1] != str(value)):
                raise RunSpecError(f"stage {name!r} command violates pin {flag}")
        stages.append(
            StageSpec(
                name=name,
                command=command,
                inputs=tuple(_declared_path(value, variables) for value in record.get("inputs") or ()),
                outputs=tuple(_declared_path(value, variables) for value in record.get("outputs") or ()),
                validators=validators,
                depends_on=dependencies,
                manifest=_declared_path(record["manifest"], variables) if record.get("manifest") is not None else None,
                parameter_pins=record.get("parameter_pins") or {},
                optional_inputs=tuple(_declared_path(value, variables) for value in record.get("optional_inputs", ())),
            )
        )
        names.add(name)
    if not stages:
        raise RunSpecError("at least one stage is required")
    activity_pins = next((stage.parameter_pins.get("activity_config") for stage in stages
                          if stage.parameter_pins.get("activity_config")), None)
    params = raw.get("scientific_parameters", {})
    if activity_pins:
        if params.get("activity_config") != activity_pins:
            raise RunSpecError("scientific_parameters.activity_config contradicts writer pins")
        aliases = {"response_null_q_total_two_sided": "response_null_q",
                   "minimum_trials_per_class": "min_trials_per_class",
                   "stimulus_onset_delay_sec": "stim_onset_delay_sec",
                   "measurement_start_block": "measure_start_block",
                   "measurement_start_event": "measure_start_event"}
        for key, value in params.items():
            target = aliases.get(key, key)
            if target in activity_pins and activity_pins[target] != value:
                raise RunSpecError(f"scientific parameter {key} contradicts writer pins")
    choices = raw.get("accepted_choices", {})
    choice_aliases = {"functional_polarity": "polarity", "exclude_first_functional_block": "exclude_first_block"}
    for key, value in choices.items():
        target = choice_aliases.get(key, key)
        for stage in stages:
            if target in stage.parameter_pins and stage.parameter_pins[target] != value:
                raise RunSpecError(f"accepted choice {key} contradicts writer pins")
    return RunSpec(
        path=spec_path,
        fish_id=fish_id,
        repo_root=Path(variables["repo_root"]),
        data_root=Path(variables["data_root"]),
        pipeline_root=Path(variables["pipeline_root"]),
        state_root=Path(_expand(str(paths["state_root"]), variables)),
        python=Path(variables["python"]),
        python_version=str(interpreter["python_version"]),
        packages={str(k): str(v) for k, v in (interpreter.get("packages") or {}).items()},
        git_commit=str(git["commit"]),
        git_policy=git_policy,
        authoritative_paths=authoritative_paths,
        environment={str(k): _expand(str(v), variables) for k, v in (raw.get("environment") or {}).items()},
        provenance_pins=tuple(
            {
                **{str(k): str(v) for k, v in pin.items()},
                "path": _declared_path(pin.get("path"), variables),
            }
            for pin in raw.get("provenance_pins") or ()
        ),
        stages=tuple(stages),
        raw=raw,
    )


def _git(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ("git", "-C", str(repo), *args),
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def _runtime_report(spec: RunSpec) -> dict[str, Any]:
    head_proc = _git(spec.repo_root, "rev-parse", "HEAD")
    status_proc = _git(spec.repo_root, "status", "--porcelain")
    head = head_proc.stdout.strip() if head_proc.returncode == 0 else None
    dirty = bool(status_proc.stdout.strip()) if status_proc.returncode == 0 else None
    git_ok = head == spec.git_commit
    changed_authoritative: list[str] = []
    if spec.git_policy == "scientific-surface" and head and head != spec.git_commit:
        diff_proc = _git(
            spec.repo_root,
            "diff",
            "--name-only",
            f"{spec.git_commit}..{head}",
            "--",
            *spec.authoritative_paths,
        )
        changed_authoritative = [line for line in diff_proc.stdout.splitlines() if line]
        git_ok = diff_proc.returncode == 0 and not changed_authoritative

    executable = spec.python.expanduser()
    py_proc = subprocess.run(
        (str(executable), "-c", "import json,sys; print(json.dumps({'version':sys.version.split()[0],'executable':sys.executable}))"),
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ) if executable.exists() else None
    py_info: dict[str, Any] = {}
    if py_proc is not None and py_proc.returncode == 0:
        py_info = json.loads(py_proc.stdout)
    package_versions: dict[str, str | None] = {}
    if py_proc is not None and py_proc.returncode == 0 and spec.packages:
        package_code = "import importlib.metadata,json; names=" + repr(list(spec.packages)) + "; print(json.dumps({n:(importlib.metadata.version(n) if n else None) for n in names}))"
        package_proc = subprocess.run(
            (str(executable), "-c", package_code),
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if package_proc.returncode == 0:
            package_versions = json.loads(package_proc.stdout)
    package_mismatches = {
        name: {"expected": expected, "actual": package_versions.get(name)}
        for name, expected in spec.packages.items()
        if package_versions.get(name) != expected
    }
    return {
        "git": {
            "expected_commit": spec.git_commit,
            "actual_commit": head,
            "policy": spec.git_policy,
            "changed_authoritative_paths": changed_authoritative,
            "dirty": dirty,
            "ok": git_ok and dirty is False,
        },
        "interpreter": {
            "expected_executable": str(executable),
            "actual_executable": py_info.get("executable"),
            "expected_python_version": spec.python_version,
            "actual_python_version": py_info.get("version"),
            "package_versions": package_versions,
            "package_mismatches": package_mismatches,
            "ok": (
                py_info.get("executable") == str(executable)
                and py_info.get("version") == spec.python_version
                and not package_mismatches
            ),
        },
    }


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def describe_path(path_value: str | Path) -> dict[str, Any]:
    """Return a deterministic content record for a file, symlink, or tree."""
    if isinstance(path_value, str) and not path_value.strip():
        raise RunSpecError("cannot hash a blank path (it would mean the working directory)")
    path = Path(path_value)
    record: dict[str, Any] = {"path": str(path), "exists": path.exists() or path.is_symlink()}
    if path.is_symlink():
        target = os.readlink(path)
        resolved = path.resolve()
        target_record = describe_path(resolved) if resolved != path else {"kind": "unresolved"}
        record.update({"kind": "symlink", "target": target, "target_record": target_record, "sha256": _json_hash({"target": target, "record": target_record})})
        return record
    if path.is_file():
        record.update({"kind": "file", "size": path.stat().st_size, "sha256": _file_hash(path)})
        return record
    if path.is_dir():
        members: list[dict[str, Any]] = []
        total_size = 0
        for child in sorted(visible_rglob(path, "*"), key=lambda value: value.relative_to(path).as_posix()):
            relative = child.relative_to(path).as_posix()
            if child.is_symlink():
                target = os.readlink(child)
                target_record = describe_path(child.resolve())
                members.append({"path": relative, "kind": "symlink", "target": target, "target_record": target_record})
            elif child.is_file():
                size = child.stat().st_size
                total_size += size
                members.append({"path": relative, "kind": "file", "size": size, "sha256": _file_hash(child)})
        record.update({"kind": "directory", "file_count": sum(m["kind"] == "file" for m in members), "size": total_size, "sha256": _json_hash(members)})
        return record
    record["kind"] = "missing"
    return record


def _describe_paths(paths: Sequence[str]) -> list[dict[str, Any]]:
    return [describe_path(path) for path in paths]


def _state_path(spec: RunSpec, stage: StageSpec) -> Path:
    return spec.state_root / "stages" / f"{stage.name}.json"


def _load_state(spec: RunSpec, stage: StageSpec) -> dict[str, Any] | None:
    path = _state_path(spec, stage)
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def _spec_hash(spec: RunSpec) -> str:
    return _json_hash(spec.raw)


def _dependency_records(spec: RunSpec, stage: StageSpec) -> dict[str, str | None]:
    by_name = {item.name: item for item in spec.stages}
    return {name: (_json_hash(state) if (state := _load_state(spec, by_name[name])) else None)
            for name in stage.depends_on}


def _stage_current(spec: RunSpec, stage: StageSpec, runtime: Mapping[str, Any]) -> tuple[bool, list[str]]:
    state = _load_state(spec, stage)
    reasons: list[str] = []
    if state is None:
        return False, ["no state record"]
    if state.get("exit_status") != 0 or state.get("validation", {}).get("status") != "pass":
        reasons.append("previous run did not pass")
    if state.get("spec_sha256") != _spec_hash(spec):
        reasons.append("spec changed")
    if state.get("git_commit") != runtime["git"].get("actual_commit"):
        reasons.append("checkout commit changed")
    if state.get("interpreter") != runtime["interpreter"].get("actual_executable"):
        reasons.append("interpreter changed")
    if state.get("dependencies", {}) != _dependency_records(spec, stage):
        reasons.append("upstream state changed")
    current_inputs = _describe_paths(stage.inputs + stage.optional_inputs)
    current_outputs = _describe_paths(stage.outputs)
    if state.get("inputs") != current_inputs:
        reasons.append("inputs changed")
    if state.get("outputs") != current_outputs:
        reasons.append("outputs changed")
    if "promoted_products" in state:
        owned = state["promoted_products"]
        if owned != _describe_paths(tuple(record["path"] for record in owned)):
            reasons.append("promoted writer products changed")
    if any(not record["exists"] for record in current_outputs):
        reasons.append("declared output missing")
    return not reasons, reasons


def validate_spec_environment(spec: RunSpec) -> dict[str, Any]:
    """Validate checkout, interpreter, package, path, and accepted provenance pins."""
    runtime = _runtime_report(spec)
    auxiliary = []
    for pin in spec.raw.get("auxiliary_interpreters", ()):
        proc = subprocess.run([pin["executable"], "-c", "import sys,importlib.metadata,json; print(json.dumps(dict(version=sys.version.split()[0], packages={n:importlib.metadata.version(n) for n in " + repr(list(pin.get("packages", {}))) + "})))"], capture_output=True, text=True)
        actual = json.loads(proc.stdout) if proc.returncode == 0 else {}
        auxiliary.append({"executable": pin["executable"], "actual": actual,
                          "ok": actual.get("version") == pin["python_version"] and actual.get("packages") == pin.get("packages", {})})
    pins: list[dict[str, Any]] = []
    for pin in spec.provenance_pins:
        path = Path(str(pin.get("path", "")))
        actual = describe_path(path)
        expected = str(pin.get("sha256", ""))
        pins.append({
            "name": str(pin.get("name", path.name)),
            "path": str(path),
            "expected_sha256": expected,
            "actual_sha256": actual.get("sha256"),
            "ok": bool(expected) and actual.get("sha256") == expected,
        })
    required_paths = {
        "repo_root": spec.repo_root.exists(),
        "data_root": spec.data_root.exists(),
        "fish_dir": (spec.data_root / spec.fish_id).exists(),
        "pipeline_root": spec.pipeline_root.exists(),
    }
    return {
        **runtime,
        "required_paths": required_paths,
        "provenance_pins": pins,
        "auxiliary_interpreters": auxiliary,
        "ok": (
            runtime["git"]["ok"]
            and runtime["interpreter"]["ok"]
            and all(required_paths.values())
            and all(pin["ok"] for pin in pins)
            and all(pin["ok"] for pin in auxiliary)
        ),
    }


def build_run_plan(spec: RunSpec, *, resume: bool) -> dict[str, Any]:
    """Build a compact, read-only execution plan."""
    environment = validate_spec_environment(spec)
    stages: list[dict[str, Any]] = []
    if spec.raw.get("native_production", {}).get("mode") == "fresh":
        from .run_preflight import preflight_native_run
        environment["execution_preflight"] = preflight_native_run(spec)
        environment["ok"] = environment["ok"] and environment["execution_preflight"]["ok"]
    for stage in spec.stages:
        current, reasons = _stage_current(spec, stage, environment)
        stages.append({
            "name": stage.name,
            "action": "skip" if resume and current else "run",
            "current": current,
            "reasons": reasons,
            "command": list(stage.command),
            "inputs": list(stage.inputs),
            "outputs": list(stage.outputs),
        })
    return {
        "fish_id": spec.fish_id,
        "spec": str(spec.path),
        "spec_sha256": _spec_hash(spec),
        "environment": environment,
        "mode": "resume" if resume else "run",
        "stages": stages,
    }


def _run_command(command: Sequence[str], *, cwd: Path, env: Mapping[str, str], log_path: Path) -> int:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as log:
        process = subprocess.run(command, cwd=cwd, env=dict(env), text=True, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, check=False)
    return int(process.returncode)


def _execute_stage(spec: RunSpec, stage: StageSpec, runtime: Mapping[str, Any]) -> dict[str, Any]:
    started_at = _utc_now()
    inputs = _describe_paths(stage.inputs + stage.optional_inputs)
    missing_inputs = [record["path"] for record in inputs if not record["exists"] and record["path"] in stage.inputs]
    log_path = spec.state_root / "logs" / f"{stage.name}.log"
    validation_checks: list[dict[str, Any]] = []
    env = os.environ.copy()
    env.update(spec.environment)
    env["PYTHONPATH"] = str(spec.repo_root / "src")
    if missing_inputs:
        exit_status = 2
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text("Missing declared inputs:\n" + "\n".join(missing_inputs) + "\n", encoding="utf-8")
    else:
        exit_status = _run_command(stage.command, cwd=spec.repo_root, env=env, log_path=log_path)
    outputs = _describe_paths(stage.outputs)
    output_check = {
        "kind": "declared_outputs_exist",
        "status": "pass" if all(record["exists"] for record in outputs) else "fail",
        "missing": [record["path"] for record in outputs if not record["exists"]],
    }
    validation_checks.append(output_check)
    if exit_status == 0 and stage.manifest:
        try:
            manifest = json.loads(Path(stage.manifest).read_text())
            valid = (manifest.get("fish_id") == spec.fish_id
                     and manifest.get("status") in {"pass", "warn"}
                     and not manifest.get("errors") and manifest.get("dry_run") is False
                     and all(check.get("status") != "fail" for check in manifest.get("checks", ())))
            parameters = manifest.get("parameters", {})
            valid = valid and parameters.get("pipeline_root") == str(spec.pipeline_root)
            valid = valid and datetime.fromisoformat(manifest.get("generated_at", "").replace("Z", "+00:00")) >= datetime.fromisoformat(started_at.replace("Z", "+00:00"))
            valid = valid and all(parameters.get(key) == value for key, value in stage.parameter_pins.items())
            validation_checks.append({"kind": "writer_manifest", "path": stage.manifest,
                                      "status": "pass" if valid else "fail"})
        except (OSError, ValueError) as exc:
            validation_checks.append({"kind": "writer_manifest", "status": "fail", "error": str(exc)})
    if exit_status == 0 and output_check["status"] == "pass":
        for index, validator in enumerate(stage.validators, start=1):
            validator_log = spec.state_root / "logs" / f"{stage.name}.validator-{index}.log"
            code = _run_command(validator, cwd=spec.repo_root, env=env, log_path=validator_log)
            validation_checks.append({
                "kind": "command",
                "command": list(validator),
                "exit_status": code,
                "log": str(validator_log),
                "status": "pass" if code == 0 else "fail",
            })
            if code != 0:
                break
    validation_status = "pass" if exit_status == 0 and all(item["status"] == "pass" for item in validation_checks) else "fail"
    state = {
        "schema_version": RUN_STATE_VERSION,
        "fish_id": spec.fish_id,
        "stage": stage.name,
        "spec_path": str(spec.path),
        "spec_sha256": _spec_hash(spec),
        "command": list(stage.command),
        "git_commit": runtime["git"].get("actual_commit"),
        "scientific_code_commit": spec.git_commit,
        "interpreter": runtime["interpreter"].get("actual_executable"),
        "started_at": started_at,
        "finished_at": _utc_now(),
        "inputs": inputs,
        "dependencies": _dependency_records(spec, stage),
        "outputs": outputs,
        "exit_status": exit_status,
        "log": str(log_path),
        "validation": {"status": validation_status, "checks": validation_checks},
    }
    _write_json_atomic(_state_path(spec, stage), state)
    return state


def execute_run(spec: RunSpec, *, resume: bool) -> dict[str, Any]:
    """Execute the declared DAG, optionally skipping hash-current stages."""
    if spec.raw.get("native_production"):
        from .native_run import execute_native_run
        import fcntl
        spec.state_root.mkdir(parents=True, exist_ok=True)
        with (spec.state_root / "run.lock").open("a") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise RuntimeError("Another native run owns this fish") from exc
            try:
                return execute_native_run(spec, resume=resume)
            except Exception as exc:
                marker = spec.state_root / "current_stage.json"
                current = json.loads(marker.read_text()) if marker.exists() else {}
                failed = next((stage for stage in spec.stages if stage.name == current.get("stage")), None)
                if failed and current.get("state") == "running":
                    _write_json_atomic(_state_path(spec, failed), {"stage": failed.name, "exit_status": 1, "validation": {"status": "fail", "error": str(exc)}})
                current.update({"state": "failed", "error": str(exc), "finished_at": _utc_now()})
                _write_json_atomic(marker, current)
                _write_json_atomic(spec.state_root / "run_summary.json", {"fish_id": spec.fish_id, "status": "fail", "failed_stage": current.get("stage"), "error": str(exc)})
                raise
    started_at = _utc_now()
    plan = build_run_plan(spec, resume=resume)
    if not plan["environment"]["ok"]:
        raise RuntimeError("run environment does not match the pinned specification")
    results: list[dict[str, Any]] = []
    for stage, planned in zip(spec.stages, plan["stages"]):
        # Recompute after every upstream execution: the initial plan is only a
        # snapshot and cannot safely decide downstream skips.
        current, _ = _stage_current(spec, stage, plan["environment"])
        if resume and current:
            results.append({"stage": stage.name, "action": "skip", "status": "pass"})
            continue
        state = _execute_stage(spec, stage, plan["environment"])
        results.append({"stage": stage.name, "action": "run", "status": state["validation"]["status"]})
        if state["validation"]["status"] != "pass":
            break
    payload = {
        "fish_id": spec.fish_id,
        "spec": str(spec.path),
        "spec_sha256": _spec_hash(spec),
        "started_at": started_at,
        "mode": "resume" if resume else "run",
        "results": results,
        "status": "pass" if len(results) == len(spec.stages) and all(item["status"] == "pass" for item in results) else "fail",
    }
    _write_json_atomic(spec.state_root / "run_summary.json", payload)
    return payload


def build_run_status(spec: RunSpec) -> dict[str, Any]:
    """Summarize saved state without executing stages."""
    runtime = _runtime_report(spec)
    stages = []
    for stage in spec.stages:
        state = _load_state(spec, stage)
        current, reasons = _stage_current(spec, stage, runtime)
        stages.append({
            "name": stage.name,
            "recorded_status": None if state is None else state.get("validation", {}).get("status"),
            "current": current,
            "reasons": reasons,
            "state_path": str(_state_path(spec, stage)),
        })
    return {"fish_id": spec.fish_id, "spec": str(spec.path), "runtime": runtime, "stages": stages}


def verify_run(spec: RunSpec) -> dict[str, Any]:
    """Verify pinned environment/provenance and every recorded stage hash."""
    environment = validate_spec_environment(spec)
    status = build_run_status(spec)
    ok = environment["ok"] and all(stage["current"] for stage in status["stages"])
    return {"fish_id": spec.fish_id, "spec": str(spec.path), "environment": environment, "stages": status["stages"], "ok": ok}


def package_version(name: str) -> str | None:
    """Return an installed package version; kept public for provenance tools."""
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return None


def audit_run_artifacts(spec: RunSpec) -> dict[str, Any]:
    """Check real persisted scientific tables without invoking any writer."""
    root = spec.pipeline_root
    checks: list[dict[str, Any]] = []
    tables: dict[str, list[dict[str, str]]] = {}
    paths = {"geometry": "match-roi-to-anatomy/registration/functional_roi_anatomy_matches.csv",
             "score": "score-activity-bpi/registration/functional_roi_activity_identity.csv",
             "identity": "assign-hcr-identity/registration/functional_roi_activity_identity.csv",
             "canonical": "export-canonical-tables/registration/functional_roi_activity_identity.csv",
             "windows": "score-activity-bpi/registration/scored_stimulus_windows.csv",
             "23c": "prepare-functional-reference-stacks/qa/suite2p_presented_stimulus_response_23c.csv"}
    def check(label: str, ok: bool, detail: Any) -> None:
        checks.append({"label": label, "status": "pass" if ok else "fail", "detail": detail})
    try:
        for name, relative in paths.items():
            stage, relative_product = relative.split("/", 1)
            if spec.raw.get("native_production"):
                from .native_paths import native_product_path
                product_path = native_product_path(root, stage, relative_product)
            else:
                product_path = root / relative
            with product_path.open(newline="") as handle:
                tables[name] = list(csv.DictReader(handle))
            check(f"{name} nonempty and fish identity", bool(tables[name]) and all(row.get("fish_id") == spec.fish_id for row in tables[name]), len(tables[name]))
        keys = {}
        for name in ("geometry", "score", "identity", "canonical"):
            rows = tables[name]; key = {(row["plane_idx"], row["func_label"]) for row in rows}; keys[name] = key
            check(f"{name} unique ROI keys", len(key) == len(rows), len(key))
        check("canonical key relationships", all(key == keys["geometry"] for key in keys.values()), {name: len(key) for name, key in keys.items()})
        rows = tables["canonical"]
        signed = ("bout_positive_response_pass", "bout_negative_response_pass", "cont_positive_response_pass", "cont_negative_response_pass", "response_has_suppression")
        check("canonical signed response schema", all(column in rows[0] for column in signed), list(signed))
        truth = lambda value: str(value).lower() in {"true", "1"}
        check("signed response union", all(truth(row["response_is_active"]) == any(truth(row[column]) for column in signed[:4]) for row in rows), "active = activation or suppression in either class")
        early = tables["23c"]; stimuli = {row["stim_type"] for row in early}
        q = float(spec.raw["scientific_parameters"]["response_null_q_total_two_sided"])
        lower = (1 - q) / (2 * len(stimuli)); upper = 1 - lower
        check("23c family-wise both tails", len(stimuli) == spec.raw["scientific_parameters"].get("early_23c_expected_stimulus_count", len(stimuli)) and all(math.isclose(float(row["null_lower_quantile_familywise"]), lower) and math.isclose(float(row["null_upper_quantile_familywise"]), upper) and truth(row["response_pass"]) == (truth(row["positive_response_pass"]) or truth(row["negative_response_pass"])) for row in early), {"stimulus_count": len(stimuli), "lower": lower, "upper": upper})
        windows = tables["windows"]; pool = [row for row in windows if truth(row["used_for_bpi"])]
        check("fixed scored windows", all(math.isclose(float(row["scored_duration_sec"]), float(spec.raw["scientific_parameters"]["response_window_sec"])) for row in pool), {"total": len(windows), "bpi_pool": len(pool)})
        baseline = spec.raw.get("artifact_baseline", {})
        membership = sorted([[row[key] for key in ("plane_idx", "session_label", "block", "stim_idx", "stim_type")] for row in pool])
        pool_hash = _json_hash(membership)
        if "bpi_pool_sha256" in baseline:
            check("BPI pool membership baseline", pool_hash == baseline["bpi_pool_sha256"], pool_hash)
        geometry_hash = _json_hash(sorted([[row.get(key, "") for key in ("plane_idx", "func_label", "anat_label", "selected_anat_label", "has_unique_anat_match")] for row in tables["geometry"]]))
        if "geometry_assignment_sha256" in baseline:
            check("geometry baseline", geometry_hash == baseline["geometry_assignment_sha256"], geometry_hash)
        if spec.raw.get("native_production") and spec.raw["native_production"].get("mode", "frozen_replay") == "frozen_replay":
            from .native_run import _rebind
            from .native_paths import native_product_path
            policy = spec.raw["native_production"]; frozen = Path(policy["frozen_root"])
            original = frozen / "match-roi-to-anatomy/registration/functional_roi_anatomy_matches.csv"
            actual = native_product_path(root, "match-roi-to-anatomy", "registration/functional_roi_anatomy_matches.csv")
            check("entire frozen geometry table including coordinates", actual.read_text() == _rebind(original.read_text(), [policy["original_root"]], root), str(actual))
            transforms = list(visible_rglob(frozen / "register-functional-to-anatomy", "*.mat"))
            check("frozen ANTs transform bytes", bool(transforms) and all(_file_hash(path) == _file_hash(native_product_path(root, "register-functional-to-anatomy", path.relative_to(frozen / "register-functional-to-anatomy").as_posix())) for path in transforms), len(transforms))
        counts = {"roi_rows": len(rows), "responsive": sum(truth(row["response_is_active"]) for row in rows), "activated": sum(truth(row[signed[0]]) or truth(row[signed[2]]) for row in rows), "suppressed": sum(truth(row["response_is_active"]) and truth(row["response_has_suppression"]) for row in rows)}
        for pin in spec.raw.get("artifact_byte_baselines", ()):
            path = root / pin["relative_path"]
            if not path.resolve().is_relative_to(root.resolve()):
                raise ValueError("Artifact baseline escapes the fish root")
            data = path.read_bytes()
            if pin.get("normalize_fish_root"):
                data = data.replace(str(root).encode(), b"{fish_dir}")
                if pin.get("source_fish_root"):
                    data = data.replace(pin["source_fish_root"].encode(), b"{fish_dir}")
            check("control artifact bytes: " + pin["relative_path"], hashlib.sha256(data).hexdigest() == pin["sha256"], str(path))
    except (OSError, KeyError, ValueError, ZeroDivisionError, IndexError) as exc:
        check("artifact readability/schema", False, str(exc)); counts = {}
    return {"fish_id": spec.fish_id, "pipeline_root": str(root), "checks": checks,
            "counts": counts, "ok": bool(checks) and all(item["status"] == "pass" for item in checks)}
