"""Validated native-file promotion; scientific stages keep their own writers."""
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
import json
import os
import shutil
from .native_paths import native_product_path


def _owned_directory(fish: Path, product: Path) -> Path:
    functional = fish / "03_analysis" / "functional"
    roots = [functional / name for name in ("raw", "registration", "ncc", "qa")]
    roots += [functional / "derived" / name for name in ("score-activity-bpi", "assign-hcr-identity")]
    roots += [functional / "derived", fish / "03_analysis" / "confocal" / "aligned", fish / "04_plots", fish / "02_reg" / "00_preprocessing" / "2p_anatomy"]
    for root in roots:
        if product.is_relative_to(root):
            return root
    raise ValueError(f"Product has no declared native ownership: {product}")


def promote_files(fish: Path, files: dict[Path, Path], journal: Path, *, manifest_path: Path | None = None) -> None:
    """Atomically replace individual files; journal a recoverable stage set.

    exFAT cannot exchange populated directories. The manifest/state publication
    gates the complete stage; arbitrary readers do not receive an atomic set.
    """
    from .single_fish_run import _file_hash, _write_json_atomic
    journal.mkdir(parents=True, exist_ok=False)
    if manifest_path is not None:
        _write_json_atomic(journal / "previous_manifest.json", {"path": str(manifest_path), "payload": json.loads(manifest_path.read_text()) if manifest_path.exists() else None})
    try:
        for index, (source, destination) in enumerate(files.items()):
            _owned_directory(fish, destination)
            destination.parent.mkdir(parents=True, exist_ok=True)
            backup = journal / f"original-{index}"
            old_hash = None
            if destination.exists():
                shutil.copy2(destination, backup)
                old_hash = _file_hash(destination)
                if _file_hash(backup) != old_hash: raise RuntimeError("Native backup checksum mismatch")
            replacement = destination.with_name(f".{destination.name}.promote-{os.getpid()}")
            shutil.copy2(source, replacement)
            new_hash = _file_hash(source)
            if _file_hash(replacement) != new_hash: raise RuntimeError("Native promotion checksum mismatch")
            intent = {"destination": str(destination), "backup": str(backup), "old_sha256": old_hash, "new_sha256": new_hash, "state": "ready"}
            _write_json_atomic(journal / f"intent-{index:05}.json", intent)
            replacement.replace(destination)
            intent["state"] = "replaced"
            _write_json_atomic(journal / f"intent-{index:05}.json", intent)
        _write_json_atomic(journal / "commit.json", {"state": "products_promoted_pending_manifest"})
    except Exception:
        rollback_promotion(journal)
        raise


def rollback_promotion(journal: Path) -> None:
    """Recover exact prior files after an interrupted uncommitted stage."""
    from .single_fish_run import _file_hash, _write_json_atomic
    if (journal / "rollback.json").exists(): return
    for path in sorted(journal.glob("intent-*.json"), reverse=True):
        intent = json.loads(path.read_text()); target = Path(intent["destination"]); backup = Path(intent["backup"])
        actual = _file_hash(target) if target.exists() else None
        if actual == intent["old_sha256"]: continue
        if actual != intent["new_sha256"]: raise RuntimeError(f"Ambiguous interrupted promotion at {target}; inspect {journal}")
        if intent["old_sha256"]:
            restored = target.with_name(f".{target.name}.rollback-{os.getpid()}")
            shutil.copy2(backup, restored)
            if _file_hash(restored) != intent["old_sha256"]: raise RuntimeError("Rollback checksum mismatch")
            restored.replace(target)
        else:
            target.replace(journal / (path.stem + "-uncommitted-new"))
    for backup in journal.glob("previous*manifest.json"):
        previous = json.loads(backup.read_text()); target = Path(previous["path"])
        if previous["payload"] is not None: _write_json_atomic(target, previous["payload"])
        elif target.exists(): target.replace(journal / (backup.stem + "-uncommitted.json"))
    _write_json_atomic(journal / "rollback.json", {"state": "rolled_back"})


def _rebind(text: str, roots: list[str], fish: Path) -> str:
    """Replace product prefixes longest first; provenance roots remain explicit."""
    stages = ("prepare-functional-reference-stacks", "register-functional-to-anatomy", "transform-functional-rois-to-anatomy", "make-functional-registration-qc", "register-hcr-to-anatomy", "match-roi-to-anatomy", "score-activity-bpi", "assign-hcr-identity", "export-canonical-tables", "make-qa-report", "make-anatomy-midline-proposal", "materialize-anatomy-midline", "make-motion-auc-tables", "make-figures")
    routes = []
    for root in roots:
        for stage in stages:
            for relative in ("functional/raw", "functional/anatomy", "functional/native", "confocal/aligned", "registration", "ncc", "qa", "04_plots", "plane_refs_summary.json", "plane_refs_summary_qc.json", "functional_roi_anatomy_transform_manifest.csv", "anatomy_midline.json", "anatomy_midline_lines.csv", "qa_report.md", "qa_report.html", "qa_report.pdf", "qa_report_summary.json"):
                try:
                    target = native_product_path(fish, stage, relative)
                except ValueError:
                    continue
                routes.append((str(Path(root) / stage / relative), str(target)))
    for source, destination in sorted(routes, key=lambda pair: len(pair[0]), reverse=True):
        text = text.replace(source, destination)
    for root in roots:
        for stage in stages:
            target = fish / "03_analysis/functional/derived" / stage if stage in {"score-activity-bpi", "assign-hcr-identity"} else fish
            text = text.replace(str(Path(root) / stage), str(target))
    # The command-level root is provenance; operative product paths already
    # went through the more specific mappings above.
    text = text.replace(roots[0], str(fish))
    return text


def _relocate_tree(stage_root: Path, roots: list[str], fish: Path) -> None:
    for path in stage_root.rglob("*"):
        if path.is_file() and path.suffix.lower() in {".json", ".csv", ".md", ".html", ".txt"} and not path.name.startswith("._"):
            original = path.read_text()
            relocated = _rebind(original, roots, fish)
            if relocated != original:
                path.write_text(relocated)


def _relocate_accepted_midline(sidecar: Path, frozen: Path, roots: list[str], fish: Path) -> None:
    """Bind reviewed sources only after proving exact path-only relocation."""
    from .single_fish_run import _file_hash, _write_json_atomic
    reviewed = json.loads((frozen / "materialize-anatomy-midline/anatomy_midline.json").read_text())
    operative = json.loads(sidecar.read_text())
    expected = json.loads(_rebind(json.dumps(reviewed), roots, fish))
    if operative != expected:
        raise RuntimeError("Accepted midline sidecar differs from reviewed path-only relocation")
    provenance = []
    for original, artifact in zip(reviewed["source_artifacts"], operative["source_artifacts"]):
        source = Path(original["path"])
        if source.is_relative_to(roots[1]):
            source = frozen / source.relative_to(roots[1])
        if _file_hash(source) != original["sha256"]:
            raise RuntimeError(f"Reviewed frozen midline source hash mismatch: {source}")
        target = Path(artifact["path"])
        actual = _file_hash(target)
        if actual == original["sha256"]:
            continue
        if source.suffix != ".json" or target.read_text() != _rebind(source.read_text(), roots, fish):
            raise RuntimeError(f"Midline source is not an exact approved path-only relocation: {target}")
        artifact["sha256"] = actual
        provenance.append({"reviewed_path": original["path"], "reviewed_sha256": original["sha256"], "frozen_path": str(source), "operative_path": str(target), "operative_sha256": actual, "change": "exact path-prefix relocation only"})
    if provenance:
        operative["relocation_provenance"] = provenance
    _write_json_atomic(sidecar, operative)


def _refresh_records(records):
    from .pipeline import describe_manifest_path, describe_glob
    result = []
    for record in records:
        if record.get("kind") == "glob":
            current = describe_glob(record["path"], record["pattern"], required=record.get("required", True), label=record.get("label"))
        else:
            current = describe_manifest_path(record["path"], required=record.get("required", True), label=record.get("label"))
        result.append(asdict(current))
    return result


def _native_proposal_manifest(frozen_manifest: Path, frozen: Path, roots: list[str], fish: Path):
    """Publish immutable reviewed proposal files with current native records."""
    from .single_fish_run import _file_hash, _utc_now
    original = json.loads(frozen_manifest.read_text())
    if original.get("errors") or original.get("status") not in {"pass", "warn"}:
        raise RuntimeError("Frozen proposal manifest did not pass")
    native = json.loads(_rebind(json.dumps(original), roots, fish))
    for reviewed, operative in zip(original["outputs"], native["outputs"]):
        source = frozen / Path(reviewed["path"]).relative_to(roots[1])
        if _file_hash(source) != _file_hash(Path(operative["path"])):
            raise RuntimeError("Native accepted proposal differs from frozen reviewed bytes")
    native["source_writer_inputs"] = original.get("inputs", [])
    native["inputs"] = _refresh_records(native.get("inputs", []))
    native["outputs"] = _refresh_records(native["outputs"])
    if any(record.get("required") and not record["exists"] for record in native["inputs"] + native["outputs"]):
        raise RuntimeError("Native accepted proposal manifest has missing required paths")
    native["parameters"]["native_layout"] = "existing-fish-tree-v1"
    native["parameters"]["pipeline_root"] = str(fish)
    native["parameters"]["output_root"] = str(fish)
    native["pipeline_root"] = str(fish)
    native["reused_from"] = {"manifest": str(frozen_manifest), "manifest_sha256": _file_hash(frozen_manifest), "writer_generated_at": original["generated_at"], "original_pipeline_root": original.get("parameters", {}).get("pipeline_root"), "action": "reuse_verified_reviewed_proposal"}
    native["generated_at"] = _utc_now()
    return native


def _verify_reuse_inputs(manifest, fish: Path, original_root: str, *, aligned_override: str | None = None) -> None:
    """Check current immutable raw inputs against the archived writer inventory."""
    aligned = fish / "03_analysis/confocal/aligned"
    if aligned_override is not None:
        # This one accepted-source override imports the frozen aligned bundle,
        # including matches that were absent from the original source folder.
        # Existing accepted masks must still agree byte-for-byte.
        from .single_fish_run import _file_hash
        override = Path(aligned_override)
        labels = list(override.glob("*_cp_masks_in_2p_labels_uint16.tif"))
        if not labels: raise RuntimeError("Accepted frozen aligned override has no labels")
        for source in labels:
            current = aligned / source.name
            if current.exists() and _file_hash(current) != _file_hash(source):
                raise RuntimeError(f"Accepted native HCR label differs from frozen source: {current}")
    for old in manifest.get("inputs", []):
        path = Path(old["path"])
        if aligned_override is not None and path == aligned: continue
        if not path.is_relative_to(fish) or str(path).startswith(original_root): continue
        if old.get("kind") not in {"file", "glob"}: continue
        current = _refresh_records([old])[0]
        if old.get("required") and not current["exists"]: raise RuntimeError(f"Frozen input no longer exists: {path}")
        if old.get("exists") and any(old.get(key) != current.get(key) for key in ("size_bytes", "count", "mtime")):
            raise RuntimeError(f"Frozen input inventory changed; recompute required: {path}")


def validate_native_inputs(inputs, destinations) -> None:
    produced = {str(path) for path in destinations}
    missing = [path for path in inputs if not Path(path).exists() and path not in produced]
    if missing: raise RuntimeError(f"Required native input missing before promotion: {missing}")


def execute_native_run(spec, *, resume: bool):
    from .single_fish_run import (_utc_now, _runtime_report, validate_spec_environment, _stage_current, _execute_stage, _write_json_atomic, _spec_hash, _describe_paths, _dependency_records, _state_path, _run_command, _file_hash)
    environment = validate_spec_environment(spec)
    if not environment["ok"]:
        raise RuntimeError("Production environment does not match the pinned specification")
    fish = spec.data_root / spec.fish_id
    policy = spec.raw["native_production"]
    mode = policy.get("mode", "frozen_replay")
    if mode not in {"fresh", "frozen_replay"}:
        raise ValueError(f"Unknown native production mode: {mode}")
    if mode == "fresh" and policy.get("reuse_stages"):
        raise ValueError("Fresh native runs cannot reuse frozen writer stages")
    if mode == "fresh" and spec.pipeline_root != fish:
        raise ValueError("Fresh native production pipeline_root must be the fish folder")
    if mode == "fresh":
        for declared in spec.stages:
            if len(declared.command) > 2 and declared.command[2] == "register-functional-to-anatomy":
                flag = "--ants-deterministic-seed"
                if flag not in declared.command or int(declared.command[declared.command.index(flag) + 1]) <= 0:
                    raise ValueError("Fresh ANTs registration requires an explicit positive fixed seed; zero uses default random seeding")
    frozen = Path(policy["frozen_root"]) if mode == "frozen_replay" else None
    source_root = policy.get("original_root") if frozen is not None else None
    graph = spec.state_root / "transaction_graph"
    if mode == "fresh":
        from .run_preflight import preflight_native_run, materialize_retained_graph
        preflight = preflight_native_run(spec)
        if not preflight["ok"]:
            raise RuntimeError("Execution preflight failed: " + "; ".join(preflight["errors"]))
        materialize_retained_graph(spec)
    graph.mkdir(parents=True, exist_ok=True)
    for journal in (spec.state_root / "history").glob("*"):
        if journal.is_dir() and list(journal.glob("intent-*.json")) and not (journal / "stage_commit.json").exists() and not (journal / "rollback.json").exists():
            rollback_promotion(journal)
            backup = journal / "previous_manifest.json"
            if backup.exists():
                previous = json.loads(backup.read_text())
                if previous["payload"] is not None: _write_json_atomic(Path(previous["path"]), previous["payload"])
    # Accepted proposal is an immutable input, outside the writer DAG.
    proposal = frozen / "make-anatomy-midline-proposal" if frozen is not None else None
    if proposal is not None and not (graph / proposal.name).exists():
        shutil.copytree(proposal, graph / proposal.name)
    layout_path = fish / "03_analysis/functional/pipeline_manifests/native_layout.json"
    layout = {"fish_id": spec.fish_id, "pipeline_root": str(fish), "layout": "existing-fish-tree-v1"}
    if not layout_path.exists():
        _write_json_atomic(layout_path, layout)
    elif json.loads(layout_path.read_text()) != layout:
        raise RuntimeError("Existing native layout disagrees with the run specification")
    results = []
    for stage in spec.stages:
        current, _ = _stage_current(spec, stage, environment)
        if resume and current:
            results.append({"stage": stage.name, "action": "skip", "status": "pass"}); continue
        if not stage.manifest:
            state = _execute_stage(spec, stage, environment)
            results.append({"stage": stage.name, "action": "run", "status": state["validation"]["status"]})
            if state["validation"]["status"] != "pass": break
            continue
        writer = stage.command[2]
        started = _utc_now()
        output_dir = graph / writer
        log = spec.state_root / "logs" / (stage.name + ".log")
        imported = writer in policy.get("reuse_stages", ())
        _write_json_atomic(spec.state_root / "current_stage.json", {"stage": stage.name, "started_at": started, "state": "running"})
        actual_command = None
        if imported:
            if writer == "prepare-in-vivo-anatomy-stack":
                output_dir.mkdir(exist_ok=True)
                source = fish / "02_reg/00_preprocessing/2p_anatomy" / f"{spec.fish_id}_anatomy_2P_GCaMP.nrrd"
                shutil.copy2(source, output_dir / source.name)
            else:
                if output_dir.exists():
                    archived = spec.state_root / "history" / ("graph-" + stage.name + "-" + datetime.now().strftime("%Y%m%dT%H%M%S%f"))
                    archived.parent.mkdir(parents=True, exist_ok=True);output_dir.replace(archived)
                shutil.copytree(frozen / writer, output_dir)
                for original in (frozen / writer).rglob("*"):
                    if original.is_file() and not original.name.startswith("._"):
                        copied = output_dir / original.relative_to(frozen / writer)
                        if _file_hash(original) != _file_hash(copied): raise RuntimeError("Frozen reuse hash mismatch")
            manifest = json.loads((Path(policy["frozen_manifests"]) / (writer + "_manifest.json")).read_text())
            override = policy.get("reuse_source_overrides", {}).get(writer)
            if override is not None:
                expected = frozen / "register-hcr-to-anatomy/confocal/aligned"
                if writer != "register-hcr-to-anatomy" or Path(override) != expected or override not in stage.inputs:
                    raise RuntimeError("Undeclared accepted aligned-source override")
                manifest["accepted_source_override"] = {"path": override, "reason": "frozen accepted labels and generated matching tables; archived source matching glob was absent", "original_source_inputs": manifest.get("inputs", [])}
            _verify_reuse_inputs(manifest, fish, source_root, aligned_override=override)
            # Reuse is a separate recorded action, never presented as recompute.
            manifest["reused_from"] = {"root": str(frozen), "writer_generated_at": manifest["generated_at"], "scientific_commit": policy["scientific_commit"]}
            log.parent.mkdir(parents=True, exist_ok=True); log.write_text("Reused verified frozen writer products; no scientific recomputation.\n")
        else:
            if mode == "fresh" and output_dir.exists():
                archived = spec.state_root / "history" / ("graph-" + stage.name + "-" + datetime.now().strftime("%Y%m%dT%H%M%S%f"))
                archived.parent.mkdir(parents=True, exist_ok=True)
                output_dir.replace(archived)
            command = list(stage.command)
            command[command.index("--pipeline-root") + 1] = str(graph)
            command = [item for item in command if item != "--write-manifest"]
            if writer == "prepare-in-vivo-anatomy-stack" and "--output-path" in command:
                index = command.index("--output-path") + 1
                command[index] = str(output_dir / Path(command[index]).name)
            actual_command = command
            if "--accepted-midline-path" in command:
                # Read the previously committed native accepted artifact.
                pass
            env = os.environ.copy();env.update(spec.environment);env["PYTHONPATH"] = str(spec.repo_root / "src")
            code = _run_command(command, cwd=spec.repo_root, env=env, log_path=log)
            if code: raise RuntimeError(f"Writer {writer} failed; see {log}")
            text = log.read_text(); decoder = json.JSONDecoder();manifest = None
            for index, character in enumerate(text):
                if character == "{":
                    try:
                        candidate, _ = decoder.raw_decode(text[index:])
                        if isinstance(candidate, dict) and candidate.get("stage_name") == writer:manifest = candidate
                    except ValueError:pass
            if manifest is None: raise RuntimeError(f"Writer JSON manifest missing: {log}")
        if manifest.get("fish_id") != spec.fish_id or manifest.get("status") not in {"pass", "warn"} or manifest.get("errors") or manifest.get("dry_run") is not False or any(c.get("status") == "fail" for c in manifest.get("checks", ())):
            raise RuntimeError(f"Writer/reuse manifest validation failed: {writer}")
        # Bind paths by individual product prefixes, preserving explicit history.
        roots = [str(graph)] + ([source_root, str(frozen)] if frozen is not None else [])
        _relocate_tree(output_dir, roots, fish)
        if writer == "materialize-anatomy-midline" and frozen is not None:
            _relocate_accepted_midline(output_dir / "anatomy_midline.json", frozen, roots, fish)
        if writer == "make-qa-report":
            from .pipeline import _write_qa_report_pdf
            _write_qa_report_pdf(json.loads((output_dir / "qa_report_summary.json").read_text()), output_dir / "qa_report.pdf")
        manifest = json.loads(_rebind(json.dumps(manifest), roots, fish))
        manifest["source_writer_inputs"] = manifest.get("inputs", [])
        parameters = manifest.setdefault("parameters", {})
        parameters.pop("log_lines", None)
        parameters["pipeline_root"] = str(fish); parameters["native_layout"] = "existing-fish-tree-v1"
        for key, value in stage.parameter_pins.items():
            observed = parameters.get(key)
            if imported and key == "anatomy_label_z_mode" and observed == "auto" and value == "reverse":
                qc_refs = json.loads((frozen / writer / "plane_refs_summary_qc.json").read_text())
                if not all(row.get("anat_label_z_mode") == "reverse" for row in qc_refs): raise RuntimeError("Frozen automatic label-page choice is not uniformly reversed")
                parameters["anatomy_label_z_mode"] = value
            elif observed != value:
                raise RuntimeError(f"Scientific parameter mismatch {writer}: {key}: {observed} != {value}")
        products = {p: native_product_path(fish, writer, p.relative_to(output_dir).as_posix()) for p in output_dir.rglob("*") if p.is_file() and not p.name.startswith("._")}
        if writer == "materialize-anatomy-midline" and proposal is not None:
            products.update({p: native_product_path(fish, proposal.name, p.relative_to(proposal).as_posix()) for p in proposal.rglob("*") if p.is_file() and not p.name.startswith("._")})
        validate_native_inputs(stage.inputs, products.values())
        journal = spec.state_root / "history" / (stage.name + "-" + datetime.now().strftime("%Y%m%dT%H%M%S%f"))
        promote_files(fish, products, journal, manifest_path=Path(stage.manifest))
        try:
            from .pipeline import describe_manifest_path
            manifest["inputs"] = [asdict(describe_manifest_path(path, required=True)) for path in stage.inputs] + [asdict(describe_manifest_path(path, required=False)) for path in stage.optional_inputs]
            # Native manifests own exact files rather than synthetic stage roots.
            from .pipeline import describe_manifest_path
            manifest["outputs"] = [asdict(describe_manifest_path(destination, label=destination.name)) for destination in products.values()]
            manifest["generated_at"] = _utc_now()
            manifest["parameters"]["output_root"] = str(fish)
            if any(not item["exists"] and item.get("required") for item in manifest["inputs"] + manifest["outputs"]):
                rollback_promotion(journal)
                raise RuntimeError(f"Required native manifest path missing: {writer}")
            env = os.environ.copy();env.update(spec.environment);env["PYTHONPATH"] = str(spec.repo_root / "src")
            for index, validator in enumerate(stage.validators):
                if _run_command(validator, cwd=spec.repo_root, env=env, log_path=spec.state_root / "logs" / f"{stage.name}.validator-{index}.log"):
                    rollback_promotion(journal)
                    raise RuntimeError(f"Native stage validator failed: {stage.name}")
            if writer == "materialize-anatomy-midline" and frozen is not None:
                proposal_manifest = fish / "03_analysis/functional/pipeline_manifests/make-anatomy-midline-proposal_manifest.json"
                _write_json_atomic(journal / "previous_proposal_manifest.json", {"path": str(proposal_manifest), "payload": json.loads(proposal_manifest.read_text()) if proposal_manifest.exists() else None})
                proposal_record = _native_proposal_manifest(Path(policy["frozen_manifests"]) / proposal_manifest.name, frozen, roots, fish)
                _write_json_atomic(proposal_manifest, proposal_record)
            _write_json_atomic(Path(stage.manifest), manifest)
            state = {"schema_version": 1, "fish_id": spec.fish_id, "stage": stage.name, "spec_path": str(spec.path), "spec_sha256": _spec_hash(spec), "command": list(stage.command), "git_commit": environment["git"]["actual_commit"], "scientific_code_commit": spec.git_commit, "interpreter": environment["interpreter"]["actual_executable"], "started_at": started, "finished_at": _utc_now(), "inputs": _describe_paths(stage.inputs + stage.optional_inputs), "outputs": _describe_paths(stage.outputs), "dependencies": _dependency_records(spec, stage), "exit_status": 0, "log": str(log), "action": "reuse_verified_frozen" if imported else "recompute_transaction", "validation": {"status": "pass", "checks": [{"kind": "writer_manifest_and_verified_promotion", "status": "pass"}]}}
            state["actual_command"] = actual_command;state["execution_root"] = str(graph)
            state["promoted_products"] = _describe_paths(tuple(str(p) for p in products.values()))
            _write_json_atomic(_state_path(spec, stage), state)
            _write_json_atomic(journal / "stage_commit.json", {"stage": stage.name, "manifest": stage.manifest})
        except Exception:
            rollback_promotion(journal)
            backup = journal / "previous_manifest.json"
            if backup.exists() and json.loads(backup.read_text())["payload"] is not None: _write_json_atomic(Path(stage.manifest), json.loads(backup.read_text())["payload"])
            raise
        results.append({"stage": stage.name, "action": state["action"], "status": "pass"})
        _write_json_atomic(spec.state_root / "current_stage.json", {"stage": stage.name, "state": "committed"})
    summary = {"fish_id": spec.fish_id, "status": "pass" if len(results) == len(spec.stages) and all(r["status"] == "pass" for r in results) else "fail", "results": results, "spec_sha256": _spec_hash(spec)}
    if spec.raw.get("review_gate"):
        summary["scientific_release"] = spec.raw["review_gate"]
    _write_json_atomic(spec.state_root / "run_summary.json", summary)
    return summary
