"""Read-only native execution-layout and ROI trace-identity preflight."""
from pathlib import Path
import json
import re


# Implicit staged-root dependencies, in addition to explicit command inputs.
GRAPH_INPUTS = {
    "transform-functional-rois-to-anatomy": ("register-functional-to-anatomy",),
    "make-functional-registration-qc": ("register-functional-to-anatomy", "transform-functional-rois-to-anatomy"),
    "match-roi-to-anatomy": ("register-functional-to-anatomy",),
    "export-canonical-tables": ("score-activity-bpi", "assign-hcr-identity"),
    "make-anatomy-midline-proposal": ("register-functional-to-anatomy", "transform-functional-rois-to-anatomy", "make-functional-registration-qc"),
}


def check_geometry_trace_identity(path, *, fish_dir, eligible_planes):
    import numpy as np
    import pandas as pd
    geometry = pd.read_csv(path)
    expected_root = Path(fish_dir) / "03_analysis/functional/suite2P"
    errors = []
    for idx, group in geometry.groupby("plane_idx"):
        idx = int(idx)
        if idx not in eligible_planes:
            errors.append(f"geometry plane {idx} is outside eligible acquisition planes")
        expected = (expected_root / f"plane{idx}").resolve()
        if "func_source" not in group or any(Path(str(p)).resolve() != expected for p in group.func_source):
            errors.append(f"geometry plane {idx} has inconsistent Suite2p source")
            continue
        files = list(expected.glob("*F.npy"))
        if len(files) != 1:
            errors.append(f"plane {idx} requires exactly one F trace array")
            continue
        n = np.load(files[0], mmap_mode="r").shape[0]
        if not group.func_label.between(1, n).all():
            errors.append(f"plane {idx} has ROI labels outside trace rows")
    if geometry.duplicated(["plane_idx", "func_label"]).any():
        errors.append("duplicate geometry ROI keys")
    return errors


def preflight_native_run(spec):
    from .single_fish_run import describe_path
    errors = []
    retained = spec.raw.get("native_production", {}).get("retained_graph", {})
    eligible = spec.raw.get("accepted_choices", {}).get("eligible_planes")
    if eligible is not None:
        from .suite2p_provenance import check_suite2p_input_provenance
        try:
            check_suite2p_input_provenance(fish_dir=spec.data_root / spec.fish_id,
                                         plane_indices=tuple(eligible))
        except Exception as exc:
            errors.append(f"selected Suite2p input provenance: {exc}")
    for owner, record in retained.items():
        if not re.fullmatch(r"[a-z][a-z0-9-]+", owner):
            errors.append(f"invalid retained graph owner: {owner}")
            continue
        source = Path(record["path"])
        if describe_path(source).get("sha256") != record["sha256"]:
            errors.append(f"retained graph changed or missing: {owner}")
        if owner == "register-functional-to-anatomy" and not (source / "plane_refs_summary.json").is_file():
            errors.append("retained registration has no plane_refs_summary.json")
        elif owner == "register-functional-to-anatomy" and eligible is not None:
            refs = json.loads((source / "plane_refs_summary.json").read_text())
            if {int(ref["index"]) for ref in refs} != set(eligible):
                errors.append("retained registration indices disagree with eligible acquisition planes")
    available = set(retained)
    writers = []
    for stage in spec.stages:
        if not stage.manifest:
            continue
        writer = stage.command[2]
        writers.append(writer)
        for dependency in GRAPH_INPUTS.get(writer, ()):
            if dependency not in available:
                errors.append(f"{writer}: missing execution-graph producer/input {dependency}")
        available.add(writer)
    # A geometry producer scheduled before scoring replaces old outputs. For
    # score-only continuations, validate the actual table that will be consumed.
    for stage in spec.stages:
        if not stage.manifest or stage.command[2] != "score-activity-bpi":
            continue
        earlier = writers[:writers.index("score-activity-bpi")]
        if "match-roi-to-anatomy" not in earlier:
            flag = "--geometry-input-path"
            if flag not in stage.command:
                errors.append("score-only continuation requires an explicit geometry input")
            else:
                path = Path(stage.command[stage.command.index(flag) + 1])
                if not path.is_file():
                    errors.append(f"geometry input missing: {path}")
                else:
                    errors.extend(check_geometry_trace_identity(path, fish_dir=spec.data_root / spec.fish_id,
                        eligible_planes=set(spec.raw["accepted_choices"]["eligible_planes"])))
    return {"ok": not errors, "errors": errors, "graph_root": str(spec.state_root / "transaction_graph"),
            "retained_graph_stages": sorted(retained), "writers": writers}


def materialize_retained_graph(spec):
    """Copy only hash-pinned graph inputs, after read-only preflight passes."""
    import shutil
    from .single_fish_run import describe_path
    for owner, record in spec.raw.get("native_production", {}).get("retained_graph", {}).items():
        target = spec.state_root / "transaction_graph" / owner
        if not target.exists():
            shutil.copytree(record["path"], target)
        if describe_path(target).get("sha256") != record["sha256"]:
            raise RuntimeError(f"Retained graph copy verification failed: {owner}")
