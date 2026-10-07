"""Build the common native writer DAG through the unreviewed-midline gate.

This is a recipe, not scientific business logic. Existing stage writers remain
authoritative. No frozen fish-specific bundle or accepted midline is required.
"""
from dataclasses import asdict
import importlib.metadata
import json
from pathlib import Path
import shutil
import subprocess
import sys

from .activity import ActivityConfig
from .context import resolve_func_polarity
from .single_fish_run import _file_hash, _write_json_atomic


WRITERS = (
    "prepare-in-vivo-anatomy-stack", "prepare-functional-reference-stacks",
    "register-functional-to-anatomy", "transform-functional-rois-to-anatomy",
    "make-functional-registration-qc", "register-hcr-to-anatomy",
    "match-roi-to-anatomy", "score-activity-bpi", "assign-hcr-identity",
    "export-canonical-tables", "make-anatomy-midline-proposal", "make-qa-report",
)

# ANTs treats zero as default/system-time seeding, not a fixed seed.
FRESH_ANTS_SEED = 123


def build_fresh_run_spec(*, fish_id: str, data_root: Path, repo_root: Path,
                         run_id: str, spec_path: Path, plane_indices: tuple[int, ...],
                         ncc_cache_source: Path | None = None,
                         geometry_control_supersede_reason: str | None = None) -> dict:
    """Pin shared naming, selected planes and approximate aligned HCR inputs.

    Refuse missing inputs and existing specs/bundles. Best Z and scale are
    searched from the current references unless an NCC initializer cache is
    explicitly supplied; its bytes are then copied and pinned, and each entry
    must reproduce from the current reference. Every registration is rerun.
    The result deliberately stops before accepting any proposed midline.
    """
    import re
    if not re.fullmatch(r"[A-Z][0-9]{3}_f[0-9]{2}", fish_id):
        raise ValueError("Invalid fish ID")
    if not re.fullmatch(r"[A-Za-z0-9_-]+", run_id):
        raise ValueError("Invalid run ID")
    if not plane_indices or tuple(sorted(set(plane_indices))) != plane_indices:
        raise ValueError("Planes must be a nonempty sorted unique tuple")
    if fish_id == "L765_f05" and plane_indices != tuple(range(5, 10)):
        raise ValueError("L765_f05 is restricted to R2, global planes 5–9")
    data_root, repo_root = data_root.resolve(), repo_root.resolve()
    fish = data_root / fish_id
    functional = fish / "03_analysis/functional"
    anatomy = fish / "02_reg/00_preprocessing/2p_anatomy" / f"{fish_id}_anatomy_2P_GCaMP.nrrd"
    labels = fish / "03_analysis/structural/cp_masks" / f"{fish_id}_anatomy_00001_uint8_8bit_cp_masks.tif"
    metadata = fish / "02_reg/00_preprocessing/2p_functional/01_individualPlanes" / f"{fish_id}_preprocessing_metadata.json"
    matching = data_root / "matchingMetadata.csv"
    drift = functional / "ncc/z_drift/z_drift_manifest.json"
    motion = [fish / "02_reg/00_preprocessing/2p_functional/02_motionCorrected" / f"{fish_id}_plane{i}_mcorrected.tif" for i in plane_indices]
    suite = [functional / "suite2P" / f"plane{i}" / f"{fish_id}_plane{i}_{suffix}.npy" for i in plane_indices for suffix in ("F", "Fneu", "stat", "ops", "iscell", "spks")]
    raw_metadata = sorted(p for p in (fish / "01_raw/2p/metadata").glob("*.csv") if not p.name.startswith("._"))
    aligned = fish / "03_analysis/confocal/aligned"
    masks = sorted(p for p in aligned.glob("*_cp_masks_in_2p_labels_uint16.tif") if not p.name.startswith("._"))
    sidecars = [Path(str(p).replace("_labels_uint16.tif", "_warp_meta.json")) for p in masks]
    caches = [] if ncc_cache_source is None else [ncc_cache_source / name for name in ("ncc_bestz_by_plane.json", "ncc_scale_by_fish.json")]
    required = [anatomy, labels, metadata, matching, drift, *motion, *suite, *raw_metadata, *masks, *sidecars, *caches]
    missing = [str(p) for p in required if not p.is_file()]
    if missing or not masks or not raw_metadata:
        raise ValueError(f"Missing fresh-run inputs: {missing}; masks={len(masks)}, metadata={len(raw_metadata)}")
    polarity, source = resolve_func_polarity(fish_id, matching, fish_dir=fish)
    if polarity not in {"north", "south"}:
        raise ValueError("Functional orientation is unresolved")
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo_root, text=True).strip()
    from .suite2p_provenance import check_suite2p_input_provenance
    from .spatial_contract import spatial_manifest_path
    spatial_manifest = spatial_manifest_path(fish)
    check_suite2p_input_provenance(fish_dir=fish, plane_indices=plane_indices)
    required.append(spatial_manifest)
    if subprocess.check_output(["git", "status", "--porcelain"], cwd=repo_root, text=True).strip():
        raise ValueError("Recipe requires a clean pinned checkout")
    bundle = spec_path.parent / "input_bundles" / run_id
    if spec_path.exists() or bundle.exists():
        raise FileExistsError("Refusing to overwrite an existing run recipe/input bundle")
    if caches:
        bundle.mkdir(parents=True)
    for p in caches:
        shutil.copy2(p, bundle / p.name)
        if _file_hash(p) != _file_hash(bundle / p.name):
            raise RuntimeError("NCC initializer copy verification failed")
    python = str(Path(sys.executable).resolve())
    common = ["--fish-id", fish_id, "--local-root", str(data_root), "--pipeline-root", str(fish), "--strict", "--write-manifest", "--force-recompute"]
    activity = asdict(ActivityConfig())
    extra = {
        WRITERS[0]: ["--anatomy-stack-path", str(anatomy), "--output-path", str(anatomy)],
        WRITERS[1]: ["--run-23c"] + [v for p in motion for v in ("--functional-stack-path", str(p))],
        WRITERS[2]: ["--anatomy-stack-path", str(anatomy), *(["--reuse-ncc-cache-dir", str(bundle)] if caches else []), "--ants-deterministic-seed", str(FRESH_ANTS_SEED), "--no-ants-fixed-mask-required"] + [v for i in plane_indices for v in ("--reference-plane-index", str(i))],
        WRITERS[4]: ["--anatomy-label-z-mode", "auto"],
        WRITERS[5]: ["--source-root", str(aligned), "--hcr-warp-route", "exvivo_bridge"],
        WRITERS[6]: ["--anatomy-label-z-mode", "auto"],
        WRITERS[7]: ["--geometry-input-path", str(functional / "registration/functional_roi_anatomy_matches.csv")],
        WRITERS[8]: ["--activity-input-path", str(functional / "derived/score-activity-bpi/registration/functional_roi_activity_identity.csv"), "--roi-anatomy-root", str(functional / "registration"), "--hcr-anatomy-root", str(aligned)],
    }
    if geometry_control_supersede_reason:
        # A deliberate upstream correction supersedes geometry and every control derived from it.
        for writer in WRITERS[6:10]:
            extra[writer] = extra.get(writer, []) + ["--supersede-accepted-control-reason", geometry_control_supersede_reason]
    stages = []
    manifests = functional / "pipeline_manifests"
    # Exact promoted output inventory is added to each state by the runner.
    for i, writer in enumerate(WRITERS):
        name = f"{(i+1)*10:03}-{writer}"
        inputs = [anatomy, labels, matching, metadata, drift, spatial_manifest]
        if i == 1: inputs += motion + suite + raw_metadata
        if i in {3, 6, 7}: inputs += suite
        if i == 2: inputs += [bundle / p.name for p in caches]
        if i == 5: inputs += masks + sidecars
        if stages: inputs += [Path(stages[-1]["manifest"])]
        manifest = manifests / f"{writer}_manifest.json"
        pins = {}
        if i == 1: pins = {"polarity": polarity, "run_23c": True, "exclude_first_block": True}
        if i == 2: pins = {"ants_deterministic_seed": FRESH_ANTS_SEED}
        if i == 7: pins = {"activity_config": activity}
        stages.append({"name": name, "command": [python, "tools/single_fish_pipeline.py", writer, *common, *extra.get(writer, [])],
                       "inputs": list(map(str, inputs)), "outputs": [str(manifest)], "manifest": str(manifest),
                       "depends_on": [stages[-1]["name"]] if stages else [], "parameter_pins": pins})
    writer_manifests = [s["manifest"] for s in stages]
    notebook_dir = functional / "qc" / run_id
    for index in range(6):
        candidates = sorted((repo_root / "notebooks/qc").glob(f"{index:02}_*.ipynb"))
        if len(candidates) != 1:
            raise ValueError(f"Require one source QC notebook for index {index}")
        notebook = candidates[0]
        output = notebook_dir / notebook.name
        stages.append({"name": f"{160+index:03}-qc-notebook-{index:02}",
                       "command": [python, "tools/notebook_run.py", str(notebook), "--kernel", "cellmatching", "--fish-id", fish_id, "--local-root", str(data_root), "--pipeline-root", str(fish), "--output", str(output), "--capture-figures"],
                       "inputs": [str(notebook), *writer_manifests], "outputs": [str(output)], "depends_on": [stages[-1]["name"]]})
    params = {"activity_config": activity, "ants_deterministic_seed": FRESH_ANTS_SEED, "response_null_q_total_two_sided": activity["response_null_q"], "response_window_sec": activity["response_window_sec"]}
    payload = {"schema_version": 1, "fish_id": fish_id,
               "paths": {"repo_root": str(repo_root), "data_root": str(data_root), "pipeline_root": str(fish), "state_root": str(functional / "run_state" / run_id)},
               "git": {"commit": commit, "policy": "exact"},
               "interpreter": {"executable": python, "python_version": sys.version.split()[0], "packages": {n: importlib.metadata.version(n) for n in ("antspyx", "numpy", "pandas", "scipy", "matplotlib", "scikit-image", "nbclient")}},
               "environment": {"MPLBACKEND": "Agg", "PYTHONHASHSEED": "0", "ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS": "1", "OMP_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1"},
               "native_production": {"mode": "fresh", "reuse_stages": []},
               "accepted_choices": {"functional_polarity": polarity, "functional_polarity_source": source, "eligible_planes": list(plane_indices), "temporal_drift_basis": str(drift), "hcr_masks": "provisional approximate overview; manual identity merge pending", "midline_gate": "review_required"},
               "scientific_parameters": params, "provenance_pins": [{"name": p.name, "path": str(p), "sha256": _file_hash(p)} for p in [metadata, matching, drift, spatial_manifest, *raw_metadata, *masks, *sidecars, *[bundle / p.name for p in caches]]],
               "review_gate": {"status": "review_required", "reason": "Anatomy Z and registration QC plus proposed midlines require visual review; laterality is not accepted by this job."}, "stages": stages}
    _write_json_atomic(spec_path, payload)
    return payload
