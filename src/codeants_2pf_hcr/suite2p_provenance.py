"""Evidence-bound reconciliation of moved legacy Suite2p inputs; never edits NPY/TIFF data."""
from __future__ import annotations
import hashlib
import json
import shutil
from pathlib import Path
import numpy as np
import tifffile
from .spatial_contract import LEGACY_ACQUISITION_XY_FRAME, load_spatial_manifest, spatial_manifest_path
from .single_fish_run import _write_json_atomic, _utc_now


def _digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _stamp(path):
    stat = Path(path).stat()
    return {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns}


def _ops_inputs(ops):
    result = set()
    for key in ("tiff_list", "filelist"):
        values = ops.get(key, [])
        if isinstance(values, (str, Path)): values = [values]
        for value in values:
            path = Path(str(value))
            if not path.is_absolute():
                bases = ops.get("data_path", [])
                if isinstance(bases, (str, Path)): bases = [bases]
                if bases: path = Path(str(bases[0])) / path
            result.add(path.resolve())
    return result


def resolve_reconciled_suite2p_input(path, *, ops_path, manifest):
    """Accept only an explicit alias tied to unchanged ops and input fingerprints."""
    path = Path(path).resolve()
    reconciliation = manifest.get("suite2p_provenance_reconciliation", {})
    if reconciliation:
        if reconciliation.get("status") != "pass" or _digest(reconciliation["metadata_path"]) != reconciliation["metadata_sha256"]:
            raise ValueError("Reconciled preprocessing metadata changed or audit is not passing")
    records = reconciliation.get("planes", [])
    for record in records:
        if str(Path(ops_path).resolve()) != record["ops_path"]: continue
        if _digest(ops_path) != record["ops_sha256"]:
            raise ValueError("Reconciled Suite2p ops hash changed")
        for name, stamp in record["input_fingerprints"].items():
            if not Path(name).is_file() or _stamp(name) != stamp:
                raise ValueError(f"Reconciled input changed: {name}")
        aliases = record["ops_input_aliases"]
        if str(path) not in aliases:
            raise ValueError(f"Input absent from reviewed Suite2p reconciliation: {path}")
        return Path(aliases[str(path)]).resolve()
    return path


def check_suite2p_input_provenance(*, fish_dir, plane_indices):
    """Read-only recipe preflight: provenance, declared frames and trace dimensions."""
    from .suite2p import _find_suite2p_file, _load_ops_npy
    fish = Path(fish_dir).resolve()
    manifest = load_spatial_manifest(fish, required=True)
    declared = {Path(r["output_path"]).resolve(): r.get("output_xy_frame",
                manifest["coordinate_frames"]["canonical_functional_xy"])
                for r in manifest["functional_planes"]}
    by_plane = {int(r["plane_index"]): Path(r["output_path"]).resolve()
                for r in manifest["functional_planes"] if "plane_index" in r}
    checked = []
    for plane in plane_indices:
        directory = fish / "03_analysis/functional/suite2P" / f"plane{plane}"
        op = _find_suite2p_file(directory, "ops")
        if op is None: raise ValueError(f"Missing ops for plane {plane}")
        ops = _load_ops_npy(op)
        sources = _ops_inputs(ops)
        targets = {resolve_reconciled_suite2p_input(p, ops_path=op, manifest=manifest) for p in sources}
        if not targets or not targets.issubset(declared):
            raise ValueError(f"Undeclared Suite2p input for plane {plane}: {sources}")
        if plane not in by_plane or targets != {by_plane[plane]}:
            raise ValueError(f"Suite2p input points at a different plane: {plane}")
        for path in targets:
            if not path.is_file(): raise ValueError(f"Missing Suite2p input: {path}")
        frames = {declared[p] for p in targets}
        if len(frames) != 1 or frames - {"two_photon_acquisition_xy", "codeants_2p_canonical_xy_v1"}:
            raise ValueError(f"Inconsistent input frames for plane {plane}")
        f = np.load(_find_suite2p_file(directory, "F"), mmap_mode="r")
        if f.ndim != 2 or f.shape[1] != int(ops["nframes"]):
            raise ValueError(f"Trace/ops frame count disagreement for plane {plane}")
        checked.append({"plane": plane, "input_xy_frame": next(iter(frames))})
    return {"status": "pass", "planes": checked}


def reconcile_legacy_functional_provenance(*, fish_dir, original_data_root, raw_path_aliases=None):
    """Audit every legacy plane against raw blocks, then archive/correct only JSON.

    Supports the recorded 5-plane/3-frame resonant recipe with first scan removed.
    Samples first/middle/last volumes in each raw block; not full historical byte parity.
    Motion orientation must agree with the unchanged Suite2p mean image.
    """
    from .suite2p import _find_suite2p_file, _load_ops_npy
    fish = Path(fish_dir).resolve()
    path = spatial_manifest_path(fish)
    manifest = load_spatial_manifest(fish, required=True)
    if manifest.get("suite2p_provenance_reconciliation"):
        raise FileExistsError("Reconciliation already exists; validate, do not overwrite")
    metadata_path = fish / "02_reg/00_preprocessing/2p_functional/01_individualPlanes" / f"{fish.name}_preprocessing_metadata.json"
    metadata = json.loads(metadata_path.read_text())
    if metadata.get("n_planes") != 5 or metadata.get("n_frames_per_plane") != 3 or not metadata.get("remove_first_frame") or metadata.get("volume_flyback_frames") != 0:
        raise ValueError("Unsupported legacy averaging recipe")
    original = Path(original_data_root)
    aliases = raw_path_aliases or {}
    def relocate(source):
        candidate = Path(source)
        if candidate.is_relative_to(original):
            candidate = fish.parent / candidate.relative_to(original)
        candidate = Path(aliases.get(str(candidate), str(candidate))).resolve()
        if not candidate.is_relative_to(fish) or not candidate.is_file():
            raise ValueError(f"Unresolved or cross-fish input: {source}")
        return candidate
    records = {int(r["plane_index"]): r for r in manifest["functional_planes"]}
    evidence = []
    for session in metadata["sessions"]:
        raws = [relocate(p) for p in session["selected_tiffs"]]
        for local, plane in enumerate(session["output_planes"]):
            record = records[int(plane)]
            target = Path(record["output_path"]).resolve()
            directory = fish / "03_analysis/functional/suite2P" / f"plane{plane}"
            op = _find_suite2p_file(directory, "ops")
            ops = _load_ops_npy(op)
            traces = np.load(_find_suite2p_file(directory, "F"), mmap_mode="r")
            if traces.ndim != 2 or traces.shape[1] != int(ops["nframes"]):
                raise ValueError(f"Trace/ops frame count disagreement before repair: {plane}")
            mapping = {str(p): str(relocate(p)) for p in _ops_inputs(ops)}
            if set(mapping.values()) != {str(target)}:
                raise ValueError(f"Wrong plane or ambiguous Suite2p input: {plane}")
            samples = []
            offset = 0
            with tifffile.TiffFile(target) as out:
                if len(out.pages) != int(ops["nframes"]) or out.pages[0].shape != (int(ops["Ly"]), int(ops["Lx"])):
                    raise ValueError(f"Input dimensions disagree with Suite2p: {plane}")
                for raw_path in raws:
                    with tifffile.TiffFile(raw_path) as raw:
                        count, remainder = divmod(len(raw.pages), 15)
                        if remainder or count < 2: raise ValueError("Unsupported raw block layout")
                        for volume in sorted({0, count // 2, count - 1}):
                            base = volume * 15 + local * 3
                            expected = (raw.pages[base+1].asarray().astype(float) + raw.pages[base+2].asarray().astype(float))/2 + metadata["negative_offset_applied"]
                            actual = out.pages[offset+volume].asarray().astype(float)
                            error = float(np.max(np.abs(actual-expected)))
                            if error > 0.5: raise ValueError(f"Acquisition-frame evidence failed: plane {plane}, volume {volume}: {error}")
                            samples.append({"raw_path": str(raw_path), "volume": volume, "output_frame": offset+volume, "max_rounding_error": error,
                                            "output_frame_sha256": hashlib.sha256(actual.tobytes()).hexdigest()})
                        offset += count
                if offset != len(out.pages): raise ValueError("Raw blocks/retained frame count disagree")
            motion = fish / "02_reg/00_preprocessing/2p_functional/02_motionCorrected" / f"{fish.name}_plane{plane}_mcorrected.tif"
            with tifffile.TiffFile(motion) as movie:
                sample_frames = sorted(set(np.linspace(0, len(movie.pages)-1, 32, dtype=int).tolist()))
                image = np.mean([movie.pages[i].asarray().astype(float) for i in sample_frames], axis=0)
            mean = np.asarray(ops["meanImg"], dtype=float)
            if image.shape != mean.shape: raise ValueError("Motion image/ops dimensions disagree")
            correlations = [float(np.corrcoef(image.ravel(), v.ravel())[0,1]) for v in (mean,mean[:,::-1],mean[::-1,:],mean[::-1,::-1])]
            if not np.isfinite(correlations).all() or correlations[0] < 0.5 or correlations[0] - max(correlations[1:]) < 0.2:
                raise ValueError(f"Motion orientation ambiguous: plane {plane}: {correlations}")
            evidence.append({"plane": int(plane), "ops_path": str(op.resolve()), "ops_sha256": _digest(op),
                             "ops_input_aliases": mapping, "input_fingerprints": {str(p): _stamp(p) for p in [target,motion,*raws]},
                             "raw_samples": samples, "motion_sample_frames": sample_frames,
                             "motion_direct_and_flipped_correlations": correlations})
    backup = path.with_name(path.name + ".before_suite2p_reconciliation")
    if backup.exists(): raise FileExistsError(backup)
    shutil.copy2(path, backup)
    if _digest(path) != _digest(backup): raise RuntimeError("Manifest backup verification failed")
    for group in ("functional_planes", "motion_corrected_movies"):
        for record in manifest[group]:
            record["previous_output_xy_frame"] = record.get("output_xy_frame")
            record["intended_canonical_xy_transform"] = record.get("xy_transform")
            record["output_xy_frame"] = LEGACY_ACQUISITION_XY_FRAME
            record["xy_transform"] = "none"
    manifest["suite2p_provenance_reconciliation"] = {"status": "pass", "created_at": _utc_now(),
        "evidence_scope": "Sampled raw-frame averaging and unchanged ops/motion orientation; not full historical byte equality",
        "original_manifest_path": str(backup), "original_manifest_sha256": _digest(backup),
        "metadata_path": str(metadata_path), "metadata_sha256": _digest(metadata_path), "planes": evidence}
    _write_json_atomic(path, manifest)
    return check_suite2p_input_provenance(fish_dir=fish, plane_indices=tuple(sorted(records)))


__all__ = ["check_suite2p_input_provenance", "reconcile_legacy_functional_provenance",
           "resolve_reconciled_suite2p_input"]
