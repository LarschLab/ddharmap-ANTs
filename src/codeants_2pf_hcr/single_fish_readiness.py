"""Read-only readiness evidence for fish that still have scientific review gates.

Never warps, registers, promotes canonical products, or accepts masks/identity.
"""
from __future__ import annotations
from codeants_2pf_hcr.paths import visible_glob, is_visible_path

import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import uuid

from .single_fish_run import _file_hash, _utc_now, _write_json_atomic


def resolve_archive_path(value: str, *, archive_root: Path, fish_dir: Path) -> Path:
    """Explicit same-fish relocation only; never substitute another fish."""
    original = Path(value)
    relative = original.relative_to(archive_root / fish_dir.name)
    if ".." in relative.parts:
        raise ValueError("Archive path traversal")
    return fish_dir / relative


def expected_transform_paths(fish: str, round_name: str, route: str) -> list[str]:
    """Forward chain in ANTs argument order: outer space first, warp then affine."""
    legs = [("04_exvivo-2p", f"{fish}_exvivo_GCaMP_to_2p")]
    if route == "rbest_to_exvivo_then_exvivo_to_2p" and round_name == "rbest":
        legs.append(("03_rbest-exvivo", f"{fish}_rbest_GCaMP_to_exvivo"))
    elif route == "direct_rn_to_exvivo_then_exvivo_to_2p" and re.fullmatch(r"r[0-9]+", round_name):
        legs.append(("05_rn-exvivo", f"{fish}_{round_name}_GCaMP_to_exvivo"))
    elif route == "rn_via_rbest_to_exvivo_then_exvivo_to_2p" and re.fullmatch(r"r[0-9]+", round_name):
        legs += [("03_rbest-exvivo", f"{fish}_rbest_GCaMP_to_exvivo"),
                 ("02_rn-rbest", f"{fish}_{round_name}_GCaMP_to_rbest")]
    else:
        raise ValueError(f"Invalid round/route: {round_name}/{route}")
    return [f"02_reg/{folder}/transMatrices/{stem}_{suffix}"
            for folder, stem in legs for suffix in ("1Warp.nii.gz", "0GenericAffine.mat")]


def audit_readiness(spec_path: Path) -> dict:
    """Hash and validate existing inputs; return review-required, never run writers."""
    import numpy as np
    import SimpleITK as sitk
    import tifffile

    spec = json.loads(spec_path.read_text())
    fish = spec["fish_id"]
    if not re.fullmatch(r"[A-Z][0-9]{3}_f[0-9]{2}", fish):
        raise ValueError("Invalid fish ID")
    fish_dir = Path(spec["fish_dir"])
    if not fish_dir.is_absolute() or fish_dir.name != fish:
        raise ValueError("Invalid fish directory")
    out = fish_dir / "03_analysis/functional/run_state/remaining_readiness" / uuid.uuid4().hex
    out.mkdir(parents=True)
    report = {"fish_id": fish, "started_at": _utc_now(), "status": "running",
              "report": str(out / "report.json"), "spec_sha256": _file_hash(spec_path),
              "scientific_writers_run": 0, "canonical_outputs_replaced": 0,
              "checks": [], "routes": [], "functional_planes": [],
              "blockers": list(spec["review_gates"]), "inventory": {}}
    _write_json_atomic(out / "report.json", report)

    def check(name, ok, detail=None):
        report["checks"].append({"name": name, "ok": bool(ok), "detail": detail})

    def record(path):
        path = Path(path)
        if not path.is_file():
            return None
        if str(path) not in report["inventory"]:
            report["inventory"][str(path)] = _file_hash(path)
        return report["inventory"][str(path)]

    try:
        repo = Path(spec["repo_root"])
        head = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
        dirty = subprocess.check_output(["git", "-C", str(repo), "status", "--porcelain"], text=True).strip()
        check("clean_pinned_code", head == spec["git_commit"] and not dirty, head)
        check("pinned_interpreter", Path(sys.executable).resolve() == Path(spec["python"]).resolve())
        for path, digest in spec["source_pins"].items():
            check(f"source_pin:{path}", record(path) == digest)
        proof = json.loads(Path(spec["baseline_report"]).read_text())
        check("baseline_unattended_proof", proof["status"] == "pass"
              and all(proof["steps"][key]["ok"] for key in
                      ("resume", "verify", "audit-artifacts", "no-op-resume")))
        metadata_path = fish_dir / f"02_reg/00_preprocessing/2p_functional/01_individualPlanes/{fish}_preprocessing_metadata.json"
        metadata = json.loads(metadata_path.read_text())
        check("preprocessing_fish", metadata["fish_id"] == fish)
        sessions = {s["session_label"]: s["output_planes"] for s in metadata["sessions"]}
        eligible = [plane for s in spec["eligible_sessions"] for plane in sessions[s]]
        check("session_plane_mapping", eligible == spec["eligible_planes"], sessions)
        drift_path = fish_dir / "03_analysis/functional/ncc/z_drift/z_drift_manifest.json"
        drift = json.loads(drift_path.read_text())
        check("drift_fish", drift["fish_id"] == fish)
        for session in spec["eligible_sessions"]:
            rows = [s for s in drift["session_summary"] if s["session"] == session]
            check(f"accepted_stable_session:{session}", len(rows) == 1
                  and rows[0]["evidence_tier"] == "stable_no_drift_evidence")
        for selection in spec["functional_sources"]:
            refs_path = Path(selection["plane_refs"])
            refs = json.loads(refs_path.read_text())
            for plane in selection["planes"]:
                rows = [r for r in refs if r["index"] == plane]
                check(f"functional_plane:{plane}", len(rows) == 1)
                if len(rows) != 1:
                    continue
                row = rows[0]
                transforms = row.get("ants_transformlist", [])
                present = [record(p) is not None for p in transforms]
                check(f"functional_transform:{plane}", row["tform_src"] == selection["method"]
                      and (all(present) and bool(present) if selection["method"] == "ants_rigid_affine" else True))
                for key in ("reference_raw_path", "reference_norm_path"):
                    check(f"{key}:{plane}", record(row[key]) is not None)
                report["functional_planes"].append(
                    {"plane": plane, "source": str(refs_path), "best_z": row["best_z"],
                     "method": row["tform_src"], "transforms": transforms,
                     "anatomy_label_z_mode_recorded": row.get("anat_label_z_mode")})
        check("eligible_plane_coverage", sorted(r["plane"] for r in report["functional_planes"]) == sorted(eligible))
        archive = Path(spec["archive_root"])
        aligned = fish_dir / "03_analysis/confocal/aligned"
        actual = {p.name for p in visible_glob(aligned, "*warp_meta.json") if is_visible_path(p)}
        check("exact_retained_hcr_inventory", actual == set(spec["hcr_routes"]), sorted(actual))
        for name, expected in spec["hcr_routes"].items():
            path = aligned / name
            sidecar = json.loads(path.read_text())
            transforms = [resolve_archive_path(p, archive_root=archive, fish_dir=fish_dir)
                          for p in sidecar["transformlist"]]
            relative = [str(p.relative_to(fish_dir)) for p in transforms]
            check(f"transform_order:{name}", sidecar["route"] == expected["route"]
                  and relative == expected_transform_paths(fish, expected["round"], expected["route"])
                  and sidecar["whichtoinvert"] == [False] * len(transforms)
                  and sidecar["interpolator"] == "nearestNeighbor")
            for transform in transforms:
                check(f"transform_available:{transform.name}", record(transform) is not None)
            grid = resolve_archive_path(sidecar["fixed_grid"], archive_root=archive, fish_dir=fish_dir)
            output = resolve_archive_path(sidecar["outputs"]["primary"], archive_root=archive, fish_dir=fish_dir)
            source = resolve_archive_path(sidecar["input"], archive_root=archive, fish_dir=fish_dir)
            record(grid); record(output)
            labels = tifffile.imread(output)
            image = sitk.ReadImage(str(grid))
            counts = len(np.unique(labels)) - int(0 in labels)
            check(f"grid_dtype_counts:{name}", labels.dtype == np.uint16
                  and list(labels.shape) == list(reversed(image.GetSize()))
                  and list(labels.shape) == sidecar["checks"]["output_shape_zyx"]
                  and counts == sidecar["checks"]["output_nonzero_label_count"])
            check(f"grid_geometry:{name}",
                  np.allclose(image.GetSpacing(), sidecar["checks"]["fixed_spacing_xyz"])
                  and np.allclose(image.GetOrigin(), sidecar["checks"]["fixed_origin_xyz"])
                  and np.allclose(image.GetDirection(), np.array(sidecar["checks"]["fixed_direction"]).ravel()))
            source_hash = record(source)
            if source_hash is None:
                report["blockers"].append(f"Native mask unavailable in mirror: {source}; native label-ID subset not revalidated")
            else:
                native_ids = np.unique(tifffile.imread(source))
                check(f"native_label_subset:{name}", np.isin(np.unique(labels), native_ids).all())
            report["routes"].append({"mask": name, "round": expected["round"],
                "route": expected["route"], "transformlist_ants_order": [str(p) for p in transforms],
                "whichtoinvert": sidecar["whichtoinvert"], "fixed_grid": str(grid),
                "moving_intensity_metadata_donor": sidecar["moving_intensity_metadata_donor"],
                "native_mask": str(source), "native_mask_available": source_hash is not None,
                "output": str(output), "label_count": counts,
                "scientific_acceptance": expected["acceptance"],
                "provenance_limit": "Original sidecars have no transform hashes; current mirror hashes are a new baseline, not proof of August transform-byte identity."})
        for source in spec["qc_figures"]:
            source = Path(source)
            check(f"qc_present:{source.name}", record(source) is not None)
            if source.is_file():
                dest = out / "qc" / source.name
                dest.parent.mkdir(exist_ok=True)
                shutil.copyfile(source, dest)
                check(f"qc_copy:{source.name}", _file_hash(dest) == record(source))
        report["status"] = "review_required" if all(c["ok"] for c in report["checks"]) else "fail"
    except Exception as exc:
        report.update(status="fail", error=f"{type(exc).__name__}: {exc}")
    report["finished_at"] = _utc_now()
    _write_json_atomic(out / "report.json", report)
    return report


__all__ = ["audit_readiness", "expected_transform_paths", "resolve_archive_path"]
