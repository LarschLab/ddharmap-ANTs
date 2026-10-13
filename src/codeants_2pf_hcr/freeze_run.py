"""Verified, read-only snapshots of a staged single-fish pipeline run."""

from __future__ import annotations
from codeants_2pf_hcr.paths import visible_glob, is_visible_path

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
from importlib import metadata as importlib_metadata
import json
import os
from pathlib import Path
import platform
import plistlib
import shutil
import stat
import subprocess
import sys
import uuid
from typing import Any, Iterable


FREEZE_FORMAT_VERSION = 1


@dataclass(frozen=True)
class InventoryRecord:
    path: str
    kind: str
    size_bytes: int
    sha256: str
    mode: str
    link_target: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "kind": self.kind,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "mode": self.mode,
            "link_target": self.link_target,
        }


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def build_inventory(roots: Iterable[tuple[str, Path]]) -> list[InventoryRecord]:
    """Hash every file/symlink below labeled roots without following symlinks."""
    records: list[InventoryRecord] = []
    for label, root in roots:
        root = root.expanduser().resolve()
        if not root.is_dir():
            raise FileNotFoundError(f"Required freeze source directory is missing: {root}")
        # Literal freeze inventory preserves sidecars (docs/freeze-single-fish-run.md).
        for path in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
            relative = f"{label}/{path.relative_to(root).as_posix()}"
            mode = stat.S_IMODE(path.lstat().st_mode)
            if path.is_symlink():
                target = os.readlink(path)
                encoded = target.encode("utf-8")
                records.append(
                    InventoryRecord(
                        path=relative,
                        kind="symlink",
                        size_bytes=len(encoded),
                        sha256=hashlib.sha256(encoded).hexdigest(),
                        mode=f"{mode:04o}",
                        link_target=target,
                    )
                )
            elif path.is_file():
                records.append(
                    InventoryRecord(
                        path=relative,
                        kind="file",
                        size_bytes=path.stat().st_size,
                        sha256=_sha256_file(path),
                        mode=f"{mode:04o}",
                    )
                )
    return records


def _content_signature(records: list[InventoryRecord]) -> list[tuple[str, str, int, str, str | None]]:
    return [
        (record.path, record.kind, record.size_bytes, record.sha256, record.link_target)
        for record in records
    ]


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_inventory(path: Path, records: list[InventoryRecord]) -> None:
    lines = ["sha256\tsize_bytes\tkind\tmode\tpath\tlink_target"]
    for record in records:
        link_target = "" if record.link_target is None else record.link_target.replace("\t", "\\t")
        lines.append(
            f"{record.sha256}\t{record.size_bytes}\t{record.kind}\t{record.mode}\t"
            f"{record.path}\t{link_target}"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _run_git(repo_root: Path, *args: str, check: bool = True) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo_root), *args],
        check=check,
        capture_output=True,
        text=True,
    )
    return result.stdout


def build_git_record(repo_root: Path) -> dict[str, Any]:
    repo_root = repo_root.expanduser().resolve()
    status = _run_git(repo_root, "status", "--porcelain=v1", "--untracked-files=all").splitlines()
    worktree_diff = _run_git(repo_root, "diff", "--binary")
    staged_diff = _run_git(repo_root, "diff", "--cached", "--binary")
    return {
        "repo_root": str(repo_root),
        "commit": _run_git(repo_root, "rev-parse", "HEAD").strip(),
        "branch": _run_git(repo_root, "branch", "--show-current").strip(),
        "commit_description": _run_git(repo_root, "log", "-1", "--format=%h %ci %s").strip(),
        "dirty": bool(status),
        "status_porcelain": status,
        "worktree_diff_sha256": _sha256_text(worktree_diff),
        "worktree_diff_bytes": len(worktree_diff.encode("utf-8")),
        "staged_diff_sha256": _sha256_text(staged_diff),
        "staged_diff_bytes": len(staged_diff.encode("utf-8")),
    }


def build_runtime_record() -> dict[str, Any]:
    distributions = sorted(
        (
            {
                "name": distribution.metadata.get("Name", ""),
                "version": distribution.version,
            }
            for distribution in importlib_metadata.distributions()
        ),
        key=lambda item: (item["name"].lower(), item["version"]),
    )
    return {
        "executable": sys.executable,
        "python_version": sys.version,
        "python_implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "packages": distributions,
    }


def build_stage_records(manifest_dir: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Extract parameters/outputs and a compact status from current stage manifests."""
    details: list[dict[str, Any]] = []
    compact: list[dict[str, Any]] = []
    for manifest_path in sorted(visible_glob(manifest_dir, "*_manifest.json")):
        if (not is_visible_path(manifest_path)) or ".pre_" in manifest_path.name:
            continue
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            compact.append(
                {
                    "manifest": manifest_path.name,
                    "stage_name": None,
                    "status": "invalid",
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            continue
        checks = payload.get("checks") or []
        check_counts = {
            level: sum(1 for check in checks if check.get("status") == level)
            for level in ("pass", "warn", "fail")
        }
        outputs = payload.get("outputs") or []
        details.append(
            {
                "manifest": manifest_path.name,
                "manifest_sha256": _sha256_file(manifest_path),
                "stage_name": payload.get("stage_name"),
                "fish_id": payload.get("fish_id"),
                "generated_at": payload.get("generated_at"),
                "parameters": payload.get("parameters") or {},
                "outputs": outputs,
            }
        )
        compact.append(
            {
                "manifest": manifest_path.name,
                "stage_name": payload.get("stage_name"),
                "generated_at": payload.get("generated_at"),
                "status": payload.get("status"),
                "check_counts": check_counts,
                "error_count": len(payload.get("errors") or []),
                "warning_count": len(payload.get("warnings") or []),
                "output_count": len(outputs),
                "output_paths": [output.get("path") for output in outputs if isinstance(output, dict)],
            }
        )
    return details, compact


def _make_read_only(root: Path, *, include_root: bool = True) -> None:
    """Protect a verified tree on POSIX filesystems and macOS noowners volumes."""
    # Protect every archived entry, including preserved AppleDouble sidecars.
    paths = sorted(root.rglob("*"), key=lambda item: len(item.parts), reverse=True)
    if include_root:
        paths.append(root)
    for path in paths:
        if path.is_symlink():
            continue
        mode = stat.S_IMODE(path.stat().st_mode)
        path.chmod(mode & ~0o222)

    # exFAT mounted with `noowners` ignores chmod. The macOS user-immutable
    # flag is supported there and prevents writes/removal until explicitly
    # cleared. Apply it only where mode bits did not become read-only.
    if hasattr(os, "chflags") and hasattr(stat, "UF_IMMUTABLE"):
        for path in paths:
            if path.is_symlink() or not stat.S_IMODE(path.stat().st_mode) & 0o222:
                continue
            os.chflags(path, path.stat().st_flags | stat.UF_IMMUTABLE)


def _is_read_only(path: Path) -> bool:
    if path.is_symlink():
        return True
    info = path.stat()
    mode_locked = not stat.S_IMODE(info.st_mode) & 0o222
    immutable_locked = bool(
        hasattr(info, "st_flags")
        and hasattr(stat, "UF_IMMUTABLE")
        and info.st_flags & stat.UF_IMMUTABLE
    )
    return mode_locked or immutable_locked


def _unlock_for_cleanup(root: Path) -> None:
    # Cleanup must unlock preserved sidecars too.
    paths = [root, *root.rglob("*")]
    for path in paths:
        if path.is_symlink():
            continue
        info = path.stat()
        if hasattr(os, "chflags") and hasattr(stat, "UF_IMMUTABLE"):
            os.chflags(path, getattr(info, "st_flags", 0) & ~stat.UF_IMMUTABLE)
        path.chmod(stat.S_IMODE(info.st_mode) | stat.S_IWUSR)


def _writable_paths(root: Path, *, include_root: bool = True) -> list[str]:
    # Verify protection of the complete literal archive, including sidecars.
    paths = [*root.rglob("*")]
    if include_root:
        paths.insert(0, root)
    return [str(path) for path in paths if not _is_read_only(path)]


def _remove_staging_tree(root: Path) -> None:
    def ignore_disappearing_sidecar(function: Any, path: str, error: Any) -> None:
        # exFAT removes AppleDouble sidecars when the corresponding file is
        # removed; scandir can have already returned that sidecar's old name.
        if not isinstance(error[1], FileNotFoundError):
            raise error[1]
    shutil.rmtree(root, onerror=ignore_disappearing_sidecar)


def _attach_read_only(image: Path, mountpoint: Path, *, read_only: bool = True) -> None:
    # macOS refuses mountpoints nested on exFAT. Let Disk Arbitration mount the
    # HFS+ payload in /Volumes; its unique volume label fixes the browse path.
    result = subprocess.run(
        ["hdiutil", "attach", *( ["-readonly"] if read_only else [] ), "-nobrowse", "-plist", str(image)],
        check=True, capture_output=True,
    )
    mounted_paths = [entity.get("mount-point") for entity in
                     plistlib.loads(result.stdout).get("system-entities", [])]
    if str(mountpoint) not in mounted_paths:
        for path in mounted_paths:
            if path:
                subprocess.run(["hdiutil", "detach", path], check=False, capture_output=True)
        raise RuntimeError(f"Unexpected image mount location: {mounted_paths}")


def verify_read_only_mount(mountpoint: Path) -> dict[str, Any]:
    """Verify the mounted filesystem is read-only and rejects real writes."""
    result = subprocess.run(
        ["diskutil", "info", "-plist", str(mountpoint)], check=True, capture_output=True,
    )
    info = plistlib.loads(result.stdout)
    if info.get("Writable") is not False:
        raise RuntimeError("Disk image mount is not reported read-only by diskutil")
    probe = mountpoint / f".freeze-write-probe-{uuid.uuid4().hex}"
    try:
        with probe.open("xb") as handle:
            handle.write(b"must not succeed")
    except OSError as exc:
        if exc.errno != 30:  # EROFS: do not count ordinary permission errors as a mount lock.
            raise RuntimeError(f"Unexpected read-only write-probe error: {exc}") from exc
    else:
        probe.unlink()
        raise RuntimeError("Read-only mount allowed creation of a file")
    return {"mountpoint": str(mountpoint), "filesystem_writable": False, "write_probe_errno": 30}


def _image_snapshot(partial: Path, destination: Path, fish_id: str, records: list[InventoryRecord]) -> dict[str, Any]:
    """Convert an already verified payload into a UDRO image, then verify its mount."""
    container = destination.parent / f".{destination.name}.image-{uuid.uuid4().hex}"
    image = container / "frozen_run.dmg"
    working_image = container / "assembly.dmg"
    volume_name = destination.name
    mounted = Path("/Volumes") / volume_name
    if mounted.exists():
        raise FileExistsError(f"Snapshot mountpoint already exists: {mounted}")
    container.mkdir()
    attached = False
    promoted = False
    try:
        # Image capacity includes literal sidecars preserved in the frozen image.
        byte_count = sum(path.stat().st_size for path in partial.rglob("*") if path.is_file())
        size_mb = max(64, (byte_count * 5 // 4) // (1024 * 1024) + 64)
        subprocess.run(
            ["hdiutil", "create", "-size", f"{size_mb}m",
             "-fs", "HFS+", "-volname", volume_name, str(working_image)],
            check=True, capture_output=True, text=True,
        )
        _attach_read_only(working_image, mounted, read_only=False)
        attached = True
        # hdiutil -srcfolder interprets valid exFAT AppleDouble files as xattrs
        # and silently removes their literal entries. Copy every verified file
        # byte-for-byte into the image, then convert its filesystem blocks.
        shutil.copytree(partial, mounted, symlinks=True, copy_function=shutil.copyfile, dirs_exist_ok=True)
        subprocess.run(["hdiutil", "detach", str(mounted)], check=True, capture_output=True, text=True)
        attached = False
        subprocess.run(["hdiutil", "convert", str(working_image), "-format", "UDRO", "-o", str(image)],
                       check=True, capture_output=True, text=True)
        working_image.unlink()
        subprocess.run(["hdiutil", "verify", str(image)], check=True, capture_output=True, text=True)
        _attach_read_only(image, mounted)
        attached = True
        mounted_records = build_inventory([
            (fish_id, mounted / "snapshot" / fish_id),
            ("pipeline_manifests", mounted / "snapshot" / "pipeline_manifests"),
        ])
        if _content_signature(records) != _content_signature(mounted_records):
            expected = {record.path: record for record in records}
            observed = {record.path: record for record in mounted_records}
            missing = sorted(expected.keys() - observed.keys())
            extra = sorted(observed.keys() - expected.keys())
            changed = [key for key in expected.keys() & observed.keys()
                       if _content_signature([expected[key]]) != _content_signature([observed[key]])]
            raise RuntimeError(f"Mounted disk-image payload differs: missing={missing[:10]}, extra={extra[:10]}, changed={changed[:10]}")
        metadata = json.loads((mounted / "freeze_metadata.json").read_text(encoding="utf-8"))
        for relative, expected in metadata["provenance"].items():
            if _sha256_file(mounted / relative) != expected:
                raise RuntimeError(f"Mounted provenance hash mismatch: {relative}")
        lock = verify_read_only_mount(mounted)
        subprocess.run(["hdiutil", "detach", str(mounted)], check=True, capture_output=True, text=True)
        attached = False
        image_hash = _sha256_file(image)
        _write_json(container / "image_verification.json", {
            "image": "frozen_run.dmg", "sha256": image_hash, "format": "UDRO",
            "hdiutil_verify": "pass", "mounted_payload_matches_source": True,
            "provenance_hashes_match": True, "lock_verification": lock,
            "protection_scope": "Mounted payload is read-only; exFAT container can still be deleted or replaced.",
        })
        (container / "README.md").write_text(
            f"# Verified frozen run\n\nThe browsable snapshot is at `{mounted}` on a read-only UDRO disk image.\n"
            "To remount after reboot: `hdiutil attach -readonly -nobrowse "
            f"'{destination / 'frozen_run.dmg'}'`\n\n"
            "The mounted filesystem rejects writes (EROFS). The exFAT container itself can still be deleted "
            "or replaced; the image SHA-256 detects modification.\n", encoding="utf-8",
        )
        container.rename(destination)
        promoted = True
        _attach_read_only(destination / "frozen_run.dmg", mounted)
        attached = True
        lock = verify_read_only_mount(mounted)
        return {"image_sha256": image_hash, "lock_verification": lock}
    except Exception:
        if attached:
            subprocess.run(["hdiutil", "detach", str(mounted)], check=False, capture_output=True)
        cleanup = destination if promoted else container
        shutil.rmtree(cleanup, ignore_errors=True)
        raise


def freeze_single_fish_run(
    *,
    fish_id: str,
    pipeline_root: Path,
    manifest_dir: Path,
    destination: Path,
    repo_root: Path,
    scientific_commit: str | None = None,
    protection: str = "permissions",
) -> dict[str, Any]:
    """Create and verify a browsable, read-only freeze without mutating sources."""
    pipeline_root = pipeline_root.expanduser().resolve()
    manifest_dir = manifest_dir.expanduser().resolve()
    destination = destination.expanduser().resolve()
    repo_root = repo_root.expanduser().resolve()
    if protection not in {"permissions", "read-only-image"}:
        raise ValueError(f"Unknown snapshot protection: {protection}")
    if protection == "read-only-image" and sys.platform != "darwin":
        raise ValueError("Read-only disk images require macOS hdiutil")
    if pipeline_root.name != fish_id:
        raise ValueError(f"Pipeline root must end in fish ID {fish_id}: {pipeline_root}")
    for source in (pipeline_root, manifest_dir):
        if not source.is_dir():
            raise FileNotFoundError(f"Required freeze source directory is missing: {source}")
        if destination == source or source in destination.parents:
            raise ValueError("Freeze destination must be outside the source trees")
    if destination.exists():
        raise FileExistsError(f"Freeze destination already exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.parent / f".{destination.name}.partial-{uuid.uuid4().hex}"
    if partial.exists():
        raise FileExistsError(f"Unexpected partial freeze path already exists: {partial}")

    source_roots = [(fish_id, pipeline_root), ("pipeline_manifests", manifest_dir)]
    started_at = _utc_now()
    source_before = build_inventory(source_roots)
    promoted = False
    try:
        snapshot_root = partial / "snapshot"
        snapshot_root.mkdir(parents=True)
        shutil.copytree(pipeline_root, snapshot_root / fish_id, symlinks=True, copy_function=shutil.copy2)
        shutil.copytree(manifest_dir, snapshot_root / "pipeline_manifests", symlinks=True, copy_function=shutil.copy2)

        source_after = build_inventory(source_roots)
        if source_before != source_after:
            raise RuntimeError("Freeze source changed while it was being copied; refusing snapshot")
        frozen_inventory = build_inventory(
            [(fish_id, snapshot_root / fish_id), ("pipeline_manifests", snapshot_root / "pipeline_manifests")]
        )
        if _content_signature(source_before) != _content_signature(frozen_inventory):
            raise RuntimeError("Frozen payload does not match the source SHA-256 inventory")

        provenance = partial / "provenance"
        provenance.mkdir()
        git_record = build_git_record(repo_root)
        runtime_record = build_runtime_record()
        scientific_revision = _run_git(repo_root, "rev-parse", "--verify", f"{scientific_commit or 'HEAD'}^{{commit}}").strip()
        scientific_record = {
            "commit": scientific_revision,
            "tree": _run_git(repo_root, "rev-parse", f"{scientific_revision}^{{tree}}").strip(),
            "description": _run_git(repo_root, "log", "-1", "--format=%h %ci %s", scientific_revision).strip(),
            "provenance_basis": "Explicit historical run commit; distinct from freeze utility checkout state",
            "snapshot_type": "committed Git tree archive (no working-tree modifications)",
        }
        subprocess.run(["git", "-C", str(repo_root), "archive", "--format=tar",
                        "--output", str(provenance / "scientific_code.tar"), scientific_revision], check=True)
        stage_details, stage_status = build_stage_records(manifest_dir)
        _write_inventory(partial / "sha256_inventory.tsv", frozen_inventory)
        _write_json(provenance / "git_state.json", git_record)
        _write_json(provenance / "scientific_runtime.json", runtime_record)
        _write_json(provenance / "scientific_git.json", scientific_record)
        _write_json(provenance / "stage_parameters_outputs.json", stage_details)
        _write_json(provenance / "stage_status_compact.json", stage_status)

        record_paths = [
            partial / "sha256_inventory.tsv",
            provenance / "git_state.json",
            provenance / "scientific_runtime.json",
            provenance / "scientific_git.json",
            provenance / "scientific_code.tar",
            provenance / "stage_parameters_outputs.json",
            provenance / "stage_status_compact.json",
        ]
        metadata = {
            "freeze_format_version": FREEZE_FORMAT_VERSION,
            "fish_id": fish_id,
            "started_at": started_at,
            "completed_at": _utc_now(),
            "source": {
                "pipeline_root": str(pipeline_root),
                "manifest_dir": str(manifest_dir),
            },
            "snapshot": {
                "destination": str(destination),
                "pipeline_root": f"snapshot/{fish_id}",
                "manifest_dir": "snapshot/pipeline_manifests",
                "browsable_root": str(Path("/Volumes") / destination.name) if protection == "read-only-image" else str(destination),
            },
            "verification": {
                "status": "pass",
                "algorithm": "sha256",
                "source_stable_during_copy": True,
                "payload_matches_source": True,
                "record_count": len(frozen_inventory),
                "file_count": sum(record.kind == "file" for record in frozen_inventory),
                "symlink_count": sum(record.kind == "symlink" for record in frozen_inventory),
                "payload_size_bytes": sum(record.size_bytes for record in frozen_inventory),
            },
            "provenance": {
                str(path.relative_to(partial)): _sha256_file(path)
                for path in record_paths
            },
            "git": {
                "commit": git_record["commit"],
                "branch": git_record["branch"],
                "dirty": git_record["dirty"],
            },
            "read_only": {
                "policy": protection,
                "scope": "read-only UDRO mounted filesystem; container deletion/replacement remains possible" if protection == "read-only-image" else "filesystem permissions or immutable flags",
                "applied": True,
            },
        }
        _write_json(partial / "freeze_metadata.json", metadata)
        (partial / "README.md").write_text(
            "# Frozen single-fish pipeline run\n\n"
            f"Fish: `{fish_id}`  \n"
            f"Created: `{metadata['completed_at']}`  \n"
            f"Git commit: `{git_record['commit']}`  \n"
            "Verification: `pass` (source stable; frozen payload matches SHA-256 inventory)\n\n"
            "- `snapshot/` is the browsable copied run and external manifest directory.\n"
            "- `sha256_inventory.tsv` inventories every copied file and symlink.\n"
            "- `provenance/` records Git state, interpreter/packages, stage parameters/outputs, and compact status.\n"
            "- `freeze_metadata.json` ties the snapshot and provenance records together.\n",
            encoding="utf-8",
        )

        if protection == "read-only-image":
            image_verification = _image_snapshot(partial, destination, fish_id, frozen_inventory)
            _remove_staging_tree(partial)
            return {**metadata, **image_verification}

        # Lock children before promotion. The directory itself is locked only
        # after rename because an immutable directory entry cannot be renamed.
        _make_read_only(partial, include_root=False)
        writable = _writable_paths(partial, include_root=False)
        if writable:
            raise RuntimeError(f"Read-only verification failed for {len(writable)} paths")
        partial.rename(destination)
        promoted = True
        _make_read_only(destination)
        writable = _writable_paths(destination)
        if writable:
            raise RuntimeError(f"Final read-only verification failed for {len(writable)} paths")
        return metadata
    except Exception:
        cleanup_root = destination if promoted and destination.exists() else partial
        if cleanup_root.exists():
            _unlock_for_cleanup(cleanup_root)
            shutil.rmtree(cleanup_root, ignore_errors=True)
        raise


__all__ = [
    "FREEZE_FORMAT_VERSION",
    "InventoryRecord",
    "build_git_record",
    "build_inventory",
    "build_runtime_record",
    "build_stage_records",
    "freeze_single_fish_run",
    "verify_read_only_mount",
]
