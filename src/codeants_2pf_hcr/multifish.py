"""High-throughput multi-fish stage builders."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import tifffile

from .cohort_suite2p import parse_fish_ids_csv
from .context import (
    OrientationResolutionError,
    default_local_root,
    default_nas_root,
    prepare_notebook_paths,
    preprocess_anatomy_uint8_stage,
    resolve_fish_context,
)
from .matching import annotate_session_anat_label_duplicates
from .segmentation import AnatomyCellposeConfig, run_anatomy_cellpose_stage
from .stimulus import discover_functional_sessions


@dataclass(frozen=True)
class MultiFishAnatomySegmentationConfig:
    fish_ids_csv: str = "L758_f02,L758_f03,L758_f04,L758_f06,L758_f07"
    owner: str = "Danin"
    data_mode: str = "local"
    local_root_override: str | Path | None = None
    nas_root_override: str | Path | None = None
    cohort_outdir_override: str | Path | None = None
    matching_metadata_csv_override: str | Path | None = None
    cellpose_model_root_override: str | Path | None = None
    anat_cp_model_path_override: str | Path | None = None
    force_recompute: bool = False
    skip_if_exists: bool = True
    save_8bit: bool = True
    use_anisotropy: bool = True
    use_gpu: bool | None = None
    compute_device: str | None = None
    continue_on_error: bool = True
    verbose: bool = True


@dataclass(frozen=True)
class MultiFishFunctionalAnatomyMatchConfig:
    fish_ids_csv: str = "L758_f02,L758_f03,L758_f04,L758_f06,L758_f07"
    owner: str = "Danin"
    data_mode: str = "local"
    local_root_override: str | Path | None = None
    nas_root_override: str | Path | None = None
    cohort_outdir_override: str | Path | None = None
    skip_build_if_cached: bool = True
    force_build: bool = False
    continue_on_error: bool = True
    verbose: bool = True


def _resolve_multifish_environment(cfg: MultiFishAnatomySegmentationConfig) -> dict[str, Any]:
    data_mode = str(cfg.data_mode).strip().lower()
    data_mode = "local" if data_mode == "local" else "nas"
    local_root = Path(cfg.local_root_override) if cfg.local_root_override else None
    nas_root = Path(cfg.nas_root_override) if cfg.nas_root_override else default_nas_root()
    data_root = local_root if data_mode == "local" and local_root is not None else None
    if data_root is None:
        data_root = Path(default_local_root()) if data_mode == "local" else nas_root
    outdir_default = (
        Path(data_root) / "cohort_outputs" / "multiFish"
        if data_mode == "local"
        else Path.cwd() / "cohort_outputs" / "multiFish"
    )
    outdir = Path(cfg.cohort_outdir_override) if cfg.cohort_outdir_override else outdir_default
    outdir.mkdir(parents=True, exist_ok=True)
    return {
        "MULTIFISH_DATA_MODE": data_mode,
        "MULTIFISH_DATA_ROOT": Path(data_root),
        "MULTIFISH_OWNER": str(cfg.owner),
        "MULTIFISH_FISH_IDS": parse_fish_ids_csv(cfg.fish_ids_csv),
        "MULTIFISH_OUTDIR": outdir,
        "MULTIFISH_OUTDIR_DEFAULT": outdir_default,
    }


def _resolve_multifish_match_environment(cfg: MultiFishFunctionalAnatomyMatchConfig) -> dict[str, Any]:
    data_mode = str(cfg.data_mode).strip().lower()
    data_mode = "local" if data_mode == "local" else "nas"
    local_root = Path(cfg.local_root_override) if cfg.local_root_override else None
    nas_root = Path(cfg.nas_root_override) if cfg.nas_root_override else default_nas_root()
    data_root = local_root if data_mode == "local" and local_root is not None else None
    if data_root is None:
        data_root = Path(default_local_root()) if data_mode == "local" else nas_root
    outdir_default = (
        Path(data_root) / "cohort_outputs" / "multiFish"
        if data_mode == "local"
        else Path.cwd() / "cohort_outputs" / "multiFish"
    )
    outdir = Path(cfg.cohort_outdir_override) if cfg.cohort_outdir_override else outdir_default
    outdir.mkdir(parents=True, exist_ok=True)
    return {
        "MULTIFISH_DATA_MODE": data_mode,
        "MULTIFISH_DATA_ROOT": Path(data_root),
        "MULTIFISH_OWNER": str(cfg.owner),
        "MULTIFISH_FISH_IDS": parse_fish_ids_csv(cfg.fish_ids_csv),
        "MULTIFISH_OUTDIR": outdir,
        "MULTIFISH_OUTDIR_DEFAULT": outdir_default,
    }


def _resolve_multifish_fish_dir(data_root: str | Path, owner: str, fish_id: str, *, data_mode: str) -> Path:
    root = Path(data_root)
    fish_id_str = str(fish_id)
    owner_str = str(owner)
    if str(data_mode).strip().lower() == "local":
        candidates = [
            root / fish_id_str,
            root / owner_str / fish_id_str,
            root / owner_str / "Microscopy" / fish_id_str,
        ]
    else:
        owner_root = root / owner_str / "Microscopy"
        if not owner_root.exists():
            owner_root = root / owner_str
        candidates = [owner_root / fish_id_str, root / fish_id_str]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


def multifish_anatomy_segmentation_cache_paths(outdir: str | Path) -> dict[str, Path]:
    outdir = Path(outdir)
    return {
        "summary_csv": outdir / "multifish_anatomy_segmentation_summary.csv",
    }


def multifish_functional_anatomy_match_cache_paths(outdir: str | Path) -> dict[str, Path]:
    outdir = Path(outdir)
    return {
        "roi_identity_csv": outdir / "multifish_functional_roi_activity_identity.csv",
        "summary_csv": outdir / "multifish_functional_roi_activity_identity_summary.csv",
        "duplicate_summary_csv": outdir / "multifish_multiplane_duplicate_summary.csv",
    }


def _load_multifish_match_outputs_from_disk(outdir: str | Path) -> dict[str, pd.DataFrame]:
    paths = multifish_functional_anatomy_match_cache_paths(outdir)
    loaded: dict[str, pd.DataFrame] = {}
    for key, path in (
        ("MULTIFISH_FUNC_ACTIVITY_IDENTITY_DF", paths["roi_identity_csv"]),
        ("MULTIFISH_FUNC_ACTIVITY_IDENTITY_SUMMARY_DF", paths["summary_csv"]),
        ("MULTIFISH_MULTIPLANE_DUPLICATE_SUMMARY_DF", paths["duplicate_summary_csv"]),
    ):
        if path.exists():
            loaded[key] = pd.read_csv(path)
    return loaded


def _plane_session_labels(fish_dir: Path, fish_id: str, plane_values: list[int]) -> dict[int, str]:
    sessions = discover_functional_sessions(fish_dir, fish_id)
    plane_to_session: dict[int, str] = {}
    for session in sessions:
        session_label = str(session.get("session_label") or "unknown")
        for plane in session.get("output_planes", []) or []:
            try:
                plane_to_session[int(plane)] = session_label
            except Exception:
                pass
    if not plane_to_session and plane_values:
        plane_to_session = {int(plane): "single_context" for plane in plane_values}
    return plane_to_session


def _count_mask_labels(path: Path | None) -> int | None:
    if path is None or not path.exists():
        return None
    arr = np.asarray(tifffile.imread(str(path)))
    if arr.size == 0:
        return 0
    return int(np.count_nonzero(np.unique(arr)))


def _multifish_match_summary_row(
    *,
    fish_id: str,
    fish_dir: Path | None,
    status: str,
    notes: str,
    source_csv: Path | None = None,
    df: pd.DataFrame | None = None,
) -> dict[str, Any]:
    n_rois = int(len(df)) if isinstance(df, pd.DataFrame) else 0
    n_retained = 0
    n_duplicate_rows = 0
    if isinstance(df, pd.DataFrame) and not df.empty:
        if "is_retained_after_multiplane_dedup" in df.columns:
            n_retained = int(df["is_retained_after_multiplane_dedup"].astype(bool).sum())
        if "dedup_outcome" in df.columns:
            n_duplicate_rows = int((df["dedup_outcome"].astype(str) == "duplicate anatomy label within session").sum())
    return {
        "fish_id": str(fish_id),
        "fish_dir": str(fish_dir) if fish_dir is not None else None,
        "status": str(status),
        "notes": str(notes),
        "source_csv": str(source_csv) if source_csv is not None else None,
        "n_rois": n_rois,
        "n_retained_after_multiplane_dedup": n_retained,
        "n_duplicate_roi_rows_removed_by_filter": n_duplicate_rows,
    }


def _duplicate_summary_df(df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "fish_id",
        "session_label",
        "dedup_group_key",
        "selected_anat_label",
        "n_group_rows",
        "n_duplicate_loser_rows",
        "retained_plane_idx",
        "retained_func_label",
    ]
    if df is None or df.empty or "dedup_group_key" not in df.columns:
        return pd.DataFrame(columns=columns)
    work = df[df["dedup_group_key"].notna()].copy()
    if work.empty:
        return pd.DataFrame(columns=columns)
    rows: list[dict[str, Any]] = []
    for group_key, sub in work.groupby("dedup_group_key", sort=True):
        if len(sub) <= 1:
            continue
        retained = sub[sub["is_retained_after_multiplane_dedup"].astype(bool)].copy()
        retained_row = retained.iloc[0] if not retained.empty else sub.iloc[0]
        rows.append(
            {
                "fish_id": retained_row.get("fish_id"),
                "session_label": retained_row.get("session_label"),
                "dedup_group_key": group_key,
                "selected_anat_label": retained_row.get("selected_anat_label"),
                "n_group_rows": int(len(sub)),
                "n_duplicate_loser_rows": int((~sub["is_retained_after_multiplane_dedup"].astype(bool)).sum()),
                "retained_plane_idx": retained_row.get("plane_idx"),
                "retained_func_label": retained_row.get("func_label"),
            }
        )
    return pd.DataFrame(rows, columns=columns)


def _segmentation_row(
    *,
    fish_id: str,
    fish_dir: Path | None,
    status: str,
    notes: str,
    anat_source_path: Path | None = None,
    anat_labels_path: Path | None = None,
) -> dict[str, Any]:
    return {
        "fish_id": str(fish_id),
        "fish_dir": str(fish_dir) if fish_dir is not None else None,
        "status": str(status),
        "notes": str(notes),
        "anat_source_path": str(anat_source_path) if anat_source_path is not None else None,
        "anat_labels_path": str(anat_labels_path) if anat_labels_path is not None else None,
        "n_anatomy_labels": _count_mask_labels(anat_labels_path),
    }


def run_multifish_anatomy_segmentation_stage(config: MultiFishAnatomySegmentationConfig | None = None) -> dict[str, Any]:
    """Run/reuse single-fish anatomy Cellpose segmentation for a configured fish cohort."""
    cfg = config or MultiFishAnatomySegmentationConfig()
    env = _resolve_multifish_environment(cfg)
    paths = multifish_anatomy_segmentation_cache_paths(env["MULTIFISH_OUTDIR"])
    fish_ids = list(env["MULTIFISH_FISH_IDS"])
    if not fish_ids:
        raise RuntimeError("[multiFish-anatomy] no fish IDs configured")

    rows: list[dict[str, Any]] = []
    log_lines: list[str] = []
    for fish_id in fish_ids:
        fish_dir: Path | None = None
        try:
            ctx = resolve_fish_context(
                fish_id=str(fish_id),
                owner=str(cfg.owner),
                data_mode=str(cfg.data_mode),
                nas_root=cfg.nas_root_override,
                local_root=cfg.local_root_override,
                matching_metadata_csv_override=cfg.matching_metadata_csv_override,
                cellpose_model_root_override=cfg.cellpose_model_root_override,
                anat_cp_model_path_override=cfg.anat_cp_model_path_override,
            )
            fish_dir = ctx.fish_dir
            path_bindings = prepare_notebook_paths(ctx)
            anat_source = path_bindings.get("ANAT_STACK_PATH")
            if anat_source in (None, "", False):
                rows.append(_segmentation_row(fish_id=str(fish_id), fish_dir=fish_dir, status="skip", notes="anatomy source missing"))
                continue
            anat_preproc = preprocess_anatomy_uint8_stage(
                anat_stack_path=anat_source,
                anat_stack_path_orig=anat_source,
                preproc_dir=ctx.preproc_dir,
                polarity=path_bindings.get("POLARITY"),
                polarity_source=path_bindings.get("POLARITY_SOURCE"),
            )
            anat_source = anat_preproc.get("anat_stack_path")
            log_lines.extend(f"[multiFish-anatomy] {fish_id}: {line}" for line in anat_preproc.get("log_lines", []))
            result = run_anatomy_cellpose_stage(
                anat_seg_source_path=anat_source,
                analysis_dir=ctx.analysis_dir,
                anat_labels_path=path_bindings.get("ANAT_LABELS_PATH"),
                anat_cp_model_path=ctx.anat_cp_model_path,
                vox_anat=None,
                assert_fish_compatible=None,
                fish_id=str(fish_id),
                config=AnatomyCellposeConfig(
                    force_recompute=bool(cfg.force_recompute),
                    skip_if_exists=bool(cfg.skip_if_exists),
                    save_8bit=bool(cfg.save_8bit),
                    use_anisotropy=bool(cfg.use_anisotropy),
                    use_gpu=cfg.use_gpu,
                    compute_device=cfg.compute_device,
                    verbose=bool(cfg.verbose),
                ),
            )
            bindings = result.get("bindings", {})
            anat_labels = bindings.get("ANAT_LABELS_PATH")
            rows.append(
                _segmentation_row(
                    fish_id=str(fish_id),
                    fish_dir=fish_dir,
                    status=str(result.get("status", "ok")),
                    notes="; ".join(str(line) for line in result.get("log_lines", [])),
                    anat_source_path=Path(anat_source),
                    anat_labels_path=Path(anat_labels) if anat_labels not in (None, "", False) else None,
                )
            )
            log_lines.extend(f"[multiFish-anatomy] {fish_id}: {line}" for line in result.get("log_lines", []))
        except OrientationResolutionError:
            raise
        except Exception as exc:
            if not cfg.continue_on_error:
                raise
            rows.append(_segmentation_row(fish_id=str(fish_id), fish_dir=fish_dir, status="error", notes=str(exc)))
            log_lines.append(f"[multiFish-anatomy] {fish_id}: error -> {exc}")

    summary_df = pd.DataFrame(rows)
    summary_df.to_csv(paths["summary_csv"], index=False)
    bindings = dict(env)
    bindings.update(
        {
            "MULTIFISH_ANATOMY_SEGMENTATION_DF": summary_df,
            "multifish_anatomy_segmentation_cache_paths": paths,
        }
    )
    return {"bindings": bindings, "state": {"n_fish": len(fish_ids)}, "config": asdict(cfg), "log_lines": log_lines}


def run_multifish_functional_anatomy_match_stage(config: MultiFishFunctionalAnatomyMatchConfig | None = None) -> dict[str, Any]:
    """Aggregate single-fish ROI master tables and flag session-scoped multiplane duplicates."""
    cfg = config or MultiFishFunctionalAnatomyMatchConfig()
    env = _resolve_multifish_match_environment(cfg)
    paths = multifish_functional_anatomy_match_cache_paths(env["MULTIFISH_OUTDIR"])
    if bool(cfg.skip_build_if_cached) and not bool(cfg.force_build):
        loaded = _load_multifish_match_outputs_from_disk(env["MULTIFISH_OUTDIR"])
        if {
            "MULTIFISH_FUNC_ACTIVITY_IDENTITY_DF",
            "MULTIFISH_FUNC_ACTIVITY_IDENTITY_SUMMARY_DF",
            "MULTIFISH_MULTIPLANE_DUPLICATE_SUMMARY_DF",
        }.issubset(loaded):
            bindings = dict(env)
            bindings.update(loaded)
            bindings["multifish_functional_anatomy_match_cache_paths"] = paths
            if cfg.verbose:
                print(f"[multiFish-match] reusing cached outputs from {env['MULTIFISH_OUTDIR']}")
            return {"bindings": bindings, "state": {"cohort_build_skipped": True}, "config": asdict(cfg), "log_lines": []}

    fish_ids = list(env["MULTIFISH_FISH_IDS"])
    if not fish_ids:
        raise RuntimeError("[multiFish-match] no fish IDs configured")

    detail_frames: list[pd.DataFrame] = []
    summary_rows: list[dict[str, Any]] = []
    log_lines: list[str] = []
    for fish_id in fish_ids:
        fish_dir = _resolve_multifish_fish_dir(
            env["MULTIFISH_DATA_ROOT"],
            env["MULTIFISH_OWNER"],
            fish_id,
            data_mode=str(env["MULTIFISH_DATA_MODE"]),
        )
        source_csv = fish_dir / "03_analysis" / "functional" / "registration" / "functional_roi_activity_identity.csv"
        try:
            if not fish_dir.exists():
                summary_rows.append(_multifish_match_summary_row(fish_id=fish_id, fish_dir=fish_dir, status="skip", notes="fish_dir missing"))
                continue
            if not source_csv.exists():
                summary_rows.append(
                    _multifish_match_summary_row(
                        fish_id=fish_id,
                        fish_dir=fish_dir,
                        status="skip",
                        notes="functional_roi_activity_identity.csv missing",
                        source_csv=source_csv,
                    )
                )
                continue
            df = pd.read_csv(source_csv)
            if "fish_id" not in df.columns:
                df["fish_id"] = str(fish_id)
            else:
                df["fish_id"] = df["fish_id"].fillna(str(fish_id)).astype(str)
            if "plane_idx" in df.columns:
                planes = sorted(pd.to_numeric(df["plane_idx"], errors="coerce").dropna().astype(int).unique().tolist())
            else:
                planes = []
            session_map = _plane_session_labels(fish_dir, str(fish_id), planes)
            if "plane_idx" in df.columns:
                plane_series = pd.to_numeric(df["plane_idx"], errors="coerce")
                df["session_label"] = plane_series.map(lambda value: session_map.get(int(value), "unknown") if pd.notna(value) else "unknown")
            else:
                df["session_label"] = "unknown"
            df["multifish_source_csv"] = str(source_csv)
            df = annotate_session_anat_label_duplicates(df)
            detail_frames.append(df)
            summary_rows.append(
                _multifish_match_summary_row(
                    fish_id=fish_id,
                    fish_dir=fish_dir,
                    status="ok",
                    notes="",
                    source_csv=source_csv,
                    df=df,
                )
            )
            n_lost = int((df["dedup_outcome"].astype(str) == "duplicate anatomy label within session").sum())
            log_lines.append(f"[multiFish-match] {fish_id}: loaded {len(df)} ROI rows; duplicate loser rows={n_lost}")
        except Exception as exc:
            if not cfg.continue_on_error:
                raise
            summary_rows.append(_multifish_match_summary_row(fish_id=fish_id, fish_dir=fish_dir, status="error", notes=str(exc), source_csv=source_csv))
            log_lines.append(f"[multiFish-match] {fish_id}: error -> {exc}")

    detail_df = pd.concat(detail_frames, ignore_index=True) if detail_frames else pd.DataFrame()
    summary_df = pd.DataFrame(summary_rows)
    duplicate_summary_df = _duplicate_summary_df(detail_df)
    detail_df.to_csv(paths["roi_identity_csv"], index=False)
    summary_df.to_csv(paths["summary_csv"], index=False)
    duplicate_summary_df.to_csv(paths["duplicate_summary_csv"], index=False)
    bindings = dict(env)
    bindings.update(
        {
            "MULTIFISH_FUNC_ACTIVITY_IDENTITY_DF": detail_df,
            "MULTIFISH_FUNC_ACTIVITY_IDENTITY_SUMMARY_DF": summary_df,
            "MULTIFISH_MULTIPLANE_DUPLICATE_SUMMARY_DF": duplicate_summary_df,
            "multifish_functional_anatomy_match_cache_paths": paths,
        }
    )
    return {"bindings": bindings, "state": {"n_fish": len(fish_ids), "n_roi_rows": int(len(detail_df))}, "config": asdict(cfg), "log_lines": log_lines}


__all__ = [
    "MultiFishAnatomySegmentationConfig",
    "MultiFishFunctionalAnatomyMatchConfig",
    "multifish_anatomy_segmentation_cache_paths",
    "multifish_functional_anatomy_match_cache_paths",
    "run_multifish_anatomy_segmentation_stage",
    "run_multifish_functional_anatomy_match_stage",
]
