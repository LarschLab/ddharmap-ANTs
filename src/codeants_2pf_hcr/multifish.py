"""High-throughput multi-fish stage builders."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import tifffile

from .cohort_suite2p import parse_fish_ids_csv
from .context import default_local_root, default_nas_root, prepare_notebook_paths, resolve_fish_context
from .segmentation import AnatomyCellposeConfig, run_anatomy_cellpose_stage


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


def multifish_anatomy_segmentation_cache_paths(outdir: str | Path) -> dict[str, Path]:
    outdir = Path(outdir)
    return {
        "summary_csv": outdir / "multifish_anatomy_segmentation_summary.csv",
    }


def _count_mask_labels(path: Path | None) -> int | None:
    if path is None or not path.exists():
        return None
    arr = np.asarray(tifffile.imread(str(path)))
    if arr.size == 0:
        return 0
    return int(np.count_nonzero(np.unique(arr)))


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


__all__ = [
    "MultiFishAnatomySegmentationConfig",
    "multifish_anatomy_segmentation_cache_paths",
    "run_multifish_anatomy_segmentation_stage",
]
