"""Cohort build/cache stage helpers for `multi_fish_56h_56g.ipynb` cells [cfg], [helpers], and [cohort-build]."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import json
import os
import pathlib
import pickle
import re
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from skimage.measure import regionprops_table

from .context import default_local_root
from .spatial import apply_func_orientation


def _as_bool_series(series_in: Any) -> pd.Series:
    series = pd.Series(series_in)
    if pd.api.types.is_bool_dtype(series):
        return series.fillna(False).astype(bool)
    if pd.api.types.is_numeric_dtype(series):
        return series.fillna(0).astype(float) != 0
    return series.astype(str).str.strip().str.lower().isin({"1", "true", "t", "yes", "y"})


def _default_nas_root() -> Path:
    if os.name == "nt":
        return Path(r"\\nasdcsr.unil.ch\RECHERCHE\FAC\FBM\CIG\jlarsch\default\D2c\07_Data")
    return Path("/Volumes/jlarsch/default/D2c/07_Data")


def _default_local_root() -> Path:
    return default_local_root()


@dataclass(frozen=True)
class CohortBuildConfig:
    data_mode: str = "local"
    matching_metadata_csv_override: str | Path | None = None
    fish_specs: tuple[dict[str, str], ...] = field(
        default_factory=lambda: (
            {"owner": "Matilde", "fish_id": "L395_f11"},
            {"owner": "Matilde", "fish_id": "L396_f01"},
            {"owner": "Matilde", "fish_id": "L396_f03"},
            {"owner": "Matilde", "fish_id": "L396_f04"},
        )
    )
    window_pre_sec: float = 20.0
    window_post_sec: float = 50.0
    stim_onset_delay_sec: float = 10.0
    window_edge_policy: str = "pad_nan"
    min_valid_points_per_seg: int = 1
    bpi_edge_policy: str = "pad_nan"
    bpi_min_valid_frac: float = 0.5
    bpi_min_trials_per_class: int = 3
    bpi_denom_eps: float = 1e-6
    zscore_min_baseline_points: int = 200
    zscore_min_baseline_std: float = 1e-6
    stim_time_scale: float = 1.0
    measure_start_block: int | str = 1
    measure_start_event: str = "start"
    remove_interblock_gaps: bool = True
    min_segments: int = 3
    min_cells: int = 1
    max_genes: int = 12
    use_sem: bool = True
    cohort_trace_dt: float | None = None
    use_high_conf_only: bool = True
    low_conf_columns: tuple[str, ...] = (
        "_is_low_conf_q95",
        "_is_low_conf_match_any_globalR50",
        "is_low_confidence_segmentation",
    )
    default_gene_order: tuple[str, ...] = ("sst1.1", "sst1.2", "npy", "tac3b", "pth2", "cfos", "cort")
    default_gene_colors: dict[str, str] = field(default_factory=dict)
    panel_grid: tuple[tuple[str, str], tuple[str, str]] = (("contra_LB", "contra_LC"), ("ipsi_LB", "ipsi_LC"))
    plot_titles: dict[str, str] = field(default_factory=dict)
    cohort_outdir_override: str | Path | None = None
    skip_build_if_cached: bool = True
    force_build: bool = False
    fail_on_segment_duplication: bool = True
    trace_dedup_keys: tuple[str, ...] = ("gene", "plane", "func_label", "midline_side")
    verbose: bool = True

    def __post_init__(self) -> None:
        if not self.default_gene_colors:
            object.__setattr__(
                self,
                "default_gene_colors",
                {
                    "sst1.1": "#d62728",
                    "sst1.2": "#d61ad2",
                    "npy": "#1f9d55",
                    "tac3b": "#ffd400",
                    "pth2": "#00bcd4",
                    "cfos": "#ff7f0e",
                    "cort": "#8c564b",
                },
            )
        if not self.plot_titles:
            object.__setattr__(
                self,
                "plot_titles",
                {
                    "contra_LB": "Contra × bout-like",
                    "contra_LC": "Contra × continuous",
                    "ipsi_LB": "Ipsi × bout-like",
                    "ipsi_LC": "Ipsi × continuous",
                },
            )


def cohort_cache_paths(outdir: str | Path) -> dict[str, Path]:
    outdir = Path(outdir)
    return {
        "fish_summary_csv": outdir / "cohort_fish_processing_summary.csv",
        "stim_summary_csv": outdir / "cohort_stim_ipsi_contra_summary.csv",
        "bpi_trials_csv": outdir / "cohort_bpi_trials.csv",
        "bpi_cells_csv": outdir / "cohort_bpi_cells.csv",
        "cohort_53a_ncc_curves_csv": outdir / "cohort_53a_ncc_curves.csv",
        "cohort_53a_diameters_csv": outdir / "cohort_53a_diameters.csv",
        "cohort_53a_diameter_filter_summary_csv": outdir / "cohort_53a_diameter_filter_summary.csv",
        "cohort_53a_func_anat_offsets_csv": outdir / "cohort_53a_func_anat_offsets.csv",
        "cohort_53a_hcr_offsets_csv": outdir / "cohort_53a_hcr_offsets.csv",
        "cohort_53a_thresholds_csv": outdir / "cohort_53a_thresholds.csv",
        "trace_cache_pkl": outdir / "cohort_trace_cache.pkl",
    }


def count_trace_genes(results: Any, plot_order: list[str], *, nested: bool = False) -> int:
    genes: set[str] = set()
    if not isinstance(results, dict):
        return 0
    if nested:
        for panel_map in results.values():
            if not isinstance(panel_map, dict):
                continue
            for panel in plot_order:
                gene_map = panel_map.get(panel, {}) or {}
                if isinstance(gene_map, dict):
                    genes.update(str(gene) for gene in gene_map.keys())
    else:
        for panel in plot_order:
            gene_map = results.get(panel, {}) or {}
            if isinstance(gene_map, dict):
                genes.update(str(gene) for gene in gene_map.keys())
    return len(genes)


def save_cohort_outputs_to_disk(
    outdir: str | Path,
    *,
    cohort_fish_summary_df: pd.DataFrame | None = None,
    cohort_stim_summary_df: pd.DataFrame | None = None,
    cohort_bpi_trials_df: pd.DataFrame | None = None,
    cohort_bpi_cells_df: pd.DataFrame | None = None,
    cohort_tvec: np.ndarray | None = None,
    cohort_results_stim_ipsi_contra: dict[str, Any] | None = None,
    cohort_results_stim_ipsi_contra_by_fish: dict[str, Any] | None = None,
    cohort_results_stim_ipsi_contra_per_cell_by_fish: dict[str, Any] | None = None,
    cohort_mode_durations: dict[str, Any] | None = None,
    cohort_mode_event_counts: dict[str, Any] | None = None,
    cohort_53a_tables: dict[str, pd.DataFrame] | None = None,
    plot_order: list[str] | None = None,
    verbose: bool = True,
) -> None:
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    paths = cohort_cache_paths(outdir)
    plot_order_local = list(plot_order or ["contra_LB", "contra_LC", "ipsi_LB", "ipsi_LC"])

    if isinstance(cohort_fish_summary_df, pd.DataFrame) and not cohort_fish_summary_df.empty:
        cohort_fish_summary_df.to_csv(paths["fish_summary_csv"], index=False)
    if isinstance(cohort_stim_summary_df, pd.DataFrame) and not cohort_stim_summary_df.empty:
        cohort_stim_summary_df.to_csv(paths["stim_summary_csv"], index=False)
    if isinstance(cohort_bpi_trials_df, pd.DataFrame) and not cohort_bpi_trials_df.empty:
        cohort_bpi_trials_df.to_csv(paths["bpi_trials_csv"], index=False)
    if isinstance(cohort_bpi_cells_df, pd.DataFrame) and not cohort_bpi_cells_df.empty:
        cohort_bpi_cells_df.to_csv(paths["bpi_cells_csv"], index=False)

    if isinstance(cohort_53a_tables, dict):
        for table_key, cache_key in (
            ("ncc_curves_df", "cohort_53a_ncc_curves_csv"),
            ("diameters_df", "cohort_53a_diameters_csv"),
            ("diameter_filter_summary_df", "cohort_53a_diameter_filter_summary_csv"),
            ("func_anat_offsets_df", "cohort_53a_func_anat_offsets_csv"),
            ("hcr_offsets_df", "cohort_53a_hcr_offsets_csv"),
            ("thresholds_df", "cohort_53a_thresholds_csv"),
        ):
            table_df = cohort_53a_tables.get(table_key)
            if isinstance(table_df, pd.DataFrame):
                table_df.to_csv(paths[cache_key], index=False)

    trace_payload = {
        "cohort_tvec": np.asarray(cohort_tvec, dtype=np.float32) if cohort_tvec is not None else None,
        "cohort_results_stim_ipsi_contra": cohort_results_stim_ipsi_contra,
        "cohort_results_stim_ipsi_contra_by_fish": cohort_results_stim_ipsi_contra_by_fish,
        "cohort_results_stim_ipsi_contra_per_cell_by_fish": cohort_results_stim_ipsi_contra_per_cell_by_fish,
        "cohort_mode_durations": cohort_mode_durations,
        "cohort_mode_event_counts": cohort_mode_event_counts,
    }
    trace_gene_count = count_trace_genes(cohort_results_stim_ipsi_contra, plot_order_local)
    trace_gene_count_by_fish = count_trace_genes(cohort_results_stim_ipsi_contra_by_fish, plot_order_local, nested=True)
    if (
        trace_payload["cohort_tvec"] is not None
        and trace_payload["cohort_results_stim_ipsi_contra"] is not None
        and trace_gene_count > 0
        and trace_gene_count_by_fish > 0
    ):
        with paths["trace_cache_pkl"].open("wb") as handle:
            pickle.dump(trace_payload, handle, protocol=pickle.HIGHEST_PROTOCOL)
    elif paths["trace_cache_pkl"].exists():
        paths["trace_cache_pkl"].unlink()

    if verbose:
        print(f"[cache] saved cohort outputs in {outdir}")


def load_cohort_outputs_from_disk(
    outdir: str | Path,
    *,
    load_tables: bool = True,
    load_trace_cache: bool = True,
    load_cohort_53a_tables: bool = True,
    verbose: bool = True,
) -> dict[str, Any]:
    outdir = Path(outdir)
    paths = cohort_cache_paths(outdir)
    loaded: list[str] = []
    state: dict[str, Any] = {}

    if load_tables:
        for key, csv_path in (
            ("cohort_fish_summary_df", paths["fish_summary_csv"]),
            ("cohort_stim_summary_df", paths["stim_summary_csv"]),
            ("cohort_bpi_trials_df", paths["bpi_trials_csv"]),
            ("cohort_bpi_cells_df", paths["bpi_cells_csv"]),
        ):
            if csv_path.exists():
                try:
                    state[key] = pd.read_csv(csv_path)
                    loaded.append(key)
                except Exception as exc:
                    if verbose:
                        print(f"[cache] failed loading {csv_path.name}: {exc}")

    if load_cohort_53a_tables:
        for table_key, csv_path in (
            ("ncc_curves_df", paths["cohort_53a_ncc_curves_csv"]),
            ("diameters_df", paths["cohort_53a_diameters_csv"]),
            ("diameter_filter_summary_df", paths["cohort_53a_diameter_filter_summary_csv"]),
            ("func_anat_offsets_df", paths["cohort_53a_func_anat_offsets_csv"]),
            ("hcr_offsets_df", paths["cohort_53a_hcr_offsets_csv"]),
            ("thresholds_df", paths["cohort_53a_thresholds_csv"]),
        ):
            if csv_path.exists():
                try:
                    state[table_key] = pd.read_csv(csv_path)
                    loaded.append(table_key)
                except Exception as exc:
                    if verbose:
                        print(f"[cache] failed loading {csv_path.name}: {exc}")

    if load_trace_cache and paths["trace_cache_pkl"].exists():
        try:
            with paths["trace_cache_pkl"].open("rb") as handle:
                payload = pickle.load(handle)
            for key in (
                "cohort_tvec",
                "cohort_results_stim_ipsi_contra",
                "cohort_results_stim_ipsi_contra_by_fish",
                "cohort_results_stim_ipsi_contra_per_cell_by_fish",
                "cohort_mode_durations",
                "cohort_mode_event_counts",
            ):
                if key in payload:
                    state[key] = payload.get(key)
                    loaded.append(key)
        except Exception as exc:
            if verbose:
                print(f"[cache] failed loading {paths['trace_cache_pkl'].name}: {exc}")

    if verbose:
        if loaded:
            print(f"[cache] loaded: {', '.join(sorted(set(loaded)))}")
        else:
            print(f"[cache] no cached cohort outputs loaded from {outdir}")

    return state


def _owner_root(data_root: str | Path, owner: str) -> Path:
    base = Path(data_root) / str(owner)
    microscopy = base / "Microscopy"
    return microscopy if microscopy.exists() else base


def _resolve_fish_dir(data_root: str | Path, owner: str, fish_id: str, data_mode: str = "nas") -> Path:
    root = Path(data_root)
    fish_id = str(fish_id)
    owner = str(owner)
    if str(data_mode).strip().lower() == "local":
        for candidate in (root / fish_id, root / owner / fish_id, root / owner / "Microscopy" / fish_id):
            if candidate.exists():
                return candidate
        return root / fish_id
    return _owner_root(root, owner) / fish_id


def _fish_paths(data_root: str | Path, owner: str, fish_id: str, data_mode: str = "nas") -> dict[str, Path]:
    fish_dir = _resolve_fish_dir(data_root, owner, fish_id, data_mode=data_mode)
    analysis_dir = fish_dir / "03_analysis"
    func_dir = analysis_dir / "functional"
    out_reg = func_dir / "registration"
    return {
        "fish_dir": fish_dir,
        "analysis_dir": analysis_dir,
        "func_dir": func_dir,
        "out_reg": out_reg,
        "suite2p_root": func_dir / "suite2P",
        "conf_csv": out_reg / "conf_to_func_pairs.csv",
        "midline_json": out_reg / "midline_params_func_ref.json",
        "metadata_dir": fish_dir / "01_raw" / "2p" / "metadata",
    }


def _load_matching_metadata(path: str | Path) -> pd.DataFrame | None:
    target = Path(path)
    if not target.exists():
        return None
    try:
        return pd.read_csv(target)
    except Exception:
        return None


def _get_polarity(fish_id: str, md_df: pd.DataFrame | None) -> str | None:
    if md_df is None or md_df.empty or "fish_id" not in md_df.columns:
        return None
    row = md_df.loc[md_df["fish_id"].astype(str) == str(fish_id)]
    if row.empty:
        return None
    pol = str(row.iloc[0].get("polarity", "")).strip().lower()
    if pol in {"north", "south"}:
        return pol
    return None


def _block_key(code: str) -> int:
    match = re.match(r"^B(\d+)$", str(code))
    return int(match.group(1)) if match else 10**9


def _find_experiment_log(metadata_dir: str | Path, fish_id: str) -> Path | None:
    base = Path(metadata_dir)
    if not base.exists():
        return None
    hits: list[Path] = []
    for pattern in (f"*{fish_id}*experiment_log*.csv", "*experiment_log*.csv"):
        hits.extend(sorted(base.glob(pattern)))
    if not hits:
        return None
    return sorted(set(hits), key=lambda path: path.stat().st_mtime)[-1]


def _load_events_df(csv_path: str | Path, time_scale: float = 1.0) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    if df.empty:
        raise ValueError("experiment log is empty")
    df = df.rename(columns={column: column.strip().lower() for column in df.columns})
    event_cols = [column for column in df.columns if "event" in column]
    time_cols = [column for column in df.columns if ("timestamp" in column) or (column == "time")]
    if "event" in df.columns:
        event_col = "event"
    elif event_cols:
        event_col = event_cols[0]
    else:
        raise ValueError(f"Could not infer event column from {list(df.columns)}")
    if "timestamp" in df.columns:
        time_col = "timestamp"
    elif "time" in df.columns:
        time_col = "time"
    elif time_cols:
        time_col = time_cols[0]
    else:
        raise ValueError(f"Could not infer time column from {list(df.columns)}")
    out = df[[event_col, time_col]].rename(columns={event_col: "event", time_col: "time"})
    out["event"] = out["event"].astype(str).str.strip()
    out["time"] = pd.to_numeric(out["time"], errors="coerce") * float(time_scale)
    return out.dropna(subset=["event", "time"]).sort_values("time").reset_index(drop=True)


def _build_stim_table(
    df_evt: pd.DataFrame,
    measure_start_block: int | str = 1,
    measure_start_event: str = "start",
    remove_interblock_gaps: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame, list[dict[str, Any]]]:
    evt = df_evt.copy()
    if measure_start_block is not None:
        block_label = measure_start_block if isinstance(measure_start_block, str) else f"B{int(measure_start_block)}"
        start_event = f"{block_label}_{measure_start_event}"
        start_match = evt.loc[evt["event"] == start_event, "time"]
        if len(start_match):
            t0 = float(start_match.iloc[0])
        else:
            block_rows = evt[evt["event"].str.startswith(f"{block_label}_")]
            t0 = float(block_rows["time"].min()) if not block_rows.empty else None
        if t0 is not None:
            evt["time"] = evt["time"] - t0
            evt = evt[evt["time"] >= 0].reset_index(drop=True)

    block_codes: list[str] = []
    for event in evt["event"]:
        match = re.match(r"^(B\d+)_", event)
        if match:
            block_codes.append(match.group(1))
    block_codes = sorted(set(block_codes), key=_block_key)

    blocks: list[dict[str, Any]] = []
    for block in block_codes:
        block_events = evt[evt["event"].str.startswith(f"{block}_")]
        if block_events.empty:
            continue
        bstart = None
        bend = None
        s = block_events.loc[block_events["event"] == f"{block}_start", "time"]
        if len(s):
            bstart = float(s.iloc[0])
        e = block_events.loc[block_events["event"] == f"{block}_end", "time"]
        if len(e):
            bend = float(e.iloc[0])
        if bstart is None:
            bstart = float(block_events["time"].min())
        ib = block_events.loc[block_events["event"] == f"{block}_interblock_pause", "time"]
        ib_time = float(ib.iloc[0]) if len(ib) else None
        if bend is None:
            bend = ib_time
        elif ib_time is not None:
            bend = ib_time
        if bend is None:
            bend = float(block_events["time"].max())
        blocks.append({"block": block, "start": bstart, "end": bend})

    if remove_interblock_gaps and blocks:
        blocks = sorted(blocks, key=lambda d: d["start"])
        shift_map: dict[str, float] = {}
        shift = 0.0
        prev_end = None
        for block in blocks:
            orig_start = float(block["start"])
            orig_end = float(block["end"])
            if prev_end is not None:
                shift += max(0.0, orig_start - prev_end)
            shift_map[str(block["block"])] = shift
            block["start"] = orig_start - shift
            block["end"] = orig_end - shift
            prev_end = orig_end

        def _shift_evt_time(row: pd.Series) -> float:
            match = re.match(r"^(B\d+)_", str(row["event"]))
            if not match:
                return float(row["time"])
            return float(row["time"]) - float(shift_map.get(match.group(1), 0.0))

        evt["time"] = evt.apply(_shift_evt_time, axis=1)
        evt = evt.sort_values("time").reset_index(drop=True)

    stims: list[dict[str, Any]] = []
    for _, row in evt.iterrows():
        match = re.match(r"^(B\d+)_stim(\d+)_(.+)$", str(row["event"]))
        if not match:
            continue
        block, stim_idx, stim_type = match.group(1), int(match.group(2)), str(match.group(3))
        t0 = float(row["time"])
        end_time = None
        post_name = f"{block}_poststim{stim_idx}_pause"
        post_match = evt.loc[evt["event"] == post_name, "time"]
        if len(post_match):
            end_time = float(post_match.iloc[0])
        if end_time is None:
            after = evt[(evt["time"] > t0) & evt["event"].str.startswith(f"{block}_")].sort_values("time")
            end_time = float(after["time"].iloc[0]) if not after.empty else t0 + 10.0
        stims.append(
            {
                "block": block,
                "stim_idx": stim_idx,
                "type": stim_type,
                "start": t0,
                "end": end_time,
                "duration": float(end_time - t0),
            }
        )
    return evt, pd.DataFrame(stims), blocks


def _load_ops_npy(path: str | Path) -> Any:
    try:
        return np.load(path, allow_pickle=True).item()
    except NotImplementedError:
        original_win = pathlib.WindowsPath
        original_pure = pathlib.PureWindowsPath
        pathlib.WindowsPath = pathlib.PosixPath
        pathlib.PureWindowsPath = pathlib.PurePosixPath
        try:
            return np.load(path, allow_pickle=True).item()
        finally:
            pathlib.WindowsPath = original_win
            pathlib.PureWindowsPath = original_pure


def _find_suite2p_file(plane_dir: str | Path, key: str) -> Path | None:
    path = Path(plane_dir) / f"{key}.npy"
    if path.exists():
        return path
    hits = sorted(Path(plane_dir).glob(f"*_{key}.npy"))
    return hits[0] if hits else None


def _plane_num_from_name(name: str) -> int | None:
    match = re.search(r"plane(\d+)", str(name))
    return int(match.group(1)) if match else None


def _build_labels_from_stat(stat: np.ndarray, iscell: np.ndarray, ops: dict[str, Any]) -> tuple[np.ndarray, np.ndarray]:
    ly = int(ops.get("Ly", 0))
    lx = int(ops.get("Lx", 0))
    labels = np.zeros((ly, lx), dtype=np.uint32)
    keep = np.asarray(iscell)[:, 0].astype(bool)
    for roi_idx in np.where(keep)[0]:
        roi = stat[roi_idx]
        if isinstance(roi, dict):
            ypix = np.asarray(roi.get("ypix", []), dtype=np.int64)
            xpix = np.asarray(roi.get("xpix", []), dtype=np.int64)
            overlap = roi.get("overlap", None)
        else:
            ypix = np.asarray(roi["ypix"], dtype=np.int64)
            xpix = np.asarray(roi["xpix"], dtype=np.int64)
            dtype_names = getattr(getattr(roi, "dtype", None), "names", None)
            overlap = roi["overlap"] if dtype_names and "overlap" in dtype_names else None
        if overlap is not None:
            valid = ~np.asarray(overlap, dtype=bool)
            ypix = ypix[valid]
            xpix = xpix[valid]
        if ypix.size and xpix.size:
            labels[ypix, xpix] = roi_idx + 1
    return labels, keep


def _load_suite2p_map(
    suite2p_root: str | Path,
    polarity: str | None,
    dfof_baseline_pct: float = 10.0,
    dfof_eps: float = 1e-6,
) -> tuple[dict[int, dict[str, Any]], pd.DataFrame, float | None]:
    root = Path(suite2p_root)
    plane_dirs = [path for path in root.glob("plane*") if path.is_dir()]
    plane_dirs = sorted(plane_dirs, key=lambda p: (_plane_num_from_name(p.name) if _plane_num_from_name(p.name) is not None else p.name))
    s2p_map: dict[int, dict[str, Any]] = {}
    roi_rows: list[pd.DataFrame] = []
    fps_vals: list[float] = []
    for pd_i, plane_dir in enumerate(plane_dirs):
        plane_num = _plane_num_from_name(plane_dir.name)
        if plane_num is None:
            plane_num = int(pd_i)
        paths = {key: _find_suite2p_file(plane_dir, key) for key in ("F", "Fneu", "spks", "stat", "ops", "iscell")}
        if any(path is None for path in paths.values()):
            continue
        f_raw = np.load(paths["F"], allow_pickle=True)
        fneu = np.load(paths["Fneu"], allow_pickle=True)
        spks = np.load(paths["spks"], allow_pickle=True)
        stat = np.load(paths["stat"], allow_pickle=True)
        ops = _load_ops_npy(paths["ops"])
        iscell = np.load(paths["iscell"], allow_pickle=True)
        labels, keep = _build_labels_from_stat(stat, iscell, ops)
        labels = apply_func_orientation(labels, polarity=polarity, flip_x=True)
        try:
            props = regionprops_table(labels.astype(np.int32, copy=False), properties=("label", "centroid"))
            rdf = pd.DataFrame(props)
            if not rdf.empty:
                rdf = rdf[rdf["label"] != 0].copy()
                if not rdf.empty:
                    rdf = rdf.rename(columns={"label": "func_label", "centroid-0": "y", "centroid-1": "x"})
                    rdf["plane"] = int(plane_num)
                    roi_rows.append(rdf[["plane", "func_label", "x", "y"]])
        except Exception:
            pass
        f_raw = np.asarray(f_raw, dtype=np.float32)
        f0 = np.percentile(f_raw, float(dfof_baseline_pct), axis=1, keepdims=True)
        dff = (f_raw - f0) / (f0 + float(dfof_eps))
        fs = ops.get("fs", None)
        if fs is not None:
            try:
                fps_vals.append(float(fs))
            except Exception:
                pass
        s2p_map[int(plane_num)] = {
            "plane_num": int(plane_num),
            "plane_dir": plane_dir,
            "labels": labels,
            "iscell_keep": keep,
            "F": f_raw,
            "Fneu": fneu,
            "spks": spks,
            "stat": stat,
            "ops": ops,
            "iscell": iscell,
            "dff": dff,
            "flip_x": False,
            "func_orient": "rot180+flipX" if str(polarity).lower() == "north" else "flipX",
        }
    roi_df = pd.concat(roi_rows, ignore_index=True) if roi_rows else pd.DataFrame(columns=["plane", "func_label", "x", "y"])
    fps = float(fps_vals[0]) if fps_vals else None
    return s2p_map, roi_df, fps


def _prepare_pairs_for_analysis(
    pairs_df: pd.DataFrame,
    *,
    use_high_conf_only: bool,
    low_conf_columns: tuple[str, ...],
) -> pd.DataFrame:
    req_cols = ["gene", "conf_mask", "conf_label", "anat_label", "func_label", "plane"]
    missing = [column for column in req_cols if column not in pairs_df.columns]
    if missing:
        raise RuntimeError(f"pairs mapping missing required columns: {missing}")
    out = pairs_df.copy()
    if "is_selected_for_analysis" in out.columns:
        out = out[_as_bool_series(out["is_selected_for_analysis"])].copy()
    if use_high_conf_only:
        used_any = False
        for column in low_conf_columns:
            if column in out.columns:
                used_any = True
                out = out[~_as_bool_series(out[column])].copy()
        if not used_any:
            print("[pairs] USE_HIGH_CONF_ONLY=True but no low-confidence columns found; using full analysis mapping")
    out["gene"] = out["gene"].astype(str)
    for column in ("conf_label", "anat_label", "func_label", "plane"):
        out[column] = pd.to_numeric(out[column], errors="coerce")
    out = out[out[["gene", "conf_label", "anat_label", "func_label", "plane"]].notna().all(axis=1)].copy()
    out["conf_label"] = out["conf_label"].astype(int)
    out["anat_label"] = out["anat_label"].astype(int)
    out["func_label"] = out["func_label"].astype(int)
    out["plane"] = out["plane"].astype(int)
    out["_sort_dist_func"] = pd.to_numeric(out.get("dist_func_anat_um", np.nan), errors="coerce").fillna(np.inf)
    out["_sort_overlap"] = pd.to_numeric(out.get("overlap_px_func_anat", out.get("overlap_px", np.nan)), errors="coerce").fillna(0)
    out["_sort_dist_conf"] = pd.to_numeric(out.get("dist_conf_anat_um", np.nan), errors="coerce").fillna(np.inf)
    out = out.sort_values(
        ["gene", "anat_label", "_sort_dist_func", "_sort_overlap", "_sort_dist_conf", "plane", "func_label"],
        ascending=[True, True, True, False, True, True, True],
    )
    out = out.drop_duplicates(subset=["gene", "anat_label"], keep="first")
    out = out.drop(columns=["_sort_dist_func", "_sort_overlap", "_sort_dist_conf"], errors="ignore")
    out = out.drop_duplicates(subset=["plane", "func_label", "gene"], keep="first")
    return out.reset_index(drop=True)


def _load_midline_bundle(path: str | Path) -> dict[str, Any] | None:
    target = Path(path)
    if not target.exists():
        return None
    try:
        return json.loads(target.read_text())
    except Exception:
        return None


def _annotate_midline_side(df_in: pd.DataFrame, midline_bundle: dict[str, Any] | None) -> pd.DataFrame:
    out = df_in.copy()
    if midline_bundle is None or not isinstance(midline_bundle, dict):
        out["midline_signed_dist_px"] = np.nan
        out["midline_side"] = "unknown"
        out["midline_uncertain"] = False
        return out
    per_plane = midline_bundle.get("per_plane", {})
    if not isinstance(per_plane, dict) or not per_plane:
        out["midline_signed_dist_px"] = np.nan
        out["midline_side"] = "unknown"
        out["midline_uncertain"] = False
        return out
    sides = midline_bundle.get("side_labels", {}) if isinstance(midline_bundle, dict) else {}
    pos_label = str(sides.get("positive", "right")).strip().lower()
    if pos_label not in {"left", "right"}:
        pos_label = "right"
    neg_label = "left" if pos_label == "right" else "right"
    try:
        band = float(midline_bundle.get("manual", {}).get("uncertain_band_px", 0.0))
    except Exception:
        band = 0.0
    dists = np.full(len(out), np.nan, dtype=float)
    for plane_idx, idx in out.groupby("plane").groups.items():
        pdata = per_plane.get(int(plane_idx), per_plane.get(str(plane_idx), None))
        if not isinstance(pdata, dict):
            continue
        try:
            x0 = float(pdata.get("x0", np.nan))
            y0 = float(pdata.get("y0", np.nan))
            th = np.deg2rad(float(pdata.get("theta_deg", np.nan)))
        except Exception:
            continue
        if not (np.isfinite(x0) and np.isfinite(y0) and np.isfinite(th)):
            continue
        nx = -np.sin(th)
        ny = np.cos(th)
        xv = pd.to_numeric(out.loc[idx, "x"], errors="coerce").to_numpy(dtype=float)
        yv = pd.to_numeric(out.loc[idx, "y"], errors="coerce").to_numpy(dtype=float)
        dists[idx] = (xv - x0) * nx + (yv - y0) * ny
    side = np.full(len(out), "unknown", dtype=object)
    finite = np.isfinite(dists)
    side[(finite) & (dists > band)] = pos_label
    side[(finite) & (dists < -band)] = neg_label
    side[(finite) & (np.abs(dists) <= band)] = "midline"
    out["midline_signed_dist_px"] = dists
    out["midline_side"] = side
    out["midline_uncertain"] = np.abs(dists) <= band
    return out


def _build_prestim_baseline_windows(df_evt: pd.DataFrame, fps: float, onset_delay_sec: float) -> list[tuple[int, int]]:
    if df_evt is None or getattr(df_evt, "empty", True):
        raise RuntimeError("df_evt is empty")
    evt = df_evt[["event", "time"]].copy()
    evt["event"] = evt["event"].astype(str).str.strip()
    evt["time"] = pd.to_numeric(evt["time"], errors="coerce")
    evt = evt.dropna(subset=["event", "time"]).sort_values("time").reset_index(drop=True)
    stim_starts: dict[tuple[str, int], float] = {}
    for event, t in evt[["event", "time"]].itertuples(index=False):
        match = re.match(r"^(B\d+)_stim(\d+)_.+$", str(event))
        if not match:
            continue
        key = (match.group(1), int(match.group(2)))
        if key not in stim_starts:
            stim_starts[key] = float(t)
    windows: list[tuple[int, int]] = []
    for event, t_pre in evt[["event", "time"]].itertuples(index=False):
        match = re.match(r"^(B\d+)_prestim(\d+)_pause$", str(event))
        if not match:
            continue
        key = (match.group(1), int(match.group(2)))
        t_stim = stim_starts.get(key)
        if t_stim is None:
            continue
        t0 = float(t_pre)
        t1 = float(t_stim) + float(onset_delay_sec)
        if not np.isfinite(t0) or not np.isfinite(t1) or t1 <= t0:
            continue
        idx0 = int(round(t0 * float(fps)))
        idx1 = int(round(t1 * float(fps)))
        if idx1 > idx0:
            windows.append((idx0, idx1))
    if not windows:
        raise RuntimeError("no prestim baseline windows found")
    windows = sorted(windows, key=lambda w: (w[0], w[1]))
    merged: list[list[int]] = []
    for s, e in windows:
        if not merged or s > merged[-1][1]:
            merged.append([s, e])
        else:
            merged[-1][1] = max(merged[-1][1], e)
    return [(int(s), int(e)) for s, e in merged if e > s]


def _compute_zscore_stats(
    dff: np.ndarray,
    baseline_windows: list[tuple[int, int]],
    *,
    min_points: int = 200,
    sigma_eps: float = 1e-6,
) -> dict[str, np.ndarray]:
    n_roi = int(dff.shape[0])
    n_frames = int(dff.shape[1])
    mask = np.zeros(n_frames, dtype=bool)
    for idx0, idx1 in baseline_windows:
        s = max(0, int(idx0))
        e = min(n_frames, int(idx1))
        if e > s:
            mask[s:e] = True
    mu = np.full(n_roi, np.nan, dtype=np.float32)
    sigma = np.full(n_roi, np.nan, dtype=np.float32)
    n_valid = np.zeros(n_roi, dtype=np.int32)
    if mask.any():
        base = dff[:, mask]
        n_valid = np.isfinite(base).sum(axis=1).astype(np.int32)
        with np.errstate(invalid="ignore", divide="ignore"):
            mu = np.nanmean(base, axis=1).astype(np.float32, copy=False)
            sigma = np.nanstd(base, axis=1).astype(np.float32, copy=False)
    has_points = n_valid >= int(min_points)
    has_sigma = np.isfinite(sigma) & (sigma > float(sigma_eps))
    has_mu = np.isfinite(mu)
    valid = has_points & has_sigma & has_mu
    return {"mu": mu, "sigma": sigma, "n_valid": n_valid, "valid": valid}


def _extract_window(
    trace: np.ndarray,
    idx0: int,
    idx1: int,
    *,
    mode: str = "pad_nan",
    min_valid_points: int = 1,
) -> np.ndarray | None:
    n_frames = int(trace.shape[0])
    win_len = int(idx1 - idx0)
    if win_len <= 0:
        return None
    if mode == "strict":
        if idx0 < 0 or idx1 > n_frames:
            return None
        return trace[idx0:idx1]
    seg = np.full(win_len, np.nan, dtype=np.float32)
    src0 = max(int(idx0), 0)
    src1 = min(int(idx1), n_frames)
    if src1 <= src0:
        return None
    dst0 = src0 - int(idx0)
    seg[dst0 : dst0 + (src1 - src0)] = trace[src0:src1]
    if (src1 - src0) < max(1, int(min_valid_points)):
        return None
    return seg


def _extract_bpi_window(
    trace: np.ndarray,
    idx0: int,
    idx1: int,
    *,
    mode: str = "pad_nan",
    min_valid_frac: float = 0.5,
) -> tuple[np.ndarray | None, int, int]:
    n_frames = int(trace.shape[0])
    idx0 = int(idx0)
    idx1 = int(idx1)
    win_len = int(idx1 - idx0)
    if win_len <= 0:
        return None, 0, 0
    if mode == "strict":
        if idx0 < 0 or idx1 > n_frames:
            return None, 0, win_len
        seg = trace[idx0:idx1]
        n_valid = int(np.isfinite(seg).sum())
        return seg, n_valid, win_len
    seg = np.full(win_len, np.nan, dtype=np.float32)
    src0 = max(idx0, 0)
    src1 = min(idx1, n_frames)
    if src1 <= src0:
        return None, 0, win_len
    dst0 = src0 - idx0
    seg[dst0 : dst0 + (src1 - src0)] = trace[src0:src1]
    n_valid = int(np.isfinite(seg).sum())
    if n_valid < int(np.ceil(float(min_valid_frac) * win_len)):
        return None, n_valid, win_len
    return seg, n_valid, win_len


def _parse_stim_components(stype: str) -> list[tuple[str, str]] | None:
    parts = [part.strip().upper() for part in str(stype).split("+") if str(part).strip()]
    if not parts:
        return None
    comps: list[tuple[str, str]] = []
    seen_sides: set[str] = set()
    for part in parts:
        match = re.match(r"^([LR])(LB|LC)$", part)
        if not match:
            return None
        side, mode = match.group(1), match.group(2)
        if side in seen_sides:
            return None
        seen_sides.add(side)
        comps.append((side, mode))
    return comps


def _classify_stim_type(stype: str) -> str | None:
    parts = [part.strip().upper() for part in str(stype).split("+") if str(part).strip()]
    if not parts:
        return None
    tags: list[str] = []
    for part in parts:
        if part.endswith("LB"):
            tags.append("B")
        elif part.endswith("LC"):
            tags.append("C")
        else:
            return None
    if all(tag == "B" for tag in tags):
        return "bout"
    if all(tag == "C" for tag in tags):
        return "continuous"
    return "mixed"


def _combine_segments(arr: np.ndarray) -> tuple[np.ndarray, np.ndarray | None]:
    arr = np.asarray(arr, dtype=np.float32)
    n_valid = np.isfinite(arr).sum(axis=0).astype(np.float32)
    mean = np.divide(
        np.nansum(arr, axis=0),
        n_valid,
        out=np.full(arr.shape[1], np.nan, dtype=np.float32),
        where=n_valid > 0,
    )
    sem = None
    if arr.shape[0] > 1:
        centered = arr - mean[np.newaxis, :]
        centered[~np.isfinite(arr)] = np.nan
        var = np.divide(
            np.nansum(centered * centered, axis=0),
            n_valid - 1.0,
            out=np.full(arr.shape[1], np.nan, dtype=np.float32),
            where=n_valid > 1,
        )
        sem = np.divide(
            np.sqrt(var),
            np.sqrt(n_valid),
            out=np.full(arr.shape[1], np.nan, dtype=np.float32),
            where=n_valid > 1,
        )
    return mean, sem


def _resample_segment_to_grid(seg: np.ndarray, fps: float, t_target: np.ndarray, window_pre: float) -> np.ndarray | None:
    seg = np.asarray(seg, dtype=np.float32)
    t_local = (np.arange(seg.size, dtype=np.float32) / float(fps)) - float(window_pre)
    valid = np.isfinite(seg)
    if int(valid.sum()) < 2:
        return None
    try:
        y = np.interp(
            t_target.astype(np.float64),
            t_local[valid].astype(np.float64),
            seg[valid].astype(np.float64),
            left=np.nan,
            right=np.nan,
        )
        return y.astype(np.float32)
    except Exception:
        return None


def _resolve_environment(cfg: CohortBuildConfig) -> dict[str, Any]:
    data_mode = str(cfg.data_mode).strip().lower()
    data_mode = "local" if data_mode == "local" else "nas"
    nas_root = _default_nas_root()
    local_root = _default_local_root()
    data_root = local_root if data_mode == "local" else nas_root
    matching_metadata_csv = (
        Path(cfg.matching_metadata_csv_override)
        if cfg.matching_metadata_csv_override
        else (data_root / "matchingMetadata.csv" if data_mode == "local" else nas_root / "Danin" / "matchingMetadata.csv")
    )
    cohort_outdir_default = (
        data_root / "cohort_outputs" / "multi_fish_56h_56g"
        if data_mode == "local"
        else Path.cwd() / "cohort_outputs" / "multi_fish_56h_56g"
    )
    cohort_outdir = Path(cfg.cohort_outdir_override) if cfg.cohort_outdir_override else cohort_outdir_default
    cohort_outdir.mkdir(parents=True, exist_ok=True)
    panel_grid = [[a, b] for a, b in cfg.panel_grid]
    plot_order = [p for row in panel_grid for p in row]
    return {
        "DATA_MODE": data_mode,
        "NAS_ROOT": nas_root,
        "LOCAL_ROOT": local_root,
        "DATA_ROOT": data_root,
        "MATCHING_METADATA_CSV": matching_metadata_csv,
        "FISH_SPECS": [dict(spec) for spec in cfg.fish_specs],
        "WINDOW_PRE_SEC": float(cfg.window_pre_sec),
        "WINDOW_POST_SEC": float(cfg.window_post_sec),
        "STIM_ONSET_DELAY_SEC": float(cfg.stim_onset_delay_sec),
        "WINDOW_EDGE_POLICY": str(cfg.window_edge_policy),
        "MIN_VALID_POINTS_PER_SEG": int(cfg.min_valid_points_per_seg),
        "BPI_EDGE_POLICY": str(cfg.bpi_edge_policy),
        "BPI_MIN_VALID_FRAC": float(cfg.bpi_min_valid_frac),
        "BPI_MIN_TRIALS_PER_CLASS": int(cfg.bpi_min_trials_per_class),
        "BPI_DENOM_EPS": float(cfg.bpi_denom_eps),
        "ZSCORE_MIN_BASELINE_POINTS": int(cfg.zscore_min_baseline_points),
        "ZSCORE_MIN_BASELINE_STD": float(cfg.zscore_min_baseline_std),
        "STIM_TIME_SCALE": float(cfg.stim_time_scale),
        "MEASURE_START_BLOCK": cfg.measure_start_block,
        "MEASURE_START_EVENT": str(cfg.measure_start_event),
        "REMOVE_INTERBLOCK_GAPS": bool(cfg.remove_interblock_gaps),
        "MIN_SEGMENTS": int(cfg.min_segments),
        "MIN_CELLS": int(cfg.min_cells),
        "MAX_GENES": int(cfg.max_genes),
        "USE_SEM": bool(cfg.use_sem),
        "COHORT_TRACE_DT": cfg.cohort_trace_dt,
        "USE_HIGH_CONF_ONLY": bool(cfg.use_high_conf_only),
        "LOW_CONF_COLUMNS": list(cfg.low_conf_columns),
        "DEFAULT_GENE_ORDER": list(cfg.default_gene_order),
        "DEFAULT_GENE_COLORS": dict(cfg.default_gene_colors),
        "GENE_ORDER": list(cfg.default_gene_order),
        "GENE_COLORS": dict(cfg.default_gene_colors),
        "PANEL_GRID": panel_grid,
        "PLOT_ORDER": plot_order,
        "PLOT_TITLES": dict(cfg.plot_titles),
        "COHORT_OUTDIR_DEFAULT": cohort_outdir_default,
        "COHORT_OUTDIR": cohort_outdir,
        "SKIP_COHORT_BUILD_IF_CACHED": bool(cfg.skip_build_if_cached),
        "FORCE_COHORT_BUILD": bool(cfg.force_build),
        "FAIL_ON_SEGMENT_DUPLICATION": bool(cfg.fail_on_segment_duplication),
        "TRACE_DEDUP_KEYS": list(cfg.trace_dedup_keys),
    }


def resolve_cohort_context_stage(config: CohortBuildConfig | None = None) -> dict[str, Any]:
    """Resolve notebook-facing cohort environment bindings for [cfg]."""
    cfg = config or CohortBuildConfig()
    bindings = _resolve_environment(cfg)
    return {"bindings": bindings, "config": asdict(cfg)}


def load_cohort_analysis_state(
    config: CohortBuildConfig | None = None,
    *,
    outdir: str | Path | None = None,
    load_tables: bool = True,
    load_trace_cache: bool = True,
    load_cohort_53a_tables: bool = True,
    verbose: bool = True,
) -> dict[str, Any]:
    """Load cohort cache payload while always publishing stable cohort env bindings."""
    cfg = config or CohortBuildConfig()
    env = _resolve_environment(cfg)
    target_outdir = Path(outdir) if outdir is not None else Path(env["COHORT_OUTDIR"])
    loaded_state = load_cohort_outputs_from_disk(
        target_outdir,
        load_tables=load_tables,
        load_trace_cache=load_trace_cache,
        load_cohort_53a_tables=load_cohort_53a_tables,
        verbose=verbose,
    )
    bindings = dict(env)
    bindings["COHORT_OUTDIR"] = target_outdir
    bindings.update(
        {
            "cohort_fish_summary_df": loaded_state.get("cohort_fish_summary_df", pd.DataFrame()),
            "cohort_stim_summary_df": loaded_state.get("cohort_stim_summary_df", pd.DataFrame()),
            "cohort_bpi_trials_df": loaded_state.get("cohort_bpi_trials_df", pd.DataFrame()),
            "cohort_bpi_cells_df": loaded_state.get("cohort_bpi_cells_df", pd.DataFrame()),
            "cohort_tvec": loaded_state.get("cohort_tvec", np.asarray([], dtype=np.float32)),
            "cohort_results_stim_ipsi_contra": loaded_state.get("cohort_results_stim_ipsi_contra", {}),
            "cohort_results_stim_ipsi_contra_by_fish": loaded_state.get("cohort_results_stim_ipsi_contra_by_fish", {}),
            "cohort_results_stim_ipsi_contra_per_cell_by_fish": loaded_state.get(
                "cohort_results_stim_ipsi_contra_per_cell_by_fish",
                {},
            ),
            "cohort_mode_durations": loaded_state.get("cohort_mode_durations", {"LB": [], "LC": []}),
            "cohort_mode_event_counts": loaded_state.get("cohort_mode_event_counts", {"LB": 0, "LC": 0}),
            "cohort_53a_tables": {
                "ncc_curves_df": loaded_state.get("ncc_curves_df", pd.DataFrame()),
                "diameters_df": loaded_state.get("diameters_df", pd.DataFrame()),
                "diameter_filter_summary_df": loaded_state.get("diameter_filter_summary_df", pd.DataFrame()),
                "func_anat_offsets_df": loaded_state.get("func_anat_offsets_df", pd.DataFrame()),
                "hcr_offsets_df": loaded_state.get("hcr_offsets_df", pd.DataFrame()),
                "thresholds_df": loaded_state.get("thresholds_df", pd.DataFrame()),
            },
        }
    )
    return {"bindings": bindings, "state": loaded_state, "config": asdict(cfg)}


def build_cohort_outputs_stage(config: CohortBuildConfig | None = None) -> dict[str, Any]:
    cfg = config or CohortBuildConfig()
    env = _resolve_environment(cfg)
    plot_order = list(env["PLOT_ORDER"])
    md_cache = _load_matching_metadata(env["MATCHING_METADATA_CSV"])
    cached_build_skipped = False
    cohort_outdir = env["COHORT_OUTDIR"]

    if env["SKIP_COHORT_BUILD_IF_CACHED"] and (not env["FORCE_COHORT_BUILD"]):
        cached_state = load_cohort_outputs_from_disk(
            cohort_outdir,
            load_tables=True,
            load_trace_cache=True,
            load_cohort_53a_tables=True,
            verbose=cfg.verbose,
        )
        cached_tvec = cached_state.get("cohort_tvec", None)
        cached_results = cached_state.get("cohort_results_stim_ipsi_contra", None)
        cached_results_by_fish = cached_state.get("cohort_results_stim_ipsi_contra_by_fish", None)
        cached_results_per_cell_by_fish = cached_state.get("cohort_results_stim_ipsi_contra_per_cell_by_fish", None)
        cached_bpi_df = cached_state.get("cohort_bpi_cells_df", None)
        has_tvec = cached_tvec is not None and (len(np.asarray(cached_tvec)) > 0)
        has_results = isinstance(cached_results, dict) and (count_trace_genes(cached_results, plot_order) > 0)
        has_results_by_fish = isinstance(cached_results_by_fish, dict) and (count_trace_genes(cached_results_by_fish, plot_order, nested=True) > 0)
        has_results_per_cell = isinstance(cached_results_per_cell_by_fish, dict) and (
            count_trace_genes(cached_results_per_cell_by_fish, plot_order, nested=True) > 0
        )
        has_bpi = isinstance(cached_bpi_df, pd.DataFrame) and (not cached_bpi_df.empty)
        cache_paths = cohort_cache_paths(cohort_outdir)
        required_53a_keys = [
            "cohort_53a_ncc_curves_csv",
            "cohort_53a_diameters_csv",
            "cohort_53a_diameter_filter_summary_csv",
            "cohort_53a_func_anat_offsets_csv",
            "cohort_53a_hcr_offsets_csv",
            "cohort_53a_thresholds_csv",
        ]
        has_53a_tables = all(cache_paths[k].exists() for k in required_53a_keys)
        cache_has_segment_outliers = False
        if isinstance(cached_results_per_cell_by_fish, dict):
            for fish_id, panel_map in cached_results_per_cell_by_fish.items():
                if not isinstance(panel_map, dict):
                    continue
                for panel, gene_map in panel_map.items():
                    seg_counts: list[int] = []
                    if not isinstance(gene_map, dict):
                        continue
                    for roi_map in gene_map.values():
                        if not isinstance(roi_map, dict):
                            continue
                        for cell_res in roi_map.values():
                            nseg = pd.to_numeric(pd.Series([cell_res.get("n_segments", np.nan)]), errors="coerce").iloc[0]
                            if pd.notna(nseg) and float(nseg) > 0:
                                seg_counts.append(int(round(float(nseg))))
                    if seg_counts:
                        seg_counts_s = pd.Series(seg_counts, dtype="int64")
                        mode = int(seg_counts_s.mode().iloc[0])
                        if bool((seg_counts_s > mode).any()):
                            cache_has_segment_outliers = True
                            if cfg.verbose:
                                print(
                                    f"[cohort-build] stale cache detected for fish={fish_id} panel={panel}: "
                                    f"segment counts exceed mode {mode}; forcing rebuild."
                                )
                            break
                if cache_has_segment_outliers:
                    break
        if (
            has_tvec
            and has_bpi
            and (not (has_results and has_results_by_fish and has_results_per_cell))
            and isinstance(cached_state.get("cohort_stim_summary_df", None), pd.DataFrame)
            and (not cached_state.get("cohort_stim_summary_df").empty)
        ):
            if cfg.verbose:
                print("[cohort-build] Trace cache is stale/incomplete; rebuilding cohort outputs.")
        if has_tvec and has_results and has_results_by_fish and has_results_per_cell and has_bpi and has_53a_tables and (not cache_has_segment_outliers):
            bindings = dict(env)
            bindings.update(
                {
                    "cohort_fish_summary_df": cached_state.get("cohort_fish_summary_df", pd.DataFrame()),
                    "cohort_stim_summary_df": cached_state.get("cohort_stim_summary_df", pd.DataFrame()),
                    "cohort_bpi_trials_df": cached_state.get("cohort_bpi_trials_df", pd.DataFrame()),
                    "cohort_bpi_cells_df": cached_bpi_df,
                    "cohort_tvec": cached_tvec,
                    "cohort_results_stim_ipsi_contra": cached_results,
                    "cohort_results_stim_ipsi_contra_by_fish": cached_results_by_fish,
                    "cohort_results_stim_ipsi_contra_per_cell_by_fish": cached_results_per_cell_by_fish,
                    "cohort_mode_durations": cached_state.get("cohort_mode_durations", {"LB": [], "LC": []}),
                    "cohort_mode_event_counts": cached_state.get("cohort_mode_event_counts", {"LB": 0, "LC": 0}),
                }
            )
            cached_build_skipped = True
            if cfg.verbose:
                print("[cohort-build] Reusing cached cohort outputs from disk; skipping fish rebuild.")
            return {
                "bindings": bindings,
                "state": {"cohort_build_skipped": cached_build_skipped},
                "config": asdict(cfg),
            }

    panel_acc = {panel: {} for panel in plot_order}
    panel_acc_by_fish: dict[str, dict[str, dict[str, Any]]] = {}
    panel_cell_acc_by_fish: dict[str, dict[str, dict[str, Any]]] = {}
    all_trial_rows: list[dict[str, Any]] = []
    fish_rows: list[dict[str, Any]] = []
    mode_durations_cohort = {"LB": [], "LC": []}
    mode_event_counts = {"LB": 0, "LC": 0}
    cohort_tvec = None
    cohort_dt = None if env["COHORT_TRACE_DT"] is None else float(env["COHORT_TRACE_DT"])

    for spec in env["FISH_SPECS"]:
        owner = str(spec["owner"])
        fish_id = str(spec["fish_id"])
        paths = _fish_paths(env["DATA_ROOT"], owner, fish_id, data_mode=env["DATA_MODE"])
        fish_dir = paths["fish_dir"]
        conf_csv = paths["conf_csv"]
        hcr_status_csv = paths["out_reg"] / "hcr_activity_status.csv"
        midline_json = paths["midline_json"]
        suite2p_root = paths["suite2p_root"]
        metadata_dir = paths["metadata_dir"]
        row_status = {
            "owner": owner,
            "fish_id": fish_id,
            "fish_dir": str(fish_dir),
            "ok": False,
            "n_pairs_in": 0,
            "n_pairs_used": 0,
            "n_pairs_with_side": 0,
            "n_trial_rows": 0,
            "n_trace_segments": 0,
            "fps": np.nan,
            "notes": "",
        }
        if not fish_dir.exists():
            row_status["notes"] = "fish_dir missing"
            fish_rows.append(row_status)
            if cfg.verbose:
                print(f"[cohort] {fish_id}: fish_dir missing -> skip")
            continue
        if not conf_csv.exists():
            row_status["notes"] = "conf_to_func_pairs.csv missing"
            fish_rows.append(row_status)
            if cfg.verbose:
                print(f"[cohort] {fish_id}: missing {conf_csv} -> skip")
            continue
        if not suite2p_root.exists():
            row_status["notes"] = "suite2P dir missing"
            fish_rows.append(row_status)
            if cfg.verbose:
                print(f"[cohort] {fish_id}: missing {suite2p_root} -> skip")
            continue
        log_csv = _find_experiment_log(metadata_dir, fish_id)
        if log_csv is None or not Path(log_csv).exists():
            row_status["notes"] = "experiment log missing"
            fish_rows.append(row_status)
            if cfg.verbose:
                print(f"[cohort] {fish_id}: experiment log missing -> skip")
            continue
        polarity = _get_polarity(fish_id, md_cache)
        try:
            df_evt = _load_events_df(log_csv, time_scale=env["STIM_TIME_SCALE"])
            df_evt, df_stim, _ = _build_stim_table(
                df_evt,
                measure_start_block=env["MEASURE_START_BLOCK"],
                measure_start_event=env["MEASURE_START_EVENT"],
                remove_interblock_gaps=env["REMOVE_INTERBLOCK_GAPS"],
            )
        except Exception as exc:
            row_status["notes"] = f"stim parsing failed: {exc}"
            fish_rows.append(row_status)
            if cfg.verbose:
                print(f"[cohort] {fish_id}: stim parsing failed -> {exc}")
            continue
        if df_stim.empty:
            row_status["notes"] = "df_stim empty"
            fish_rows.append(row_status)
            if cfg.verbose:
                print(f"[cohort] {fish_id}: df_stim empty -> skip")
            continue
        try:
            s2p_map, roi_df, fps = _load_suite2p_map(suite2p_root, polarity)
        except Exception as exc:
            row_status["notes"] = f"suite2p load failed: {exc}"
            fish_rows.append(row_status)
            if cfg.verbose:
                print(f"[cohort] {fish_id}: suite2p load failed -> {exc}")
            continue
        if not s2p_map:
            row_status["notes"] = "no suite2p planes"
            fish_rows.append(row_status)
            if cfg.verbose:
                print(f"[cohort] {fish_id}: no suite2p planes -> skip")
            continue
        if fps is None or not np.isfinite(fps) or fps <= 0:
            row_status["notes"] = "invalid fps"
            fish_rows.append(row_status)
            if cfg.verbose:
                print(f"[cohort] {fish_id}: invalid fps -> skip")
            continue
        row_status["fps"] = float(fps)
        if cohort_tvec is None:
            if cohort_dt is None:
                cohort_dt = 1.0 / float(fps)
            n_pts = int(round((env["WINDOW_PRE_SEC"] + env["WINDOW_POST_SEC"]) / float(cohort_dt))) + 1
            cohort_tvec = np.linspace(-env["WINDOW_PRE_SEC"], env["WINDOW_POST_SEC"], n_pts, dtype=np.float32)
            if cfg.verbose:
                print(f"[cohort] cohort_tvec initialized: n={n_pts}, dt={cohort_dt:.6f}s")
        try:
            pairs_raw = pd.read_csv(conf_csv)
        except Exception as exc:
            row_status["notes"] = f"failed reading conf csv: {exc}"
            fish_rows.append(row_status)
            if cfg.verbose:
                print(f"[cohort] {fish_id}: failed reading conf csv -> {exc}")
            continue
        if "fish_id" in pairs_raw.columns:
            pairs_raw = pairs_raw[pairs_raw["fish_id"].astype(str) == fish_id].copy()
        row_status["n_pairs_in"] = int(len(pairs_raw))
        try:
            pairs = _prepare_pairs_for_analysis(
                pairs_raw,
                use_high_conf_only=env["USE_HIGH_CONF_ONLY"],
                low_conf_columns=tuple(env["LOW_CONF_COLUMNS"]),
            )
        except Exception as exc:
            row_status["notes"] = f"pair prep failed: {exc}"
            fish_rows.append(row_status)
            if cfg.verbose:
                print(f"[cohort] {fish_id}: pair prep failed -> {exc}")
            continue
        if pairs.empty:
            row_status["notes"] = "no valid analysis pairs"
            fish_rows.append(row_status)
            if cfg.verbose:
                print(f"[cohort] {fish_id}: no valid analysis pairs -> skip")
            continue
        pairs = pairs[pairs["plane"].isin(list(s2p_map.keys()))].copy()
        if pairs.empty:
            row_status["notes"] = "no pairs in loaded suite2p planes"
            fish_rows.append(row_status)
            if cfg.verbose:
                print(f"[cohort] {fish_id}: no pairs in loaded planes -> skip")
            continue
        pairs = pairs.merge(roi_df, on=["plane", "func_label"], how="left")
        midline_bundle = _load_midline_bundle(midline_json)
        pairs = _annotate_midline_side(pairs, midline_bundle)
        pairs["midline_side"] = pairs["midline_side"].astype(str).str.strip().str.lower()
        valid_side = pairs["midline_side"].isin({"left", "right"})
        pairs_side = pairs[valid_side].copy()
        pairs_side["is_low_activity"] = False
        row_status["n_pairs_used"] = int(len(pairs))
        row_status["n_pairs_with_side"] = int(len(pairs_side))
        if pairs_side.empty:
            row_status["notes"] = "no pairs with valid midline side"
            fish_rows.append(row_status)
            if cfg.verbose:
                print(f"[cohort] {fish_id}: no pairs with valid midline side -> skip")
            continue
        try:
            baseline_windows = _build_prestim_baseline_windows(df_evt, fps, env["STIM_ONSET_DELAY_SEC"])
        except Exception as exc:
            row_status["notes"] = f"baseline windows failed: {exc}"
            fish_rows.append(row_status)
            if cfg.verbose:
                print(f"[cohort] {fish_id}: baseline windows failed -> {exc}")
            continue
        zstats_by_plane = {}
        for pidx, pdata in s2p_map.items():
            dff = pdata.get("dff", None)
            if dff is None:
                continue
            zstats_by_plane[int(pidx)] = _compute_zscore_stats(
                dff,
                baseline_windows,
                min_points=env["ZSCORE_MIN_BASELINE_POINTS"],
                sigma_eps=env["ZSCORE_MIN_BASELINE_STD"],
            )
        low_activity_pairs = None
        if hcr_status_csv.exists():
            try:
                status_df_low = pd.read_csv(hcr_status_csv)
                if "fish_id" in status_df_low.columns:
                    status_df_low = status_df_low[status_df_low["fish_id"].astype(str) == fish_id].copy()
                low_df = status_df_low[status_df_low["functional_status"].astype(str) == "in-plane low-activity ROI"].copy()
                if not low_df.empty:
                    low_df["plane"] = pd.to_numeric(low_df.get("selected_plane", pd.Series(np.nan, index=low_df.index)), errors="coerce").astype("Int64")
                    low_df["func_label"] = pd.to_numeric(
                        low_df.get("selected_func_label", pd.Series(np.nan, index=low_df.index)),
                        errors="coerce",
                    ).astype("Int64")
                    low_df["gene"] = low_df["gene"].astype(str).str.strip()
                    low_df = low_df[low_df[["plane", "func_label", "gene"]].notna().all(axis=1)].copy()
                    if not low_df.empty:
                        low_df = low_df[["gene", "plane", "func_label"]].copy()
                        low_df["plane"] = low_df["plane"].astype(int)
                        low_df["func_label"] = low_df["func_label"].astype(int)
                        n_low_before = int(len(low_df))
                        low_df = low_df.drop_duplicates(subset=["gene", "plane", "func_label"], keep="first").reset_index(drop=True)
                        n_low_dups = int(n_low_before - len(low_df))
                        if n_low_dups > 0 and cfg.verbose:
                            print(f"[cohort] {fish_id}: dropped {n_low_dups} duplicate low-activity ROI rows")
                        low_df["is_low_activity"] = True
                        low_activity_pairs = low_df.copy()
            except Exception as exc:
                if cfg.verbose:
                    print(f"[cohort] {fish_id}: WARNING low-activity load failed -> {exc}")
                low_activity_pairs = None
        pairs_for_traces = pairs_side.copy()
        if low_activity_pairs is not None and not low_activity_pairs.empty:
            responsive_keys = set(pairs_side[["plane", "func_label"]].apply(tuple, axis=1))
            low_activity_new = low_activity_pairs[~low_activity_pairs[["plane", "func_label"]].apply(tuple, axis=1).isin(responsive_keys)].copy()
            if not low_activity_new.empty:
                low_activity_new = low_activity_new.merge(
                    roi_df[["plane", "func_label", "x", "y"]],
                    on=["plane", "func_label"],
                    how="left",
                )
                low_activity_new = _annotate_midline_side(low_activity_new, midline_bundle)
                low_activity_new["midline_side"] = low_activity_new["midline_side"].astype(str).str.strip().str.lower()
                low_activity_new = low_activity_new[low_activity_new["midline_side"].isin({"left", "right"})].copy()
                low_activity_new = low_activity_new[["gene", "plane", "func_label", "is_low_activity", "midline_side"]].copy()
                low_activity_new = low_activity_new.drop_duplicates(subset=["gene", "plane", "func_label"], keep="first").reset_index(drop=True)
            if not low_activity_new.empty:
                pairs_for_traces = pd.concat([pairs_for_traces, low_activity_new], ignore_index=True)
        pairs_for_traces["gene"] = pairs_for_traces["gene"].astype(str).str.strip()
        n_before = int(len(pairs_for_traces))
        pairs_for_traces = pairs_for_traces.drop_duplicates(subset=env["TRACE_DEDUP_KEYS"], keep="first").reset_index(drop=True)
        n_drop = int(n_before - len(pairs_for_traces))
        if n_drop > 0 and cfg.verbose:
            print(f"[cohort] {fish_id}: dropped {n_drop} duplicate trace-candidate rows using keys={env['TRACE_DEDUP_KEYS']}")
        win_len = int(round((env["WINDOW_PRE_SEC"] + env["WINDOW_POST_SEC"]) * float(fps))) + 1
        stim_events_uni = []
        for _, r in df_stim.iterrows():
            stype = str(r.get("type", ""))
            comps = _parse_stim_components(stype)
            if comps is None or len(comps) != 1:
                continue
            side_letter, mode = comps[0]
            side_word = "left" if side_letter == "L" else "right"
            t0 = float(r.get("start", np.nan)) + float(env["STIM_ONSET_DELAY_SEC"])
            dur = float(r.get("duration", np.nan))
            if not np.isfinite(t0):
                continue
            idx0 = int(round((t0 - env["WINDOW_PRE_SEC"]) * float(fps)))
            idx1 = idx0 + win_len
            stim_events_uni.append({"side": side_word, "mode": mode, "idx0": idx0, "idx1": idx1, "dur": dur, "stim_type": stype})
            if mode in mode_durations_cohort and np.isfinite(dur):
                mode_durations_cohort[mode].append(float(dur))
                mode_event_counts[mode] += 1
        panel_acc_fish = {panel: {} for panel in plot_order}
        panel_cell_acc_fish = {panel: {} for panel in plot_order}
        if not stim_events_uni:
            row_status["notes"] = "no unilateral stim events"
            fish_rows.append(row_status)
            if cfg.verbose:
                print(f"[cohort] {fish_id}: no unilateral stim events -> skip traces")
            continue
        n_seg_fish = 0
        genes_present = sorted(pairs_for_traces["gene"].dropna().astype(str).unique().tolist())
        for gene in genes_present:
            sub = pairs_for_traces[pairs_for_traces["gene"].astype(str) == str(gene)]
            for _, pr in sub.iterrows():
                plane = int(pr["plane"])
                roi_idx = int(pr["func_label"]) - 1
                dff = s2p_map[plane].get("dff", None)
                if dff is None or roi_idx < 0 or roi_idx >= dff.shape[0]:
                    continue
                zstats = zstats_by_plane.get(plane, None)
                if zstats is None or roi_idx >= len(zstats["valid"]) or not bool(zstats["valid"][roi_idx]):
                    continue
                trace = (dff[roi_idx] - float(zstats["mu"][roi_idx])) / float(zstats["sigma"][roi_idx])
                cell_side = str(pr.get("midline_side", "unknown")).strip().lower()
                if cell_side not in {"left", "right"}:
                    continue
                roi_key = (fish_id, plane, roi_idx)
                for ev in stim_events_uni:
                    panel = ("ipsi" if ev["side"] == cell_side else "contra") + f"_{ev['mode']}"
                    if panel not in panel_acc:
                        continue
                    seg = _extract_window(
                        trace,
                        ev["idx0"],
                        ev["idx1"],
                        mode=env["WINDOW_EDGE_POLICY"],
                        min_valid_points=env["MIN_VALID_POINTS_PER_SEG"],
                    )
                    if seg is None:
                        continue
                    seg_rs = _resample_segment_to_grid(seg, fps, cohort_tvec, env["WINDOW_PRE_SEC"])
                    if seg_rs is None:
                        continue
                    gacc = panel_acc[panel].setdefault(gene, {"segs": [], "rois": set(), "source_types": []})
                    gacc["segs"].append(seg_rs)
                    gacc["rois"].add(roi_key)
                    gacc["source_types"].append(ev["stim_type"])
                    gacc_fish = panel_acc_fish[panel].setdefault(gene, {"segs": [], "rois": set(), "source_types": []})
                    gacc_fish["segs"].append(seg_rs)
                    gacc_fish["rois"].add(roi_key)
                    gacc_fish["source_types"].append(ev["stim_type"])
                    gene_cell_acc_fish = panel_cell_acc_fish[panel].setdefault(gene, {})
                    cacc_fish = gene_cell_acc_fish.setdefault(
                        roi_key,
                        {
                            "segs": [],
                            "plane": plane,
                            "roi_idx": roi_idx,
                            "func_label": int(pr["func_label"]),
                            "cell_key": str(pr.get("cell_key", f"{fish_id}|{gene}|{plane}|{int(pr['func_label'])}")),
                            "midline_side": cell_side,
                            "is_low_activity": bool(pr.get("is_low_activity", False)),
                            "source_types": [],
                        },
                    )
                    cacc_fish["segs"].append(seg_rs)
                    cacc_fish["source_types"].append(ev["stim_type"])
                    n_seg_fish += 1
        row_status["n_trace_segments"] = int(n_seg_fish)
        expected_events_per_roi = {
            side: {
                panel: int(sum(1 for ev in stim_events_uni if (("ipsi" if ev["side"] == side else "contra") + f"_{ev['mode']}") == panel))
                for panel in plot_order
            }
            for side in ("left", "right")
        }
        for panel, gene_roi_map in panel_cell_acc_fish.items():
            panel_mode = "LB" if str(panel).endswith("LB") else "LC"
            for gene, roi_map in gene_roi_map.items():
                for roi_key, cacc in roi_map.items():
                    cell_side = str(cacc.get("midline_side", "unknown")).strip().lower()
                    expected_nseg = int((expected_events_per_roi.get(cell_side, {}) or {}).get(panel, 0))
                    if expected_nseg <= 0:
                        expected_nseg = int(sum(1 for ev in stim_events_uni if str(ev.get("mode", "")) == panel_mode))
                    if expected_nseg <= 0:
                        continue
                    nseg = int(len(cacc.get("segs", [])))
                    if nseg != expected_nseg:
                        msg = (
                            f"[cohort] {fish_id}: unexpected segment count panel={panel} gene={gene} roi={roi_key} "
                            f"expected={expected_nseg} observed={nseg}"
                        )
                        if env["FAIL_ON_SEGMENT_DUPLICATION"]:
                            raise RuntimeError(msg)
                        if cfg.verbose:
                            print(msg)
        stim_events_bpi = []
        for _, r in df_stim.iterrows():
            stype = str(r.get("type", ""))
            sclass = _classify_stim_type(stype)
            if sclass not in {"bout", "continuous"}:
                continue
            t0 = float(r.get("start", np.nan)) + float(env["STIM_ONSET_DELAY_SEC"])
            dur = float(r.get("duration", np.nan))
            if not np.isfinite(t0) or not np.isfinite(dur) or dur <= 0:
                continue
            idx0 = int(round(t0 * float(fps)))
            idx1 = int(round((t0 + dur) * float(fps)))
            if idx1 <= idx0:
                continue
            stim_events_bpi.append(
                {
                    "block": r.get("block", np.nan),
                    "stim_idx": int(r.get("stim_idx", -1)) if pd.notna(r.get("stim_idx", np.nan)) else -1,
                    "stim_type": stype,
                    "stim_class": sclass,
                    "start_s": t0,
                    "duration_s": dur,
                    "idx0": idx0,
                    "idx1": idx1,
                }
            )
        n_trial_before = len(all_trial_rows)
        if stim_events_bpi:
            for _, pr in pairs_side.iterrows():
                gene = str(pr["gene"])
                anat_label = int(pr["anat_label"])
                plane = int(pr["plane"])
                roi_idx = int(pr["func_label"]) - 1
                dff = s2p_map[plane].get("dff", None)
                if dff is None or roi_idx < 0 or roi_idx >= dff.shape[0]:
                    continue
                trace = dff[roi_idx].astype(np.float32, copy=False)
                zstats = zstats_by_plane.get(plane, None)
                trace_z = None
                if zstats is not None and roi_idx < len(zstats["valid"]) and bool(zstats["valid"][roi_idx]):
                    trace_z = (trace - float(zstats["mu"][roi_idx])) / float(zstats["sigma"][roi_idx])
                for ev in stim_events_bpi:
                    seg, n_valid, n_total = _extract_bpi_window(
                        trace,
                        ev["idx0"],
                        ev["idx1"],
                        mode=env["BPI_EDGE_POLICY"],
                        min_valid_frac=env["BPI_MIN_VALID_FRAC"],
                    )
                    if seg is None:
                        continue
                    resp = float(np.nanmean(seg))
                    if not np.isfinite(resp):
                        continue
                    resp_z = np.nan
                    if trace_z is not None:
                        seg_z, _, _ = _extract_bpi_window(
                            trace_z,
                            ev["idx0"],
                            ev["idx1"],
                            mode=env["BPI_EDGE_POLICY"],
                            min_valid_frac=env["BPI_MIN_VALID_FRAC"],
                        )
                        if seg_z is not None:
                            rz = float(np.nanmean(seg_z))
                            if np.isfinite(rz):
                                resp_z = rz
                    all_trial_rows.append(
                        {
                            "owner": owner,
                            "fish_id": fish_id,
                            "gene": gene,
                            "anat_label": anat_label,
                            "plane": plane,
                            "func_label": int(pr["func_label"]),
                            "cell_key": f"{fish_id}|{gene}|{anat_label}",
                            "stim_type": ev["stim_type"],
                            "stim_class": ev["stim_class"],
                            "block": ev["block"],
                            "stim_idx": ev["stim_idx"],
                            "response_mean_dff": resp,
                            "response_mean_zdff": resp_z,
                            "n_valid_points": int(n_valid),
                            "n_total_points": int(n_total),
                            "fps": float(fps),
                        }
                    )
        row_status["n_trial_rows"] = int(len(all_trial_rows) - n_trial_before)
        row_status["ok"] = True
        if not row_status["notes"]:
            row_status["notes"] = "ok"
        fish_rows.append(row_status)
        panel_acc_by_fish[fish_id] = panel_acc_fish
        panel_cell_acc_by_fish[fish_id] = panel_cell_acc_fish
        if cfg.verbose:
            print(
                f"[cohort] {fish_id}: pairs={row_status['n_pairs_used']} side_valid={row_status['n_pairs_with_side']} "
                f"segs={row_status['n_trace_segments']} trials={row_status['n_trial_rows']} fps={row_status['fps']:.3f}"
            )

    if cohort_tvec is None:
        raise RuntimeError("No valid fish processed; cohort_tvec not initialized.")

    fish_summary_df = pd.DataFrame(fish_rows)
    cohort_results = {panel: {} for panel in plot_order}
    for panel in plot_order:
        for gene, gacc in panel_acc.get(panel, {}).items():
            segs = gacc.get("segs", [])
            if not segs:
                continue
            arr = np.vstack(segs).astype(np.float32, copy=False)
            mean, sem = _combine_segments(arr)
            n_segments = int(arr.shape[0])
            n_cells = int(len(gacc.get("rois", set())))
            n_trials_per_cell = float(n_segments / n_cells) if n_cells > 0 else np.nan
            cohort_results[panel][gene] = {
                "mean": mean,
                "sem": sem,
                "n_segments": n_segments,
                "n_cells": n_cells,
                "n_trials_per_cell": n_trials_per_cell,
                "source_stim_types": sorted(set(gacc.get("source_types", []))),
            }

    cohort_results_by_fish: dict[str, Any] = {}
    cohort_results_per_cell_by_fish: dict[str, Any] = {}
    for fish_id, panel_map in panel_acc_by_fish.items():
        fish_results = {panel: {} for panel in plot_order}
        fish_results_per_cell = {panel: {} for panel in plot_order}
        panel_cell_map = panel_cell_acc_by_fish.get(fish_id, {}) or {}
        for panel in plot_order:
            for gene, gacc in (panel_map.get(panel, {}) or {}).items():
                segs = gacc.get("segs", [])
                if not segs:
                    continue
                arr = np.vstack(segs).astype(np.float32, copy=False)
                mean, sem = _combine_segments(arr)
                n_segments = int(arr.shape[0])
                n_cells = int(len(gacc.get("rois", set())))
                n_trials_per_cell = float(n_segments / n_cells) if n_cells > 0 else np.nan
                fish_results[panel][gene] = {
                    "mean": mean,
                    "sem": sem,
                    "n_segments": n_segments,
                    "n_cells": n_cells,
                    "n_trials_per_cell": n_trials_per_cell,
                    "source_stim_types": sorted(set(gacc.get("source_types", []))),
                }
            for gene, roi_map in (panel_cell_map.get(panel, {}) or {}).items():
                panel_gene_cells = {}
                for roi_key, cacc in roi_map.items():
                    segs = cacc.get("segs", [])
                    if not segs:
                        continue
                    arr = np.vstack(segs).astype(np.float32, copy=False)
                    mean, sem = _combine_segments(arr)
                    panel_gene_cells[roi_key] = {
                        "mean": mean,
                        "sem": sem,
                        "n_segments": int(arr.shape[0]),
                        "plane": int(cacc.get("plane", roi_key[1] if len(roi_key) > 1 else 0)),
                        "roi_idx": int(cacc.get("roi_idx", roi_key[2] if len(roi_key) > 2 else 0)),
                        "func_label": int(cacc.get("func_label", (roi_key[2] + 1) if len(roi_key) > 2 else 1)),
                        "cell_key": str(cacc.get("cell_key", "")),
                        "midline_side": str(cacc.get("midline_side", "unknown")),
                        "is_low_activity": bool(cacc.get("is_low_activity", False)),
                        "source_stim_types": sorted(set(cacc.get("source_types", []))),
                    }
                if panel_gene_cells:
                    fish_results_per_cell[panel][gene] = panel_gene_cells
        cohort_results_by_fish[str(fish_id)] = fish_results
        cohort_results_per_cell_by_fish[str(fish_id)] = fish_results_per_cell

    summary_rows = []
    for panel in plot_order:
        mode = "LB" if panel.endswith("LB") else "LC"
        n_events_mode = int(mode_event_counts.get(mode, 0))
        for gene, res in cohort_results.get(panel, {}).items():
            summary_rows.append(
                {
                    "panel": panel,
                    "panel_title": env["PLOT_TITLES"].get(panel, panel),
                    "gene": gene,
                    "n_cells": int(res.get("n_cells", 0)),
                    "n_segments": int(res["n_segments"]),
                    "n_trials_per_cell": float(res.get("n_trials_per_cell", np.nan)),
                    "n_unilateral_events_mode": n_events_mode,
                    "source_stim_types": ", ".join(res.get("source_stim_types", [])),
                }
            )
    cohort_summary_df = pd.DataFrame(summary_rows)
    trial_df = pd.DataFrame(all_trial_rows)
    if trial_df.empty:
        raise RuntimeError("No cohort BPI trial rows were produced.")

    cell_rows = []
    grp = trial_df.groupby(["owner", "fish_id", "gene", "anat_label", "plane", "func_label", "cell_key"], dropna=False)
    for keys, sub in grp:
        bout_vals = sub.loc[sub["stim_class"] == "bout", "response_mean_dff"].to_numpy(dtype=float)
        cont_vals = sub.loc[sub["stim_class"] == "continuous", "response_mean_dff"].to_numpy(dtype=float)
        n_b = int(np.isfinite(bout_vals).sum())
        n_c = int(np.isfinite(cont_vals).sum())
        if n_b < env["BPI_MIN_TRIALS_PER_CLASS"] or n_c < env["BPI_MIN_TRIALS_PER_CLASS"]:
            continue
        b = float(np.nanmean(bout_vals))
        c = float(np.nanmean(cont_vals))
        denom = b + c
        if not np.isfinite(denom) or abs(denom) <= float(env["BPI_DENOM_EPS"]):
            continue
        bpi = float((b - c) / denom)
        if not np.isfinite(bpi):
            continue
        bout_vals_z = sub.loc[sub["stim_class"] == "bout", "response_mean_zdff"].to_numpy(dtype=float)
        cont_vals_z = sub.loc[sub["stim_class"] == "continuous", "response_mean_zdff"].to_numpy(dtype=float)
        n_b_z = int(np.isfinite(bout_vals_z).sum())
        n_c_z = int(np.isfinite(cont_vals_z).sum())
        b_z = float(np.nanmean(bout_vals_z)) if n_b_z > 0 else np.nan
        c_z = float(np.nanmean(cont_vals_z)) if n_c_z > 0 else np.nan
        denom_z = b_z + c_z if np.isfinite(b_z) and np.isfinite(c_z) else np.nan
        bpi_z = np.nan
        if n_b_z >= env["BPI_MIN_TRIALS_PER_CLASS"] and n_c_z >= env["BPI_MIN_TRIALS_PER_CLASS"]:
            if np.isfinite(denom_z) and abs(denom_z) > float(env["BPI_DENOM_EPS"]):
                bpi_z_tmp = float((b_z - c_z) / denom_z)
                if np.isfinite(bpi_z_tmp):
                    bpi_z = bpi_z_tmp
        owner, fish_id, gene, anat_label, plane, func_label, cell_key = keys
        cell_rows.append(
            {
                "owner": owner,
                "fish_id": fish_id,
                "gene": gene,
                "anat_label": int(anat_label),
                "plane": int(plane),
                "func_label": int(func_label),
                "cell_key": cell_key,
                "n_bout_trials": n_b,
                "n_cont_trials": n_c,
                "n_bout_trials_z": n_b_z,
                "n_cont_trials_z": n_c_z,
                "mean_bout_dff": b,
                "mean_cont_dff": c,
                "mean_bout_zdff": b_z,
                "mean_cont_zdff": c_z,
                "denom": float(denom),
                "denom_z": float(denom_z) if np.isfinite(denom_z) else np.nan,
                "bpi": bpi,
                "bpi_z": bpi_z,
            }
        )
    bpi_cells_df = pd.DataFrame(cell_rows)
    if bpi_cells_df.empty:
        raise RuntimeError("No cohort cells passed BPI filters.")

    from .plots.qa import collect_cohort_53a_tables

    cohort_53a_tables = collect_cohort_53a_tables(
        fish_specs=env["FISH_SPECS"],
        data_root=env["DATA_ROOT"],
        data_mode=env["DATA_MODE"],
    )
    save_cohort_outputs_to_disk(
        cohort_outdir,
        cohort_fish_summary_df=fish_summary_df,
        cohort_stim_summary_df=cohort_summary_df,
        cohort_bpi_trials_df=trial_df,
        cohort_bpi_cells_df=bpi_cells_df,
        cohort_tvec=cohort_tvec,
        cohort_results_stim_ipsi_contra=cohort_results,
        cohort_results_stim_ipsi_contra_by_fish=cohort_results_by_fish,
        cohort_results_stim_ipsi_contra_per_cell_by_fish=cohort_results_per_cell_by_fish,
        cohort_mode_durations=mode_durations_cohort,
        cohort_mode_event_counts=mode_event_counts,
        cohort_53a_tables=cohort_53a_tables,
        plot_order=plot_order,
        verbose=False,
    )

    bindings = dict(env)
    bindings.update(
        {
            "cohort_fish_summary_df": fish_summary_df,
            "cohort_stim_summary_df": cohort_summary_df,
            "cohort_bpi_trials_df": trial_df,
            "cohort_bpi_cells_df": bpi_cells_df,
            "cohort_tvec": cohort_tvec,
            "cohort_results_stim_ipsi_contra": cohort_results,
            "cohort_results_stim_ipsi_contra_by_fish": cohort_results_by_fish,
            "cohort_results_stim_ipsi_contra_per_cell_by_fish": cohort_results_per_cell_by_fish,
            "cohort_mode_durations": mode_durations_cohort,
            "cohort_mode_event_counts": mode_event_counts,
        }
    )
    if cfg.verbose:
        print(f"[cohort] processed fish: {int(fish_summary_df['ok'].sum())}/{len(fish_summary_df)}")
        print(f"[cohort] bpi cells: {len(bpi_cells_df)}")
        print(f"[cohort] trace summary rows: {len(cohort_summary_df)}")
    return {"bindings": bindings, "state": {"cohort_build_skipped": cached_build_skipped}, "config": asdict(cfg)}


__all__ = [
    "CohortBuildConfig",
    "build_cohort_outputs_stage",
    "cohort_cache_paths",
    "count_trace_genes",
    "load_cohort_analysis_state",
    "load_cohort_outputs_from_disk",
    "resolve_cohort_context_stage",
    "save_cohort_outputs_to_disk",
]
