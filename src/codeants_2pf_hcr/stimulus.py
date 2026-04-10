"""Stimulus parsing and table builders for notebook cells [55] and downstream trace work."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class StimulusConfig:
    stim_time_scale: float = 1.0
    measure_start_block: int | str = 1
    measure_start_event: str = "start"
    remove_interblock_gaps: bool = True
    onset_delay_sec: float = 10.0


def parse_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        cleaned = re.sub(r"[^0-9eE+\-.]", "", str(value))
        return float(cleaned) if cleaned else None


def block_key(block: str) -> tuple[int, str]:
    match = re.match(r"^B(\d+)$", str(block).strip())
    if match:
        return int(match.group(1)), str(block)
    return 10**9, str(block)


def find_experiment_log(fish_dir: str | Path, fish_id: str) -> Path | None:
    base = Path(fish_dir) / "01_raw" / "2p" / "metadata"
    if not base.exists():
        return None
    patterns = [f"*{fish_id}*experiment_log*.csv", "*experiment_log*.csv"] if fish_id else ["*experiment_log*.csv"]
    hits: list[Path] = []
    for pattern in patterns:
        hits.extend(sorted(base.glob(pattern)))
    if not hits:
        return None
    hits = sorted(set(hits), key=lambda path: path.stat().st_mtime)
    return hits[-1]


def find_metadata_csv(fish_dir: str | Path, fish_id: str) -> Path | None:
    base = Path(fish_dir) / "01_raw" / "2p" / "metadata"
    if not base.exists():
        return None
    patterns = [f"*{fish_id}*metadata*.csv", "*metadata*.csv"] if fish_id else ["*metadata*.csv"]
    hits: list[Path] = []
    for pattern in patterns:
        hits.extend(sorted(base.glob(pattern)))
    hits = [path for path in hits if "experiment_log" not in path.name.lower()]
    if not hits:
        return None
    hits = sorted(set(hits), key=lambda path: path.stat().st_mtime)
    return hits[-1]


def load_events_df(csv_path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    if df.empty:
        raise ValueError("experiment log is empty")
    renamed = {column: column.strip().lower() for column in df.columns}
    df = df.rename(columns=renamed)
    event_cols = [column for column in df.columns if "event" in column]
    time_cols = [column for column in df.columns if "timestamp" in column or re.search(r"\btime\b", column)]
    event_col = "event" if "event" in df.columns else (event_cols[0] if event_cols else None)
    time_col = "timestamp" if "timestamp" in df.columns else ("time" if "time" in df.columns else (time_cols[0] if time_cols else None))
    if event_col is None or time_col is None:
        raise ValueError(f"Could not infer event/time columns from {list(df.columns)}")
    out = df[[event_col, time_col]].rename(columns={event_col: "event", time_col: "time"})
    out["event"] = out["event"].astype(str).str.strip()
    out["time"] = pd.to_numeric(out["time"], errors="coerce")
    return out.dropna(subset=["event", "time"]).reset_index(drop=True)


def load_metadata_params(csv_path: str | Path) -> dict[str, Any]:
    df = pd.read_csv(csv_path)
    if df.empty:
        raise ValueError("metadata is empty")
    renamed = {column: column.strip().lower() for column in df.columns}
    df = df.rename(columns=renamed)
    if "parameter" not in df.columns or "value" not in df.columns:
        raise ValueError(f"metadata csv missing parameter/value columns: {list(df.columns)}")
    params: dict[str, Any] = {}
    for _, row in df.iterrows():
        params[str(row["parameter"]).strip().lower()] = row["value"]
    return params


def effective_motion_window(start_s: object, duration_s: object, end_s: object, onset_delay_sec: float) -> tuple[float, float, float]:
    start = parse_float(start_s)
    duration = parse_float(duration_s)
    end = parse_float(end_s)
    if start is None or not np.isfinite(start):
        return np.nan, np.nan, np.nan
    motion_start = float(start) + float(onset_delay_sec)
    if end is not None and np.isfinite(end):
        motion_end = float(end)
    elif duration is not None and np.isfinite(duration):
        motion_end = float(start) + float(duration)
    else:
        return motion_start, np.nan, np.nan
    motion_duration = motion_end - motion_start
    if not np.isfinite(motion_duration) or motion_duration <= 0:
        return motion_start, motion_end, np.nan
    return motion_start, motion_end, motion_duration


def parse_unilateral_stim(stype: str) -> tuple[str | None, str | None]:
    parts = [part.strip().upper() for part in str(stype).split("+") if str(part).strip()]
    if len(parts) != 1:
        return None, None
    match = re.match(r"^([LR])(LB|LC)$", parts[0])
    if not match:
        return None, None
    stim_side = "left" if match.group(1) == "L" else "right"
    stim_mode = "bout" if match.group(2) == "LB" else "continuous"
    return stim_side, stim_mode


def build_stim_tables(
    df_evt: pd.DataFrame,
    *,
    fps: float | None,
    onset_delay_sec: float,
    remove_interblock_gaps: bool,
    measure_start_block: int | str = 1,
    measure_start_event: str = "start",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    events = df_evt.copy()
    block_label = str(measure_start_block) if isinstance(measure_start_block, str) and str(measure_start_block).startswith("B") else f"B{int(measure_start_block)}"
    start_event = f"{block_label}_{measure_start_event}"
    start_match = events.loc[events["event"] == start_event, "time"]
    if len(start_match):
        t0 = float(start_match.iloc[0])
    else:
        block_rows = events[events["event"].str.startswith(f"{block_label}_")]
        t0 = float(block_rows["time"].min()) if not block_rows.empty else None
    if t0 is not None:
        events["time"] = events["time"] - t0
        events = events[events["time"] >= 0].reset_index(drop=True)

    block_codes = []
    for event in events["event"]:
        match = re.match(r"^(B\d+)_", str(event))
        if match:
            block_codes.append(match.group(1))
    block_codes = sorted(set(block_codes), key=block_key)

    blocks: list[dict[str, Any]] = []
    for block in block_codes:
        block_events = events[events["event"].str.startswith(f"{block}_")]
        if block_events.empty:
            continue
        start_time = block_events.loc[block_events["event"] == f"{block}_start", "time"]
        end_time = block_events.loc[block_events["event"] == f"{block}_end", "time"]
        interblock = block_events.loc[block_events["event"] == f"{block}_interblock_pause", "time"]
        block_start = float(start_time.iloc[0]) if len(start_time) else float(block_events["time"].min())
        block_end = float(end_time.iloc[0]) if len(end_time) else None
        interblock_time = float(interblock.iloc[0]) if len(interblock) else None
        if block_end is None:
            block_end = interblock_time
        elif interblock_time is not None:
            block_end = interblock_time
        if block_end is None:
            block_end = float(block_events["time"].max())
        blocks.append({"block": block, "start": block_start, "end": block_end})

    if remove_interblock_gaps and blocks:
        shift_map: dict[str, float] = {}
        shift = 0.0
        prev_end = None
        for block in sorted(blocks, key=lambda item: float(item["start"])):
            orig_start = float(block["start"])
            orig_end = float(block["end"])
            if prev_end is not None:
                shift += max(0.0, orig_start - prev_end)
            shift_map[str(block["block"])] = shift
            block["start"] = orig_start - shift
            block["end"] = orig_end - shift
            prev_end = orig_end

        def shift_event_time(row: pd.Series) -> float:
            match = re.match(r"^(B\d+)_", str(row["event"]))
            if not match:
                return float(row["time"])
            return float(row["time"]) - shift_map.get(match.group(1), 0.0)

        events["time"] = events.apply(shift_event_time, axis=1)
        events = events.sort_values("time").reset_index(drop=True)

    stims: list[dict[str, Any]] = []
    for row in events.itertuples(index=False):
        match = re.match(r"^(B\d+)_stim(\d+)_(.+)$", str(row.event))
        if not match:
            continue
        block = match.group(1)
        stim_idx = int(match.group(2))
        stim_type = match.group(3)
        start = float(row.time)
        post_name = f"{block}_poststim{stim_idx}_pause"
        post_match = events.loc[events["event"] == post_name, "time"]
        if len(post_match):
            end = float(post_match.iloc[0])
        else:
            after = events[(events["time"] > start) & (events["event"].str.startswith(f"{block}_"))].sort_values("time")
            end = float(after["time"].iloc[0]) if not after.empty else np.nan
        stims.append(
            {
                "block": block,
                "stim_idx": stim_idx,
                "type": stim_type,
                "start": start,
                "end": end,
                "duration": end - start if np.isfinite(end) else np.nan,
            }
        )

    df_stim = pd.DataFrame(stims)
    if df_stim.empty:
        raise RuntimeError("No stimuli parsed from experiment log")
    side_mode = df_stim["type"].apply(lambda value: pd.Series(parse_unilateral_stim(value)))
    side_mode.columns = ["stim_side", "stim_mode"]
    df_stim = pd.concat([df_stim, side_mode], axis=1)
    motion = df_stim.apply(
        lambda row: pd.Series(
            effective_motion_window(row.get("start", np.nan), row.get("duration", np.nan), row.get("end", np.nan), onset_delay_sec),
            index=["motion_start", "motion_end", "motion_duration"],
        ),
        axis=1,
    )
    df_stim = pd.concat([df_stim, motion], axis=1)
    df_stim["trial_id"] = df_stim.apply(lambda row: f"{row['block']}_stim{int(row['stim_idx'])}_{row['type']}", axis=1)
    if fps is not None:
        df_stim["fps"] = float(fps)
    return events, df_stim


def resolve_stimulus_context(
    *,
    fish_dir: str | Path,
    fish_id: str,
    experiment_log_csv: str | Path | None = None,
    experiment_meta_csv: str | Path | None = None,
    frame_rate: float | None = None,
    config: StimulusConfig | None = None,
) -> dict[str, Any]:
    cfg = config or StimulusConfig()
    meta_path = Path(experiment_meta_csv) if experiment_meta_csv else find_metadata_csv(fish_dir, fish_id)
    fps = frame_rate
    if fps is None and meta_path is not None and meta_path.exists():
        params = load_metadata_params(meta_path)
        fps = parse_float(params.get("framerate", params.get("frame_rate", params.get("fps"))))

    log_path = Path(experiment_log_csv) if experiment_log_csv else find_experiment_log(fish_dir, fish_id)
    if log_path is None or not log_path.exists():
        raise FileNotFoundError(
            "[stim] experiment log CSV is required for [55]. "
            "Set EXPERIMENT_LOG_CSV or place an experiment_log*.csv under 01_raw/2p/metadata."
        )
    df_evt = load_events_df(log_path)
    df_evt["time"] = df_evt["time"].astype(float) * float(cfg.stim_time_scale)
    df_evt, df_stim = build_stim_tables(
        df_evt,
        fps=fps,
        onset_delay_sec=float(cfg.onset_delay_sec),
        remove_interblock_gaps=bool(cfg.remove_interblock_gaps),
        measure_start_block=cfg.measure_start_block,
        measure_start_event=cfg.measure_start_event,
    )
    return {
        "log_path": log_path,
        "meta_path": meta_path,
        "frame_rate": fps,
        "df_evt": df_evt,
        "df_stim": df_stim,
        "source_table": pd.DataFrame(
            [
                {"key": "EXPERIMENT_LOG_CSV", "value": str(log_path), "exists": log_path.exists(), "rows": len(df_evt)},
                {"key": "EXPERIMENT_META_CSV", "value": str(meta_path) if meta_path is not None else None, "exists": meta_path.exists() if meta_path is not None else None, "rows": None},
                {"key": "FRAME_RATE", "value": fps, "exists": None, "rows": None},
                {"key": "STIM_TIME_SCALE", "value": cfg.stim_time_scale, "exists": None, "rows": None},
                {"key": "MEASURE_START_BLOCK", "value": cfg.measure_start_block, "exists": None, "rows": None},
                {"key": "MEASURE_START_EVENT", "value": cfg.measure_start_event, "exists": None, "rows": None},
                {"key": "REMOVE_INTERBLOCK_GAPS", "value": cfg.remove_interblock_gaps, "exists": None, "rows": None},
            ]
        ),
    }


__all__ = [
    "StimulusConfig",
    "block_key",
    "build_stim_tables",
    "effective_motion_window",
    "find_experiment_log",
    "find_metadata_csv",
    "load_events_df",
    "load_metadata_params",
    "parse_float",
    "parse_unilateral_stim",
    "resolve_stimulus_context",
]
