"""Stimulus parsing and shared trial-window helpers for notebook cells [55]+."""

from __future__ import annotations

from dataclasses import dataclass
import json
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


def classify_stim_type(stype: str) -> str | None:
    parts = [part.strip().upper() for part in str(stype).split("+") if str(part).strip()]
    if not parts:
        return None
    tags: list[str] = []
    for part in parts:
        token = re.split(r"[_\s]", part, maxsplit=1)[0]
        if token.endswith("LB"):
            tags.append("B")
        elif token.endswith("LC"):
            tags.append("C")
        else:
            return None
    if all(tag == "B" for tag in tags):
        return "bout"
    if all(tag == "C" for tag in tags):
        return "continuous"
    return "mixed"


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


def normalize_session_label(session_label: str | int | None) -> str | None:
    if session_label in (None, "", False):
        return None
    text = str(session_label).strip().lower()
    if text in {"", "none", "nan", "null"}:
        return None
    if text.isdigit():
        return f"r{int(text)}"
    match = re.search(r"r(\d+)", text)
    if match:
        return f"r{int(match.group(1))}"
    return text


def _session_token_in_name(path: str | Path, fish_id: str | None = None) -> str:
    name = Path(path).name.lower()
    fish = "" if fish_id is None else re.escape(str(fish_id).lower())
    if fish:
        match = re.search(rf"(?:^|[_-])f?{fish}_r(\d+)(?:[_-]|$)", name)
        if match:
            return f"r{int(match.group(1))}"
    match = re.search(r"(?:^|[_-])r(\d+)(?:[_-]|$)", name)
    if match:
        return f"r{int(match.group(1))}"
    return "r1"


def _explicit_session_token_in_name(path: str | Path, fish_id: str | None = None) -> str | None:
    name = Path(path).name.lower()
    fish = "" if fish_id is None else re.escape(str(fish_id).lower())
    if fish:
        match = re.search(rf"(?:^|[_-])f?{fish}_r(\d+)(?:[_-]|$)", name)
        if match:
            return f"r{int(match.group(1))}"
    match = re.search(r"(?:^|[_-])r(\d+)(?:[_-]|$)", name)
    if match:
        return f"r{int(match.group(1))}"
    return None


def _filter_session_files(hits: list[Path], fish_id: str, session_label: str | int | None) -> list[Path]:
    session = normalize_session_label(session_label)
    if session is None:
        return hits
    return [path for path in hits if _session_token_in_name(path, fish_id) == session]


def _latest_file(hits: list[Path]) -> Path | None:
    if not hits:
        return None
    return sorted(set(hits), key=lambda path: (path.stat().st_mtime, path.name))[-1]


def _visible_csv_hits(hits: list[Path]) -> list[Path]:
    return [path for path in hits if path.name.endswith(".csv") and not path.name.startswith(".")]


def _session_companion_log_from_metadata(hits: list[Path], fish_id: str, session_label: str | int | None) -> Path | None:
    metadata_hits = [path for path in hits if "metadata" in path.name.lower() and "experiment_log" not in path.name.lower()]
    metadata_hits = _filter_session_files(sorted(set(metadata_hits)), fish_id, session_label)
    metadata_path = _latest_file(metadata_hits)
    if metadata_path is None:
        return None
    stem = metadata_path.stem
    prefixes = [
        re.sub(r"_r\d+_metadata$", "", stem),
        re.sub(r"_metadata$", "", stem),
    ]
    for prefix in dict.fromkeys(prefixes):
        candidate = metadata_path.with_name(f"{prefix}_experiment_log{metadata_path.suffix}")
        if candidate.exists() and not candidate.name.startswith("."):
            return candidate
    return None


def find_experiment_log(fish_dir: str | Path, fish_id: str, session_label: str | int | None = None) -> Path | None:
    base = Path(fish_dir) / "01_raw" / "2p" / "metadata"
    if not base.exists():
        return None
    patterns = [f"*{fish_id}*experiment_log*.csv", "*experiment_log*.csv"] if fish_id else ["*experiment_log*.csv"]
    hits: list[Path] = []
    for pattern in patterns:
        hits.extend(sorted(base.glob(pattern)))
    hits = _visible_csv_hits(hits)
    hits = _filter_session_files(sorted(set(hits)), fish_id, session_label)
    latest = _latest_file(hits)
    if latest is not None:
        return latest
    if session_label is None:
        return None
    metadata_patterns = [f"*{fish_id}*metadata*.csv", "*metadata*.csv"] if fish_id else ["*metadata*.csv"]
    metadata_hits: list[Path] = []
    for pattern in metadata_patterns:
        metadata_hits.extend(sorted(base.glob(pattern)))
    return _session_companion_log_from_metadata(_visible_csv_hits(metadata_hits), fish_id, session_label)


def find_metadata_csv(fish_dir: str | Path, fish_id: str, session_label: str | int | None = None) -> Path | None:
    base = Path(fish_dir) / "01_raw" / "2p" / "metadata"
    if not base.exists():
        return None
    patterns = [f"*{fish_id}*metadata*.csv", "*metadata*.csv"] if fish_id else ["*metadata*.csv"]
    hits: list[Path] = []
    for pattern in patterns:
        hits.extend(sorted(base.glob(pattern)))
    hits = _visible_csv_hits(hits)
    hits = [path for path in hits if "experiment_log" not in path.name.lower()]
    hits = _filter_session_files(sorted(set(hits)), fish_id, session_label)
    return _latest_file(hits)


def _session_sort_key(session_label: str | int | None) -> tuple[int, str]:
    label = normalize_session_label(session_label) or ""
    match = re.fullmatch(r"r(\d+)", label)
    if match:
        return int(match.group(1)), label
    return 10**9, label


def _discover_explicit_stimulus_sessions(fish_dir: str | Path, fish_id: str) -> list[str]:
    base = Path(fish_dir) / "01_raw" / "2p" / "metadata"
    if not base.exists():
        return []
    patterns = [f"*{fish_id}*experiment_log*.csv", "*experiment_log*.csv"] if fish_id else ["*experiment_log*.csv"]
    labels: set[str] = set()
    for pattern in patterns:
        for path in base.glob(pattern):
            if path.name.startswith("."):
                continue
            label = _explicit_session_token_in_name(path, fish_id)
            if label is not None:
                labels.add(label)
    return sorted(labels, key=_session_sort_key)


def _infer_equal_split_sessions(
    *,
    fish_dir: str | Path,
    fish_id: str,
    planes: list[int],
) -> list[dict[str, Any]]:
    session_labels = _discover_explicit_stimulus_sessions(fish_dir, fish_id)
    if len(session_labels) <= 1:
        return []
    if not planes:
        return []
    if len(planes) % len(session_labels) != 0:
        raise RuntimeError(
            "[stim] multiple explicit imaging sessions found but preprocessing metadata is missing; "
            f"cannot evenly split planes {planes} across sessions {session_labels}"
        )
    chunk_size = len(planes) // len(session_labels)
    out: list[dict[str, Any]] = []
    for idx, session_label in enumerate(session_labels):
        session_planes = planes[idx * chunk_size : (idx + 1) * chunk_size]
        if not session_planes:
            continue
        out.append(
            {
                "session_label": session_label,
                "session_number": idx + 1,
                "plane_offset": session_planes[0],
                "output_planes": session_planes,
                "preprocessing_metadata": None,
                "session_mapping_source": "inferred_equal_split",
            }
        )
    return out


def _companion_path_from_log(log_path: str | Path, companion_kind: str) -> Path | None:
    path = Path(log_path)
    stem = path.stem
    if "experiment_log" in stem:
        candidate = path.with_name(stem.replace("experiment_log", companion_kind) + path.suffix)
        if candidate.exists():
            return candidate
    prefix = re.sub(r"_?experiment_log$", "", stem)
    hits = sorted(path.parent.glob(f"{prefix}*{companion_kind}*.csv"))
    return hits[0] if hits else None


def _read_stimulus_sequence_csv(path: str | Path) -> list[str]:
    df = pd.read_csv(path)
    if df.empty:
        return []
    renamed = {column: column.strip().lower() for column in df.columns}
    df = df.rename(columns=renamed)
    if "stimulus" in df.columns:
        values = df["stimulus"]
    elif "stimulus_name" in df.columns:
        values = df["stimulus_name"]
    elif "stimulus_key" in df.columns:
        values = df["stimulus_key"]
    else:
        return []
    out = values.astype(str).str.strip()
    return [str(value) for value in out.tolist() if value and value.lower() not in {"nan", "none", "null"}]


def _read_planned_schedule_stimuli(path: str | Path) -> list[str]:
    df = pd.read_csv(path)
    if df.empty:
        return []
    renamed = {column: column.strip().lower() for column in df.columns}
    df = df.rename(columns=renamed)
    if "kind" in df.columns:
        df = df[df["kind"].astype(str).str.strip().str.lower().eq("stimulus")].copy()
    for col in ("stimulus_name", "stimulus_key", "label"):
        if col in df.columns:
            values = df[col].astype(str).str.strip()
            return [str(value) for value in values.tolist() if value and value.lower() not in {"nan", "none", "null"}]
    return []


def _read_planned_schedule_tables(path: str | Path) -> dict[str, pd.DataFrame]:
    df = pd.read_csv(path)
    if df.empty:
        return {"blocks": pd.DataFrame(), "stimuli": pd.DataFrame()}
    renamed = {column: column.strip().lower() for column in df.columns}
    df = df.rename(columns=renamed)
    if "block_num" not in df.columns or "kind" not in df.columns:
        return {"blocks": pd.DataFrame(), "stimuli": pd.DataFrame()}
    df = df.copy()
    df["kind"] = df["kind"].astype(str).str.strip().str.lower()
    df["block_num"] = pd.to_numeric(df["block_num"], errors="coerce")
    df = df[df["block_num"].notna()].copy()
    if df.empty:
        return {"blocks": pd.DataFrame(), "stimuli": pd.DataFrame()}
    df["block"] = df["block_num"].astype(int).map(lambda value: f"B{int(value)}")
    for col in ("start_sec", "end_sec", "duration_sec", "trial_index"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    blocks: list[dict[str, Any]] = []
    for block, group in df.groupby("block", sort=False):
        starts = pd.to_numeric(group.get("start_sec", pd.Series(dtype=float)), errors="coerce")
        ends = pd.to_numeric(group.get("end_sec", pd.Series(dtype=float)), errors="coerce")
        start = float(starts.min()) if starts.notna().any() else np.nan
        end = float(ends.max()) if ends.notna().any() else np.nan
        blocks.append({"block": str(block), "start": start, "end": end})
    block_df = pd.DataFrame(blocks)
    if not block_df.empty:
        block_df = block_df.sort_values("block", key=lambda col: col.map(block_key)).reset_index(drop=True)

    stim = df[df["kind"].eq("stimulus")].copy()
    if stim.empty:
        return {"blocks": block_df, "stimuli": pd.DataFrame()}
    type_col = next((col for col in ("stimulus_name", "stimulus_key", "label") if col in stim.columns), None)
    if type_col is None:
        return {"blocks": block_df, "stimuli": pd.DataFrame()}
    stim["type"] = stim[type_col].astype(str).str.strip()
    stim = stim[stim["type"].ne("") & ~stim["type"].str.lower().isin({"nan", "none", "null"})].copy()
    if stim.empty:
        return {"blocks": block_df, "stimuli": pd.DataFrame()}
    stim["stim_idx"] = pd.to_numeric(stim.get("trial_index", pd.Series(np.nan, index=stim.index)), errors="coerce")
    if stim["stim_idx"].isna().any():
        stim["stim_idx"] = stim.groupby("block").cumcount()
    stim["start"] = pd.to_numeric(stim.get("start_sec", pd.Series(np.nan, index=stim.index)), errors="coerce")
    stim["end"] = pd.to_numeric(stim.get("end_sec", pd.Series(np.nan, index=stim.index)), errors="coerce")
    stim["duration"] = pd.to_numeric(stim.get("duration_sec", pd.Series(np.nan, index=stim.index)), errors="coerce")
    missing_end = stim["end"].isna() & stim["start"].notna() & stim["duration"].notna()
    stim.loc[missing_end, "end"] = stim.loc[missing_end, "start"] + stim.loc[missing_end, "duration"]
    missing_duration = stim["duration"].isna() & stim["start"].notna() & stim["end"].notna()
    stim.loc[missing_duration, "duration"] = stim.loc[missing_duration, "end"] - stim.loc[missing_duration, "start"]
    stim_df = stim[["block", "stim_idx", "type", "start", "end", "duration"]].copy()
    stim_df["stim_idx"] = stim_df["stim_idx"].astype(int)
    side_mode = stim_df["type"].apply(lambda value: pd.Series(parse_unilateral_stim(value)))
    side_mode.columns = ["stim_side", "stim_mode"]
    stim_df = pd.concat([stim_df.reset_index(drop=True), side_mode.reset_index(drop=True)], axis=1)
    return {"blocks": block_df, "stimuli": stim_df.reset_index(drop=True)}


def resolve_presented_stimulus_metadata(
    *,
    log_path: str | Path,
    df_stim: pd.DataFrame,
    strict: bool = True,
) -> dict[str, Any]:
    """Resolve presented stimulus names from companion metadata and validate the log parse."""
    log_path_p = Path(log_path)
    log_sequence = df_stim["type"].astype(str).str.strip().tolist() if "type" in df_stim.columns else []
    log_types = list(dict.fromkeys(log_sequence))
    sources: list[dict[str, Any]] = []
    metadata_sequence: list[str] = []
    metadata_path: Path | None = None
    planned_schedule_blocks = pd.DataFrame()
    planned_schedule_stimuli = pd.DataFrame()

    trial_path = _companion_path_from_log(log_path_p, "trial_sequence")
    if trial_path is not None:
        seq = _read_stimulus_sequence_csv(trial_path)
        sources.append({"kind": "trial_sequence", "path": str(trial_path), "exists": trial_path.exists(), "n_stimuli": len(seq)})
        if seq:
            metadata_sequence = seq
            metadata_path = trial_path

    schedule_path = _companion_path_from_log(log_path_p, "planned_schedule")
    if schedule_path is not None:
        seq = _read_planned_schedule_stimuli(schedule_path)
        schedule_tables = _read_planned_schedule_tables(schedule_path)
        planned_schedule_blocks = schedule_tables["blocks"]
        planned_schedule_stimuli = schedule_tables["stimuli"]
        sources.append({"kind": "planned_schedule", "path": str(schedule_path), "exists": schedule_path.exists(), "n_stimuli": len(seq)})
        if not metadata_sequence and seq:
            metadata_sequence = seq
            metadata_path = schedule_path

    metadata_types = list(dict.fromkeys(metadata_sequence))
    if metadata_sequence and log_sequence and metadata_sequence != log_sequence:
        same_multiset = sorted(metadata_sequence) == sorted(log_sequence)
        msg = (
            "[stim] stimulus metadata does not match parsed experiment log "
            f"({metadata_path} vs {log_path_p}); metadata_n={len(metadata_sequence)} log_n={len(log_sequence)}"
        )
        if strict and not same_multiset:
            raise RuntimeError(msg)
        if strict and same_multiset:
            raise RuntimeError(msg + " (same names, different order)")

    if not sources:
        sources.append({"kind": "experiment_log", "path": str(log_path_p), "exists": log_path_p.exists(), "n_stimuli": len(log_sequence)})

    return {
        "stimulus_sequence": metadata_sequence or log_sequence,
        "stimulus_types": metadata_types or log_types,
        "stimulus_metadata_path": metadata_path,
        "stimulus_metadata_sources": pd.DataFrame(sources),
        "stimulus_types_source": "metadata" if metadata_sequence else "experiment_log",
        "planned_schedule_blocks": planned_schedule_blocks,
        "planned_schedule_stimuli": planned_schedule_stimuli,
    }


def discover_functional_sessions(fish_dir: str | Path, fish_id: str) -> list[dict[str, Any]]:
    preproc = Path(fish_dir) / "02_reg" / "00_preprocessing" / "2p_functional" / "01_individualPlanes"
    candidates = sorted(preproc.glob(f"*{fish_id}*preprocessing_metadata.json")) if preproc.exists() else []
    if not candidates and preproc.exists():
        candidates = sorted(preproc.glob("*preprocessing_metadata.json"))
    for path in reversed(candidates):
        try:
            payload = json.loads(path.read_text())
        except Exception:
            continue
        sessions = payload.get("sessions")
        if not isinstance(sessions, list):
            continue
        out: list[dict[str, Any]] = []
        for idx, session in enumerate(sessions):
            if not isinstance(session, dict):
                continue
            label = normalize_session_label(session.get("session_label") or session.get("session_number") or idx + 1)
            planes_raw = session.get("output_planes", [])
            planes: list[int] = []
            for value in planes_raw if isinstance(planes_raw, list) else []:
                try:
                    planes.append(int(value))
                except Exception:
                    pass
            if not planes:
                continue
            out.append(
                {
                    "session_label": label or f"r{idx + 1}",
                    "session_number": session.get("session_number"),
                    "plane_offset": session.get("plane_offset"),
                    "output_planes": sorted(set(planes)),
                    "preprocessing_metadata": path,
                }
            )
        if out:
            return out
    return []


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


def build_prestim_baseline_windows(
    df_evt: pd.DataFrame,
    fps: float,
    onset_delay_sec: float,
    *,
    tag: str = "[stim]",
    verbose: bool = False,
) -> list[tuple[int, int]]:
    if df_evt is None or getattr(df_evt, "empty", True):
        raise RuntimeError(f"{tag} df_evt is empty")
    evt = df_evt[["event", "time"]].copy()
    evt["event"] = evt["event"].astype(str).str.strip()
    evt["time"] = pd.to_numeric(evt["time"], errors="coerce")
    evt = evt.dropna(subset=["event", "time"]).sort_values("time").reset_index(drop=True)

    stim_starts: dict[tuple[str, int], float] = {}
    for event, time_s in evt[["event", "time"]].itertuples(index=False):
        match = re.match(r"^(B\d+)_stim(\d+)_.+$", event)
        if not match:
            continue
        key = (match.group(1), int(match.group(2)))
        stim_starts.setdefault(key, float(time_s))

    windows: list[tuple[int, int]] = []
    missing = 0
    for event, t_pre in evt[["event", "time"]].itertuples(index=False):
        match = re.match(r"^(B\d+)_prestim(\d+)_pause$", event)
        if not match:
            continue
        key = (match.group(1), int(match.group(2)))
        t_stim = stim_starts.get(key)
        if t_stim is None:
            missing += 1
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
        raise RuntimeError(f"{tag} no prestim baseline windows found")
    windows = sorted(windows, key=lambda win: (win[0], win[1]))
    merged: list[list[int]] = []
    for start, end in windows:
        if not merged or start > merged[-1][1]:
            merged.append([start, end])
        else:
            merged[-1][1] = max(merged[-1][1], end)
    out = [(int(start), int(end)) for start, end in merged if end > start]
    if verbose:
        total_sec = float(sum(end - start for start, end in out) / float(fps))
        if missing:
            print(f"{tag} baseline windows: {len(out)} (missing {missing} prestim->stim mappings; total {total_sec:.1f}s)")
        else:
            print(f"{tag} baseline windows: {len(out)} (total {total_sec:.1f}s)")
    return out


def build_prestim_trial_windows(
    df_evt: pd.DataFrame,
    fps: float,
    onset_delay_sec: float,
    *,
    tag: str = "[stim]",
) -> list[dict[str, int | str]]:
    if df_evt is None or getattr(df_evt, "empty", True):
        raise RuntimeError(f"{tag} df_evt is empty")
    evt = df_evt[["event", "time"]].copy()
    evt["event"] = evt["event"].astype(str).str.strip()
    evt["time"] = pd.to_numeric(evt["time"], errors="coerce")
    evt = evt.dropna(subset=["event", "time"]).sort_values("time").reset_index(drop=True)

    stim_starts: dict[tuple[str, int], float] = {}
    for event, time_s in evt[["event", "time"]].itertuples(index=False):
        match = re.match(r"^(B\d+)_stim(\d+)_.+$", event)
        if not match:
            continue
        key = (match.group(1), int(match.group(2)))
        stim_starts.setdefault(key, float(time_s))

    windows: list[dict[str, int | str]] = []
    for event, t_pre in evt[["event", "time"]].itertuples(index=False):
        match = re.match(r"^(B\d+)_prestim(\d+)_pause$", event)
        if not match:
            continue
        block = match.group(1)
        stim_idx = int(match.group(2))
        t_stim = stim_starts.get((block, stim_idx))
        if t_stim is None:
            continue
        t0 = float(t_pre)
        t1 = float(t_stim) + float(onset_delay_sec)
        if not np.isfinite(t0) or not np.isfinite(t1) or t1 <= t0:
            continue
        idx0 = int(round(t0 * float(fps)))
        idx1 = int(round(t1 * float(fps)))
        if idx1 > idx0:
            windows.append(
                {
                    "block": block,
                    "stim_idx": stim_idx,
                    "idx0": idx0,
                    "idx1": idx1,
                    "duration_frames": int(idx1 - idx0),
                }
            )
    if not windows:
        raise RuntimeError(f"{tag} no prestim trial windows found")
    return windows


def build_null_window_start_map(
    prestim_windows: list[dict[str, int | str]],
    duration_frames: list[int],
    *,
    step_frames: int = 1,
    min_windows: int = 20,
) -> dict[int, np.ndarray]:
    out: dict[int, np.ndarray] = {}
    unique_durations = sorted({int(d) for d in duration_frames if pd.notna(d) and int(d) > 0})
    for duration in unique_durations:
        starts: list[int] = []
        for window in prestim_windows:
            idx0 = int(window["idx0"])
            idx1 = int(window["idx1"])
            if idx1 - idx0 < duration:
                continue
            starts.extend(range(idx0, idx1 - duration + 1, max(1, int(step_frames))))
        starts_arr = np.asarray(sorted(set(starts)), dtype=np.int32)
        if starts_arr.size >= int(min_windows):
            out[int(duration)] = starts_arr
    return out


def compute_zscore_stats(
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
        start = max(0, int(idx0))
        end = min(n_frames, int(idx1))
        if end > start:
            mask[start:end] = True

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
    return {
        "mu": mu,
        "sigma": sigma,
        "n_valid": n_valid,
        "valid": valid,
        "low_points": ~has_points,
        "low_sigma": has_points & ~has_sigma,
    }


def combine_segments(arr: np.ndarray) -> tuple[np.ndarray, np.ndarray | None]:
    """Combine stacked segment traces into a mean and per-timepoint SEM."""
    arr = np.asarray(arr, dtype=np.float32)
    if arr.ndim != 2:
        raise ValueError("combine_segments expects a 2D array [n_segments, n_timepoints]")
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
    df_evt_full = load_events_df(log_path)
    df_evt_full["time"] = df_evt_full["time"].astype(float) * float(cfg.stim_time_scale)
    df_evt, df_stim = build_stim_tables(
        df_evt_full,
        fps=fps,
        onset_delay_sec=float(cfg.onset_delay_sec),
        remove_interblock_gaps=bool(cfg.remove_interblock_gaps),
        measure_start_block=cfg.measure_start_block,
        measure_start_event=cfg.measure_start_event,
    )
    stim_meta = resolve_presented_stimulus_metadata(log_path=log_path, df_stim=df_stim, strict=True)
    return {
        "log_path": log_path,
        "meta_path": meta_path,
        "frame_rate": fps,
        "df_evt_full": df_evt_full,
        "df_evt": df_evt,
        "df_stim": df_stim,
        "stimulus_sequence": stim_meta["stimulus_sequence"],
        "stimulus_types": stim_meta["stimulus_types"],
        "stimulus_metadata_path": stim_meta["stimulus_metadata_path"],
        "stimulus_metadata_sources": stim_meta["stimulus_metadata_sources"],
        "stimulus_types_source": stim_meta["stimulus_types_source"],
        "planned_schedule_blocks": stim_meta["planned_schedule_blocks"],
        "planned_schedule_stimuli": stim_meta["planned_schedule_stimuli"],
        "source_table": pd.DataFrame(
            [
                {"key": "EXPERIMENT_LOG_CSV", "value": str(log_path), "exists": log_path.exists(), "rows": len(df_evt)},
                {"key": "EXPERIMENT_META_CSV", "value": str(meta_path) if meta_path is not None else None, "exists": meta_path.exists() if meta_path is not None else None, "rows": None},
                {"key": "STIMULUS_TYPES_SOURCE", "value": stim_meta["stimulus_types_source"], "exists": None, "rows": len(stim_meta["stimulus_types"])},
                {"key": "STIMULUS_TYPES", "value": ", ".join(stim_meta["stimulus_types"]), "exists": None, "rows": len(stim_meta["stimulus_sequence"])},
                {"key": "FRAME_RATE", "value": fps, "exists": None, "rows": None},
                {"key": "STIM_TIME_SCALE", "value": cfg.stim_time_scale, "exists": None, "rows": None},
                {"key": "MEASURE_START_BLOCK", "value": cfg.measure_start_block, "exists": None, "rows": None},
                {"key": "MEASURE_START_EVENT", "value": cfg.measure_start_event, "exists": None, "rows": None},
                {"key": "REMOVE_INTERBLOCK_GAPS", "value": cfg.remove_interblock_gaps, "exists": None, "rows": None},
            ]
        ),
    }


def resolve_plane_stimulus_contexts(
    *,
    fish_dir: str | Path,
    fish_id: str,
    plane_indices: list[int] | np.ndarray | pd.Series | None = None,
    experiment_log_csv: str | Path | None = None,
    experiment_meta_csv: str | Path | None = None,
    frame_rate: float | None = None,
    config: StimulusConfig | None = None,
) -> dict[int, dict[str, Any]]:
    if plane_indices is None:
        planes: list[int] = []
    else:
        plane_series = pd.to_numeric(pd.Series(plane_indices), errors="coerce").dropna()
        planes = sorted(set(plane_series.astype(int).tolist()))

    if experiment_log_csv is not None or experiment_meta_csv is not None:
        ctx = resolve_stimulus_context(
            fish_dir=fish_dir,
            fish_id=fish_id,
            experiment_log_csv=experiment_log_csv,
            experiment_meta_csv=experiment_meta_csv,
            frame_rate=frame_rate,
            config=config,
        )
        ctx["session_label"] = "override"
        return {int(plane): ctx for plane in planes}

    sessions = discover_functional_sessions(fish_dir, fish_id)
    if not sessions:
        sessions = _infer_equal_split_sessions(fish_dir=fish_dir, fish_id=fish_id, planes=planes)
    if not sessions:
        ctx = resolve_stimulus_context(
            fish_dir=fish_dir,
            fish_id=fish_id,
            frame_rate=frame_rate,
            config=config,
        )
        ctx["session_label"] = _session_token_in_name(ctx["log_path"], fish_id)
        ctx["session_mapping_source"] = "single_context"
        return {int(plane): ctx for plane in planes}

    out: dict[int, dict[str, Any]] = {}
    requested = set(planes)
    for session in sessions:
        session_label = normalize_session_label(session.get("session_label"))
        session_planes = [int(plane) for plane in session.get("output_planes", [])]
        target_planes = sorted(requested.intersection(session_planes)) if requested else session_planes
        if not target_planes:
            continue
        log_path = find_experiment_log(fish_dir, fish_id, session_label=session_label)
        meta_path = find_metadata_csv(fish_dir, fish_id, session_label=session_label)
        if log_path is None:
            raise FileNotFoundError(f"[stim] experiment log not found for session {session_label} ({fish_id})")
        ctx = resolve_stimulus_context(
            fish_dir=fish_dir,
            fish_id=fish_id,
            experiment_log_csv=log_path,
            experiment_meta_csv=meta_path,
            frame_rate=frame_rate,
            config=config,
        )
        ctx["session_label"] = session_label
        ctx["session_planes"] = session_planes
        ctx["preprocessing_metadata"] = session.get("preprocessing_metadata")
        ctx["session_mapping_source"] = session.get("session_mapping_source") or "preprocessing_metadata"
        for plane in target_planes:
            out[int(plane)] = ctx

    missing = sorted(requested - set(out))
    if missing:
        fallback = resolve_stimulus_context(
            fish_dir=fish_dir,
            fish_id=fish_id,
            frame_rate=frame_rate,
            config=config,
        )
        fallback["session_label"] = _session_token_in_name(fallback["log_path"], fish_id)
        fallback["session_mapping_source"] = "fallback_latest"
        for plane in missing:
            out[int(plane)] = fallback
    return out


__all__ = [
    "StimulusConfig",
    "block_key",
    "build_null_window_start_map",
    "build_prestim_baseline_windows",
    "build_prestim_trial_windows",
    "build_stim_tables",
    "classify_stim_type",
    "combine_segments",
    "compute_zscore_stats",
    "discover_functional_sessions",
    "effective_motion_window",
    "find_experiment_log",
    "find_metadata_csv",
    "load_events_df",
    "load_metadata_params",
    "normalize_session_label",
    "parse_float",
    "parse_unilateral_stim",
    "resolve_presented_stimulus_metadata",
    "resolve_plane_stimulus_contexts",
    "resolve_stimulus_context",
]
