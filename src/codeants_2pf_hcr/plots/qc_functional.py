"""Read-only notebook views for functional-reference, drift, and registration QC.

The builders in this module consume persisted manifests, numeric data, and tables. They
never run a pipeline stage and never write analysis products.
"""

from __future__ import annotations
from codeants_2pf_hcr.paths import visible_glob, is_visible_path
from ..native_paths import stage_product_path

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import matplotlib.image as mpimg
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import tifffile

from ..spatial import imread_any, norm01
from ..stimulus import StimulusConfig, map_stimulus_context_to_trace_grid, resolve_plane_stimulus_contexts


_FISH_ID_RE = re.compile(r"^[A-Z][0-9]{3}_f[0-9]{2}$")
_PLANE_RE = re.compile(r"plane[_-]?(\d+)", re.IGNORECASE)


def _path(value: str | Path, *, label: str) -> Path:
    path = Path(value)
    if not path.exists():
        raise FileNotFoundError(f"Missing {label}: {path}")
    return path


def _json(path: str | Path, *, label: str) -> tuple[Path, Any]:
    resolved = _path(path, label=label)
    try:
        return resolved, json.loads(resolved.read_text())
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in {label}: {resolved}") from exc


def _csv(path: str | Path, *, label: str) -> tuple[Path, pd.DataFrame]:
    resolved = _path(path, label=label)
    frame = pd.read_csv(resolved)
    if frame.empty:
        raise ValueError(f"{label} is empty: {resolved}")
    return resolved, frame


def _validate_fish_id(fish_id: str) -> str:
    value = str(fish_id)
    if not _FISH_ID_RE.fullmatch(value):
        raise ValueError(f"Invalid fish ID: {value!r}")
    return value


def _validate_manifest_fish(payload: Any, fish_id: str, path: Path) -> None:
    if not isinstance(payload, Mapping):
        raise ValueError(f"Manifest must contain a JSON object: {path}")
    observed = payload.get("fish_id")
    if observed not in (None, "", fish_id):
        raise ValueError(f"Fish mismatch in {path}: expected {fish_id}, found {observed}")


def _validate_table_fish(frame: pd.DataFrame, fish_id: str, path: Path) -> None:
    if "fish_id" not in frame.columns:
        return
    observed = set(frame["fish_id"].dropna().astype(str).unique())
    if observed and observed != {fish_id}:
        raise ValueError(f"Fish mismatch in {path}: expected only {fish_id}, found {sorted(observed)}")


def _manifest_row(label: str, path: Path, payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "artifact": label,
        "path": str(path),
        "status": payload.get("status", "not_recorded"),
        "generated_at": payload.get("generated_at"),
    }


def _plane_from_value(value: Any) -> int | None:
    if value is None:
        return None
    match = _PLANE_RE.search(str(value))
    return int(match.group(1)) if match else None


def load_functional_reference_drift_qc(
    *,
    fish_id: str,
    functional_reference_manifest_path: str | Path,
    z_drift_root: str | Path,
    response_evidence_paths: Sequence[str | Path] = (),
) -> dict[str, Any]:
    """Load the persisted functional-reference and temporal-drift evidence bundle."""
    fish = _validate_fish_id(fish_id)
    ref_path, ref_manifest = _json(functional_reference_manifest_path, label="functional-reference manifest")
    _validate_manifest_fish(ref_manifest, fish, ref_path)
    reference_paths: list[Path] = []
    outputs = ref_manifest.get("outputs") if isinstance(ref_manifest, Mapping) else None
    if isinstance(outputs, list):
        for record in outputs:
            if not isinstance(record, Mapping) or not record.get("path"):
                continue
            candidate = Path(str(record["path"]))
            if candidate.suffix.lower() in {".tif", ".tiff"}:
                reference_paths.append(_path(candidate, label="functional-reference TIFF"))
    if not reference_paths:
        raise ValueError(f"Functional-reference manifest has no persisted TIFF outputs: {ref_path}")

    drift_root = _path(z_drift_root, label="temporal Z-drift directory")
    drift_manifest_path, drift_manifest = _json(drift_root / "z_drift_manifest.json", label="Z-drift manifest")
    _validate_manifest_fish(drift_manifest, fish, drift_manifest_path)

    interval_path, intervals = _csv(drift_root / "z_drift_intervals.csv", label="Z-drift intervals table")
    profile_path, profiles = _csv(drift_root / "z_drift_ncc_profiles.csv", label="Z-drift NCC profiles table")
    summary_path, summary = _csv(drift_root / "z_drift_session_summary.csv", label="Z-drift session summary")
    for path, frame in ((interval_path, intervals), (profile_path, profiles), (summary_path, summary)):
        _validate_table_fish(frame, fish, path)

    required_interval_columns = {"session", "plane_index", "interval_index", "interval_label", "best_z_subslice"}
    missing = required_interval_columns.difference(intervals.columns)
    if missing:
        raise ValueError(f"Z-drift intervals table lacks required columns {sorted(missing)}: {interval_path}")
    required_profile_columns = {"session", "plane_index", "interval_index", "anatomy_z", "ncc"}
    missing = required_profile_columns.difference(profiles.columns)
    if missing:
        raise ValueError(f"Z-drift NCC profiles table lacks required columns {sorted(missing)}: {profile_path}")

    tracks_png = _path(drift_root / "z_drift_tracks.png", label="Z-drift trajectory PNG")
    profiles_png = _path(drift_root / "z_drift_ncc_profiles.png", label="Z-drift NCC-profile PNG")

    parameters = ref_manifest.get("parameters", {})
    if not isinstance(parameters, Mapping):
        parameters = {}
    frame_selection = parameters.get("frame_selection_by_plane", {})
    provenance_rows: list[dict[str, Any]] = []
    if isinstance(frame_selection, Mapping):
        for plane_label, record in frame_selection.items():
            if not isinstance(record, Mapping):
                continue
            provenance_rows.append(
                {
                    "plane_label": str(plane_label),
                    "plane_index": _plane_from_value(plane_label),
                    "decision": record.get("decision"),
                    "frame_start": record.get("frame_start"),
                    "source_frame_count": record.get("source_frame_count"),
                    "reference_frame_count": record.get("reference_frame_count"),
                }
            )
    provenance = pd.DataFrame(provenance_rows)
    if bool(parameters.get("exclude_first_block")) and provenance.empty:
        raise ValueError(f"Functional-reference manifest records Block-0 exclusion but no per-plane provenance: {ref_path}")

    response_rows = [
        {"path": str(Path(value)), "available": Path(value).exists(), "name": Path(value).name}
        for value in response_evidence_paths
    ]
    response_evidence = pd.DataFrame(response_rows, columns=["name", "path", "available"])
    artifact_paths = (
        ref_path,
        *reference_paths,
        drift_manifest_path,
        interval_path,
        profile_path,
        summary_path,
        tracks_png,
        profiles_png,
    )
    return {
        "fish_id": fish,
        "functional_reference_manifest": ref_manifest,
        "functional_reference_paths": tuple(reference_paths),
        "z_drift_manifest": drift_manifest,
        "intervals": intervals,
        "profiles": profiles,
        "session_summary": summary,
        "block0_provenance": provenance,
        "response_evidence": response_evidence,
        "trajectory_png": tracks_png,
        "profile_png": profiles_png,
        "artifact_paths": artifact_paths,
    }


def render_functional_reference_drift_summary(bundle: Mapping[str, Any]) -> plt.Figure:
    """Render persisted drift trajectories plus an explicit response-evidence panel."""
    fish_id = str(bundle["fish_id"])
    intervals = pd.DataFrame(bundle["intervals"])
    response = pd.DataFrame(bundle["response_evidence"])
    sessions = list(dict.fromkeys(intervals["session"].astype(str)))
    fig, axes = plt.subplots(
        max(1, len(sessions)),
        2,
        figsize=(12.0, 3.6 * max(1, len(sessions))),
        squeeze=False,
        constrained_layout=True,
    )
    for row_index, session in enumerate(sessions):
        ax = axes[row_index, 0]
        subset = intervals[intervals["session"].astype(str) == session]
        for plane, group in subset.groupby("plane_index", sort=True):
            group = group.sort_values("interval_index")
            ax.plot(group["interval_index"], group["best_z_subslice"], marker="o", label=f"plane {int(plane)}")
        labels = (
            subset.sort_values("interval_index").drop_duplicates("interval_index").set_index("interval_index")["interval_label"]
        )
        ticks = sorted(int(value) for value in labels.index)
        ax.set_xticks(ticks, [str(labels.loc[value]).replace("\n", "\n") for value in ticks], fontsize=7)
        ax.set_ylabel("Best anatomy Z (subslice)")
        ax.set_title(f"{session}: retained-block depth trajectory")
        ax.grid(alpha=0.2)
        ax.legend(frameon=False, fontsize=8, ncol=2)

        info_ax = axes[row_index, 1]
        info_ax.axis("off")
        summary = pd.DataFrame(bundle["session_summary"])
        summary_row = summary[summary["session"].astype(str) == session] if "session" in summary else summary.iloc[0:0]
        lines = ["Persisted session summary"]
        if summary_row.empty:
            lines.append("No matching session row")
        else:
            for key, value in summary_row.iloc[0].items():
                if key not in {"fish_id", "session"}:
                    lines.append(f"{key}: {value}")
        if row_index == 0:
            lines.append("")
            lines.append("Response evidence")
            if response.empty:
                lines.append("No response-evidence paths configured")
            else:
                lines.extend(f"{'available' if row.available else 'missing'}: {row.name}" for row in response.itertuples())
        info_ax.text(0.0, 1.0, "\n".join(lines), va="top", family="monospace", fontsize=8)
    fig.suptitle(f"{fish_id}: functional-reference and temporal Z-drift review", fontsize=13, fontweight="bold")
    return fig


def render_functional_reference_artifacts(bundle: Mapping[str, Any]) -> plt.Figure:
    """Display functional-reference TIFFs declared by the persisted manifest."""
    paths = tuple(Path(value) for value in bundle["functional_reference_paths"])
    if not paths:
        raise ValueError("No functional-reference TIFFs are available in the QC bundle")
    ncols = 2
    nrows = int(np.ceil(len(paths) / ncols))
    fig, axes = plt.subplots(nrows, ncols, figsize=(11.0, 4.8 * nrows), squeeze=False, constrained_layout=True)
    for ax, path in zip(axes.flat, paths):
        image = np.asarray(tifffile.imread(path))
        if image.ndim > 2:
            image = np.max(image, axis=0)
        if image.ndim != 2 or image.size == 0:
            raise ValueError(f"Functional-reference TIFF is not a non-empty 2D image: {path}")
        ax.imshow(image, cmap="gray")
        ax.set_title(path.name, fontsize=9)
        ax.axis("off")
    for ax in axes.flat[len(paths) :]:
        ax.axis("off")
    fig.suptitle(f"{bundle['fish_id']}: persisted functional references", fontsize=13, fontweight="bold")
    return fig


def build_early_response_trial_count_tables(
    *, fish_id: str, scored_windows_path: str | Path, presented_response_path: str | Path,
) -> dict[str, pd.DataFrame]:
    """Show presented and valid trial counts separately for each imaging session."""
    fish = _validate_fish_id(fish_id)
    windows_path, windows = _csv(scored_windows_path, label="scored stimulus windows")
    response_path, responses = _csv(presented_response_path, label="presented-stimulus responses")
    _validate_table_fish(windows, fish, windows_path)
    _validate_table_fish(responses, fish, response_path)
    event_keys = ["session_label", "block", "stim_idx", "stim_type"]
    response_keys = ["session_label", "stim_type", "n_valid_trials"]
    for path, frame, required in (
        (windows_path, windows, event_keys), (response_path, responses, response_keys),
    ):
        missing = sorted(set(required) - set(frame.columns))
        if missing:
            raise ValueError(f"Missing trial-count columns {missing}: {path}")
    presented = windows[event_keys].drop_duplicates().groupby(
        ["session_label", "stim_type"], dropna=False,
    ).size().rename("counts")
    valid = responses.groupby(["session_label", "stim_type"], dropna=False)["n_valid_trials"].agg(["min", "max"])
    if set(presented.index) != set(valid.index):
        raise ValueError("Presented and valid-trial tables cover different session/stimulus conditions")
    merged = presented.to_frame().join(valid)
    if (merged["min"] > merged["counts"]).any() or (merged["max"] > merged["counts"]).any():
        raise ValueError("Valid trial count exceeds presented count")
    merged["valid_counts"] = [
        str(int(low)) if low == high else f"{int(low)}–{int(high)}"
        for low, high in zip(merged["min"], merged["max"])
    ]
    return {
        str(session): frame.reset_index(level="session_label", drop=True).reset_index().rename(
            columns={"stim_type": "stimulus"}
        )[["stimulus", "counts", "valid_counts"]].reset_index(drop=True)
        for session, frame in merged.groupby(level="session_label", sort=True)
    }


def load_early_response_timing_qc(
    *,
    fish_id: str,
    fish_dir: str | Path,
    diagnostic_root: str | Path,
    scored_windows_path: str | Path,
    rounding_tolerance_frames: float = 0.5,
) -> dict[str, Any]:
    """Load Q1 evidence via independent recorded and scoring-window paths.

    The recorded events are re-resolved from raw session logs.  They are never
    reconstructed from the scoring sidecar, so a shared timing defect remains
    visible to the reviewer.
    """
    fish = _validate_fish_id(fish_id)
    root = _path(diagnostic_root, label="early-response diagnostic root")
    score_path = Path(scored_windows_path)
    warnings: list[str] = []
    source_path = root / "suite2p_stimulus_locked_sources_23b.csv"
    source_df = pd.DataFrame()
    if source_path.is_file():
        source_path, source_df = _csv(source_path, label="early-response sources")
        _validate_table_fish(source_df, fish, source_path)
    else:
        warnings.append(
            "Persisted [23b] source metadata is missing; raw-log timing can only be "
            "resolved when scored-window provenance supplies plane/session keys."
        )
    matrix_path = root / "suite2p_full_session_heatmap_matrix_23c.npy"
    rows_path = root / "suite2p_full_session_heatmap_rows_23c.csv"
    if matrix_path.is_file() and rows_path.is_file():
        heatmap_matrix = np.load(matrix_path, allow_pickle=False)
        _, heatmap_rows = _csv(rows_path, label="persisted full-session heatmap rows")
    else:
        heatmap_matrix = np.empty((0, 0), dtype=float)
        heatmap_rows = pd.DataFrame()
        warnings.append("Persisted full-session heatmap payload is missing; rerun [23c] to render independent recorded/scored overlays.")
    if score_path.is_file():
        score_path, scored_df = _csv(score_path, label="scored stimulus-window provenance")
        _validate_table_fish(scored_df, fish, score_path)
    else:
        scored_df = pd.DataFrame()
        warnings.append("Exact score-activity-bpi event windows are not persisted; rerun that writer before accepting timing alignment.")

    if source_df.empty and not scored_df.empty:
        fallback_columns = [column for column in ("fish_id", "plane_idx", "session_label") if column in scored_df]
        source_df = scored_df.loc[:, fallback_columns].drop_duplicates().copy()
        source_df["source_provenance"] = "score-window plane/session keys only; events resolved independently from raw logs"
        warnings.append(
            "Using score-window plane/session keys only because [23b] metadata is absent; "
            "recorded events are still reloaded from raw logs."
        )

    plane_values = source_df.get("plane_idx", pd.Series(dtype=float))
    plane_indices = pd.to_numeric(plane_values, errors="coerce").dropna().astype(int).unique().tolist()
    contexts = (
        resolve_plane_stimulus_contexts(
            fish_dir=Path(fish_dir), fish_id=fish, plane_indices=plane_indices, config=StimulusConfig()
        )
        if plane_indices
        else {}
    )
    recorded_rows: list[pd.DataFrame] = []
    for plane_idx, context in contexts.items():
        plane_rows = heatmap_rows[
            pd.to_numeric(heatmap_rows.get("plane_idx"), errors="coerce").eq(int(plane_idx))
        ] if not heatmap_rows.empty and "plane_idx" in heatmap_rows else pd.DataFrame()
        n_frame_values = (
            pd.to_numeric(plane_rows["n_frames"], errors="coerce").dropna()
            if "n_frames" in plane_rows
            else pd.Series(dtype=float)
        )
        score_plane = scored_df[
            pd.to_numeric(scored_df.get("plane_idx"), errors="coerce").eq(int(plane_idx))
        ] if not scored_df.empty and "plane_idx" in scored_df else pd.DataFrame()
        score_frame_values = (
            pd.to_numeric(score_plane["scored_offset_frame"], errors="coerce").dropna()
            if "scored_offset_frame" in score_plane
            else pd.Series(dtype=float)
        )
        n_frames = (
            int(n_frame_values.iloc[0])
            if len(n_frame_values)
            else int(heatmap_matrix.shape[1])
            if heatmap_matrix.ndim == 2 and heatmap_matrix.shape[1] > 0
            else int(score_frame_values.max())
            if len(score_frame_values)
            else 0
        )
        fps = pd.to_numeric(context.get("frame_rate", np.nan), errors="coerce")
        if not np.isfinite(fps) or float(fps) <= 0 or n_frames <= 0:
            warnings.append(f"Trace-grid timing could not be resolved for plane {plane_idx}.")
            continue
        mapped_context = map_stimulus_context_to_trace_grid(
            context,
            n_frames=n_frames,
            fps=float(fps),
            onset_delay_sec=10.0,
            tag="[Q1 timing audit]",
        )
        events = mapped_context.get("df_stim")
        if not isinstance(events, pd.DataFrame) or events.empty:
            warnings.append(f"No recorded stimulus events resolved for plane {plane_idx}.")
            continue
        frame = events.copy()
        frame["plane_idx"] = int(plane_idx)
        frame["session_label"] = str(context.get("session_label", ""))
        frame["recorded_fps"] = float(fps)
        frame["recorded_log_path"] = str(context.get("log_path", ""))
        recorded_rows.append(frame)
    recorded_df = pd.concat(recorded_rows, ignore_index=True) if recorded_rows else pd.DataFrame()
    if not recorded_df.empty:
        recorded_df["recorded_onset_frame"] = (pd.to_numeric(recorded_df["start"], errors="coerce") * recorded_df["recorded_fps"]).round().astype("Int64")
        recorded_df["recorded_offset_frame"] = (pd.to_numeric(recorded_df["end"], errors="coerce") * recorded_df["recorded_fps"]).round().astype("Int64")
    if not scored_df.empty and not recorded_df.empty:
        keys = [key for key in ("plane_idx", "session_label", "block", "stim_idx", "stim_type") if key in scored_df and (key in recorded_df or key == "stim_type" and "type" in recorded_df)]
        recorded_for_join = recorded_df.rename(columns={"type": "stim_type", "start": "recorded_log_onset_sec", "end": "recorded_log_offset_sec"})
        keys = [key for key in keys if key in recorded_for_join]
        timing_audit = scored_df.merge(
            recorded_for_join[[*keys, "recorded_log_onset_sec", "recorded_log_offset_sec", "recorded_fps", "recorded_log_path"]],
            on=keys, how="outer", indicator=True,
        )
        scored_input_onset = (
            timing_audit["recorded_onset_sec"]
            if "recorded_onset_sec" in timing_audit
            else timing_audit["scored_onset_sec"]
        )
        scored_input_offset = (
            timing_audit["recorded_offset_sec"]
            if "recorded_offset_sec" in timing_audit
            else timing_audit["scored_offset_sec"]
        )
        timing_audit["provenance_onset_sec"] = pd.to_numeric(scored_input_onset, errors="coerce")
        timing_audit["provenance_offset_sec"] = pd.to_numeric(scored_input_offset, errors="coerce")
        timing_audit["provenance_onset_delta_sec"] = timing_audit["provenance_onset_sec"] - pd.to_numeric(timing_audit.get("recorded_log_onset_sec"), errors="coerce")
        timing_audit["provenance_offset_delta_sec"] = timing_audit["provenance_offset_sec"] - pd.to_numeric(timing_audit.get("recorded_log_offset_sec"), errors="coerce")
        timing_audit["onset_delta_sec"] = pd.to_numeric(timing_audit.get("scored_onset_sec"), errors="coerce") - pd.to_numeric(timing_audit.get("recorded_log_onset_sec"), errors="coerce")
        timing_audit["offset_delta_sec"] = pd.to_numeric(timing_audit.get("scored_offset_sec"), errors="coerce") - pd.to_numeric(timing_audit.get("recorded_log_offset_sec"), errors="coerce")
        timing_audit["onset_delta_frames"] = timing_audit["onset_delta_sec"] * pd.to_numeric(timing_audit.get("recorded_fps"), errors="coerce")
        timing_audit["offset_delta_frames"] = timing_audit["offset_delta_sec"] * pd.to_numeric(timing_audit.get("recorded_fps"), errors="coerce")
        timing_audit["provenance_onset_delta_frames"] = timing_audit["provenance_onset_delta_sec"] * pd.to_numeric(timing_audit.get("recorded_fps"), errors="coerce")
        timing_audit["provenance_offset_delta_frames"] = timing_audit["provenance_offset_delta_sec"] * pd.to_numeric(timing_audit.get("recorded_fps"), errors="coerce")
        timing_audit["rounding_tolerance_frames"] = float(rounding_tolerance_frames)
        timing_audit["event_key_status"] = np.where(timing_audit["_merge"] == "both", "matched", timing_audit["_merge"])
        timing_audit["rounding_within_tolerance"] = (timing_audit[["provenance_onset_delta_frames", "provenance_offset_delta_frames"]].abs().max(axis=1) <= float(rounding_tolerance_frames))
        if (timing_audit["event_key_status"] == "matched").any() and not timing_audit.loc[timing_audit["event_key_status"] == "matched", "rounding_within_tolerance"].all():
            warnings.append("Score-provenance input onset or offset differs from the independently loaded raw log by more than the documented rounding tolerance.")
        if (timing_audit["_merge"] != "both").any():
            warnings.append("Some raw-log event types are outside the persisted score-window selection; inspect those explicit unscored rows before interpreting responses.")
    else:
        timing_audit = pd.DataFrame()
    return {
        "source_df": source_df,
        "heatmap_matrix": heatmap_matrix,
        "heatmap_rows": heatmap_rows,
        "recorded_events": recorded_df,
        "scored_windows": scored_df,
        "timing_audit": timing_audit,
        "warnings": tuple(warnings),
        "heatmap_path": root / "suite2p_stimulus_locked_heatmaps_23b.png",
        "traces_path": root / "suite2p_stimulus_locked_traces_23b.png",
    }


def load_early_response_trace_qc(*, fish_id: str, diagnostic_root: str | Path) -> dict[str, Any]:
    """Load numeric [23c] traces, rejecting a mixed-generation summary/payload."""
    root = _path(diagnostic_root, label="early-response diagnostic root")
    summary_path, summary = _csv(root / "suite2p_stimulus_locked_summary_23b.csv", label="stimulus-locked trace summary")
    _validate_table_fish(summary, _validate_fish_id(fish_id), summary_path)
    required = {
        "plane_idx", "func_label", "session_label", "stim_type",
        "any_presented_response_is_active", "any_presented_response_state",
    }
    if not required.issubset(summary.columns):
        raise ValueError(f"Missing [23c] trace classification columns: {sorted(required - set(summary.columns))}")
    payload_path = _path(root / "suite2p_stimulus_locked_trace_payload_23c.npz", label="numeric stimulus-locked trace payload")
    with np.load(payload_path, allow_pickle=False) as payload:
        traces = np.asarray(payload["traces"], dtype=np.float32)
        tvec = np.asarray(payload["tvec"], dtype=np.float32)
        summary_digest = str(payload["summary_sha256"].item())
        metadata = json.loads(str(payload["plot_metadata_json"].item()))
    if summary_digest != hashlib.sha256(summary_path.read_bytes()).hexdigest():
        raise RuntimeError("[23c] trace payload and summary are from different runs")
    if tvec.ndim != 1 or traces.shape != (len(summary), len(tvec)):
        raise RuntimeError("[23c] numeric trace payload does not align with summary rows or time axis")
    summary["mean_trace"] = list(traces)
    responsive = summary[summary["any_presented_response_is_active"].astype(str).str.lower().eq("true")].copy()
    nonresponsive = summary[summary["any_presented_response_state"].eq("nonresponsive")].copy()
    if responsive.empty:
        raise RuntimeError("[23c] no any-stimulus responders are available for trace QC")
    return {
        "trace_df": responsive,
        "nonresponsive_trace_df": nonresponsive,
        "tvec": tvec,
        "stim_order": metadata["stim_order"],
        "session_colors": {
            session: tuple(color) if isinstance(color, list) else color
            for session, color in metadata["session_colors"].items()
        },
        "duration_by_stim": metadata["duration_by_stim"],
        "motion_onset_delay_sec": float(metadata["motion_onset_delay_sec"]),
        "summary_path": summary_path,
        "payload_path": payload_path,
    }


def render_early_response_stimulus_traces(bundle: Mapping[str, Any]) -> plt.Figure:
    """Render the staged numeric trace payload with the stage-owned figure logic."""
    from .analysis import render_suite2p_stimulus_locked_trace_panels

    return render_suite2p_stimulus_locked_trace_panels(
        trace_df=bundle["trace_df"],
        nonresponsive_trace_df=bundle["nonresponsive_trace_df"],
        tvec=bundle["tvec"],
        stim_order=bundle["stim_order"],
        session_colors=bundle["session_colors"],
        duration_by_stim=bundle["duration_by_stim"],
        motion_onset_delay_sec=bundle["motion_onset_delay_sec"],
    )


def build_early_response_qc_from_raw(*, fish_id: str, fish_dir: str | Path) -> dict[str, Any]:
    """Rebuild the legacy [23c] review payload in memory from canonical inputs.

    This is a read-only QC fallback for older runs which completed response/BPI
    scoring but did not retain the separate [23b]/[23c] diagnostic files.  It
    does not write a diagnostic, score, or canonical table.
    """
    fish = _validate_fish_id(fish_id)
    resolved_fish_dir = _path(fish_dir, label="fish directory")
    suite2p_root = resolved_fish_dir / "03_analysis" / "functional" / "suite2P"
    from ..suite2p import (
        Suite2pStimulusLockedDiagnosticConfig,
        build_suite2p_stimulus_locked_diagnostic,
        load_suite2p_stage,
    )

    loaded = load_suite2p_stage(
        plane_refs=[], suite2p_root=suite2p_root, fish_id=fish,
    )
    diagnostic = build_suite2p_stimulus_locked_diagnostic(
        fish_dir=resolved_fish_dir,
        fish_id=fish,
        suite2p_by_ref_idx=loaded["suite2p_by_ref_idx"],
        config=Suite2pStimulusLockedDiagnosticConfig(save_figures=False, show_figures=False, verbose=False),
    )
    return {
        "source_df": diagnostic["source_df"],
        "heatmap_matrix": diagnostic["full_session_heatmap_matrix"],
        "heatmap_rows": diagnostic["full_session_heatmap_rows"],
        "stimulus_spans": diagnostic["full_session_stimulus_spans"],
        "block_starts": diagnostic["full_session_block_starts"],
        "trace_df": diagnostic["trace_df"],
        "tvec": diagnostic["tvec"],
        "stim_order": diagnostic["stim_order"],
        "session_colors": diagnostic["session_colors"],
        "duration_by_stim": diagnostic["duration_by_stim"],
        "suite2p_sources": loaded["df_src"],
        "warnings": (
            "Legacy [23b]/[23c] diagnostic files are absent; this view was rebuilt in memory "
            "from the canonical Suite2p arrays and raw stimulus logs without writing outputs.",
        ),
    }


def render_early_response_timing_audit(bundle: Mapping[str, Any]) -> plt.Figure:
    """Render the Q1 event-level recorded-versus-scored timing audit."""
    audit = bundle.get("timing_audit")
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.2))
    if not isinstance(audit, pd.DataFrame) or audit.empty:
        axes[0].text(0.5, 0.5, "No persisted scoring-window provenance", ha="center", va="center")
        axes[1].axis("off")
    else:
        matched = audit[audit["event_key_status"] == "matched"].copy()
        colors = pd.Categorical(matched.get("session_label", pd.Series(dtype=str))).codes
        axes[0].scatter(matched["recorded_log_onset_sec"], matched["provenance_onset_sec"], c=colors, cmap="tab10", s=30)
        finite = pd.concat([matched["recorded_log_onset_sec"], matched["provenance_onset_sec"]]).dropna()
        if not finite.empty:
            axes[0].plot([finite.min(), finite.max()], [finite.min(), finite.max()], "k--", linewidth=0.8, label="recorded onset")
        axes[0].set(xlabel="Raw-log onset (s)", ylabel="Score-provenance input onset (s)", title="Independent raw-log versus score input")
        axes[0].legend(frameon=False)
        axes[1].scatter(np.arange(len(matched)), matched["onset_delta_frames"], c=colors, cmap="tab10", s=30)
        axes[1].set(xlabel="Matched event", ylabel="Analysis onset delay (frames)", title="Scored window starts after documented delay")
    fig.suptitle("Early response timing audit: raw logs are loaded independently of scoring provenance")
    fig.tight_layout()
    return fig


def render_early_response_full_session_heatmap(bundle: Mapping[str, Any]) -> plt.Figure:
    """Render persisted Suite2p heatmap with independent recorded/scored spans."""
    matrix = np.asarray(bundle.get("heatmap_matrix"), dtype=float)
    rows = bundle.get("heatmap_rows")
    fig, ax = plt.subplots(figsize=(12.5, max(4.0, min(14.0, 2.6 + 0.018 * len(rows)))))
    if matrix.ndim != 2 or matrix.size == 0 or not isinstance(rows, pd.DataFrame) or rows.empty:
        ax.text(0.5, 0.5, "Persisted heatmap payload unavailable", transform=ax.transAxes, ha="center", va="center")
        return fig
    stimulus_spans = bundle.get("stimulus_spans")
    block_starts = bundle.get("block_starts")
    if isinstance(stimulus_spans, pd.DataFrame) and isinstance(block_starts, pd.DataFrame):
        from .analysis import render_suite2p_full_session_heatmap

        return render_suite2p_full_session_heatmap(
            matrix=matrix,
            row_df=rows,
            stimulus_spans=stimulus_spans,
            block_starts=block_starts,
            session_colors={},
        )
    cmap = plt.get_cmap("gray_r").copy(); cmap.set_bad("white")
    ax.imshow(np.ma.masked_invalid(matrix), aspect="auto", interpolation="nearest", cmap=cmap,
              vmin=0.0, vmax=5.0, extent=[0, matrix.shape[1], len(rows), 0])
    row_ranges = rows.reset_index().groupby("plane_idx", sort=False)["index"].agg(["min", "max"])
    for frame, color, label in ((bundle.get("recorded_events"), "#2ca25f", "recorded window"), (bundle.get("scored_windows"), "#d7301f", "scored window")):
        if not isinstance(frame, pd.DataFrame) or frame.empty or "plane_idx" not in frame:
            continue
        starts = "recorded_onset_frame" if label.startswith("recorded") else "scored_onset_frame"
        ends = "recorded_offset_frame" if label.startswith("recorded") else "scored_offset_frame"
        for event in frame.itertuples(index=False):
            plane = getattr(event, "plane_idx", None)
            if plane not in row_ranges.index:
                continue
            start, end = getattr(event, starts, np.nan), getattr(event, ends, np.nan)
            if pd.isna(start) or pd.isna(end) or end <= start:
                continue
            bounds = row_ranges.loc[plane]
            ax.axvspan(float(start), float(end), ymin=float(bounds["min"]) / len(rows), ymax=float(bounds["max"] + 1) / len(rows), color=color, alpha=0.22, label=label if not ax.get_legend_handles_labels()[1].count(label) else None)
    ax.set(xlabel="Frame", ylabel="Suite2p cell", title="Full-session activity with independent recorded and scored windows")
    ax.legend(frameon=False, loc="lower right")
    fig.tight_layout()
    return fig


def load_presented_stimulus_auc_qc(*, fish_id: str, diagnostic_root: str | Path) -> dict[str, pd.DataFrame]:
    """Read the independent [23c] AUC/call table and fixed responder membership."""
    root = _path(diagnostic_root, label="early-response diagnostic root")
    frames = {}
    for key, filename in (("presented", "suite2p_presented_stimulus_response_23c.csv"),
                          ("any_response", "suite2p_any_presented_response_23c.csv")):
        path, frame = _csv(root / filename, label=key)
        _validate_table_fish(frame, _validate_fish_id(fish_id), path)
        frames[key] = frame
    return frames


def render_presented_stimulus_auc_violins(presented: pd.DataFrame, any_response: pd.DataFrame, *, fish_id: str) -> plt.Figure:
    """Signed scorer-window AUCs, with saved calls and one fixed any-stimulus cohort.

    [23c] contains the stage-owned Suite2p-cell population; no response tests or
    per-stimulus responder subsets are reconstructed here. Missing AUCs remain
    in the n/call counts but cannot contribute to a violin or its median.
    """
    keys = ["plane_idx", "func_label"]
    required = {*keys, "session_label", "stim_type", "mean_auc_dff", "response_valence"}
    if missing := required - set(presented):
        raise ValueError(f"Presented AUC table lacks {sorted(missing)}")
    if missing := {*keys, "any_presented_response_is_active"} - set(any_response):
        raise ValueError(f"Any-presented response table lacks {sorted(missing)}")
    if presented.duplicated([*keys, "session_label", "stim_type"]).any() or any_response.duplicated(keys).any():
        raise ValueError("Duplicate presented-response or fixed-cohort keys")
    session_keys = presented[[*keys, "session_label"]].drop_duplicates()
    if session_keys.duplicated(keys).any():
        raise ValueError("An acquisition ROI belongs to multiple sessions")
    work = presented.copy()
    if "suite2p_is_cell" in work:
        work = work.loc[work["suite2p_is_cell"].astype(str).str.lower().isin(["true", "1"])].copy()
    work = work.merge(any_response[[*keys, "any_presented_response_is_active"]], on=keys,
                      how="left", validate="many_to_one", indicator=True)
    if not work["_merge"].eq("both").all():
        raise ValueError("Presented AUC rows are absent from the fixed any-stimulus cohort table")
    work["_responder"] = work["any_presented_response_is_active"].astype(str).str.lower().isin(["true", "1"])
    work["mean_auc_dff"] = pd.to_numeric(work["mean_auc_dff"], errors="coerce")
    call_order = ["activated", "suppressed", "not-significant", "unavailable"]
    if unknown := set(work["response_valence"].fillna("unavailable")) - set(call_order):
        raise ValueError(f"Unknown persisted presented-response calls: {sorted(unknown)}")
    sessions = sorted(work["session_label"].astype(str).unique())
    preferred = ["LB_trajectory", "RB_trajectory", "LC_trajectory", "RC_trajectory",
                 "LAB_trajectory", "RAB_trajectory", "Ll_RB_trajectory", "Rl_RB_trajectory", "WFCl", "WFCo"]
    available = set(work["stim_type"].astype(str))
    stimuli = [s for s in preferred if s in available] + sorted(available - set(preferred))
    if not sessions or not stimuli:
        raise ValueError("No presented AUC rows are available")
    fig, axes = plt.subplots(2, len(sessions), figsize=(10.5 * len(sessions), 11.5),
                             sharey="row", squeeze=False)
    finite = work["mean_auc_dff"][np.isfinite(work["mean_auc_dff"])]
    if finite.empty:
        raise ValueError("No finite presented AUCs are available")
    # Display bounds only: calls, cohort membership and medians retain all rows.
    row_views = []
    for row in range(2):
        values = work.loc[work["_responder"] if row else slice(None), "mean_auc_dff"].to_numpy()
        values = values[np.isfinite(values)]
        quantiles = np.quantile(values, [.005, .995]) if len(values) else [0., 0.]
        bound = max(float(np.max(np.abs(quantiles))), 1e-6)
        typical = float(np.median(np.abs(values))) if len(values) else bound
        linthresh = max(typical, bound * 1e-9)
        row_views.append((bound, linthresh, bound / linthresh > 15))
    colors = plt.get_cmap("tab10")
    for col, session in enumerate(sessions):
        for row in range(2):
            ax = axes[row, col]
            group = work.loc[work["session_label"].astype(str).eq(session) & (work["_responder"] if row else True)]
            bound, linthresh, use_symlog = row_views[row]
            if use_symlog:
                ax.set_yscale("symlog", linthresh=linthresh)
            for i, stimulus in enumerate(stimuli):
                sub = group.loc[group["stim_type"].eq(stimulus)]
                values = sub["mean_auc_dff"].dropna().to_numpy()
                values = values[np.isfinite(values)]
                visible = values[(values >= -bound) & (values <= bound)]
                above, below = int((values > bound).sum()), int((values < -bound).sum())
                if len(visible) > 1 and np.ptp(visible) > 0:
                    parts = ax.violinplot([visible], positions=[i], widths=.75, showextrema=False, showmedians=False)
                    parts["bodies"][0].set_facecolor(colors(i % 10)); parts["bodies"][0].set_alpha(.65)
                if len(values):
                    ax.plot(i, np.median(values), marker="_", markersize=18, color="#222222", zorder=3)
                median = f"{np.median(values):.3f}" if len(values) else "unavailable"
                counts = sub["response_valence"].fillna("unavailable").value_counts()
                a, s, ns, u = (int(counts.get(call, 0)) for call in call_order)
                ax.text(i, 1.025, f"n={len(sub)}\nmed={median}\nA {a} · S {s}\nNS {ns} · U {u}\n{above} above\n{below} below",
                        transform=ax.get_xaxis_transform(), ha="center", va="bottom", fontsize=7)
            ax.axhline(0, color="#444444", linewidth=.8)
            ax.set_ylim(-bound, bound)
            ax.set_xlim(-.6, len(stimuli) - .4)
            ax.set_xticks(range(len(stimuli)), [s.replace("_trajectory", "") for s in stimuli], rotation=35, ha="right", fontsize=8)
            ax.set_ylabel("Signed evoked AUC (dF/F·s)" + (f"\nsymlog; linthresh={linthresh:.4g}" if use_symlog else "\nlinear axis"))
            ax.set_title(f"{session} · " + ("Fixed any-presented-stimulus responders" if row else "All Suite2p cells"), fontsize=11, y=1.40)
            ax.spines[["top", "right"]].set_visible(False)
    fig.suptitle(f"{_validate_fish_id(fish_id)}: signed stimulus-evoked responses", fontsize=15, fontweight="bold", y=.99)
    fig.text(.5, .947, "12.5 s from motion onset, minus matched prestimulus pause; symmetric row-wise 0.5–99.5% quantile bounds\n"
             "Row 2 is selected by these same responses and is descriptive. A=activated; S=suppressed; NS=not significant; U=unavailable; medians use all finite AUCs. Above/below counts mark values omitted from violin bodies.",
             ha="center", va="top", fontsize=9)
    fig.subplots_adjust(top=.77, bottom=.10, left=.05, right=.985, wspace=.14, hspace=.85)
    return fig


def load_functional_registration_qc(
    *,
    fish_id: str,
    anatomy_preparation_manifest_path: str | Path | None,
    anatomy_stack_path: str | Path | None = None,
    registration_manifest_path: str | Path,
    roi_transform_manifest_path: str | Path,
    qc_manifest_path: str | Path,
    registration_root: str | Path,
    transformed_roi_root: str | Path,
    qc_root: str | Path,
) -> dict[str, Any]:
    """Load persisted anatomy-prep, registration, ROI-transform, and QC evidence.

    Historical runs may predate the anatomy-preparation manifest. In that case,
    the explicitly supplied in-vivo anatomy stack is retained as unmanifested
    provenance; the registration, transform, and QC manifests remain required.
    """
    fish = _validate_fish_id(fish_id)
    manifests: dict[str, Mapping[str, Any]] = {}
    manifest_rows: list[dict[str, Any]] = []
    manifest_paths: list[Path] = []
    if anatomy_preparation_manifest_path is not None and Path(anatomy_preparation_manifest_path).is_file():
        path, payload = _json(anatomy_preparation_manifest_path, label="anatomy preparation manifest")
        _validate_manifest_fish(payload, fish, path)
        manifests["anatomy preparation"] = payload
        manifest_rows.append(_manifest_row("anatomy preparation", path, payload))
        manifest_paths.append(path)
    else:
        if anatomy_stack_path is None:
            missing = Path(anatomy_preparation_manifest_path) if anatomy_preparation_manifest_path else None
            raise FileNotFoundError(f"Missing anatomy preparation manifest: {missing}")
        anatomy_path = _path(anatomy_stack_path, label="in-vivo anatomy stack")
        if not anatomy_path.name.startswith(fish):
            raise ValueError(f"In-vivo anatomy stack does not match fish {fish}: {anatomy_path}")
        manifests["anatomy preparation"] = {"status": "legacy_unmanifested", "anatomy_stack_path": str(anatomy_path)}
        manifest_rows.append({"artifact": "anatomy preparation", "path": str(anatomy_path), "status": "legacy_unmanifested", "generated_at": None})
        manifest_paths.append(anatomy_path)

    for label, value in (
        ("functional registration", registration_manifest_path),
        ("functional ROI transform", roi_transform_manifest_path),
        ("functional registration QC", qc_manifest_path),
    ):
        path, payload = _json(value, label=f"{label} manifest")
        _validate_manifest_fish(payload, fish, path)
        manifests[label] = payload
        manifest_rows.append(_manifest_row(label, path, payload))
        manifest_paths.append(path)

    registration_dir = _path(registration_root, label="functional registration stage directory")
    transformed_dir = _path(transformed_roi_root, label="transformed functional ROI stage directory")
    qc_dir = _path(qc_root, label="functional registration QC stage directory")
    plane_refs_path, plane_refs_payload = _json(stage_product_path(registration_dir, "register-functional-to-anatomy", "plane_refs_summary.json"), label="plane refs summary")
    if not isinstance(plane_refs_payload, list) or not plane_refs_payload:
        raise ValueError(f"Plane refs summary has no records: {plane_refs_path}")
    plane_refs = pd.DataFrame(plane_refs_payload)
    if "label" in plane_refs and not plane_refs["label"].astype(str).str.startswith(fish).all():
        raise ValueError(f"Plane refs summary contains a different fish: {plane_refs_path}")

    transforms_path, transforms = _csv(stage_product_path(registration_dir, "register-functional-to-anatomy", "registration/tforms_by_plane.csv"), label="per-plane transform table")
    ncc_path, ncc = _csv(stage_product_path(qc_dir, "make-functional-registration-qc", "qa/functional_ncc_depth_profiles.csv"), label="functional NCC review table")
    for path, frame in ((transforms_path, transforms), (ncc_path, ncc)):
        _validate_table_fish(frame, fish, path)

    qc_png_names = (
        "functional_anatomy_plane_qc_rows.png",
        "functional_anatomy_intensity_overlay_rows.png",
        "functional_anatomy_center_label_overlay_200px.png",
        "functional_ncc_depth_profiles.png",
    )
    qc_pngs = {name: _path(stage_product_path(qc_dir, "make-functional-registration-qc", "qa/" + name), label=f"registration QC PNG {name}") for name in qc_png_names}
    anatomy_label_dir = _path(stage_product_path(transformed_dir, "transform-functional-rois-to-anatomy", "functional/anatomy"), label="anatomy-space transformed ROI labels")
    transformed_labels = tuple(
        path
        for path in (tuple(sorted(visible_glob(anatomy_label_dir, "*.tif"))) + tuple(sorted(visible_glob(anatomy_label_dir, "*.tiff"))))
        if is_visible_path(path)
    )
    if not transformed_labels:
        raise FileNotFoundError(f"No anatomy-space transformed ROI label TIFFs found: {anatomy_label_dir}")

    label_rows = [
        {"plane_index": _plane_from_value(path.name), "path": str(path), "name": path.name}
        for path in transformed_labels
    ]
    artifacts = tuple(manifest_paths) + (plane_refs_path, transforms_path, ncc_path) + tuple(qc_pngs.values()) + transformed_labels
    return {
        "fish_id": fish,
        "manifests": manifests,
        "manifest_inventory": pd.DataFrame(manifest_rows),
        "plane_refs": plane_refs,
        "transforms": transforms,
        "ncc_profiles": ncc,
        "qc_pngs": qc_pngs,
        "qc_artifact_paths": (ncc_path, *qc_pngs.values()),
        "anatomy_stack_path": manifest_paths[0] if anatomy_preparation_manifest_path is None else Path(anatomy_stack_path) if anatomy_stack_path is not None else None,
        "transformed_labels": pd.DataFrame(label_rows),
        "artifact_paths": artifacts,
    }


def functional_registration_plane_review(bundle: Mapping[str, Any], *, plane_index: int) -> pd.DataFrame:
    """Return the persisted records associated with one functional plane."""
    plane = int(plane_index)
    parts: list[pd.DataFrame] = []
    for source, keys in (
        ("plane_refs", ("index", "plane_index", "plane_idx")),
        ("transforms", ("plane_index", "plane_idx", "plane")),
        ("ncc_profiles", ("plane_index", "plane_idx", "plane")),
        ("transformed_labels", ("plane_index",)),
    ):
        frame = pd.DataFrame(bundle[source])
        key = next((candidate for candidate in keys if candidate in frame.columns), None)
        if key is None:
            continue
        selected = frame[pd.to_numeric(frame[key], errors="coerce") == plane].copy()
        if not selected.empty:
            selected.insert(0, "source", source)
            parts.append(selected)
    if not parts:
        raise KeyError(f"No persisted functional-registration records found for plane {plane}")
    return pd.concat(parts, ignore_index=True, sort=False)


def render_functional_registration_plane(bundle: Mapping[str, Any], *, plane_index: int) -> plt.Figure:
    """Display one persisted anatomy-space ROI-label output with its review records."""
    plane = int(plane_index)
    records = functional_registration_plane_review(bundle, plane_index=plane)
    labels = pd.DataFrame(bundle["transformed_labels"])
    selected = labels[pd.to_numeric(labels["plane_index"], errors="coerce") == plane]
    if selected.empty:
        raise FileNotFoundError(f"No transformed functional ROI label TIFF found for plane {plane}")
    label_path = Path(str(selected.iloc[0]["path"]))
    image = np.asarray(tifffile.imread(label_path))
    image = np.squeeze(image)
    if image.ndim != 2 or image.size == 0:
        raise ValueError(f"Transformed functional ROI labels are not a non-empty 2D image: {label_path}")

    fig, axes = plt.subplots(1, 2, figsize=(12.0, 5.0), constrained_layout=True)
    axes[0].imshow(image, cmap="nipy_spectral", interpolation="nearest")
    axes[0].set_title(f"plane {plane}: anatomy-space functional ROI labels")
    axes[0].axis("off")
    axes[1].axis("off")
    lines = [f"{source}: {count} record(s)" for source, count in records["source"].value_counts().items()]
    ncc_rows = records[records["source"] == "ncc_profiles"]
    if not ncc_rows.empty:
        for key in ("best_z", "best_ncc", "n_z"):
            if key in ncc_rows and pd.notna(ncc_rows.iloc[0].get(key)):
                lines.append(f"{key}: {ncc_rows.iloc[0][key]}")
    lines.append(f"label output: {label_path.name}")
    axes[1].text(0.0, 1.0, "\n".join(lines), va="top", family="monospace", fontsize=9)
    fig.suptitle(f"{bundle['fish_id']}: plane {plane} registration evidence", fontsize=13, fontweight="bold")
    return fig


def _anatomy_stack_path(bundle: Mapping[str, Any]) -> Path:
    """Resolve the explicitly supplied in-vivo anatomy stack for an overlay view."""
    path = bundle.get("anatomy_stack_path")
    if path is None:
        manifest = bundle.get("manifests", {}).get("anatomy preparation", {})
        path = manifest.get("anatomy_stack_path") if isinstance(manifest, Mapping) else None
    if path is None:
        raise ValueError("An in-vivo anatomy stack is required to display anatomy-space ROI overlays")
    return _path(path, label="in-vivo anatomy stack")


def _read_anatomy_plane(bundle: Mapping[str, Any], *, plane_index: int) -> tuple[np.ndarray, np.ndarray, int, Path]:
    """Read one anatomy-space intensity and transformed-label plane."""
    plane = int(plane_index)
    labels = pd.DataFrame(bundle["transformed_labels"])
    selected = labels[pd.to_numeric(labels["plane_index"], errors="coerce") == plane]
    if selected.empty:
        raise FileNotFoundError(f"No transformed functional ROI label TIFF found for plane {plane}")
    label_path = Path(str(selected.iloc[0]["path"]))
    label_image = np.squeeze(np.asarray(tifffile.imread(label_path)))
    if label_image.ndim != 2 or label_image.size == 0:
        raise ValueError(f"Transformed functional ROI labels are not a non-empty 2D image: {label_path}")

    refs = pd.DataFrame(bundle["plane_refs"])
    key = next((name for name in ("index", "plane_index", "plane_idx") if name in refs.columns), None)
    if key is None:
        raise ValueError("Plane-reference table has no plane-index column")
    ref = refs[pd.to_numeric(refs[key], errors="coerce") == plane]
    if ref.empty or "best_z" not in ref.columns:
        raise KeyError(f"No best anatomy Z is recorded for plane {plane}")
    best_z = int(ref.iloc[0]["best_z"])
    anatomy = np.asarray(imread_any(_anatomy_stack_path(bundle)))
    if anatomy.ndim != 3 or not 0 <= best_z < anatomy.shape[0]:
        raise ValueError(f"Anatomy stack has no valid Z={best_z} plane for functional plane {plane}")
    anatomy_plane = np.squeeze(anatomy[best_z])
    if anatomy_plane.ndim != 2 or anatomy_plane.shape != label_image.shape:
        raise ValueError(
            f"Anatomy and transformed-label shapes differ for plane {plane}: "
            f"{anatomy_plane.shape} versus {label_image.shape}"
        )
    return anatomy_plane, label_image, best_z, label_path


def _label_boundaries(labels: np.ndarray) -> np.ndarray:
    """Return one-pixel boundaries for non-zero labels without recolouring them."""
    image = np.asarray(labels)
    boundary = np.zeros(image.shape, dtype=bool)
    boundary[1:, :] |= (image[1:, :] != image[:-1, :]) & ((image[1:, :] != 0) | (image[:-1, :] != 0))
    boundary[:, 1:] |= (image[:, 1:] != image[:, :-1]) & ((image[:, 1:] != 0) | (image[:, :-1] != 0))
    return boundary


def _draw_anatomy_roi_overlay(ax: plt.Axes, anatomy: np.ndarray, labels: np.ndarray, *, title: str) -> None:
    ax.imshow(norm01(anatomy), cmap="gray", interpolation="nearest")
    overlay = np.ma.masked_where(~_label_boundaries(labels), _label_boundaries(labels))
    ax.imshow(overlay, cmap="spring", interpolation="nearest", alpha=0.95)
    ax.set_title(title, fontsize=10)
    ax.axis("off")


def render_functional_registration_all_planes(bundle: Mapping[str, Any]) -> plt.Figure:
    """Show every transformed ROI-label plane over its matched anatomy image."""
    labels = pd.DataFrame(bundle["transformed_labels"])
    plane_indices = sorted(pd.to_numeric(labels["plane_index"], errors="coerce").dropna().astype(int).unique())
    if not plane_indices:
        raise ValueError("No transformed functional ROI planes are available")
    ncols = 2
    nrows = int(np.ceil(len(plane_indices) / ncols))
    fig, axes = plt.subplots(nrows, ncols, figsize=(14.0, 6.8 * nrows), squeeze=False, constrained_layout=True)
    for ax, plane in zip(axes.flat, plane_indices):
        anatomy, transformed_labels, best_z, _ = _read_anatomy_plane(bundle, plane_index=plane)
        _draw_anatomy_roi_overlay(ax, anatomy, transformed_labels, title=f"Functional plane {plane} on anatomy Z={best_z}")
    for ax in axes.flat[len(plane_indices) :]:
        ax.axis("off")
    fig.suptitle(f"{bundle['fish_id']}: transformed ROI outlines in anatomy space", fontsize=13, fontweight="bold")
    return fig


def show_functional_anatomy_zoom_viewer(
    bundle: Mapping[str, Any], *, initial_plane_index: int | None = None, enable_widgets: bool = True
) -> dict[str, Any]:
    """Show a plane chooser with scroll-wheel zoom for anatomy-space ROI outlines."""
    labels = pd.DataFrame(bundle["transformed_labels"])
    planes = sorted(pd.to_numeric(labels["plane_index"], errors="coerce").dropna().astype(int).unique())
    if not planes:
        raise ValueError("No transformed functional ROI planes are available")
    initial = int(initial_plane_index) if initial_plane_index is not None else int(planes[0])
    if initial not in planes:
        raise KeyError(f"No transformed functional ROI labels are available for plane {initial}")

    fig, ax = plt.subplots(figsize=(9.0, 9.0), constrained_layout=True)
    state: dict[str, Any] = {"plane": initial, "limits": None}

    def _render(plane: int, *, keep_limits: bool = False) -> None:
        anatomy, transformed_labels, best_z, _ = _read_anatomy_plane(bundle, plane_index=plane)
        limits = (ax.get_xlim(), ax.get_ylim()) if keep_limits else None
        ax.clear()
        _draw_anatomy_roi_overlay(ax, anatomy, transformed_labels, title=f"Functional plane {plane} on anatomy Z={best_z}")
        if limits is not None:
            ax.set_xlim(*limits[0])
            ax.set_ylim(*limits[1])
        state["plane"] = int(plane)

    def _zoom(event: Any) -> None:
        if event.inaxes is not ax or event.xdata is None or event.ydata is None:
            return
        scale = 0.8 if event.button == "up" else 1.25
        x0, x1 = ax.get_xlim()
        y0, y1 = ax.get_ylim()
        ax.set_xlim(event.xdata - (event.xdata - x0) * scale, event.xdata + (x1 - event.xdata) * scale)
        ax.set_ylim(event.ydata - (event.ydata - y0) * scale, event.ydata + (y1 - event.ydata) * scale)
        fig.canvas.draw_idle()

    _render(initial)
    fig.canvas.mpl_connect("scroll_event", _zoom)
    result: dict[str, Any] = {"figure": fig, "interactive": False, "plane_indices": tuple(planes)}
    if not enable_widgets:
        return result
    try:
        import ipywidgets as widgets
        from IPython.display import display
    except Exception:
        return result
    plane_widget = widgets.Dropdown(options=planes, value=initial, description="Plane")
    reset_button = widgets.Button(description="Reset zoom")

    def _change_plane(change: Mapping[str, Any]) -> None:
        if change.get("name") == "value":
            _render(int(change["new"]))
            fig.canvas.draw_idle()

    def _reset(_button: Any) -> None:
        _render(int(state["plane"]))
        fig.canvas.draw_idle()

    plane_widget.observe(_change_plane, names="value")
    reset_button.on_click(_reset)
    display(widgets.HBox([plane_widget, reset_button]))
    result.update({"interactive": True, "widgets": {"plane": plane_widget, "reset_zoom": reset_button}})
    return result


def render_functional_registration_artifacts(
    bundle: Mapping[str, Any],
    *,
    artifact_names: Iterable[str] | None = None,
) -> plt.Figure:
    """Display existing registration-QC PNGs without regenerating them."""
    pngs = dict(bundle["qc_pngs"])
    names = list(artifact_names) if artifact_names is not None else list(pngs)
    unknown = [name for name in names if name not in pngs]
    if unknown:
        raise KeyError(f"Unknown registration QC artifacts: {unknown}")
    if not names:
        raise ValueError("At least one registration QC artifact must be selected")
    single_artifact = len(names) == 1
    images = {name: mpimg.imread(pngs[name]) for name in names}
    empty = [name for name, image in images.items() if image.size == 0]
    if empty:
        raise ValueError(f"Registration QC image is empty: {pngs[empty[0]]}")
    if single_artifact:
        image = images[names[0]]
        figsize = (14.0, max(4.0, 14.0 * image.shape[0] / image.shape[1]))
    else:
        figsize = (14.0, 6.0 * len(names))
    fig, axes = plt.subplots(len(names), 1, figsize=figsize, squeeze=False, constrained_layout=True)
    for ax, name in zip(axes[:, 0], names):
        ax.imshow(images[name])
        if not single_artifact:
            ax.set_title(name.replace("_", " ").removesuffix(".png"))
        ax.axis("off")
    if not single_artifact:
        fig.suptitle(f"{bundle['fish_id']}: persisted functional-registration evidence", fontsize=13, fontweight="bold")
    return fig


__all__ = [
    "build_early_response_trial_count_tables",
    "build_early_response_qc_from_raw",
    "functional_registration_plane_review",
    "load_functional_reference_drift_qc",
    "load_functional_registration_qc",
    "render_functional_reference_drift_summary",
    "render_early_response_timing_audit",
    "render_early_response_full_session_heatmap",
    "load_presented_stimulus_auc_qc",
    "render_presented_stimulus_auc_violins",
    "render_functional_reference_artifacts",
    "render_functional_registration_plane",
    "render_functional_registration_all_planes",
    "render_functional_registration_artifacts",
    "show_functional_anatomy_zoom_viewer",
]
