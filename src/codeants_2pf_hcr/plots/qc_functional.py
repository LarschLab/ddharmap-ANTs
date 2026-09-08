"""Read-only notebook views for functional-reference, drift, and registration QC.

The builders in this module consume persisted manifests, tables, and PNGs.  They
never run a pipeline stage and never write analysis products.
"""

from __future__ import annotations

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
from ..stimulus import StimulusConfig, resolve_plane_stimulus_contexts


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
        events = context.get("df_stim")
        if not isinstance(events, pd.DataFrame) or events.empty:
            warnings.append(f"No recorded stimulus events resolved for plane {plane_idx}.")
            continue
        frame = events.copy()
        frame["plane_idx"] = int(plane_idx)
        frame["session_label"] = str(context.get("session_label", ""))
        frame["recorded_fps"] = pd.to_numeric(context.get("frame_rate", np.nan), errors="coerce")
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
        timing_audit["onset_delta_sec"] = pd.to_numeric(timing_audit.get("scored_onset_sec"), errors="coerce") - pd.to_numeric(timing_audit.get("recorded_log_onset_sec"), errors="coerce")
        timing_audit["offset_delta_sec"] = pd.to_numeric(timing_audit.get("scored_offset_sec"), errors="coerce") - pd.to_numeric(timing_audit.get("recorded_log_offset_sec"), errors="coerce")
        timing_audit["onset_delta_frames"] = timing_audit["onset_delta_sec"] * pd.to_numeric(timing_audit.get("recorded_fps"), errors="coerce")
        timing_audit["offset_delta_frames"] = timing_audit["offset_delta_sec"] * pd.to_numeric(timing_audit.get("recorded_fps"), errors="coerce")
        timing_audit["rounding_tolerance_frames"] = float(rounding_tolerance_frames)
        timing_audit["event_key_status"] = np.where(timing_audit["_merge"] == "both", "matched", timing_audit["_merge"])
        timing_audit["rounding_within_tolerance"] = (timing_audit[["onset_delta_frames", "offset_delta_frames"]].abs().max(axis=1) <= float(rounding_tolerance_frames))
        if (timing_audit["event_key_status"] == "matched").any() and not timing_audit.loc[timing_audit["event_key_status"] == "matched", "rounding_within_tolerance"].all():
            warnings.append("Recorded/scored onset or offset differs by more than the documented rounding tolerance.")
        if (timing_audit["_merge"] != "both").any():
            warnings.append("Recorded/scored event keys disagree; inspect unmatched rows before interpreting responses.")
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
        axes[0].scatter(matched["recorded_log_onset_sec"], matched["scored_onset_sec"], c=colors, cmap="tab10", s=30)
        finite = pd.concat([matched["recorded_log_onset_sec"], matched["scored_onset_sec"]]).dropna()
        if not finite.empty:
            axes[0].plot([finite.min(), finite.max()], [finite.min(), finite.max()], "k--", linewidth=0.8, label="recorded onset")
        axes[0].set(xlabel="Recorded onset (s)", ylabel="Scored-window onset (s)", title="Recorded and scored event timing")
        axes[0].legend(frameon=False)
        axes[1].axhline(0, color="black", linewidth=0.8)
        axes[1].scatter(np.arange(len(matched)), matched["onset_delta_frames"], c=colors, cmap="tab10", s=30)
        axes[1].set(xlabel="Matched event", ylabel="Scored − recorded onset (frames)", title="Analysis delay and rounding are explicit")
    fig.suptitle("Early response timing audit: raw logs are loaded independently of scoring provenance")
    fig.tight_layout()
    return fig


def render_early_response_full_session_heatmap(bundle: Mapping[str, Any]) -> plt.Figure:
    """Render persisted Suite2p heatmap with independent recorded/scored spans."""
    matrix = np.asarray(bundle.get("heatmap_matrix"), dtype=float)
    rows = bundle.get("heatmap_rows")
    fig, ax = plt.subplots(figsize=(12.5, max(4.0, 2.6 + 0.018 * len(rows))))
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


def render_stimulus_type_mean_dff(summary_path: str | Path, *, fish_id: str) -> plt.Figure:
    """Render legacy-[23c] all-Suite2p-cell mean post-stimulus dF/F by type."""
    path, summary = _csv(summary_path, label="stimulus-locked response summary")
    _validate_table_fish(summary, _validate_fish_id(fish_id), path)
    return render_stimulus_type_mean_dff_frame(summary, fish_id=fish_id)


def render_stimulus_type_mean_dff_frame(summary: pd.DataFrame, *, fish_id: str) -> plt.Figure:
    """Render mean post-stimulus dF/F from a persisted or in-memory [23c] table."""
    fish = _validate_fish_id(fish_id)
    fig, ax = plt.subplots(figsize=(max(6.0, 1.2 * summary.get("stim_type", pd.Series()).nunique()), 4.5))
    if not {"stim_type", "mean_dff_post"}.issubset(summary.columns):
        ax.text(0.5, 0.5, "Persisted mean dF/F is unavailable; rerun [23c] with current code.", ha="center", va="center", transform=ax.transAxes)
        ax.set_axis_off()
        return fig
    grouped = summary.groupby("stim_type", sort=True)["mean_dff_post"].agg(["mean", "sem", "count"]).reset_index()
    ax.bar(grouped["stim_type"], grouped["mean"], yerr=grouped["sem"].fillna(0), color="#4c78a8", capsize=3)
    for index, row in grouped.iterrows():
        ax.text(index, row["mean"], f"n={int(row['count'])}", ha="center", va="bottom", fontsize=8)
    ax.axhline(0, color="#444444", linewidth=0.8)
    ax.set(xlabel="Stimulus type", ylabel="Mean post-stimulus dF/F", title="All Suite2p cells; per-cell stimulus means")
    ax.tick_params(axis="x", rotation=30)
    fig.suptitle(f"{fish}: mean dF/F by stimulus type", fontweight="bold")
    fig.tight_layout()
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
    plane_refs_path, plane_refs_payload = _json(registration_dir / "plane_refs_summary.json", label="plane refs summary")
    if not isinstance(plane_refs_payload, list) or not plane_refs_payload:
        raise ValueError(f"Plane refs summary has no records: {plane_refs_path}")
    plane_refs = pd.DataFrame(plane_refs_payload)
    if "label" in plane_refs and not plane_refs["label"].astype(str).str.startswith(fish).all():
        raise ValueError(f"Plane refs summary contains a different fish: {plane_refs_path}")

    transforms_path, transforms = _csv(registration_dir / "registration" / "tforms_by_plane.csv", label="per-plane transform table")
    ncc_path, ncc = _csv(qc_dir / "qa" / "functional_ncc_depth_profiles.csv", label="functional NCC review table")
    for path, frame in ((transforms_path, transforms), (ncc_path, ncc)):
        _validate_table_fish(frame, fish, path)

    qc_png_names = (
        "functional_anatomy_plane_qc_rows.png",
        "functional_anatomy_intensity_overlay_rows.png",
        "functional_anatomy_center_label_overlay_200px.png",
        "functional_ncc_depth_profiles.png",
    )
    qc_pngs = {name: _path(qc_dir / "qa" / name, label=f"registration QC PNG {name}") for name in qc_png_names}
    anatomy_label_dir = _path(transformed_dir / "functional" / "anatomy", label="anatomy-space transformed ROI labels")
    transformed_labels = tuple(
        path
        for path in (tuple(sorted(anatomy_label_dir.glob("*.tif"))) + tuple(sorted(anatomy_label_dir.glob("*.tiff"))))
        if not path.name.startswith("._")
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
    fig, axes = plt.subplots(len(names), 1, figsize=(14.0, 6.0 * len(names)), squeeze=False, constrained_layout=True)
    for ax, name in zip(axes[:, 0], names):
        image = mpimg.imread(pngs[name])
        if image.size == 0:
            raise ValueError(f"Registration QC image is empty: {pngs[name]}")
        ax.imshow(image)
        ax.set_title(name.replace("_", " ").removesuffix(".png"))
        ax.axis("off")
    fig.suptitle(f"{bundle['fish_id']}: persisted functional-registration evidence", fontsize=13, fontweight="bold")
    return fig


__all__ = [
    "build_early_response_qc_from_raw",
    "functional_registration_plane_review",
    "load_functional_reference_drift_qc",
    "load_functional_registration_qc",
    "render_functional_reference_drift_summary",
    "render_early_response_timing_audit",
    "render_early_response_full_session_heatmap",
    "render_stimulus_type_mean_dff",
    "render_stimulus_type_mean_dff_frame",
    "render_functional_reference_artifacts",
    "render_functional_registration_plane",
    "render_functional_registration_all_planes",
    "render_functional_registration_artifacts",
    "show_functional_anatomy_zoom_viewer",
]
