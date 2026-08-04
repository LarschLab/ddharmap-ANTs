"""Time-resolved NCC diagnostic for within-session functional Z drift."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from itertools import permutations
import json
from pathlib import Path
import re
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import SimpleITK as sitk
import tifffile

from .spatial import (
    apply_func_orientation,
    best_z_by_ncc,
    local_unsharp,
    norm01,
    registration_metric_from_scores,
    scale_image,
    top_correlated_mean,
)


@dataclass(frozen=True)
class FunctionalZDriftConfig:
    intervals_per_session: int = 6
    sampled_frames_per_interval: int = 80
    top_correlated_frames: int = 20
    top_corr_pre_smooth_sigma: float = 0.5
    sharpen_sigma: float = 1.0
    sharpen_amount: float = 0.6
    use_cv2: bool = False
    flip_x: bool = True
    min_consensus_change_slices: float = 2.0
    min_plane_direction_fraction: float = 0.6


def interval_bounds(frame_count: int, interval_count: int) -> tuple[tuple[int, int], ...]:
    if frame_count < 1:
        raise ValueError("frame_count must be positive")
    if interval_count < 2:
        raise ValueError("interval_count must be at least 2")
    if interval_count > frame_count:
        raise ValueError("interval_count cannot exceed frame_count")
    edges = np.linspace(0, frame_count, interval_count + 1, dtype=int)
    return tuple((int(edges[idx]), int(edges[idx + 1])) for idx in range(interval_count))


def interval_block_labels(
    *,
    excluded_block_frame_count: int,
    retained_frame_count: int,
    interval_count: int,
) -> tuple[str, ...]:
    """Name retained windows by acquisition block and within-block position."""
    if excluded_block_frame_count <= 0:
        raise ValueError("A positive excluded-block frame count is required for block-relative labels")
    if retained_frame_count % excluded_block_frame_count != 0:
        raise ValueError("Retained frames do not contain an integer number of acquisition blocks")
    retained_block_count = retained_frame_count // excluded_block_frame_count
    if retained_block_count < 1 or interval_count % retained_block_count != 0:
        raise ValueError("Intervals do not divide evenly across retained acquisition blocks")
    windows_per_block = interval_count // retained_block_count
    if windows_per_block == 2:
        positions = ("first half", "second half")
    elif windows_per_block == 3:
        positions = ("first third", "middle third", "final third")
    else:
        positions = tuple(f"window {index + 1}/{windows_per_block}" for index in range(windows_per_block))
    return tuple(
        f"Block {block_index + 1}\n{positions[window_index]}"
        for block_index in range(retained_block_count)
        for window_index in range(windows_per_block)
    )


def quadratic_peak_z(scores: np.ndarray) -> float:
    values = np.asarray(scores, dtype=np.float64)
    peak = int(np.argmax(values))
    if peak == 0 or peak == values.size - 1:
        return float(peak)
    left, center, right = values[peak - 1 : peak + 2]
    denom = left - (2.0 * center) + right
    if not np.isfinite(denom) or abs(denom) < 1e-12:
        return float(peak)
    offset = 0.5 * (left - right) / denom
    return float(peak + np.clip(offset, -1.0, 1.0))


def load_plane_scales(scale_cache_path: str | Path, fish_id: str) -> dict[int, tuple[str, float]]:
    payload = json.loads(Path(scale_cache_path).read_text())
    try:
        planes = payload["per_fish"][fish_id]["planes"]
    except (KeyError, TypeError) as exc:
        raise ValueError(f"Scale cache has no per-plane entry for {fish_id}") from exc
    result: dict[int, tuple[str, float]] = {}
    for label, record in planes.items():
        match = re.search(r"plane(\d+)", str(label))
        if match is None or not isinstance(record, dict) or "scale" not in record:
            continue
        result[int(match.group(1))] = (str(label), float(record["scale"]))
    if not result:
        raise ValueError(f"Scale cache has no parseable plane scales for {fish_id}")
    return result


def load_session_map(metadata_path: str | Path) -> dict[int, str]:
    payload = json.loads(Path(metadata_path).read_text())
    result: dict[int, str] = {}
    for session in payload.get("sessions", []):
        if not isinstance(session, dict):
            continue
        label = str(session.get("session_label") or f"session{session.get('session_number', '')}")
        for plane in session.get("output_planes", []):
            result[int(plane)] = label
    if not result:
        raise ValueError(f"No session-to-plane mapping found in {metadata_path}")
    return result


def load_functional_polarity(manifest_path: str | Path, fish_id: str) -> tuple[str, str]:
    """Load the accepted fish orientation from a functional-reference stage manifest."""
    target = Path(manifest_path)
    payload = json.loads(target.read_text())
    manifest_fish = str(payload.get("fish_id", "")).strip()
    if manifest_fish != str(fish_id):
        raise ValueError(
            f"Functional-reference manifest fish mismatch: expected {fish_id}, got {manifest_fish or '<missing>'}"
        )
    status = str(payload.get("status", "")).strip().lower()
    if status != "pass":
        raise ValueError(f"Functional-reference manifest is not accepted/pass: {target} (status={status or '<missing>'})")
    parameters = payload.get("parameters")
    polarity = str(parameters.get("polarity", "")).strip().lower() if isinstance(parameters, dict) else ""
    if polarity not in {"north", "south"}:
        raise ValueError(f"Functional-reference manifest has no valid north/south polarity: {target}")
    source = str(parameters.get("polarity_source", "")).strip() if isinstance(parameters, dict) else ""
    return polarity, source or str(target)


def load_functional_frame_selection(manifest_path: str | Path, fish_id: str) -> dict[int, dict[str, Any]]:
    """Load the accepted per-plane post-block-0 frame selection from preparation provenance."""
    target = Path(manifest_path)
    payload = json.loads(target.read_text())
    manifest_fish = str(payload.get("fish_id", "")).strip()
    if manifest_fish != str(fish_id):
        raise ValueError(
            f"Functional-reference manifest fish mismatch: expected {fish_id}, got {manifest_fish or '<missing>'}"
        )
    status = str(payload.get("status", "")).strip().lower()
    if status != "pass":
        raise ValueError(f"Functional-reference manifest is not accepted/pass: {target} (status={status or '<missing>'})")
    parameters = payload.get("parameters")
    if not isinstance(parameters, dict) or parameters.get("exclude_first_block") is not True:
        raise ValueError(f"Functional-reference manifest does not require first-block exclusion: {target}")
    records = parameters.get("frame_selection_by_plane")
    if not isinstance(records, dict) or not records:
        raise ValueError(f"Functional-reference manifest has no per-plane frame-selection provenance: {target}")
    result: dict[int, dict[str, Any]] = {}
    for label, record in records.items():
        match = re.search(r"plane(\d+)", str(label))
        if match is None or not isinstance(record, dict):
            continue
        decision = str(record.get("decision", ""))
        if decision not in {"excluded_first_selected_tiff_block", "first_block_already_excluded_upstream"}:
            raise ValueError(f"Plane {match.group(1)} has unaccepted frame-selection decision: {decision or '<missing>'}")
        try:
            frame_start = int(record["frame_start"])
            source_frame_count = int(record["source_frame_count"])
            reference_frame_count = int(record["reference_frame_count"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"Plane {match.group(1)} has incomplete frame-selection provenance") from exc
        if frame_start < 0 or source_frame_count <= frame_start or reference_frame_count != source_frame_count - frame_start:
            raise ValueError(f"Plane {match.group(1)} has inconsistent frame-selection provenance")
        result[int(match.group(1))] = {
            "decision": decision,
            "frame_start": frame_start,
            "source_frame_count": source_frame_count,
            "reference_frame_count": reference_frame_count,
        }
    if not result:
        raise ValueError(f"Functional-reference manifest has no parseable per-plane frame selections: {target}")
    return result


def _sample_indices(start: int, stop: int, sample_count: int) -> np.ndarray:
    count = min(int(sample_count), int(stop - start))
    if count < 1:
        raise ValueError(f"Empty interval {start}:{stop}")
    return np.linspace(start, stop - 1, count, dtype=int)


def _interval_reference(
    movie: Any,
    start: int,
    stop: int,
    config: FunctionalZDriftConfig,
    *,
    polarity: str,
) -> tuple[np.ndarray, int]:
    indices = _sample_indices(start, stop, config.sampled_frames_per_interval)
    sampled = np.asarray(movie[indices], dtype=np.float32)
    reference, _, _ = top_correlated_mean(
        sampled,
        take_k=min(config.top_correlated_frames, sampled.shape[0]),
        pre_smooth_sigma=config.top_corr_pre_smooth_sigma,
    )
    oriented = apply_func_orientation(reference, polarity=polarity, flip_x=config.flip_x)
    return np.asarray(oriented, dtype=np.float32), int(indices.size)


def _read_anatomy_zyx(path: str | Path) -> np.ndarray:
    """Read anatomy in the Z,Y,X convention used by the registration stage."""
    target = Path(path)
    if target.suffix.lower() == ".nrrd":
        return np.asarray(sitk.GetArrayFromImage(sitk.ReadImage(str(target))))
    array = np.asarray(tifffile.imread(target))
    if array.ndim != 3:
        raise ValueError(f"Expected a three-dimensional anatomy stack, got {array.shape}: {target}")
    return array


def _slope(values: np.ndarray) -> float:
    x = np.arange(len(values), dtype=np.float64)
    return float(np.polyfit(x, np.asarray(values, dtype=np.float64), 1)[0])


def _exact_slope_pvalue(values: np.ndarray) -> float:
    values = np.asarray(values, dtype=np.float64)
    observed = abs(_slope(values))
    if values.size > 8:
        return float("nan")
    null = [abs(_slope(np.asarray(order, dtype=np.float64))) for order in permutations(values.tolist())]
    return float((1 + sum(value >= observed - 1e-12 for value in null)) / (1 + len(null)))


def summarize_session_drift(interval_df: pd.DataFrame, config: FunctionalZDriftConfig) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for (fish_id, session), group in interval_df.groupby(["fish_id", "session"], sort=True):
        pivot = group.pivot(index="interval_index", columns="plane_index", values="best_z_subslice").sort_index()
        centered = pivot - pivot.median(axis=0)
        consensus = centered.median(axis=1).to_numpy(dtype=np.float64)
        plane_changes = pivot.iloc[-1] - pivot.iloc[0]
        consensus_change = float(consensus[-1] - consensus[0])
        direction = np.sign(consensus_change)
        if direction == 0:
            direction_fraction = float(np.mean(np.isclose(plane_changes.to_numpy(dtype=float), 0.0)))
        else:
            direction_fraction = float(np.mean(np.sign(plane_changes.to_numpy(dtype=float)) == direction))
        pvalue = _exact_slope_pvalue(consensus)
        post_initial = consensus[1:]
        post_initial_changes = (pivot.iloc[-1] - pivot.iloc[1]).to_numpy(dtype=float)
        post_initial_change = float(post_initial[-1] - post_initial[0])
        post_initial_direction = np.sign(post_initial_change)
        post_initial_direction_fraction = float(
            np.mean(np.sign(post_initial_changes) == post_initial_direction)
        ) if post_initial_direction != 0 else float(np.mean(np.isclose(post_initial_changes, 0.0)))
        post_initial_pvalue = _exact_slope_pvalue(post_initial)
        post_initial_change_iqr = float(
            np.percentile(post_initial_changes, 75) - np.percentile(post_initial_changes, 25)
        )
        initial_step = float(consensus[1] - consensus[0])
        post_initial_range = float(np.ptp(post_initial))
        full_range = float(np.ptp(consensus))
        if (
            abs(post_initial_change) >= config.min_consensus_change_slices
            and post_initial_direction_fraction >= 0.8
            and post_initial_change_iqr <= config.min_consensus_change_slices
            and (not np.isfinite(post_initial_pvalue) or post_initial_pvalue <= 0.05)
        ):
            tier = "coherent_progressive_drift"
        elif (
            abs(initial_step) >= config.min_consensus_change_slices
            and post_initial_range < config.min_consensus_change_slices
            and abs(post_initial_change) < config.min_consensus_change_slices
        ):
            tier = "initial_transient_then_stable"
        elif full_range >= config.min_consensus_change_slices:
            tier = "complex_or_heterogeneous_z_change"
        else:
            tier = "stable_no_drift_evidence"
        rows.append(
            {
                "fish_id": fish_id,
                "session": session,
                "plane_count": int(pivot.shape[1]),
                "interval_count": int(pivot.shape[0]),
                "consensus_change_slices": consensus_change,
                "consensus_slope_slices_per_interval": _slope(consensus),
                "exact_order_permutation_p": pvalue,
                "same_direction_plane_fraction": direction_fraction,
                "max_plane_abs_change_slices": float(np.max(np.abs(plane_changes.to_numpy(dtype=float)))),
                "initial_step_slices": initial_step,
                "post_initial_change_slices": post_initial_change,
                "post_initial_slope_slices_per_interval": _slope(post_initial),
                "post_initial_exact_order_permutation_p": post_initial_pvalue,
                "post_initial_same_direction_plane_fraction": post_initial_direction_fraction,
                "post_initial_plane_change_iqr_slices": post_initial_change_iqr,
                "post_initial_range_slices": post_initial_range,
                "full_consensus_range_slices": full_range,
                "evidence_tier": tier,
            }
        )
    return pd.DataFrame(rows)


def _render_tracks(interval_df: pd.DataFrame, summary_df: pd.DataFrame, out_path: Path) -> None:
    sessions = sorted(interval_df["session"].unique())
    fig, axes = plt.subplots(1, len(sessions), figsize=(7.4 * len(sessions), 5.2), squeeze=False)
    for ax, session in zip(axes[0], sessions):
        subset = interval_df[interval_df["session"] == session]
        for plane, group in subset.groupby("plane_index"):
            group = group.sort_values("interval_index")
            ax.plot(group["interval_index"], group["best_z_subslice"], marker="o", alpha=0.55, label=f"plane {plane}")
        pivot = subset.pivot(index="interval_index", columns="plane_index", values="best_z_subslice").sort_index()
        consensus = (pivot - pivot.median(axis=0)).median(axis=1)
        ax2 = ax.twinx()
        ax2.plot(consensus.index, consensus.values, color="black", marker="s", linewidth=3, label="consensus shift")
        ax2.axhline(0, color="black", linewidth=0.7, alpha=0.5)
        ax2.set_ylabel("Consensus ΔZ from plane median (slices)")
        summary = summary_df[summary_df["session"] == session].iloc[0]
        ax.set_title(
            f"{session}: {summary['evidence_tier']}\n"
            f"retained-span ΔZ={summary['consensus_change_slices']:.2f}, "
            f"p={summary['exact_order_permutation_p']:.3f}"
        )
        labels = (
            subset[["interval_index", "interval_label"]]
            .drop_duplicates()
            .sort_values("interval_index")
        )
        ax.set_xticks(labels["interval_index"].to_numpy())
        ax.set_xticklabels(labels["interval_label"].to_list(), fontsize=8)
        ax.set_xlabel("Retained acquisition block and within-block window (Block 0 excluded)")
        ax.set_ylabel("Anatomy best Z (sub-slice NCC peak)")
        ax.grid(alpha=0.2)
        ax.legend(loc="best", fontsize=8, ncol=2)
    fish_id = str(interval_df["fish_id"].iloc[0])
    fig.suptitle(f"{fish_id}: post-Block-0 functional-reference NCC Z diagnostic", fontweight="bold")
    fig.tight_layout()
    fig.savefig(out_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def _render_profiles(profile_df: pd.DataFrame, out_path: Path) -> None:
    planes = sorted(profile_df["plane_index"].unique())
    ncols = 2
    nrows = int(np.ceil(len(planes) / ncols))
    fig, axes = plt.subplots(nrows, ncols, figsize=(12, 3.2 * nrows), squeeze=False)
    for ax, plane in zip(axes.ravel(), planes):
        subset = profile_df[profile_df["plane_index"] == plane]
        for interval, group in subset.groupby("interval_index"):
            group = group.sort_values("anatomy_z")
            ax.plot(group["anatomy_z"], group["ncc"], label=str(group["interval_label"].iloc[0]).replace("\n", " — "), alpha=0.8)
        ax.set_title(f"plane {plane}")
        ax.set_xlabel("Canonical anatomy Z")
        ax.set_ylabel("NCC")
        ax.grid(alpha=0.2)
    for ax in axes.ravel()[len(planes) :]:
        ax.axis("off")
    fish_id = str(profile_df["fish_id"].iloc[0])
    handles, labels = axes.ravel()[0].get_legend_handles_labels()
    fig.suptitle(
        f"{fish_id}: NCC depth profiles across retained blocks (Block 0 excluded)",
        y=1.01,
        fontweight="bold",
    )
    fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 0.99), fontsize=8, ncol=3)
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    fig.savefig(out_path, dpi=160, bbox_inches="tight")
    plt.close(fig)


def run_functional_z_drift_diagnostic(
    *,
    fish_id: str,
    motion_corrected_dir: str | Path,
    anatomy_stack_path: str | Path,
    preprocessing_metadata_path: str | Path,
    functional_reference_manifest_path: str | Path,
    scale_cache_path: str | Path,
    output_dir: str | Path,
    config: FunctionalZDriftConfig | None = None,
) -> dict[str, Any]:
    cfg = config or FunctionalZDriftConfig()
    motion_dir = Path(motion_corrected_dir)
    anatomy_path = Path(anatomy_stack_path)
    metadata_path = Path(preprocessing_metadata_path)
    reference_manifest_path = Path(functional_reference_manifest_path)
    scale_path = Path(scale_cache_path)
    out_dir = Path(output_dir)
    if out_dir.exists() and any(out_dir.iterdir()):
        raise FileExistsError(f"Output directory is not empty: {out_dir}")
    out_dir.mkdir(parents=True, exist_ok=True)

    scales = load_plane_scales(scale_path, fish_id)
    session_map = load_session_map(metadata_path)
    polarity, polarity_source = load_functional_polarity(reference_manifest_path, fish_id)
    frame_selection = load_functional_frame_selection(reference_manifest_path, fish_id)
    anatomy = np.asarray(_read_anatomy_zyx(anatomy_path), dtype=np.float32)
    if anatomy.ndim != 3:
        raise ValueError(f"Expected Z,Y,X anatomy stack, got {anatomy.shape}: {anatomy_path}")
    anatomy_filtered = np.stack(
        [local_unsharp(norm01(image), cfg.sharpen_sigma, cfg.sharpen_amount) for image in anatomy], axis=0
    )
    interval_rows: list[dict[str, Any]] = []
    profile_rows: list[dict[str, Any]] = []

    for plane_index in sorted(scales):
        movie_path = motion_dir / f"{fish_id}_plane{plane_index}_mcorrected.tif"
        if not movie_path.exists():
            raise FileNotFoundError(f"Motion-corrected plane movie not found: {movie_path}")
        label, scale = scales[plane_index]
        session = session_map.get(plane_index)
        if session is None:
            raise ValueError(f"No session mapping for plane {plane_index}")
        movie = tifffile.memmap(movie_path)
        if movie.ndim != 3:
            raise ValueError(f"Expected T,Y,X movie for plane {plane_index}, got {movie.shape}")
        selection = frame_selection.get(plane_index)
        if selection is None:
            raise ValueError(f"No accepted frame-selection provenance for plane {plane_index}")
        if int(movie.shape[0]) != selection["source_frame_count"]:
            raise ValueError(
                f"Motion-corrected frame count disagrees with preparation provenance for plane {plane_index}: "
                f"movie={movie.shape[0]}, manifest={selection['source_frame_count']}"
            )
        selected_start = selection["frame_start"]
        selected_count = selection["reference_frame_count"]
        display_labels = interval_block_labels(
            excluded_block_frame_count=selected_start,
            retained_frame_count=selected_count,
            interval_count=cfg.intervals_per_session,
        )
        for interval_index, (relative_start, relative_stop) in enumerate(
            interval_bounds(selected_count, cfg.intervals_per_session)
        ):
            start = selected_start + relative_start
            stop = selected_start + relative_stop
            reference, sampled_count = _interval_reference(movie, start, stop, cfg, polarity=polarity)
            scaled = scale_image(reference, scale)
            best_z, scores = best_z_by_ncc(scaled, anatomy_filtered, use_cv2=cfg.use_cv2)
            metrics = registration_metric_from_scores(scores) or {}
            best_subslice = quadratic_peak_z(scores)
            interval_rows.append(
                {
                    "fish_id": fish_id,
                    "session": session,
                    "plane_index": plane_index,
                    "plane_label": label,
                    "interval_index": interval_index,
                    "interval_label": display_labels[interval_index],
                    "frame_start": start,
                    "frame_stop": stop,
                    "frame_midpoint": (start + stop - 1) / 2.0,
                    "sampled_frame_count": sampled_count,
                    "scale": scale,
                    "best_z": best_z,
                    "best_z_subslice": best_subslice,
                    "max_score": metrics.get("max_score"),
                    "peak_delta": metrics.get("peak_delta"),
                    "peak_zscore": metrics.get("peak_zscore"),
                }
            )
            profile_rows.extend(
                {
                    "fish_id": fish_id,
                    "session": session,
                    "plane_index": plane_index,
                    "interval_index": interval_index,
                    "interval_label": display_labels[interval_index],
                    "anatomy_z": z_idx,
                    "ncc": float(score),
                }
                for z_idx, score in enumerate(scores)
            )

    interval_df = pd.DataFrame(interval_rows)
    profile_df = pd.DataFrame(profile_rows)
    summary_df = summarize_session_drift(interval_df, cfg)
    interval_path = out_dir / "z_drift_intervals.csv"
    profile_path = out_dir / "z_drift_ncc_profiles.csv"
    summary_path = out_dir / "z_drift_session_summary.csv"
    interval_df.to_csv(interval_path, index=False)
    profile_df.to_csv(profile_path, index=False)
    summary_df.to_csv(summary_path, index=False)
    _render_tracks(interval_df, summary_df, out_dir / "z_drift_tracks.png")
    _render_profiles(profile_df, out_dir / "z_drift_ncc_profiles.png")

    manifest = {
        "diagnostic": "functional_z_drift_ncc_v4_manifest_selected_trajectory",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "fish_id": fish_id,
        "inputs": {
            "motion_corrected_dir": str(motion_dir),
            "anatomy_stack_path": str(anatomy_path),
            "preprocessing_metadata_path": str(metadata_path),
            "functional_reference_manifest_path": str(reference_manifest_path),
            "scale_cache_path": str(scale_path),
        },
        "parameters": {
            **asdict(cfg),
            "effective_polarity": polarity,
            "polarity_source": polarity_source,
            "frame_selection_by_plane": frame_selection,
        },
        "outputs": [str(path) for path in (interval_path, profile_path, summary_path, out_dir / "z_drift_tracks.png", out_dir / "z_drift_ncc_profiles.png")],
        "session_summary": summary_df.to_dict(orient="records"),
        "interpretation_note": "Evidence tiers are diagnostic heuristics. Scientific acceptance requires visual review of NCC profiles and coherent movement across planes within a session.",
    }
    manifest_path = out_dir / "z_drift_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    return manifest


__all__ = [
    "FunctionalZDriftConfig",
    "interval_block_labels",
    "interval_bounds",
    "load_plane_scales",
    "load_functional_frame_selection",
    "load_functional_polarity",
    "load_session_map",
    "quadratic_peak_z",
    "run_functional_z_drift_diagnostic",
    "summarize_session_drift",
]
