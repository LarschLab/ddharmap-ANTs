"""QC figures for review-only raw-anatomy polarity inference."""

from __future__ import annotations

from pathlib import Path
from typing import Mapping, Sequence

import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import numpy as np


def render_anatomy_polarity_qc(*, rows: Sequence[Mapping[str, object]], projections: Mapping[str, np.ndarray], output_path: Path | str) -> Path:
    count = max(1, len(rows))
    columns = min(6, count)
    row_count = int(np.ceil(count / columns))
    figure, axes = plt.subplots(row_count, columns, figsize=(3.2 * columns, 3.35 * row_count), dpi=160, squeeze=False)
    for axis in axes.flat:
        axis.axis("off")
    for axis, row in zip(axes.flat, rows):
        sample_id = str(row["sample_id"])
        projection = projections.get(sample_id)
        if projection is None:
            axis.set_facecolor("#eeeeee")
            axis.text(0.5, 0.5, "NO RAW\nANATOMY", ha="center", va="center", fontsize=12, transform=axis.transAxes)
        else:
            axis.imshow(projection, cmap="gray", vmin=0.0, vmax=1.0)
        known = str(row.get("known_polarity") or "unknown")
        prediction = str(row.get("prediction") or "review")
        agreement = row.get("agreement")
        color = (
            "#2ca02c"
            if agreement is True
            else "#d62728"
            if agreement is False
            else "#ff9800"
            if prediction == "review"
            else "#377eb8"
            if prediction in {"north", "south"}
            else "#777777"
        )
        axis.add_patch(
            Rectangle((0.0, 0.0), 1.0, 1.0, transform=axis.transAxes, fill=False, edgecolor=color, linewidth=4.0)
        )
        margin = row.get("median_north_minus_south")
        margin_text = f"margin={float(margin):+.4f}" if margin not in (None, "") else str(row.get("status", ""))
        stack_count = int(row.get("stack_count") or 0)
        stack_index = row.get("stack_index")
        stack_text = f" [{stack_index}/{stack_count}]" if stack_count > 1 else ""
        axis.set_title(f"{row['fish_id']}{stack_text}\npred={prediction} | known={known}\n{margin_text}", fontsize=8.5)
        axis.axis("off")
    figure.suptitle(
        "Matilde raw in-vivo 2P anatomy polarity audit (L427 excluded)\n"
        "green=agreement; red=disagreement; orange=review; blue=unlabeled prediction; gray=missing",
        fontsize=14,
    )
    figure.tight_layout(rect=(0.0, 0.0, 1.0, 0.94))
    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(target, bbox_inches="tight")
    plt.close(figure)
    return target


__all__ = ["render_anatomy_polarity_qc"]
