"""Shared annotation placement helpers for cohort plot labels."""

from __future__ import annotations

from typing import Any

import matplotlib.pyplot as plt


def place_labels_no_overlap(
    ax: plt.Axes,
    items: list[tuple[float, float, str] | tuple[float, float, str, dict[str, Any]]],
    *,
    y_span: float,
    x_neighbor_thresh: float = 1.1,
    y_pad_frac: float = 0.03,
    min_sep_frac: float = 0.05,
    top_margin_frac: float = 0.08,
    fontsize: float = 7.0,
    text_kwargs: dict[str, Any] | None = None,
) -> list[tuple[float, float]]:
    """Place labels above data, stacking upward on local x-collisions."""
    if not items:
        return []
    fig = ax.figure
    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()

    span = max(1e-6, float(y_span))
    y_pad = float(y_pad_frac) * span
    min_sep = float(min_sep_frac) * span
    kwargs = dict(text_kwargs or {})
    placed: list[tuple[float, float]] = []
    placed_meta: list[tuple[float, float, Any, Any]] = []
    top_used: float | None = None

    def _height_data_from_bbox(bbox: Any) -> float:
        p0 = ax.transData.inverted().transform((bbox.x0, bbox.y0))
        p1 = ax.transData.inverted().transform((bbox.x0, bbox.y1))
        return max(1e-9, float(abs(float(p1[1]) - float(p0[1]))))

    def _x_overlaps(a: Any, b: Any) -> bool:
        return bool(float(a.x0) < float(b.x1) and float(a.x1) > float(b.x0))

    for item in sorted(items, key=lambda item: float(item[0])):
        if len(item) == 3:
            xpos, y_base, label = item
            item_kwargs: dict[str, Any] = {}
        else:
            xpos, y_base, label, item_kwargs = item
        y_draw = float(y_base) + y_pad
        draw_kwargs = dict(kwargs)
        draw_kwargs.update(item_kwargs or {})
        txt = ax.text(float(xpos), float(y_draw), str(label), ha="center", va="bottom", fontsize=fontsize, **draw_kwargs)
        moved = True
        bbox = txt.get_window_extent(renderer=renderer)
        height_data = _height_data_from_bbox(bbox)
        while moved:
            moved = False
            for px, py, prev_bbox, prev_h in placed_meta:
                is_neighbor = abs(float(xpos) - float(px)) <= float(x_neighbor_thresh)
                if not is_neighbor and not _x_overlaps(bbox, prev_bbox):
                    continue
                req_sep = max(float(min_sep), float(height_data), float(prev_h))
                if float(y_draw) < float(py) + float(req_sep):
                    y_draw = float(py) + float(req_sep)
                    txt.set_position((float(xpos), float(y_draw)))
                    bbox = txt.get_window_extent(renderer=renderer)
                    height_data = _height_data_from_bbox(bbox)
                    moved = True
                    break
        placed.append((float(xpos), float(y_draw)))
        placed_meta.append((float(xpos), float(y_draw), bbox, float(height_data)))
        txt_top = float(y_draw) + float(height_data)
        top_used = float(txt_top) if top_used is None else max(float(top_used), float(txt_top))
    if top_used is not None:
        bottom, top = ax.get_ylim()
        target_top = max(float(top), float(top_used) + float(top_margin_frac) * span)
        if target_top > float(top):
            ax.set_ylim(float(bottom), float(target_top))
    return placed
