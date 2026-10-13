"""Presentation-only gallery panels; inputs are persisted figures or saved status."""
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from PIL import Image

HCR_STATUS_ORDER = (
    'in-plane responsive ROI', 'in-plane low-activity ROI',
    'in-plane response unavailable', 'in-plane no functional ROI candidate',
    'out-of-plane anatomy label', 'unmatched',
)
HCR_STATUS_COLORS = ('#1b9e77', '#6a6a6a', '#fdb462', '#d73027', '#80b1d3', '#bdbdbd')


def render_gallery_composite(panel_paths, output_path):
    """Stack complete PNG panels without resampling, cropping or dropping planes."""
    images = []
    for path in panel_paths:
        with Image.open(path) as source:
            images.append(source.convert('RGB'))
    if not images:
        raise ValueError('A composite requires panels')
    canvas = Image.new('RGB', (max(p.width for p in images), sum(p.height for p in images)), 'white')
    y = 0
    for panel in images:
        canvas.paste(panel, ((canvas.width - panel.width) // 2, y))
        y += panel.height
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    canvas.save(output_path)
    return output_path


def hcr_status_ring_counts(status, gene):
    """Partition persisted HCR-label rows, with aligned inner/outer sector sums.

    Unlike the historical cohort donut, do not append labels from another
    denominator (its unmatched summary). Only supplied status rows count here.
    """
    sub = status.loc[status['gene'].astype(str).eq(gene)]
    labels = sub['functional_status'].astype(str)
    unknown = set(labels) - set(HCR_STATUS_ORDER)
    if unknown:
        raise ValueError(f'Unknown persisted HCR functional status: {sorted(unknown)}')
    outer = np.array([labels.eq(label).sum() for label in HCR_STATUS_ORDER], dtype=int)
    inner = np.array([outer[:4].sum(), outer[4], outer[5]], dtype=int)
    return inner, outer


def draw_hcr_status_rings(ax, status, gene):
    """Legacy scope inner ring and functional-state outer ring; HCR-label units."""
    inner, outer = hcr_status_ring_counts(status, gene)
    total = int(inner.sum())
    if total:
        ax.pie(inner, radius=0.82, startangle=90, counterclock=False,
               colors=['#4daf4a', '#377eb8', '#bdbdbd'],
               wedgeprops={'width': 0.30, 'edgecolor': 'white'})
        wedges, _ = ax.pie(outer, radius=1.03, startangle=90, counterclock=False,
                           colors=HCR_STATUS_COLORS,
                           wedgeprops={'width': 0.17, 'edgecolor': 'white'})
        # A textual count key avoids illegible labels on tiny wedges.
        ax.text(1.12, 0, '\n'.join(f'{label}: {n}' for label, n in zip(HCR_STATUS_ORDER, outer) if n),
                ha='left', va='center', fontsize=7)
    else:
        ax.text(0, 0.45, 'No HCR status rows', ha='center', fontsize=8)
    ax.text(0, 0, f'HCR labels\nn={total}', ha='center', va='center', fontsize=9)
    ax.set_title(gene, fontsize=11)
    ax.set_aspect('equal', anchor='W'); ax.set_xlim(-1.1, 3.0); ax.set_ylim(-1.15, 1.15)
    ax.axis('off')
    return inner, outer


def render_hcr_pair_status_planes(viewer_data, plane_z):
    """Every functional anatomy plane, with saved accepted/rejected HCR outlines."""
    from skimage.segmentation import find_boundaries
    from scipy.ndimage import binary_dilation
    planes = sorted(plane_z)
    if not planes:
        raise ValueError('No functional planes supplied')
    ncols = min(2, len(planes))
    nrows = int(np.ceil(len(planes) / ncols))
    fig, axes = plt.subplots(nrows, ncols, figsize=(14, 6.5 * nrows + 0.8), squeeze=False)
    palette = {'accepted': '#2ca25f', 'rejected': '#e34a33'}
    for ax, plane in zip(axes.flat, planes):
        z = int(plane_z[plane])
        anatomy = np.asarray(viewer_data.anatomy_zyx[z])
        ax.imshow(anatomy, cmap='gray', vmin=np.percentile(anatomy, 1), vmax=np.percentile(anatomy, 99))
        counts = dict.fromkeys(palette, 0)
        for name, stack in viewer_data.label_masks_zyx.items():
            category = viewer_data.label_categories[name]
            labels = np.asarray(stack[z])
            counts[category] += len(np.unique(labels[labels > 0]))
            rgba = np.zeros((*labels.shape, 4))
            from matplotlib.colors import to_rgba
            boundary = binary_dilation(find_boundaries(labels, mode='inner'), iterations=1)
            rgba[boundary] = to_rgba(palette[category])
            ax.imshow(rgba, interpolation='nearest')
        ax.set_title(f'Functional plane {plane} · anatomy z={z}\nAccepted {counts["accepted"]} · rejected {counts["rejected"]}', fontsize=11)
        ax.axis('off')
    for ax in list(axes.flat)[len(planes):]:
        ax.set_visible(False)
    fig.legend(handles=[Patch(color=color, label=f'Saved {key} pair') for key, color in palette.items()],
               loc='lower center', ncol=2, frameon=False)
    fig.suptitle('Persisted HCR pair status on every functional anatomy plane', y=0.997)
    fig.subplots_adjust(top=0.965, bottom=0.035, left=0.025, right=0.975, hspace=0.14, wspace=0.06)
    return fig


class PngComposite:
    """Composite that notebooks display as PNG only.

    A bare PIL image is displayed as JPEG and PNG, and the gallery exporter
    accepts PNG notebook outputs only.
    """

    def __init__(self, image):
        self.image = image

    size = property(lambda self: self.image.size)
    width = property(lambda self: self.image.width)
    height = property(lambda self: self.image.height)

    def __array__(self, dtype=None, copy=None):
        return np.asarray(self.image, dtype=dtype)

    def save(self, *args, **kwargs):
        return self.image.save(*args, **kwargs)

    def _repr_png_(self):
        import io
        buffer = io.BytesIO()
        self.image.save(buffer, format='PNG')
        return buffer.getvalue()


def _composite_image(panels, *, equal_width=False):
    """Full-resolution panels, optionally enlarged to comparable display scale.

    Never shrink a source PNG. The gallery inventory also retains each original.
    """
    import io
    images = []
    for panel in panels:
        if hasattr(panel, 'savefig'):
            buffer = io.BytesIO()
            panel.savefig(buffer, format='png', dpi=300, bbox_inches='tight')
            buffer.seek(0)
            with Image.open(buffer) as image:
                images.append(image.convert('RGB'))
        else:
            with Image.open(panel) as image:
                images.append(image.convert('RGB'))
    if equal_width:
        width = max(p.width for p in images)
        images = [p if p.width == width else p.resize(
            (width, round(p.height * width / p.width)), Image.Resampling.LANCZOS) for p in images]
    canvas = Image.new('RGB', (max(p.width for p in images), sum(p.height for p in images)), 'white')
    y = 0
    for panel in images:
        canvas.paste(panel, ((canvas.width-panel.width)//2, y)); y += panel.height
    return PngComposite(canvas)


def render_geometry_offsets_composite(summary, offsets):
    return _composite_image([summary, offsets])


def render_midline_review_composite(accepted_overlay, side_assignment):
    return _composite_image([accepted_overlay, side_assignment], equal_width=True)


def render_identity_coverage_composite(status_coverage, exact_identity):
    return _composite_image([status_coverage, exact_identity], equal_width=True)
