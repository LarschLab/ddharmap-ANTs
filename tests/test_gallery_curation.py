from pathlib import Path
from types import SimpleNamespace

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from PIL import Image
import pytest

from codeants_2pf_hcr.qc_figure_export import CURATED_CARDS, select_curated_cards
from codeants_2pf_hcr.plots.gallery import (
    render_gallery_composite, hcr_status_ring_counts, draw_hcr_status_rings,
    HCR_STATUS_ORDER, render_hcr_pair_status_planes,
)
from codeants_2pf_hcr.plots.qc_molecular import plot_molecular_identity_qc, plot_activity_bpi_gate


def test_order_selection_and_complete_exclusion_map():
    figures = []
    roles = dict.fromkeys(r for _, _, rr in CURATED_CARDS for r in rr)
    for i, role in enumerate(roles):
        figures.append({'id': f'varying-{i}', 'sha256': str(i), 'sources': [{'roles': [role]}]})
    figures += [{'id': 'legacy', 'sha256': 'old', 'sources': [{'roles': ['writer:bpi_all_pairs']}]}]
    selected, exclusions, missing = select_curated_cards(list(reversed(figures)))
    assert not missing
    assert [c['id'] for c in selected] == [f'P{i:03}' for i in range(1, 21)] + ['P020b'] + [f'P{i:03}' for i in range(21, 26)]
    assert exclusions.keys() == {'legacy'}
    assert exclusions['legacy']['sources'] == figures[-1]['sources']
    assert selected[2]['roles'] == ['writer:functional_anatomy_plane_qc_rows', 'writer:functional_anatomy_intensity_overlay_rows']
    assert selected[13]['decision'] == 'approved curation'


def test_composite_keeps_every_original_pixel_and_all_panel_rows(tmp_path):
    for name, shape, color in [('a', (200, 300), 'red'), ('b', (400, 500), 'blue')]:
        Image.new('RGB', shape, color).save(tmp_path / f'{name}.png')
    out = render_gallery_composite([tmp_path / 'a.png', tmp_path / 'b.png'], tmp_path / 'composite.png')
    pixels = np.asarray(Image.open(out))
    assert pixels.shape == (800, 400, 3)
    np.testing.assert_array_equal(pixels[:300, 100:300], np.asarray(Image.open(tmp_path / 'a.png')))
    np.testing.assert_array_equal(pixels[300:], np.asarray(Image.open(tmp_path / 'b.png')))


def test_two_ring_partition_includes_unavailable_and_aligns_sectors():
    status = pd.DataFrame({'gene': ['g'] * 7, 'functional_status': [*HCR_STATUS_ORDER, HCR_STATUS_ORDER[0]]})
    inner, outer = hcr_status_ring_counts(status, 'g')
    assert inner.tolist() == [5, 1, 1]
    assert outer.tolist() == [2, 1, 1, 1, 1, 1]
    assert inner.sum() == outer.sum() == len(status)
    fig, ax = plt.subplots()
    draw_hcr_status_rings(ax, status, 'g')
    assert len(ax.patches) == 9
    assert sum(p.theta2 - p.theta1 for p in ax.patches[:3]) == pytest.approx(360)
    assert sum(p.theta2 - p.theta1 for p in ax.patches[3:]) == pytest.approx(360)
    plt.close(fig)


def test_pair_status_grid_all_original_planes_no_new_matching():
    stack = np.ones((4, 15, 20))
    accepted = np.zeros_like(stack, dtype=int); accepted[:, 3:7, 4:8] = 3
    rejected = np.zeros_like(stack, dtype=int); rejected[:, 10:13, 12:16] = 4
    viewer = SimpleNamespace(anatomy_zyx=stack, label_masks_zyx={'a': accepted, 'b': rejected}, label_categories={'a': 'accepted', 'b': 'rejected'})
    fig = render_hcr_pair_status_planes(viewer, {9: 0, 5: 3})
    assert len(fig.axes) == 2
    assert 'plane 5' in fig.axes[0].get_title()
    assert 'z=3' in fig.axes[0].get_title()
    assert 'Accepted 1 · rejected 1' in fig.axes[1].get_title()
    assert len(fig.axes[0].images) == 3
    plt.close(fig)


def test_identity_small_counts_are_labeled_and_zoomed():
    summary = pd.DataFrame({'identity': ['unidentified', 'g1', 'g2'], 'n_rois': [9000, 2, 7]})
    fig = plot_molecular_identity_qc({'fish_id': 'test', 'identity_summary': summary})
    assert {t.get_text() for t in fig.axes[0].texts} == {'9000', '2', '7'}
    inset = fig.axes[0].child_axes[0]
    assert inset.get_xlim()[1] < 10
    assert {t.get_text() for t in inset.texts} == {'2', '7'}
    plt.close(fig)


def test_per_stimulus_signed_margins_use_own_null_and_floor(tmp_path):
    row = dict(mean_bout_auc_dff=.10, mean_cont_auc_dff=.01, response_magnitude_auc=.05, activity_mag=.2, bpi=.8,
               response_is_active=True, response_class='bout-responsive', bpi_category='bout-responsive',
               bpi_preference='bout-preferential', bpi_zero_band=.5, bpi_activity_threshold=.05, bpi_side='left')
    for i, stim in enumerate(('lb', 'rb', 'lc', 'rc')):
        row.update({f'{stim}_mean_auc_dff': .10, f'{stim}_null_upper_auc': .02 + .03 * i, f'{stim}_null_lower_auc': -.02 - .03 * i, f'{stim}_response_valence': 'activated' if i < 3 else 'not-significant'})
    path = tmp_path / 'cells.csv'; pd.DataFrame([row]).to_csv(path, index=False)
    fig, _ = plot_activity_bpi_gate(path, fish_id='test')
    assert 'Comparison-only' in fig.axes[0].get_title()
    assert len(fig.axes[0].lines) == 1  # comparison diagonal; no pooled floors
    for i, ax in enumerate(fig.axes[6:10]):
        np.testing.assert_allclose(next(c for c in ax.collections if len(c.get_offsets())).get_offsets()[0], [.10 - max(.05, .02 + .03*i), min(-.05, -.02-.03*i) - .10])
    assert 'tuning side' in fig.axes[10].get_title()
    plt.close(fig)


@pytest.mark.parametrize('renderer_name', [
    'render_registration_review_composite', 'render_geometry_offsets_composite',
    'render_midline_review_composite', 'render_identity_coverage_composite',
])
def test_each_notebook_composite_preserves_native_png_panels(tmp_path, renderer_name):
    from codeants_2pf_hcr.plots import gallery
    a = tmp_path / 'a.png'; b = tmp_path / 'b.png'
    Image.new('RGB', (120, 130), 'red').save(a)
    Image.new('RGB', (150, 160), 'blue').save(b)
    result = getattr(gallery, renderer_name)(a, b)
    if renderer_name == 'render_registration_review_composite':
        assert result.size == (298, 160)
        np.testing.assert_array_equal(np.asarray(result)[:, 148:], np.asarray(Image.open(b)))
        assert result.height >= 160  # no source shrinking
    elif renderer_name == 'render_geometry_offsets_composite':
        assert result.size == (150, 290)
        np.testing.assert_array_equal(np.asarray(result)[:130, 15:135], np.asarray(Image.open(a)))
        np.testing.assert_array_equal(np.asarray(result)[130:], np.asarray(Image.open(b)))
    else:
        assert result.size == (150, 322)
        np.testing.assert_array_equal(np.asarray(result)[162:], np.asarray(Image.open(b)))
        assert not np.any(np.all(np.asarray(result)[:162] == 255, axis=2))  # full-width content


def test_pass_colors_saved_calls_and_activation_only_sides(tmp_path):
    base = dict(mean_bout_auc_dff=.1, mean_cont_auc_dff=.01, response_magnitude_auc=.05,
                activity_mag=.2, bpi=.8, response_is_active=True, response_class='bout-responsive',
                bpi_category='bout-responsive', bpi_preference='bout-preferential',
                bpi_zero_band=.5, bpi_activity_threshold=.05)
    rows = []
    for call, side in [('not-significant', None), ('activated', 'left'), ('activated', 'right'), ('suppressed', None)]:
        row = dict(base, bpi_side=side)
        for stim in ['lb', 'rb', 'lc', 'rc']:
            row.update({f'{stim}_mean_auc_dff': .1, f'{stim}_null_upper_auc': .02,
                        f'{stim}_null_lower_auc': -.02, f'{stim}_response_valence': call})
        rows.append(row)
    path = tmp_path/'calls.csv'; pd.DataFrame(rows).to_csv(path, index=False)
    fig, _ = plot_activity_bpi_gate(path, fish_id='test')
    for ax in fig.axes[6:10]:
        assert [c.get_label() for c in ax.collections] == [
            'Not significant (n=1)', 'Activated pass (n=2)', 'Suppression pass (n=1)']
        assert len({tuple(c.get_facecolors()[0]) for c in ax.collections}) == 3
    assert [patch.get_height() for patch in fig.axes[10].patches] == [1, 1]
    assert 'excluded 2' in fig.axes[10].get_title()
    assert not fig.axes[2].axison  # degenerate binned curve removed
    plt.close(fig)


def test_ten_plane_grid_and_thicker_saved_outlines():
    stack = np.zeros((10, 20, 20))
    labels = np.zeros_like(stack, dtype=int); labels[:, 8:12, 8:12] = 1
    viewer = SimpleNamespace(anatomy_zyx=stack, label_masks_zyx={'a': labels}, label_categories={'a': 'accepted'})
    fig = render_hcr_pair_status_planes(viewer, {i:i for i in range(10)})
    assert len(fig.axes) == 10
    grid = fig.axes[0].get_subplotspec().get_gridspec()
    assert grid.get_geometry() == (5, 2)
    boundary_alpha = np.asarray(fig.axes[0].images[1].get_array())[..., 3]
    assert (boundary_alpha > 0).sum() > 12  # original thin perimeter
    assert len(fig.legends[0].get_texts()) == 2
    plt.close(fig)


def test_notebook_composites_display_as_png_only(tmp_path):
    from IPython.core.formatters import DisplayFormatter
    from codeants_2pf_hcr.plots import gallery
    a = tmp_path / 'a.png'; b = tmp_path / 'b.png'
    Image.new('RGB', (20, 30), 'red').save(a)
    Image.new('RGB', (25, 35), 'blue').save(b)
    data, _ = DisplayFormatter().format(gallery.render_registration_review_composite(a, b))
    assert 'image/png' in data and 'image/jpeg' not in data


def test_remove_tree_tolerates_vanished_sidecar(tmp_path, monkeypatch):
    import os
    from codeants_2pf_hcr.paths import remove_tree
    root = tmp_path / 'staged'; root.mkdir()
    (root / 'x.png').write_bytes(b'x'); (root / '._x.png').write_bytes(b'\x00\x05\x16\x07')
    real_unlink = os.unlink

    def unlink(path, *args, **kwargs):
        # Emulate exFAT: removing x.png also removes its ._ sidecar.
        real_unlink(path, *args, **kwargs)
        if str(path).endswith('x.png') and not str(path).endswith('._x.png'):
            try:
                real_unlink('._x.png', *args, **kwargs)
            except FileNotFoundError:
                pass
    monkeypatch.setattr(os, 'unlink', unlink)
    remove_tree(root)
    assert not root.exists()
