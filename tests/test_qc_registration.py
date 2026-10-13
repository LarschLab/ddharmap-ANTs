import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pytest
import tifffile

from codeants_2pf_hcr.plots.qc_registration import render_registration_plane_review, registration_review_product_paths
from codeants_2pf_hcr.qc_figure_export import CURATED_CARDS, COMPOSITE_ROLES, select_curated_cards


@pytest.fixture
def products(tmp_path):
    image = np.arange(24 * 32, dtype=np.float32).reshape(24, 32)
    labels = np.zeros((3, 24, 32), dtype=np.uint16)
    labels[:, 6:12, 8:14] = 1
    tifffile.imwrite(tmp_path / 'anatomy.tif', np.stack([image] * 3), photometric='minisblack')
    tifffile.imwrite(tmp_path / 'labels.tif', labels, photometric='minisblack')
    refs, comparison = [], []
    for plane, method in [(5, 'ants_rigid_affine'), (9, 'ncc_xy')]:
        label = f'L765_f05_plane{plane}_mcorrected_flipX'
        refs.append(dict(index=plane, label=label, best_z=1, anat_label_z=1, anat_label_z_mode='reverse',
                         inplane_active_method=method, reference_norm_path='/absent/historical/reference.tif'))
        tifffile.imwrite(tmp_path / f'{label}_ref_norm.tif', image)
        tifffile.imwrite(tmp_path / f'{label}_suite2p_labels_native.tif', labels[0])
        # Both exist: selecting the first TIFF would be wrong for the NCC plane.
        for candidate in ('ants_rigid_affine', 'ncc_xy'):
            tifffile.imwrite(tmp_path / f'{label}_{candidate}_warped.tif', image if candidate == method else image[::-1])
        comparison.append(dict(plane_idx=plane, method=method, post_ncc=.75, bestz_max_score=.65))
    (tmp_path / 'refs.json').write_text(json.dumps(refs))
    pd.DataFrame(comparison).to_csv(tmp_path / 'inplane_registration_comparison.csv', index=False)
    return dict(fish_id='L765_f05', plane_refs_summary_path=tmp_path/'refs.json',
                anatomy_stack_path=tmp_path/'anatomy.tif', anatomy_labels_path=tmp_path/'labels.tif',
                suite2p_label_dir=tmp_path, functional_reference_dir=tmp_path, positioned_reference_dir=tmp_path)


def test_four_columns_all_persisted_planes_outlines_extent_and_selected_method(products):
    from codeants_2pf_hcr.plots.qa import _normalize_for_display
    fig = render_registration_plane_review(**products)
    assert len(fig.axes) == 8
    assert len(fig.subfigs) == 2
    for row, plane in enumerate([5, 9]):
        axes = fig.axes[row*4:row*4+4]
        assert axes[0].get_subplotspec().get_gridspec().get_geometry() == (1, 4)
        assert f'Plane {plane}' in fig.subfigs[row]._suptitle.get_text()
        assert 'post NCC=0.750' in fig.subfigs[row]._suptitle.get_text()
        assert 'Z-selection NCC=0.650' in fig.subfigs[row]._suptitle.get_text()
        assert 'n=1' in axes[0].get_title() and 'label Z=1' in axes[1].get_title()
        for ax in axes[:2]:
            rgb = np.asarray(ax.images[0].get_array())
            assert np.any(rgb[..., 0] != rgb[..., 1])  # coloured thin boundaries
        assert all(ax.images[0].get_extent() == axes[1].images[0].get_extent() for ax in axes[1:])
        registered = np.asarray(axes[2].images[0].get_array())
        expected = _normalize_for_display(tifffile.imread(products['anatomy_stack_path'])[1])
        np.testing.assert_array_equal(registered, expected)
        overlay = np.asarray(axes[3].images[0].get_array())
        np.testing.assert_array_equal(overlay[..., 0], expected)
        np.testing.assert_array_equal(overlay[..., 1], registered)
    assert 'NCC-xy' in fig.subfigs[1]._suptitle.get_text()
    plt.close(fig)


def test_png_only_notebook_display(products):
    from IPython.core.formatters import DisplayFormatter
    from matplotlib.figure import Figure
    from IPython.core.pylabtools import print_figure
    formatter = DisplayFormatter()
    formatter.formatters['image/png'].for_type(Figure, lambda fig: print_figure(fig, 'png'))
    fig = render_registration_plane_review(**products)
    data, _ = formatter.format(fig)
    assert 'image/png' in data and 'image/jpeg' not in data
    notebook = json.loads(Path('notebooks/qc/02_functional_registration_qc.ipynb').read_text())
    source = ''.join(notebook['cells'][-1]['source'])
    assert source.startswith('# 13')
    assert 'set_matplotlib_formats("png")' in source
    assert 'render_registration_plane_review(**registration_review_product_paths(CONTEXT))' in source
    assert 'render_registration_review_composite' not in source
    plt.close(fig)


def test_missing_selected_warp_does_not_substitute_other_method(products):
    (products['positioned_reference_dir'] / 'L765_f05_plane9_mcorrected_flipX_ncc_xy_warped.tif').unlink()
    with pytest.raises(FileNotFoundError):
        render_registration_plane_review(**products)
    plt.close('all')


def test_curation_selects_new_notebook_role_and_excludes_original_writers():
    assert dict((card, roles) for card, _, roles in CURATED_CARDS)['P003'] == ('nb:02:13:0',)
    assert 'P003' not in COMPOSITE_ROLES
    figures = [dict(id=str(i), sha256=str(i), sources=[dict(roles=[role])]) for i, role in enumerate([
        'nb:02:13:0', 'writer:functional_anatomy_plane_qc_rows', 'writer:functional_anatomy_intensity_overlay_rows'])]
    cards, excluded, _ = select_curated_cards(figures)
    assert cards[0]['id'] == 'P003'
    assert set(excluded) == {'1', '2'}


def test_product_paths_support_native_and_staged_contexts(tmp_path):
    from codeants_2pf_hcr.qc_notebooks import QCNotebookContext
    fish = tmp_path/'L765_f05'
    masks = fish/'03_analysis/structural/cp_masks'; masks.mkdir(parents=True)
    (masks/'L765_f05_anatomy_cp_masks.tif').touch()
    stages = ['prepare-in-vivo-anatomy-stack', 'make-functional-registration-qc',
              'transform-functional-rois-to-anatomy', 'prepare-functional-reference-stacks', 'register-functional-to-anatomy']
    staged = QCNotebookContext('L765_f05', tmp_path, fish, tmp_path/'run',
                              {stage: tmp_path/'run'/stage for stage in stages}, {}, {})
    paths = registration_review_product_paths(staged)
    assert paths['plane_refs_summary_path'] == tmp_path/'run/make-functional-registration-qc/plane_refs_summary_qc.json'
    assert paths['anatomy_stack_path'] == fish/'02_reg/00_preprocessing/2p_anatomy/L765_f05_anatomy_2P_GCaMP.nrrd'
    marker = fish/'03_analysis/functional/pipeline_manifests'; marker.mkdir(parents=True)
    (marker/'native_layout.json').write_text('{}')
    native = QCNotebookContext('L765_f05', tmp_path, fish, fish, {stage: fish for stage in stages}, {}, {})
    paths = registration_review_product_paths(native)
    assert paths['suite2p_label_dir'] == fish/'03_analysis/functional/qa/suite2p_native_labels'
    assert paths['plane_refs_summary_path'] == fish/'03_analysis/functional/registration/plane_refs_summary_qc.json'


def test_ncc_offset_and_zero_padding_keep_visible_alignment(products):
    path = products['positioned_reference_dir'] / 'L765_f05_plane9_mcorrected_flipX_ncc_xy_warped.tif'
    warped = np.zeros((24, 32), dtype=np.float32)
    warped[4:20, 5:27] = 10000 + np.arange(16*22).reshape(16,22)
    tifffile.imwrite(path, warped)
    fig = render_registration_plane_review(**products)
    display = np.asarray(fig.axes[6].images[0].get_array())
    assert display[4:20,5:27].min() == 0 and display[4:20,5:27].max() == 1
    assert display[:4].max() == 0
    assert 'contrast within placed FOV' in fig.axes[6].get_title()
    np.testing.assert_array_equal(fig.axes[7].images[0].get_array()[...,1], display)
    plt.close(fig)
