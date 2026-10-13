from pathlib import Path
import json
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pytest
from codeants_2pf_hcr.plots.qc_functional import render_presented_stimulus_auc_violins
from codeants_2pf_hcr.qc_figure_export import _provenance_rows


def test_violins_fixed_cohort_all_stimuli_and_saved_signed_calls():
    rows = []
    for session, plane in [('r1', 0), ('r2', 5)]:
        for label, value, call in [(1, -.4, 'suppressed'), (2, .1, 'activated'), (3, .02, 'not-significant'), (4, np.nan, 'unavailable')]:
            for stim in ['LB_trajectory', 'WFCo']:
                # Only WFCo passes for label 2, but the fixed cohort must still
                # include that ROI in the LB violin and its NS call count.
                rows.append(dict(plane_idx=plane, func_label=label, session_label=session, stim_type=stim,
                                 mean_auc_dff=value, response_valence='not-significant' if label == 2 and stim.startswith('LB') else call))
    presented = pd.DataFrame(rows)
    cohort = pd.DataFrame([dict(plane_idx=p, func_label=l, any_presented_response_is_active=l < 3) for p in [0, 5] for l in range(1, 5)])
    fig = render_presented_stimulus_auc_violins(presented, cohort, fish_id='L765_f02')
    assert len(fig.axes) == 4
    assert all(ax.get_yscale() == 'linear' for ax in fig.axes)
    assert all([t.get_text() for t in ax.get_xticklabels()] == ['LB', 'WFCo'] for ax in fig.axes)
    assert 'n=4' in fig.axes[0].texts[0].get_text() and 'U 1' in fig.axes[0].texts[0].get_text()
    assert 'n=2' in fig.axes[2].texts[0].get_text() and 'NS 1' in fig.axes[2].texts[0].get_text()
    assert 'med=-0.150' in fig.axes[2].texts[0].get_text()
    assert any('descriptive' in t.get_text() for t in fig.texts)
    assert all('dF/F·s' in ax.get_ylabel() for ax in fig.axes)
    plt.close(fig)


def test_violin_rejects_mixed_session_roi():
    frame = pd.DataFrame([dict(plane_idx=0, func_label=1, session_label=s, stim_type='WFCo', mean_auc_dff=.2, response_valence='activated') for s in ['r1', 'r2']])
    with pytest.raises(ValueError, match='multiple sessions'):
        render_presented_stimulus_auc_violins(frame, pd.DataFrame([dict(plane_idx=0, func_label=1, any_presented_response_is_active=True)]), fish_id='L765_f02')


def test_provenance_one_row_per_gate_with_inline_saved_values(tmp_path):
    tables = [pd.DataFrame({'stimulus':['LB','WFCo'], 'counts':[4,4], 'valid_counts':[4,4]})]
    nb = tmp_path/'01_qc.ipynb'
    outputs = []
    for session in ['r1','r2']:
        outputs.extend([{'output_type':'stream', 'text':[f'{session}: presented and valid trials per stimulus']},
                        {'data':{'text/plain':['saved trial table'], 'text/html':[tables[0].to_html()]}}])
    nb.write_text(json.dumps({'cells':[{'source':['# 10 — trials'], 'outputs':outputs}]}))
    sidecar = tmp_path/'anatomy_midline.json'; sidecar.write_text('{}')
    manifest = tmp_path/'materialize_manifest.json'; manifest.write_text(json.dumps({'stage_name':'materialize-anatomy-midline','outputs':[{'path':str(sidecar)}]}))
    rows = _provenance_rows([nb], [manifest], None)
    assert len(rows) == 7 and len({r['gate'] for r in rows}) == 7
    trials = rows[0]
    assert 'r1: 8 presented' in trials['value'] and 'r2: 8 presented' in trials['value']
    assert 'valid 4/type' in trials['value'] and len(trials['evidence']) == 4
    assert 'SHA-256' in rows[4]['value']
    assert 'provisional' in rows[6]['value']


def test_violin_robust_row_bounds_clipping_and_full_data_medians():
    values = np.r_[np.linspace(-.01, .02, 200), -100., 100.]
    rows = [dict(plane_idx=plane, func_label=i, session_label=session,
                 stim_type='WFCo', mean_auc_dff=value, response_valence='activated')
            for plane, session in [(0, 'r1'), (5, 'r2')]
            for i, value in enumerate(values)]
    cohort = pd.DataFrame([dict(plane_idx=p, func_label=i,
                               any_presented_response_is_active=i < 100)
                           for p in [0, 5] for i in range(len(values))])
    frame = pd.DataFrame(rows)
    fig = render_presented_stimulus_auc_violins(frame, cohort, fish_id='L765_f02')
    expected = max(abs(np.quantile(np.tile(values, 2), [.005, .995])))
    assert fig.axes[0].get_ylim() == pytest.approx((-expected, expected))
    assert fig.axes[0].get_ylim() == fig.axes[1].get_ylim()
    assert fig.axes[2].get_ylim() == fig.axes[3].get_ylim()
    assert fig.axes[2].get_ylim() != fig.axes[0].get_ylim()
    text = fig.axes[0].texts[0].get_text()
    assert '1 above' in text and '1 below' in text
    assert 'n=202' in text and 'A 202' in text
    assert f'med={np.median(values):.3f}' in text
    # Display-only density excludes extremes; median still uses full data.
    vertices = fig.axes[0].collections[0].get_paths()[0].vertices[:, 1]
    assert np.max(np.abs(vertices)) <= expected
    plt.close(fig)


def test_violin_labels_symlog_when_dynamic_range_hides_body():
    values = np.r_[np.linspace(-.001, .001, 200), np.linspace(.1, 1., 20)]
    frame = pd.DataFrame([dict(plane_idx=0, func_label=i, session_label='r1',
                              stim_type='WFCo', mean_auc_dff=v, response_valence='activated')
                          for i, v in enumerate(values)])
    cohort = pd.DataFrame(dict(plane_idx=0, func_label=np.arange(len(values)),
                               any_presented_response_is_active=True))
    fig = render_presented_stimulus_auc_violins(frame, cohort, fish_id='L765_f02')
    assert all(ax.get_yscale() == 'symlog' for ax in fig.axes)
    assert all('symlog; linthresh=' in ax.get_ylabel() for ax in fig.axes)
    assert fig.axes[0].yaxis._scale.linthresh == pytest.approx(np.median(np.abs(values)))
    plt.close(fig)
