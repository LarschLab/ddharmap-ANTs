#!/usr/bin/env python3
"""Compare a baseline Git revision with this checkout using temporary review sessions.

Run with the GUI environment and PYTHONPATH=src. The full path includes napari
slice changes, Qt event delivery, the existing 30 ms debounce and widget paints.
The side-view measurement isolates update_orthos; it excludes napari slicing.
"""
import argparse
import importlib.util
import json
import platform
from importlib.metadata import version
from pathlib import Path
import statistics
import subprocess
import tempfile
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--baseline', required=True, help='Git commit before the optimization')
    parser.add_argument('--steps', type=int, default=40)
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--screenshots', help='Optional directory for untimed full-window screenshots')
    args = parser.parse_args()
    from qtpy.QtWidgets import QApplication
    from qtpy.QtTest import QTest
    from codeants_2pf_hcr.manual_identity import Dataset, AnnotationSession
    from codeants_2pf_hcr.apps import manual_identity as optimized

    source = subprocess.check_output(['git', 'show', args.baseline + ':src/codeants_2pf_hcr/apps/manual_identity.py'])
    with tempfile.TemporaryDirectory(prefix='cellmatcher-benchmark-') as folder:
        folder = Path(folder)
        baseline_path = folder / 'baseline.py'
        baseline_path.write_bytes(source)
        spec = importlib.util.spec_from_file_location('cellmatcher_baseline', baseline_path)
        baseline = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(baseline)
        dataset = Dataset(args.manifest)
        results = {'python': platform.python_version(), 'platform': platform.platform(),
                   'napari': version('napari'), 'numpy': version('numpy'),
                   'window_pixels': [1500, 1000],
                   'shape_zyx': dataset.labels.shape, 'eligible_cells': len(dataset.eligible),
                   'baseline': args.baseline, 'steps': args.steps, 'repeats': args.repeats,
                   'debounce_wait_ms': 35, 'trials': []}
        for repeat in range(args.repeats):
            order = [('baseline', baseline), ('optimized', optimized)]
            if repeat % 2:
                order.reverse()
            for name, module in order:
                session = AnnotationSession(dataset, folder / f'{name}-{repeat}', 'temporary benchmark')
                controls = {}
                viewer = module.create_viewer(dataset, session, show=True, controls=controls)
                try:
                    viewer.window.resize(1500, 1000)
                    app = QApplication.instance()
                    QTest.qWait(500)
                    app.processEvents()
                    if args.screenshots:
                        screenshot_dir = Path(args.screenshots)
                        screenshot_dir.mkdir(parents=True, exist_ok=True)
                        viewer.screenshot(path=str(screenshot_dir / f'{name}-{repeat}.png'), canvas_only=False)
                    update = controls['update_orthos']
                    # One unmeasured sweep warms the same slices and rendering path.
                    start_z = max(0, dataset.labels.shape[0] // 2 - args.steps // 2)
                    zs = [(start_z + i) % dataset.labels.shape[0] for i in range(args.steps)]
                    for z in zs:
                        viewer.dims.set_current_step(0, z)
                        QTest.qWait(35)
                        app.processEvents()
                    side = []
                    for _ in range(args.steps):
                        start = time.perf_counter()
                        update()
                        side.append((time.perf_counter() - start) * 1000)
                    full = []
                    for z in zs:
                        start = time.perf_counter()
                        viewer.dims.set_current_step(0, z)
                        QTest.qWait(35)
                        app.processEvents()
                        viewer.window._qt_window.repaint()
                        full.append((time.perf_counter() - start) * 1000)
                        assert viewer.dims.current_step[0] == z, 'Scroll did not reach requested slice'
                    trial = dict(implementation=name, repeat=repeat,
                                 orthos_median_ms=statistics.median(side),
                                 full_scroll_median_ms=statistics.median(full),
                                 full_scroll_total_ms=sum(full))
                    results['trials'].append(trial)
                    print(json.dumps(trial), flush=True)
                finally:
                    viewer.close()
                    session.close()
        print(json.dumps(results, indent=2))


if __name__ == '__main__':
    main()
