# cellMatcher Z-scroll performance

Measured on 2026-10-12 on the real, prepared L765_f04 dataset: 72 × 750 × 750 voxels, 20,858 eligible ex-vivo masks, first HCR marker stack. The scientific review-scope restriction was absent in both implementations, so this comparison isolates display optimization.

The performance branch caches scaled XZ/YZ image sections and their annotation outlines. A pure Z change paints position indicators onto copies of the cached images and leaves the image crosshair unchanged. Changes to XY cuts, panel width, stacks, selected/pending/recorded cells, positive visibility and image appearance/data invalidate the image cache. Anatomy gamma now follows the image layer in the side views as HCR gamma already did.

## Result

Three clean paired trials alternated order (baseline/optimized, optimized/baseline, baseline/optimized). Each measured 30 successive Z steps after an identical warm-up sweep, with real native Qt windows at 1500 × 1000 pixels, napari slicing, Qt event delivery, the existing 30 ms side-view debounce, a 35 ms event-processing wait per step and window repaint. Dataset loading, window initialization, screenshots and warm-up were outside timing. The session and view settings were temporary; production annotations were untouched.

| Trial | Baseline total | Optimized total | Time reduction |
| --- | ---: | ---: | ---: |
| 1 | 13.850 s | 8.211 s | 40.7% |
| 2 | 14.378 s | 7.819 s | 45.6% |
| 3 | 14.852 s | 8.720 s | 41.3% |

Median total: **14.378 s → 8.211 s (42.9% less time)**. Median of trial frame medians: **450.98 ms → 260.58 ms**. Isolated side-view callback median of trial medians: **138.80 ms → 0.098 ms**; this callback measurement excludes napari slice changes and Qt paints and must not be presented as overall scrolling speed.

Raw trial measurements: [cellmatcher-scroll-performance-results.json](cellmatcher-scroll-performance-results.json). Runtime: Python 3.10.20, napari 0.6.6, NumPy 2.2.6, PyQt5 5.15.11 / Qt package 5.15.19, macOS 26.6.2 ARM64.

The first attempted measurement overlapped another CPU-heavy pytest run and was discarded. The reported three pairs were rerun after that process ended. This is one fish and one marker stack on this computer; it demonstrates a reduction in the tested complete scroll path, not a guarantee of the same percentage for every fish, GPU or display. Main image/label rendering still contributes substantial latency. The harness drives the same dimension-change path used for Z scrolling; it does not measure physical mouse-wheel input latency.

## Reproduce

From the checkout being assessed, use the GUI environment and the commit immediately before the optimization:

```bash
PYTHONPATH="$PWD/src" ~/miniforge3/envs/manualIdentity/bin/python \
  tools/benchmark_cellmatcher_scroll.py \
  --manifest /Volumes/dataDrive/dataProcessing/2p_processing/L765_f04/03_analysis/structural/ex_vivo/manual_identity/L765_f04_exvivo_8f39cc28c9b6/dataset_manifest.json \
  --baseline 294603f17d27b744e4dca9d513225b6dd24aabaf \
  --steps 30 --repeats 3 --screenshots /tmp/cellmatcher-scroll-visuals
```

The harness obtains the baseline viewer source from Git and runs both against the same current read-only dataset and temporary annotation-session implementation. Baseline selection should therefore precede only GUI display changes, rather than unrelated scientific/session changes. Avoid concurrent CPU-heavy runs.

## Correctness evidence

`tests/test_manual_identity_gui.py` passed all three real Qt tests in the isolated checkout before final documentation changes. Its focused regression verifies cached image reuse and no crosshair data events on Z changes, pixel-identical output when returning to the same Z, and invalidation for annotation, stack, XY cut, data refresh and appearance/panel-width changes. Combined scope/performance validation passed all 21 manual identity, pipeline, and real Qt GUI tests, including the positive and crosshair visibility assertions. The normal native run reached all passing tests but crashed at interpreter teardown; the final run preserved pytest’s return status and bypassed native cleanup with `os._exit(status)` after `pytest.main` returned. The native teardown issue remains outside this scrolling change.

Untimed real-fish screenshots were saved at `/tmp/cellmatcher-scroll-visuals/{baseline,optimized}-{0,1,2}.png`. The baseline and optimized first screenshots were opened and checked: both full-depth anatomy side sections and red position indicators rendered, marker signal and cell masks were present, and histogram/review controls were readable. Startup Z differs by one slice in those two captures; measured sweeps use the same explicit Z sequence. Rendered evidence stays outside Git.
