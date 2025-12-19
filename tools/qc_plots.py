# Auto-extracted notebook helpers.
# Source notebooks: antsQC.ipynb, 2PF_to_HCR.ipynb
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from IPython.display import display, HTML
from tifffile import imread
from .labels import _ensure_uint_labels

__all__ = [
    'quickshow',
    '_load_labels_or_none',
    '_apply_color',
    'display_scrollable',
]

def quickshow(img, title='', vmin=None, vmax=None):
    plt.figure(figsize=(5,5))
    plt.imshow(img, vmin=vmin, vmax=vmax)
    plt.title(title); plt.axis('off'); plt.show()

def _load_labels_or_none(path):
    if path is None:
        return None
    try:
        if not os.path.exists(path):
            return None
        arr = _ensure_uint_labels(imread(path))
        if arr.ndim == 3 and arr.shape[-1] in (3,4):
            arr = arr[...,0]
        return arr
    except Exception as _e:
        print('Could not load labels from', path, ':', _e)
        return None

def _apply_color(gray01, rgb):
    r, g, b = rgb
    return np.stack([gray01 * r, gray01 * g, gray01 * b], axis=-1)

def display_scrollable(df: pd.DataFrame, max_h=600):
    html = df.to_html(index=False).replace('<table', f'<table style="display:block; max-height:{max_h}px; overflow-y:auto; width:100%;"')
    display(HTML(html))
