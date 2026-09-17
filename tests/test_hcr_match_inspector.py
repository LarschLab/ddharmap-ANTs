"""Focused checks for the read-only HCR match inspector overlays."""

import base64
import io

import numpy as np
from PIL import Image

from codeants_2pf_hcr.hcr_match_inspector import _crop_bounds, _image_data, _panel


def test_crop_bounds_contains_hcr_and_anatomy_partner():
    hcr = (slice(2, 4), slice(10, 16), slice(10, 18))
    partner = (slice(2, 4), slice(35, 42), slice(38, 43))
    y0, y1, x0, x1 = _crop_bounds(hcr, partner, (80, 80))
    assert y0 <= 10 < 42 <= y1
    assert x0 <= 10 < 43 <= x1
    assert y1 - y0 == x1 - x0


def test_panel_marks_only_linked_plane_roi_in_bright_cyan():
    background = np.zeros((1, 20, 20), dtype=np.uint16)
    anatomy = background.copy()
    hcr = background.copy()
    first = np.zeros((20, 20), dtype=np.uint16)
    second = first.copy()
    first[5:9, 5:9] = 7
    second[12:16, 12:16] = 7
    picture = np.asarray(_panel(background, anatomy, hcr, [(0, first), (1, second)], {0: {7}}, None, 4, 0, (0, 20, 0, 20)))
    assert np.any(np.all(picture == (40, 225, 255), axis=2))
    assert np.any(np.all(picture == (45, 135, 170), axis=2))


def test_image_data_uses_recorded_spacing_for_field_width():
    background = np.zeros((4, 40, 40), dtype=np.uint16)
    anatomy = background.copy()
    hcr = background.copy()
    hcr[2, 12:16, 13:17] = 8
    box = (slice(2, 3), slice(12, 16), slice(13, 17))
    image, peak, second, width, second_view = _image_data(
        background, anatomy, hcr, {}, [], None, 8, box, None, {}, 0.5,
    )
    decoded = Image.open(io.BytesIO(base64.b64decode(image)))
    assert decoded.format == "WEBP"
    assert decoded.size == (400, 200)
    assert peak == 2
    assert second == 3
    assert width == 20.0
    assert second_view == "adjacent section"
