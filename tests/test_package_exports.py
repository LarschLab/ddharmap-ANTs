from codeants_2pf_hcr import (
    _find_embedded_nrrd_header,
    _infer_voxels_from_open_tiff,
    _infer_voxels_nrrd,
    _parse_nrrd_header_text,
    _res_to_um_per_px,
    _to_um,
    apply_func_orientation,
    best_z_by_ncc,
    corrcoef_img,
    imread_any,
    infer_voxels_tiff,
    local_unsharp,
    load_or_cache_voxels,
    norm01,
    top_correlated_mean,
    zproject_mean,
)


def test_notebook_spatial_exports_are_available() -> None:
    assert callable(_find_embedded_nrrd_header)
    assert callable(_infer_voxels_from_open_tiff)
    assert callable(_infer_voxels_nrrd)
    assert callable(_parse_nrrd_header_text)
    assert callable(_res_to_um_per_px)
    assert callable(_to_um)
    assert callable(apply_func_orientation)
    assert callable(best_z_by_ncc)
    assert callable(corrcoef_img)
    assert callable(imread_any)
    assert callable(infer_voxels_tiff)
    assert callable(local_unsharp)
    assert callable(load_or_cache_voxels)
    assert callable(norm01)
    assert callable(top_correlated_mean)
    assert callable(zproject_mean)
