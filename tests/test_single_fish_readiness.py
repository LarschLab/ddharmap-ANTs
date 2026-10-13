from pathlib import Path
import pytest
from codeants_2pf_hcr.single_fish_readiness import expected_transform_paths, resolve_archive_path

@pytest.mark.parametrize("round_name,route,count", [
    ("rbest", "rbest_to_exvivo_then_exvivo_to_2p", 4),
    ("r2", "direct_rn_to_exvivo_then_exvivo_to_2p", 4),
    ("r3", "direct_rn_to_exvivo_then_exvivo_to_2p", 4),
    ("r4", "rn_via_rbest_to_exvivo_then_exvivo_to_2p", 6)])
def test_routes(round_name, route, count):
    paths = expected_transform_paths("L765_f02", round_name, route)
    assert len(paths) == count
    assert paths[0].endswith("exvivo_GCaMP_to_2p_1Warp.nii.gz")
    assert all(p.endswith("1Warp.nii.gz") for p in paths[::2])
    assert all(p.endswith("0GenericAffine.mat") for p in paths[1::2])
    if round_name == "r4":
        assert paths[-1].endswith("r4_GCaMP_to_rbest_0GenericAffine.mat")

@pytest.mark.parametrize("round_name,route", [
    ("rbest", "direct_rn_to_exvivo_then_exvivo_to_2p"),
    ("r2", "rbest_to_exvivo_then_exvivo_to_2p"),
    ("r2", "unknown")])
def test_bad_routes(round_name, route):
    with pytest.raises(ValueError):
        expected_transform_paths("L765_f02", round_name, route)

def test_explicit_same_fish_relocation():
    assert resolve_archive_path("/archive/L765_f02/a/b", archive_root=Path("/archive"),
                                fish_dir=Path("/data/L765_f02")) == Path("/data/L765_f02/a/b")
    for path in ("/archive/L765_f04/a", "/data/L765_f02/a", "/archive/L765_f02/../L765_f04/a"):
        with pytest.raises(ValueError):
            resolve_archive_path(path, archive_root=Path("/archive"), fish_dir=Path("/data/L765_f02"))
