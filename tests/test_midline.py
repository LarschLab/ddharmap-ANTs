import numpy as np

from codeants_2pf_hcr.midline import (
    MidlineProposal,
    fit_fish_midline_consensus,
    fit_native_dark_corridor_model,
    propose_fish_consensus_midline,
    propose_native_dark_corridor_midline,
)


def test_native_dark_corridor_proposes_dark_manual_axis_with_or_without_symmetry():
    image = np.ones((100, 100), dtype=float)
    image[:, 48:53] = 0.0
    labels = [{"x0": 50.0, "y0": 50.0, "theta_deg": -90.0}] * 3
    model = fit_native_dark_corridor_model([image] * 3, labels)

    for symmetry_weight in (0.0, 1.0):
        proposal = propose_native_dark_corridor_midline(
            image, model, symmetry_weight=symmetry_weight
        )
        assert proposal.coordinate_space == "native functional pixel grid"
        assert proposal.requires_manual_acceptance
        assert abs(proposal.x0 - 50.0) < 3.0
        assert abs(abs(proposal.theta_deg) - 90.0) < 5.0


def test_fish_consensus_uses_one_angle_and_robust_linear_depth_shift():
    depths = [0.0, 10.0, 20.0, 30.0, 40.0]
    shape = (100, 100)
    # A vertical line has a horizontal normal, so this makes x increase linearly.
    local = {
        depth: MidlineProposal(45.0 + 0.1 * depth, 50.0, -90.0 + jitter, "local", 0.9)
        for depth, jitter in zip(depths, [-0.5, 0.4, -0.2, 0.3, 35.0])
    }
    consensus = fit_fish_midline_consensus(local, {depth: shape for depth in depths})
    fixed = propose_fish_consensus_midline(25.0, shape, consensus)
    flexible = propose_fish_consensus_midline(
        25.0, shape, consensus, local_line=local[0.0], permitted_angle_deviation_deg=2.0
    )

    assert abs(abs(fixed.theta_deg) - 90.0) < 1.0
    assert abs(fixed.x0 - 47.5) < 1.0
    assert consensus["n_angle_inliers"] == 4
    assert consensus["n_offset_inliers"] >= 4
    assert abs(((flexible.theta_deg - fixed.theta_deg + 90.0) % 180.0) - 90.0) <= 2.0


def test_fish_consensus_keeps_focus_sessions_separate_and_rejects_exact_outliers():
    shape = (100, 100)
    depths = list(range(10))
    local = {
        depth: MidlineProposal(
            40.0 + 2.0 * (depth % 5) + (40.0 if depth == 9 else 0.0),
            50.0,
            -90.0 + (45.0 if depth == 8 else 0.0),
            "local",
            0.9,
        )
        for depth in depths
    }
    sessions = {depth: "first" if depth < 5 else "second" for depth in depths}
    consensus = fit_fish_midline_consensus(local, {depth: shape for depth in depths}, sessions=sessions)
    first = propose_fish_consensus_midline(2.5, shape, consensus, session="first")
    second = propose_fish_consensus_midline(7.5, shape, consensus, session="second")

    assert consensus["n_angle_inliers"] == 9
    assert consensus["offset_models"]["second"]["n_inliers"] == 4
    assert abs(first.x0 - 45.0) < 1.0
    assert abs(second.x0 - 45.0) < 1.0
