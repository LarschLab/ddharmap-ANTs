#!/usr/bin/env python3
"""Create a pinned native single-fish recipe through the midline review gate."""
import argparse
import json
from pathlib import Path
from codeants_2pf_hcr.fresh_run_spec import build_fresh_run_spec


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("fish-id", "run-id"):
        parser.add_argument("--" + name, required=True)
    for name in ("data-root", "repo-root", "spec"):
        parser.add_argument("--" + name, required=True, type=Path)
    parser.add_argument("--ncc-cache-source", type=Path, default=None,
                        help="Optional legacy best-Z/scale cache; omitted means search from current references")
    parser.add_argument("--plane-index", type=int, action="append", required=True)
    parser.add_argument("--midline-decision-path", type=Path, default=None,
                        help="Fish-specific logbook note with midline_gate: accepted and the reviewed proposal hashes")
    parser.add_argument("--midline-reviewer", default=None)
    parser.add_argument("--geometry-control-supersede-reason", default=None,
                        help="Explicit reason a deliberate upstream correction supersedes the fish-local ROI/anatomy control")
    args = parser.parse_args()
    payload = build_fresh_run_spec(fish_id=args.fish_id, data_root=args.data_root, repo_root=args.repo_root,
                                  run_id=args.run_id, spec_path=args.spec, plane_indices=tuple(args.plane_index),
                                  ncc_cache_source=args.ncc_cache_source,
                                  geometry_control_supersede_reason=args.geometry_control_supersede_reason,
                                  midline_decision_path=args.midline_decision_path, midline_reviewer=args.midline_reviewer)
    print(json.dumps({"spec": str(args.spec), "fish_id": args.fish_id, "stages": len(payload["stages"]), "review_gate": payload["review_gate"]}))


if __name__ == "__main__":
    main()
