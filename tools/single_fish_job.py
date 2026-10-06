#!/usr/bin/env python3
"""Run the saved fish DAG and finishing checks without interactive supervision."""
import argparse
import json
from pathlib import Path
from codeants_2pf_hcr.single_fish_job import run_finishing_job


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, required=True)
    for name in ("notebook-dir", "manifest-dir", "gallery-dir", "report-dir"):
        parser.add_argument(f"--{name}", type=Path)
    parser.add_argument("--validate-noop", action="store_true")
    args = vars(parser.parse_args())
    args["spec_path"] = args.pop("spec")
    result = run_finishing_job(**args)
    print(json.dumps({key: result.get(key) for key in ("status", "phase", "report", "log", "error")}))
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
