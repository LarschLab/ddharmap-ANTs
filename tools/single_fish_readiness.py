#!/usr/bin/env python3
"""Audit remaining-fish inputs without accepting gates or running scientific writers."""
import argparse
import json
from pathlib import Path
from codeants_2pf_hcr.single_fish_readiness import audit_readiness

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, required=True)
    report = audit_readiness(parser.parse_args().spec)
    print(json.dumps({k: report.get(k) for k in ("fish_id", "status", "report", "error")}))
    raise SystemExit(1 if report["status"] == "fail" else 0)
