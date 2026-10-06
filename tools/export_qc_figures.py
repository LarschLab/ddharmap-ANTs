"""Retrieve PNG artifacts from seven executed QC notebooks and writer manifests."""
import argparse
import json
from pathlib import Path

from codeants_2pf_hcr.qc_figure_export import export_qc_figure_gallery


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--notebook-dir", required=True, type=Path)
    parser.add_argument("--manifest-dir", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--fish-dir", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(export_qc_figure_gallery(**vars(args)), indent=2))


if __name__ == "__main__":
    main()
