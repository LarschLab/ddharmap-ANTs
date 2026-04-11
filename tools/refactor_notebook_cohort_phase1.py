#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path


NB_PATH = Path("notebooks/multi_fish_56h_56g.ipynb")


REPLACEMENTS = {
    "cfg": """# [cfg] Imports + cohort configuration
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from IPython.display import display
from codeants_2pf_hcr import CohortBuildConfig, cohort_cache_paths, resolve_cohort_context_stage

COHORT_BUILD_CONFIG = CohortBuildConfig(
    data_mode="local",
)

cohort_context = resolve_cohort_context_stage(COHORT_BUILD_CONFIG)
globals().update(cohort_context["bindings"])
""",
    "helpers": """# [helpers] Cohort build/cache owner API bindings
from codeants_2pf_hcr import (
    build_cohort_outputs_stage,
    load_cohort_outputs_from_disk,
    save_cohort_outputs_to_disk,
)
""",
    "cohort-build": """# [cohort-build] Load each fish and aggregate trial/cell/trace data
cohort_build_result = build_cohort_outputs_stage(COHORT_BUILD_CONFIG)
globals().update(cohort_build_result["bindings"])
if isinstance(globals().get("cohort_fish_summary_df"), pd.DataFrame) and (not globals()["cohort_fish_summary_df"].empty):
    display(globals()["cohort_fish_summary_df"])
""",
}


def _replace_source(notebook: dict, tag: str, new_source: str) -> None:
    for cell in notebook["cells"]:
        if cell.get("cell_type") != "code":
            continue
        src = "".join(cell.get("source", []))
        if src.startswith(f"# [{tag}]"):
            cell["source"] = [line + "\n" for line in new_source.rstrip("\n").split("\n")]
            return
    raise RuntimeError(f"Notebook cell [{tag}] not found")


def main() -> None:
    notebook = json.loads(NB_PATH.read_text())
    for tag, source in REPLACEMENTS.items():
        _replace_source(notebook, tag, source)
    NB_PATH.write_text(json.dumps(notebook, indent=1))


if __name__ == "__main__":
    main()
