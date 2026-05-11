from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys


def test_segmentation_stage_import_stays_narrow() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    src_path = str(repo_root / "src")
    existing = env.get("PYTHONPATH")
    env["PYTHONPATH"] = src_path if not existing else os.pathsep.join([src_path, existing])

    script = """
import json
from codeants_2pf_hcr import AnatomyCellposeConfig, run_anatomy_cellpose_stage
report = {
    "config_name": AnatomyCellposeConfig.__name__,
    "callable": callable(run_anatomy_cellpose_stage),
    "has_matching": "codeants_2pf_hcr.matching" in __import__("sys").modules,
    "has_spatial": "codeants_2pf_hcr.spatial" in __import__("sys").modules,
    "has_scipy": "scipy" in __import__("sys").modules,
    "has_skimage": "skimage" in __import__("sys").modules,
}
print(json.dumps(report))
""".strip()

    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        check=True,
        cwd=repo_root,
        env=env,
        text=True,
    )
    report = json.loads(proc.stdout.strip())

    assert report["config_name"] == "AnatomyCellposeConfig"
    assert report["callable"] is True
    assert report["has_matching"] is False
    assert report["has_spatial"] is False
    assert report["has_scipy"] is False
    assert report["has_skimage"] is False
