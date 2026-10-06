"""Explicit mapping from writer products to the established fish tree."""
from pathlib import Path


def native_product_path(fish: Path, stage: str, relative: str = "") -> Path:
    fish = Path(fish)
    functional = fish / "03_analysis" / "functional"
    rel = Path(relative)
    if not relative:
        return fish
    if stage in {"score-activity-bpi", "assign-hcr-identity"}:
        return functional / "derived" / stage / rel
    if stage == "prepare-in-vivo-anatomy-stack":
        return fish / "02_reg" / "00_preprocessing" / "2p_anatomy" / rel.name
    if stage == "prepare-functional-reference-stacks":
        if relative.startswith("functional/raw"):
            return functional / "raw" / Path(relative).relative_to("functional/raw")
        if relative.startswith("qa"):
            return functional / rel
    if stage == "register-functional-to-anatomy":
        if rel.name == "plane_refs_summary.json":
            return functional / "registration" / rel.name
        if rel.parts[0] in {"ncc", "registration", "qa"}:
            return functional / rel
    if stage == "transform-functional-rois-to-anatomy":
        if relative.startswith("functional/anatomy"):
            return functional / "registration" / "reference_planes" / rel.relative_to("functional/anatomy")
        if relative.startswith("functional/native"):
            return functional / "qa" / "suite2p_native_labels" / rel.relative_to("functional/native")
        if rel.name == "functional_roi_anatomy_transform_manifest.csv":
            return functional / "registration" / rel.name
    if stage in {"make-functional-registration-qc", "make-anatomy-midline-proposal"}:
        if rel.parts[0] == "qa":
            return functional / rel
        if rel.name == "plane_refs_summary_qc.json":
            return functional / "registration" / rel.name
    if stage == "register-hcr-to-anatomy" and relative.startswith("confocal/aligned"):
        return fish / "03_analysis" / "confocal" / "aligned" / rel.relative_to("confocal/aligned")
    if stage in {"match-roi-to-anatomy", "export-canonical-tables"} and rel.parts[0] == "registration":
        return functional / rel
    if stage == "materialize-anatomy-midline":
        return functional / "qa" / (rel.relative_to("qa") if rel.parts[0] == "qa" else rel)
    if stage == "make-motion-auc-tables" and rel.parts[0] == "registration":
        return functional / "derived" / rel.relative_to("registration")
    if stage == "make-qa-report":
        return functional / "qa" / rel
    if stage == "make-figures" and rel.parts[0] == "04_plots":
        return fish / rel
    raise ValueError(f"No native destination for {stage}/{relative}")


def is_native_root(root: Path) -> bool:
    return (Path(root) / "03_analysis" / "functional" / "pipeline_manifests" / "native_layout.json").is_file()


def stage_product_path(root: Path, stage: str, relative: str) -> Path:
    """Read one native product or retain the historical staged-root interface."""
    if is_native_root(root):
        return native_product_path(root, stage, relative)
    return Path(root) / relative
