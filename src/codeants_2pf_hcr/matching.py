"""Matching helpers for notebook cells [50i] and HCR/anatomy identity lookup."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any, Callable

import pandas as pd


@dataclass(frozen=True)
class MatchingConfig:
    require_overlap: bool = True
    min_overlap: int = 1
    max_dist_um: float = float("inf")
    match_policy_version: str = "roi_all_vs_anat_all_candidate_gated_v1"


def gene_from_mask(path_str: str | Path | None) -> str:
    name = Path(path_str).name if path_str is not None else ""
    match = re.search(r"channel\d+_(.+?)_cp_masks", name)
    gene = match.group(1) if match else name
    return gene.replace("sst1_", "sst1.")


def build_anat_identity_lookup_df(
    hcr_match_results: list[dict[str, Any]] | None,
    gene_order: list[str] | None = None,
    gene_from_mask_func: Callable[[str | Path | None], str] | None = None,
) -> pd.DataFrame:
    order = list(gene_order or [])
    order_map = {str(gene): idx for idx, gene in enumerate(order)}
    infer_gene = gene_from_mask_func if callable(gene_from_mask_func) else gene_from_mask

    def ordered_gene_tuple(genes: set[str]) -> tuple[str, ...]:
        clean = {str(g) for g in genes if g is not None and str(g) and str(g).lower() != "nan"}
        return tuple(sorted(clean, key=lambda gene: (order_map.get(gene, 10**6), gene)))

    anat_gene_sets: dict[int, set[str]] = {}
    for result in hcr_match_results or []:
        final_pairs = result.get("final_pairs") if isinstance(result, dict) else None
        if final_pairs is None or getattr(final_pairs, "empty", True):
            continue
        gene = str(infer_gene(result.get("mask_path", "unknown")))
        anat_series = pd.to_numeric(final_pairs.get("twoP_label", pd.Series(dtype=float)), errors="coerce").dropna().astype(int)
        for anat_label in anat_series.unique():
            anat_gene_sets.setdefault(int(anat_label), set()).add(gene)

    rows = []
    for anat_label, genes in sorted(anat_gene_sets.items()):
        gene_tuple = ordered_gene_tuple(genes)
        rows.append(
            {
                "anat_label": int(anat_label),
                "identity_label": "/".join(gene_tuple) if gene_tuple else pd.NA,
                "identity_gene_count": int(len(gene_tuple)),
                "identity_genes": list(gene_tuple),
            }
        )
    return pd.DataFrame(rows)


__all__ = ["MatchingConfig", "build_anat_identity_lookup_df", "gene_from_mask"]
