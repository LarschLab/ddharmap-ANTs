# Auto-extracted notebook helpers.
# Source notebooks: antsQC.ipynb, 2PF_to_HCR.ipynb
from pathlib import Path
import re

__all__ = [
    'gene_from_mask',
    '_parse_round_from_name',
]

def gene_from_mask(path_str):
    name = Path(path_str).name if path_str is not None else ""
    import re
    m = re.search(r'channel\d+_(.+?)_cp_masks', name)
    gene = m.group(1) if m else name
    return gene.replace('sst1_', 'sst1.')

def _parse_round_from_name(path: Path):
    m = re.search(r"round(\d+)", path.name.lower())
    return int(m.group(1)) if m else None
