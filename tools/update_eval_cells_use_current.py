import json
from pathlib import Path


def update_notebook(nb_path: Path) -> None:
    d = json.loads(nb_path.read_text())
    cells = d.get('cells', [])

    # 1) Update EVAL_DATASETS config: remove 'ants' entry and add a note
    for c in cells:
        if c.get('cell_type') == 'code':
            src = ''.join(c.get('source', []))
            if '# --- Evaluation datasets (pre-warped confocal in 2P space) ---' in src:
                lines = src.splitlines()
                # Build new config block
                new = []
                new.append("# --- Evaluation datasets (pre-warped confocal in 2P space) ---")
                new.append("# Only external datasets here; the current ANTs run is added automatically from memory.")
                new.append("EVAL_DATASETS = {")
                new.append("    # Example placeholders — update to your files")
                new.append("    'bigwarp':  {'name': 'BigWarp',  'conf_labels_2p_path': '/path/to/confocal_labels_BigWarp_in_2P.tif'},")
                new.append("    'baseline': {'name': 'Baseline', 'conf_labels_2p_path': '/path/to/confocal_labels_baseline_in_2P.tif'},")
                new.append("}")
                c['source'] = [line + '\n' for line in new]
                break

    # 2) Update precompute cell to add current ANTs run into EVAL_CACHE before file-based datasets
    for c in cells:
        if c.get('cell_type') == 'code':
            src = ''.join(c.get('source', []))
            if src.startswith("# --- Precompute centroids per dataset"):
                # Insert after EVAL_CACHE = {}
                target = "\nEVAL_CACHE = {}\n"
                if target in src and "name': meta.get('name', key)" in src:
                    inject = (
                        "\nEVAL_CACHE = {}\n"
                        "# Add current run as 'ANTs' from in-memory variables if available\n"
                        "if all(k in globals() for k in ('conf_labels_2p','df_conf','P_conf_in_2p_um')):\n"
                        "    EVAL_CACHE['ants'] = {\n"
                        "        'name': 'ANTs',\n"
                        "        'conf_arr': conf_labels_2p,\n"
                        "        'df_conf': df_conf,\n"
                        "        'P_conf_um': P_conf_in_2p_um,\n"
                        "        'conf_lut': dict(zip(df_conf['label'].to_numpy(), P_conf_in_2p_um)),\n"
                        "    }\n"
                    )
                    src = src.replace(target, inject)
                    c['source'] = [line + '\n' for line in src.splitlines()]
                break

    d['cells'] = cells
    nb_path.write_text(json.dumps(d, indent=1, ensure_ascii=False))


def main():
    nb = Path('antsQC.ipynb')
    if not nb.exists():
        raise SystemExit('antsQC.ipynb not found')
    update_notebook(nb)
    print('Updated EVAL_DATASETS to exclude ANTs path and added in-memory ANTs to EVAL_CACHE.')


if __name__ == '__main__':
    main()

