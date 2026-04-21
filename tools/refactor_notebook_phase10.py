from __future__ import annotations

import json
from pathlib import Path


NOTEBOOK_PATH = Path("notebooks/2PF_to_HCR.ipynb")


def _replace_56g(source: str) -> str:
    old_helper = """def _bool_from_any_local(value):
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except Exception:
        pass
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, (int, np.integer)):
        return bool(value)
    if isinstance(value, (float, np.floating)):
        if not np.isfinite(value):
            return False
        return bool(int(value))
    return str(value).strip().lower() in {'1', 'true', 't', 'yes', 'y'}
"""
    new_block = """_response_active_truthy = {'1', 'true', 't', 'yes', 'y'}
"""
    source = source.replace(old_helper, new_block)
    old_call = "            df['response_is_active'] = df['response_is_active'].map(_bool_from_any_local).astype(bool)\n"
    new_call = """            _response_active_series = df['response_is_active']
            _response_active_numeric = pd.to_numeric(_response_active_series, errors='coerce')
            df['response_is_active'] = np.where(
                _response_active_series.isna(),
                False,
                np.where(
                    _response_active_numeric.notna(),
                    _response_active_numeric.astype(float) != 0.0,
                    _response_active_series.astype(str).str.strip().str.lower().isin(_response_active_truthy),
                ),
            ).astype(bool)
"""
    return source.replace(old_call, new_call)


def _replace_57(source: str) -> str:
    old_helper = """def _effective_motion_span_57(row, onset_delay_sec):
    t0, t1, _ = effective_motion_window(
        row.get('start', np.nan),
        row.get('duration', np.nan),
        row.get('end', np.nan),
        onset_delay_sec,
    )
    return t0, t1

"""
    source = source.replace(old_helper, "")
    old_call = "                                    t0, t1 = _effective_motion_span_57(row, FULL_TRACE_STIM_ONSET_DELAY_SEC)\n"
    new_call = """                                    t0, t1, _ = effective_motion_window(
                                        row.get('start', np.nan),
                                        row.get('duration', np.nan),
                                        row.get('end', np.nan),
                                        FULL_TRACE_STIM_ONSET_DELAY_SEC,
                                    )
"""
    return source.replace(old_call, new_call)


def main() -> None:
    notebook = json.loads(NOTEBOOK_PATH.read_text())
    for cell in notebook.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        source = "".join(cell.get("source", []))
        if source.startswith("# [56g]"):
            source = _replace_56g(source)
        elif source.startswith("# [57]"):
            source = _replace_57(source)
        cell["source"] = source.splitlines(keepends=True)
    NOTEBOOK_PATH.write_text(json.dumps(notebook, indent=1))


if __name__ == "__main__":
    main()
