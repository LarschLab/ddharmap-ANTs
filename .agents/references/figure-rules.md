# Figure Rules

**Purpose:** keep figures consistent with authoritative tables and response semantics.

**Use this file when:** adding/refactoring plots or trace-summary panels.

## Data source rules

1. Start from the authoritative ROI-centric table unless the figure is explicitly HCR-centric.
2. Define subset filters first; join trace-level data after subset definition.
3. Do not rebuild identity in plotting cells.
4. Do not let figure-local logic silently switch identity sources.

## Activity semantics in figures

- Use response-aware fields from `[50ia]`: `response_is_active`, `response_class`, `response_summary_class`, threshold pass columns.
- Do not use legacy `activity_class`/`is_active` as sole functional-activity definitions.
- If using Suite2p provenance, label explicitly with `suite2p_is_cell` / `suite2p_activity_class`.
- Trace-focused figures should usually filter `suite2p_is_cell == True` unless explicitly analyzing low-quality traces.
- Do not derive `low activity` from ad hoc quantiles/medians when response calls are available.

## HCR-centric figure exception

For explicitly identified-cell activity figures, starting from `hcr_activity_status.csv` or `conf_to_func_pairs.csv` is valid, but scope must be clearly labeled HCR-centric.

## Figure defaults

- Font: prefer `Aptos (Body)` with sans-serif fallback.
- Panel title size: `11`.
- Default width: `A4_width * 0.8` unless panel constraints require otherwise.
- Share axes when directly comparable; disable only with explicit reason.
- Keep paired-condition colors clearly distinct and semantically stable.
- Parameterize spacing/geometry constants at top of cell.
- Auto y-limit with clearance (e.g., `max + 0.1`) for panel-level scaling.
- Use fixed RNG seeds for jittered points.
- Save publication PNG at `dpi=300`; add vector output when needed.
