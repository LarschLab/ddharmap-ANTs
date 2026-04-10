# Current State

**Purpose:** short status of the intentional mixed migration state.

**Use this file when:** editing downstream cells that may combine ROI-centric and HCR-centric paths.

## Current mixed state (intentional)

- ROI-centric path (`[50i]`, `[50ia]`, `[50j]`, `[50k]`) is used for authoritative whole-population outputs.
- HCR-centric identified-cell path (`[50]`, `[50e]`, `[51]`, `[56]`, `[56h]`, `[57]`) remains active for identified-cell activity workflows.

## Practical warnings

- `[50]` is response-aware via join to the `[50ia]` master ROI table.
- Downstream HCR-centric consumers are expected to require response-aware columns and fail fast if missing.
- Keep path purposes separate:
  - ROI-centric for whole-population inference
  - HCR-centric for identified-cell activity reporting/export
- Avoid implicit hybrids in plotting cells.
