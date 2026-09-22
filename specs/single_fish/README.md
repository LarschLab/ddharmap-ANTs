# Deterministic single-fish run specifications

These versioned JSON files declare orchestration only. Existing package-owned
stage writers remain authoritative for scientific behavior and outputs.

Use the thin wrapper from the repository root:

```bash
PYTHONPATH=src python tools/single_fish_run.py plan   --spec specs/single_fish/L765_f04.json
PYTHONPATH=src python tools/single_fish_run.py run    --spec specs/single_fish/L765_f04.json
PYTHONPATH=src python tools/single_fish_run.py resume --spec specs/single_fish/L765_f04.json
PYTHONPATH=src python tools/single_fish_run.py status --spec specs/single_fish/L765_f04.json
PYTHONPATH=src python tools/single_fish_run.py verify --spec specs/single_fish/L765_f04.json
```

`plan`, `status`, and `verify` are read-only. `run` executes every declared
stage. `resume` skips a stage only when the saved spec hash, checkout commit,
interpreter, input hashes, output hashes, exit status, and validation all still
match. Full combined stdout/stderr lives under the spec's `state_root/logs`;
compact JSON state lives under `state_root/stages`.

The L765_f04 specification pins the last validated scientific commit with a
`scientific-surface` policy. Later commits are allowed only when none of the
listed scientific writers, renderers, contracts, or notebook execution tools
changed. The actual checkout must still be clean, and every stage records its
exact checkout commit. This avoids the self-reference problem of storing a
spec's own eventual Git commit inside the same committed file while retaining
a fail-closed scientific-code pin.

The current spec targets the staged working root. It does not promote outputs
into the actual fish folder. Atomic fish-local promotion is a separate gate.
