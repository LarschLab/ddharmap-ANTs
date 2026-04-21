# Cross-Platform Notebook Runtime

- Use `envs/environment-windows.yml` on Windows and `envs/environment-macos.yml` on macOS.
- Set `CODEANTS_2PF_HCR_LOCAL_ROOT` to the local analysis root when running in `DATA_MODE="local"`.
- Base notebook imports should work without optional native deps, but native stages still require:
  - `cellpose` and `torch` for `[24]` / `[24a]`
  - `antspyx` for `[43]` / `[43b]`
  - `pynrrd` or `SimpleITK` for `.nrrd` reads
- To inspect the active environment from Python:

```python
from codeants_2pf_hcr import build_dependency_preflight_report
build_dependency_preflight_report()
```
