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

- On Windows, `cellpose installed` is not enough: `torch` also has to be loadable in the same process as the rest of the scientific stack.
- If `[24]` or `[24a]` fails with `WinError 127` on `torch\\lib\\shm.dll`, or with `OMP Error #15` mentioning `libomp.dll` and `libiomp5md.dll`, the environment has mixed native runtimes.
- The Windows env spec now prefers a conda-installed CPU PyTorch (`pytorch-cpu`) instead of a pip `torch` wheel to avoid that OpenMP collision with the conda-forge stack.
- The package top level now resolves exports lazily so importing a small notebook stage does not eagerly import the full scientific stack before `torch`/`cellpose`.
- Use a targeted preflight when debugging segmentation stages:

```python
from codeants_2pf_hcr import build_dependency_preflight_report
build_dependency_preflight_report(["torch", "cellpose"])
```
