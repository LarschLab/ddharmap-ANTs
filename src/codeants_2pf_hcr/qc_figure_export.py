"""Extract executed-notebook PNG artifacts and current writer PNGs for review."""
from __future__ import annotations
from codeants_2pf_hcr.paths import visible_glob, visible_rglob, is_visible_path

import base64
import ast
import csv
import hashlib
import html
import io
import json
from pathlib import Path
import re
import shutil
import textwrap
import uuid

from PIL import Image, ImageDraw, ImageFont, ImageStat


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _build_qc_figure_gallery(*, notebook_dir: Path, manifest_dir: Path, output_dir: Path, fish_dir: Path,
                            review_gate: dict | None = None, manifest_paths: tuple[Path, ...] | None = None,
                            notebook_count: int | None = None) -> dict:
    """Export all rendered PNGs with exact source associations, leaving inputs untouched."""
    if output_dir.exists():
        raise FileExistsError(f"Refusing existing gallery: {output_dir}")
    if review_gate is not None and review_gate.get("status") != "review_required":
        raise ValueError("Preliminary gallery must retain a review_required scientific gate")
    notebooks = sorted(visible_glob(notebook_dir, "*.ipynb"))
    numbers = [re.match(r"^(\d\d)_", p.name).group(1) for p in notebooks if re.match(r"^(\d\d)_", p.name)]
    # A review-gated run stops before notebook 06 unless its recipe declares the
    # accepted-midline continuation (seven notebooks).
    notebook_count = int(notebook_count) if notebook_count is not None else (6 if review_gate else 7)
    if notebook_count not in {6, 7}:
        raise ValueError(f"Unsupported QC notebook count: {notebook_count}")
    expected = [f"{i:02}" for i in range(notebook_count)]
    if numbers != expected:
        raise ValueError(f"Require exactly executed QC notebooks {expected}; found {numbers}")
    output_dir.mkdir(parents=True)
    png_dir = output_dir / "figures"
    png_dir.mkdir()
    figures, by_hash, notebook_reports, manifest_reports = [], {}, [], []

    def add_png(payload: bytes, source: dict, title: str) -> None:
        digest = hashlib.sha256(payload).hexdigest()
        if digest in by_hash:
            by_hash[digest]["sources"].append(source)
            return
        with Image.open(io.BytesIO(payload)) as image:
            image.load()
            width, height = image.size
            stats = ImageStat.Stat(image.convert("RGB"))
            variation = max(stats.stddev)
            if min(width, height) < 100 or variation < 0.1:
                raise ValueError(f"Blank or undersized figure: {title}: {width}x{height}; stddev={variation}")
        ordinal = len(figures) + 1
        slug = re.sub(r"[^a-zA-Z0-9]+", "_", title).strip("_")[:90]
        filename = f"{ordinal:03}_{slug}.png"
        (png_dir / filename).write_bytes(payload)
        record = {"id": f"F{ordinal:03}", "title": title, "file": f"figures/{filename}",
                  "sha256": digest, "width": width, "height": height,
                  "max_channel_stddev": variation, "sources": [source]}
        figures.append(record)
        by_hash[digest] = record

    for notebook in notebooks:
        payload = json.loads(notebook.read_text())
        notebook_hash = sha256(notebook)
        heading = notebook.stem
        png_count, code_count, other_images, blocked_messages, cell_coverage = 0, 0, [], [], []
        for cell_index, cell in enumerate(payload["cells"]):
            source_text = "".join(cell.get("source", []))
            if cell.get("cell_type") == "markdown":
                headings = [line.lstrip("# ") for line in source_text.splitlines() if line.startswith("#")]
                if headings:
                    heading = headings[-1]
                continue
            if cell.get("cell_type") != "code" or not source_text.strip():
                continue
            code_count += 1
            if cell.get("execution_count") is None:
                raise ValueError(f"Unexecuted nonempty cell {notebook.name}:{cell_index}")
            try:
                calls = [node.func.id if isinstance(node.func, ast.Name) else node.func.attr
                         for node in ast.walk(ast.parse(source_text))
                         if isinstance(node, ast.Call) and isinstance(node.func, (ast.Name, ast.Attribute))]
                renderer_calls = [name for name in calls if name.startswith(("render_", "plot_"))]
            except SyntaxError:
                renderer_calls = []  # Execution-copy IPython backend configuration may use a magic.
            cell_png_count = 0
            for output_index, output in enumerate(cell.get("outputs", [])):
                if output.get("output_type") == "error":
                    raise ValueError(f"Notebook error {notebook.name}:{cell_index}: {output.get('evalue')}")
                data = output.get("data", {})
                for mime in data:
                    if mime.startswith("image/") and mime != "image/png":
                        other_images.append({"cell_index": cell_index, "output_index": output_index, "mime": mime})
                plain = "".join(data.get("text/plain", []))
                if "blocked" in plain.lower() or "unavailable" in plain.lower():
                    blocked_messages.append({"cell_index": cell_index, "text": plain[:1000]})
                if "image/png" not in data:
                    continue
                encoded = data["image/png"]
                if isinstance(encoded, list):
                    encoded = "".join(encoded)
                png_count += 1
                cell_png_count += 1
                add_png(base64.b64decode(encoded), {
                    "kind": "executed_notebook_png", "path": str(notebook),
                    "notebook_sha256": notebook_hash, "cell_index_zero_based": cell_index,
                    "output_index_zero_based": output_index, "heading": heading,
                }, f"{notebook.name[:2]} · {heading} · image {png_count}")
            cell_coverage.append({"cell_index_zero_based": cell_index, "heading": heading,
                                  "renderer_calls": renderer_calls, "png_outputs": cell_png_count})
        if not notebook.name.startswith("00_") and not png_count:
            raise ValueError(f"QC notebook has no rendered figures: {notebook.name}")
        if other_images:
            raise ValueError(f"Non-PNG notebook figures require explicit retrieval: {notebook.name}: {other_images}")
        notebook_reports.append({"path": str(notebook), "sha256": notebook_hash,
                                 "executed_code_cells": code_count, "png_outputs": png_count,
                                 "cell_figure_coverage": cell_coverage,
                                 "renderer_cells_without_png": [entry for entry in cell_coverage if entry["renderer_calls"] and not entry["png_outputs"]],
                                 "blocked_or_unavailable_outputs": blocked_messages})

    for manifest in sorted(manifest_paths if manifest_paths is not None else visible_glob(manifest_dir, "*_manifest.json")):
        if (not is_visible_path(manifest)) or ".pre_" in manifest.name:
            continue
        payload = json.loads(manifest.read_text())
        manifest_hash = sha256(manifest)
        pngs = []
        for output in payload.get("outputs", []):
            if not isinstance(output, dict) or not output.get("path"):
                continue
            path = Path(output["path"])
            candidates = [path] if path.suffix.lower() == ".png" else list(visible_rglob(path, "*.png")) if path.is_dir() else []
            for candidate in sorted(candidates):
                if not candidate.is_file():
                    raise FileNotFoundError(f"Manifest PNG missing: {candidate}")
                if not candidate.resolve().is_relative_to(fish_dir.resolve()):
                    raise ValueError(f"Current production PNG points outside actual fish folder: {candidate}")
                pngs.append(str(candidate))
                add_png(candidate.read_bytes(), {"kind": "writer_png", "path": str(candidate),
                                                "manifest": str(manifest), "manifest_sha256": manifest_hash,
                                                "stage_name": payload.get("stage_name")},
                        f"writer · {candidate.stem}")
        manifest_reports.append({"path": str(manifest), "sha256": manifest_hash, "png_paths": pngs})

    try:
        font = ImageFont.truetype("/System/Library/Fonts/Supplemental/Arial.ttf", 34)
    except OSError:
        try:
            font = ImageFont.truetype("DejaVuSans.ttf", 34)
        except OSError:
            font = ImageFont.load_default()
    pages = []
    for page_start in range(0, len(figures), 6):
        items = figures[page_start:page_start + 6]
        sheet = Image.new("RGB", (2800, 3300), "#eeeeee")
        draw = ImageDraw.Draw(sheet)
        for offset, record in enumerate(items):
            col, row = offset % 2, offset // 2
            x, y = col * 1400 + 25, row * 1100 + 20
            label = f"{record['id']}  {record['title']}"
            lines = textwrap.wrap(label, width=67)[:3]
            for line_index, line in enumerate(lines):
                draw.text((x + 10, y + 8 + 38 * line_index), line, fill="#111111", font=font)
            with Image.open(output_dir / record["file"]) as original:
                preview = original.convert("RGB")
                preview.thumbnail((1350, 930), Image.Resampling.LANCZOS)
                sheet.paste(preview, (x + (1350 - preview.width) // 2, y + 140 + (930 - preview.height) // 2))
        filename = f"contact_sheet_{len(pages)+1:02}.png"
        sheet.save(output_dir / filename)
        pages.append({"file": filename, "figure_ids": [record["id"] for record in items], "sha256": sha256(output_dir / filename)})

    report = {"format_version": 1, "fish_id": fish_dir.name, "fish_dir": str(fish_dir),
              "notebook_coverage": notebook_reports, "writer_manifest_coverage": manifest_reports,
              "figure_count": len(figures), "notebook_png_output_count": sum(r["png_outputs"] for r in notebook_reports),
              "writer_png_reference_count": sum(len(r["png_paths"]) for r in manifest_reports),
              "figures": figures, "contact_sheets": pages,
              "deduplication_policy": "Exact SHA-256 of PNG bytes; every notebook/writer association retained",
              "notebook_00_note": "Table-only provenance overview; no figure to retrieve"}
    (output_dir / "gallery_manifest.json").write_text(json.dumps(report, indent=2) + "\n")
    if review_gate:
        report["scientific_release"] = review_gate
        report["notebook_06_note"] = "Deferred until midline review; not a complete final QC gallery"
        (output_dir / "gallery_manifest.json").write_text(json.dumps(report, indent=2) + "\n")
    with (output_dir / "figure_index.csv").open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["id", "title", "file", "sha256", "width", "height", "source_count"])
        for record in figures:
            writer.writerow([record[key] for key in ("id", "title", "file", "sha256", "width", "height")] + [len(record["sources"])])
    cards = []
    for record in figures:
        sources = "<br>".join(html.escape(f"{s['kind']}: {s['path']}" + (f" cell {s['cell_index_zero_based']} output {s['output_index_zero_based']}" if s['kind'] == 'executed_notebook_png' else "")) for s in record["sources"])
        cards.append(f"<article><h2>{record['id']} {html.escape(record['title'])}</h2><a href='{record['file']}'><img loading='lazy' src='{record['file']}'></a><details><summary>Source provenance and SHA-256</summary>{sources}<p>{record['sha256']}</p></details></article>")
    (output_dir / "index.html").write_text("<!doctype html><meta charset='utf-8'><title>L765_f04 production QC figures</title><style>body{font:16px system-ui;max-width:1800px;margin:30px auto;padding:20px}article{margin:50px 0;border-top:1px solid #ccc}img{max-width:100%;height:auto}details{overflow-wrap:anywhere}</style><h1>L765_f04: complete production QC figure gallery</h1><p>Full-resolution images extracted from all seven executed QC notebooks and current writer manifests. Notebook 00 is table-only. Exact duplicate PNGs share an entry; all source associations are retained.</p>" + "".join(cards))
    page = output_dir / "index.html"
    page.write_text(page.read_text().replace("L765_f04", html.escape(fish_dir.name)).replace("complete production QC figure gallery", ("midline-review QC figure gallery" if notebook_count == 6 else "accepted-midline review QC figure gallery") if review_gate else "complete production QC figure gallery").replace("all seven executed QC notebooks", "six executed pre-midline QC notebooks" if notebook_count == 6 else "all seven executed QC notebooks"))
    return {"output_dir": str(output_dir), "figure_count": len(figures), "notebook_png_outputs": report["notebook_png_output_count"], "writer_png_references": report["writer_png_reference_count"], "contact_sheets": len(pages), "blocked_messages": sum(len(r["blocked_or_unavailable_outputs"]) for r in notebook_reports)}


def export_qc_figure_gallery(*, notebook_dir: Path, manifest_dir: Path, output_dir: Path, fish_dir: Path,
                            review_gate: dict | None = None, manifest_paths: tuple[Path, ...] | None = None,
                            notebook_count: int | None = None) -> dict:
    """Publish a complete gallery only after coverage and image checks pass."""
    output_dir = output_dir.expanduser().resolve()
    if output_dir.exists():
        raise FileExistsError(f"Refusing existing gallery: {output_dir}")
    staged = output_dir.parent / f".{output_dir.name}.partial-{uuid.uuid4().hex}"
    try:
        report = _build_qc_figure_gallery(notebook_dir=notebook_dir, manifest_dir=manifest_dir,
                                         output_dir=staged, fish_dir=fish_dir, review_gate=review_gate, manifest_paths=manifest_paths,
                                         notebook_count=notebook_count)
        staged.rename(output_dir)
        return {**report, "output_dir": str(output_dir)}
    except Exception:
        if staged.exists():
            shutil.rmtree(staged)
        raise


__all__ = ["export_qc_figure_gallery"]
