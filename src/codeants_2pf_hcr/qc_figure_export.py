"""Extract executed-notebook PNG artifacts and current writer PNGs for review."""
from __future__ import annotations
from codeants_2pf_hcr.paths import visible_glob, visible_rglob, is_visible_path, remove_tree

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


# Source roles are notebook/cell labels plus PNG position within that cell, or
# exact writer output names. Extraction ordinals (F...) never select a card.
CURATED_CARDS = (
    ("P001", "Z-drift", ("nb:01:08:0",)),
    ("P002", "Z-drift", ("nb:01:15:0",)),
    ("P003", "Registration/geometry", ("writer:functional_anatomy_plane_qc_rows", "writer:functional_anatomy_intensity_overlay_rows")),
    ("P004", "Registration/geometry", ("nb:03:06:0",)),
    ("P005", "Registration/geometry", ("nb:03:04:0", "nb:03:08:0")),
    ("P006", "Registration/geometry", ("nb:04:06:0",)),
    ("P007", "Registration/geometry", ("nb:05:04:0",)),
    ("P008", "Registration/geometry", ("renderer:render_hcr_pair_status_planes",)),
    ("P009", "Registration/geometry", ("nb:05:06:0",)),
    ("P010", "Midline", ("writer:anatomy_midline_accepted_overlay", "nb:06:08:0")),
    ("P011", "Response/BPI", ("nb:01:11:0",)),
    ("P012", "Response/BPI", ("writer:suite2p_stimulus_locked_heatmaps_23b",)),
    ("P013", "Response/BPI", ("nb:01:11:2",)),
    ("P014", "Response/BPI", ("nb:01:11:3",)),
    ("P015", "Response/BPI", ("nb:06:06:0",)),
    ("P016", "Identity-functional", ("nb:05:05:0",)),
    ("P017", "Identity-functional", ("nb:05:08:0",)),
    ("P018", "Identity-functional", ("nb:06:14:0",)),
    ("P019", "Identity-functional", ("nb:06:15:0",)),
    ("P020", "Identity-functional", ("nb:06:16:0", "writer:single_fish_50l_responsive_identity_donut")),
    ("P020b", "Identity-functional", ("writer:compound_50j_56i_unified",)),
    ("P021", "Identity-functional", ("nb:06:17:0",)),
    ("P022", "Identity-functional", ("writer:single_fish_hcr_anatomy_coexpression_summary",)),
    ("P023", "Laterality", ("nb:06:09:0",)),
    ("P024", "Laterality", ("nb:06:11:0",)),
    ("P025", "Laterality", ("nb:06:11:1",)),
)


CARD_TITLES = {
    "P001": "Session depth drift evidence", "P002": "Depth stability across time",
    "P003": "Registration labels and intensity correspondence", "P004": "In-plane label placement",
    "P005": "Geometry outcomes and physical offsets", "P006": "Cross-modality mask sizes",
    "P007": "HCR geometry and acceptance gates", "P008": "Saved HCR pair status on all functional planes",
    "P009": "Molecular-to-functional correspondences", "P010": "Accepted midline and ROI side assignments",
    "P011": "Stimulus timing and response evidence", "P012": "Stimulus-locked heatmaps",
    "P013": "Fixed any-stimulus responder traces", "P014": "Signed evoked responses across presented stimuli",
    "P015": "Per-stimulus response tests and selected tuning side", "P016": "Molecular identity counts",
    "P017": "Molecular label fate", "P018": "Population stimulus-response structure",
    "P019": "Marker stimulus traces and HCR-label status", "P020": "HCR fate and responsive ROI identity coverage",
    "P020b": "Population response and comparable laterality AUCs", "P021": "Identity-functional anatomy map",
    "P022": "Anatomy-label possible coexpression", "P023": "Global ipsilateral and contralateral AUC",
    "P024": "Left/right preference anatomy map", "P025": "Left/right preference distributions",
}

COMPOSITE_ROLES = {
    "P003": "renderer:render_registration_review_composite",
    "P005": "renderer:render_geometry_offsets_composite",
    "P010": "renderer:render_midline_review_composite",
    "P020": "renderer:render_identity_coverage_composite",
}


def select_curated_cards(figures):
    """Return ordered card sources and an explicit exclusion for every inventory item."""
    by_role = {}
    for figure in figures:
        for source in figure["sources"]:
            for role in source.get("roles", []):
                by_role.setdefault(role, []).append(figure)
    selected, used, missing = [], {}, []
    for card_id, gate, roles in CURATED_CARDS:
        panels = []
        for role in roles:
            matches = by_role.get(role, [])
            if matches:
                panels.append(next((f for f in matches if any(s.get("kind") == "curated_package_renderer" for s in f["sources"])), matches[0]))
        if len(panels) != len(roles):
            missing.append({"id": card_id, "gate": gate, "missing_roles": [r for r in roles if r not in by_role]})
            continue
        selected.append({"id": card_id, "title": CARD_TITLES[card_id], "gate": gate, "roles": list(roles), "panels": panels,
                         "decision": "approved curation"})
        for panel in panels:
            used.setdefault(panel["id"], []).append(card_id)
    excluded = {f["id"]: {"reason": "Not selected by approved source-role curation (duplicate, superseded, or provenance-only)",
                           "sha256": f["sha256"], "sources": f["sources"]}
                for f in figures if f["id"] not in used}
    return selected, excluded, missing


def _saved_notebook_tables(rendered_html):
    """Read the saved pandas HTML table cells without optional HTML libraries."""
    from html.parser import HTMLParser
    import pandas as pd

    class Tables(HTMLParser):
        def __init__(self):
            super().__init__()
            self.active = False
            self.rows, self.row, self.cell, self.tables = [], [], None, []

        def handle_starttag(self, tag, attrs):
            if tag == "table":
                self.active = True
                self.rows = []
            elif self.active and tag == "tr":
                self.row = []
            elif self.active and tag in {"td", "th"}:
                self.cell = []

        def handle_data(self, data):
            if self.active and self.cell is not None:
                self.cell.append(data)

        def handle_endtag(self, tag):
            if self.active and tag in {"td", "th"} and self.cell is not None:
                self.row.append("".join(self.cell).strip())
                self.cell = None
            elif self.active and tag == "tr" and self.row:
                self.rows.append(self.row)
            elif tag == "table" and self.active:
                if len(self.rows) > 1:
                    frame = pd.DataFrame(self.rows[1:], columns=[c or "index" for c in self.rows[0]])
                    for column in frame:
                        numbers = pd.to_numeric(frame[column], errors="coerce")
                        if numbers.notna().all():
                            frame[column] = numbers
                    self.tables.append(frame)
                self.active = False

    parser = Tables()
    parser.feed(rendered_html)
    return parser.tables


def _provenance_rows(notebooks, manifests, review_gate):
    """One presentation row per gate, using saved notebook tables and manifests."""
    import pandas as pd
    gates = ("Sessions / trials", "Drift state", "Geometry parity", "Bridge tracks",
             "Accepted midline", "Export scopes", "Release status")
    grouped = {gate: {"gate": gate, "value": "Not recorded in supplied evidence", "sources": [], "evidence": []} for gate in gates}
    for notebook in notebooks:
        nb = json.loads(notebook.read_text())
        for index, cell in enumerate(nb["cells"]):
            source = "".join(cell.get("source", []))
            number, gate = notebook.name[:2], None
            if number == "01" and source.startswith("# 08"):
                gate = "Drift state"
            elif number == "01" and source.startswith("# 10"):
                gate = "Sessions / trials"
            elif number == "03" and "summarize_stored_vs_physical_xy_offset_audit" in source:
                gate = "Geometry parity"
            elif number == "04" and source.startswith("# 07"):
                gate = "Bridge tracks"
            elif number == "06" and source.startswith("# 04"):
                gate = "Export scopes"
            if gate is None:
                continue
            row = grouped[gate]
            row["sources"].append({"path": str(notebook), "label": f"QC {number}, cell {index}"})
            session = None
            summaries = []
            for output in cell.get("outputs", []):
                data = output.get("data", {})
                if "image/png" in data:
                    continue
                plain = "".join(data.get("text/plain", [])) or "".join(output.get("text", []))
                if plain:
                    row["evidence"].append(plain)
                if match := re.search(r"(\S+): presented and valid trials", plain):
                    session = match.group(1)
                rendered_html = "".join(data.get("text/html", []))
                if "<table" not in rendered_html:
                    continue
                for frame in _saved_notebook_tables(rendered_html):
                    if gate == "Sessions / trials" and {"stimulus", "counts", "valid_counts"}.issubset(frame):
                        counts = pd.to_numeric(frame["counts"], errors="coerce")
                        valid = frame["valid_counts"].astype(str).unique().tolist()
                        valid_text = valid[0] + "/type" if len(valid) == 1 else ", ".join(f"{r.stimulus}={r.valid_counts}" for r in frame.itertuples())
                        summaries.append(f"{session or 'session unspecified'}: {int(counts.sum())} presented ({len(frame)} stimulus types); valid {valid_text}")
                    elif gate == "Drift state" and {"session", "evidence_tier"}.issubset(frame):
                        summaries.extend(f"{r.session}: {r.evidence_tier}" for r in frame.itertuples())
                    elif gate == "Geometry parity" and {"scope", "stored_median_xy_offset_value", "physical_median_xy_offset_um"}.issubset(frame):
                        overall = frame[frame["scope"].astype(str).eq("overall")]
                        for r in overall.itertuples():
                            summaries.append(f"n={r.n}; stored / physical medians {r.stored_median_xy_offset_value:.6g} / {r.physical_median_xy_offset_um:.6g} µm")
                    elif gate == "Bridge tracks" and {"track", "status"}.issubset(frame):
                        summaries.extend(f"{r.track}: {r.status} ({r.present_artifacts}/{r.configured_artifacts})" for r in frame.itertuples())
                    elif gate == "Export scopes" and {"artifact", "scope", "rows"}.issubset(frame):
                        summaries.extend(f"{r.scope}: {r.rows} rows" for r in frame.itertuples())
            if summaries:
                row["value"] = "; ".join(summaries)
    for manifest in manifests:
        payload = json.loads(manifest.read_text())
        gate = "Accepted midline" if payload.get("stage_name") == "materialize-anatomy-midline" else "Release status"
        row = grouped[gate]
        row["sources"].append({"path": str(manifest), "label": manifest.stem})
        row["evidence"].append({"sha256": sha256(manifest), **payload})
        if gate == "Accepted midline":
            for output in payload.get("outputs", []):
                if isinstance(output, dict) and Path(output.get("path", "")).name == "anatomy_midline.json":
                    path = Path(output["path"])
                    if path.is_file():
                        row["value"] = "Materialized accepted-midline sidecar SHA-256: " + sha256(path)
                        row["sources"].append({"path": str(path), "label": "accepted midline sidecar"})
    release = review_gate or {"status": "provisional", "reason": "Gallery export does not establish scientific acceptance"}
    grouped["Release status"]["value"] = f"{release['status']}: {release.get('reason', '')}"
    grouped["Release status"]["evidence"].insert(0, release)
    return list(grouped.values())


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _build_qc_figure_gallery(*, notebook_dir: Path, manifest_dir: Path, output_dir: Path, fish_dir: Path,
                            review_gate: dict | None = None, manifest_paths: tuple[Path, ...] | None = None,
                            notebook_count: int | None = None, response_trace_inspector: Path | None = None,
                            curated_artifacts: dict[str, Path] | None = None) -> dict:
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
                    "roles": ([f"nb:{notebook.name[:2]}:{match.group(1)}:{cell_png_count - 1}"]
                              if (match := re.match(r"# (\d\d)\b", source_text.splitlines()[0])) else [])
                             + [f"renderer:{name}" for name in renderer_calls],
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
                                                "stage_name": payload.get("stage_name"), "roles": [f"writer:{candidate.stem}"]},
                        f"writer · {candidate.stem}")
        manifest_reports.append({"path": str(manifest), "sha256": manifest_hash, "png_paths": pngs})

    for role, path in (curated_artifacts or {}).items():
        path = Path(path)
        add_png(path.read_bytes(), {"kind": "curated_package_renderer", "path": str(path),
                                   "roles": [role]}, f"curated · {role}")
    selected, exclusions, missing = select_curated_cards(figures)
    visible_cards = []
    for card in selected:
        role = COMPOSITE_ROLES.get(card["id"])
        if role:
            matches = [f for f in figures if any(role in source.get("roles", []) for source in f["sources"])]
            if not matches:
                missing.append({"id": card["id"], "gate": card["gate"], "missing_roles": [role]})
                continue
            composite = matches[0]
            card["composite_source"] = composite
            card["file"], card["sha256"] = composite["file"], composite["sha256"]
            exclusions.pop(composite["id"], None)
        else:
            card["file"] = card["panels"][0]["file"]
            card["sha256"] = card["panels"][0]["sha256"]
        visible_cards.append(card)
    selected = visible_cards
    included = {panel["id"] for card in selected for panel in card["panels"]}
    included.update(card["composite_source"]["id"] for card in selected if "composite_source" in card)
    exclusions = {f["id"]: {"reason": "Not selected by approved curation, or required composite unavailable",
                            "sha256": f["sha256"], "sources": f["sources"]}
                  for f in figures if f["id"] not in included}
    provenance = _provenance_rows(notebooks, [Path(r["path"]) for r in manifest_reports], review_gate)
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
              "curation_version": "2026-10-13", "cards": selected, "exclusion_map": exclusions,
              "missing_cards": missing, "provenance": provenance,
              "response_trace_inspector": str(response_trace_inspector) if response_trace_inspector else None,
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
    rendered = []
    previous_gate = None
    for card in selected:
        if card["gate"] != previous_gate:
            rendered.append(f"<h2>{html.escape(card['gate'])}</h2>")
            previous_gate = card["gate"]
        sources = html.escape(json.dumps({"roles": card["roles"], "panels": card["panels"]}, indent=2))
        rendered.append(f"<article id='{card['id']}'><h3>{card['id']} · {html.escape(card['title'])}</h3><p>{html.escape(card['decision'])}</p><a href='{card['file']}'><img loading='lazy' src='{card['file']}'></a><details><summary>Source provenance and SHA-256</summary><pre>{sources}</pre><p>{card['sha256']}</p></details></article>")
    provenance_html = ""
    for row in provenance:
        links = " · ".join(f"<a href='{html.escape(Path(source['path']).resolve().as_uri(), quote=True)}'>{html.escape(source['label'])}</a>" for source in row["sources"])
        evidence = html.escape(json.dumps(row["evidence"], indent=2))
        provenance_html += (f"<tr><th>{html.escape(row['gate'])}</th><td>{html.escape(row['value'])}"
                            f"<small class='sources'>{links}</small><details><summary>Saved evidence</summary><pre>{evidence}</pre></details></td></tr>")
    if response_trace_inspector is not None:
        response_trace_inspector = Path(response_trace_inspector).expanduser().resolve().as_uri()
    inspector_link = f"<p><a href='{html.escape(str(response_trace_inspector), quote=True)}'>Response trace inspector</a></p>" if response_trace_inspector else ""
    title = "midline-review QC figure gallery" if notebook_count == 6 else "accepted-midline review QC figure gallery"
    missing_html = html.escape(json.dumps(missing, indent=2))
    (output_dir / "index.html").write_text(
        "<!doctype html><meta charset='utf-8'><style>body{font:16px system-ui;max-width:1800px;margin:30px auto;padding:20px}article{margin:50px 0;border-top:1px solid #ccc}img{max-width:100%;height:auto}pre{white-space:pre-wrap;overflow-wrap:anywhere}td,th{vertical-align:top;border:1px solid #ddd;padding:8px;text-align:left}small.sources{display:block;font-size:11px;overflow-wrap:anywhere;margin-top:5px}table{width:100%;border-collapse:collapse}</style>"
        + f"<title>{html.escape(fish_dir.name)} QC</title><h1>{html.escape(fish_dir.name)}: {title}</h1>"
        + "<h2>Provenance and review gates</h2><p>Provisional release. Mechanical export is not scientific acceptance. Complete extraction inventory and exclusions are retained in gallery_manifest.json.</p>"
        + inspector_link + "<table><tr><th>Gate</th><th>Saved key values</th></tr>" + provenance_html + "</table>"
        + (f"<details open><summary>Missing card evidence</summary><pre>{missing_html}</pre></details>" if missing else "")
        + "".join(rendered))
    return {"output_dir": str(output_dir), "figure_count": len(figures), "notebook_png_outputs": report["notebook_png_output_count"], "writer_png_references": report["writer_png_reference_count"], "contact_sheets": len(pages), "blocked_messages": sum(len(r["blocked_or_unavailable_outputs"]) for r in notebook_reports)}


def export_qc_figure_gallery(*, notebook_dir: Path, manifest_dir: Path, output_dir: Path, fish_dir: Path,
                            review_gate: dict | None = None, manifest_paths: tuple[Path, ...] | None = None,
                            notebook_count: int | None = None, response_trace_inspector: Path | None = None,
                            curated_artifacts: dict[str, Path] | None = None) -> dict:
    """Publish a complete gallery only after coverage and image checks pass."""
    output_dir = output_dir.expanduser().resolve()
    if output_dir.exists():
        raise FileExistsError(f"Refusing existing gallery: {output_dir}")
    staged = output_dir.parent / f".{output_dir.name}.partial-{uuid.uuid4().hex}"
    try:
        report = _build_qc_figure_gallery(notebook_dir=notebook_dir, manifest_dir=manifest_dir,
                                         output_dir=staged, fish_dir=fish_dir, review_gate=review_gate, manifest_paths=manifest_paths,
                                         notebook_count=notebook_count, response_trace_inspector=response_trace_inspector,
                                         curated_artifacts=curated_artifacts)
        staged.rename(output_dir)
        return {**report, "output_dir": str(output_dir)}
    except Exception:
        if staged.exists():
            remove_tree(staged)
        raise


__all__ = ["export_qc_figure_gallery"]
