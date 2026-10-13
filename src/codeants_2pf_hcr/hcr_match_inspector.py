"""Self-contained, read-only HCR/anatomy/functional mask review artifact."""

from __future__ import annotations
from codeants_2pf_hcr.paths import visible_glob

import base64
import io
import json
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import SimpleITK as sitk
import tifffile
from PIL import Image
from scipy import ndimage as ndi


def _bool(value: object) -> bool:
    return str(value).lower() == "true"


def _number(value: object, digits: int = 2) -> float | None:
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    return round(value, digits) if np.isfinite(value) else None


def _outline(mask: np.ndarray) -> np.ndarray:
    return mask & ~ndi.binary_erosion(mask)


def _crop_bounds(box: tuple[slice, ...], partner: tuple[slice, ...] | None, shape: tuple[int, int]) -> tuple[int, int, int, int]:
    y0, y1 = box[1].start, box[1].stop
    x0, x1 = box[2].start, box[2].stop
    if partner is not None:
        y0, y1 = min(y0, partner[1].start), max(y1, partner[1].stop)
        x0, x1 = min(x0, partner[2].start), max(x1, partner[2].stop)
    side = max(72, y1 - y0 + 20, x1 - x0 + 20)
    side = min(side, *shape)
    cy, cx = (y0 + y1) // 2, (x0 + x1) // 2
    top = max(0, min(shape[0] - side, cy - side // 2))
    left = max(0, min(shape[1] - side, cx - side // 2))
    return top, top + side, left, left + side


def _panel(
    intensity: np.ndarray,
    anatomy: np.ndarray,
    hcr: np.ndarray,
    functional_slices: list[tuple[int, np.ndarray]],
    selected_func: dict[int, set[int]],
    anat_label: int | None,
    conf_label: int,
    z: int,
    bounds: tuple[int, int, int, int],
) -> Image.Image:
    y0, y1, x0, x1 = bounds
    gray = intensity[z, y0:y1, x0:x1].astype(np.float32)
    low, high = np.percentile(gray, [2, 99])
    scaled = np.clip((gray - low) / max(high - low, 1), 0, 1)
    base = Image.fromarray(np.uint8(22 + scaled * 125), "L").resize((200, 200), Image.Resampling.BILINEAR)
    rgb = np.repeat(np.asarray(base)[:, :, None], 3, axis=2).copy()
    labels = anatomy[z, y0:y1, x0:x1]
    hcr_labels = hcr[z, y0:y1, x0:x1]
    def resized(mask: np.ndarray) -> np.ndarray:
        return np.asarray(Image.fromarray(mask.astype(np.uint8) * 255).resize((200, 200), Image.Resampling.NEAREST)) > 0
    rgb[_outline(resized(labels > 0))] = (95, 105, 115)
    if anat_label is not None:
        rgb[_outline(resized(labels == anat_label))] = (83, 240, 122)
    rgb[_outline(resized(hcr_labels == conf_label))] = (255, 70, 195)
    for plane, functional in functional_slices:
        area = functional[y0:y1, x0:x1]
        rgb[_outline(resized(area > 0))] = (45, 135, 170)
        for func_label in selected_func.get(plane, set()):
            rgb[_outline(resized(area == func_label))] = (40, 225, 255)
    return Image.fromarray(rgb)


def _image_data(
    intensity: np.ndarray,
    anatomy: np.ndarray,
    hcr: np.ndarray,
    functional_by_z: dict[int, list[tuple[int, np.ndarray]]],
    linked_rois: list[dict],
    anat_label: int | None,
    conf_label: int,
    box: tuple[slice, ...],
    partner_box: tuple[slice, ...] | None,
    plane_z: dict[int, int],
    spacing_x_um: float,
) -> tuple[str, int, int, float, str]:
    bounds = _crop_bounds(box, partner_box, anatomy.shape[1:])
    region = hcr[box] == conf_label
    section_counts = region.sum(axis=(1, 2))
    peak_z = box[0].start + int(np.argmax(section_counts))
    linked_z = [
        plane_z[r["plane_idx"]] for r in linked_rois if r["plane_idx"] in plane_z
    ]
    if linked_z:
        second_z = min(linked_z, key=lambda z: abs(z - peak_z))
        second_view = "nearest anatomy-linked functional plane"
    elif partner_box is not None:
        second_z = max(partner_box[0].start, min(partner_box[0].stop - 1, peak_z))
        second_view = "anatomy-partner section"
    else:
        second_z = min(anatomy.shape[0] - 1, peak_z + 2)
        second_view = "adjacent section"
    if second_z == peak_z:
        other_hcr_sections = [box[0].start + i for i, count in enumerate(section_counts) if count > 0 and box[0].start + i != peak_z]
        if other_hcr_sections:
            second_z = max(other_hcr_sections, key=lambda z: (section_counts[z - box[0].start], abs(z - peak_z)))
            second_view = "alternate HCR section"
        else:
            second_z = peak_z - 1 if peak_z > 0 else peak_z + 1
            second_view = "adjacent section"
    tiles = []
    for z in (peak_z, second_z):
        selected: dict[int, set[int]] = defaultdict(set)
        for roi in linked_rois:
            if plane_z.get(roi["plane_idx"]) == z:
                selected[roi["plane_idx"]].add(roi["func_label"])
        tiles.append(_panel(intensity, anatomy, hcr, functional_by_z.get(z, []), selected, anat_label, conf_label, z, bounds))
    image = Image.new("RGB", (400, 200))
    image.paste(tiles[0], (0, 0))
    image.paste(tiles[1], (200, 0))
    stream = io.BytesIO()
    image.save(stream, format="WEBP", quality=80, method=3)
    return base64.b64encode(stream.getvalue()).decode("ascii"), peak_z, second_z, round((bounds[3] - bounds[2]) * spacing_x_um, 1), second_view


def render_hcr_match_inspector(
    *,
    fish_id: str,
    fish_dir: Path,
    pipeline_root: Path,
    output_path: Path,
) -> dict[str, int]:
    """Render saved geometry as a self-contained HTML review surface; change no source tables."""
    if not re.fullmatch(r"[A-Z][0-9]{3}_f[0-9]{2}", fish_id):
        raise ValueError(f"Invalid fish ID: {fish_id}")
    aligned = pipeline_root / "register-hcr-to-anatomy/confocal/aligned"
    metrics = pd.read_csv(aligned / "hcr_label_metrics.csv")
    if set(metrics.fish_id) != {fish_id}:
        raise ValueError("HCR metrics fish ID does not match the requested fish")
    anatomy_path = fish_dir / "03_analysis/structural/cp_masks" / f"{fish_id}_anatomy_00001_uint8_8bit_cp_masks.tif"
    anatomy = tifffile.imread(anatomy_path)
    intensity_path = pipeline_root / "prepare-in-vivo-anatomy-stack/2p_anatomy" / f"{fish_id}_anatomy_2P_GCaMP.nrrd"
    intensity_image = sitk.ReadImage(str(intensity_path))
    intensity = sitk.GetArrayFromImage(intensity_image)
    spacing_x = float(intensity_image.GetSpacing()[0])
    sidecar = json.loads((pipeline_root / "match-roi-to-anatomy/registration/plane_refs_summary_geometry.json").read_text())
    modes = {row["anat_label_z_mode"] for row in sidecar}
    if len(modes) != 1 or modes.pop() not in {"direct", "reverse"}:
        raise ValueError("Inconsistent anatomy-label Z orientation")
    if sidecar[0]["anat_label_z_mode"] == "reverse":
        intensity = intensity[::-1]
    if anatomy.shape != intensity.shape:
        raise ValueError("Anatomy label and intensity grids differ")
    plane_z = {int(row["index"]): int(row["anat_label_z"]) for row in sidecar}
    functional_by_z: dict[int, list[tuple[int, np.ndarray]]] = defaultdict(list)
    func_dir = pipeline_root / "transform-functional-rois-to-anatomy/functional/anatomy"
    for plane, z in plane_z.items():
        paths = sorted(visible_glob(func_dir, f"{fish_id}_plane{plane}_*_func_mask_in_2p.tif"))
        if len(paths) != 1:
            raise ValueError(f"Expected one transformed functional mask for plane {plane}")
        functional = tifffile.imread(paths[0])
        if functional.shape != anatomy.shape[1:]:
            raise ValueError(f"Functional plane {plane} grid differs")
        functional_by_z[z].append((plane, functional))
    roi_table = pd.read_csv(pipeline_root / "assign-hcr-identity/registration/functional_roi_activity_identity.csv")
    if set(roi_table.fish_id) != {fish_id}:
        raise ValueError("ROI table fish ID does not match")
    roi_by_anat: dict[int, list[dict]] = defaultdict(list)
    for row in roi_table.itertuples(index=False):
        if pd.isna(row.selected_anat_label):
            continue
        roi_by_anat[int(row.selected_anat_label)].append({
            "plane_idx": int(row.plane_idx),
            "func_label": int(row.func_label),
            "identity": None if pd.isna(row.identity_label) else str(row.identity_label),
            "response": None if pd.isna(row.response_class) else str(row.response_class),
            "bpi": _number(row.bpi),
            "assigned": _bool(row.has_identity_assigned),
        })
    anat_boxes = ndi.find_objects(anatomy)
    rows = []
    mask_paths = sorted(visible_glob(aligned, "*_cp_masks_in_2p_labels_uint16.tif"))
    for mask_path in mask_paths:
        uid = mask_path.name.removesuffix("_cp_masks_in_2p_labels_uint16.tif")
        mask_metrics = metrics.loc[metrics.mask_uid.eq(uid)]
        if mask_metrics.empty:
            raise ValueError(f"No metrics for {uid}")
        hcr = tifffile.imread(mask_path)
        if hcr.shape != anatomy.shape:
            raise ValueError(f"HCR mask grid differs: {mask_path}")
        hcr_boxes = ndi.find_objects(hcr)
        matches = pd.read_csv(mask_path.with_name(uid + "_cp_masks_in_2p_matches.csv"))
        finals = pd.read_csv(mask_path.with_name(uid + "_cp_masks_in_2p_final_pairs.csv"))
        final_keys = {(int(r.conf_label), int(r.twoP_label)) for r in finals.itertuples(index=False)}
        candidates_by_label = {int(k): group for k, group in matches.groupby("conf_label")}
        gene_token = uid.split("_channel")[-1].split("_", 1)[-1]
        gene = {"sst1_1": "sst1.1", "sst1_2": "sst1.2"}.get(gene_token, gene_token)
        if gene not in {"sst1.1", "sst1.2", "cort", "pth2", "gad2"}:
            raise ValueError(f"Unknown HCR marker in {uid}")
        marker_excluded = gene == "gad2"
        for item in mask_metrics.itertuples(index=False):
            label = int(item.conf_label)
            box = hcr_boxes[label - 1] if label <= len(hcr_boxes) else None
            candidates = []
            for match in candidates_by_label.get(label, pd.DataFrame()).itertuples(index=False):
                anat_id = int(match.twoP_label)
                candidates.append({
                    "anat": anat_id,
                    "accepted": (label, anat_id) in final_keys,
                    "quality": str(match.quality),
                    "pair_type": str(match.pair_type),
                    "xy_um": _number(match.centroid_xy_distance_um),
                    "distance_um": _number(match.distance_um),
                    "iou": _number(match.iou, 3),
                    "overlap_voxels": int(match.overlap_voxels),
                    "passes_radius": _bool(match.passes_anatomy_radius_gate),
                    "passes_quality": _bool(match.passes_label_quality_gate),
                    "reason": None if pd.isna(match.analysis_exclusion_reason) else str(match.analysis_exclusion_reason),
                })
            candidates.sort(key=lambda c: (not c["accepted"], -c["overlap_voxels"], c["distance_um"] or 0))
            primary = candidates[0] if candidates else None
            anat_id = primary["anat"] if primary else None
            partner_box = anat_boxes[anat_id - 1] if anat_id and anat_id <= len(anat_boxes) else None
            linked_rois = roi_by_anat.get(anat_id, []) if anat_id else []
            if box is not None:
                # Source images are label-grid aligned; intensity Z was explicitly
                # reversed above when the geometry sidecar requires it.
                image, z_peak, z_second, fov_um, second_view = _image_data(
                    intensity, anatomy, hcr, functional_by_z, linked_rois, anat_id,
                    label, box, partner_box, plane_z, spacing_x,
                )
            else:
                image, z_peak, z_second, fov_um, second_view = None, None, None, None, None
            rows.append({
                "key": uid + ":" + str(label),
                "mask": uid,
                "gene": gene,
                "label": label,
                "marker_excluded": marker_excluded,
                "size": "small" if _bool(item.is_small_speck) else "large" if _bool(item.is_low_confidence_segmentation) else "eligible",
                "eligible": _bool(item.analysis_eligible),
                "voxels": int(item.measurement_n_voxels),
                "volume_um3": _number(item.measurement_volume_um3, 1),
                "diameter_um": _number(item.measurement_equivalent_diameter_um),
                "q95_diameter_um": _number(item.large_mask_q95_equivalent_diameter_um),
                "measurement_space": str(item.measurement_space),
                "reason": None if pd.isna(item.analysis_exclusion_reason) else str(item.analysis_exclusion_reason),
                "candidates": candidates,
                "rois": linked_rois,
                "z_peak": z_peak,
                "z_second": z_second,
                "second_view": second_view,
                "fov_um": fov_um,
                "image": image,
            })
    if len(rows) != len(metrics):
        raise ValueError("Rendered label count differs from metrics table")
    rows.sort(key=lambda r: ({"large": 0, "eligible": 1, "small": 2}[r["size"]], r["gene"], r["label"]))
    radius = _number(pd.read_csv(mask_paths[0].with_name(mask_paths[0].name.replace("_labels_uint16.tif", "_matches.csv"))).median_anatomy_xy_radius_um.iloc[0], 3)
    summary = {
        "labels": len(rows),
        "large": sum(r["size"] == "large" for r in rows),
        "small": sum(r["size"] == "small" for r in rows),
        "eligible": sum(r["size"] == "eligible" for r in rows),
        "accepted": sum(any(c["accepted"] for c in r["candidates"]) for r in rows),
        "candidate": sum(bool(r["candidates"]) for r in rows),
        "with_functional": sum(bool(r["rois"]) for r in rows),
    }
    small_refs = set(metrics.min_component_voxels_reference)
    large_quantiles = set(metrics.large_mask_quantile)
    if len(small_refs) != 1 or len(large_quantiles) != 1:
        raise ValueError("HCR label-quality policy differs across saved masks")
    page = (_HTML.replace("__FISH__", fish_id)
            .replace("__ROOT__", str(pipeline_root))
            .replace("__GENERATED__", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
            .replace("__RADIUS__", str(radius))
            .replace("__SMALL_REF__", str(int(next(iter(small_refs)))))
            .replace("__LARGE_PERCENTILE__", str(round(float(next(iter(large_quantiles))) * 100))))
    payload = json.dumps({"rows": rows, "summary": summary}, separators=(",", ":"), allow_nan=False).replace("</", "<\\/")
    page = page.replace("__DATA__", payload)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(page, encoding="utf-8")
    return summary


_HTML = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>__FISH__ · HCR–anatomy match inspector</title>
<style>
:root{color-scheme:dark;font:14px/1.45 system-ui,sans-serif;background:#101923;color:#e5edf4}
*{box-sizing:border-box}body{margin:0}header{padding:18px 24px;background:#172431;border-bottom:1px solid #34485a}
h1{font-size:1.45rem;margin:0 0 5px}p{margin:5px 0;color:#b4c6d5}
main{display:grid;grid-template-columns:minmax(350px,38%) 1fr;height:calc(100vh - 155px);min-height:520px}
aside{border-right:1px solid #34485a;display:flex;flex-direction:column;min-height:0}
.controls{padding:12px;display:grid;grid-template-columns:1fr 1fr;gap:8px;background:#16222d}
input,select,button{font:inherit;border:1px solid #476073;border-radius:6px;background:#213544;color:#f0f5f9;padding:7px}
input{grid-column:1/-1}button{cursor:pointer}button:hover,.row:hover{background:#304b5c}
#count{padding:7px 12px;color:#abc0ce}#list{overflow:auto;flex:1}
.row{display:grid;grid-template-columns:1fr auto;gap:3px 10px;padding:9px 12px;border-bottom:1px solid #2c3c48;cursor:pointer}
.row.active{background:#284258;border-left:4px solid #56d4e4;padding-left:8px}.muted{color:#9db3c1}.tiny{font-size:11px}
.badge{display:inline-block;border-radius:999px;padding:1px 7px;font-size:11px}
.large{background:#7a3941;color:#fff}.small{background:#555267;color:#fff}.eligible{background:#275847;color:#d9ffe8}
#pages{display:flex;justify-content:space-between;align-items:center;padding:8px;border-top:1px solid #34485a}
article{overflow:auto;padding:18px 22px}article h2{margin:0 0 8px;font-size:1.3rem}
.stats{display:flex;flex-wrap:wrap;gap:8px}.stat{background:#1d303e;border:1px solid #395263;border-radius:8px;padding:7px 10px}
.warning{background:#4b3824;border-left:4px solid #f3b85a;padding:10px;margin:12px 0}
.overlay{width:min(100%,960px);height:auto;image-rendering:pixelated;border:1px solid #456070;background:#0b1118}
.legend{display:flex;flex-wrap:wrap;gap:15px;margin:8px 0 14px}.legend span:before{content:"●";padding-right:5px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:8px;margin:12px 0}
.card{background:#192a37;border:1px solid #334e60;border-radius:8px;padding:10px}
table{border-collapse:collapse;width:100%}th,td{text-align:left;border-bottom:1px solid #334552;padding:5px 7px}
footer{padding:9px 20px;color:#9fb3c2;font-size:11px;background:#172431;border-top:1px solid #34485a;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
@media(max-width:850px){main{display:block;height:auto}aside{height:45vh;border-right:0;border-bottom:1px solid #34485a}article{min-height:55vh}}
</style></head><body>
<header><h1>__FISH__ · HCR–anatomy match inspector</h1>
<p>Every transformed HCR label, including small speckles, large low-confidence masks, and unmatched labels. Read-only evidence; no gate decisions are saved here.</p>
<p>Small means below the physical-volume equivalent of __SMALL_REF__ native HCR voxels. Large means above the within-mask __LARGE_PERCENTILE__th percentile of non-small equivalent-volume diameters. Both are excluded from accepted identity; large masks remain visible for review.</p>
<div id="summary" class="stats"></div></header>
<main><aside><div class="controls">
<input id="search" type="search" placeholder="Search marker, mask, HCR ID, anatomy ID, or ROI">
<select id="gene"><option value="">All markers</option></select>
<select id="size"><option value="">All sizes</option><option value="large">Large / low confidence</option><option value="small">Small speckles</option><option value="eligible">Size eligible</option></select>
<select id="match"><option value="">All match states</option><option value="accepted">Accepted pair</option><option value="rejected">Candidate but not accepted</option><option value="none">No candidate</option></select>
<select id="functional"><option value="">Any functional link</option><option value="yes">Has anatomy-linked ROI</option><option value="no">No anatomy-linked ROI</option></select>
</div><div id="count"></div><div id="list"></div><div id="pages"><button id="prevPage">Previous</button><span id="pageLabel"></span><button id="nextPage">Next</button></div></aside>
<article id="detail"></article></main>
<footer>Generated __GENERATED__ · Source: __ROOT__ · Matching uses a fish-specific median anatomical maximal-section XY radius of __RADIUS__ µm. Anatomy grayscale is GCaMP, not HCR fluorescence. HCR signal is represented by label contours.</footer>
<script id="payload" type="application/json">__DATA__</script>
<script>
const data=JSON.parse(document.getElementById("payload").textContent), rows=data.rows;
let filtered=rows,page=0,selected=null;
const $=id=>document.getElementById(id),esc=s=>String(s??"—").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const fmt=n=>n==null?"—":Number(n).toLocaleString();
const state=r=>r.candidates.some(c=>c.accepted)?"accepted":r.candidates.length?"rejected":"none";
$("summary").innerHTML=Object.entries(data.summary).map(([k,v])=>'<div class="stat">'+esc(k.replaceAll("_"," "))+' <strong>'+fmt(v)+'</strong></div>').join("");
[...new Set(rows.map(r=>r.gene))].sort().forEach(g=>{$("gene").add(new Option(g,g))});
function apply(){
 const q=$("search").value.trim().toLowerCase(),gene=$("gene").value,size=$("size").value,match=$("match").value,func=$("functional").value;
 filtered=rows.filter(r=>(!gene||r.gene===gene)&&(!size||r.size===size)&&(!match||state(r)===match)&&(!func||(func==="yes")===Boolean(r.rois.length))&&(!q||[r.key,r.gene,r.label,...r.candidates.map(c=>c.anat),...r.rois.map(f=>f.plane_idx+":"+f.func_label)].join(" ").toLowerCase().includes(q)));
 page=0;if(!filtered.some(r=>r.key===selected))selected=filtered[0]?.key??null;render();
}
function render(){
 const start=page*50, slice=filtered.slice(start,start+50);
 $("count").textContent=fmt(filtered.length)+" labels match filters";
 $("list").innerHTML=slice.map(r=>'<div class="row '+(r.key===selected?"active":"")+'" data-key="'+esc(r.key)+'"><div><strong>'+esc(r.gene)+' · HCR '+r.label+'</strong><br><span class="tiny muted">'+esc(r.mask)+'</span></div><div><span class="badge '+r.size+'">'+esc(r.size)+'</span><br><span class="tiny">'+esc(state(r))+'</span></div></div>').join("");
 $("list").querySelectorAll(".row").forEach(el=>el.onclick=()=>{selected=el.dataset.key;render()});
 $("pageLabel").textContent=(filtered.length?start+1:0)+"–"+Math.min(start+50,filtered.length)+" / "+fmt(filtered.length);
 $("prevPage").disabled=page===0;$("nextPage").disabled=start+50>=filtered.length;
 detail();
}
function detail(){
 const r=rows.find(v=>v.key===selected);if(!r){$("detail").innerHTML="<p>No labels match the current filters.</p>";return}
 const top=r.candidates[0],status=state(r);
 $("detail").innerHTML='<h2>'+esc(r.gene)+' · HCR label '+r.label+'</h2><p>'+esc(r.mask)+'</p>'+
 (r.marker_excluded?'<div class="warning">This marker (gad2) is outside the currently retained L765_f04 molecular-identity review scope. The saved mask is shown for completeness, not biological acceptance.</div>':'')+
 '<div class="stats"><div class="stat"><span class="badge '+r.size+'">'+esc(r.size)+'</span> '+(r.eligible?"size eligible":"size excluded")+'</div><div class="stat">Match: <strong>'+status+'</strong></div><div class="stat">Anatomy: <strong>'+esc(top?.anat)+'</strong></div><div class="stat">Anatomy-linked ROIs: <strong>'+r.rois.length+'</strong></div></div>'+
 '<div class="grid"><div class="card">Size: <strong>'+fmt(r.voxels)+'</strong> measured voxels · '+fmt(r.volume_um3)+' µm³ · equivalent diameter '+fmt(r.diameter_um)+' µm<br>Per-mask large threshold: '+fmt(r.q95_diameter_um)+' µm · measurement space: '+esc(r.measurement_space)+'</div><div class="card">Segmentation exclusion: '+esc(r.reason??"none")+'<br>Matching exclusion: '+esc(top?.reason??"none")+'<br>XY centroid offset: '+fmt(top?.xy_um)+' µm / __RADIUS__ µm radius</div></div>'+
 (r.image?'<img class="overlay" alt="Two anatomy-grid slices with HCR, anatomy, and functional contours" src="data:image/webp;base64,'+r.image+'"><p class="tiny">Left: HCR maximal-section label Z '+r.z_peak+'. Right: '+esc(r.second_view)+' label Z '+r.z_second+'. Same XY field in both; field width '+fmt(r.fov_um)+' µm. Z is anatomy-label-grid index. Outlines are nearest-neighbour masks.</p>':'<div class="warning">This label has no surviving transformed pixels, so no overlay can be rendered.</div>')+
 '<div class="legend"><span style="color:#ff46c3">HCR label</span><span style="color:#53f07a">selected anatomy label</span><span style="color:#28e1ff">anatomy-linked functional ROI</span><span style="color:#2d87aa">other functional ROI</span><span style="color:#5f6973">other anatomy labels</span></div>'+
 '<h3>Anatomical candidate</h3><table><tr><th>Anatomy</th><th>Pair</th><th>Quality</th><th>XY µm</th><th>3D µm</th><th>Overlap</th><th>IoU</th><th>Gates / reason</th></tr>'+
 (r.candidates.length?r.candidates.map(c=>'<tr><td>'+c.anat+'</td><td>'+(c.accepted?"accepted":"not accepted")+'</td><td>'+esc(c.quality)+' / '+esc(c.pair_type)+'</td><td>'+fmt(c.xy_um)+'</td><td>'+fmt(c.distance_um)+'</td><td>'+fmt(c.overlap_voxels)+'</td><td>'+fmt(c.iou)+'</td><td>'+(c.passes_quality?"size ✓":"size ✕")+' · '+(c.passes_radius?"XY ✓":"XY ✕")+' · '+esc(c.reason??"—")+'</td></tr>').join(""):'<tr><td colspan="8">No saved candidate pair.</td></tr>')+'</table>'+
 '<h3>Functional labels linked through this anatomy cell</h3><table><tr><th>Plane</th><th>ROI label</th><th>Identity attached</th><th>Response class</th><th>BPI</th></tr>'+
 (r.rois.length?r.rois.map(f=>'<tr><td>'+f.plane_idx+'</td><td>'+f.func_label+'</td><td>'+esc(f.identity??"none")+(f.assigned?" ✓":"")+'</td><td>'+esc(f.response)+'</td><td>'+fmt(f.bpi)+'</td></tr>').join(""):'<tr><td colspan="5">No functional ROI is linked to the selected anatomy candidate in the identity-writer table.</td></tr>')+'</table>'+
 '<p class="warning">Functional rows are contextual anatomy links, not independent HCR identities. A rejected HCR pair does not become accepted because an ROI appears here. Response and BPI do not affect geometry.</p>';
}
["search","gene","size","match","functional"].forEach(id=>$(id).addEventListener("input",apply));
$("prevPage").onclick=()=>{if(page>0){page--;render()}};
$("nextPage").onclick=()=>{if((page+1)*50<filtered.length){page++;render()}};
apply();
</script></body></html>"""
