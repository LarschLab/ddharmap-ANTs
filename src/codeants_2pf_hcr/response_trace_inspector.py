"""Read-only all-neuron raw-trace inspector for score-activity-bpi outputs.

Generalises the one-off L765_f04 review tool of 2026-09-17
(``qc/all_neuron_trace_inspector_2026-09-17/render_all_neuron_trace_inspector.py``)
to any fish. Trials come from the persisted ``scored_stimulus_windows.csv``
rows of each plane, so the inspector shows exactly the windows that were
scored; it never recomputes response calls or records gate decisions.
"""

from __future__ import annotations

import base64
import html
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

STIM_TYPES = ("LB_trajectory", "RB_trajectory", "LC_trajectory", "RC_trajectory")
TRIALS_PER_STIM = 4
CLASS_ORDER = ("bout-responsive", "continuous-responsive", "both-responsive", "low activity", "response unavailable")
CLASS_COLORS = {
    "bout-responsive": "#2c7fb8",
    "continuous-responsive": "#d95f0e",
    "both-responsive": "#d946ef",
    "low activity": "#9e9e9e",
    "response unavailable": "#ececec",
}
PREF_ORDER = ("bout-preferential", "continuous-preferential", "non-preferential", "not-responsive", "unavailable")
REVIEW_GROUPS = ("response-positive", "floor-excluded", "other")
AUC_REPRODUCTION_TOLERANCE = 1e-5
_SCALE_EPS = 1e-6


def floor_excluded_mask(cells: pd.DataFrame) -> pd.Series:
    """Rows that pass a persisted null threshold in some condition but are not
    response-positive because that condition's |mean AUC| is below the floor."""
    floor = pd.to_numeric(cells["response_auc_threshold"], errors="coerce")
    hit = pd.Series(False, index=cells.index)
    for cond in ("bout", "cont"):
        auc = pd.to_numeric(cells[f"mean_{cond}_auc_dff"], errors="coerce")
        upper = pd.to_numeric(cells[f"{cond}_null_upper_auc"], errors="coerce")
        lower = pd.to_numeric(cells[f"{cond}_null_lower_auc"], errors="coerce")
        pos = (auc >= upper) & (auc < floor)
        neg = (auc <= lower) & (auc > -floor)
        hit |= (pos | neg).fillna(False)
    active = cells["response_is_active"].astype("boolean").fillna(False).astype(bool)
    available = cells["response_class"].ne("response unavailable")
    return hit & ~active & available


def _plane_trials(windows: pd.DataFrame, plane_idx: int) -> pd.DataFrame:
    plane = windows[(windows["plane_idx"] == plane_idx) & windows["used_for_bpi"].astype(bool)]
    plane = plane[plane["stim_type"].isin(STIM_TYPES)].copy()
    plane["_stim_order"] = plane["stim_type"].map({s: i for i, s in enumerate(STIM_TYPES)})
    plane = plane.sort_values(["_stim_order", "prestim_baseline_start_frame"]).reset_index(drop=True)
    counts = plane["stim_type"].value_counts().reindex(list(STIM_TYPES), fill_value=0)
    if not counts.eq(TRIALS_PER_STIM).all():
        raise RuntimeError(f"plane {plane_idx}: expected {TRIALS_PER_STIM} BPI trials per stimulus, found {counts.to_dict()}")
    if plane["session_label"].nunique() != 1 or plane["fps"].nunique() != 1:
        raise RuntimeError(f"plane {plane_idx}: windows must share one session and frame rate")
    return plane


def _load_plane_f(suite2p_dir: Path, plane_idx: int) -> tuple[np.ndarray, Path]:
    matches = sorted((suite2p_dir / f"plane{plane_idx}").glob("*_F.npy"))
    if len(matches) != 1:
        raise RuntimeError(f"expected one *_F.npy for plane {plane_idx}, found {matches}")
    return np.load(matches[0], mmap_mode="r"), matches[0]


def build_trace_cube(
    cells: pd.DataFrame, windows: pd.DataFrame, suite2p_dir: Path
) -> tuple[pd.DataFrame, np.ndarray, dict[str, Any]]:
    """Return sorted rows, a (rows, 16, n_pts) baseline-shifted dF/F cube and provenance."""
    rows = cells.copy()
    trials = {int(p): _plane_trials(windows, int(p)) for p in sorted(rows["plane_idx"].astype(int).unique())}
    rows["session_label"] = rows["plane_idx"].astype(int).map({p: str(t["session_label"].iloc[0]) for p, t in trials.items()})
    lengths = {int(t.scored_offset_frame - t.prestim_baseline_start_frame) for tr in trials.values() for t in tr.itertuples()}
    if len(lengths) != 1:
        raise RuntimeError(f"trial segments differ in length: {sorted(lengths)}")
    n_pts = lengths.pop()
    first = next(iter(trials.values())).iloc[0]
    fps = float(first["fps"])
    markers = {
        "appearance_frac": float(first["recorded_onset_frame"] - first["prestim_baseline_start_frame"]) / (n_pts - 1),
        "movement_frac": float(first["scored_onset_frame"] - first["prestim_baseline_start_frame"]) / (n_pts - 1),
        "window_sec": [
            -float(first["recorded_onset_frame"] - first["prestim_baseline_start_frame"]) / fps,
            float(first["scored_offset_frame"] - first["recorded_onset_frame"]) / fps,
        ],
        "movement_onset_sec": float(first["scored_onset_frame"] - first["recorded_onset_frame"]) / fps,
    }

    rows["review_group"] = np.where(
        rows["response_is_active"].astype("boolean").fillna(False).astype(bool),
        "response-positive",
        np.where(floor_excluded_mask(rows), "floor-excluded", "other"),
    )
    rows["_class_order"] = rows["response_class"].map({c: i for i, c in enumerate(CLASS_ORDER)}).fillna(len(CLASS_ORDER))
    rows["bpi_preference"] = rows["bpi_preference"].fillna("unavailable")
    rows["bpi_group"] = np.select(
        [rows["bpi"].eq(1.0), rows["bpi"].eq(-1.0), rows["bpi"].notna()],
        ["exact +1", "exact -1", "finite interior"],
        default="unavailable",
    )
    rows = rows.sort_values(["_class_order", "session_label", "plane_idx", "func_label"]).reset_index(drop=True)

    cube = np.full((len(rows), len(STIM_TYPES) * TRIALS_PER_STIM, n_pts), np.nan, dtype=np.float32)
    diffs: list[float] = []
    sources: dict[str, str] = {}
    planes: dict[int, np.ndarray] = {}
    for i, row in enumerate(rows.itertuples(index=False)):
        p = int(row.plane_idx)
        if p not in planes:
            planes[p], src = _load_plane_f(suite2p_dir, p)
            sources[str(p)] = str(src)
        f = np.asarray(planes[p][int(row.func_label) - 1], dtype=np.float32)
        f0 = float(np.percentile(f, 10.0))
        dff = (f - f0) / (f0 + 1e-6)
        aucs = []
        for col, t in enumerate(trials[p].itertuples()):
            b0, b1 = int(t.prestim_baseline_start_frame), int(t.prestim_baseline_end_frame)
            r0, r1 = int(t.scored_onset_frame), int(t.scored_offset_frame)
            base = float(np.nanmean(dff[b0:b1]))
            cube[i, col] = dff[b0:r1] - base
            aucs.append(float(np.nansum(dff[r0:r1] - base) / float(t.fps)))
        half = len(aucs) // 2
        if pd.notna(row.mean_bout_auc_dff) and pd.notna(row.mean_cont_auc_dff):
            diffs.append(max(abs(np.mean(aucs[:half]) - row.mean_bout_auc_dff), abs(np.mean(aucs[half:]) - row.mean_cont_auc_dff)))
    max_diff = float(max(diffs)) if diffs else float("nan")
    if not diffs or max_diff > AUC_REPRODUCTION_TOLERANCE:
        raise RuntimeError(f"trial AUC reproduction failed: max difference {max_diff}")
    provenance = {"n_points": n_pts, "fps": fps, **markers, "max_recomputed_mean_auc_difference": max_diff, "trace_sources": sources}
    return rows, cube, provenance


def _scales(cube: np.ndarray) -> np.ndarray:
    return np.maximum(np.nanmax(np.abs(cube), axis=(1, 2)), _SCALE_EPS).astype(np.float32)


def _encode(cube: np.ndarray, scales: np.ndarray) -> str:
    q = np.rint(np.clip(np.nan_to_num(cube / scales[:, None, None]), -1.0, 1.0) * 32767.0).astype("<i2")
    return base64.b64encode(q.tobytes(order="C")).decode("ascii")


def _row_html(rows: pd.DataFrame, scales: np.ndarray, view: str) -> str:
    parts = []
    for idx, row in enumerate(rows.itertuples(index=False)):
        cls = str(row.response_class)
        pref = str(row.bpi_preference)
        bpi_text = "n/a" if pd.isna(row.bpi) else f"{float(row.bpi):+.3f}"
        valence = str(row.response_valence_pattern) if pd.notna(getattr(row, "response_valence_pattern", np.nan)) else "n/a"
        group = str(row.review_group)
        search = f"{cls} {pref} {row.bpi_group} {group} {valence} {row.session_label} plane {int(row.plane_idx)} roi {int(row.func_label)}".lower()
        flag = " · <em>floor-excluded</em>" if group == "floor-excluded" else ""
        label = (
            f"<strong>{html.escape(cls)}</strong>{flag}<br>{html.escape(pref)} · BPI {bpi_text}<br>"
            f"valence {html.escape(valence)}<br>"
            f"{html.escape(str(row.session_label))} · plane {int(row.plane_idx)} · ROI {int(row.func_label)}<br>"
            f"AUC b {float(row.mean_bout_auc_dff):+.4f} / c {float(row.mean_cont_auc_dff):+.4f} · scale ±{float(scales[idx]):.4f}"
        )
        parts.append(
            f'<div class="trace-row" data-view="{view}" data-search="{html.escape(search)}" data-class="{html.escape(cls)}" '
            f'data-pref="{html.escape(pref)}" data-bpi-group="{html.escape(str(row.bpi_group))}" data-group="{group}">'
            f'<div class="row-label class-{cls.replace(" ", "-")}">{label}</div>'
            f'<svg class="trace-svg" data-index="{idx}" preserveAspectRatio="none" role="img" aria-label="{html.escape(search)}"></svg></div>'
        )
    return "\n".join(parts)


def _document(fish_id: str, rows: pd.DataFrame, cube: np.ndarray, mean_cube: np.ndarray, prov: dict[str, Any], floor: float) -> str:
    scales, mean_scales = _scales(cube), _scales(mean_cube)
    n_cols, n_pts = cube.shape[1], cube.shape[2]
    trial_labels = [f"{s.replace('_trajectory', '')}{k}" for s in STIM_TYPES for k in range(1, TRIALS_PER_STIM + 1)]
    mean_labels = [s.replace("_trajectory", "") for s in STIM_TYPES]
    cls_counts = rows["response_class"].value_counts().reindex(list(CLASS_ORDER), fill_value=0).astype(int)
    pref_counts = rows["bpi_preference"].value_counts().reindex(list(PREF_ORDER), fill_value=0).astype(int)
    grp_counts = rows["review_group"].value_counts().reindex(list(REVIEW_GROUPS), fill_value=0).astype(int)
    opt = lambda items, counts: "".join(f'<option value="{html.escape(k)}">{html.escape(k)} ({counts[k]})</option>' for k in items)
    legend = "".join(f'<span><i class="swatch" style="background:{CLASS_COLORS[c]}"></i>{c} <b>{cls_counts[c]}</b></span>' for c in CLASS_ORDER)
    head = lambda labels, half: "".join(f'<span class="{"bout-head" if i < half else "cont-head"}">{l}</span>' for i, l in enumerate(labels))
    w0, w1 = prov["window_sec"]
    app, mov = prov["appearance_frac"], prov["movement_frac"]
    return f'''<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{fish_id} response trace inspector</title>
<style>
:root{{--label:300px;--noncol:190px;--meancol:360px;--bout:#e5f1f7;--cont:#fff0e3}}
*{{box-sizing:border-box}} body{{margin:0;font:14px/1.35 system-ui,-apple-system,sans-serif;color:#202124;background:#fff}}
header{{background:#fff;border-bottom:1px solid #ccc;padding:12px 18px}} h1{{font-size:20px;margin:0 0 5px}} .note{{max-width:1200px;color:#444}}
.controls{{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-top:9px}} button,select,input{{font:inherit;padding:6px 9px}}
button.active{{background:#1f5f86;color:#fff;border-color:#1f5f86}} .summary{{display:flex;gap:16px;flex-wrap:wrap;margin:8px 0 0}}
.view{{display:none;padding:8px 14px 30px}} .view.active{{display:block}} .scroll{{overflow:auto;border:1px solid #ccc}}
.column-header{{display:grid;background:#fff;border-bottom:2px solid #666;min-width:max-content}}
.column-header.nonavg,.trace-row[data-view="nonavg"]{{grid-template-columns:var(--label) repeat({n_cols},var(--noncol))}}
.column-header.mean,.trace-row[data-view="mean"]{{grid-template-columns:var(--label) repeat({len(mean_labels)},var(--meancol))}}
.column-header span{{text-align:center;font-weight:700;padding:7px 3px;border-left:1px solid #bbb}} .column-header .label-head{{text-align:left;border-left:0}}
.bout-head{{background:var(--bout)}} .cont-head{{background:var(--cont)}} .trace-row{{display:grid;min-width:max-content;border-bottom:1px solid #ddd}}
.trace-row[data-view="nonavg"]{{height:126px}} .trace-row[data-view="mean"]{{height:150px}}
.row-label{{padding:7px 10px;border-right:1px solid #aaa;border-left:12px solid #ccc;font-size:12px;font-variant-numeric:tabular-nums}}
.class-bout-responsive{{border-left-color:#2c7fb8}} .class-continuous-responsive{{border-left-color:#d95f0e}} .class-both-responsive{{border-left-color:#d946ef}} .class-low-activity{{border-left-color:#9e9e9e}} .class-response-unavailable{{border-left-color:#ececec}}
.trace-row[data-group="floor-excluded"] .row-label{{background:#fff8d6}}
.trace-svg{{grid-column:2/-1;width:100%;height:100%;display:block}} .hidden{{display:none!important}}
.legend{{display:flex;gap:14px;flex-wrap:wrap;margin:5px 0 10px}} .swatch{{display:inline-block;width:13px;height:13px;vertical-align:-2px;margin-right:4px}}
code{{background:#f3f3f3;padding:1px 4px}} details{{margin:7px 0}} details table{{border-collapse:collapse;margin-top:6px}} details th,details td{{border:1px solid #ccc;padding:4px 7px;text-align:left}}
</style></head><body>
<header><h1>{fish_id}: all-neuron response trace inspector</h1>
<div class="note">All {len(rows):,} ROI rows of the score-activity-bpi table. Unsmoothed dF/F = (F − P10)/P10 per ROI, each trial shifted by its own prestimulus mean; trials are the persisted BPI windows. Window {w0:+.1f} to {w1:+.1f} s from appearance; gray dotted = appearance, red dashed = movement onset (+{prov["movement_onset_sec"]:.1f} s). One symmetric scale per neuron and view, printed beside the row. Read-only review surface: it records no decisions.</div>
<div class="summary"><span>All ROIs <b>{len(rows):,}</b></span><span>Response-positive <b>{grp_counts["response-positive"]:,}</b></span><span>Floor-excluded <b>{grp_counts["floor-excluded"]:,}</b></span><span>Assessable <b>{int(rows["response_class"].ne("response unavailable").sum()):,}</b></span><span>AUC reproduction error <b>{prov["max_recomputed_mean_auc_difference"]:.2e}</b></span></div>
<details><summary><b>Definitions</b></summary><table><tr><th>Term</th><th>n</th><th>Meaning</th></tr>
<tr><td>bout-/continuous-/both-responsive</td><td>{cls_counts["bout-responsive"]}/{cls_counts["continuous-responsive"]}/{cls_counts["both-responsive"]}</td><td>Condition mean AUC passes its two-sided per-cell null threshold (activation ≥ upper, suppression ≤ lower) <i>and</i> |AUC| ≥ the {floor:g} dF/F·s floor.</td></tr>
<tr><td>low activity</td><td>{cls_counts["low activity"]}</td><td>Assessable; neither condition passes.</td></tr>
<tr><td>response unavailable</td><td>{cls_counts["response unavailable"]}</td><td>Not assessable under the trace-quality/trial gate; not biological non-response.</td></tr>
<tr><td>floor-excluded (review group)</td><td>{grp_counts["floor-excluded"]}</td><td>Passes a null threshold in some condition but fails only the {floor:g} floor. Highlighted yellow. Review aid only; canonical classes unchanged.</td></tr></table>
<p>BPI preference is a separate effect-size label. Valence shows activation/suppression per condition.</p></details>
<div class="controls"><button id="tab-nonavg" class="active">{n_cols} individual trials</button><button id="tab-mean">{len(mean_labels)} stimulus means</button>
<label>Review group <select id="group-filter"><option value="all">all</option>{opt(REVIEW_GROUPS, grp_counts)}</select></label>
<label>Response class <select id="class-filter"><option value="all">all</option>{opt(CLASS_ORDER, cls_counts)}</select></label>
<label>BPI preference <select id="pref-filter"><option value="all">all</option>{opt(PREF_ORDER, pref_counts)}</select></label>
<label>Find <input id="search" placeholder="r1, plane 2, roi 140, suppress…"></label><span id="visible-count"></span></div></header>
<main>
<section id="view-nonavg" class="view active"><div class="legend">{legend}</div><div class="scroll"><div class="column-header nonavg"><span class="label-head">class · BPI · valence · plane · ROI · AUC · scale</span>{head(trial_labels, n_cols // 2)}</div>{_row_html(rows, scales, "nonavg")}</div></section>
<section id="view-mean" class="view"><div class="legend">{legend}<span>Mean of four baseline-shifted repetitions.</span></div><div class="scroll"><div class="column-header mean"><span class="label-head">class · BPI · valence · plane · ROI · AUC · scale</span>{head(mean_labels, len(mean_labels) // 2)}</div>{_row_html(rows, mean_scales, "mean")}</div></section>
</main>
<script>
const encoded={{nonavg:"{_encode(cube, scales)}",mean:"{_encode(mean_cube, mean_scales)}"}};
const shapes={{nonavg:[{len(rows)},{n_cols},{n_pts}],mean:[{len(rows)},{len(mean_labels)},{n_pts}]}};
const APP={app:.6f},MOV={mov:.6f};const decoded={{}};const NS="http://www.w3.org/2000/svg";
function decode(v){{if(decoded[v])return decoded[v];const r=atob(encoded[v]);const b=new Uint8Array(r.length);for(let i=0;i<r.length;i++)b[i]=r.charCodeAt(i);return decoded[v]=new Int16Array(b.buffer);}}
function add(s,n,a){{const e=document.createElementNS(NS,n);for(const[k,v]of Object.entries(a))e.setAttribute(k,v);s.appendChild(e);return e;}}
function draw(svg){{if(svg.dataset.drawn)return;const view=svg.closest('.trace-row').dataset.view;const[,nC,nP]=shapes[view];const row=+svg.dataset.index;const W=nC*100,H=view==='nonavg'?126:150;svg.setAttribute('viewBox',`0 0 ${{W}} ${{H}}`);const vals=decode(view);
for(let c=0;c<nC;c++){{add(svg,'rect',{{x:c*100,y:0,width:100,height:H,fill:c<nC/2?'#e5f1f7':'#fff0e3'}});add(svg,'line',{{x1:c*100,y1:H/2,x2:(c+1)*100,y2:H/2,stroke:'#bbb','stroke-width':.5}});const a=c*100+5+APP*90,m=c*100+5+MOV*90;add(svg,'line',{{x1:a,y1:0,x2:a,y2:H,stroke:'#666','stroke-width':.6,'stroke-dasharray':'2 3'}});add(svg,'line',{{x1:m,y1:0,x2:m,y2:H,stroke:'#c43c39','stroke-width':.8,'stroke-dasharray':'5 3'}});let d='';for(let p=0;p<nP;p++){{const i=(row*nC+c)*nP+p;const x=c*100+5+(p/(nP-1))*90;const y=H/2-(vals[i]/32767)*(H*.46);d+=(p?'L':'M')+x.toFixed(1)+' '+y.toFixed(1);}}add(svg,'path',{{d,fill:'none',stroke:'#111','stroke-width':.9,'vector-effect':'non-scaling-stroke'}});if(c)add(svg,'line',{{x1:c*100,y1:0,x2:c*100,y2:H,stroke:'#aaa','stroke-width':.6}});}}svg.dataset.drawn='1';}}
const obs=new IntersectionObserver(es=>es.forEach(e=>{{if(e.isIntersecting)draw(e.target)}}),{{rootMargin:'500px'}});document.querySelectorAll('.trace-svg').forEach(s=>obs.observe(s));
let active='nonavg';function setTab(v){{active=v;document.querySelectorAll('.view').forEach(x=>x.classList.remove('active'));document.getElementById('view-'+v).classList.add('active');document.getElementById('tab-nonavg').classList.toggle('active',v==='nonavg');document.getElementById('tab-mean').classList.toggle('active',v==='mean');applyFilter();}}
document.getElementById('tab-nonavg').onclick=()=>setTab('nonavg');document.getElementById('tab-mean').onclick=()=>setTab('mean');
function applyFilter(){{const q=document.getElementById('search').value.trim().toLowerCase();const g=document.getElementById('group-filter').value,c=document.getElementById('class-filter').value,p=document.getElementById('pref-filter').value;let n=0;document.querySelectorAll(`.trace-row[data-view="${{active}}"]`).forEach(r=>{{const s=(!q||r.dataset.search.includes(q))&&(g==='all'||r.dataset.group===g)&&(c==='all'||r.dataset.class===c)&&(p==='all'||r.dataset.pref===p);r.classList.toggle('hidden',!s);if(s)n++;}});document.getElementById('visible-count').textContent=`${{n.toLocaleString()}} visible`;}}
['search','group-filter','class-filter','pref-filter'].forEach(id=>{{const e=document.getElementById(id);e.oninput=e.onchange=applyFilter;}});applyFilter();
</script></body></html>'''


def render_response_trace_inspector(
    *, fish_id: str, fish_dir: Path, score_dir: Path, output_dir: Path, suite2p_dir: Path | None = None
) -> dict[str, Any]:
    """Write ``<fish_id>_response_trace_inspector.html`` plus metadata and a row index.

    ``score_dir`` holds the score-activity-bpi ``functional_roi_activity_bpi_cells.csv``
    and ``scored_stimulus_windows.csv``. Raises if any trial AUC fails to reproduce
    the persisted table, so the display cannot silently diverge from scoring.
    """
    fish_dir, score_dir, output_dir = Path(fish_dir), Path(score_dir), Path(output_dir)
    suite2p_dir = Path(suite2p_dir) if suite2p_dir else fish_dir / "03_analysis" / "functional" / "suite2P"
    cells_path = score_dir / "functional_roi_activity_bpi_cells.csv"
    windows_path = score_dir / "scored_stimulus_windows.csv"
    cells, windows = pd.read_csv(cells_path), pd.read_csv(windows_path)
    floors = pd.to_numeric(cells["response_auc_threshold"], errors="coerce").dropna().unique()
    if len(floors) != 1:
        raise RuntimeError(f"expected one response_auc_threshold, found {floors}")
    rows, cube, prov = build_trace_cube(cells, windows, suite2p_dir)
    mean_cube = cube.reshape(len(rows), len(STIM_TYPES), TRIALS_PER_STIM, cube.shape[2]).mean(axis=2)
    output_dir.mkdir(parents=True, exist_ok=True)
    html_path = output_dir / f"{fish_id}_response_trace_inspector.html"
    html_path.write_text(_document(fish_id, rows, cube, mean_cube, prov, float(floors[0])))
    rows.loc[:, ["session_label", "plane_idx", "func_label", "response_class", "review_group", "bpi_preference", "bpi", "bpi_group"]].to_csv(
        output_dir / "row_index.csv", index_label="display_row_zero_based"
    )
    from hashlib import sha256

    meta = {
        "fish_id": fish_id,
        "n_all_rois": int(len(rows)),
        "response_class_counts": rows["response_class"].value_counts().astype(int).to_dict(),
        "review_group_counts": rows["review_group"].value_counts().astype(int).to_dict(),
        "bpi_preference_counts": rows["bpi_preference"].value_counts().astype(int).to_dict(),
        "response_auc_floor": float(floors[0]),
        "trace_preprocessing": "full-session 10th-percentile dF/F; each trial shifted by its own prestimulus mean; no smoothing",
        "display_scaling": "one symmetric max-absolute scale per neuron and view",
        **{k: v for k, v in prov.items()},
        "inputs": {str(p): sha256(p.read_bytes()).hexdigest() for p in (cells_path, windows_path)},
        "html": str(html_path),
        "html_sha256": sha256(html_path.read_bytes()).hexdigest(),
        "html_bytes": html_path.stat().st_size,
    }
    (output_dir / "inspector_metadata.json").write_text(json.dumps(meta, indent=2) + "\n")
    return meta
