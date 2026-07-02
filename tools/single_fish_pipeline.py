#!/usr/bin/env python3
"""Thin CLI wrapper for staged single-fish pipeline commands."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from codeants_2pf_hcr.pipeline import (
    SingleFishPipelineConfig,
    build_single_fish_status,
    build_single_fish_compare_legacy_baseline_manifest,
    build_single_fish_downstream_stage_manifest,
    build_single_fish_compare_staged_manifest,
    build_single_fish_hcr_activity_replay_manifest,
    build_single_fish_score_activity_bpi_recompute_manifest,
    compare_single_fish_legacy_baseline,
    compare_single_fish_staged_outputs,
    downstream_stage_names,
    pipeline_contracts,
    resolve_pipeline_paths,
    run_prepare_functional_reference_stacks_stage,
    run_prepare_in_vivo_anatomy_stack_stage,
    run_prepare_ex_vivo_anatomy_stack_stage,
    run_register_functional_to_anatomy_stage,
    run_register_hcr_to_anatomy_stage,
    run_match_roi_to_anatomy_stage,
    run_segment_ex_vivo_anatomy_cellpose_stage,
    run_segment_hcr_cellpose_stage,
    run_single_fish_assign_hcr_identity_stage,
    run_single_fish_export_canonical_tables_stage,
    run_single_fish_freeze_legacy_baseline_stage,
    run_single_fish_make_figures_stage,
    run_single_fish_make_qa_report_stage,
    run_single_fish_audit_inputs_stage,
    run_single_fish_score_activity_bpi_stage,
    stage_manifest_to_json,
    write_cellpose_stage_manifest,
    write_stage_manifest,
)


def _add_common_fish_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--fish-id", required=True)
    parser.add_argument("--local-root", required=True, type=Path)
    parser.add_argument("--owner", default="Matilde")
    parser.add_argument("--strict", action="store_true")
    parser.add_argument(
        "--pipeline-root",
        type=Path,
        default=None,
        help="Optional staged output root. Read-only commands inspect/report it; writer commands may create outputs there.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Read-only first-pass mode. This remains the default unless --write-manifest is set.",
    )
    parser.add_argument(
        "--write-manifest",
        action="store_true",
        help="Opt in to writing the stage manifest under the fish pipeline_manifests directory.",
    )


def _add_gpu_args(parser: argparse.ArgumentParser) -> None:
    gpu = parser.add_mutually_exclusive_group()
    gpu.add_argument("--use-gpu", dest="use_gpu", action="store_true", help="Run Cellpose with GPU acceleration.")
    gpu.add_argument("--no-gpu", dest="use_gpu", action="store_false", help="Run Cellpose on CPU.")
    parser.set_defaults(use_gpu=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("contracts", help="Print declared stage contracts as JSON.")

    audit = subparsers.add_parser("audit-inputs", help="Read-only audit of fish-scoped inputs.")
    _add_common_fish_args(audit)

    status = subparsers.add_parser("status", help="Summarize read-only staged pipeline trust state.")
    _add_common_fish_args(status)

    stage_status = subparsers.add_parser(
        "stage-status",
        help="Read-only manifest for an existing post-preprocessing staged output folder.",
    )
    _add_common_fish_args(stage_status)
    stage_status.add_argument("--stage-name", required=True, choices=downstream_stage_names())

    compare = subparsers.add_parser(
        "compare-staged",
        help="Read-only comparison of existing post-preprocessing staged outputs to control outputs.",
    )
    _add_common_fish_args(compare)
    compare.add_argument("--stage-name", choices=downstream_stage_names())

    freeze_legacy = subparsers.add_parser(
        "freeze-legacy-baseline",
        help="Writer stage: freeze declared legacy/control outputs into a fish-scoped baseline bundle.",
    )
    _add_common_fish_args(freeze_legacy)
    freeze_legacy.add_argument("--stage-name", choices=downstream_stage_names())
    freeze_legacy.add_argument("--overwrite", action="store_true")

    compare_legacy = subparsers.add_parser(
        "compare-legacy-baseline",
        help="Read-only comparison of staged outputs against a frozen legacy baseline bundle.",
    )
    _add_common_fish_args(compare_legacy)
    compare_legacy.add_argument("--stage-name", choices=downstream_stage_names())

    score_audit = subparsers.add_parser(
        "audit-score-activity-bpi",
        help="Read-only recompute audit for response/BPI scoring against control tables.",
    )
    _add_common_fish_args(score_audit)

    hcr_replay_audit = subparsers.add_parser(
        "audit-hcr-activity-replay",
        help="Read-only replay audit for HCR-centric identified-cell activity tables.",
    )
    _add_common_fish_args(hcr_replay_audit)
    hcr_replay_audit.add_argument("--plane-refs-summary-path", type=Path)
    hcr_replay_audit.add_argument("--hcr-anatomy-root", type=Path)
    hcr_replay_audit.add_argument("--identity-input-path", type=Path)
    hcr_replay_audit.add_argument("--anatomy-labels-path", type=Path)

    assign_writer = subparsers.add_parser(
        "assign-hcr-identity",
        help="Writer stage: stage baseline identity/HCR registration CSVs behind the assign-hcr-identity contract.",
    )
    _add_common_fish_args(assign_writer)
    assign_writer.add_argument(
        "--source-root",
        type=Path,
        help="Explicit identity/HCR registration CSV source root. Defaults to the fish registration folder.",
    )
    assign_writer.add_argument(
        "--roi-anatomy-root",
        type=Path,
        help="Explicit staged ROI/anatomy geometry root. Defaults to pipeline_root/match-roi-to-anatomy/registration.",
    )
    assign_writer.add_argument(
        "--hcr-anatomy-root",
        type=Path,
        help="Explicit staged HCR/anatomy aligned artifact root. Defaults to pipeline_root/register-hcr-to-anatomy/confocal/aligned.",
    )
    assign_writer.add_argument("--force-recompute", action="store_true")

    score_writer = subparsers.add_parser(
        "score-activity-bpi",
        help="Writer stage: recompute response/BPI scoring and write staged score CSVs.",
    )
    _add_common_fish_args(score_writer)
    score_writer.add_argument(
        "--identity-input-path",
        type=Path,
        help="Explicit ROI identity input CSV. Defaults to staged assign-hcr-identity output.",
    )
    score_writer.add_argument("--force-recompute", action="store_true")

    export_canonical = subparsers.add_parser(
        "export-canonical-tables",
        help="Writer stage: assemble staged canonical registration CSV bundle from upstream staged tables.",
    )
    _add_common_fish_args(export_canonical)
    export_canonical.add_argument(
        "--score-input-root",
        type=Path,
        help="Explicit score-activity-bpi registration CSV root. Defaults to staged score output root.",
    )
    export_canonical.add_argument(
        "--hcr-input-root",
        type=Path,
        help="Explicit HCR/identity registration CSV root. Defaults to staged assign-hcr-identity output root.",
    )
    export_canonical.add_argument("--force-recompute", action="store_true")

    make_qa_report = subparsers.add_parser(
        "make-qa-report",
        help="Writer stage: generate a staged single-fish QA report from canonical table outputs.",
    )
    _add_common_fish_args(make_qa_report)
    make_qa_report.add_argument(
        "--canonical-input-root",
        type=Path,
        help="Explicit canonical registration CSV root. Defaults to staged export-canonical-tables output root.",
    )
    make_qa_report.add_argument("--force-recompute", action="store_true")

    make_figures = subparsers.add_parser(
        "make-figures",
        help="Writer stage: stage final figure artifacts behind the make-figures manifest contract.",
    )
    _add_common_fish_args(make_figures)
    make_figures.add_argument(
        "--canonical-input-root",
        type=Path,
        help="Explicit canonical registration CSV root. Defaults to staged export-canonical-tables output root.",
    )
    make_figures.add_argument(
        "--figure-input-root",
        type=Path,
        help="Explicit source figure root. Defaults to the fish 04_plots directory.",
    )
    make_figures.add_argument("--force-recompute", action="store_true")

    prepare_func_refs = subparsers.add_parser(
        "prepare-functional-reference-stacks",
        help="Writer stage: prepare oriented functional reference TIFFs from motion-corrected stacks.",
    )
    _add_common_fish_args(prepare_func_refs)
    prepare_func_refs.add_argument("--functional-stack-path", action="append", type=Path)
    prepare_func_refs.add_argument("--output-dir", type=Path)
    prepare_func_refs.add_argument("--force-recompute", action="store_true")

    prepare_in_vivo = subparsers.add_parser(
        "prepare-in-vivo-anatomy-stack",
        help="Writer stage: prepare the in vivo 2P anatomy stack as a registration-ready NRRD.",
    )
    _add_common_fish_args(prepare_in_vivo)
    prepare_in_vivo.add_argument("--anatomy-stack-path", type=Path)
    prepare_in_vivo.add_argument("--output-path", type=Path)
    prepare_in_vivo.add_argument("--force-recompute", action="store_true")

    register_func = subparsers.add_parser(
        "register-functional-to-anatomy",
        help="Writer stage: register staged functional references to the prepared in vivo anatomy stack.",
    )
    _add_common_fish_args(register_func)
    register_func.add_argument("--reference-dir", type=Path)
    register_func.add_argument("--functional-reference-dir", dest="reference_dir", type=Path)
    register_func.add_argument("--anatomy-stack-path", type=Path)
    register_func.add_argument("--anatomy-labels-path", type=Path)
    register_func.add_argument("--functional-labels-anatomy-dir", type=Path)
    register_func.add_argument("--output-root", type=Path)
    register_func.add_argument("--skip-inplane-comparison", action="store_true")
    register_func.add_argument("--inplane-method", action="append", default=None)
    register_func.add_argument("--active-inplane-method", default="ncc_xy")
    register_func.add_argument("--skip-visual-qa", action="store_true")
    register_func.add_argument("--visual-qa-crop-size-px", type=int, default=200)
    register_func.add_argument("--no-cv2", dest="use_cv2", action="store_false")
    register_func.set_defaults(use_cv2=False)
    register_func.add_argument("--force-recompute", action="store_true")

    register_hcr = subparsers.add_parser(
        "register-hcr-to-anatomy",
        help="Writer stage: stage accepted HCR-to-anatomy aligned artifacts.",
    )
    _add_common_fish_args(register_hcr)
    register_hcr.add_argument("--source-root", type=Path)
    register_hcr.add_argument("--output-root", type=Path)
    register_hcr.add_argument("--force-recompute", action="store_true")

    match_roi = subparsers.add_parser(
        "match-roi-to-anatomy",
        help="Writer stage: compute geometry-only ROI/anatomy matches from staged registration outputs.",
    )
    _add_common_fish_args(match_roi)
    match_roi.add_argument("--source-root", type=Path)
    match_roi.add_argument("--plane-refs-summary-path", type=Path)
    match_roi.add_argument("--anatomy-labels-path", type=Path)
    match_roi.add_argument("--output-root", type=Path)
    match_roi.add_argument("--force-recompute", action="store_true")

    prepare_ex_vivo = subparsers.add_parser(
        "prepare-ex-vivo-anatomy-stack",
        help="Writer stage: prepare the ex vivo 2P anatomy stack as a registration-ready NRRD.",
    )
    _add_common_fish_args(prepare_ex_vivo)
    prepare_ex_vivo.add_argument("--ex-vivo-stack-path", type=Path)
    prepare_ex_vivo.add_argument("--output-path", type=Path)
    prepare_ex_vivo.add_argument("--force-recompute", action="store_true")

    segment_ex_vivo = subparsers.add_parser(
        "segment-ex-vivo-anatomy-cellpose",
        help="Writer stage: segment the prepared ex vivo anatomy stack with Cellpose.",
    )
    _add_common_fish_args(segment_ex_vivo)
    segment_ex_vivo.add_argument("--anatomy-stack-path", type=Path)
    segment_ex_vivo.add_argument("--anat-cp-model-path", required=True, type=Path)
    _add_gpu_args(segment_ex_vivo)
    segment_ex_vivo.add_argument("--compute-device")
    segment_ex_vivo.add_argument("--force-recompute", action="store_true")

    segment_hcr = subparsers.add_parser(
        "segment-hcr-cellpose",
        help="Writer stage: segment HCR intensity stacks from a specific preprocessing source with Cellpose.",
    )
    _add_common_fish_args(segment_hcr)
    segment_hcr.add_argument("--hcr-source", choices=("rbest", "rn", "all"), default="rbest")
    segment_hcr.add_argument("--cp-hcr-model-path", required=True, type=Path)
    _add_gpu_args(segment_hcr)
    segment_hcr.add_argument("--force-recompute", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "contracts":
        payload = [contract.__dict__ for contract in pipeline_contracts()]
        sys.stdout.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        return 0
    if args.command == "audit-inputs":
        config = SingleFishPipelineConfig(
            fish_id=args.fish_id,
            local_root=args.local_root,
            owner=args.owner,
            strict=args.strict,
            dry_run=True,
            write_manifest=args.write_manifest,
            pipeline_root=args.pipeline_root,
        )
        manifest = run_single_fish_audit_inputs_stage(config)
        if args.write_manifest:
            write_stage_manifest(manifest, resolve_pipeline_paths(config))
        sys.stdout.write(stage_manifest_to_json(manifest))
        return 1 if manifest.status == "fail" else 0
    if args.command == "status":
        config = SingleFishPipelineConfig(
            fish_id=args.fish_id,
            local_root=args.local_root,
            owner=args.owner,
            strict=args.strict,
            dry_run=True,
            write_manifest=False,
            pipeline_root=args.pipeline_root,
        )
        payload = build_single_fish_status(config)
        sys.stdout.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        return 1 if payload["status"] == "fail" else 0
    if args.command == "stage-status":
        config = SingleFishPipelineConfig(
            fish_id=args.fish_id,
            local_root=args.local_root,
            owner=args.owner,
            strict=args.strict,
            dry_run=True,
            write_manifest=args.write_manifest,
            pipeline_root=args.pipeline_root,
        )
        manifest = build_single_fish_downstream_stage_manifest(config, args.stage_name)
        if args.write_manifest:
            write_stage_manifest(manifest, resolve_pipeline_paths(config))
        sys.stdout.write(stage_manifest_to_json(manifest))
        return 1 if manifest.status == "fail" else 0
    if args.command == "compare-staged":
        config = SingleFishPipelineConfig(
            fish_id=args.fish_id,
            local_root=args.local_root,
            owner=args.owner,
            strict=args.strict,
            dry_run=True,
            write_manifest=args.write_manifest,
            pipeline_root=args.pipeline_root,
        )
        payload = compare_single_fish_staged_outputs(config, args.stage_name)
        if args.write_manifest:
            paths = resolve_pipeline_paths(config)
            stage_names = (args.stage_name,) if args.stage_name else downstream_stage_names()
            for stage_name in stage_names:
                manifest = build_single_fish_compare_staged_manifest(config, stage_name)
                write_stage_manifest(manifest, paths)
        sys.stdout.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        return 1 if payload["status"] == "fail" else 0
    if args.command == "freeze-legacy-baseline":
        config = SingleFishPipelineConfig(
            fish_id=args.fish_id,
            local_root=args.local_root,
            owner=args.owner,
            strict=args.strict,
            dry_run=False,
            write_manifest=args.write_manifest,
            pipeline_root=args.pipeline_root,
        )
        manifest = run_single_fish_freeze_legacy_baseline_stage(
            config,
            stage_name=args.stage_name,
            overwrite=args.overwrite,
        )
        if args.write_manifest:
            write_stage_manifest(manifest, resolve_pipeline_paths(config))
        sys.stdout.write(stage_manifest_to_json(manifest))
        return 1 if manifest.status == "fail" else 0
    if args.command == "compare-legacy-baseline":
        config = SingleFishPipelineConfig(
            fish_id=args.fish_id,
            local_root=args.local_root,
            owner=args.owner,
            strict=args.strict,
            dry_run=True,
            write_manifest=args.write_manifest,
            pipeline_root=args.pipeline_root,
        )
        payload = compare_single_fish_legacy_baseline(config, args.stage_name)
        if args.write_manifest:
            paths = resolve_pipeline_paths(config)
            stage_names = (args.stage_name,) if args.stage_name else downstream_stage_names()
            for stage_name in stage_names:
                manifest = build_single_fish_compare_legacy_baseline_manifest(config, stage_name)
                write_stage_manifest(manifest, paths)
        sys.stdout.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        return 1 if payload["status"] == "fail" else 0
    if args.command == "audit-hcr-activity-replay":
        config = SingleFishPipelineConfig(
            fish_id=args.fish_id,
            local_root=args.local_root,
            owner=args.owner,
            strict=args.strict,
            dry_run=True,
            write_manifest=args.write_manifest,
            pipeline_root=args.pipeline_root,
        )
        manifest = build_single_fish_hcr_activity_replay_manifest(
            config,
            plane_refs_summary_path=args.plane_refs_summary_path,
            hcr_anatomy_root=args.hcr_anatomy_root,
            identity_input_path=args.identity_input_path,
            anatomy_labels_path=args.anatomy_labels_path,
        )
        if args.write_manifest:
            write_stage_manifest(manifest, resolve_pipeline_paths(config))
        sys.stdout.write(stage_manifest_to_json(manifest))
        return 1 if manifest.status == "fail" else 0
    if args.command == "audit-score-activity-bpi":
        config = SingleFishPipelineConfig(
            fish_id=args.fish_id,
            local_root=args.local_root,
            owner=args.owner,
            strict=args.strict,
            dry_run=True,
            write_manifest=args.write_manifest,
            pipeline_root=args.pipeline_root,
        )
        manifest = build_single_fish_score_activity_bpi_recompute_manifest(config)
        if args.write_manifest:
            write_stage_manifest(manifest, resolve_pipeline_paths(config))
        sys.stdout.write(stage_manifest_to_json(manifest))
        return 1 if manifest.status == "fail" else 0
    if args.command == "assign-hcr-identity":
        config = SingleFishPipelineConfig(
            fish_id=args.fish_id,
            local_root=args.local_root,
            owner=args.owner,
            strict=args.strict,
            dry_run=False,
            write_manifest=args.write_manifest,
            pipeline_root=args.pipeline_root,
        )
        manifest = run_single_fish_assign_hcr_identity_stage(
            config,
            source_root=args.source_root,
            roi_anatomy_root=args.roi_anatomy_root,
            hcr_anatomy_root=args.hcr_anatomy_root,
            force_recompute=args.force_recompute,
        )
        if args.write_manifest:
            write_stage_manifest(manifest, resolve_pipeline_paths(config))
        sys.stdout.write(stage_manifest_to_json(manifest))
        return 1 if manifest.status == "fail" else 0
    if args.command == "score-activity-bpi":
        config = SingleFishPipelineConfig(
            fish_id=args.fish_id,
            local_root=args.local_root,
            owner=args.owner,
            strict=args.strict,
            dry_run=False,
            write_manifest=args.write_manifest,
            pipeline_root=args.pipeline_root,
        )
        manifest = run_single_fish_score_activity_bpi_stage(
            config,
            identity_input_path=args.identity_input_path,
            force_recompute=args.force_recompute,
        )
        if args.write_manifest:
            write_stage_manifest(manifest, resolve_pipeline_paths(config))
        sys.stdout.write(stage_manifest_to_json(manifest))
        return 1 if manifest.status == "fail" else 0
    if args.command == "export-canonical-tables":
        config = SingleFishPipelineConfig(
            fish_id=args.fish_id,
            local_root=args.local_root,
            owner=args.owner,
            strict=args.strict,
            dry_run=False,
            write_manifest=args.write_manifest,
            pipeline_root=args.pipeline_root,
        )
        manifest = run_single_fish_export_canonical_tables_stage(
            config,
            score_input_root=args.score_input_root,
            hcr_input_root=args.hcr_input_root,
            force_recompute=args.force_recompute,
        )
        if args.write_manifest:
            write_stage_manifest(manifest, resolve_pipeline_paths(config))
        sys.stdout.write(stage_manifest_to_json(manifest))
        return 1 if manifest.status == "fail" else 0
    if args.command == "make-qa-report":
        config = SingleFishPipelineConfig(
            fish_id=args.fish_id,
            local_root=args.local_root,
            owner=args.owner,
            strict=args.strict,
            dry_run=False,
            write_manifest=args.write_manifest,
            pipeline_root=args.pipeline_root,
        )
        manifest = run_single_fish_make_qa_report_stage(
            config,
            canonical_input_root=args.canonical_input_root,
            force_recompute=args.force_recompute,
        )
        if args.write_manifest:
            write_stage_manifest(manifest, resolve_pipeline_paths(config))
        sys.stdout.write(stage_manifest_to_json(manifest))
        return 1 if manifest.status == "fail" else 0
    if args.command == "make-figures":
        config = SingleFishPipelineConfig(
            fish_id=args.fish_id,
            local_root=args.local_root,
            owner=args.owner,
            strict=args.strict,
            dry_run=False,
            write_manifest=args.write_manifest,
            pipeline_root=args.pipeline_root,
        )
        manifest = run_single_fish_make_figures_stage(
            config,
            canonical_input_root=args.canonical_input_root,
            figure_input_root=args.figure_input_root,
            force_recompute=args.force_recompute,
        )
        if args.write_manifest:
            write_stage_manifest(manifest, resolve_pipeline_paths(config))
        sys.stdout.write(stage_manifest_to_json(manifest))
        return 1 if manifest.status == "fail" else 0
    if args.command == "prepare-functional-reference-stacks":
        config = SingleFishPipelineConfig(
            fish_id=args.fish_id,
            local_root=args.local_root,
            owner=args.owner,
            strict=args.strict,
            dry_run=False,
            write_manifest=args.write_manifest,
            pipeline_root=args.pipeline_root,
        )
        manifest = run_prepare_functional_reference_stacks_stage(
            config,
            functional_stack_paths=args.functional_stack_path,
            output_dir=args.output_dir,
            force_recompute=args.force_recompute,
        )
        if args.write_manifest:
            write_stage_manifest(manifest, resolve_pipeline_paths(config))
        sys.stdout.write(stage_manifest_to_json(manifest))
        return 1 if manifest.status == "fail" else 0
    if args.command == "register-functional-to-anatomy":
        config = SingleFishPipelineConfig(
            fish_id=args.fish_id,
            local_root=args.local_root,
            owner=args.owner,
            strict=args.strict,
            dry_run=False,
            write_manifest=args.write_manifest,
            pipeline_root=args.pipeline_root,
        )
        manifest = run_register_functional_to_anatomy_stage(
            config,
            reference_dir=args.reference_dir,
            anatomy_stack_path=args.anatomy_stack_path,
            anatomy_labels_path=args.anatomy_labels_path,
            functional_labels_anatomy_dir=args.functional_labels_anatomy_dir,
            output_root=args.output_root,
            force_recompute=args.force_recompute,
            run_inplane_comparison=not args.skip_inplane_comparison,
            inplane_methods=tuple(args.inplane_method or ("ncc_xy",)),
            active_inplane_method=args.active_inplane_method,
            use_cv2=args.use_cv2,
            emit_visual_qa=not args.skip_visual_qa,
            visual_qa_crop_size_px=args.visual_qa_crop_size_px,
        )
        if args.write_manifest:
            write_stage_manifest(manifest, resolve_pipeline_paths(config))
        sys.stdout.write(stage_manifest_to_json(manifest))
        return 1 if manifest.status == "fail" else 0
    if args.command == "register-hcr-to-anatomy":
        config = SingleFishPipelineConfig(
            fish_id=args.fish_id,
            local_root=args.local_root,
            owner=args.owner,
            strict=args.strict,
            dry_run=False,
            write_manifest=args.write_manifest,
            pipeline_root=args.pipeline_root,
        )
        manifest = run_register_hcr_to_anatomy_stage(
            config,
            source_root=args.source_root,
            output_root=args.output_root,
            force_recompute=args.force_recompute,
        )
        if args.write_manifest:
            write_stage_manifest(manifest, resolve_pipeline_paths(config))
        sys.stdout.write(stage_manifest_to_json(manifest))
        return 1 if manifest.status == "fail" else 0
    if args.command == "match-roi-to-anatomy":
        config = SingleFishPipelineConfig(
            fish_id=args.fish_id,
            local_root=args.local_root,
            owner=args.owner,
            strict=args.strict,
            dry_run=False,
            write_manifest=args.write_manifest,
            pipeline_root=args.pipeline_root,
        )
        manifest = run_match_roi_to_anatomy_stage(
            config,
            source_root=args.source_root,
            plane_refs_summary_path=args.plane_refs_summary_path,
            anatomy_labels_path=args.anatomy_labels_path,
            output_root=args.output_root,
            force_recompute=args.force_recompute,
        )
        if args.write_manifest:
            write_stage_manifest(manifest, resolve_pipeline_paths(config))
        sys.stdout.write(stage_manifest_to_json(manifest))
        return 1 if manifest.status == "fail" else 0
    if args.command == "prepare-in-vivo-anatomy-stack":
        config = SingleFishPipelineConfig(
            fish_id=args.fish_id,
            local_root=args.local_root,
            owner=args.owner,
            strict=args.strict,
            dry_run=False,
            write_manifest=args.write_manifest,
            pipeline_root=args.pipeline_root,
        )
        manifest = run_prepare_in_vivo_anatomy_stack_stage(
            config,
            anatomy_stack_path=args.anatomy_stack_path,
            output_path=args.output_path,
            force_recompute=args.force_recompute,
        )
        if args.write_manifest:
            write_stage_manifest(manifest, resolve_pipeline_paths(config))
        sys.stdout.write(stage_manifest_to_json(manifest))
        return 1 if manifest.status == "fail" else 0
    if args.command == "prepare-ex-vivo-anatomy-stack":
        config = SingleFishPipelineConfig(
            fish_id=args.fish_id,
            local_root=args.local_root,
            owner=args.owner,
            strict=args.strict,
            dry_run=False,
            write_manifest=args.write_manifest,
            pipeline_root=args.pipeline_root,
        )
        manifest = run_prepare_ex_vivo_anatomy_stack_stage(
            config,
            ex_vivo_stack_path=args.ex_vivo_stack_path,
            output_path=args.output_path,
            force_recompute=args.force_recompute,
        )
        if args.write_manifest:
            write_cellpose_stage_manifest(manifest, resolve_pipeline_paths(config))
        sys.stdout.write(stage_manifest_to_json(manifest))
        return 1 if manifest.status == "fail" else 0
    if args.command == "segment-ex-vivo-anatomy-cellpose":
        config = SingleFishPipelineConfig(
            fish_id=args.fish_id,
            local_root=args.local_root,
            owner=args.owner,
            strict=args.strict,
            dry_run=False,
            write_manifest=args.write_manifest,
            pipeline_root=args.pipeline_root,
        )
        manifest = run_segment_ex_vivo_anatomy_cellpose_stage(
            config,
            anatomy_stack_path=args.anatomy_stack_path,
            anat_cp_model_path=args.anat_cp_model_path,
            use_gpu=args.use_gpu,
            compute_device=args.compute_device,
            force_recompute=args.force_recompute,
        )
        if args.write_manifest:
            write_cellpose_stage_manifest(manifest, resolve_pipeline_paths(config))
        sys.stdout.write(stage_manifest_to_json(manifest))
        return 1 if manifest.status == "fail" else 0
    if args.command == "segment-hcr-cellpose":
        config = SingleFishPipelineConfig(
            fish_id=args.fish_id,
            local_root=args.local_root,
            owner=args.owner,
            strict=args.strict,
            dry_run=False,
            write_manifest=args.write_manifest,
            pipeline_root=args.pipeline_root,
        )
        manifest = run_segment_hcr_cellpose_stage(
            config,
            hcr_source=args.hcr_source,
            cp_hcr_model_path=args.cp_hcr_model_path,
            use_gpu=args.use_gpu,
            force_recompute=args.force_recompute,
        )
        if args.write_manifest:
            write_cellpose_stage_manifest(manifest, resolve_pipeline_paths(config))
        sys.stdout.write(stage_manifest_to_json(manifest))
        return 1 if manifest.status == "fail" else 0
    parser.error(f"unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
