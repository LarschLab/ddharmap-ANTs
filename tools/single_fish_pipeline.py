#!/usr/bin/env python3
"""Thin CLI wrapper for read-only staged single-fish pipeline commands."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from codeants_2pf_hcr.pipeline import (
    SingleFishPipelineConfig,
    build_single_fish_status,
    build_single_fish_downstream_stage_manifest,
    build_single_fish_compare_staged_manifest,
    compare_single_fish_staged_outputs,
    downstream_stage_names,
    pipeline_contracts,
    resolve_pipeline_paths,
    run_single_fish_audit_inputs_stage,
    stage_manifest_to_json,
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
        help="Optional future output root. Read-only commands only report it; they do not create it.",
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
    parser.error(f"unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
