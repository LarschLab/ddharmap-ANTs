#!/usr/bin/env python3
"""Launch manual HCR annotation or export a frozen session to ROI tables."""
import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    prepare = sub.add_parser('prepare')
    prepare.add_argument('--config', required=True)
    prepare.add_argument('--output', required=True)
    recover = sub.add_parser('recover-lock')
    recover.add_argument('--session', required=True)
    recover.add_argument('--expected-pid', type=int, required=True)
    canonical = sub.add_parser('canonical')
    for name in ('manual-root', 'score-root', 'output'):
        canonical.add_argument('--' + name, required=True)
    view = sub.add_parser('view')
    view.add_argument('--manifest', required=True)
    view.add_argument('--session', required=True)
    view.add_argument('--reviewer', required=True)
    export = sub.add_parser('export')
    for name in ('manifest', 'snapshot', 'correspondence', 'geometry', 'roi-master', 'output'):
        export.add_argument('--' + name, required=True)
    export.add_argument('--gene-order', required=True, help='Comma-separated configured pipeline gene order')
    args = parser.parse_args()
    if args.command == 'recover-lock':
        from codeants_2pf_hcr.manual_identity import recover_session_lock
        recover_session_lock(args.session, args.expected_pid)
        return 0
    if args.command == 'canonical':
        from codeants_2pf_hcr.manual_identity import export_manual_canonical_tables
        export_manual_canonical_tables(args.manual_root, args.score_root, args.output)
        return 0
    if args.command == 'prepare':
        from codeants_2pf_hcr.manual_identity import prepare_dataset
        prepare_dataset(args.config, args.output)
        return 0
    if args.command == 'view':
        from codeants_2pf_hcr.apps.manual_identity import main as view_main
        return view_main(['--manifest', args.manifest, '--session', args.session, '--reviewer', args.reviewer])
    from codeants_2pf_hcr.manual_identity import Dataset, export_manual_identity
    export_manual_identity(Dataset(args.manifest), args.snapshot, args.correspondence, args.geometry,
                           args.roi_master, args.output, args.gene_order.split(','))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
