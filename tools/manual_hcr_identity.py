#!/usr/bin/env python3
"""Manual ex-vivo identity: launcher app, dataset preparation, review, identity gate, continuation."""
import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('app', help='Open the cellMatcher launcher')
    prepare_fish = sub.add_parser('prepare-fish', help='Discover pipeline inputs for one fish and bind them')
    prepare = sub.add_parser('prepare', help='Bind an explicit input config (advanced)')
    prepare.add_argument('--config', required=True)
    prepare.add_argument('--output', required=True)
    recover = sub.add_parser('recover-lock')
    recover.add_argument('--session', required=True)
    recover.add_argument('--expected-pid', type=int, required=True)
    view = sub.add_parser('view')
    view.add_argument('--manifest', required=True)
    view.add_argument('--session', required=True)
    view.add_argument('--reviewer', required=True)
    gate = sub.add_parser('gate', help='Write the two ROI identity tables from a frozen snapshot and stop')
    gate.add_argument('--manifest', required=True)
    gate.add_argument('--snapshot', required=True)
    gate.add_argument('--confirm-replace', action='store_true', help='Replace/retire existing identity outputs (backed up)')
    resume = sub.add_parser('continue', help='Run assign-hcr-identity and downstream stages past the gate')
    for name, command in (('prepare-fish', prepare_fish), ('gate', gate), ('continue', resume)):
        command.add_argument('--local-root', required=True)
        if name != 'gate':
            command.add_argument('--fish-id', required=True)
    args = parser.parse_args()
    if args.command == 'app':
        from codeants_2pf_hcr.apps.cellmatcher import main as app_main
        return app_main()
    if args.command == 'recover-lock':
        from codeants_2pf_hcr.manual_identity import recover_session_lock
        recover_session_lock(args.session, args.expected_pid)
        return 0
    if args.command == 'prepare':
        from codeants_2pf_hcr.manual_identity import prepare_dataset
        print(prepare_dataset(args.config, args.output))
        return 0
    if args.command == 'view':
        from codeants_2pf_hcr.apps.manual_identity import main as view_main
        return view_main(['--manifest', args.manifest, '--session', args.session, '--reviewer', args.reviewer])
    from codeants_2pf_hcr import manual_identity_pipeline as mip
    if args.command == 'prepare-fish':
        print(mip.prepare_fish_dataset(args.fish_id, args.local_root))
        return 0
    if args.command == 'gate':
        from codeants_2pf_hcr.manual_identity import Dataset
        dataset = Dataset(args.manifest)
        plan = mip.plan_identity_gate(dataset, args.local_root)
        pending = plan['replace'] + plan['retire']
        if pending and not args.confirm_replace:
            print('Existing identity outputs would be replaced; rerun with --confirm-replace:\n' + '\n'.join(pending))
            return 2
        gate_record = mip.run_identity_gate(dataset, args.snapshot, args.local_root, confirmed=pending)
        print(gate_record['status'], gate_record['export'])
        return 0
    result = mip.continue_after_gate(args.fish_id, args.local_root)
    print(result['status'], result['continuation']['run_root'])
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
