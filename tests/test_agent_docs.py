import re
import subprocess
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (REPO_ROOT / rel_path).read_text()


class AgentDocsTests(unittest.TestCase):
    def test_agents_entrypoint_points_to_router_and_reference_docs(self) -> None:
        source = _read("AGENTS.md")
        self.assertIn(".agents/workflows/2pf-hcr-router.md", source)
        self.assertIn(".agents/references/notebook-stage-map.md", source)
        self.assertIn(".agents/references/canonical-tables.md", source)
        self.assertIn(".agents/references/recent-changes.md", source)
        self.assertIn(".agents/references/recent-changes-single-fish.md", source)
        self.assertIn(".agents/references/recent-changes-cohort.md", source)
        self.assertIn(".agents/references/agentic-workflow-roadmap.md", source)
        self.assertIn("append the workflow-specific recent-changes file", source)

    def test_top_router_dispatches_to_profile_routers(self) -> None:
        source = _read(".agents/workflows/2pf-hcr-router.md")
        self.assertIn("Read this router first.", source)
        self.assertIn(".agents/workflows/2pf-hcr-single-fish-router.md", source)
        self.assertIn(".agents/workflows/2pf-hcr-cohort-router.md", source)
        self.assertIn("Cross-workflow invariants (always)", source)
        self.assertIn("Compact scaling rule", source)
        self.assertIn("shares stage semantics, ownership modules, and validation surface", source)

    def test_profile_routers_reference_owning_stage_map_and_handoff_log(self) -> None:
        single = _read(".agents/workflows/2pf-hcr-single-fish-router.md")
        cohort = _read(".agents/workflows/2pf-hcr-cohort-router.md")
        self.assertIn("references/notebook-stage-map.md", single)
        self.assertIn("references/recent-changes-single-fish.md", single)
        self.assertIn("references/agentic-workflow-roadmap.md", single)
        self.assertIn("references/single-fish-pipeline-roadmap.md", single)
        self.assertIn("Do **not** use `tools/` as business-logic authority.", single)
        self.assertIn("references/cohort-stage-map.md", cohort)
        self.assertIn("references/recent-changes-cohort.md", cohort)
        self.assertIn("Do **not** use `tools/` as business-logic authority.", cohort)

    def test_refactor_rules_define_ownership_and_edit_scope(self) -> None:
        source = _read(".agents/references/refactor-rules.md")
        self.assertIn("Notebook orchestration lives in notebook cells only.", source)
        self.assertIn("Reusable logic lives in `src/codeants_2pf_hcr/`.", source)
        self.assertIn("General CLI behavior lives in `tools/` wrappers.", source)
        self.assertIn("Maintained manual registration entrypoints live in `registrations/`.", source)
        self.assertIn("Figure construction lives in `plots.*`.", source)
        self.assertIn("Table semantics are owned by the stage that writes the table", source)
        self.assertIn("Fix at the narrowest owning layer.", source)
        self.assertIn("Do not patch downstream figures to compensate for upstream semantic bugs.", source)
        self.assertIn("Notebook-callable functions must be public and listed in `__all__`.", source)

    def test_canonical_tables_name_authorities(self) -> None:
        source = _read(".agents/references/canonical-tables.md")
        self.assertIn("Whole-population identity -> `functional_roi_activity_identity.csv`.", source)
        self.assertIn("Identified-cell activity export -> HCR-centric tables from `[50]`.", source)
        self.assertIn("Response semantics -> `[50ia]` / activity stage outputs.", source)
        self.assertIn("Geometry matching -> matching stage only.", source)
        self.assertIn("Figure semantics -> canonical tables plus subset filters", source)

    def test_current_state_and_recent_changes_have_distinct_purposes(self) -> None:
        current_state = _read(".agents/references/current-state.md")
        recent_changes = _read(".agents/references/recent-changes.md")
        self.assertIn("mixed migration state", current_state)
        self.assertIn("Cohort notebook path", current_state)
        self.assertIn("index and compatibility pointer", recent_changes)
        self.assertIn("Workflow log routing", recent_changes)
        self.assertIn("recent-changes-single-fish.md", recent_changes)
        self.assertIn("recent-changes-cohort.md", recent_changes)

    def test_cohort_stage_map_covers_cohort_workflow(self) -> None:
        cohort_map = _read(".agents/references/cohort-stage-map.md")
        self.assertIn("[cohort-build]", cohort_map)
        self.assertIn("cohort_outputs/multi_fish_56h_56g/", cohort_map)

    def test_symbol_index_includes_trusted_patterns(self) -> None:
        source = _read(".agents/references/symbol-index.md")
        self.assertIn("## Trusted patterns", source)
        self.assertIn("`context.py`: path/config normalization pattern.", source)
        self.assertIn("`stimulus.py`: explicit stage input/output pattern.", source)
        self.assertIn("`activity.py`: response/BPI stage-owned semantics pattern.", source)
        self.assertIn("`tools/`: general wrapper pattern only, not business-logic authority.", source)
        self.assertIn("`registrations/`: maintained manual registration entrypoints only", source)

    def test_cache_policy_includes_smallest_smoke_first_validation(self) -> None:
        source = _read(".agents/references/cache-rerun-policy.md")
        self.assertIn("Run the smallest relevant smoke or contract test first.", source)
        self.assertIn("Validate the first downstream consumer of the edited writer stage.", source)
        self.assertIn("Do not declare success from static reasoning alone.", source)

    def test_agentic_workflow_roadmap_tracks_operational_state(self) -> None:
        source = _read(".agents/references/agentic-workflow-roadmap.md")
        self.assertIn("living status board", source)
        self.assertIn("## Working", source)
        self.assertIn("## Broken Or Missing", source)
        self.assertIn("## Next Slice", source)
        self.assertIn("tools/single_fish_pipeline.py", source)

    def test_single_fish_pipeline_roadmap_records_l395_baseline_decision(self) -> None:
        source = _read(".agents/references/single-fish-pipeline-roadmap.md")
        self.assertIn("use the existing `L395_f11` staged outputs as the first control/baseline", source)
        self.assertIn("Preprocessing is intentionally out of scope for this slice", source)
        self.assertIn("audit-inputs --fish-id FISH_ID --local-root DATA_ROOT --strict --write-manifest", source)
        self.assertIn("stage-status --fish-id FISH_ID --local-root DATA_ROOT --strict --stage-name STAGE_NAME", source)
        self.assertIn("compare-staged --fish-id FISH_ID --local-root DATA_ROOT --strict --stage-name STAGE_NAME", source)
        self.assertIn("Target scaffold, not all currently runnable", source)
        self.assertIn("freeze-legacy-baseline` and `compare-legacy-baseline` now provide a first frozen-bundle surface", source)
        self.assertNotIn("Current `score-activity-bpi` / `export-canonical-tables` status:", source)
        self.assertIn("baseline `make-figures --strict` now runs", source)
        self.assertIn("compare-staged --stage-name make-figures", source)

    def test_symbol_index_includes_pipeline_manifest_persistence_surface(self) -> None:
        source = _read(".agents/references/symbol-index.md")
        self.assertIn("PersistedManifestStatus", source)
        self.assertIn("StageOutputSpec", source)
        self.assertIn("stage_manifest_path", source)
        self.assertIn("write_stage_manifest", source)
        self.assertIn("compare_persisted_manifest", source)
        self.assertIn("build_single_fish_compare_staged_manifest", source)
        self.assertIn("build_single_fish_compare_legacy_baseline_manifest", source)
        self.assertIn("build_single_fish_downstream_stage_manifest", source)
        self.assertIn("run_single_fish_freeze_legacy_baseline_stage", source)
        self.assertIn("compare_single_fish_legacy_baseline", source)
        self.assertIn("compare_single_fish_staged_outputs", source)
        self.assertIn("downstream_stage_names", source)
        self.assertIn("--write-manifest", source)
        self.assertIn("stage-status", source)
        self.assertIn("compare-staged", source)
        self.assertIn("freeze-legacy-baseline", source)
        self.assertIn("compare-legacy-baseline", source)

    def test_docs_describe_current_downstream_writer_stage_boundary(self) -> None:
        current_state = _read(".agents/references/current-state.md")
        stage_map = _read(".agents/references/notebook-stage-map.md")
        self.assertIn(
            "assign-hcr-identity`, `score-activity-bpi`, `export-canonical-tables`, `make-qa-report`, and `make-figures` now also have writer commands",
            current_state,
        )
        self.assertIn("Upstream preprocessing, registration, matching, and broader comparison commands remain declarative contracts only", current_state)
        self.assertIn("These stages now have package-owned writer commands plus read-only status/comparison surfaces", stage_map)
        self.assertIn("remaining migration work is stage-specific promotion of upstream writers", stage_map)
        for source in (current_state, stage_map):
            self.assertNotIn("staged CLI path now stages", source)
            self.assertNotIn("promotes staged identity/score outputs", source)


    def test_router_markdown_paths_resolve_from_repo_root(self) -> None:
        for router in (REPO_ROOT / ".agents" / "workflows").glob("*.md"):
            source = router.read_text()
            paths = {
                token
                for token in re.findall(r"`([^`]+\.md)`", source)
                if token.startswith((".agents/", "notebooks/", "legacy/"))
            }
            unresolved = sorted(path for path in paths if not (REPO_ROOT / path).is_file())
            self.assertEqual(unresolved, [], f"{router}: unresolved paths")

    def test_reference_classification_lists_every_reference(self) -> None:
        references = REPO_ROOT / ".agents" / "references"
        expected = {path.name for path in references.glob("*.md")} - {"README.md"}
        source = _read(".agents/references/README.md")
        listed = set(re.findall(r"^\| `([^/`]+\.md)` \|", source, re.MULTILINE))
        self.assertEqual(listed, expected)
        self.assertIn("## Authority order", source)
        self.assertIn("Historical and compatibility documents are never current scientific authority.", source)

    def test_router_covers_qc_replay_registration_and_legacy_tasks(self) -> None:
        top = _read(".agents/workflows/2pf-hcr-router.md")
        single = _read(".agents/workflows/2pf-hcr-single-fish-router.md")
        self.assertIn("notebooks/qc/", top)
        self.assertIn("notebooks/hcr_activity_replay_qa.ipynb", top)
        self.assertIn("registrations/", top)
        self.assertIn("legacy/", top)
        self.assertIn("QC notebook review or refactor", single)
        self.assertIn("HCR activity replay QA", single)
        self.assertIn("maintained manual registration utility", single)
        self.assertIn("explicit legacy comparison or reproduction", single)
        self.assertIn("legacy behavior is evidence, not current authority", single)

    def test_recent_change_query_is_repo_owned_and_runnable(self) -> None:
        script = REPO_ROOT / ".agents" / "scripts" / "query_recent_changes.py"
        self.assertTrue(script.is_file())
        completed = subprocess.run(
            [
                sys.executable,
                str(script),
                "--repo",
                "codeANTs",
                "--query",
                "external confocal registration",
                "--limit",
                "1",
            ],
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("recent-changes-single-fish.md", completed.stdout)
        self.assertIn("external confocal registration uses current rbest/rn names", completed.stdout)


if __name__ == "__main__":
    unittest.main()
