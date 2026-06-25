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
        self.assertIn("Do **not** use `tools/` as business-logic authority.", single)
        self.assertIn("references/cohort-stage-map.md", cohort)
        self.assertIn("references/recent-changes-cohort.md", cohort)
        self.assertIn("Do **not** use `tools/` as business-logic authority.", cohort)

    def test_refactor_rules_define_ownership_and_edit_scope(self) -> None:
        source = _read(".agents/references/refactor-rules.md")
        self.assertIn("Notebook orchestration lives in notebook cells only.", source)
        self.assertIn("Reusable logic lives in `src/codeants_2pf_hcr/`.", source)
        self.assertIn("CLI behavior lives in `tools/` wrappers only", source)
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
        self.assertIn("`tools/`: wrapper pattern only, not business-logic authority.", source)

    def test_cache_policy_includes_smallest_smoke_first_validation(self) -> None:
        source = _read(".agents/references/cache-rerun-policy.md")
        self.assertIn("Run the smallest relevant smoke or contract test first.", source)
        self.assertIn("Validate the first downstream consumer of the edited writer stage.", source)
        self.assertIn("Do not declare success from static reasoning alone.", source)


if __name__ == "__main__":
    unittest.main()
