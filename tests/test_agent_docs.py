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

    def test_router_contains_strict_read_order_and_never_first_rules(self) -> None:
        source = _read(".agents/workflows/2pf-hcr-router.md")
        self.assertIn("Read this router first.", source)
        self.assertIn("Open `symbol-index.md` only if symbol lookup is needed.", source)
        self.assertIn("Open `notebook-stage-map.md` only if stage ownership or cell mapping is still unclear.", source)
        self.assertIn("Do **not** open large notebook regions first.", source)
        self.assertIn("Do **not** use `tools/` as business-logic authority.", source)
        self.assertIn("Do **not** infer semantics from downstream figures before reading the writer stage.", source)

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
        self.assertIn("rolling manual handoff log", recent_changes)
        self.assertIn("Update template", recent_changes)

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
