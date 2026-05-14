import unittest
from pathlib import Path
from unittest.mock import patch

from codeants_2pf_hcr.runtime import candidate_local_roots


class RuntimeTests(unittest.TestCase):
    def test_candidate_local_roots_includes_data_drive_before_home_defaults(self) -> None:
        with patch.dict("os.environ", {}, clear=True), patch("pathlib.Path.home", return_value=Path("/Users/example")):
            candidates = candidate_local_roots()

        self.assertEqual(candidates[0], Path("/Volumes/dataDrive/dataProcessing/2p_processing"))
        self.assertIn(Path("/Users/example/dataProcessing/2p_processing"), candidates)

    def test_candidate_local_roots_keeps_environment_overrides_first(self) -> None:
        with patch.dict(
            "os.environ",
            {"CODEANTS_2PF_HCR_LOCAL_ROOT": "/tmp/codeants-local-root"},
            clear=True,
        ), patch("pathlib.Path.home", return_value=Path("/Users/example")):
            candidates = candidate_local_roots()

        self.assertEqual(candidates[0], Path("/tmp/codeants-local-root"))
        self.assertEqual(candidates[1], Path("/Volumes/dataDrive/dataProcessing/2p_processing"))


if __name__ == "__main__":
    unittest.main()
