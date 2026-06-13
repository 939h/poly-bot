import shutil
import subprocess
import tempfile
import unittest

import momentum_v3


class DashboardIntegrityTests(unittest.TestCase):
    def test_dashboard_has_valid_title_and_golden_rvol_state(self):
        html = momentum_v3._DASHBOARD_HTML
        self.assertIn("<title>MomentumBot</title>", html)
        self.assertIn("goldenRvol=s.golden_rvol||{}", html)
        self.assertNotIn("<ttitle", html)

    def test_dashboard_contains_no_removed_optimizer_javascript(self):
        html = momentum_v3._DASHBOARD_HTML
        for orphan in ("otc.min_rvol", "oto.readiness_reason", "ratioStats", "o.candidate_count"):
            self.assertNotIn(orphan, html)

    @unittest.skipUnless(shutil.which("node"), "node is required for JavaScript syntax validation")
    def test_dashboard_javascript_syntax(self):
        html = momentum_v3._DASHBOARD_HTML
        script = html.split("<script>", 1)[1].split("</script>", 1)[0]
        with tempfile.NamedTemporaryFile("w", suffix=".js") as file:
            file.write(script)
            file.flush()
            result = subprocess.run(
                ["node", "--check", file.name],
                capture_output=True,
                text=True,
                check=False,
            )
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
