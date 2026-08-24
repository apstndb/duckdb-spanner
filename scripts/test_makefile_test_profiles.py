from pathlib import Path
import subprocess
import unittest


REPOSITORY_ROOT = Path(__file__).resolve().parent.parent


def dry_run_test_profile(offline: bool) -> str:
    command = ["make", "-n"]
    if offline:
        command.append("DUCKDB_SPANNER_OFFLINE_TESTS=1")
    command.append("test_extension_release_internal")
    return subprocess.run(
        command,
        cwd=REPOSITORY_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout


class MakefileTestProfileTests(unittest.TestCase):
    def test_community_profile_selects_only_the_offline_smoke(self) -> None:
        output = dry_run_test_profile(offline=True)
        self.assertIn("spanner_smoke.test", output)
        self.assertNotIn("tests/setup_sqllogic_db.sh", output)
        self.assertNotIn("docker run", output)

    def test_default_profile_retains_the_emulator_suite(self) -> None:
        output = dry_run_test_profile(offline=False)
        self.assertIn("tests/setup_sqllogic_db.sh", output)
        self.assertIn("docker run", output)
        self.assertNotIn("--file-path", output)


if __name__ == "__main__":
    unittest.main()
