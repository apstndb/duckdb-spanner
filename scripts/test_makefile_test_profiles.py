from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile
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
    def test_version_file_can_be_created_before_configure_directory_exists(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copy2(REPOSITORY_ROOT / "Cargo.toml", root / "Cargo.toml")
            relative = Path("extension-ci-tools/makefiles/c_api_extensions")
            (root / relative).mkdir(parents=True)
            for name in ["base.Makefile", "rust.Makefile"]:
                shutil.copy2(REPOSITORY_ROOT / relative / name, root / relative / name)
            subprocess.run(
                ["make", "-f", str(REPOSITORY_ROOT / "Makefile"), "extension_version"],
                cwd=root, check=True, capture_output=True, text=True,
            )
            expected = next(
                line.split('"')[1] for line in (root / "Cargo.toml").read_text().splitlines()
                if line.startswith("version = ")
            )
            self.assertEqual((root / "configure/extension_version.txt").read_text().strip(), expected)

    def test_parallel_configure_waits_for_the_pinned_host_before_platform(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            venv_ready = root / "venv-ready"
            host_ready = root / "host-ready"

            def python_command(source: str) -> str:
                return f"{shlex.quote(sys.executable)} -c {shlex.quote(source)}"

            venv = python_command(
                f"import time; from pathlib import Path; time.sleep(0.1); "
                f"Path({str(venv_ready)!r}).touch()"
            )
            pin = python_command(
                f"from pathlib import Path; assert Path({str(venv_ready)!r}).exists(); "
                f"Path({str(host_ready)!r}).touch()"
            )
            platform = python_command(
                f"from pathlib import Path; assert Path({str(host_ready)!r}).exists()"
            )
            overrides = root / "overrides.mk"
            # Replace external operations while exercising the real Makefile's
            # dependency graph. A missing platform edge fails deterministically.
            overrides.write_text(
                "configure/venv:; @:\n"
                "configure/extension_version.txt:; @:\n"
                f"venv:\n\t@{venv}\n"
                f"ensure-pinned-duckdb-test-host:\n\t@{pin}\n"
                f"platform:\n\t@{platform}\n"
                "extension_version:; @:\n"
            )
            subprocess.run(
                ["make", "-j4", "-f", "Makefile", "-f", str(overrides), "configure"],
                cwd=REPOSITORY_ROOT, check=True, capture_output=True, text=True,
            )

    def test_fresh_configure_pins_the_compiled_host_despite_an_override(self) -> None:
        output = subprocess.run(
            ["make", "-Bn", "configure", "DUCKDB_TEST_VERSION=2.0.0"],
            cwd=REPOSITORY_ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        self.assertIn("-m pip install duckdb==1.5.6", output)
        self.assertNotIn("-m pip install duckdb==2.0.0", output)

    def test_every_runner_repairs_a_reused_virtualenv_before_execution(self) -> None:
        for target, offline in [
            ("test_extension_release_internal", False),
            ("test_extension_release_internal", True),
            ("test_extension_debug_internal", False),
            ("test_extension_release_peg_parser_internal", False),
        ]:
            with self.subTest(target=target, offline=offline):
                command = ["make", "-n", target]
                if offline:
                    command.append("DUCKDB_SPANNER_OFFLINE_TESTS=1")
                output = subprocess.run(
                    command, cwd=REPOSITORY_ROOT, check=True,
                    capture_output=True, text=True,
                ).stdout
                pin = '-m pip install --disable-pip-version-check "duckdb==1.5.6"'
                runner = ("scripts/run_sqllogictest_with_peg_parser.py" if "peg" in target
                          else "scripts/run_sqllogictest_checked.py")
                self.assertLess(output.index(pin), output.index(runner))

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
