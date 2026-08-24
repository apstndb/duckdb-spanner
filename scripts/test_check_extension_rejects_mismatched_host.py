import importlib.util
from pathlib import Path
import unittest


SCRIPT = Path(__file__).with_name("check_extension_rejects_mismatched_host.py")
SPEC = importlib.util.spec_from_file_location("mismatched_host", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
mismatched_host = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mismatched_host)


class VersionRejectionTests(unittest.TestCase):
    def test_requires_both_exact_versions_in_the_loader_error(self) -> None:
        mismatched_host.assert_version_rejection(
            RuntimeError("extension v1.5.5 does not match host v1.5.4"),
            "1.5.5",
            "1.5.4",
        )

    def test_rejects_an_unrelated_load_error(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "exact version mismatch"):
            mismatched_host.assert_version_rejection(
                RuntimeError("could not open shared object file"), "1.5.5", "1.5.4"
            )


class InitializationTests(unittest.TestCase):
    def test_requires_no_spanner_registration_after_rejection(self) -> None:
        class Connection:
            def execute(self, query):
                return self

            def fetchall(self):
                return []

        mismatched_host.assert_spanner_uninitialized(Connection())

    def test_rejects_visible_spanner_registration(self) -> None:
        class Connection:
            def __init__(self):
                self.calls = 0

            def execute(self, query):
                self.calls += 1
                return self

            def fetchall(self):
                return [("spanner_project",)] if self.calls == 1 else []

        with self.assertRaisesRegex(RuntimeError, "registration is visible"):
            mismatched_host.assert_spanner_uninitialized(Connection())


if __name__ == "__main__":
    unittest.main()
