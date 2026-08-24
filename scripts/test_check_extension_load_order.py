import importlib.util
from pathlib import Path
from unittest import mock
import unittest


SCRIPT = Path(__file__).with_name("check_extension_load_order.py")
SPEC = importlib.util.spec_from_file_location("extension_load_order", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
load_order = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(load_order)


class ExactVersionTests(unittest.TestCase):
    def test_rejects_an_unexpected_host(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "expected DuckDB 1.5.5"):
            load_order.assert_exact_duckdb_version("1.5.4", "1.5.5")


class AutocompleteStateTests(unittest.TestCase):
    def test_requires_core_autocomplete_to_be_installed_and_loaded(self) -> None:
        class Connection:
            def execute(self, query):
                self.query = query
                return self

            def fetchone(self):
                return (True, True, "core")

        load_order.assert_autocomplete_loaded(Connection())

    def test_rejects_an_unloaded_autocomplete_extension(self) -> None:
        class Connection:
            def execute(self, query):
                return self

            def fetchone(self):
                return (True, False, "core")

        with self.assertRaisesRegex(RuntimeError, "autocomplete was not loaded"):
            load_order.assert_autocomplete_loaded(Connection())


class MetadataValidationTests(unittest.TestCase):
    def test_accepts_duckdb_python_boolean_false(self) -> None:
        class Connection:
            def execute(self, query):
                return self

            def fetchone(self):
                return (False,)

        load_order.assert_metadata_validation_enabled(Connection())

    def test_rejects_metadata_mismatch_opt_in(self) -> None:
        class Connection:
            def execute(self, query):
                return self

            def fetchone(self):
                return (True,)

        with self.assertRaisesRegex(RuntimeError, "metadata mismatch validation"):
            load_order.assert_metadata_validation_enabled(Connection())


class WorkerCommandTests(unittest.TestCase):
    def test_runs_the_same_script_in_explicit_worker_mode(self) -> None:
        artifact = Path("/tmp/spanner.duckdb_extension")
        extension_directory = Path("/tmp/extensions")
        with mock.patch.object(load_order.sys, "executable", "/tmp/python"):
            command = load_order.worker_command(
                artifact, extension_directory, "1.5.5", "spanner-first"
            )

        self.assertEqual(command[0], "/tmp/python")
        self.assertEqual(command[2], str(artifact))
        self.assertEqual(
            command[-6:],
            [
                "--expected-duckdb-version",
                "1.5.5",
                "--extension-directory",
                str(extension_directory),
                "--worker-order",
                "spanner-first",
            ],
        )


if __name__ == "__main__":
    unittest.main()
