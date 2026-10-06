import contextlib
import importlib.util
import io
from pathlib import Path
import sys
import unittest


SCRIPT = Path(__file__).with_name("run_sqllogictest_checked.py")
SPEC = importlib.util.spec_from_file_location("scripts.checked_sqllogictest", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
checked = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(checked)


class CheckedRunnerTests(unittest.TestCase):
    def run_child(self, source: str) -> tuple[int, str]:
        output = io.StringIO()
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
            status = checked.run_command([sys.executable, "-c", source])
        return status, output.getvalue()

    def test_parser_error_with_zero_exit_is_failure(self) -> None:
        status, output = self.run_child("print('Parser Error: malformed.test:4: missing separator')")
        self.assertEqual(status, 1)
        self.assertIn("malformed.test", output)

    def test_parser_error_on_stderr_is_failure(self) -> None:
        status, _ = self.run_child("import sys; print('Parser Error: broken.test:1', file=sys.stderr)")
        self.assertEqual(status, 1)

    def test_ansi_colored_parser_error_is_failure(self) -> None:
        status, _ = self.run_child("print('\\x1b[31mParser Error: broken.test:1\\x1b[0m')")
        self.assertEqual(status, 1)

    def test_nonzero_child_status_is_preserved(self) -> None:
        for source in ["raise SystemExit(7)", "print('Parser Error: broken.test:1'); raise SystemExit(7)"]:
            status, _ = self.run_child(source)
            self.assertEqual(status, 7)

    def test_success_skips_and_expected_sql_errors_are_not_failures(self) -> None:
        status, output = self.run_child("print('SUCCESS'); print('SKIPPED'); print('Expected error: Parser Error: example')")
        self.assertEqual(status, 0)
        self.assertIn("SKIPPED", output)


if __name__ == "__main__":
    unittest.main()
