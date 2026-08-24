import importlib.util
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest


SCRIPT = Path(__file__).with_name("run_sqllogictest_with_peg_parser.py")
SPEC = importlib.util.spec_from_file_location("peg_sqllogictest", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
peg_sqllogictest = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(peg_sqllogictest)


class MirrorTestSuiteTests(unittest.TestCase):
    def test_mirrors_nested_suite_and_prepends_peg_setup(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            destination = root / "destination"
            (source / "nested").mkdir(parents=True)
            (source / "first.test").write_bytes(b"query I\nSELECT 1;\n")
            (source / "nested" / "second.test").write_bytes(
                b"statement ok\nSELECT 2;\n"
            )

            count = peg_sqllogictest.mirror_test_suite(source, destination)

            self.assertEqual(count, 2)
            self.assertEqual(
                {path.relative_to(destination) for path in destination.rglob("*.test")},
                {Path("first.test"), Path("nested/second.test")},
            )
            self.assertEqual(
                (destination / "first.test").read_bytes(),
                peg_sqllogictest.PEG_PREAMBLE + b"query I\nSELECT 1;\n",
            )

    def test_rejects_an_empty_suite(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(RuntimeError, "no SQLLogicTest files"):
                peg_sqllogictest.mirror_test_suite(root, root / "destination")


if __name__ == "__main__":
    unittest.main()
