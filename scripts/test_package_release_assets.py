import importlib.util
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from zipfile import ZipFile


SCRIPT = Path(__file__).with_name("package-release-assets.py")
SPEC = importlib.util.spec_from_file_location("package_release_assets", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
packaging = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(packaging)
TEST_DUCKDB_VERSION = "v9.8.7"


class PackageReleaseAssetsTests(unittest.TestCase):
    def create_inputs(self, root: Path) -> dict[str, bytes]:
        payloads = {}
        for platform in packaging.PLATFORMS:
            payload = f"extension bytes for {platform}".encode()
            payloads[platform] = payload
            artifact = (
                root
                / f"spanner-{TEST_DUCKDB_VERSION}-extension-{platform}"
                / packaging.CANONICAL_FILENAME
            )
            artifact.parent.mkdir(parents=True)
            artifact.write_bytes(payload)
        return payloads

    def test_packages_exact_bytes_under_canonical_name(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            payloads = self.create_inputs(root / "input")
            archives = packaging.package_release_assets(
                root / "input", root / "output", TEST_DUCKDB_VERSION
            )

            self.assertEqual(len(archives), len(packaging.PLATFORMS))
            for platform, archive in zip(packaging.PLATFORMS, archives, strict=True):
                self.assertEqual(
                    archive.name, f"spanner-{TEST_DUCKDB_VERSION}-{platform}.zip"
                )
                with ZipFile(archive) as zip_file:
                    self.assertEqual(
                        zip_file.namelist(), [packaging.CANONICAL_FILENAME]
                    )
                    self.assertEqual(
                        zip_file.read(packaging.CANONICAL_FILENAME), payloads[platform]
                    )

    def test_output_is_deterministic(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            self.create_inputs(root / "input")
            first = packaging.package_release_assets(
                root / "input", root / "first", TEST_DUCKDB_VERSION
            )
            second = packaging.package_release_assets(
                root / "input", root / "second", TEST_DUCKDB_VERSION
            )
            self.assertEqual(
                [archive.read_bytes() for archive in first],
                [archive.read_bytes() for archive in second],
            )

    def test_rejects_missing_platform(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            self.create_inputs(root / "input")
            missing = (
                root
                / "input"
                / f"spanner-{TEST_DUCKDB_VERSION}-extension-osx_arm64"
                / packaging.CANONICAL_FILENAME
            )
            missing.unlink()
            with self.assertRaisesRegex(RuntimeError, "missing verified artifact"):
                packaging.package_release_assets(
                    root / "input", root / "output", TEST_DUCKDB_VERSION
                )
            self.assertFalse((root / "output").exists())
            self.assertFalse((root / ".output.tmp").exists())

    def test_rejects_existing_output_directory(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            self.create_inputs(root / "input")
            (root / "output").mkdir()
            with self.assertRaisesRegex(RuntimeError, "output directory already exists"):
                packaging.package_release_assets(
                    root / "input", root / "output", TEST_DUCKDB_VERSION
                )

    def test_rejects_existing_temporary_output_directory(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            self.create_inputs(root / "input")
            (root / ".output.tmp").mkdir()
            with self.assertRaisesRegex(
                RuntimeError, "temporary output directory already exists"
            ):
                packaging.package_release_assets(
                    root / "input", root / "output", TEST_DUCKDB_VERSION
                )

    def test_rejects_invalid_duckdb_version(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            self.create_inputs(root / "input")
            with self.assertRaisesRegex(RuntimeError, "invalid DuckDB version"):
                packaging.package_release_assets(
                    root / "input", root / "output", "../../unexpected"
                )


if __name__ == "__main__":
    unittest.main()
