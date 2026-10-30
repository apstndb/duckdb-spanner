#!/usr/bin/env python3
"""Run an exact mirror of the SQLLogicTest suite with DuckDB's PEG parser."""

import argparse
from pathlib import Path
import sys
from tempfile import TemporaryDirectory

if __package__:
    from .run_sqllogictest_checked import run_command
else:
    from run_sqllogictest_checked import run_command


PEG_PREAMBLE = b"""statement ok
SET autoinstall_known_extensions = false;

statement ok
CALL enable_peg_parser();

"""


def mirror_test_suite(source_root: Path, destination_root: Path, *, default_parser: bool = False) -> int:
    """Mirror every test file and prepend the requested parser profile."""
    source_files = sorted(source_root.rglob("*.test"))
    if not source_files:
        raise RuntimeError(f"no SQLLogicTest files found under {source_root}")

    for source in source_files:
        relative = source.relative_to(source_root)
        destination = destination_root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        preamble = b"statement ok\nSET autoinstall_known_extensions = false;\n\n" if default_parser else PEG_PREAMBLE
        destination.write_bytes(preamble + source.read_bytes())

    mirrored = {
        path.relative_to(destination_root) for path in destination_root.rglob("*.test")
    }
    expected = {path.relative_to(source_root) for path in source_files}
    if mirrored != expected:
        raise RuntimeError(
            f"PEG suite does not mirror the source file set: expected={expected!r}, "
            f"mirrored={mirrored!r}"
        )
    return len(source_files)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--test-dir", required=True, type=Path)
    parser.add_argument("--external-extension", required=True, type=Path)
    parser.add_argument("--default-parser", action="store_true", help="DuckDB 2 uses PEG by default; do not call the removed 1.x activation procedure")
    args = parser.parse_args()

    source_root = args.test_dir.resolve(strict=True)
    artifact = args.external_extension.resolve(strict=True)
    with TemporaryDirectory(prefix="duckdb-spanner-peg-suite-") as directory:
        mirror_root = Path(directory)
        file_count = mirror_test_suite(source_root, mirror_root, default_parser=args.default_parser)
        print(f"running PEG mirror of {file_count} SQLLogicTest files")
        status = run_command(
            [
                sys.executable,
                "-m",
                "duckdb_sqllogictest",
                "--test-dir",
                str(mirror_root),
                "--external-extension",
                str(artifact),
                "--preinstall-extensions",
                "autocomplete",
            ]
        )
        if status:
            raise SystemExit(status)


if __name__ == "__main__":
    main()
