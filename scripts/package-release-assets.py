#!/usr/bin/env python3
"""Package platform artifacts without changing the loadable extension filename."""

from argparse import ArgumentParser
from pathlib import Path
import re
import shutil
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo


EXTENSION_NAME = "spanner"
CANONICAL_FILENAME = f"{EXTENSION_NAME}.duckdb_extension"
DUCKDB_VERSION_PATTERN = re.compile(r"v[0-9]+\.[0-9]+\.[0-9]+")
PLATFORMS = (
    "linux_amd64",
    "linux_arm64",
    "osx_amd64",
    "osx_arm64",
    "windows_amd64",
    "windows_amd64_mingw",
)


def package_release_assets(
    input_root: Path, output_root: Path, duckdb_version: str
) -> list[Path]:
    if DUCKDB_VERSION_PATTERN.fullmatch(duckdb_version) is None:
        raise RuntimeError(f"invalid DuckDB version: {duckdb_version}")
    if output_root.exists():
        raise RuntimeError(f"output directory already exists: {output_root}")
    temporary_root = output_root.with_name(f".{output_root.name}.tmp")
    if temporary_root.exists():
        raise RuntimeError(f"temporary output directory already exists: {temporary_root}")
    temporary_root.mkdir()

    archives = []
    try:
        for platform in PLATFORMS:
            source = (
                input_root
                / f"{EXTENSION_NAME}-{duckdb_version}-extension-{platform}"
                / CANONICAL_FILENAME
            )
            if not source.is_file():
                raise RuntimeError(f"missing verified artifact for {platform}: {source}")

            extension_bytes = source.read_bytes()
            if not extension_bytes:
                raise RuntimeError(f"verified artifact is empty: {source}")

            archive = temporary_root / (
                f"{EXTENSION_NAME}-{duckdb_version}-{platform}.zip"
            )
            member = ZipInfo(CANONICAL_FILENAME, date_time=(1980, 1, 1, 0, 0, 0))
            member.compress_type = ZIP_DEFLATED
            member.create_system = 3
            member.external_attr = 0o100644 << 16
            with ZipFile(
                archive, mode="x", compression=ZIP_DEFLATED, compresslevel=9
            ) as zip_file:
                zip_file.writestr(member, extension_bytes)

            # Fail closed if packaging changed the bytes or added another member.
            with ZipFile(archive) as zip_file:
                if zip_file.namelist() != [CANONICAL_FILENAME]:
                    raise RuntimeError(f"unexpected archive members in {archive}")
                if zip_file.read(CANONICAL_FILENAME) != extension_bytes:
                    raise RuntimeError(f"archive payload differs from {source}")
            archives.append(archive)

        temporary_root.rename(output_root)
    except Exception:
        shutil.rmtree(temporary_root, ignore_errors=True)
        raise

    return [output_root / archive.name for archive in archives]


def main() -> None:
    parser = ArgumentParser()
    parser.add_argument("input_root", type=Path)
    parser.add_argument("output_root", type=Path)
    parser.add_argument("--duckdb-version", required=True)
    args = parser.parse_args()
    package_release_assets(args.input_root, args.output_root, args.duckdb_version)


if __name__ == "__main__":
    main()
