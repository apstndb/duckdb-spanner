#!/usr/bin/env python3
"""Package platform artifacts without changing the loadable extension filename."""

from argparse import ArgumentParser
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo


DUCKDB_VERSION = "v1.5.4"
EXTENSION_NAME = "spanner"
CANONICAL_FILENAME = f"{EXTENSION_NAME}.duckdb_extension"
PLATFORMS = (
    "linux_amd64",
    "linux_arm64",
    "osx_amd64",
    "osx_arm64",
    "windows_amd64",
    "windows_amd64_mingw",
)


def package_release_assets(input_root: Path, output_root: Path) -> list[Path]:
    if output_root.exists():
        raise RuntimeError(f"output directory already exists: {output_root}")
    output_root.mkdir()

    archives = []
    for platform in PLATFORMS:
        source = (
            input_root
            / f"{EXTENSION_NAME}-{DUCKDB_VERSION}-extension-{platform}"
            / CANONICAL_FILENAME
        )
        if not source.is_file():
            raise RuntimeError(f"missing verified artifact for {platform}: {source}")

        extension_bytes = source.read_bytes()
        if not extension_bytes:
            raise RuntimeError(f"verified artifact is empty: {source}")

        archive = output_root / f"{EXTENSION_NAME}-{DUCKDB_VERSION}-{platform}.zip"
        member = ZipInfo(CANONICAL_FILENAME, date_time=(1980, 1, 1, 0, 0, 0))
        member.compress_type = ZIP_DEFLATED
        member.create_system = 3
        member.external_attr = 0o100644 << 16
        with ZipFile(
            archive, mode="x", compression=ZIP_DEFLATED, compresslevel=9
        ) as zip_file:
            zip_file.writestr(member, extension_bytes)

        # Fail closed if packaging changed the bytes or introduced another member.
        with ZipFile(archive) as zip_file:
            if zip_file.namelist() != [CANONICAL_FILENAME]:
                raise RuntimeError(f"unexpected archive members in {archive}")
            if zip_file.read(CANONICAL_FILENAME) != extension_bytes:
                raise RuntimeError(f"archive payload differs from {source}")
        archives.append(archive)

    return archives


def main() -> None:
    parser = ArgumentParser()
    parser.add_argument("input_root", type=Path)
    parser.add_argument("output_root", type=Path)
    args = parser.parse_args()
    package_release_assets(args.input_root, args.output_root)


if __name__ == "__main__":
    main()
