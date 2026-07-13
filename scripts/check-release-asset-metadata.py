#!/usr/bin/env python3
"""Validate the fixed DuckDB extension metadata footer without loading it."""

from pathlib import Path
import os
import sys


FOOTER_SIZE = 512
FIELD_SIZE = 32
# Offsets follow the pinned extension-ci-tools append_extension_metadata.py:
# three unused fields, then ABI, extension version, DuckDB version, and platform.
FIELDS = {
    "abi": (96, "C_STRUCT_UNSTABLE"),
    "extension_version": (128, None),
    "duckdb_version": (160, "v1.5.4"),
    "platform": (192, "windows_amd64_mingw"),
}


def footer_field(footer: bytes, offset: int) -> str:
    raw = footer[offset : offset + FIELD_SIZE].split(b"\0", 1)[0]
    return raw.decode("ascii")


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit(f"usage: {sys.argv[0]} ARTIFACT")

    artifact = Path(sys.argv[1]).resolve(strict=True)
    if artifact.stat().st_size <= FOOTER_SIZE:
        raise RuntimeError(f"release artifact is too small: {artifact.stat().st_size} bytes")

    release_tag = os.environ.get("EXPECTED_EXTENSION_TAG", "")
    if not release_tag.startswith("v") or len(release_tag) == 1:
        raise RuntimeError("EXPECTED_EXTENSION_TAG must be a v-prefixed release tag")
    expected = dict(FIELDS)
    expected["extension_version"] = (128, release_tag[1:])

    with artifact.open("rb") as stream:
        stream.seek(-FOOTER_SIZE, 2)
        footer = stream.read(FOOTER_SIZE)

    actual = {name: footer_field(footer, offset) for name, (offset, _) in expected.items()}
    mismatches = {
        name: {"expected": value, "actual": actual[name]}
        for name, (_, value) in expected.items()
        if actual[name] != value
    }
    if mismatches:
        raise RuntimeError(f"unexpected DuckDB extension metadata: {mismatches}")

    print(f"validated {artifact.name} metadata: {actual}")


if __name__ == "__main__":
    main()
