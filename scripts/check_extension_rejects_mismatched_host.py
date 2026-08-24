#!/usr/bin/env python3
"""Prove a C_STRUCT_UNSTABLE artifact is rejected before spanner initialization."""

import argparse
from pathlib import Path


def assert_exact_duckdb_version(actual: str, expected: str) -> None:
    if actual != expected:
        raise RuntimeError(f"expected DuckDB {expected}, got {actual}")


def assert_version_rejection(error: Exception, artifact_version: str, host_version: str) -> None:
    message = str(error)
    if artifact_version not in message or host_version not in message:
        raise RuntimeError(f"LOAD did not report the exact version mismatch: {message}") from error


def assert_spanner_uninitialized(connection) -> None:
    settings = connection.execute(
        "SELECT name FROM duckdb_settings() WHERE name LIKE 'spanner_%'"
    ).fetchall()
    functions = connection.execute(
        "SELECT function_name FROM duckdb_functions() WHERE function_name LIKE 'spanner_%'"
    ).fetchall()
    if settings or functions:
        raise RuntimeError(
            "spanner registration is visible after a metadata rejection; "
            f"settings={settings!r}, functions={functions!r}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=Path)
    parser.add_argument("--expected-host-version", required=True)
    parser.add_argument("--expected-artifact-version", required=True)
    args = parser.parse_args()

    artifact = args.artifact.resolve(strict=True)

    import duckdb

    assert_exact_duckdb_version(duckdb.__version__, args.expected_host_version)
    connection = duckdb.connect(config={"allow_unsigned_extensions": True})
    try:
        metadata_mismatch = connection.execute(
            "SELECT current_setting('allow_extensions_metadata_mismatch')"
        ).fetchone()
        if metadata_mismatch not in ((False,), ("false",)):
            raise RuntimeError(
                "negative loader test would bypass metadata validation: "
                f"{metadata_mismatch!r}"
            )

        escaped_path = artifact.as_posix().replace("'", "''")
        try:
            connection.execute(f"LOAD '{escaped_path}'")
        except duckdb.Error as error:
            assert_version_rejection(
                error, args.expected_artifact_version, args.expected_host_version
            )
        else:
            raise RuntimeError("mismatched C_STRUCT_UNSTABLE artifact unexpectedly loaded")
        assert_spanner_uninitialized(connection)
    finally:
        connection.close()

    print(
        "rejected DuckDB "
        f"{args.expected_artifact_version} artifact on {args.expected_host_version} before initialization"
    )


if __name__ == "__main__":
    main()
