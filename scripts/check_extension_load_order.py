#!/usr/bin/env python3
"""Exercise spanner and DuckDB's core autocomplete extension in both orders."""

import argparse
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory


ORDERS = ("autocomplete-first", "spanner-first")


def assert_exact_duckdb_version(actual: str, expected: str) -> None:
    if actual != expected:
        raise RuntimeError(f"expected DuckDB {expected}, got {actual}")


def assert_metadata_validation_enabled(connection) -> None:
    value = connection.execute(
        "SELECT current_setting('allow_extensions_metadata_mismatch')"
    ).fetchone()
    if value not in ((False,), ("false",)):
        raise RuntimeError(f"metadata mismatch validation is not enabled: {value!r}")


def assert_autocomplete_loaded(connection) -> None:
    state = connection.execute(
        "SELECT installed, loaded, installed_from "
        "FROM duckdb_extensions() WHERE extension_name = 'autocomplete'"
    ).fetchone()
    if state is None or state[0:2] != (True, True) or state[2] != "core":
        raise RuntimeError(f"autocomplete was not loaded from DuckDB core: {state!r}")


def assert_both_extensions_usable(connection) -> None:
    connection.execute("CALL enable_peg_parser()")
    result = connection.execute(
        "SELECT interval_to_iso8601(INTERVAL '1 day')"
    ).fetchone()
    if result != ("P1D",):
        raise RuntimeError(f"spanner scalar was not usable after loading: {result!r}")


def load_spanner(connection, artifact: Path) -> None:
    escaped_path = artifact.as_posix().replace("'", "''")
    connection.execute(f"LOAD '{escaped_path}'")


def install_autocomplete(extension_directory: Path) -> None:
    import duckdb

    connection = duckdb.connect(config={"extension_directory": str(extension_directory)})
    try:
        connection.execute("INSTALL autocomplete")
    finally:
        connection.close()


def run_worker(artifact: Path, extension_directory: Path, order: str) -> None:
    import duckdb

    connection = duckdb.connect(
        config={
            "allow_unsigned_extensions": True,
            "autoload_known_extensions": False,
            "autoinstall_known_extensions": False,
            "extension_directory": str(extension_directory),
        }
    )
    try:
        assert_metadata_validation_enabled(connection)
        if order == "autocomplete-first":
            connection.execute("LOAD autocomplete")
            load_spanner(connection, artifact)
        elif order == "spanner-first":
            load_spanner(connection, artifact)
            connection.execute("LOAD autocomplete")
        else:
            raise RuntimeError(f"unknown load order: {order}")
        assert_autocomplete_loaded(connection)
        assert_both_extensions_usable(connection)
    finally:
        connection.close()


def worker_command(
    artifact: Path, extension_directory: Path, expected_version: str, order: str
) -> list[str]:
    return [
        sys.executable,
        str(Path(__file__).resolve()),
        str(artifact),
        "--expected-duckdb-version",
        expected_version,
        "--extension-directory",
        str(extension_directory),
        "--worker-order",
        order,
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=Path)
    parser.add_argument("--expected-duckdb-version", required=True)
    parser.add_argument("--extension-directory", type=Path)
    parser.add_argument("--worker-order", choices=ORDERS)
    args = parser.parse_args()

    artifact = args.artifact.resolve(strict=True)

    import duckdb

    assert_exact_duckdb_version(duckdb.__version__, args.expected_duckdb_version)
    if args.worker_order:
        if args.extension_directory is None:
            parser.error("--extension-directory is required in worker mode")
        run_worker(artifact, args.extension_directory.resolve(), args.worker_order)
        return

    with TemporaryDirectory(prefix="duckdb-spanner-load-order-") as directory:
        extension_directory = Path(directory)
        install_autocomplete(extension_directory)
        for order in ORDERS:
            subprocess.run(
                worker_command(
                    artifact,
                    extension_directory,
                    args.expected_duckdb_version,
                    order,
                ),
                check=True,
            )
            print(f"verified {order} load order in a fresh process")


if __name__ == "__main__":
    main()
