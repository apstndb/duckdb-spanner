#!/usr/bin/env python3
"""Load and exercise one exact duckdb-spanner release artifact."""

from pathlib import Path
import sys

import duckdb


EXPECTED_SPANNER_FUNCTIONS = {
    "spanner_ddl",
    "spanner_ddl_async",
    "spanner_ddl_async_raw",
    "spanner_ddl_raw",
    "spanner_operations",
    "spanner_operations_raw",
    "spanner_params",
    "spanner_query",
    "spanner_query_raw",
    "spanner_scan",
    "spanner_tables",
    "spanner_typed",
    "spanner_value",
}


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit(f"usage: {sys.argv[0]} ARTIFACT")

    artifact = Path(sys.argv[1]).resolve(strict=True)
    escaped_path = artifact.as_posix().replace("'", "''")

    signed_only_connection = duckdb.connect()
    try:
        signed_only_connection.execute(f"LOAD '{escaped_path}'")
    except duckdb.Error as error:
        message = str(error).lower()
        # Both markers are required so an unrelated LOAD failure cannot pass.
        if "signature" not in message or "unsigned extension" not in message:
            raise RuntimeError(f"unexpected signed-only LOAD failure: {error}") from error
    else:
        raise RuntimeError("unsigned release artifact unexpectedly loaded without opt-in")
    finally:
        signed_only_connection.close()

    connection = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    connection.execute(f"LOAD '{escaped_path}'")

    interval = connection.execute(
        "SELECT interval_to_iso8601(INTERVAL '1 day')"
    ).fetchone()
    if interval != ("P1D",):
        raise RuntimeError(f"unexpected interval_to_iso8601 result: {interval!r}")

    registered = {
        row[0]
        for row in connection.execute(
            "SELECT DISTINCT function_name FROM duckdb_functions() "
            "WHERE function_name LIKE 'spanner%'"
        ).fetchall()
    }
    # Exact equality is intentional: partial or unexpected registration fails closed.
    if registered != EXPECTED_SPANNER_FUNCTIONS:
        missing = sorted(EXPECTED_SPANNER_FUNCTIONS - registered)
        extra = sorted(registered - EXPECTED_SPANNER_FUNCTIONS)
        raise RuntimeError(
            f"unexpected registered function surface; missing={missing}, extra={extra}"
        )

    print(f"loaded {artifact.name} with DuckDB {duckdb.__version__}")


if __name__ == "__main__":
    main()
