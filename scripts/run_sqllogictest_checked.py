#!/usr/bin/env python3
"""Preserve runner failures and reject parser errors even when it exits zero."""

import re
import subprocess
import sys


ANSI_ESCAPE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")


def run_command(command: list[str]) -> int:
    """Stream the runner diagnostics without retaining a whole suite log."""
    parser_error = False
    with subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, encoding="utf-8", errors="replace", bufsize=1,
    ) as process:
        assert process.stdout is not None
        for line in process.stdout:
            print(line, end="", flush=True)
            # The upstream CLI catches file parse errors, prints this reserved
            # diagnostic and can exit0 without running that file. Counting all
            # SUCCESS lines would incorrectly reject legitimate skipped tests.
            diagnostic = ANSI_ESCAPE.sub("", line).lstrip()
            if diagnostic.startswith("Parser Error:"):
                parser_error = True
        status = process.wait()
    if status:
        return status
    if parser_error:
        print("SQLLogicTest parser errors invalidate this run", file=sys.stderr)
        return 1
    return 0


def main() -> int:
    return run_command([sys.executable, "-m", "duckdb_sqllogictest", *sys.argv[1:]])


if __name__ == "__main__":
    raise SystemExit(main())
