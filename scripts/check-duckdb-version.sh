#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
	echo "usage: $0 TARGET_VERSION METADATA_VERSION [DETECTED_CLI_VERSION]" >&2
	exit 2
fi

validate_version() {
	local version="${1#v}"
	[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]
}

# Validate before normalization so Bash 3.2 error handling does not depend on
# command-substitution assignment status under errexit.
if ! validate_version "$1"; then
	echo "error: invalid compiled DuckDB target version '$1'" >&2
	exit 2
fi
target="v${1#v}"
if ! validate_version "$2"; then
	echo "error: invalid DuckDB metadata version '$2'; expected vMAJOR.MINOR.PATCH" >&2
	exit 1
fi
metadata="v${2#v}"

if [[ "$metadata" != "$target" ]]; then
	echo "error: DuckDB ABI version mismatch: metadata version $metadata does not match compiled target $target (C_STRUCT_UNSTABLE)." >&2
	echo "Use DuckDB $target; a metadata override cannot make a binary built for another version safe." >&2
	exit 1
fi

if [[ $# -eq 3 && -n "$3" ]]; then
	if ! validate_version "$3"; then
		echo "error: invalid detected DuckDB CLI version '$3'; expected vMAJOR.MINOR.PATCH" >&2
		exit 1
	fi
	detected="v${3#v}"
	if [[ "$detected" != "$target" ]]; then
		echo "error: DuckDB ABI version mismatch: detected DuckDB CLI $detected does not match compiled target $target (C_STRUCT_UNSTABLE)." >&2
		echo "Use a $target CLI, or set DUCKDB_BIN to a CLI that reports $target; DUCKDB_VERSION cannot retarget the compiled binary." >&2
		exit 1
	fi
fi

echo "DuckDB ABI version verified: $target"
