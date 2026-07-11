#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
	echo "usage: $0 TARGET_VERSION METADATA_VERSION [DETECTED_CLI_VERSION]" >&2
	exit 2
fi

normalize_version() {
	local version="${1#v}"
	if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
		return 1
	fi
	printf 'v%s\n' "$version"
}

# These assignments are guarded OR-list commands. Bash 3.2 therefore defers
# errexit until the explicit diagnostic handler has had a chance to run.
target="$(normalize_version "$1")" || {
	echo "error: invalid compiled DuckDB target version '$1'" >&2
	exit 2
}
metadata="$(normalize_version "$2")" || {
	echo "error: invalid DuckDB metadata version '$2'; expected vMAJOR.MINOR.PATCH" >&2
	exit 1
}

if [[ "$metadata" != "$target" ]]; then
	echo "error: DuckDB ABI version mismatch: metadata version $metadata does not match compiled target $target (C_STRUCT_UNSTABLE)." >&2
	echo "Use DuckDB $target; a metadata override cannot make a binary built for another version safe." >&2
	exit 1
fi

if [[ $# -eq 3 && -n "$3" ]]; then
	detected="$(normalize_version "$3")" || {
		echo "error: invalid detected DuckDB CLI version '$3'; expected vMAJOR.MINOR.PATCH" >&2
		exit 1
	}
	if [[ "$detected" != "$target" ]]; then
		echo "error: DuckDB ABI version mismatch: detected DuckDB CLI $detected does not match compiled target $target (C_STRUCT_UNSTABLE)." >&2
		echo "Use a $target CLI, or set DUCKDB_BIN to a CLI that reports $target; DUCKDB_VERSION cannot retarget the compiled binary." >&2
		exit 1
	fi
fi

echo "DuckDB ABI version verified: $target"
