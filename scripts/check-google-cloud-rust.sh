#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

readonly GOOGLE_CLOUD_RUST_SOURCE='git+https://github.com/googleapis/google-cloud-rust?rev='

metadata="$(cargo metadata --locked --no-deps --format-version=1)"
dependencies="$(
  jq -r --arg source_prefix "$GOOGLE_CLOUD_RUST_SOURCE" '
    .packages[]
    | select(.name == "duckdb-spanner")
    | .dependencies[]
    | select(.name | startswith("google-cloud-"))
    | select((.source // "") | startswith($source_prefix))
    | [.name, (.source | capture("rev=(?<revision>[^#]+)").revision)]
    | @tsv
  ' <<<"$metadata"
)"

if [[ -z "$dependencies" ]]; then
  echo "error: no direct google-cloud-rust dependencies found" >&2
  exit 1
fi

invalid_dependencies="$(
  jq -r --arg source_prefix "$GOOGLE_CLOUD_RUST_SOURCE" '
    .packages[]
    | select(.name == "duckdb-spanner")
    | .dependencies[]
    | select(.name | startswith("google-cloud-"))
    | select(((.source // "") | startswith($source_prefix)) | not)
    | [.name, (.source // "missing source")]
    | @tsv
  ' <<<"$metadata"
)"

if [[ -n "$invalid_dependencies" ]]; then
  echo "error: google-cloud-* dependencies must come from google-cloud-rust:" >&2
  printf '%s\n' "$invalid_dependencies" >&2
  exit 1
fi

revision_count="$(printf '%s\n' "$dependencies" | cut -f2 | sort -u | sed '/^$/d' | wc -l | tr -d ' ')"
if [[ "$revision_count" != "1" ]]; then
  echo "error: google-cloud-rust revisions are not aligned:" >&2
  printf '%s\n' "$dependencies" >&2
  exit 1
fi

# Check every target so a target-specific feature cannot break the MinGW build.
# Inspect the complete normal-edge tree: cargo tree --invert exits successfully
# with empty output when a package exists only outside the selected edge set.
normal_dependency_tree="$(
  cargo tree --locked --edges normal --target all --prefix none --format '{p}'
)"
for forbidden_package in aws-lc-rs aws-lc-sys; do
  matching_packages="$(
    grep -E "^${forbidden_package} v" <<<"$normal_dependency_tree" || true
  )"
  if [[ -n "$matching_packages" ]]; then
    echo "error: normal production dependencies contain $forbidden_package:" >&2
    printf '%s\n' "$matching_packages" >&2
    exit 1
  fi
done

# The Spanner crate now defaults to a background Cloud Monitoring exporter.
# Keep it and base64 0.23's unsafe SIMD implementation out of production builds.
normal_feature_tree="$(
  cargo tree --locked --edges normal,features --target all --prefix none --format '{p} {f}'
)"
for forbidden_feature in 'google-cloud-spanner v.*builtin-metrics' 'base64 v0\.23\.[^ ]+ .*simd-unsafe'; do
  if grep -E "^${forbidden_feature}" <<<"$normal_feature_tree"; then
    echo "error: normal production dependencies enable a forbidden feature: $forbidden_feature" >&2
    exit 1
  fi
done
if grep -E '^google-cloud-monitoring-v3 v' <<<"$normal_dependency_tree"; then
  echo "error: normal production dependencies contain Cloud Monitoring" >&2
  exit 1
fi

echo "google-cloud-rust revisions aligned; aws-lc, built-in metrics, and unsafe base64 SIMD are absent from normal dependencies"
