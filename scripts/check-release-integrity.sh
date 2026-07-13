#!/usr/bin/env bash
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

release_tag=""
readonly EXPECTED_WORKFLOW_REFS=1
readonly EXPECTED_CI_TOOLS_VERSIONS=1
while (($# > 0)); do
  case "$1" in
    --release-tag)
      if (($# < 2)); then
        echo "error: --release-tag requires a value" >&2
        exit 2
      fi
      release_tag=$2
      shift 2
      ;;
    *)
      echo "usage: $0 [--release-tag TAG]" >&2
      exit 2
      ;;
  esac
done

# Validate one coherent staged snapshot. Keep compatibility with macOS /bin/bash
# 3.2: mapfile/readarray require Bash 4, so read git's NUL-delimited index paths.
workflow_files=()
while IFS= read -r -d '' workflow; do
  workflow_files+=("$workflow")
done < <(git ls-files -z -- '.github/workflows/*.yml' '.github/workflows/*.yaml')
readonly -a workflow_files
if ((${#workflow_files[@]} == 0)); then
  echo "error: no GitHub Actions workflows found in the git index" >&2
  exit 1
fi

gitlink_commit="$(git ls-files -s -- extension-ci-tools | awk '$1 == "160000" && $4 == "extension-ci-tools" { print $2 }')"
if [[ ! "$gitlink_commit" =~ ^[0-9a-fA-F]{40}$ ]]; then
  echo "error: could not read the extension-ci-tools gitlink from the git index" >&2
  exit 1
fi

readonly full_sha_use_pattern="^[[:space:]]*(-[[:space:]]+)?uses:[[:space:]]+['\"]?[^@'\"[:space:]]+@[0-9a-fA-F]{40}['\"]?[[:space:]]+#.+$"
readonly local_ref_pattern="^[[:space:]]*['\"]?\.\.?/"
readonly ci_tools_workflow_pattern="^[[:space:]]*['\"]?duckdb/extension-ci-tools/\.github/workflows/_extension_distribution\.yml@([0-9a-fA-F]{40})['\"]?[[:space:]]+#.+$"
readonly ci_tools_version_pattern="^[[:space:]]*ci_tools_version:[[:space:]]*['\"]?([0-9a-fA-F]{40})['\"]?[[:space:]]*(#.*)?$"

invalid_uses=0
workflow_ref_count=0
ci_tools_version_count=0
for workflow in "${workflow_files[@]}"; do
  while IFS= read -r line; do
    use_ref=${line#*uses:}
    if [[ "$use_ref" =~ $local_ref_pattern ]]; then
      continue
    fi
    if [[ ! "$line" =~ $full_sha_use_pattern ]]; then
      echo "error: direct workflow use is not an annotated full SHA: $line" >&2
      invalid_uses=1
    fi
    if [[ "$use_ref" =~ $ci_tools_workflow_pattern ]]; then
      workflow_ref=${BASH_REMATCH[1]}
      if [[ "$workflow_ref" != "$gitlink_commit" ]]; then
        echo "error: reusable workflow ref $workflow_ref does not match extension-ci-tools gitlink $gitlink_commit" >&2
        exit 1
      fi
      ((workflow_ref_count += 1))
    fi
  done < <(git show ":$workflow" | grep -E '^[[:space:]]*(-[[:space:]]+)?uses:' || true)

  while IFS= read -r line; do
    if [[ ! "$line" =~ $ci_tools_version_pattern ]]; then
      echo "error: ci_tools_version is not an exact 40-character SHA value: $line" >&2
      exit 1
    fi
    ci_tools_version=${BASH_REMATCH[1]}
    if [[ "$ci_tools_version" != "$gitlink_commit" ]]; then
      echo "error: ci_tools_version $ci_tools_version does not match extension-ci-tools gitlink $gitlink_commit" >&2
      exit 1
    fi
    ((ci_tools_version_count += 1))
  done < <(git show ":$workflow" | grep -E '^[[:space:]]*ci_tools_version:' || true)
done

if ((invalid_uses)); then
  exit 1
fi

if ((workflow_ref_count != EXPECTED_WORKFLOW_REFS)); then
  echo "error: expected $EXPECTED_WORKFLOW_REFS pinned extension-ci-tools reusable workflow references, found $workflow_ref_count" >&2
  exit 1
fi

if ((ci_tools_version_count != EXPECTED_CI_TOOLS_VERSIONS)); then
  echo "error: expected $EXPECTED_CI_TOOLS_VERSIONS pinned ci_tools_version values, found $ci_tools_version_count" >&2
  exit 1
fi

if [[ -n "$release_tag" ]]; then
  package_version="$(
    awk '
      /^[[:space:]]*\[package\][[:space:]]*$/ { in_package=1; next }
      /^[[:space:]]*\[/ && in_package { exit }
      in_package && /^[[:space:]]*version[[:space:]]*=/ {
        line=$0
        sub(/^[^=]*=[[:space:]]*/, "", line)
        quote=substr(line, 1, 1)
        if (quote != "\"" && quote != "\047") { exit }
        line=substr(line, 2)
        end=index(line, quote)
        if (end == 0) { exit }
        print substr(line, 1, end - 1)
        exit
      }
    ' < <(git show :Cargo.toml)
  )"
  if [[ -z "$package_version" ]]; then
    echo "error: could not read the package version from Cargo.toml" >&2
    exit 1
  fi

  expected_tag="v${package_version}"
  if [[ "$release_tag" != "$expected_tag" ]]; then
    echo "error: release tag $release_tag does not match Cargo.toml package version $expected_tag" >&2
    exit 1
  fi
  echo "release tag $release_tag matches Cargo.toml package version"
fi

echo "repository release-integrity checks passed"
