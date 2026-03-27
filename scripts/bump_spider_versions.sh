#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

usage() {
    cat <<'EOF'
Usage:
  scripts/bump_spider_versions.sh patch
  scripts/bump_spider_versions.sh minor
  scripts/bump_spider_versions.sh major
  scripts/bump_spider_versions.sh <version>
  scripts/bump_spider_versions.sh --dry-run <patch|minor|major|version>

Description:
  Bump all package versions whose crate name starts with "spider-",
  then synchronize internal dependency versions across the workspace.

Examples:
  scripts/bump_spider_versions.sh patch
  scripts/bump_spider_versions.sh minor
  scripts/bump_spider_versions.sh 3.1.0
  scripts/bump_spider_versions.sh --dry-run patch
EOF
}

log() {
    printf '%s\n' "$1"
}

err() {
    printf 'Error: %s\n' "$1" >&2
}

is_semver() {
    [[ "$1" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]
}

bump_semver() {
    local version="$1"
    local part="$2"
    local major minor patch

    IFS=. read -r major minor patch <<EOF
$version
EOF

    case "$part" in
        major)
            printf '%s.0.0\n' "$((major + 1))"
            ;;
        minor)
            printf '%s.%s.0\n' "$major" "$((minor + 1))"
            ;;
        patch)
            printf '%s.%s.%s\n' "$major" "$minor" "$((patch + 1))"
            ;;
        *)
            err "Unsupported bump type: $part"
            exit 1
            ;;
    esac
}

extract_field() {
    local field="$1"
    local file="$2"

    awk -F'"' -v field="$field" '
        /^\[package\]/ { in_package = 1; next }
        /^\[/ && $0 != "[package]" { if (in_package) exit }
        in_package && $1 ~ "^[[:space:]]*" field "[[:space:]]*=" {
            print $2
            exit
        }
    ' "$file"
}

update_package_version() {
    local file="$1"
    local new_version="$2"
    local tmp

    tmp="$(mktemp)"

    awk -v new_version="$new_version" '
        /^\[package\]/ { in_package = 1; print; next }
        /^\[/ && $0 != "[package]" { in_package = 0; print; next }
        in_package && /^[[:space:]]*version[[:space:]]*=/ {
            sub(/"[^"]*"/, "\"" new_version "\"")
            print
            next
        }
        { print }
    ' "$file" > "$tmp"

    mv "$tmp" "$file"
}

DRY_RUN=0

if [[ $# -eq 0 ]]; then
    usage
    exit 1
fi

if [[ "${1:-}" == "--dry-run" ]]; then
    DRY_RUN=1
    shift
fi

if [[ $# -ne 1 ]]; then
    usage
    exit 1
fi

TARGET="$1"

cd "$PROJECT_ROOT"

mapfile -t CARGO_FILES < <(find . -maxdepth 2 -name Cargo.toml | sort)

declare -a TARGET_FILES=()
declare -A CURRENT_VERSIONS=()

for cargo_file in "${CARGO_FILES[@]}"; do
    crate_name="$(extract_field "name" "$cargo_file")"
    crate_version="$(extract_field "version" "$cargo_file")"

    if [[ "$crate_name" == spider-* ]]; then
        if ! is_semver "$crate_version"; then
            err "Unsupported version format in $cargo_file: $crate_version"
            exit 1
        fi

        TARGET_FILES+=("$cargo_file")
        CURRENT_VERSIONS["$cargo_file"]="$crate_version"
    fi
done

if [[ ${#TARGET_FILES[@]} -eq 0 ]]; then
    err "No spider-* crates found."
    exit 1
fi

if [[ "$TARGET" == "major" || "$TARGET" == "minor" || "$TARGET" == "patch" ]]; then
    declare -A NEW_VERSIONS=()

    for cargo_file in "${TARGET_FILES[@]}"; do
        NEW_VERSIONS["$cargo_file"]="$(bump_semver "${CURRENT_VERSIONS[$cargo_file]}" "$TARGET")"
    done
else
    if ! is_semver "$TARGET"; then
        err "Version must be in x.y.z format or one of: major, minor, patch"
        exit 1
    fi

    declare -A NEW_VERSIONS=()
    for cargo_file in "${TARGET_FILES[@]}"; do
        NEW_VERSIONS["$cargo_file"]="$TARGET"
    done
fi

log "Target crates:"
for cargo_file in "${TARGET_FILES[@]}"; do
    crate_name="$(extract_field "name" "$cargo_file")"
    log "  $crate_name: ${CURRENT_VERSIONS[$cargo_file]} -> ${NEW_VERSIONS[$cargo_file]}"
done

if [[ "$DRY_RUN" -eq 1 ]]; then
    log ""
    log "Dry run only. No files were changed."
    exit 0
fi

for cargo_file in "${TARGET_FILES[@]}"; do
    update_package_version "$cargo_file" "${NEW_VERSIONS[$cargo_file]}"
done

"$SCRIPT_DIR/sync_versions.sh"

log ""
log "Version bump complete."
