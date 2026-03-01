#!/usr/bin/env bash
#
# build_all_features.sh
# Build spider-lib with all feature combinations (auto-discovered from Cargo.toml)
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Counters
TOTAL=0
PASSED=0
FAILED=0

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[PASS]${NC} $1"
}

log_error() {
    echo -e "${RED}[FAIL]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_header() {
    echo ""
    echo "========================================"
    echo " $1"
    echo "========================================"
}

print_summary() {
    echo ""
    echo "========================================"
    echo " BUILD SUMMARY"
    echo "========================================"
    echo -e " Total:   ${TOTAL}"
    echo -e " Passed:  ${GREEN}${PASSED}${NC}"
    echo -e " Failed:  ${RED}${FAILED}${NC}"
    echo "========================================"

    if [ "$FAILED" -gt 0 ]; then
        exit 1
    fi
}

# Extract features from Cargo.toml
# Returns space-separated list of feature names (excluding 'default' and 'core')
extract_features() {
    local cargo_toml="$1"
    local in_features=0
    local features=()

    while IFS= read -r line; do
        # Check if we're entering [features] section
        if [[ "$line" =~ ^\[features\] ]]; then
            in_features=1
            continue
        fi

        # Check if we're entering another section
        if [[ "$line" =~ ^\[.+\] ]] && [[ ! "$line" =~ ^\[features\] ]]; then
            in_features=0
            continue
        fi

        # If we're in features section, extract feature name
        if [[ $in_features -eq 1 ]]; then
            # Match lines like: feature-name = [...]
            if [[ "$line" =~ ^([a-zA-Z0-9_-]+)[[:space:]]*= ]]; then
                local feature_name="${BASH_REMATCH[1]}"
                # Skip 'default' and 'core' features
                if [[ "$feature_name" != "default" && "$feature_name" != "core" ]]; then
                    features+=("$feature_name")
                fi
            fi
        fi
    done < "$cargo_toml"

    echo "${features[@]}"
}

# Generate all combinations of features
# Usage: generate_combinations "feat1 feat2 feat3"
generate_combinations() {
    local features=($1)
    local n=${#features[@]}
    local combinations=()

    # Always include empty combination (no features)
    combinations+=("")

    # Single features
    for ((i=0; i<n; i++)); do
        combinations+=("${features[$i]}")
    done

    # Pairs
    if [[ $n -ge 2 ]]; then
        for ((i=0; i<n-1; i++)); do
            for ((j=i+1; j<n; j++)); do
                combinations+=("${features[$i]},${features[$j]}")
            done
        done
    fi

    # Triples
    if [[ $n -ge 3 ]]; then
        for ((i=0; i<n-2; i++)); do
            for ((j=i+1; j<n-1; j++)); do
                for ((k=j+1; k<n; k++)); do
                    combinations+=("${features[$i]},${features[$j]},${features[$k]}")
                done
            done
        done
    fi

    # Quadruples and more (only if many features)
    if [[ $n -ge 4 ]]; then
        # Add combination of first 4
        local quad="${features[0]},${features[1]},${features[2]},${features[3]}"
        for ((i=4; i<n; i++)); do
            quad="${quad},${features[$i]}"
        done
        combinations+=("$quad")
    fi

    # Print unique combinations
    printf '%s\n' "${combinations[@]}" | sort -u
}

build_feature() {
    local feature="$1"
    local package="$2"
    TOTAL=$((TOTAL + 1))

    if [ -n "$package" ]; then
        log_info "Building ${package} --features ${feature:-<none>}"
        if [ -n "$feature" ]; then
            cargo check -p "$package" --features "$feature" 2>&1
        else
            cargo check -p "$package" 2>&1
        fi
        if [ $? -eq 0 ]; then
            log_success "${package} --features ${feature:-<none>}"
            PASSED=$((PASSED + 1))
        else
            log_error "${package} --features ${feature:-<none>}"
            FAILED=$((FAILED + 1))
        fi
    else
        log_info "Building root --features ${feature:-<none>}"
        if [ -n "$feature" ]; then
            cargo check --features "$feature" 2>&1
        else
            cargo check 2>&1
        fi
        if [ $? -eq 0 ]; then
            log_success "root --features ${feature:-<none>}"
            PASSED=$((PASSED + 1))
        else
            log_error "root --features ${feature:-<none>}"
            FAILED=$((FAILED + 1))
        fi
    fi
}

build_no_default() {
    local package="$1"
    TOTAL=$((TOTAL + 1))

    if [ -n "$package" ]; then
        log_info "Building ${package} --no-default-features"
        if cargo check -p "$package" --no-default-features 2>&1; then
            log_success "${package} --no-default-features"
            PASSED=$((PASSED + 1))
        else
            log_error "${package} --no-default-features"
            FAILED=$((FAILED + 1))
        fi
    fi
}

build_package_features() {
    local package="$1"
    local cargo_toml="$PROJECT_ROOT/$package/Cargo.toml"

    if [[ ! -f "$cargo_toml" ]]; then
        log_warn "Cargo.toml not found for $package, skipping..."
        return
    fi

    local features=$(extract_features "$cargo_toml")
    log_info "Discovered features for ${package}: ${features:-<none>}"

    # Generate combinations
    local combinations=$(generate_combinations "$features")

    # Build with default features
    build_feature "" "$package"

    # Build with each combination
    while IFS= read -r combo; do
        if [[ -n "$combo" ]]; then
            build_feature "$combo" "$package"
        fi
    done <<< "$combinations"

    # Build with no-default-features
    build_no_default "$package"
}

# Main execution
cd "$PROJECT_ROOT"

print_header "SPIDER-LIB FEATURE BUILD TEST"
echo "Project root: $PROJECT_ROOT"
echo "Started at: $(date)"

# ============================================
# Workspace crates (auto-discover features)
# ============================================

for crate_dir in spider-util spider-middleware spider-pipeline spider-core spider-downloader spider-macro; do
    if [[ -d "$PROJECT_ROOT/$crate_dir" ]]; then
        print_header "$crate_dir"
        build_package_features "$crate_dir"
    fi
done

# ============================================
# Root crate (spider-lib) features
# ============================================
print_header "spider-lib (root)"

root_features=$(extract_features "$PROJECT_ROOT/Cargo.toml")
log_info "Discovered features for root: ${root_features:-<none>}"

root_combinations=$(generate_combinations "$root_features")

# Build with default features
build_feature "" ""

# Build with each combination
while IFS= read -r combo; do
    if [[ -n "$combo" ]]; then
        build_feature "$combo" ""
    fi
done <<< "$root_combinations"

# Build with no-default-features
build_no_default ""

# ============================================
# Release build with all features
# ============================================
print_header "Release Build (all features)"

log_info "Building release with all features..."
TOTAL=$((TOTAL + 1))
if cargo build --release --all-features 2>&1; then
    log_success "Release build (all features)"
    PASSED=$((PASSED + 1))
else
    log_error "Release build (all features)"
    FAILED=$((FAILED + 1))
fi

# ============================================
# Summary
# ============================================
print_summary

echo ""
echo "Completed at: $(date)"
