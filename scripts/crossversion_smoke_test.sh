#!/usr/bin/env bash

# Smoke test for the crossversion test infrastructure.
#
# This script verifies that the crossversion test can detect backwards
# compatibility bugs by:
# 1. Running the crossversion test without any bugs (should PASS)
# 2. Applying a patch that introduces an intentional upgrade bug
# 3. Running the crossversion test again (should FAIL)
# 4. Verifying that the test failed (detecting the bug)
#
# If the crossversion test passes despite the intentional bug, this smoke
# test will fail, alerting us that the crossversion test infrastructure
# may be broken.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
PATCH_FILE="${SCRIPT_DIR}/crossversion_smoke_test.patch"
CROSSVERSION_DIR="${REPO_ROOT}/internal/metamorphic/crossversion"

# Track whether we applied the patch (for cleanup)
PATCH_APPLIED=false

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log() {
    echo -e "${GREEN}[SMOKE TEST]${NC} $*"
}

warn() {
    echo -e "${YELLOW}[SMOKE TEST]${NC} $*"
}

error() {
    echo -e "${RED}[SMOKE TEST ERROR]${NC} $*" >&2
}

# Cleanup function
cleanup() {
    log "Cleaning up..."

    # Remove test binaries
    rm -f "${CROSSVERSION_DIR}/release.test"
    rm -f "${CROSSVERSION_DIR}/head.test"
    rm -f "${CROSSVERSION_DIR}/head-buggy.test"
    rm -rf "${CROSSVERSION_DIR}/smoke-test-artifacts"

    # Only restore if we applied the patch
    if [ "$PATCH_APPLIED" = true ]; then
        if ! git diff --quiet; then
            warn "Reverting patch changes..."
            git checkout -- .
        fi
    fi
}

# Set up cleanup trap
trap cleanup EXIT

# Check prerequisites
log "Checking prerequisites..."

if [ ! -f "${PATCH_FILE}" ]; then
    error "Patch file not found: ${PATCH_FILE}"
    exit 1
fi

if ! command -v go >/dev/null 2>&1; then
    error "Go is not installed or not in PATH"
    exit 1
fi

# Ensure we're in a git repository
if ! git rev-parse --git-dir >/dev/null 2>&1; then
    error "Not in a git repository"
    exit 1
fi

# Check for uncommitted changes
if ! git diff --quiet || ! git diff --cached --quiet; then
    error "Repository has uncommitted changes. Please commit or stash them first."
    exit 1
fi

log "Prerequisites OK"

# Step 1: Find the latest release branch
log "Finding latest release branch..."
git fetch origin >/dev/null 2>&1 || warn "Could not fetch from origin (continuing anyway)"
LATEST_RELEASE=$(git branch -r --list '*/crl-release-*' | grep -o 'crl-release-.*$' | sort | tail -1 || echo "")

if [ -z "${LATEST_RELEASE}" ]; then
    warn "No release branch found. Trying local branches..."
    LATEST_RELEASE=$(git branch --list 'crl-release-*' | sed 's/^[* ]*//' | sort | tail -1 || echo "")
fi

if [ -z "${LATEST_RELEASE}" ]; then
    error "Could not find any release branch (crl-release-*)"
    error "The crossversion test requires at least one release branch to test against."
    exit 1
fi

log "Using release branch: ${LATEST_RELEASE}"

# Step 2: Build test binary from release branch
log "Building test binary from ${LATEST_RELEASE}..."
CURRENT_BRANCH=$(git rev-parse HEAD)
git checkout "${LATEST_RELEASE}" >/dev/null 2>&1

if ! go test -c ./internal/metamorphic -o "${CROSSVERSION_DIR}/release.test"; then
    error "Failed to build test binary from ${LATEST_RELEASE}"
    git checkout "${CURRENT_BRANCH}" >/dev/null 2>&1
    exit 1
fi

git checkout "${CURRENT_BRANCH}" >/dev/null 2>&1
log "Release test binary built successfully"

# Step 3: Build clean HEAD test binary
log "Building clean HEAD test binary..."
if ! go test -c ./internal/metamorphic -o "${CROSSVERSION_DIR}/head.test"; then
    error "Failed to build HEAD test binary"
    exit 1
fi
log "HEAD test binary built successfully"

# Step 4: Run crossversion test WITHOUT the bug (should PASS)
log ""
log "===== PHASE 1: Running crossversion test WITHOUT bug (should PASS) ====="
log ""
mkdir -p "${CROSSVERSION_DIR}/smoke-test-artifacts"

set +e
echo "::group::Phase 1: Crossversion test WITHOUT bug"
go test -tags invariants -v -timeout 10m -run 'TestMetaCrossVersion' \
    ./internal/metamorphic/crossversion \
    --version "${LATEST_RELEASE},${LATEST_RELEASE},release.test" \
    --version 'HEAD,HEAD,head.test' \
    --artifacts "${CROSSVERSION_DIR}/smoke-test-artifacts/phase1" \
    --seed 42 \
    --factor 3 \
    2>&1 | tee /tmp/crossversion_smoke_test_phase1.log

PHASE1_EXIT_CODE=$?
echo "::endgroup::"
set -e

if [ ${PHASE1_EXIT_CODE} -ne 0 ]; then
    error "SMOKE TEST FAILED!"
    error "The crossversion test FAILED without any intentional bug."
    error "This indicates there may be an existing compatibility issue."
    error "Exit code: ${PHASE1_EXIT_CODE}"
    error "Check logs at: /tmp/crossversion_smoke_test_phase1.log"
    exit 1
fi

log ""
log "Phase 1 SUCCESS: Crossversion test passed without bug"
log ""

# Step 5: Apply the intentional bug patch
log "===== PHASE 2: Applying intentional bug patch ====="
if ! git apply "${PATCH_FILE}"; then
    error "Failed to apply patch. The codebase may have changed."
    error "You may need to update the patch file: ${PATCH_FILE}"
    exit 1
fi
PATCH_APPLIED=true
log "Patch applied successfully"

# Step 6: Build buggy HEAD test binary
log "Building buggy HEAD test binary..."
if ! go test -c ./internal/metamorphic -o "${CROSSVERSION_DIR}/head-buggy.test"; then
    error "Failed to build buggy test binary"
    exit 1
fi
log "Buggy test binary built successfully"

# Step 7: Test buggy version by itself (should PASS - verifies patch doesn't break single-version)
log ""
log "===== PHASE 1.5: Testing buggy version by itself (should PASS) ====="
log "This verifies the patch only breaks cross-version compatibility, not single-version."
log ""

set +e
echo "::group::Phase 1.5: Buggy version by itself"
go test -tags invariants -v -timeout 10m -run 'TestMetaCrossVersion' \
    ./internal/metamorphic/crossversion \
    --version 'HEAD-buggy-only,HEAD,head-buggy.test' \
    --artifacts "${CROSSVERSION_DIR}/smoke-test-artifacts/phase1.5" \
    --seed 42 \
    --factor 3 \
    2>&1 | tee /tmp/crossversion_smoke_test_phase1.5.log

PHASE1_5_EXIT_CODE=$?
echo "::endgroup::"
set -e

if [ ${PHASE1_5_EXIT_CODE} -ne 0 ]; then
    error "SMOKE TEST FAILED!"
    error "The buggy version failed when testing itself (single version)."
    error "This means the patch introduces a bug that breaks even non-upgrade scenarios."
    error "The patch should only break cross-version compatibility, not single-version."
    error "Exit code: ${PHASE1_5_EXIT_CODE}"
    error "Check logs at: /tmp/crossversion_smoke_test_phase1.5.log"
    exit 1
fi

log ""
log "Phase 1.5 SUCCESS: Buggy version works correctly by itself"
log ""

# Step 8: Revert the patch
log "Reverting patch..."
git checkout -- .
PATCH_APPLIED=false
log "Patch reverted"

# Step 9: Run crossversion test WITH the bug multiple times (should FAIL at least once)
log ""
log "===== Running crossversion test WITH bug (should FAIL) ====="
log "Running up to 10 iterations with different seeds to account for randomization..."
log ""

MAX_ATTEMPTS=10
PHASE2_FAILED=false
PHASE2_EXIT_CODE=0

for i in $(seq 1 ${MAX_ATTEMPTS}); do
    SEED=$((42 + i))
    log "Attempt $i/${MAX_ATTEMPTS} with seed ${SEED}..."

    set +e
    echo "::group::Phase 2 Attempt $i: Crossversion test WITH bug (seed ${SEED})"
    go test -tags invariants -v -timeout 10m -run 'TestMetaCrossVersion' \
        ./internal/metamorphic/crossversion \
        --version "${LATEST_RELEASE},${LATEST_RELEASE},release.test" \
        --version 'HEAD-buggy,HEAD,head-buggy.test' \
        --artifacts "${CROSSVERSION_DIR}/smoke-test-artifacts/phase2-attempt${i}" \
        --seed ${SEED} \
        --factor 3 \
        2>&1 | tee /tmp/crossversion_smoke_test_phase2_attempt${i}.log

    ATTEMPT_EXIT_CODE=$?
    echo "::endgroup::"
    set -e

    if [ ${ATTEMPT_EXIT_CODE} -ne 0 ]; then
        log "Attempt $i FAILED (as expected) with exit code: ${ATTEMPT_EXIT_CODE}"
        PHASE2_FAILED=true
        PHASE2_EXIT_CODE=${ATTEMPT_EXIT_CODE}
        break
    else
        warn "Attempt $i passed (unexpected, but may be due to randomization)"
    fi
done

# Step 10: Verify that the test failed in phase 2
log ""
log "===== Checking Phase 2 results ====="

if [ "$PHASE2_FAILED" = false ]; then
    error "SMOKE TEST FAILED!"
    error "The crossversion test PASSED in all ${MAX_ATTEMPTS} attempts despite the intentional bug."
    error "This indicates the crossversion test may not be working correctly."
    error "It should have detected the backwards compatibility issue."
    error ""
    error "Phase 1 (clean cross-version): PASSED (as expected)"
    error "Phase 1.5 (buggy single-version): PASSED (as expected)"
    error "Phase 2 (buggy cross-version): PASSED in all attempts (UNEXPECTED - should have failed)"
    error ""
    error "Check logs at: /tmp/crossversion_smoke_test_phase2_attempt*.log"
    exit 1
else
    log "Phase 2 SUCCESS: Crossversion test correctly detected the intentional bug"
    log "Test failed on attempt $i with exit code: ${PHASE2_EXIT_CODE}"
fi

# Show a summary of what was tested
log ""
log "=============================================="
log "SMOKE TEST PASSED!"
log "=============================================="
log ""
log "Summary:"
log "  - Release branch: ${LATEST_RELEASE}"
log "  - Phase 1 (clean cross-version): PASSED"
log "  - Phase 1.5 (buggy single-version): PASSED"
log "  - Phase 2 (buggy cross-version): FAILED on attempt $i/$MAX_ATTEMPTS (as expected)"
log "  - Intentional bug: Changed metaindex block encoding threshold from v6 to v7"
log ""
log "The crossversion test infrastructure is working correctly and can detect"
log "backwards compatibility issues while not breaking single-version tests."
log ""
log "Logs:"
log "  - Phase 1 (clean): /tmp/crossversion_smoke_test_phase1.log"
log "  - Phase 1.5 (buggy single): /tmp/crossversion_smoke_test_phase1.5.log"
log "  - Phase 2 (buggy cross): /tmp/crossversion_smoke_test_phase2_attempt*.log"

exit 0
