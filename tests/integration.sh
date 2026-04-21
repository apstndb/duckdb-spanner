#!/bin/bash
#
# Integration tests for duckdb-spanner extension.
# Requires: Docker (for emulator), DuckDB CLI, curl
#
set -euo pipefail

# ─── Configuration ───────────────────────────────────────────────────────────

EMULATOR_HOST="${SPANNER_EMULATOR_HOST:-localhost:9010}"
EMULATOR_REST="http://localhost:9020"
EMULATOR_IMAGE="gcr.io/cloud-spanner-emulator/emulator:1.5.51"
PROJECT="test-project"
INSTANCE="test-instance"
DATABASE="test-db"
FULL_DB="projects/${PROJECT}/instances/${INSTANCE}/databases/${DATABASE}"

# Extension path (passed as argument or auto-detected)
if [[ $# -ge 1 ]]; then
    EXTENSION="$1"
else
    EXTENSION="$(pwd)/spanner.duckdb_extension"
fi

# Ensure absolute path (macOS hardened runtime requires it)
case "$EXTENSION" in
    /*) ;; # already absolute
    *)  EXTENSION="$(pwd)/${EXTENSION}" ;;
esac

PASS=0
FAIL=0
SKIP=0

# ─── Colors ──────────────────────────────────────────────────────────────────

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BOLD='\033[1m'
NC='\033[0m'

# ─── Helper Functions ────────────────────────────────────────────────────────

log_info()  { echo -e "${BOLD}[INFO]${NC}  $*"; }
log_pass()  { echo -e "  ${GREEN}PASS${NC} $*"; PASS=$((PASS + 1)); }
log_fail()  { echo -e "  ${RED}FAIL${NC} $*"; FAIL=$((FAIL + 1)); }
log_skip()  { echo -e "  ${YELLOW}SKIP${NC} $*"; SKIP=$((SKIP + 1)); }

# Run a SQL query via DuckDB CLI and return output (no header, CSV format)
run_query() {
    local sql="$1"
    SPANNER_EMULATOR_HOST="${EMULATOR_HOST}" \
    duckdb -unsigned -noheader -csv -c "
        SET allow_extensions_metadata_mismatch=true;
        LOAD '${EXTENSION}';
        ${sql}
    " 2> >(grep -v '^\[duckdb-spanner\]' >&2)
}

sql_escape() {
    local quote="'"
    printf "%s" "${1//${quote}/${quote}${quote}}"
}

spanner_query_source() {
    local spanner_sql
    spanner_sql=$(sql_escape "$1")
    printf "spanner_query('%s', database_path := '%s', endpoint := '%s')" \
        "$spanner_sql" "$FULL_DB" "$EMULATOR_HOST"
}

spanner_scan_source() {
    local table_name
    table_name=$(sql_escape "$1")
    printf "spanner_scan('%s', database_path := '%s', endpoint := '%s')" \
        "$table_name" "$FULL_DB" "$EMULATOR_HOST"
}

# Assert that query output exactly matches expected string
assert_eq() {
    local test_name="$1"
    local query="$2"
    local expected="$3"

    local actual
    actual=$(run_query "$query") || {
        log_fail "${test_name}: query failed with: ${actual}"
        return
    }

    if [[ "$actual" == "$expected" ]]; then
        log_pass "${test_name}"
    else
        log_fail "${test_name}"
        echo "    Expected:"
        echo "$expected" | sed 's/^/      /'
        echo "    Actual:"
        echo "$actual" | sed 's/^/      /'
    fi
}

# Assert that query output contains a substring
assert_contains() {
    local test_name="$1"
    local query="$2"
    local expected_substr="$3"

    local actual
    actual=$(run_query "$query") || {
        log_fail "${test_name}: query failed with: ${actual}"
        return
    }

    if echo "$actual" | grep -qF -- "$expected_substr"; then
        log_pass "${test_name}"
    else
        log_fail "${test_name}: expected to contain '${expected_substr}'"
        echo "    Actual:"
        echo "$actual" | sed 's/^/      /'
    fi
}

# Assert row count
assert_row_count() {
    local test_name="$1"
    local query="$2"
    local expected_count="$3"

    local actual
    actual=$(run_query "$query") || {
        log_fail "${test_name}: query failed"
        return
    }

    # Count non-empty lines
    local count
    count=$(echo "$actual" | grep -c '.' || true)

    if [[ "$count" -eq "$expected_count" ]]; then
        log_pass "${test_name} (${count} rows)"
    else
        log_fail "${test_name}: expected ${expected_count} rows, got ${count}"
        echo "    Output:"
        echo "$actual" | sed 's/^/      /'
    fi
}

# Assert that a query succeeds (doesn't error)
assert_ok() {
    local test_name="$1"
    local query="$2"

    local actual
    if actual=$(run_query "$query" 2>&1); then
        log_pass "${test_name}"
    else
        log_fail "${test_name}: query failed"
        echo "    Output:"
        echo "$actual" | sed 's/^/      /'
    fi
}

# ─── Prerequisites ───────────────────────────────────────────────────────────

check_prerequisites() {
    log_info "Checking prerequisites..."

    if ! command -v docker &>/dev/null; then
        echo "ERROR: docker is required but not installed."
        exit 1
    fi

    if ! command -v duckdb &>/dev/null; then
        echo "ERROR: duckdb CLI is required but not installed."
        echo "  Install: brew install duckdb  (macOS)"
        echo "       or: https://duckdb.org/docs/installation/"
        exit 1
    fi

    if ! command -v curl &>/dev/null; then
        echo "ERROR: curl is required but not installed."
        exit 1
    fi

    if [[ ! -f "$EXTENSION" ]]; then
        echo "ERROR: Extension not found at ${EXTENSION}"
        echo "  Run: make extension"
        exit 1
    fi

    log_info "Extension: ${EXTENSION}"
    log_info "DuckDB:    $(duckdb -version 2>/dev/null | head -1)"
}

# ─── Emulator Setup ─────────────────────────────────────────────────────────

ensure_emulator_running() {
    log_info "Checking Spanner emulator..."

    if ! docker ps --format '{{.Names}}' | grep -q '^spanner-emulator$'; then
        log_info "Starting emulator..."
        docker run -d --name spanner-emulator -p 9010:9010 -p 9020:9020 \
            "${EMULATOR_IMAGE}" >/dev/null
        sleep 3
    fi

    # Wait until REST endpoint is ready
    local retries=10
    while ! curl -s -o /dev/null -w '%{http_code}' "${EMULATOR_REST}/" 2>/dev/null | grep -q '[245]'; do
        retries=$((retries - 1))
        if [[ $retries -le 0 ]]; then
            echo "ERROR: Emulator REST endpoint not reachable at ${EMULATOR_REST}"
            exit 1
        fi
        sleep 1
    done

    log_info "Emulator ready at ${EMULATOR_HOST}"
}

setup_database() {
    log_info "Setting up test database..."

    # Create instance (ignore if already exists)
    curl -sf -X POST "${EMULATOR_REST}/v1/projects/${PROJECT}/instances" \
        -H "Content-Type: application/json" \
        -d "{
            \"instanceId\": \"${INSTANCE}\",
            \"instance\": {
                \"config\": \"emulator-config\",
                \"displayName\": \"Test Instance\",
                \"nodeCount\": 1
            }
        }" >/dev/null 2>&1 || true

    # Drop existing database if present
    curl -sf -X DELETE \
        "${EMULATOR_REST}/v1/projects/${PROJECT}/instances/${INSTANCE}/databases/${DATABASE}" \
        >/dev/null 2>&1 || true

    # Create database with test tables
    curl -sf -X POST \
        "${EMULATOR_REST}/v1/projects/${PROJECT}/instances/${INSTANCE}/databases" \
        -H "Content-Type: application/json" \
        -d '{
            "createStatement": "CREATE DATABASE `test-db`",
            "extraStatements": [
                "CREATE TABLE ScalarTypes (Id INT64 NOT NULL, BoolCol BOOL, Int64Col INT64, Float64Col FLOAT64, StringCol STRING(MAX), BytesCol BYTES(MAX), DateCol DATE, TimestampCol TIMESTAMP) PRIMARY KEY (Id)",
                "CREATE TABLE EmptyTable (Id INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY (Id)",
                "CREATE TABLE NullableTypes (Id INT64 NOT NULL, Val STRING(MAX)) PRIMARY KEY (Id)",
                "CREATE TABLE NumericTypes (Id INT64 NOT NULL, NumCol NUMERIC, JsonCol JSON) PRIMARY KEY (Id)",
                "CREATE TABLE ArrayTypes (Id INT64 NOT NULL, IntArray ARRAY<INT64>, StrArray ARRAY<STRING(MAX)>) PRIMARY KEY (Id)"
            ]
        }' >/dev/null 2>&1

    # Wait for the long-running operation to complete
    sleep 1

    # Create a session for data insertion
    local session
    session=$(curl -sf -X POST \
        "${EMULATOR_REST}/v1/projects/${PROJECT}/instances/${INSTANCE}/databases/${DATABASE}/sessions" \
        -H "Content-Type: application/json" -d '{}' \
        | python3 -c "import json,sys; print(json.load(sys.stdin)['name'])")

    # Insert test data into ScalarTypes
    curl -sf -X POST "${EMULATOR_REST}/v1/${session}:commit" \
        -H "Content-Type: application/json" \
        -d '{
            "singleUseTransaction": {"readWrite": {}},
            "mutations": [{
                "insertOrUpdate": {
                    "table": "ScalarTypes",
                    "columns": ["Id", "BoolCol", "Int64Col", "Float64Col", "StringCol", "BytesCol", "DateCol", "TimestampCol"],
                    "values": [
                        ["1", true, "42", 3.14, "hello", "aGVsbG8=", "2024-01-15", "2024-06-15T10:30:00Z"],
                        ["2", false, "-100", 2.718, "world", "d29ybGQ=", "1999-12-31", "2000-01-01T00:00:00Z"],
                        ["3", null, null, null, null, null, null, null]
                    ]
                }
            }]
        }' >/dev/null

    # Insert data into NullableTypes
    curl -sf -X POST "${EMULATOR_REST}/v1/${session}:commit" \
        -H "Content-Type: application/json" \
        -d '{
            "singleUseTransaction": {"readWrite": {}},
            "mutations": [{
                "insertOrUpdate": {
                    "table": "NullableTypes",
                    "columns": ["Id", "Val"],
                    "values": [
                        ["1", "alpha"],
                        ["2", null],
                        ["3", "gamma"]
                    ]
                }
            }]
        }' >/dev/null

    # Insert data into NumericTypes
    curl -sf -X POST "${EMULATOR_REST}/v1/${session}:commit" \
        -H "Content-Type: application/json" \
        -d '{
            "singleUseTransaction": {"readWrite": {}},
            "mutations": [{
                "insertOrUpdate": {
                    "table": "NumericTypes",
                    "columns": ["Id", "NumCol", "JsonCol"],
                    "values": [
                        ["1", "123.456789000", "{\"key\": \"value\"}"],
                        ["2", "-99999.999999999", null],
                        ["3", null, "{\"arr\": [1,2,3]}"]
                    ]
                }
            }]
        }' >/dev/null

    # Insert data into ArrayTypes
    curl -sf -X POST "${EMULATOR_REST}/v1/${session}:commit" \
        -H "Content-Type: application/json" \
        -d '{
            "singleUseTransaction": {"readWrite": {}},
            "mutations": [{
                "insertOrUpdate": {
                    "table": "ArrayTypes",
                    "columns": ["Id", "IntArray", "StrArray"],
                    "values": [
                        ["1", ["10", "20", "30"], ["a", "b", "c"]],
                        ["2", [], []],
                        ["3", null, null]
                    ]
                }
            }]
        }' >/dev/null

    # Clean up session
    curl -sf -X DELETE "${EMULATOR_REST}/v1/${session}" >/dev/null 2>&1 || true

    log_info "Test data inserted"
}

# ─── Test Cases ──────────────────────────────────────────────────────────────

run_spanner_query_tests() {
    log_info "═══ spanner_query tests ═══"
    echo ""

    # --- Basic connectivity ---
    log_info "Test group: Basic connectivity"

    assert_row_count \
        "SELECT all rows" \
        "SELECT * FROM $(spanner_query_source 'SELECT Id FROM ScalarTypes ORDER BY Id')" \
        3

    assert_row_count \
        "Empty table returns 0 rows" \
        "SELECT * FROM $(spanner_query_source 'SELECT * FROM EmptyTable')" \
        0

    echo ""

    # --- String columns ---
    log_info "Test group: STRING type"

    assert_eq \
        "String values" \
        "SELECT * FROM $(spanner_query_source 'SELECT StringCol FROM ScalarTypes WHERE Id = 1')" \
        "hello"

    assert_eq \
        "NULL string" \
        "SELECT * FROM $(spanner_query_source 'SELECT StringCol IS NULL FROM ScalarTypes WHERE Id = 3')" \
        "true"

    echo ""

    # --- INT64 ---
    log_info "Test group: INT64 type"

    assert_eq \
        "Positive integer" \
        "SELECT * FROM $(spanner_query_source 'SELECT Int64Col FROM ScalarTypes WHERE Id = 1')" \
        "42"

    assert_eq \
        "Negative integer" \
        "SELECT * FROM $(spanner_query_source 'SELECT Int64Col FROM ScalarTypes WHERE Id = 2')" \
        "-100"

    echo ""

    # --- BOOL ---
    log_info "Test group: BOOL type"

    assert_eq \
        "Bool true" \
        "SELECT * FROM $(spanner_query_source 'SELECT BoolCol FROM ScalarTypes WHERE Id = 1')" \
        "true"

    assert_eq \
        "Bool false" \
        "SELECT * FROM $(spanner_query_source 'SELECT BoolCol FROM ScalarTypes WHERE Id = 2')" \
        "false"

    echo ""

    # --- FLOAT64 ---
    log_info "Test group: FLOAT64 type"

    assert_contains \
        "Float64 value (3.14)" \
        "SELECT * FROM $(spanner_query_source 'SELECT Float64Col FROM ScalarTypes WHERE Id = 1')" \
        "3.14"

    echo ""

    # --- DATE ---
    log_info "Test group: DATE type"

    assert_eq \
        "Date value" \
        "SELECT * FROM $(spanner_query_source 'SELECT DateCol FROM ScalarTypes WHERE Id = 1')" \
        "2024-01-15"

    assert_eq \
        "Date Y2K boundary" \
        "SELECT * FROM $(spanner_query_source 'SELECT DateCol FROM ScalarTypes WHERE Id = 2')" \
        "1999-12-31"

    echo ""

    # --- TIMESTAMP ---
    log_info "Test group: TIMESTAMP type"

    assert_contains \
        "Timestamp value" \
        "SELECT * FROM $(spanner_query_source 'SELECT TimestampCol FROM ScalarTypes WHERE Id = 1')" \
        "2024-06-15"

    echo ""

    # --- NUMERIC ---
    log_info "Test group: NUMERIC type"

    assert_contains \
        "Numeric positive" \
        "SELECT * FROM $(spanner_query_source 'SELECT NumCol FROM NumericTypes WHERE Id = 1')" \
        "123.456789"

    assert_contains \
        "Numeric negative" \
        "SELECT * FROM $(spanner_query_source 'SELECT NumCol FROM NumericTypes WHERE Id = 2')" \
        "-99999.999999999"

    assert_eq \
        "Numeric NULL" \
        "SELECT * FROM $(spanner_query_source 'SELECT NumCol IS NULL FROM NumericTypes WHERE Id = 3')" \
        "true"

    echo ""

    # --- JSON ---
    log_info "Test group: JSON type"

    assert_contains \
        "JSON value" \
        "SELECT * FROM $(spanner_query_source 'SELECT JsonCol FROM NumericTypes WHERE Id = 1')" \
        "key"

    echo ""

    # --- ARRAY ---
    log_info "Test group: ARRAY type"

    assert_ok \
        "ARRAY<INT64> query succeeds" \
        "SELECT * FROM $(spanner_query_source 'SELECT IntArray FROM ArrayTypes WHERE Id = 1')"

    assert_ok \
        "ARRAY<STRING> query succeeds" \
        "SELECT * FROM $(spanner_query_source 'SELECT StrArray FROM ArrayTypes WHERE Id = 1')"

    assert_ok \
        "Empty ARRAY query succeeds" \
        "SELECT * FROM $(spanner_query_source 'SELECT IntArray FROM ArrayTypes WHERE Id = 2')"

    assert_ok \
        "NULL ARRAY query succeeds" \
        "SELECT * FROM $(spanner_query_source 'SELECT IntArray FROM ArrayTypes WHERE Id = 3')"

    echo ""

    # --- NULL handling ---
    log_info "Test group: NULL handling"

    assert_eq \
        "All-null row count" \
        "SELECT * FROM $(spanner_query_source 'SELECT COUNT(*) FROM ScalarTypes WHERE BoolCol IS NULL')" \
        "1"

    echo ""

    # --- Multi-column queries ---
    log_info "Test group: Multi-column queries"

    assert_row_count \
        "Multi-column projection" \
        "SELECT * FROM $(spanner_query_source 'SELECT Id, StringCol, Int64Col FROM ScalarTypes ORDER BY Id')" \
        3

    echo ""

    # --- DuckDB-side filtering ---
    log_info "Test group: DuckDB-side operations"

    assert_row_count \
        "DuckDB WHERE on spanner results" \
        "SELECT * FROM $(spanner_query_source 'SELECT Id, StringCol FROM ScalarTypes') WHERE Id > 1" \
        2

    assert_eq \
        "DuckDB aggregate on spanner results" \
        "SELECT SUM(Int64Col) FROM $(spanner_query_source 'SELECT Int64Col FROM ScalarTypes')" \
        "-58"

    echo ""

    # --- Spanner-side filtering ---
    log_info "Test group: Spanner-side filtering"

    assert_eq \
        "Spanner WHERE clause" \
        "SELECT * FROM $(spanner_query_source 'SELECT StringCol FROM ScalarTypes WHERE Id = 2')" \
        "world"

    assert_row_count \
        "Spanner WHERE with no matches" \
        "SELECT * FROM $(spanner_query_source 'SELECT * FROM ScalarTypes WHERE Id = 999')" \
        0

    echo ""

    # --- Complex Spanner SQL ---
    log_info "Test group: Complex Spanner SQL"

    assert_row_count \
        "Spanner LIMIT" \
        "SELECT * FROM $(spanner_query_source 'SELECT Id FROM ScalarTypes ORDER BY Id LIMIT 2')" \
        2

    assert_eq \
        "Spanner expression" \
        "SELECT * FROM $(spanner_query_source 'SELECT Int64Col * 2 AS doubled FROM ScalarTypes WHERE Id = 1')" \
        "84"
}

run_spanner_scan_tests() {
    echo ""
    log_info "═══ spanner_scan tests ═══"
    echo ""

    # --- Basic scan ---
    log_info "Test group: Basic scan"

    assert_row_count \
        "Scan all rows" \
        "SELECT * FROM $(spanner_scan_source 'ScalarTypes')" \
        3

    assert_row_count \
        "Scan empty table" \
        "SELECT * FROM $(spanner_scan_source 'EmptyTable')" \
        0

    echo ""

    # --- Projection pushdown ---
    log_info "Test group: Projection pushdown"

    assert_row_count \
        "Scan with column selection" \
        "SELECT StringCol FROM $(spanner_scan_source 'ScalarTypes')" \
        3

    assert_row_count \
        "Scan with two columns" \
        "SELECT Id, StringCol FROM $(spanner_scan_source 'ScalarTypes')" \
        3

    echo ""

    # --- DuckDB filtering on scan ---
    log_info "Test group: DuckDB filtering on scan"

    assert_row_count \
        "DuckDB WHERE on scan results" \
        "SELECT * FROM $(spanner_scan_source 'ScalarTypes') WHERE Id = 1" \
        1

    assert_eq \
        "Scan with DuckDB aggregate" \
        "SELECT SUM(Int64Col) FROM $(spanner_scan_source 'ScalarTypes')" \
        "-58"

    echo ""

    # --- Type correctness in scan ---
    log_info "Test group: Type correctness in scan"

    assert_eq \
        "Scan BOOL value" \
        "SELECT BoolCol FROM $(spanner_scan_source 'ScalarTypes') WHERE Id = 1" \
        "true"

    assert_contains \
        "Scan FLOAT64 value" \
        "SELECT Float64Col FROM $(spanner_scan_source 'ScalarTypes') WHERE Id = 1" \
        "3.14"

    assert_eq \
        "Scan DATE value" \
        "SELECT DateCol FROM $(spanner_scan_source 'ScalarTypes') WHERE Id = 1" \
        "2024-01-15"

    assert_contains \
        "Scan TIMESTAMP value" \
        "SELECT TimestampCol FROM $(spanner_scan_source 'ScalarTypes') WHERE Id = 1" \
        "2024-06-15"

    echo ""

    # --- NUMERIC in scan ---
    log_info "Test group: NUMERIC type in scan"

    assert_contains \
        "Scan NUMERIC value" \
        "SELECT NumCol FROM $(spanner_scan_source 'NumericTypes') WHERE Id = 1" \
        "123.456789"

    assert_contains \
        "Scan JSON value" \
        "SELECT JsonCol FROM $(spanner_scan_source 'NumericTypes') WHERE Id = 1" \
        "key"
}

# ─── Main ────────────────────────────────────────────────────────────────────

main() {
    echo ""
    echo -e "${BOLD}═══ duckdb-spanner Integration Tests ═══${NC}"
    echo ""

    check_prerequisites
    ensure_emulator_running
    setup_database

    echo ""
    run_spanner_query_tests
    run_spanner_scan_tests

    # Summary
    echo ""
    echo -e "${BOLD}═══ Summary ═══${NC}"
    echo -e "  ${GREEN}Passed: ${PASS}${NC}"
    if [[ $FAIL -gt 0 ]]; then
        echo -e "  ${RED}Failed: ${FAIL}${NC}"
    fi
    if [[ $SKIP -gt 0 ]]; then
        echo -e "  ${YELLOW}Skipped: ${SKIP}${NC}"
    fi
    echo ""

    if [[ $FAIL -gt 0 ]]; then
        exit 1
    fi
}

main
