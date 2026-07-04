#!/bin/bash
# Seed the Spanner emulator database used by test/sql/*.test (SQLLogicTest).
#
# Prerequisites:
#   - Docker emulator running (make emulator-start)
#   - SPANNER_EMULATOR_HOST set or default localhost:9010
#
# Creates projects/test-project/instances/test-instance/databases/test-db with
# the same tables and seed data as tests/integration.sh.

set -euo pipefail

EMULATOR_HOST="${SPANNER_EMULATOR_HOST:-localhost:9010}"
EMULATOR_REST="http://localhost:9020"
PROJECT="test-project"
INSTANCE="test-instance"
DATABASE="test-db"

# Wait until REST endpoint is ready
retries=10
while ! curl -s -o /dev/null -w '%{http_code}' "${EMULATOR_REST}/" 2>/dev/null | grep -q '[245]'; do
	retries=$((retries - 1))
	if [[ $retries -le 0 ]]; then
		echo "ERROR: Emulator REST endpoint not reachable at ${EMULATOR_REST}" >&2
		echo "Run: make emulator-start" >&2
		exit 1
	fi
	sleep 1
done

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

# Create database with test tables (fail fast — swallowed errors cause confusing test failures)
if ! curl -sf -X POST \
	"${EMULATOR_REST}/v1/projects/${PROJECT}/instances/${INSTANCE}/databases" \
	-H "Content-Type: application/json" \
	-d '{
		"createStatement": "CREATE DATABASE `test-db`",
		"extraStatements": [
			"CREATE TABLE ScalarTypes (Id INT64 NOT NULL, BoolCol BOOL, Int64Col INT64, Float64Col FLOAT64, StringCol STRING(MAX), BytesCol BYTES(MAX), DateCol DATE, TimestampCol TIMESTAMP) PRIMARY KEY (Id)",
			"CREATE TABLE EmptyTable (Id INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY (Id)",
			"CREATE TABLE NullableTypes (Id INT64 NOT NULL, Val STRING(MAX)) PRIMARY KEY (Id)",
			"CREATE TABLE NumericTypes (Id INT64 NOT NULL, NumCol NUMERIC, JsonCol JSON) PRIMARY KEY (Id)",
			"CREATE TABLE ArrayTypes (Id INT64 NOT NULL, IntArray ARRAY<INT64>, StrArray ARRAY<STRING(MAX)>) PRIMARY KEY (Id)",
			"CREATE TABLE CopyTarget (Id INT64 NOT NULL, Name STRING(MAX), Value FLOAT64) PRIMARY KEY (Id)",
			"CREATE TABLE CopyTypes (Id INT64 NOT NULL, BoolCol BOOL, Int64Col INT64, Float64Col FLOAT64, StringCol STRING(MAX), DateCol DATE, TimestampCol TIMESTAMP) PRIMARY KEY (Id)"
		]
	}' >/dev/null 2>&1; then
	echo "ERROR: failed to create database ${DATABASE}" >&2
	exit 1
fi

sleep 1

session=$(curl -sf -X POST \
	"${EMULATOR_REST}/v1/projects/${PROJECT}/instances/${INSTANCE}/databases/${DATABASE}/sessions" \
	-H "Content-Type: application/json" -d '{}' \
	| python3 -c "import json,sys; print(json.load(sys.stdin)['name'])")

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

curl -sf -X DELETE "${EMULATOR_REST}/v1/${session}" >/dev/null 2>&1 || true

# Empty database for DDL SQLLogicTest (isolated from read/copy tables)
DATABASE_DDL="test-ddl-db"
curl -sf -X DELETE \
	"${EMULATOR_REST}/v1/projects/${PROJECT}/instances/${INSTANCE}/databases/${DATABASE_DDL}" \
	>/dev/null 2>&1 || true

curl -sf -X POST \
	"${EMULATOR_REST}/v1/projects/${PROJECT}/instances/${INSTANCE}/databases" \
	-H "Content-Type: application/json" \
	-d '{
		"createStatement": "CREATE DATABASE `test-ddl-db`"
	}' >/dev/null 2>&1

echo "SQLLogicTest database ready at ${EMULATOR_HOST} (${PROJECT}/${INSTANCE}/${DATABASE})"
echo "DDL test database ready at ${EMULATOR_HOST} (${PROJECT}/${INSTANCE}/${DATABASE_DDL})"
