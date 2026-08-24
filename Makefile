.PHONY: build build-sweep build-release check-google-cloud-rust check-duckdb-version check-duckdb-cli-version check-target-duckdb-version extension duckdb emulator-start emulator-stop emulator-status test test_debug test_release test_peg_parser test_extension_load_order test_extension_loader_rejection test_duckdb_compatibility community_smoke clean sweep sweep-dry-run ensure-cargo-sweep ensure-pinned-duckdb-test-host configure debug release clean_all

# Detect OS for library extension
UNAME := $(shell uname)
ARCH := $(shell uname -m)
ifeq ($(UNAME), Darwin)
  LIB_EXT := dylib
  ifeq ($(ARCH), x86_64)
    PLATFORM := osx_amd64
  else
    PLATFORM := osx_arm64
  endif
else
  LIB_EXT := so
  ifeq ($(ARCH), aarch64)
    PLATFORM := linux_arm64
  else
    PLATFORM := linux_amd64
  endif
endif

RAW_LIB := target/release/libduckdb_spanner.$(LIB_EXT)
EXTENSION := spanner.duckdb_extension
METADATA_SCRIPT := extension-ci-tools/scripts/append_extension_metadata.py
DUCKDB_VERSION_CHECK := scripts/check-duckdb-version.sh
EMULATOR_NAME := spanner-emulator
EMULATOR_IMAGE := gcr.io/cloud-spanner-emulator/emulator:1.5.56@sha256:18a56fd557011e50e1733a9232e8d17ec9bdd7e51f6cf7660f14c234479f4f36
# This is the compile-time ABI target, not a caller-selectable metadata value.
override DUCKDB_TARGET_VERSION := v1.5.5
DUCKDB_TEST_HOST_VERSION := $(patsubst v%,%,$(DUCKDB_TARGET_VERSION))
DUCKDB_MISMATCH_TEST_VERSION := 1.5.4
DUCKDB_BIN ?= duckdb
DUCKDB_CLI_VERSION := $(shell "$(DUCKDB_BIN)" --version 2>/dev/null | sed -nE 's/^v?([0-9]+\.[0-9]+\.[0-9]+).*/v\1/p')

# The unstable C_STRUCT ABI is compiled for one exact DuckDB version. An
# explicit metadata override is retained only when it resolves to that target.
normalize_duckdb_version = $(if $(strip $(1)),v$(patsubst v%,%,$(strip $(1))))
DUCKDB_VERSION_INPUT := $(DUCKDB_VERSION)
ifeq ($(strip $(DUCKDB_VERSION_INPUT)),)
  DUCKDB_VERSION_INPUT := $(DUCKDB_CLI_VERSION)
endif
ifeq ($(strip $(DUCKDB_VERSION_INPUT)),)
  DUCKDB_VERSION_INPUT := $(DUCKDB_TARGET_VERSION)
endif
DUCKDB_VERSION_EFFECTIVE := $(call normalize_duckdb_version,$(DUCKDB_VERSION_INPUT))
override DUCKDB_VERSION := $(DUCKDB_VERSION_EFFECTIVE)

# extension-ci-tools uses this variable for compilation and metadata. Keep
# the effective value canonical while checking any caller-supplied override.
TARGET_DUCKDB_VERSION ?= $(DUCKDB_TARGET_VERSION)
TARGET_DUCKDB_VERSION_INPUT := $(TARGET_DUCKDB_VERSION)
TARGET_DUCKDB_VERSION_EFFECTIVE := $(call normalize_duckdb_version,$(TARGET_DUCKDB_VERSION_INPUT))
override TARGET_DUCKDB_VERSION := $(TARGET_DUCKDB_VERSION_EFFECTIVE)
# Keep the existing v-prefixed extension metadata while deriving the numeric
# version from the crate package section to avoid manual drift.
# Derive from Cargo.toml with grep/sed (portable on Windows CI; awk `[` breaks mawk).
EXT_VERSION ?= $(shell grep -E '^[[:space:]]*version[[:space:]]*=' Cargo.toml | head -1 | sed 's/.*"\([^"]*\)".*/v\1/')
SWEEP_DAYS ?= 3

build:
	cargo build --features loadable-extension

build-sweep: ensure-cargo-sweep build
	cargo sweep --time $(SWEEP_DAYS)

build-release: check-duckdb-version
	cargo build --features loadable-extension --release

check-google-cloud-rust:
	bash scripts/check-google-cloud-rust.sh

check-duckdb-version:
	@bash $(DUCKDB_VERSION_CHECK) "$(DUCKDB_TARGET_VERSION)" "$(DUCKDB_VERSION)" "$(DUCKDB_CLI_VERSION)"

check-duckdb-cli-version:
	@if [ -z "$(DUCKDB_CLI_VERSION)" ]; then \
		echo "error: DUCKDB_BIN '$(DUCKDB_BIN)' did not report a MAJOR.MINOR.PATCH version; install DuckDB $(DUCKDB_TARGET_VERSION) or set DUCKDB_BIN to that CLI" >&2; \
		exit 1; \
	fi
	@bash $(DUCKDB_VERSION_CHECK) "$(DUCKDB_TARGET_VERSION)" "$(DUCKDB_TARGET_VERSION)" "$(DUCKDB_CLI_VERSION)"

check-target-duckdb-version:
	@bash $(DUCKDB_VERSION_CHECK) "$(DUCKDB_TARGET_VERSION)" "$(TARGET_DUCKDB_VERSION)"

extension: build-release
ifeq ($(strip $(EXT_VERSION)),)
	$(error EXT_VERSION is empty; failed to derive it from Cargo.toml. Set EXT_VERSION explicitly.)
endif
	@cp $(RAW_LIB) spanner_raw.$(LIB_EXT)
	@python3 $(METADATA_SCRIPT) \
		-l spanner_raw.$(LIB_EXT) \
		-o $(EXTENSION) \
		-n spanner \
		-dv $(DUCKDB_VERSION) \
		-ev $(EXT_VERSION) \
		-p $(PLATFORM) \
		--abi-type C_STRUCT_UNSTABLE
	@rm -f spanner_raw.$(LIB_EXT)
	@echo "Extension ready: $(EXTENSION)"

# Keep CLI validation ahead of the expensive build even under parallel make.
duckdb: check-duckdb-cli-version
	@$(MAKE) --no-print-directory extension
	@echo "Starting DuckDB with spanner extension loaded..."
	@"$(DUCKDB_BIN)" -unsigned -cmd "LOAD '$$(pwd)/$(EXTENSION)'"

emulator-start:
	@if docker ps --format '{{.Names}}' | grep -q '^$(EMULATOR_NAME)$$'; then \
		echo "Emulator already running"; \
	else \
		docker run -d --name $(EMULATOR_NAME) -p 9010:9010 -p 9020:9020 \
			$(EMULATOR_IMAGE); \
		echo "Waiting for emulator to start..."; \
		sleep 2; \
		echo "Emulator started"; \
	fi

emulator-stop:
	@docker stop $(EMULATOR_NAME) 2>/dev/null && docker rm $(EMULATOR_NAME) 2>/dev/null || true
	@echo "Emulator stopped"

emulator-status:
	@docker ps --filter "name=$(EMULATOR_NAME)" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# SQLLogicTest via extension-ci-tools (requires: make configure release)
test: test_release
test_debug: test_extension_debug
test_release: test_extension_release

# These compatibility lanes use explicit DuckDB wheel versions. Do not reuse
# extension-ci-tools' default (latest) test-host selection for unstable ABI checks.
ensure-pinned-duckdb-test-host:
	@if [ ! -x "$(PYTHON_VENV_BIN)" ]; then \
		$(MAKE) --no-print-directory configure DUCKDB_TEST_VERSION=$(DUCKDB_TEST_HOST_VERSION); \
	fi
	@$(PYTHON_VENV_BIN) -m pip install --disable-pip-version-check "duckdb==$(DUCKDB_TEST_HOST_VERSION)"
	@$(PYTHON_VENV_BIN) -c "import duckdb; assert duckdb.__version__ == '$(DUCKDB_TEST_HOST_VERSION)', duckdb.__version__"

ensure-cargo-sweep:
	@command -v cargo-sweep >/dev/null 2>&1 || { \
		echo "cargo-sweep is required for sweep targets. Install it with: brew install cargo-sweep"; \
		exit 1; \
	}

sweep-dry-run: ensure-cargo-sweep
	cargo sweep --dry-run --time $(SWEEP_DAYS)

sweep: ensure-cargo-sweep
	cargo sweep --time $(SWEEP_DAYS)

clean:
	cargo clean
	rm -f $(EXTENSION)

clean_all: clean clean_build clean_configure clean_rust

# ─── extension-ci-tools integration (CI distribution pipeline) ─────────────
# Matches duckdb/extension-template-rs layout. Local dev targets above stay unchanged.

EXTENSION_NAME=spanner
USE_UNSTABLE_C_API=1
# Keep extension-ci-tools metadata in sync with Cargo.toml (not stale git short hash).
EXTENSION_VERSION ?= $(patsubst v%,%,$(EXT_VERSION))

include extension-ci-tools/makefiles/c_api_extensions/base.Makefile

# Cargo package name (duckdb-spanner) differs from extension name (spanner), so
# the cdylib artifact keeps the crate name: libduckdb_spanner.so/.dylib on
# Linux/Darwin and duckdb_spanner.dll on Windows (no lib prefix, hyphen -> underscore).
ifeq ($(OS),Windows_NT)
	EXTENSION_LIB_FILENAME=duckdb_spanner.dll
else
	CI_UNAME_S := $(shell uname -s)
	ifeq ($(CI_UNAME_S),Linux)
		EXTENSION_LIB_FILENAME=libduckdb_spanner.so
	endif
	ifeq ($(CI_UNAME_S),Darwin)
		EXTENSION_LIB_FILENAME=libduckdb_spanner.dylib
	endif
endif

include extension-ci-tools/makefiles/c_api_extensions/rust.Makefile

# Keep extension-ci-tools' copy step aligned with Cargo when callers isolate
# artifacts through CARGO_TARGET_DIR (for example, an isolated distribution CI
# job).
CARGO_TARGET_PATH ?= $(TARGET_PATH)
ifneq ($(strip $(CARGO_TARGET_DIR)),)
CARGO_TARGET_PATH := $(CARGO_TARGET_DIR)
ifneq ($(strip $(TARGET)),)
CARGO_TARGET_PATH := $(CARGO_TARGET_DIR)/$(TARGET)
endif
endif

# Always refresh extension version from Cargo.toml (avoid stale git-hash file).
extension_version:
	@mkdir -p configure
	@echo "$(EXTENSION_VERSION)" > configure/extension_version.txt

# Metadata builds must refresh the version even when configure/ already exists.
build_extension_with_metadata_debug build_extension_with_metadata_release: extension_version

# duckdb-spanner gates loadable-extension behind a crate feature (integration tests use rlib mode).
build_extension_library_debug: check_configure check-target-duckdb-version
	DUCKDB_EXTENSION_NAME=$(EXTENSION_NAME) DUCKDB_EXTENSION_MIN_DUCKDB_VERSION=$(TARGET_DUCKDB_VERSION) cargo build --features loadable-extension $(CARGO_OVERRIDE_DUCKDB_RS_FLAG) $(TARGET_INFO)
	$(PYTHON_VENV_BIN) -c "from pathlib import Path;Path('$(EXTENSION_BUILD_PATH)/debug/extension/$(EXTENSION_NAME)').mkdir(parents=True, exist_ok=True)"
	$(PYTHON_VENV_BIN) -c "import shutil;shutil.copyfile('$(CARGO_TARGET_PATH)/debug$(IS_EXAMPLE)/$(EXTENSION_LIB_FILENAME)', '$(EXTENSION_BUILD_PATH)/debug/$(EXTENSION_LIB_FILENAME)')"

build_extension_library_release: check_configure check-target-duckdb-version
	DUCKDB_EXTENSION_NAME=$(EXTENSION_NAME) DUCKDB_EXTENSION_MIN_DUCKDB_VERSION=$(TARGET_DUCKDB_VERSION) cargo build --features loadable-extension $(CARGO_OVERRIDE_DUCKDB_RS_FLAG) --release $(TARGET_INFO)
	$(PYTHON_VENV_BIN) -c "from pathlib import Path;Path('$(EXTENSION_BUILD_PATH)/release/extension/$(EXTENSION_NAME)').mkdir(parents=True, exist_ok=True)"
	$(PYTHON_VENV_BIN) -c "import shutil;shutil.copyfile('$(CARGO_TARGET_PATH)/release$(IS_EXAMPLE)/$(EXTENSION_LIB_FILENAME)', '$(EXTENSION_BUILD_PATH)/release/$(EXTENSION_LIB_FILENAME)')"

configure: venv platform extension_version

debug: check-target-duckdb-version build_extension_library_debug build_extension_with_metadata_debug
release: check-target-duckdb-version build_extension_library_release build_extension_with_metadata_release

# Offline smoke profile used by extension distribution validation. It exercises
# extension loading without requiring Docker, a Spanner emulator, or an
# external repository submission.
COMMUNITY_SMOKE_TEST_FILE ?= test/sql/spanner_smoke.test
COMMUNITY_SMOKE_TEST_DIR ?= $(dir $(COMMUNITY_SMOKE_TEST_FILE))
community_smoke: configure release
	@echo "Running offline release smoke test: $(COMMUNITY_SMOKE_TEST_FILE)"
	@$(TEST_RUNNER) --test-dir "$(COMMUNITY_SMOKE_TEST_DIR)" --file-path "$(COMMUNITY_SMOKE_TEST_FILE)" --external-extension build/release/$(EXTENSION_NAME).duckdb_extension

# Keep this separate from test_release so the existing legacy-parser lane
# remains a full, independent acceptance run.
TEST_RUNNER_PEG_RELEASE = $(PYTHON_VENV_BIN) scripts/run_sqllogictest_with_peg_parser.py --test-dir test/sql --external-extension build/release/$(EXTENSION_NAME).duckdb_extension
test_peg_parser: ensure-pinned-duckdb-test-host release test_extension_release_peg_parser_internal

test_extension_release_peg_parser_internal: check_configure emulator-start
	@bash tests/setup_sqllogic_db.sh
	@echo "Running RELEASE tests with the opt-in PEG parser.."
	@$(TEST_RUNNER_PEG_RELEASE)

# autocomplete is installed once from DuckDB's exact-version core repository,
# then both orders run in fresh processes with automatic install/load disabled.
# This target deliberately does not use allow_extensions_metadata_mismatch.
test_extension_load_order: ensure-pinned-duckdb-test-host release
	@$(PYTHON_VENV_BIN) scripts/check_extension_load_order.py build/release/$(EXTENSION_NAME).duckdb_extension --expected-duckdb-version $(DUCKDB_TEST_HOST_VERSION)

MISMATCH_DUCKDB_VENV := configure/duckdb-$(DUCKDB_MISMATCH_TEST_VERSION)-mismatch
ifeq ($(OS),Windows_NT)
MISMATCH_DUCKDB_PYTHON := $(MISMATCH_DUCKDB_VENV)/Scripts/python.exe
else
MISMATCH_DUCKDB_PYTHON := $(MISMATCH_DUCKDB_VENV)/bin/python3
endif
test_extension_loader_rejection: ensure-pinned-duckdb-test-host release
	@$(PYTHON_BIN) -m venv "$(MISMATCH_DUCKDB_VENV)"
	@$(MISMATCH_DUCKDB_PYTHON) -m pip install --disable-pip-version-check "duckdb==$(DUCKDB_MISMATCH_TEST_VERSION)"
	@$(MISMATCH_DUCKDB_PYTHON) scripts/check_extension_rejects_mismatched_host.py build/release/$(EXTENSION_NAME).duckdb_extension --expected-host-version $(DUCKDB_MISMATCH_TEST_VERSION) --expected-artifact-version $(DUCKDB_TEST_HOST_VERSION)

test_duckdb_compatibility: test_extension_load_order test_extension_loader_rejection

# Community Extensions builds cannot provision the Spanner emulator. Their
# descriptor sets this variable through test_config so the standard
# test_release entrypoint still verifies that the extension loads. Local and
# project CI runs leave it unset and retain the full emulator-backed suite.
ifeq ($(DUCKDB_SPANNER_OFFLINE_TESTS),1)
test_extension_release_internal: check_configure
	@echo "Running offline release smoke test: $(COMMUNITY_SMOKE_TEST_FILE)"
	@$(TEST_RUNNER) --test-dir "$(COMMUNITY_SMOKE_TEST_DIR)" --file-path "$(COMMUNITY_SMOKE_TEST_FILE)" --external-extension build/release/$(EXTENSION_NAME).duckdb_extension
else
# SQLLogicTest (test/sql/*.test) needs a running Spanner emulator and seeded database.
EMULATOR_HOST ?= localhost:9010
export SPANNER_EMULATOR_HOST ?= $(EMULATOR_HOST)

test_extension_release_internal: check_configure emulator-start
	@bash tests/setup_sqllogic_db.sh
	@echo "Running RELEASE tests.."
	@$(TEST_RUNNER_RELEASE)
endif

test_extension_debug_internal: check_configure emulator-start
	@bash tests/setup_sqllogic_db.sh
	@echo "Running DEBUG tests.."
	@$(TEST_RUNNER_DEBUG)
