.PHONY: build build-sweep build-release check-google-cloud-rust extension duckdb emulator-start emulator-stop emulator-status test test_debug test_release clean sweep sweep-dry-run ensure-cargo-sweep configure debug release clean_all

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
EMULATOR_NAME := spanner-emulator
DUCKDB_VERSION ?= $(shell duckdb --version 2>/dev/null | sed -nE 's/^v?([0-9]+\.[0-9]+\.[0-9]+).*/v\1/p')
ifeq ($(DUCKDB_VERSION),)
DUCKDB_VERSION := v1.5.4
endif
# Keep the existing v-prefixed extension metadata while deriving the numeric
# version from the crate package section to avoid manual drift.
# Derive from Cargo.toml with grep/sed (portable on Windows CI; awk `[` breaks mawk).
EXT_VERSION ?= $(shell grep -E '^[[:space:]]*version[[:space:]]*=' Cargo.toml | head -1 | sed 's/.*"\([^"]*\)".*/v\1/')
SWEEP_DAYS ?= 3

build:
	cargo build --features loadable-extension

build-sweep: ensure-cargo-sweep build
	cargo sweep --time $(SWEEP_DAYS)

build-release:
	cargo build --features loadable-extension --release

check-google-cloud-rust:
	bash scripts/check-google-cloud-rust.sh

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

duckdb: extension
	@echo "Starting DuckDB with spanner extension loaded..."
	@duckdb -unsigned -cmd "LOAD '$$(pwd)/$(EXTENSION)'"

emulator-start:
	@if docker ps --format '{{.Names}}' | grep -q '^$(EMULATOR_NAME)$$'; then \
		echo "Emulator already running"; \
	else \
		docker run -d --name $(EMULATOR_NAME) -p 9010:9010 -p 9020:9020 \
			gcr.io/cloud-spanner-emulator/emulator; \
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
TARGET_DUCKDB_VERSION=v1.5.4
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

# Always refresh extension version from Cargo.toml (avoid stale git-hash file).
extension_version:
	@echo "$(EXTENSION_VERSION)" > configure/extension_version.txt

# duckdb-spanner gates loadable-extension behind a crate feature (integration tests use rlib mode).
build_extension_library_debug: check_configure
	DUCKDB_EXTENSION_NAME=$(EXTENSION_NAME) DUCKDB_EXTENSION_MIN_DUCKDB_VERSION=$(TARGET_DUCKDB_VERSION) cargo build --features loadable-extension $(CARGO_OVERRIDE_DUCKDB_RS_FLAG) $(TARGET_INFO)
	$(PYTHON_VENV_BIN) -c "from pathlib import Path;Path('$(EXTENSION_BUILD_PATH)/debug/extension/$(EXTENSION_NAME)').mkdir(parents=True, exist_ok=True)"
	$(PYTHON_VENV_BIN) -c "import shutil;shutil.copyfile('$(TARGET_PATH)/debug$(IS_EXAMPLE)/$(EXTENSION_LIB_FILENAME)', '$(EXTENSION_BUILD_PATH)/debug/$(EXTENSION_LIB_FILENAME)')"

build_extension_library_release: check_configure
	DUCKDB_EXTENSION_NAME=$(EXTENSION_NAME) DUCKDB_EXTENSION_MIN_DUCKDB_VERSION=$(TARGET_DUCKDB_VERSION) cargo build --features loadable-extension $(CARGO_OVERRIDE_DUCKDB_RS_FLAG) --release $(TARGET_INFO)
	$(PYTHON_VENV_BIN) -c "from pathlib import Path;Path('$(EXTENSION_BUILD_PATH)/release/extension/$(EXTENSION_NAME)').mkdir(parents=True, exist_ok=True)"
	$(PYTHON_VENV_BIN) -c "import shutil;shutil.copyfile('$(TARGET_PATH)/release$(IS_EXAMPLE)/$(EXTENSION_LIB_FILENAME)', '$(EXTENSION_BUILD_PATH)/release/$(EXTENSION_LIB_FILENAME)')"

configure: venv platform extension_version

debug: build_extension_library_debug build_extension_with_metadata_debug
release: build_extension_library_release build_extension_with_metadata_release

# SQLLogicTest (test/sql/*.test) needs a running Spanner emulator and seeded database.
EMULATOR_HOST ?= localhost:9010
export SPANNER_EMULATOR_HOST ?= $(EMULATOR_HOST)

test_extension_release_internal: check_configure emulator-start
	@bash tests/setup_sqllogic_db.sh
	@echo "Running RELEASE tests.."
	@$(TEST_RUNNER_RELEASE)

test_extension_debug_internal: check_configure emulator-start
	@bash tests/setup_sqllogic_db.sh
	@echo "Running DEBUG tests.."
	@$(TEST_RUNNER_DEBUG)
