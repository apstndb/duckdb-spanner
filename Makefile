.PHONY: build build-sweep build-release extension duckdb emulator-start emulator-stop emulator-status test clean sweep sweep-dry-run ensure-cargo-sweep

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
DUCKDB_VERSION := v1.5.2
endif
# Keep the existing v-prefixed extension metadata while deriving the numeric
# version from the crate package section to avoid manual drift.
EXT_VERSION := $(shell awk -F'"' '/^\[package\]/{in_pkg=1; next} /^\[/{in_pkg=0} in_pkg && /^version = /{print "v"$$2; exit}' Cargo.toml)
SWEEP_DAYS ?= 3

build:
	cargo build --features loadable-extension

build-sweep: ensure-cargo-sweep build
	cargo sweep --time $(SWEEP_DAYS)

build-release:
	cargo build --features loadable-extension --release

extension: build-release
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

test: extension
	@bash tests/integration.sh "$$(pwd)/$(EXTENSION)"

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
