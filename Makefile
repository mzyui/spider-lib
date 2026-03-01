.PHONY: publish-all sync-versions check-all-features build-all-features clean check-fast

# Build/check with all feature combinations (auto-discovered from Cargo.toml)
check-all-features:
	@chmod +x ./scripts/build_all_features.sh
	@./scripts/build_all_features.sh

build-all-features:
	@echo "Building with all features (release mode)..."
	@cargo build --release --all-features

# Fast check for development (no optimizations)
check-fast:
	@echo "Running fast check (dev, no optimizations)..."
	@CARGO_INCREMENTAL=1 cargo check --workspace --all-targets

# Check specific package only
# Usage: make check-pkg PKG=spider-util
check-pkg:
	@echo "Checking $(PKG)..."
	@cargo check -p $(PKG)

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	@cargo clean

# Check with timing info
check-timing:
	@echo "Running check with timing information..."
	@CARGO_INCREMENTAL=1 cargo check --workspace --all-targets --timings

sync-versions:
	@chmod +x ./scripts/sync_versions.sh
	@./scripts/sync_versions.sh

publish-submodule:
	@echo "Checking current version against crates.io for submodule $(SUBMODULE)..."
	@cd $(SUBMODULE) && \
	CURRENT_VERSION=$$(grep -m 1 "^version =" Cargo.toml | cut -d '"' -f 2); \
	CRATE_NAME=$$(grep -m 1 "^name =" Cargo.toml | cut -d '"' -f 2); \
	echo "Current crate: $$CRATE_NAME, version: $$CURRENT_VERSION"; \
	if cargo search "$$CRATE_NAME" --limit 1 | grep -q "$$CRATE_NAME = \"$$CURRENT_VERSION\""; then \
		echo "Version $$CURRENT_VERSION already exists on crates.io, skipping publish for $$CRATE_NAME."; \
	else \
		echo "Local version ($$CURRENT_VERSION) not found on crates.io, publishing $$CRATE_NAME..."; \
		cargo publish --all-features --allow-dirty; \
	fi

publish-main:
	@echo "Checking current version against crates.io for main repository..."
	@CURRENT_VERSION=$$(grep -m 1 "^version =" Cargo.toml | cut -d '"' -f 2); \
	CRATE_NAME=$$(grep -m 1 "^name =" Cargo.toml | cut -d '"' -f 2); \
	echo "Current crate: $$CRATE_NAME, version: $$CURRENT_VERSION"; \
	if cargo search "$$CRATE_NAME" --limit 1 | grep -q "$$CRATE_NAME = \"$$CURRENT_VERSION\""; then \
		echo "Version $$CURRENT_VERSION already exists on crates.io, skipping publish for $$CRATE_NAME."; \
	else \
		echo "Local version ($$CURRENT_VERSION) not found on crates.io, publishing $$CRATE_NAME..."; \
		cargo publish --all-features --allow-dirty; \
	fi

publish-all:
	@$(MAKE) sync-versions
	@echo "Publishing all packages with spider-lib (main repo) last..."
	@$(MAKE) SUBMODULE="spider-util" publish-submodule
	@$(MAKE) SUBMODULE="spider-macro" publish-submodule
	@$(MAKE) SUBMODULE="spider-middleware" publish-submodule
	@$(MAKE) SUBMODULE="spider-pipeline" publish-submodule
	@$(MAKE) SUBMODULE="spider-downloader" publish-submodule
	@$(MAKE) SUBMODULE="spider-core" publish-submodule
	@$(MAKE) publish-main
	@echo "Publishing complete!"
