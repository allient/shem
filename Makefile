# Shem Makefile
# Provides convenient commands for development and testing

.PHONY: help test test-introspect test-feature clean clean-dbs build check

# Default target
help:
	@echo "Shem - Declarative Migration Tool"
	@echo ""
	@echo "Available commands:"
	@echo "  build           Build the project"
	@echo "  test            Run all tests"
	@echo "  test-introspect Run introspect tests"
	@echo "  test-feature    Run specific feature test (e.g., make test-feature FEATURE=tables)"
	@echo "  check           Run cargo check"
	@echo "  clean           Clean build artifacts"
	@echo "  clean-dbs       Clean up orphaned test databases"
	@echo "  help            Show this help message"

dev-db: ## Start development environment
	@echo "Starting development environment..."
	docker-compose --env-file .env up

# Build the project
build:
	cargo build

# Run all tests
test:
	cargo test

# Run introspect tests
test-introspect:
	cargo test -p shem-postgres --test simple_introspect_test -- --nocapture

# Run specific feature test
test-feature:
	@if [ -z "$(FEATURE)" ]; then \
		echo "Error: FEATURE is required. Example: make test-feature FEATURE=tables"; \
		exit 1; \
	fi
	cargo test -p shem-postgres --test simple_introspect_test test_$(FEATURE)_introspect -- --nocapture

# Run cargo check
check:
	cargo check

# Clean build artifacts
clean:
	cargo clean

# Clean up orphaned test databases
clean-dbs:
	./scripts/test-introspect.sh cleanup

# Show test database status
db-status:
	./scripts/test-introspect.sh status

# Check PostgreSQL connection
check-postgres:
	./scripts/test-introspect.sh check

# Run all introspect tests individually
test-all-features:
	cargo test -p shem-postgres --test simple_introspect_test test_tables_introspect -- --nocapture
	cargo test -p shem-postgres --test simple_introspect_test test_enums_introspect -- --nocapture
	cargo test -p shem-postgres --test simple_introspect_test test_functions_introspect -- --nocapture
	cargo test -p shem-postgres --test simple_introspect_test test_extensions_introspect -- --nocapture 