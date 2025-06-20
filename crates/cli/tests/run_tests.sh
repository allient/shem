#!/bin/bash

# Test runner script for Shem CLI tests
# This script sets up the test environment and runs the tests

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if we're in the right directory
if [ ! -f "Cargo.toml" ]; then
    print_error "This script must be run from the CLI crate directory"
    exit 1
fi

print_status "Setting up test environment..."

# Check if PostgreSQL is running
if ! pg_isready -q; then
    print_warning "PostgreSQL is not running. Please start PostgreSQL before running tests."
    print_warning "You can start it with: brew services start postgresql (macOS) or sudo systemctl start postgresql (Linux)"
    exit 1
fi

# Set up test database
print_status "Setting up test database..."

# Check if test database exists, create if not
if ! psql -lqt | cut -d \| -f 1 | grep -qw shem_test; then
    print_status "Creating test database 'shem_test'..."
    createdb shem_test
    print_success "Test database created"
else
    print_status "Test database 'shem_test' already exists"
fi

# Set test environment variables
export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/shem_test"
export RUST_LOG="debug"

print_status "Running CLI tests..."

# Run the tests
if cargo test --test integration_tests -- --nocapture; then
    print_success "All tests passed!"
else
    print_error "Some tests failed!"
    exit 1
fi

print_success "Test run completed successfully!" 