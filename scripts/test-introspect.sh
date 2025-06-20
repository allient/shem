#!/bin/bash

# Shem Introspect Testing Script
# This script helps run and manage introspect tests

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

# Function to check if PostgreSQL is running
check_postgres() {
    print_status "Checking PostgreSQL connection..."
    
    if ! psql -h localhost -U postgres -d postgres -c "SELECT version();" > /dev/null 2>&1; then
        print_error "PostgreSQL is not accessible. Please ensure:"
        print_error "1. PostgreSQL is running"
        print_error "2. You can connect with: psql -h localhost -U postgres -d postgres"
        print_error "3. The postgres user has CREATE/DROP privileges"
        exit 1
    fi
    
    print_success "PostgreSQL connection successful"
}

# Function to clean up orphaned test databases
cleanup_orphaned_dbs() {
    print_status "Cleaning up orphaned test databases..."
    
    # Get list of orphaned test databases
    orphaned_dbs=$(psql -h localhost -U postgres -d postgres -t -c "SELECT datname FROM pg_database WHERE datname LIKE 'shem_test_%';" 2>/dev/null || echo "")
    
    if [ -n "$orphaned_dbs" ]; then
        echo "$orphaned_dbs" | while read -r db; do
            if [ -n "$db" ]; then
                print_warning "Dropping orphaned database: $db"
                psql -h localhost -U postgres -d postgres -c "DROP DATABASE IF EXISTS \"$db\";" > /dev/null 2>&1 || true
            fi
        done
        print_success "Cleanup completed"
    else
        print_status "No orphaned databases found"
    fi
}

# Function to run all introspect tests
run_all_tests() {
    print_status "Running all introspect tests..."
    cargo test introspect -- --nocapture
}

# Function to run individual feature test
run_feature_test() {
    local feature=$1
    print_status "Running $feature introspection test..."
    cargo test "test_${feature}_introspect" -- --nocapture
}

# Function to run full test with all objects
run_full_test() {
    print_status "Running full introspect test with all objects..."
    cargo test test_introspect_with_all_objects -- --nocapture
}

# Function to show test database status
show_db_status() {
    print_status "Current test databases:"
    psql -h localhost -U postgres -d postgres -c "SELECT datname, pg_size_pretty(pg_database_size(datname)) as size FROM pg_database WHERE datname LIKE 'shem_test_%' ORDER BY datname;" 2>/dev/null || print_warning "Could not retrieve database status"
}

# Function to show help
show_help() {
    echo "Shem Introspect Testing Script"
    echo ""
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  all              Run all introspect tests"
    echo "  feature NAME     Run specific feature test (e.g., tables, functions)"
    echo "  full             Run full test with all objects"
    echo "  cleanup          Clean up orphaned test databases"
    echo "  status           Show status of test databases"
    echo "  check            Check PostgreSQL connection"
    echo "  help             Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 all                    # Run all tests"
    echo "  $0 feature tables         # Test only tables"
    echo "  $0 feature functions      # Test only functions"
    echo "  $0 cleanup               # Clean up orphaned databases"
    echo ""
    echo "Available features:"
    echo "  tables, views, materialized_views, functions, procedures"
    echo "  enums, domains, composite_types, range_types, sequences"
    echo "  extensions, triggers, policies, servers, collations, rules"
}

# Main script logic
case "${1:-help}" in
    "all")
        check_postgres
        cleanup_orphaned_dbs
        run_all_tests
        ;;
    "feature")
        if [ -z "$2" ]; then
            print_error "Please specify a feature name"
            echo "Available features: tables, views, materialized_views, functions, procedures, enums, domains, composite_types, range_types, sequences, extensions, triggers, policies, servers, collations, rules"
            exit 1
        fi
        check_postgres
        cleanup_orphaned_dbs
        run_feature_test "$2"
        ;;
    "full")
        check_postgres
        cleanup_orphaned_dbs
        run_full_test
        ;;
    "cleanup")
        check_postgres
        cleanup_orphaned_dbs
        ;;
    "status")
        check_postgres
        show_db_status
        ;;
    "check")
        check_postgres
        ;;
    "help"|*)
        show_help
        ;;
esac 