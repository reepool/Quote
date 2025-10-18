# Quote System Makefile
# Provides convenient commands for development and testing

.PHONY: help install test test-unit test-integration test-performance test-e2e test-all test-quick
.PHONY: lint format clean coverage docs setup dev

# Default target
help:
	@echo "Quote System - Available Commands:"
	@echo ""
	@echo "Setup and Installation:"
	@echo "  install     Install all dependencies"
	@echo "  setup       Set up development environment"
	@echo "  dev         Start development server"
	@echo ""
	@echo "Testing:"
	@echo "  test        Run all tests"
	@echo "  test-unit   Run unit tests only"
	@echo "  test-integration  Run integration tests only"
	@echo "  test-performance  Run performance tests only"
	@echo "  test-e2e    Run end-to-end tests only"
	@echo "  test-quick  Run quick tests (development)"
	@echo "  test-all    Run all tests with coverage"
	@echo ""
	@echo "Code Quality:"
	@echo "  lint        Run linting checks"
	@echo "  format      Format code"
	@echo "  coverage    Generate coverage report"
	@echo ""
	@echo "Documentation:"
	@echo "  docs        Generate documentation"
	@echo ""
	@echo "Maintenance:"
	@echo "  clean       Clean temporary files"
	@echo "  setup-test  Setup test environment"

# Installation and setup
install:
	pip install -r requirements.txt
	pip install -r tests/requirements_test.txt

setup:
	@echo "Setting up development environment..."
	python tests/run_tests.py install
	python tests/run_tests.py prepare
	pre-commit install

# Development
dev:
	python main.py api --host 0.0.0.0 --port 8000

# Testing commands
test:
	python tests/run_tests.py all

test-unit:
	python tests/run_tests.py unit --coverage

test-integration:
	python tests/run_tests.py integration

test-performance:
	python tests/run_tests.py performance

test-e2e:
	python tests/run_tests.py e2e

test-quick:
	python tests/run_tests.py quick

test-all:
	python tests/run_tests.py all --coverage --verbose

# Code quality
lint:
	@echo "Running linting checks..."
	flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics
	flake8 . --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics
	mypy .
	black --check .
	isort --check-only .

format:
	@echo "Formatting code..."
	black .
	isort .

coverage:
	@echo "Generating coverage report..."
	python tests/run_tests.py unit --coverage
	@echo "Coverage report available in htmlcov/"

# Documentation
docs:
	@echo "Generating documentation..."
	@echo "Documentation generation not yet implemented"

# Maintenance
clean:
	@echo "Cleaning temporary files..."
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type f -name ".coverage" -delete
	find . -type d -name "htmlcov" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".mypy_cache" -exec rm -rf {} +
	rm -rf tests/reports/
	rm -rf tests/coverage/
	rm -rf .coverage coverage.xml
	python tests/run_tests.py cleanup

setup-test:
	@echo "Setting up test environment..."
	python tests/run_tests.py prepare

# Database operations
db-init:
	@echo "Initializing database..."
	python main.py init

db-backup:
	@echo "Creating database backup..."
	python main.py backup

db-restore:
	@echo "Restoring database from backup..."
	@echo "Specify backup file: make db-restore BACKUP=backup_file.db"

# Data operations
data-download:
	@echo "Downloading stock data..."
	python main.py download --exchanges SSE SZSE

data-update:
	@echo "Updating stock data..."
	python main.py update --exchanges SSE SZSE

data-status:
	@echo "Checking data status..."
	python main.py status

# Service operations
start-scheduler:
	@echo "Starting scheduler..."
	python main.py scheduler

start-api:
	@echo "Starting API server..."
	python main.py api --host 0.0.0.0 --port 8000

start-full:
	@echo "Starting full system..."
	python main.py full --host 0.0.0.0 --port 8000

# Docker operations (if applicable)
docker-build:
	@echo "Building Docker image..."
	docker build -t quote-system .

docker-run:
	@echo "Running Docker container..."
	docker run -p 8000:8000 quote-system

# Development utilities
watch:
	@echo "Watching for file changes..."
	@echo "Install watchmedo: pip install watchdog"
	watchmedo shell-command --patterns="*.py" --recursive --command='make test-quick'

benchmark:
	@echo "Running performance benchmarks..."
	python tests/run_tests.py performance --verbose

security-scan:
	@echo "Running security scan..."
	safety check
	bandit -r .

# CI/CD helpers
ci-test:
	@echo "Running CI tests..."
	python tests/run_tests.py all --coverage
	make lint
	make security-scan

ci-build:
	@echo "Building for CI..."
	python -m build

# Release helpers
version-patch:
	@echo "Bumping patch version..."
	bump2version patch

version-minor:
	@echo "Bumping minor version..."
	bump2version minor

version-major:
	@echo "Bumping major version..."
	bump2version major

# Monitoring and health
health-check:
	@echo "Checking system health..."
	curl -f http://localhost:8000/health || echo "API server not running"

logs:
	@echo "Showing logs..."
	tail -f log/sys.log

# Advanced testing
test-specific:
	@echo "Running specific test file..."
	@echo "Usage: make test-specific TEST=path/to/test_file.py"
	@if [ -z "$(TEST)" ]; then echo "Please specify TEST=path/to/test_file.py"; exit 1; fi
	pytest $(TEST) -v

test-marker:
	@echo "Running tests with marker..."
	@echo "Usage: make test-marker MARKER=unit|integration|performance|e2e"
	@if [ -z "$(MARKER)" ]; then echo "Please specify MARKER"; exit 1; fi
	pytest -m $(MARKER) -v

# Database migrations
migrate:
	@echo "Running database migrations..."
	python main.py migrate

# Utility functions
check-deps:
	@echo "Checking for outdated dependencies..."
	pip list --outdated

install-dev:
	@echo "Installing development dependencies..."
	pip install -r requirements.txt
	pip install -r tests/requirements_test.txt
	pip install black flake8 isort mypy pre-commit bandit safety pytest-xdist

# Performance profiling
profile:
	@echo "Running with profiling..."
	python -m cProfile -o profile.stats main.py api

# Quick development cycle
dev-test: format lint test-quick
	@echo "Development cycle completed"

# Full check before commit
pre-commit-check: format lint test-unit
	@echo "Pre-commit checks passed"

# Comprehensive check
full-check: format lint test-all security-scan
	@echo "All checks passed"