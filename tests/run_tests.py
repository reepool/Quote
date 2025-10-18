#!/usr/bin/env python3
"""
Test runner script for Quote System
Provides convenient test execution with various options
"""

import os
import sys
import argparse
import subprocess
import time
from pathlib import Path


def run_command(cmd, cwd=None):
    """Run command and return result"""
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            cwd=cwd,
            capture_output=True,
            text=True,
            check=True
        )
        return True, result.stdout, result.stderr
    except subprocess.CalledProcessError as e:
        return False, e.stdout, e.stderr


def install_test_dependencies():
    """Install test dependencies"""
    print("Installing test dependencies...")
    success, stdout, stderr = run_command(
        f"pip install -r {Path(__file__).parent / 'requirements_test.txt'}"
    )

    if success:
        print("✓ Test dependencies installed successfully")
    else:
        print(f"✗ Failed to install test dependencies: {stderr}")
        return False

    return True


def prepare_test_environment():
    """Prepare test environment"""
    print("Preparing test environment...")

    # Create necessary directories
    dirs_to_create = [
        "tests/reports",
        "tests/logs",
        "tests/coverage",
        "htmlcov"
    ]

    for dir_name in dirs_to_create:
        Path(dir_name).mkdir(parents=True, exist_ok=True)

    # Set environment variables for testing
    os.environ["TESTING"] = "true"
    os.environ["LOG_LEVEL"] = "WARNING"

    print("✓ Test environment prepared")
    return True


def run_unit_tests(coverage=True, verbose=False):
    """Run unit tests"""
    print("Running unit tests...")

    cmd = ["python", "-m", "pytest", "tests/unit/"]

    if verbose:
        cmd.append("-v")

    if coverage:
        cmd.extend([
            "--cov=.",
            "--cov-report=html:htmlcov",
            "--cov-report=xml:coverage.xml",
            "--cov-report=term-missing",
            "--cov-fail-under=80"
        ])

    cmd.extend([
        "--html=tests/reports/unit_test_report.html",
        "--self-contained-html",
        "-x"  # Stop on first failure
    ])

    success, stdout, stderr = run_command(" ".join(cmd))

    if success:
        print("✓ Unit tests passed")
    else:
        print(f"✗ Unit tests failed:\n{stderr}")

    return success


def run_integration_tests(verbose=False):
    """Run integration tests"""
    print("Running integration tests...")

    cmd = ["python", "-m", "pytest", "tests/integration/"]

    if verbose:
        cmd.append("-v")

    cmd.extend([
        "--html=tests/reports/integration_test_report.html",
        "--self-contained-html",
        "-x"
    ])

    success, stdout, stderr = run_command(" ".join(cmd))

    if success:
        print("✓ Integration tests passed")
    else:
        print(f"✗ Integration tests failed:\n{stderr}")

    return success


def run_performance_tests(verbose=False):
    """Run performance tests"""
    print("Running performance tests...")

    cmd = ["python", "-m", "pytest", "tests/performance/"]

    if verbose:
        cmd.append("-v")

    cmd.extend([
        "--benchmark-only",
        "--benchmark-json=tests/reports/performance_benchmark.json",
        "--html=tests/reports/performance_test_report.html",
        "--self-contained-html"
    ])

    success, stdout, stderr = run_command(" ".join(cmd))

    if success:
        print("✓ Performance tests completed")
    else:
        print(f"✗ Performance tests failed:\n{stderr}")

    return success


def run_e2e_tests(verbose=False):
    """Run end-to-end tests"""
    print("Running end-to-end tests...")

    cmd = ["python", "-m", "pytest", "tests/e2e/"]

    if verbose:
        cmd.append("-v")

    cmd.extend([
        "--html=tests/reports/e2e_test_report.html",
        "--self-contained-html",
        "-x"
    ])

    success, stdout, stderr = run_command(" ".join(cmd))

    if success:
        print("✓ End-to-end tests passed")
    else:
        print(f"✗ End-to-end tests failed:\n{stderr}")

    return success


def run_all_tests(coverage=True, verbose=False, skip_slow=False):
    """Run all tests"""
    print("Running all tests...")
    results = {}

    # Run unit tests
    results["unit"] = run_unit_tests(coverage=coverage, verbose=verbose)

    # Run integration tests
    results["integration"] = run_integration_tests(verbose=verbose)

    # Run performance tests
    results["performance"] = run_performance_tests(verbose=verbose)

    # Run e2e tests (skip if requested)
    if not skip_slow:
        results["e2e"] = run_e2e_tests(verbose=verbose)
    else:
        print("Skipping end-to-end tests (--skip-slow)")
        results["e2e"] = True

    return results


def run_quick_tests():
    """Run quick tests for development"""
    print("Running quick tests...")

    cmd = [
        "python", "-m", "pytest",
        "tests/unit/",
        "-v",
        "--tb=short",
        "-x"
    ]

    success, stdout, stderr = run_command(" ".join(cmd))

    if success:
        print("✓ Quick tests passed")
    else:
        print(f"✗ Quick tests failed:\n{stderr}")

    return success


def generate_test_report():
    """Generate comprehensive test report"""
    print("Generating test report...")

    # This would aggregate all test reports into a single report
    # Implementation depends on specific reporting requirements

    report_file = "tests/reports/combined_test_report.html"
    Path(report_file).touch()  # Create placeholder

    print(f"✓ Test report generated: {report_file}")
    return True


def cleanup_test_environment():
    """Cleanup test environment"""
    print("Cleaning up test environment...")

    # Cleanup test databases
    test_db_files = list(Path(".").glob("test_*.db"))
    for db_file in test_db_files:
        db_file.unlink()
        print(f"Removed test database: {db_file}")

    # Cleanup temporary files
    temp_files = list(Path("tests").glob("**/*.tmp"))
    for temp_file in temp_files:
        temp_file.unlink()

    print("✓ Test environment cleaned up")


def main():
    """Main test runner function"""
    parser = argparse.ArgumentParser(description="Quote System Test Runner")
    parser.add_argument(
        "command",
        choices=[
            "install", "prepare", "unit", "integration",
            "performance", "e2e", "all", "quick", "report", "cleanup"
        ],
        help="Test command to run"
    )
    parser.add_argument(
        "--coverage", "-c",
        action="store_true",
        help="Generate coverage report"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose output"
    )
    parser.add_argument(
        "--skip-slow", "-s",
        action="store_true",
        help="Skip slow tests (e2e, performance)"
    )
    parser.add_argument(
        "--no-install",
        action="store_true",
        help="Skip dependency installation"
    )

    args = parser.parse_args()

    # Change to project root
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    os.chdir(project_root)

    start_time = time.time()

    try:
        if args.command == "install":
            success = install_test_dependencies()

        elif args.command == "prepare":
            success = prepare_test_environment()

        elif args.command == "unit":
            success = run_unit_tests(coverage=args.coverage, verbose=args.verbose)

        elif args.command == "integration":
            success = run_integration_tests(verbose=args.verbose)

        elif args.command == "performance":
            success = run_performance_tests(verbose=args.verbose)

        elif args.command == "e2e":
            success = run_e2e_tests(verbose=args.verbose)

        elif args.command == "all":
            if not args.no_install:
                install_test_dependencies()
            prepare_test_environment()
            results = run_all_tests(
                coverage=args.coverage,
                verbose=args.verbose,
                skip_slow=args.skip_slow
            )
            success = all(results.values())
            generate_test_report()

        elif args.command == "quick":
            success = run_quick_tests()

        elif args.command == "report":
            success = generate_test_report()

        elif args.command == "cleanup":
            success = cleanup_test_environment()

        else:
            print(f"Unknown command: {args.command}")
            success = False

    except KeyboardInterrupt:
        print("\nTest execution interrupted by user")
        success = False

    except Exception as e:
        print(f"Test execution failed with error: {e}")
        success = False

    # Final summary
    end_time = time.time()
    duration = end_time - start_time

    print(f"\nTest execution completed in {duration:.2f} seconds")

    if success:
        print("✓ All tests completed successfully")
        sys.exit(0)
    else:
        print("✗ Some tests failed")
        sys.exit(1)


if __name__ == "__main__":
    main()