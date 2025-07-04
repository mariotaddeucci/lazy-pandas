#!/usr/bin/env python3
"""
Simple Test Runner CLI for Lazy Pandas

This script provides a command-line interface for running simple unit tests
without requiring knowledge of pytest.

Usage:
    python run_simple_tests.py [test_file.py]
    python run_simple_tests.py --all
    python run_simple_tests.py --help
"""

import argparse
import os
import sys
import glob

# Add the src directory to the path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(current_dir, '..', 'src')
sys.path.insert(0, src_dir)

from lazy_pandas.testing import run_tests_from_file, run_module_tests


def find_simple_test_files(directory="."):
    """Find all files that look like simple test files"""
    patterns = [
        "simple_test_*.py",
        "*_simple_test.py", 
        "test_simple_*.py"
    ]
    
    test_files = []
    for pattern in patterns:
        test_files.extend(glob.glob(os.path.join(directory, pattern)))
        # Also search in subdirectories
        test_files.extend(glob.glob(os.path.join(directory, "**", pattern), recursive=True))
    
    return test_files


def main():
    parser = argparse.ArgumentParser(
        description="Simple Test Runner for Lazy Pandas",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s test_file.py          # Run tests in specific file
  %(prog)s --all                 # Run all simple tests
  %(prog)s --list               # List available test files
        """
    )
    
    parser.add_argument('file', nargs='?', help='Test file to run')
    parser.add_argument('--all', action='store_true', help='Run all simple test files')
    parser.add_argument('--list', action='store_true', help='List available test files')
    parser.add_argument('--quiet', '-q', action='store_true', help='Quiet mode (less output)')
    parser.add_argument('--directory', '-d', default='.', help='Directory to search for tests')
    
    args = parser.parse_args()
    
    # List available test files
    if args.list:
        test_files = find_simple_test_files(args.directory)
        if test_files:
            print("Available simple test files:")
            for test_file in sorted(test_files):
                print(f"  {test_file}")
        else:
            print("No simple test files found.")
        return 0
    
    # Run all simple tests
    if args.all:
        test_files = find_simple_test_files(args.directory)
        if not test_files:
            print("No simple test files found.")
            return 1
        
        print(f"Running {len(test_files)} simple test files...")
        print("=" * 60)
        
        all_passed = True
        for test_file in sorted(test_files):
            print(f"\n📁 Running {test_file}:")
            print("-" * 40)
            
            success = run_tests_from_file(test_file, verbose=not args.quiet)
            if not success:
                all_passed = False
        
        print("\n" + "=" * 60)
        if all_passed:
            print("✅ All test files passed!")
            return 0
        else:
            print("❌ Some test files failed!")
            return 1
    
    # Run specific test file
    if args.file:
        if not os.path.exists(args.file):
            print(f"Error: Test file '{args.file}' not found.")
            return 1
        
        print(f"Running tests from {args.file}...")
        print("=" * 50)
        
        success = run_tests_from_file(args.file, verbose=not args.quiet)
        
        if success:
            print("✅ All tests passed!")
            return 0
        else:
            print("❌ Some tests failed!")
            return 1
    
    # No arguments provided
    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())