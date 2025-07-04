# Simple Unit Testing Framework for Lazy Pandas

This document describes the simple unit testing framework that has been implemented for lazy-pandas. This framework provides an easy way to write and run unit tests without requiring deep knowledge of pytest.

## Overview

The simple testing framework includes:

- **SimpleTestRunner**: A lightweight test runner
- **SimpleAssert**: Basic assertion utilities
- **TestDataFactory**: Helper class for creating common test data
- **Decorators**: Simple decorators for marking test functions
- **CLI Runner**: Command-line interface for running tests

## Quick Start

### Writing a Simple Test

```python
import sys
import os

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import lazy_pandas as lp
from lazy_pandas.testing import SimpleAssert, TestDataFactory, test, run_module_tests

@test
def test_basic_operations():
    """Test basic LazyFrame operations"""
    # Create test data
    pandas_df = TestDataFactory.simple_dataframe()
    lazy_df = lp.from_pandas(pandas_df)
    
    # Test that they're equivalent
    result = lazy_df.collect()
    SimpleAssert.dataframes_equal(result, pandas_df)

if __name__ == "__main__":
    success = run_module_tests(globals())
    sys.exit(0 if success else 1)
```

### Running Tests

You can run tests in several ways:

1. **Run a specific test file:**
   ```bash
   PYTHONPATH=src python run_simple_tests.py my_test_file.py
   ```

2. **Run all simple tests:**
   ```bash
   PYTHONPATH=src python run_simple_tests.py --all
   ```

3. **List available test files:**
   ```bash
   PYTHONPATH=src python run_simple_tests.py --list
   ```

## Available Assertions

The `SimpleAssert` class provides the following assertion methods:

- `equals(actual, expected, message="")` - Assert two values are equal
- `not_equals(actual, expected, message="")` - Assert two values are not equal  
- `is_true(value, message="")` - Assert a value is True
- `is_false(value, message="")` - Assert a value is False
- `is_none(value, message="")` - Assert a value is None
- `is_not_none(value, message="")` - Assert a value is not None
- `contains(container, item, message="")` - Assert a container contains an item
- `dataframes_equal(df1, df2, message="")` - Assert two DataFrames are equal
- `lazy_pandas_equal(lazy_df, pandas_df, message="")` - Assert LazyFrame equals pandas DataFrame

## Test Data Factory

The `TestDataFactory` class provides convenient methods for creating test data:

- `simple_dataframe()` - Creates a simple 2-column DataFrame
- `numeric_dataframe()` - Creates a DataFrame with numeric columns
- `string_dataframe()` - Creates a DataFrame with string columns  
- `datetime_dataframe()` - Creates a DataFrame with datetime columns
- `create_lazy_frame(query)` - Creates a LazyFrame from SQL query
- `dataframe_pair(query=None, pandas_df=None)` - Creates equivalent LazyFrame and pandas DataFrame

## Decorators

- `@test` - Mark a function as a test (optional, functions starting with `test_` are auto-detected)
- `@setup` - Mark a function to run before all tests
- `@teardown` - Mark a function to run after all tests
- `@skip_test(reason)` - Mark a test to be skipped

## Example Test File Structure

```python
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import lazy_pandas as lp
from lazy_pandas.testing import *

@setup
def setup_tests():
    """Setup function that runs before all tests"""
    print("Setting up tests...")

@teardown  
def teardown_tests():
    """Teardown function that runs after all tests"""
    print("Cleaning up tests...")

@test
def test_feature_one():
    """Test description"""
    # Test implementation
    pass

@test
def test_feature_two():
    """Another test description"""
    # Test implementation  
    pass

if __name__ == "__main__":
    success = run_module_tests(globals(), verbose=True)
    sys.exit(0 if success else 1)
```

## Integration with Existing Tests

This simple testing framework is designed to complement, not replace, the existing pytest infrastructure. You can:

1. Continue using pytest for complex tests
2. Use the simple framework for basic functionality tests
3. Use both frameworks in the same project
4. Gradually migrate tests if desired

## Benefits

- **Easy to learn**: No pytest knowledge required
- **Lightweight**: Minimal dependencies
- **Familiar**: Similar patterns to other testing frameworks
- **Flexible**: Can be used alongside pytest
- **Self-contained**: All test logic in one file

## Command Line Options

The `run_simple_tests.py` script supports:

- `--all` - Run all simple test files
- `--list` - List available test files  
- `--quiet` - Reduce output verbosity
- `--directory DIR` - Specify directory to search for tests
- `--help` - Show help message

## File Naming Conventions

The test runner automatically finds files with these patterns:

- `simple_test_*.py`
- `*_simple_test.py`
- `test_simple_*.py`

This helps distinguish simple tests from pytest-based tests.