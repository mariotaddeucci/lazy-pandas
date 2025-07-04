# Simple Unit Testing System

This directory contains a simple unit testing framework for lazy-pandas that complements the existing pytest infrastructure.

## Why a Simple Testing System?

While lazy-pandas already has comprehensive pytest-based tests, this simple testing system provides:

- **Easy learning curve**: No pytest knowledge required
- **Lightweight**: Minimal setup and dependencies  
- **Self-contained**: All test logic in one file
- **Beginner-friendly**: Familiar patterns for new contributors
- **Complementary**: Works alongside existing pytest tests

## Quick Start

### 1. Write a Simple Test

Create a file named `simple_test_myfeature.py`:

```python
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import lazy_pandas as lp
from lazy_pandas.testing import SimpleAssert, TestDataFactory, test, run_module_tests

@test
def test_my_feature():
    """Test my lazy-pandas feature"""
    # Create test data
    df = TestDataFactory.simple_dataframe()
    lazy_df = lp.from_pandas(df)
    
    # Test the feature
    result = lazy_df.collect()
    SimpleAssert.dataframes_equal(result, df)

if __name__ == "__main__":
    success = run_module_tests(globals())
    sys.exit(0 if success else 1)
```

### 2. Run Your Test

```bash
# Run a specific test file
PYTHONPATH=src python run_simple_tests.py simple_test_myfeature.py

# Run all simple tests
PYTHONPATH=src python run_simple_tests.py --all

# List available test files
PYTHONPATH=src python run_simple_tests.py --list
```

## Examples

- **`simple_test_example.py`**: Basic usage examples
- **`simple_test_comprehensive.py`**: Comprehensive feature demonstration

## Framework Components

### SimpleAssert
Basic assertion methods:
- `equals()`, `not_equals()`
- `is_true()`, `is_false()`
- `is_none()`, `is_not_none()`
- `contains()`
- `dataframes_equal()`
- `lazy_pandas_equal()`

### TestDataFactory
Common test data creation:
- `simple_dataframe()`
- `numeric_dataframe()`
- `string_dataframe()`
- `datetime_dataframe()`
- `dataframe_pair()`

### Decorators
- `@test` - Mark test functions
- `@setup` - Run before all tests
- `@teardown` - Run after all tests  
- `@skip_test(reason)` - Skip tests

## File Naming

The test runner finds files matching:
- `simple_test_*.py`
- `*_simple_test.py`
- `test_simple_*.py`

## Integration with Existing Tests

This system complements rather than replaces pytest:

✅ **Keep using pytest for:**
- Complex test scenarios
- Parameterized tests
- Test fixtures and mocking
- CI/CD integration

✅ **Use simple tests for:**
- Basic functionality verification
- Teaching/learning examples
- Quick prototyping
- New contributor onboarding

## Commands

```bash
# Run specific test
PYTHONPATH=src python run_simple_tests.py my_test.py

# Run all simple tests
PYTHONPATH=src python run_simple_tests.py --all

# Quiet mode
PYTHONPATH=src python run_simple_tests.py --all --quiet

# Search specific directory
PYTHONPATH=src python run_simple_tests.py --all --directory tests/

# List available tests
PYTHONPATH=src python run_simple_tests.py --list
```

## Benefits

- **Lower barrier to entry** for new contributors
- **Self-documenting** test patterns
- **Quick feedback** for simple changes
- **Educational value** for learning the framework
- **Coexistence** with existing pytest infrastructure

This simple testing system makes it easier for anyone to contribute tests to lazy-pandas!