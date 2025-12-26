# Test Organization

This document describes the organization of tests in the lazy-pandas project.

## Structure

```
tests/
├── conftest.py              # Shared fixtures and test utilities
├── test_general.py          # Tests for general utilities (from_pandas)
├── test_readers.py          # Tests for all reader functions (CSV, Parquet, JSON, Delta, Iceberg)
├── test_utils.py            # Tests for utility functions (column expressions, operators)
├── test_exceptions.py       # Tests for custom exceptions
├── column/                  # Tests for column operations
│   ├── test_lazy_column.py
│   ├── test_lazy_datetime_column.py
│   ├── test_lazy_str_column.py
│   ├── test_arithmetic_operations.py
│   ├── test_clip.py
│   ├── test_column_extended.py
│   ├── test_column_operations.py
│   ├── test_datetime_basic.py
│   ├── test_datetime_components.py
│   ├── test_datetime_extended.py
│   ├── test_str_advanced_methods.py
│   ├── test_str_basic_methods.py
│   ├── test_str_cat.py
│   └── test_string_extended.py
└── frame/                   # Tests for frame operations
    ├── test_collect.py
    ├── test_columns.py
    ├── test_drop_duplicates.py
    ├── test_frame_column_operations.py
    ├── test_head.py
    ├── test_merge.py
    ├── test_sample.py
    ├── test_sample_advanced.py
    └── test_sort_values.py
```

## Naming Convention

All test files follow the `test_*.py` naming convention as per pytest best practices.

## Test Categories

### Root Level Tests

- **test_general.py**: Tests for general conversion functions like `from_pandas()`
- **test_readers.py**: Consolidated tests for all data reading functions
- **test_utils.py**: Tests for internal utility functions
- **test_exceptions.py**: Tests for custom exception classes

### Column Tests (`column/`)

Tests specific to column operations, organized by feature:
- Basic column operations (`test_lazy_column.py`)
- DateTime operations (`test_lazy_datetime_column.py`, `test_datetime_*.py`)
- String operations (`test_lazy_str_column.py`, `test_str_*.py`)
- Arithmetic and other operations

### Frame Tests (`frame/`)

Tests specific to DataFrame/LazyFrame operations, organized by method:
- Data retrieval (`test_collect.py`, `test_head.py`)
- Column management (`test_columns.py`, `test_frame_column_operations.py`)
- Data manipulation (`test_drop_duplicates.py`, `test_sort_values.py`)
- Joining (`test_merge.py`)
- Sampling (`test_sample.py`, `test_sample_advanced.py`)

## Fixtures

Common test fixtures are defined in `conftest.py`:
- `simple_df_pair`: Simple DataFrame with two columns
- `multi_row_df_pair`: DataFrame with multiple rows
- `numeric_df_pair`: DataFrame with numeric data
- `duplicate_df_pair`: DataFrame with duplicate rows
- And more...

These fixtures return `DataFramePair` objects that contain both a `LazyFrame` and its equivalent `pandas.DataFrame` for comparison testing.

## Running Tests

Run all tests:
```bash
pytest
```

Run tests for a specific category:
```bash
pytest tests/test_readers.py  # Reader tests
pytest tests/column/          # All column tests
pytest tests/frame/           # All frame tests
```

Run a specific test file:
```bash
pytest tests/frame/test_merge.py
```
