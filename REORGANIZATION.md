# Code Organization Improvements

This document describes the organizational improvements made to the lazy-pandas repository.

## Overview

The repository has been reorganized to improve code maintainability, test organization, and developer experience. The changes focus on:

1. Better separation of concerns in source code
2. Consistent test naming and structure
3. Removal of duplicate tests
4. Improved documentation

## Source Code Changes

### New Module Structure

#### `src/lazy_pandas/readers.py` (NEW)

Created a dedicated module for all data reading functions:
- `read_csv()` - Read CSV files and buffers
- `read_json()` - Read JSON files and buffers
- `read_parquet()` - Read Parquet files
- `read_delta()` - Read Delta Lake tables
- `read_iceberg()` - Read Apache Iceberg tables

**Benefits:**
- Logical grouping of related functionality
- Easier to find and maintain reader implementations
- Clear separation from general utilities

#### `src/lazy_pandas/general.py` (UPDATED)

Simplified to contain only general conversion utilities:
- `from_pandas()` - Convert pandas DataFrame to LazyFrame
- Re-exports all reader functions for backward compatibility

**Benefits:**
- Cleaner separation of concerns
- Maintains backward compatibility
- Reduced file size (from 345 lines to 31 lines)

### API Compatibility

All public APIs remain unchanged. Users can continue to use:
```python
import lazy_pandas as lp

# All these work exactly as before
df = lp.read_csv('file.csv')
df = lp.read_json('file.json')
df = lp.read_parquet('file.parquet')
df = lp.from_pandas(pandas_df)
```

## Test Organization Changes

### Naming Standardization

All test files now follow pytest's recommended `test_*.py` naming convention:

**Before:**
- `dataframe_test.py`
- `reader_test.py`
- `lazy_column_test.py`
- `lazy_datetime_column_test.py`
- `lazy_str_column_test.py`

**After:**
- `test_dataframe.py` (later removed as duplicate)
- `test_readers.py` (consolidated)
- `test_lazy_column.py`
- `test_lazy_datetime_column.py`
- `test_lazy_str_column.py`

### Test Consolidation

Removed duplicate tests and consolidated overlapping test files:

1. **Removed `test_dataframe.py`**: Tests were duplicates of better-organized tests in `frame/` subdirectory
2. **Created `test_readers.py`**: Merged tests from `reader_test.py` and reader-related tests from `test_general.py`
3. **Simplified `test_general.py`**: Now only contains tests for `from_pandas()` function

**Impact:**
- Reduced from 133 tests to 119 tests (14 duplicate tests removed)
- Better organization by feature
- Clearer test purpose and location

### Test Structure

```
tests/
├── README.md                    # Documentation of test organization
├── conftest.py                  # Shared fixtures
├── test_general.py              # General utility tests (from_pandas)
├── test_readers.py              # All reader function tests
├── test_utils.py                # Internal utility tests
├── test_exceptions.py           # Exception handling tests
├── column/                      # Column operation tests (14 files)
│   └── test_*.py
└── frame/                       # Frame operation tests (9 files)
    └── test_*.py
```

## Other Improvements

### Examples Directory

Moved example/demo files from `tests/` to `examples/`:
- `example_feature.py` - Example of ContributionAcceptedError usage
- `test_example_feature.py` - Example test demonstrating skip decorator
- `README.md` - Documentation for examples

**Benefits:**
- Clear separation of examples from actual tests
- Examples don't interfere with test collection
- Better documentation

### Documentation

Added comprehensive README files:
- `tests/README.md` - Documents test structure and organization
- `examples/README.md` - Explains example files and their purpose

## Migration Guide

### For Contributors

No changes needed! All imports and APIs remain the same. The changes are purely organizational:

```python
# These all still work exactly as before
import lazy_pandas as lp

df = lp.read_csv('data.csv')
df = lp.from_pandas(pandas_df)
result = df.collect()
```

### For Test Writers

When writing new tests:
1. Use the `test_*.py` naming convention
2. Place tests in the appropriate subdirectory:
   - Column operations → `tests/column/`
   - Frame operations → `tests/frame/`
   - Reader functions → Add to `tests/test_readers.py`
   - General utilities → Add to `tests/test_general.py`
3. Use fixtures from `conftest.py` when possible

## Testing

All tests pass after reorganization:
```bash
# Run all tests
pytest tests/

# Run specific test categories
pytest tests/test_readers.py
pytest tests/column/
pytest tests/frame/
```

**Results:**
- ✅ 115 passed (excluding network-dependent tests)
- ⏭️ 1 skipped (intentionally skipped test)
- ⚠️ 3 network-dependent tests (require external resources)

## Benefits Summary

1. **Better Code Organization**: Reader functions grouped logically in dedicated module
2. **Consistent Naming**: All tests follow pytest conventions
3. **No Duplicates**: Removed 14 duplicate tests
4. **Better Documentation**: Clear README files explain structure
5. **Backward Compatible**: All existing code continues to work
6. **Easier Maintenance**: Clear structure makes it easier to find and update code
7. **Better Developer Experience**: New contributors can quickly understand the codebase

## Future Recommendations

1. Consider splitting `lazy_frame.py` if it grows too large
2. Add more integration tests for reader functions
3. Consider adding performance benchmarks
4. Document testing best practices in contributing guide
