"""
Example tests using the simple unit testing framework

This file demonstrates how to use the simple testing framework
to write unit tests for lazy-pandas functionality.
"""

import sys
import os

# Add the src directory to the path so we can import lazy_pandas
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

import lazy_pandas as lp
from lazy_pandas.testing import (
    SimpleAssert, TestDataFactory, test, setup, teardown,
    run_module_tests
)


# Setup function - runs before all tests
@setup
def setup_tests():
    """Setup function that runs before all tests"""
    print("Setting up tests...")


# Teardown function - runs after all tests  
@teardown
def teardown_tests():
    """Teardown function that runs after all tests"""
    print("Cleaning up tests...")


@test
def test_simple_dataframe_creation():
    """Test creating a simple LazyFrame from pandas DataFrame"""
    # Create test data
    pandas_df = TestDataFactory.simple_dataframe()
    
    # Create LazyFrame from pandas
    lazy_df = lp.from_pandas(pandas_df)
    
    # Test basic properties
    result = lazy_df.collect()
    SimpleAssert.dataframes_equal(result, pandas_df, "LazyFrame should equal original pandas DataFrame")
    SimpleAssert.equals(len(result), 3, "DataFrame should have 3 rows")
    SimpleAssert.equals(list(result.columns), ['a', 'b'], "DataFrame should have columns 'a' and 'b'")


@test
def test_numeric_operations():
    """Test basic numeric operations on LazyFrame"""
    # Create test data pair
    data_pair = TestDataFactory.dataframe_pair(
        query="SELECT 1 AS a, 2 AS b UNION ALL SELECT 3, 4"
    )
    
    lazy_df = data_pair['lazy']
    pandas_df = data_pair['pandas']
    
    # Test addition
    lazy_df['c'] = lazy_df['a'] + lazy_df['b']
    pandas_df['c'] = pandas_df['a'] + pandas_df['b']
    
    lazy_result = lazy_df.collect()
    SimpleAssert.lazy_pandas_equal(lazy_df, pandas_df, "Addition operation should match pandas")


@test
def test_string_operations():
    """Test string operations on LazyFrame"""
    # Create string data
    pandas_df = TestDataFactory.string_dataframe()
    lazy_df = lp.from_pandas(pandas_df)
    
    # Test string length operation
    lazy_df['str_len'] = lazy_df['str_col'].str.len()
    result = lazy_df.collect()
    
    # Verify results
    expected_lengths = [5, 5, 4, 4]  # lengths of 'hello', 'world', 'test', 'data'
    SimpleAssert.equals(result['str_len'].tolist(), expected_lengths, "String lengths should be correct")


@test
def test_datetime_operations():
    """Test datetime operations on LazyFrame"""
    # Create datetime data
    pandas_df = TestDataFactory.datetime_dataframe()
    lazy_df = lp.from_pandas(pandas_df)
    
    # Test year extraction
    lazy_df['year'] = lazy_df['date_col'].dt.year
    result = lazy_df.collect()
    
    # All dates should be from 2023
    expected_years = [2023, 2023, 2023, 2023]
    SimpleAssert.equals(result['year'].tolist(), expected_years, "Years should be 2023")


@test
def test_filtering():
    """Test filtering operations on LazyFrame"""
    # Create numeric data
    data_pair = TestDataFactory.dataframe_pair(
        query="SELECT 1 AS value UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4"
    )
    
    lazy_df = data_pair['lazy']
    
    # Filter for values greater than 2
    filtered = lazy_df[lazy_df['value'] > 2]
    result = filtered.collect()
    
    # Should have 2 rows (values 3 and 4)
    SimpleAssert.equals(len(result), 2, "Filtered result should have 2 rows")
    SimpleAssert.equals(result['value'].tolist(), [3, 4], "Filtered values should be [3, 4]")


@test 
def test_aggregation():
    """Test aggregation operations on LazyFrame"""
    # Create test data
    data_pair = TestDataFactory.dataframe_pair(
        query="SELECT 'A' AS group_col, 10 AS value UNION ALL SELECT 'A', 20 UNION ALL SELECT 'B', 30"
    )
    
    lazy_df = data_pair['lazy']
    
    # Test groupby and sum
    grouped = lazy_df.groupby('group_col').sum()
    result = grouped.collect()
    
    # Should have 2 groups
    SimpleAssert.equals(len(result), 2, "Grouped result should have 2 rows")
    
    # Group A should have sum of 30, Group B should have sum of 30
    SimpleAssert.is_true('A' in result['group_col'].values, "Group A should be present")
    SimpleAssert.is_true('B' in result['group_col'].values, "Group B should be present")



if __name__ == "__main__":
    print("Running simple unit tests for lazy-pandas...")
    print("=" * 50)
    
    # Run all tests in this module
    success = run_module_tests(globals(), verbose=True)
    
    if success:
        print("\n✅ All tests passed!")
        sys.exit(0)
    else:
        print("\n❌ Some tests failed!")
        sys.exit(1)