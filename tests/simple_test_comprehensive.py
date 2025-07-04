"""
Comprehensive example demonstrating all features of the simple testing framework

This test file showcases:
- All assertion types
- Test data factory usage
- Setup/teardown functions
- Error handling
- Various LazyFrame operations
"""

import sys
import os

# Add the src directory to the path so we can import lazy_pandas
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

import lazy_pandas as lp
from lazy_pandas.testing import (
    SimpleAssert, TestDataFactory, test, setup, teardown,
    run_module_tests, skip_test
)


# Global test counter for demonstration
test_counter = 0


@setup
def setup_comprehensive_tests():
    """Setup function - runs once before all tests"""
    global test_counter
    test_counter = 0
    print("🚀 Starting comprehensive test suite...")


@teardown
def teardown_comprehensive_tests():
    """Teardown function - runs once after all tests"""
    print(f"🏁 Completed {test_counter} tests")


def increment_counter():
    """Helper function to track test execution"""
    global test_counter
    test_counter += 1


@test
def test_all_assertion_types():
    """Test all available assertion methods"""
    increment_counter()
    
    # Test equals/not_equals
    SimpleAssert.equals(2 + 2, 4, "Basic math should work")
    SimpleAssert.not_equals(2 + 2, 5, "Basic math inequality")
    
    # Test boolean assertions
    SimpleAssert.is_true(True, "True should be true")
    SimpleAssert.is_false(False, "False should be false")
    SimpleAssert.is_true(1 == 1, "Expression should be true")
    SimpleAssert.is_false(1 == 2, "Expression should be false")
    
    # Test None assertions
    SimpleAssert.is_none(None, "None should be None")
    SimpleAssert.is_not_none("not none", "String should not be None")
    SimpleAssert.is_not_none(0, "Zero should not be None")
    
    # Test contains
    SimpleAssert.contains([1, 2, 3], 2, "List should contain 2")
    SimpleAssert.contains("hello world", "world", "String should contain substring")
    SimpleAssert.contains({"a": 1, "b": 2}, "a", "Dict should contain key")


@test
def test_data_factory_usage():
    """Test all TestDataFactory methods"""
    increment_counter()
    
    # Test simple dataframe
    simple_df = TestDataFactory.simple_dataframe()
    SimpleAssert.equals(len(simple_df), 3, "Simple dataframe should have 3 rows")
    SimpleAssert.equals(list(simple_df.columns), ['a', 'b'], "Simple dataframe columns")
    
    # Test numeric dataframe
    numeric_df = TestDataFactory.numeric_dataframe()
    SimpleAssert.is_true('int_col' in numeric_df.columns, "Should have int_col")
    SimpleAssert.is_true('float_col' in numeric_df.columns, "Should have float_col")
    SimpleAssert.is_true('negative_col' in numeric_df.columns, "Should have negative_col")
    
    # Test string dataframe
    string_df = TestDataFactory.string_dataframe()
    SimpleAssert.is_true('str_col' in string_df.columns, "Should have str_col")
    SimpleAssert.is_true('mixed_col' in string_df.columns, "Should have mixed_col")
    
    # Test datetime dataframe
    datetime_df = TestDataFactory.datetime_dataframe()
    SimpleAssert.is_true('date_col' in datetime_df.columns, "Should have date_col")
    SimpleAssert.is_true('timestamp_col' in datetime_df.columns, "Should have timestamp_col")


@test
def test_lazyframe_creation_methods():
    """Test different ways to create LazyFrames"""
    increment_counter()
    
    # Test from pandas
    pandas_df = TestDataFactory.simple_dataframe()
    lazy_df = lp.from_pandas(pandas_df)
    result = lazy_df.collect()
    SimpleAssert.dataframes_equal(result, pandas_df, "from_pandas should preserve data")
    
    # Test from SQL query
    lazy_df_sql = TestDataFactory.create_lazy_frame("SELECT 1 AS x, 2 AS y")
    result_sql = lazy_df_sql.collect()
    SimpleAssert.equals(len(result_sql), 1, "SQL query should create 1 row")
    SimpleAssert.equals(list(result_sql.columns), ['x', 'y'], "SQL query columns")
    
    # Test dataframe pair
    pair = TestDataFactory.dataframe_pair(query="SELECT 'test' AS col")
    SimpleAssert.dataframes_equal(pair['lazy'].collect(), pair['pandas'], "Pair should be equivalent")


@test
def test_column_operations():
    """Test various column operations"""
    increment_counter()
    
    # Create numeric data for operations
    pandas_df = TestDataFactory.numeric_dataframe()
    lazy_df = lp.from_pandas(pandas_df)
    
    # Test arithmetic operations
    lazy_df['sum_col'] = lazy_df['int_col'] + lazy_df['float_col']
    lazy_df['diff_col'] = lazy_df['int_col'] - lazy_df['negative_col']
    lazy_df['product_col'] = lazy_df['int_col'] * 2
    
    result = lazy_df.collect()
    
    # Verify new columns exist
    SimpleAssert.contains(result.columns, 'sum_col', "Should have sum_col")
    SimpleAssert.contains(result.columns, 'diff_col', "Should have diff_col")
    SimpleAssert.contains(result.columns, 'product_col', "Should have product_col")
    
    # Verify arithmetic is correct for first row
    SimpleAssert.equals(result['sum_col'].iloc[0], 2.1, "Sum should be correct")  # 1 + 1.1
    SimpleAssert.equals(result['diff_col'].iloc[0], 2, "Difference should be correct")  # 1 - (-1)
    SimpleAssert.equals(result['product_col'].iloc[0], 2, "Product should be correct")  # 1 * 2


@test
def test_string_operations():
    """Test string column operations"""
    increment_counter()
    
    string_df = TestDataFactory.string_dataframe()
    lazy_df = lp.from_pandas(string_df)
    
    # Test string operations
    lazy_df['str_upper'] = lazy_df['str_col'].str.upper()
    lazy_df['str_lower'] = lazy_df['mixed_col'].str.lower()
    lazy_df['str_length'] = lazy_df['str_col'].str.len()
    
    result = lazy_df.collect()
    
    # Verify string operations
    SimpleAssert.equals(result['str_upper'].iloc[0], 'HELLO', "String should be uppercase")
    SimpleAssert.equals(result['str_lower'].iloc[0], 'hello', "String should be lowercase")
    SimpleAssert.equals(result['str_length'].iloc[0], 5, "String length should be correct")


@test
def test_datetime_operations():
    """Test datetime column operations"""
    increment_counter()
    
    datetime_df = TestDataFactory.datetime_dataframe()
    lazy_df = lp.from_pandas(datetime_df)
    
    # Test datetime operations
    lazy_df['year'] = lazy_df['date_col'].dt.year
    lazy_df['month'] = lazy_df['date_col'].dt.month
    lazy_df['day'] = lazy_df['date_col'].dt.day
    
    result = lazy_df.collect()
    
    # Verify datetime operations
    SimpleAssert.equals(result['year'].iloc[0], 2023, "Year should be extracted correctly")
    SimpleAssert.equals(result['month'].iloc[0], 1, "Month should be extracted correctly")
    SimpleAssert.equals(result['day'].iloc[0], 1, "Day should be extracted correctly")


@test
def test_filtering_and_selection():
    """Test filtering and column selection"""
    increment_counter()
    
    # Create test data with varied values
    pair = TestDataFactory.dataframe_pair(
        query="SELECT i AS value, i * 2 AS double_value FROM range(1, 6) t(i)"
    )
    
    lazy_df = pair['lazy']
    
    # Test filtering
    filtered = lazy_df[lazy_df['value'] > 3]
    filtered_result = filtered.collect()
    
    SimpleAssert.equals(len(filtered_result), 2, "Should have 2 rows after filtering")
    SimpleAssert.is_true(all(v > 3 for v in filtered_result['value']), "All values should be > 3")
    
    # Test column selection
    selected = lazy_df[['value']]
    selected_result = selected.collect()
    
    SimpleAssert.equals(list(selected_result.columns), ['value'], "Should only have 'value' column")
    SimpleAssert.equals(len(selected_result), 5, "Should have all rows")


@test
def test_groupby_operations():
    """Test groupby operations"""
    increment_counter()
    
    # Create data with groups
    pair = TestDataFactory.dataframe_pair(
        query="""
        SELECT 'Group A' AS group_name, 10 AS value
        UNION ALL SELECT 'Group A', 20
        UNION ALL SELECT 'Group B', 30
        UNION ALL SELECT 'Group B', 40
        """
    )
    
    lazy_df = pair['lazy']
    
    # Test groupby sum
    grouped_sum = lazy_df.groupby('group_name').sum()
    sum_result = grouped_sum.collect()
    
    SimpleAssert.equals(len(sum_result), 2, "Should have 2 groups")
    SimpleAssert.contains(sum_result['group_name'].values, 'Group A', "Should contain Group A")
    SimpleAssert.contains(sum_result['group_name'].values, 'Group B', "Should contain Group B")
    
    # Test groupby max
    grouped_max = lazy_df.groupby('group_name').max()
    max_result = grouped_max.collect()
    
    SimpleAssert.equals(len(max_result), 2, "Should have 2 groups for max")


@skip_test("Demonstrating skip functionality")
def test_skipped_example():
    """This test is skipped to demonstrate skip functionality"""
    increment_counter()
    SimpleAssert.is_true(False, "This should never execute")


@test
def test_edge_cases():
    """Test edge cases and error conditions"""
    increment_counter()
    
    # Test with empty dataframe
    empty_df = TestDataFactory.dataframe_pair(query="SELECT 1 AS col WHERE FALSE")
    empty_result = empty_df['lazy'].collect()
    SimpleAssert.equals(len(empty_result), 0, "Empty query should return 0 rows")
    
    # Test with single row
    single_df = TestDataFactory.dataframe_pair(query="SELECT 'single' AS col")
    single_result = single_df['lazy'].collect()
    SimpleAssert.equals(len(single_result), 1, "Single row query should return 1 row")
    
    # Test with null values
    null_df = TestDataFactory.dataframe_pair(query="SELECT NULL AS null_col, 'not null' AS other_col")
    null_result = null_df['lazy'].collect()
    SimpleAssert.is_true(null_result['null_col'].isna().iloc[0], "Null value should be detected")
    SimpleAssert.is_false(null_result['other_col'].isna().iloc[0], "Non-null value should not be null")


if __name__ == "__main__":
    print("Running comprehensive simple testing framework demonstration...")
    print("=" * 70)
    
    # Run all tests in this module
    success = run_module_tests(globals(), verbose=True)
    
    print("\n" + "=" * 70)
    if success:
        print("🎉 All comprehensive tests passed!")
        print("The simple testing framework is working correctly!")
        sys.exit(0)
    else:
        print("💥 Some comprehensive tests failed!")
        sys.exit(1)