"""
Simple Unit Testing Framework for Lazy Pandas

This module provides a lightweight testing framework that complements
the existing pytest infrastructure, making it easier for contributors
to write and run basic unit tests.
"""

import sys
import traceback
from typing import Any, Callable, Dict, List, Optional, Union
from functools import wraps
import pandas as pd
import duckdb
from .frame.lazy_frame import LazyFrame


class TestResult:
    """Represents the result of a single test"""
    
    def __init__(self, name: str, passed: bool, error: Optional[str] = None, 
                 execution_time: float = 0.0, skipped: bool = False, skip_reason: str = ""):
        self.name = name
        self.passed = passed
        self.error = error
        self.execution_time = execution_time
        self.skipped = skipped
        self.skip_reason = skip_reason
    
    def __str__(self):
        if self.skipped:
            status = "SKIP"
        else:
            status = "PASS" if self.passed else "FAIL"
        return f"[{status}] {self.name}"


class SimpleTestRunner:
    """A simple test runner for lazy-pandas tests"""
    
    def __init__(self):
        self.tests: List[Callable] = []
        self.results: List[TestResult] = []
        self.setup_functions: List[Callable] = []
        self.teardown_functions: List[Callable] = []
    
    def add_test(self, test_func: Callable):
        """Add a test function to the runner"""
        self.tests.append(test_func)
    
    def add_setup(self, setup_func: Callable):
        """Add a setup function to run before all tests"""
        self.setup_functions.append(setup_func)
    
    def add_teardown(self, teardown_func: Callable):
        """Add a teardown function to run after all tests"""
        self.teardown_functions.append(teardown_func)
    
    def run_tests(self, verbose: bool = True) -> bool:
        """Run all registered tests and return True if all passed"""
        self.results.clear()
        
        # Run setup functions
        for setup_func in self.setup_functions:
            try:
                setup_func()
            except Exception as e:
                print(f"Setup failed: {e}")
                return False
        
        # Run tests
        for test_func in self.tests:
            result = self._run_single_test(test_func)
            self.results.append(result)
            
            if verbose:
                print(result)
                if not result.passed and not result.skipped and result.error:
                    print(f"  Error: {result.error}")
                elif result.skipped and result.skip_reason:
                    print(f"  Reason: {result.skip_reason}")
        
        # Run teardown functions
        for teardown_func in self.teardown_functions:
            try:
                teardown_func()
            except Exception as e:
                print(f"Teardown failed: {e}")
        
        # Print summary
        passed = sum(1 for r in self.results if r.passed)
        skipped = sum(1 for r in self.results if r.skipped)
        failed = sum(1 for r in self.results if not r.passed and not r.skipped)
        total = len(self.results)
        
        if verbose:
            print(f"\n--- Test Summary ---")
            print(f"Tests run: {total}")
            print(f"Passed: {passed}")
            print(f"Failed: {failed}")
            if skipped > 0:
                print(f"Skipped: {skipped}")
        
        return failed == 0
    
    def _run_single_test(self, test_func: Callable) -> TestResult:
        """Run a single test function and return the result"""
        import time
        
        start_time = time.time()
        try:
            test_func()
            execution_time = time.time() - start_time
            return TestResult(test_func.__name__, True, execution_time=execution_time)
        except Exception as e:
            execution_time = time.time() - start_time
            error_msg = str(e)
            
            # Check if this is a skip exception
            if "Test skipped:" in error_msg:
                skip_reason = error_msg.replace("Test skipped: ", "")
                return TestResult(test_func.__name__, False, execution_time=execution_time, 
                                skipped=True, skip_reason=skip_reason)
            
            if hasattr(e, '__traceback__'):
                error_msg = traceback.format_exc()
            return TestResult(test_func.__name__, False, error_msg, execution_time)


class SimpleAssert:
    """Simple assertion utilities for testing"""
    
    @staticmethod
    def equals(actual: Any, expected: Any, message: str = ""):
        """Assert that two values are equal"""
        if actual != expected:
            raise AssertionError(f"{message} Expected: {expected}, but got: {actual}")
    
    @staticmethod
    def not_equals(actual: Any, expected: Any, message: str = ""):
        """Assert that two values are not equal"""
        if actual == expected:
            raise AssertionError(f"{message} Expected values to be different, but both were: {actual}")
    
    @staticmethod
    def is_true(value: Any, message: str = ""):
        """Assert that a value is True"""
        if not value:
            raise AssertionError(f"{message} Expected True, but got: {value}")
    
    @staticmethod
    def is_false(value: Any, message: str = ""):
        """Assert that a value is False"""
        if value:
            raise AssertionError(f"{message} Expected False, but got: {value}")
    
    @staticmethod
    def is_none(value: Any, message: str = ""):
        """Assert that a value is None"""
        if value is not None:
            raise AssertionError(f"{message} Expected None, but got: {value}")
    
    @staticmethod
    def is_not_none(value: Any, message: str = ""):
        """Assert that a value is not None"""
        if value is None:
            raise AssertionError(f"{message} Expected non-None value, but got None")
    
    @staticmethod
    def contains(container: Any, item: Any, message: str = ""):
        """Assert that a container contains an item"""
        if item not in container:
            raise AssertionError(f"{message} Expected {container} to contain {item}")
    
    @staticmethod
    def dataframes_equal(df1: pd.DataFrame, df2: pd.DataFrame, message: str = ""):
        """Assert that two DataFrames are equal"""
        try:
            pd.testing.assert_frame_equal(df1, df2, check_dtype=False)
        except AssertionError as e:
            raise AssertionError(f"{message} DataFrames are not equal: {str(e)}")
    
    @staticmethod
    def lazy_pandas_equal(lazy_df: LazyFrame, pandas_df: pd.DataFrame, message: str = ""):
        """Assert that a LazyFrame result equals a pandas DataFrame"""
        lazy_result = lazy_df.collect()
        SimpleAssert.dataframes_equal(lazy_result, pandas_df, message)


class TestDataFactory:
    """Factory for creating test data commonly used in lazy-pandas tests"""
    
    @staticmethod
    def simple_dataframe() -> pd.DataFrame:
        """Create a simple test DataFrame"""
        return pd.DataFrame({
            'a': [1, 2, 3],
            'b': [4, 5, 6]
        })
    
    @staticmethod
    def numeric_dataframe() -> pd.DataFrame:
        """Create a DataFrame with numeric data"""
        return pd.DataFrame({
            'int_col': [1, 2, 3, 4],
            'float_col': [1.1, 2.2, 3.3, 4.4],
            'negative_col': [-1, -2, -3, -4]
        })
    
    @staticmethod
    def string_dataframe() -> pd.DataFrame:
        """Create a DataFrame with string data"""
        return pd.DataFrame({
            'str_col': ['hello', 'world', 'test', 'data'],
            'mixed_col': ['Hello', 'WORLD', 'Test', 'DATA']
        })
    
    @staticmethod
    def datetime_dataframe() -> pd.DataFrame:
        """Create a DataFrame with datetime data"""
        return pd.DataFrame({
            'date_col': pd.date_range('2023-01-01', periods=4, freq='D'),
            'timestamp_col': pd.date_range('2023-01-01 10:00:00', periods=4, freq='h')
        })
    
    @staticmethod
    def create_lazy_frame(query: str) -> LazyFrame:
        """Create a LazyFrame from SQL query"""
        relation = duckdb.sql(query)
        return LazyFrame(relation)
    
    @staticmethod
    def dataframe_pair(query: str = None, pandas_df: pd.DataFrame = None):
        """Create a pair of equivalent LazyFrame and pandas DataFrame for testing"""
        if query:
            lazy_df = TestDataFactory.create_lazy_frame(query)
            pandas_df = lazy_df.collect()
        elif pandas_df is not None:
            lazy_df = LazyFrame(duckdb.from_df(pandas_df))
        else:
            # Default simple pair
            pandas_df = TestDataFactory.simple_dataframe()
            lazy_df = LazyFrame(duckdb.from_df(pandas_df))
        
        return {'lazy': lazy_df, 'pandas': pandas_df}


# Decorators for test functions
def test(func: Callable) -> Callable:
    """Decorator to mark a function as a test"""
    func._is_test = True
    return func


def setup(func: Callable) -> Callable:
    """Decorator to mark a function as setup"""
    func._is_setup = True
    return func


def teardown(func: Callable) -> Callable:
    """Decorator to mark a function as teardown"""
    func._is_teardown = True
    return func


def skip_test(reason: str = ""):
    """Decorator to skip a test"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            raise Exception(f"Test skipped: {reason}")
        return wrapper
    return decorator


# Global test runner instance
_test_runner = SimpleTestRunner()


def run_module_tests(module_dict: Dict[str, Any], verbose: bool = True) -> bool:
    """
    Discover and run all tests in a module
    
    Args:
        module_dict: Usually pass globals() to test current module
        verbose: Whether to print detailed output
    
    Returns:
        True if all tests passed, False otherwise
    """
    runner = SimpleTestRunner()
    
    # Discover test functions, setup, and teardown
    for name, obj in module_dict.items():
        if callable(obj):
            if hasattr(obj, '_is_test') or name.startswith('test_'):
                runner.add_test(obj)
            elif hasattr(obj, '_is_setup') or name.startswith('setup_'):
                runner.add_setup(obj)
            elif hasattr(obj, '_is_teardown') or name.startswith('teardown_'):
                runner.add_teardown(obj)
    
    return runner.run_tests(verbose)


def run_tests_from_file(filepath: str, verbose: bool = True) -> bool:
    """
    Run tests from a Python file
    
    Args:
        filepath: Path to the Python file containing tests
        verbose: Whether to print detailed output
    
    Returns:
        True if all tests passed, False otherwise
    """
    import importlib.util
    import os
    
    if not os.path.exists(filepath):
        print(f"Test file not found: {filepath}")
        return False
    
    # Load the module
    spec = importlib.util.spec_from_file_location("test_module", filepath)
    module = importlib.util.module_from_spec(spec)
    
    try:
        spec.loader.exec_module(module)
    except Exception as e:
        print(f"Error loading test file {filepath}: {e}")
        return False
    
    # Run tests from the module
    return run_module_tests(module.__dict__, verbose)