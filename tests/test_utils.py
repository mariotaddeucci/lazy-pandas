import pytest

from lazy_pandas.utils import (
    bin_op,
    func_op,
    get_expr,
    invoke_function,
    invoke_function_over_columns,
    to_column_expr,
)


def test_to_column_expr_with_string():
    """Tests the to_column_expr function with string."""
    # Test with a string
    result = to_column_expr("column_name")
    # We verify that the result contains the column name (format may vary)
    assert "column_name" in str(result)


def test_to_column_expr_with_expression():
    """Tests the to_column_expr function with expression."""
    # Test with an existing expression
    # We create an expression directly with the duckdb module
    from duckdb import ColumnExpression

    expr = ColumnExpression("existing")
    result = to_column_expr(expr)
    assert result is expr  # Should return the same instance


def test_to_column_expr_with_invalid_type():
    """Tests the to_column_expr function with invalid type."""
    # Test with unsupported type
    with pytest.raises(NotImplementedError):
        to_column_expr(123)


def test_get_expr_with_expression():
    """Tests the get_expr function with expression."""
    # Test with an existing expression
    from duckdb import ColumnExpression

    expr = ColumnExpression("column")
    result = get_expr(expr)
    assert result is expr  # Should return the same instance


def test_get_expr_with_constant():
    """Tests the get_expr function with constant value."""
    # Test with constant value (should create a ConstantExpression)
    result = get_expr(42)
    assert "42" in str(result)

    # Test with string (which is not an expression)
    result = get_expr("value")
    assert "value" in str(result)


def test_func_op_doc():
    """Tests if documentation is preserved in the func_op function."""
    # We verify if the documentation was preserved
    neg_op = func_op("__neg__", "Negation")
    assert neg_op.__doc__ == "Negation"


def test_bin_op_doc():
    """Tests if documentation is preserved in the bin_op function."""
    # We verify if the documentation was preserved
    add_op = bin_op("__add__", "Addition")
    assert add_op.__doc__ == "Addition"


def test_invoke_function_basic():
    """Tests the invoke_function with simple arguments."""
    # We test the function with arguments that DuckDB accepts
    from duckdb import ColumnExpression

    col_expr = ColumnExpression("col1")
    result = invoke_function("abs", col_expr)
    assert "abs" in str(result).lower()
    assert "col1" in str(result)


def test_invoke_function_over_columns_strings():
    """Tests the invoke_function_over_columns with strings."""
    # We test the function with column names (strings)
    result = invoke_function_over_columns("sum", "col1", "col2")
    assert "sum" in str(result).lower()
    assert "col1" in str(result)
    assert "col2" in str(result)
