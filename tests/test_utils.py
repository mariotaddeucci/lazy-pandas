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
    """Testa a função to_column_expr com string."""
    # Teste com uma string
    result = to_column_expr("column_name")
    # Verificamos que o resultado contém o nome da coluna (formato pode variar)
    assert "column_name" in str(result)


def test_to_column_expr_with_expression():
    """Testa a função to_column_expr com expressão."""
    # Teste com uma expressão existente
    # Criamos uma expressão diretamente com o módulo duckdb
    from duckdb import ColumnExpression

    expr = ColumnExpression("existing")
    result = to_column_expr(expr)
    assert result is expr  # Deve retornar a mesma instância


def test_to_column_expr_with_invalid_type():
    """Testa a função to_column_expr com tipo inválido."""
    # Teste com tipo não suportado
    with pytest.raises(NotImplementedError):
        to_column_expr(123)


def test_get_expr_with_expression():
    """Testa a função get_expr com expressão."""
    # Teste com uma expressão existente
    from duckdb import ColumnExpression

    expr = ColumnExpression("column")
    result = get_expr(expr)
    assert result is expr  # Deve retornar a mesma instância


def test_get_expr_with_constant():
    """Testa a função get_expr com valor constante."""
    # Teste com valor constante (deve criar uma ConstantExpression)
    result = get_expr(42)
    assert "42" in str(result)

    # Teste com string (que não é uma expressão)
    result = get_expr("value")
    assert "value" in str(result)


def test_func_op_doc():
    """Testa se a documentação é preservada na função func_op."""
    # Verificamos se a documentação foi preservada
    neg_op = func_op("__neg__", "Negação")
    assert neg_op.__doc__ == "Negação"


def test_bin_op_doc():
    """Testa se a documentação é preservada na função bin_op."""
    # Verificamos se a documentação foi preservada
    add_op = bin_op("__add__", "Adição")
    assert add_op.__doc__ == "Adição"


def test_invoke_function_basic():
    """Testa a função invoke_function com argumentos simples."""
    # Testamos a função com argumentos que o DuckDB aceita
    from duckdb import ColumnExpression

    col_expr = ColumnExpression("col1")
    result = invoke_function("abs", col_expr)
    assert "abs" in str(result).lower()
    assert "col1" in str(result)


def test_invoke_function_over_columns_strings():
    """Testa a função invoke_function_over_columns com strings."""
    # Testamos a função com nomes de colunas (strings)
    result = invoke_function_over_columns("sum", "col1", "col2")
    assert "sum" in str(result).lower()
    assert "col1" in str(result)
    assert "col2" in str(result)
