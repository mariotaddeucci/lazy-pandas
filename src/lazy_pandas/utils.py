from typing import Any, Callable, Union

from duckdb import ColumnExpression, ConstantExpression, Expression, FunctionExpression

ColumnOrExpression = Union["Expression", str]


def to_column_expr(col: ColumnOrExpression) -> Expression:
    if isinstance(col, Expression):
        return col
    elif isinstance(col, str):
        return ColumnExpression(col)
    raise NotImplementedError


def get_expr(x) -> Expression:
    return x if isinstance(x, Expression) else ConstantExpression(x)


def func_op(name: str, doc: str = "") -> Callable[[Expression], Expression]:
    def _(expr: Expression) -> Expression:
        return getattr(expr, name)()

    _.__doc__ = doc
    return _


def bin_op(
    name: str,
    doc: str = "binary operator",
) -> Callable[[Expression, Any], Expression]:
    def _(expr: Expression, other) -> Expression:
        jc = get_expr(other)
        return getattr(expr, name)(jc)

    _.__doc__ = doc
    return _


def invoke_function(function: str, *arguments: Expression) -> Expression:
    return FunctionExpression(function, *arguments)


def invoke_function_over_columns(name: str, *cols: ColumnOrExpression) -> Expression:
    exprs = [to_column_expr(expr) for expr in cols]
    return invoke_function(name, *exprs)
