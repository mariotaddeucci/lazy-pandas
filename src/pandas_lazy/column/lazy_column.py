from typing import Any, Callable, Literal, Tuple, Union, cast

from duckdb import CoalesceOperator, ConstantExpression, Expression, FunctionExpression
from duckdb.typing import DuckDBPyType
from pandas_lazy.column.lazy_datetime_column import LazyDateTimeColumn
from pandas_lazy.column.lazy_string_column import LazyStringColumn

__all__ = ["LazyColumn"]

ColumnOrName = Union["LazyColumn", str]


class UnsupporttedOperation(Exception): ...


def _get_expr(x) -> Expression:
    return x.expr if isinstance(x, LazyColumn) else ConstantExpression(x)


def _func_op(name: str, doc: str = "") -> Callable[["LazyColumn"], "LazyColumn"]:
    def _(self: "LazyColumn") -> "LazyColumn":
        njc = getattr(self.expr, name)()
        return LazyColumn(njc)

    _.__doc__ = doc
    return _


def _bin_op(
    name: str,
    doc: str = "binary operator",
) -> Callable[["LazyColumn", Any], "LazyColumn"]:
    def _(self: "LazyColumn", other) -> "LazyColumn":
        jc = _get_expr(other)
        njc = getattr(self.expr, name)(jc)
        return LazyColumn(njc)

    _.__doc__ = doc
    return _


class LazyColumn:
    __div__ = _bin_op("__div__")
    __rdiv__ = _bin_op("__rdiv__")

    def __init__(self, expr: Expression):
        self.expr = expr

    def abs(self) -> "LazyColumn":
        return self.create_from_function("abs", self.expr)

    def round(self, decimals: int = 0) -> "LazyColumn":
        return self.create_from_function("round", self.expr, ConstantExpression(decimals))

    @classmethod
    def create_from_function(cls, function: str, *arguments: Expression) -> "LazyColumn":
        return LazyColumn(FunctionExpression(function, *arguments))

    def isin(self, *cols: Any) -> "LazyColumn":
        if len(cols) == 1 and isinstance(cols[0], (list, set)):
            cols = cast(Tuple, cols[0])

        cols = cast(Tuple, [_get_expr(c) for c in cols])

        return LazyColumn(self.expr.isin(*cols))

    def astype(self, dtype: str | DuckDBPyType) -> "LazyColumn":
        if isinstance(dtype, str):
            dtype = DuckDBPyType(dtype)
        return LazyColumn(self.expr.cast(dtype))

    def fillna(self, value: Any) -> "LazyColumn":
        return LazyColumn(CoalesceOperator(self.expr, _get_expr(value)))

    def isnull(self) -> "LazyColumn":
        return LazyColumn(self.expr.isnull())

    def isna(self) -> "LazyColumn":
        return self.isnull()

    def notnull(self) -> "LazyColumn":
        return LazyColumn(self.expr.isnotnull())

    def notna(self) -> "LazyColumn":
        return self.notnull()

    def between(
        self, left: Any, right: Any, inclusive: Literal["both", "neither", "left", "right"] = "both"
    ) -> "LazyColumn":
        left_expr = _get_expr(left)
        right_expr = _get_expr(right)

        if inclusive == "both":
            result = self.expr >= left_expr & self.expr <= right_expr
        elif inclusive == "neither":
            result = self.expr > left_expr & self.expr < right_expr
        elif inclusive == "left":
            result = self.expr >= left_expr & self.expr < right_expr
        elif inclusive == "right":
            result = self.expr > left_expr & self.expr <= right_expr
        else:
            raise ValueError(f"Invalid value for inclusive: {inclusive}")

        return LazyColumn(result)

    @property
    def dt(self) -> LazyDateTimeColumn:
        return LazyDateTimeColumn(self)

    @property
    def str(self) -> "LazyStringColumn":
        return LazyStringColumn(self)

    __add__ = _bin_op("__add__")
    __radd__ = _bin_op("__radd__")
    __sub__ = _bin_op("__sub__")
    __rsub__ = _bin_op("__rsub__")
    __mul__ = _bin_op("__mul__")
    __rmul__ = _bin_op("__rmul__")
    __truediv__ = _bin_op("__truediv__")
    __rtruediv__ = _bin_op("__rtruediv__")
    __mod__ = _bin_op("__mod__")
    __rmod__ = _bin_op("__rmod__")
    __pow__ = _bin_op("__pow__")
    __rpow__ = _bin_op("__rpow__")
    __and__ = _bin_op("__and__")
    __rand__ = _bin_op("__rand__")
    __or__ = _bin_op("__or__")
    __ror__ = _bin_op("__ror__")

    def __neg__(self):
        return LazyColumn(-self.expr)

    __invert__ = _func_op("__invert__")
    __lt__ = _bin_op("__lt__")
    __le__ = _bin_op("__le__")

    def __eq__(  # type: ignore[override]
        self,
        other,
    ) -> "LazyColumn":
        return LazyColumn(self.expr == (_get_expr(other)))

    def __ne__(  # type: ignore[override]
        self,
        other: Any,
    ) -> "LazyColumn":
        return LazyColumn(self.expr != (_get_expr(other)))

    __gt__ = _bin_op("__gt__")
    __ge__ = _bin_op("__ge__")
