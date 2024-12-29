from typing import Any, Callable, Tuple, cast

from duckdb import ConstantExpression, Expression

__all__ = ["LazyColumn"]


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
    def __init__(self, expr: Expression):
        self.expr = expr

    def __neg__(self):
        return LazyColumn(-self.expr)

    __and__ = _bin_op("__and__")
    __or__ = _bin_op("__or__")
    __invert__ = _func_op("__invert__")
    __rand__ = _bin_op("__rand__")
    __ror__ = _bin_op("__ror__")

    __add__ = _bin_op("__add__")
    __sub__ = _bin_op("__sub__")
    __mul__ = _bin_op("__mul__")
    __div__ = _bin_op("__div__")
    __truediv__ = _bin_op("__truediv__")
    __mod__ = _bin_op("__mod__")
    __pow__ = _bin_op("__pow__")
    __radd__ = _bin_op("__radd__")
    __rsub__ = _bin_op("__rsub__")
    __rmul__ = _bin_op("__rmul__")
    __rdiv__ = _bin_op("__rdiv__")
    __rtruediv__ = _bin_op("__rtruediv__")
    __rmod__ = _bin_op("__rmod__")
    __rpow__ = _bin_op("__rpow__")
    __lt__ = _bin_op("__lt__")
    __le__ = _bin_op("__le__")
    __ge__ = _bin_op("__ge__")
    __gt__ = _bin_op("__gt__")

    # logistic operators
    def __eq__(  # type: ignore[override]
        self,
        other,
    ) -> "LazyColumn":
        """binary function"""
        return LazyColumn(self.expr == (_get_expr(other)))

    def __ne__(  # type: ignore[override]
        self,
        other: Any,
    ) -> "LazyColumn":
        """binary function"""
        return LazyColumn(self.expr != (_get_expr(other)))

    def isin(self, *cols: Any) -> "LazyColumn":
        if len(cols) == 1 and isinstance(cols[0], (list, set)):
            cols = cast(Tuple, cols[0])

        cols = cast(
            Tuple,
            [_get_expr(c) for c in cols],
        )
        return LazyColumn(self.expr.isin(*cols))
