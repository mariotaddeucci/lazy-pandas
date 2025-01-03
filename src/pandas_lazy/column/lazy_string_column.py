from typing import TYPE_CHECKING

from duckdb import ConstantExpression

if TYPE_CHECKING:
    from pandas_lazy.column.lazy_column import LazyColumn


class LazyStringColumn:
    def __init__(self, col: "LazyColumn"):
        self.col = col

    def lower(self) -> "LazyColumn":
        return self.col.create_from_function("lower", self.col.expr)

    def upper(self) -> "LazyColumn":
        return self.col.create_from_function("upper", self.col.expr)

    def strip(self) -> "LazyColumn":
        return self.col.create_from_function("trim", self.col.expr)

    def lstrip(self) -> "LazyColumn":
        return self.col.create_from_function("ltrim", self.col.expr)

    def rstrip(self) -> "LazyColumn":
        return self.col.create_from_function("rtrim", self.col.expr)

    def len(self) -> "LazyColumn":
        return self.col.create_from_function("len", self.col.expr)

    def replace(self, old: str, new: str) -> "LazyColumn":
        return self.col.create_from_function("replace", self.col.expr, ConstantExpression(old), ConstantExpression(new))

    def startswith(self, prefix: str) -> "LazyColumn":
        return self.col.create_from_function("starts_with", self.col.expr, ConstantExpression(prefix))

    def endswith(self, suffix: str) -> "LazyColumn":
        return self.col.create_from_function("ends_with", self.col.expr, ConstantExpression(suffix))

    def contains(self, pat: str) -> "LazyColumn":
        return self.col.create_from_function("contains", self.col.expr, ConstantExpression(pat))

    def zfill(self, width: int) -> "LazyColumn":
        return self.col.create_from_function("lpad", self.col.expr, ConstantExpression(width), ConstantExpression("0"))
