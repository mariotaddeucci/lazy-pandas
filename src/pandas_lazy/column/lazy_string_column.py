from typing import TYPE_CHECKING, Literal

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

    def find(self, sub: str) -> "LazyColumn":
        return self.col.create_from_function("instr", self.col.expr, ConstantExpression(sub)) - 1

    def pad(self, width: int, side: Literal["left", "right", "both"] = "left", fillchar: str = " ") -> "LazyColumn":
        if side not in {"left", "right", "both"}:
            raise ValueError("side must be 'left', 'right', or 'both'")

        if side == "both":
            raise NotImplementedError("side='both' is not supported yet")

        return self.col.create_from_function(
            "lpad" if side == "left" else "rpad", self.col.expr, ConstantExpression(width), ConstantExpression(fillchar)
        )

    def zfill(self, width: int) -> "LazyColumn":
        return self.pad(width, fillchar="0")

    def ljust(self, width: int, fillchar: str = " ") -> "LazyColumn":
        return self.pad(width, side="left", fillchar=fillchar)

    def rjust(self, width: int, fillchar: str = " ") -> "LazyColumn":
        return self.pad(width, side="right", fillchar=fillchar)
