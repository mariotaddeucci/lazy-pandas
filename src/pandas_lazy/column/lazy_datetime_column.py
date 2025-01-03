from typing import TYPE_CHECKING

from duckdb import ConstantExpression
from duckdb.typing import DATE

if TYPE_CHECKING:
    from pandas_lazy.column.lazy_column import LazyColumn


class LazyDateTimeColumn:
    def __init__(self, col: "LazyColumn"):
        self.col = col

    def date(self) -> "LazyColumn":
        return self.col.astype(DATE)

    def year(self) -> "LazyColumn":
        return self.col.create_from_function("year", self.col.expr)

    def quarter(self) -> "LazyColumn":
        return self.col.create_from_function("quarter", self.col.expr)

    def month(self) -> "LazyColumn":
        return self.col.create_from_function("month", self.col.expr)

    def day(self) -> "LazyColumn":
        return self.col.create_from_function("day", self.col.expr)

    def is_month_start(self) -> "LazyColumn":
        return self.col == self.col.create_from_function("date_trunc", ConstantExpression("month"), self.col.expr)

    def is_quarter_start(self) -> "LazyColumn":
        return self.col == self.col.create_from_function("date_trunc", ConstantExpression("quarter"), self.col.expr)

    def is_year_start(self) -> "LazyColumn":
        return self.col == self.col.create_from_function("date_trunc", ConstantExpression("year"), self.col.expr)

    def is_month_end(self) -> "LazyColumn":
        return self.col == self.col.create_from_function("last_day", self.col.expr)
